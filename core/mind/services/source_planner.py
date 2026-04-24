from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Sequence

from scripts.books import enrich as books_enrich
from scripts.substack import auth as substack_auth
from scripts.substack import enrich as substack_enrich
from scripts.youtube import enrich as youtube_enrich
from scripts.youtube.transcript import NoCaptionsAvailable
from mind.services import reingest as reingest_service
from mind.services.source_adapters import ADAPTERS, CacheOnlyCandidate, PageBackedCandidate
from mind.services.source_models import (
    CompletedExecutionItem,
    InventoryItem,
    InventoryRequest,
    InventoryResult,
    PlanExecutionResult,
    PlanItem,
    PlanRequest,
    PlanResult,
    ReconcileResult,
    SELECTION_VALUES,
    SourceKey,
)
from mind.services.source_registry import (
    SourceAliasRow,
    SourceArtifactRow,
    SourceRegistry,
    SourceRegistryRecord,
    SourceRegistryRow,
    SourceStageRow,
)


def _utc_now_string() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_dumps(value: dict[str, Any] | None) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True)


def _selection_values(raw: str | Sequence[str] | None) -> set[str]:
    if raw is None:
        return {"all"}
    values: list[str] = []
    if isinstance(raw, str):
        values.extend(item.strip() for item in raw.split(","))
    else:
        for item in raw:
            values.extend(part.strip() for part in str(item).split(","))
    selected = {item for item in values if item}
    if not selected:
        return {"all"}
    invalid = selected - set(SELECTION_VALUES)
    if invalid:
        raise ValueError(
            f"unsupported selection values: {', '.join(sorted(invalid))}; expected one of {', '.join(SELECTION_VALUES)}"
        )
    return {"all"} if "all" in selected else selected


def _is_slice_scoped(request: InventoryRequest) -> bool:
    return bool(request.path or request.today or request.source_ids or request.external_ids)


def _filter_inventory_items(items: list[InventoryItem], request: InventoryRequest) -> list[InventoryItem]:
    selections = _selection_values(request.selection)
    requested_source_ids = set(request.source_ids)
    requested_external_ids = set(request.external_ids)
    filtered: list[InventoryItem] = []
    for item in items:
        if selections != {"all"} and item.status not in selections:
            continue
        aliases = {str(item.source_key), item.source_id or "", item.external_id or "", *item.aliases}
        if requested_source_ids and not (aliases & requested_source_ids):
            continue
        if requested_external_ids and not (aliases & requested_external_ids):
            continue
        filtered.append(item)
    filtered.sort(key=lambda item: (item.status, item.title.lower(), str(item.source_key)))
    return filtered


def _match_candidate(aliases: Sequence[str], unmatched: dict[SourceKey, Any], alias_map: dict[str, Any]) -> Any | None:
    for alias in aliases:
        candidate = alias_map.get(alias)
        if candidate is None:
            continue
        unmatched.pop(candidate.source_key, None)
        return candidate
    return None


def _slice_selected_candidates(
    *,
    request: InventoryRequest,
    page_candidates: dict[SourceKey, PageBackedCandidate],
    cache_candidates: dict[SourceKey, CacheOnlyCandidate],
) -> tuple[list[PageBackedCandidate], list[CacheOnlyCandidate]]:
    if not (request.source_ids or request.external_ids):
        return [], []
    requested = set(request.source_ids) | set(request.external_ids)
    selected_pages = [
        candidate
        for candidate in page_candidates.values()
        if {str(candidate.source_key), candidate.source_id or "", candidate.external_id or "", *candidate.aliases} & requested
    ]
    selected_caches = [
        candidate
        for candidate in cache_candidates.values()
        if {str(candidate.source_key), candidate.source_id or "", candidate.external_id or "", *candidate.aliases} & requested
    ]
    return selected_pages, selected_caches


def build_inventory(
    request: InventoryRequest,
    *,
    repo_root: Path,
    use_registry: bool = True,
    phase_callback: Callable[[str], None] | None = None,
) -> InventoryResult:
    if phase_callback is not None:
        if _is_slice_scoped(request):
            phase_callback(f"inventorying selected {request.lane}")
        else:
            phase_callback(f"inventorying all {request.lane}")
    adapter = ADAPTERS[request.lane]
    registry = SourceRegistry.for_repo_root(repo_root) if use_registry else None

    if phase_callback is not None:
        phase_callback(f"scanning upstream {request.lane}")
    upstream_items = adapter.enumerate_upstream(request, repo_root)
    if phase_callback is not None:
        phase_callback(f"scanning durable {request.lane} pages")
    page_candidates = {candidate.source_key: candidate for candidate in adapter.enumerate_page_backed(repo_root)}
    if phase_callback is not None:
        phase_callback(f"scanning cache-only {request.lane} sources")
    cache_candidates = {candidate.source_key: candidate for candidate in adapter.enumerate_cache_only(repo_root)}
    page_alias_map = {alias: candidate for candidate in page_candidates.values() for alias in candidate.aliases}
    cache_alias_map = {alias: candidate for candidate in cache_candidates.values() for alias in candidate.aliases}

    items: list[InventoryItem] = []
    for lane_item in upstream_items:
        aliases = adapter.upstream_aliases(lane_item)
        page_candidate = _match_candidate(aliases, page_candidates, page_alias_map)
        cache_candidate = _match_candidate(aliases, cache_candidates, cache_alias_map)
        items.append(
            adapter.build_inventory_from_upstream(
                lane_item,
                request=request,
                page_candidate=page_candidate,
                cache_candidate=cache_candidate,
                repo_root=repo_root,
                registry=registry,
            )
        )

    if _is_slice_scoped(request):
        slice_pages, slice_caches = _slice_selected_candidates(
            request=request,
            page_candidates=page_candidates,
            cache_candidates=cache_candidates,
        )
        for candidate in slice_pages:
            items.append(adapter.build_inventory_from_page(candidate, repo_root=repo_root, registry=registry))
            page_candidates.pop(candidate.source_key, None)
        for candidate in slice_caches:
            items.append(adapter.build_inventory_from_cache(candidate, repo_root=repo_root, registry=registry))
            cache_candidates.pop(candidate.source_key, None)
    else:
        for candidate in page_candidates.values():
            items.append(adapter.build_inventory_from_page(candidate, repo_root=repo_root, registry=registry))
        for candidate in cache_candidates.values():
            items.append(adapter.build_inventory_from_cache(candidate, repo_root=repo_root, registry=registry))

    filtered = _filter_inventory_items(items, request)
    if request.limit is not None:
        filtered = filtered[: request.limit]
    return InventoryResult(request=request, items=tuple(filtered))


def _plan_item(
    item: InventoryItem,
    *,
    lane: str,
    resume: bool,
    skip_materialized: bool,
    refresh_stale: bool,
    recompute_missing: bool,
    explicit_from_stage: str | None,
) -> tuple[str, str | None, str | None]:
    if item.excluded_reason:
        return "excluded", None, item.excluded_reason
    if explicit_from_stage is not None:
        return f"resume_from_{explicit_from_stage}", explicit_from_stage, None
    if item.status == "materialized":
        return ("skip_materialized", None, None) if skip_materialized else ("noop", None, None)
    if item.status == "stale":
        stale_stage = next((stage.stage for stage in item.stage_states if stage.freshness == "stale"), None)
        if refresh_stale and stale_stage is not None:
            return f"refresh_stale_from_{stale_stage}", stale_stage, None
        return "noop", None, None
    if item.status == "incomplete":
        resume_stage = _earliest_resume_stage(item)
        if resume and resume_stage is not None:
            return f"resume_from_{resume_stage}", resume_stage, None
        return "noop", None, None
    if item.status == "unseen":
        if lane in {"substack", "youtube"} or recompute_missing:
            return "resume_from_acquire", "acquire", None
        return "blocked_missing_artifacts", None, item.blocked_reason or "missing required reusable artifacts"
    if item.status == "blocked":
        if recompute_missing:
            return "resume_from_acquire", "acquire", None
        return "blocked_missing_artifacts", None, item.blocked_reason or "missing required reusable artifacts"
    return "noop", None, None


def _earliest_resume_stage(item: InventoryItem) -> str | None:
    stage_map = {stage.stage: stage for stage in item.stage_states}
    dependencies = {
        "pass_a": ("acquire",),
        "pass_b": ("acquire", "pass_a"),
        "pass_c": ("acquire", "pass_a", "pass_b"),
        "pass_d": ("acquire", "pass_a", "pass_b", "pass_c"),
        "materialize": ("acquire", "pass_a", "pass_b", "pass_c"),
        "propagate": ("acquire", "pass_a", "pass_b", "pass_c", "materialize"),
    }
    for stage in reingest_service.REINGEST_STAGE_ORDER[1:]:
        probe = stage_map.get(stage)
        if probe is None or probe.status == "completed":
            continue
        if all(stage_map.get(dep) and stage_map[dep].status == "completed" for dep in dependencies.get(stage, ())):
            return stage
        return None
    return None


def build_plan(
    request: PlanRequest,
    *,
    repo_root: Path,
    use_registry: bool = True,
    phase_callback: Callable[[str], None] | None = None,
) -> PlanResult:
    inventory = build_inventory(
        InventoryRequest(
            lane=request.lane,
            path=request.path,
            today=request.today,
            source_ids=request.source_ids,
            external_ids=request.external_ids,
            selection=request.selection,
            limit=request.limit,
            lane_options=request.lane_options,
        ),
        repo_root=repo_root,
        use_registry=use_registry,
        phase_callback=phase_callback,
    )
    if phase_callback is not None:
        phase_callback("planning resumable actions")
    normalized_through = reingest_service.normalize_reingest_stage(request.through)
    explicit_from_stage = reingest_service.normalize_reingest_stage(request.from_stage) if request.from_stage else None
    items: list[PlanItem] = []
    for item in inventory.items:
        action, start_stage, blocked_reason = _plan_item(
            item,
            lane=request.lane,
            resume=request.resume,
            skip_materialized=request.skip_materialized,
            refresh_stale=request.refresh_stale,
            recompute_missing=request.recompute_missing,
            explicit_from_stage=explicit_from_stage,
        )
        items.append(
            PlanItem(
                source_key=item.source_key,
                lane=item.lane,
                title=item.title,
                status=item.status,
                action=action,
                start_stage=start_stage,
                through_stage=normalized_through if start_stage else None,
                source_id=item.source_id,
                external_id=item.external_id,
                blocked_reason=blocked_reason or item.blocked_reason,
                excluded_reason=item.excluded_reason,
                canonical_page_path=item.canonical_page_path,
            )
        )
    return PlanResult(request=request, inventory=inventory, items=tuple(items))


def inventory_item_to_registry_record(item: InventoryItem) -> SourceRegistryRecord:
    now = _utc_now_string()
    metadata = {
        "source_id": item.source_id,
        "external_id": item.external_id,
        "registry_status": item.registry_status,
        **item.metadata,
    }
    source = SourceRegistryRow(
        source_key=str(item.source_key),
        lane=item.lane,
        adapter=item.adapter,
        title=item.title,
        source_date=item.source_date,
        status=item.status,
        first_seen_at=now,
        last_seen_at=now,
        canonical_page_path=item.canonical_page_path,
        excluded_reason=item.excluded_reason,
        blocked_reason=item.blocked_reason,
        metadata_json=_json_dumps(metadata),
    )
    aliases = [
        SourceAliasRow(source_key=str(item.source_key), alias=alias, alias_type=_alias_type_for(item, alias))
        for alias in dict.fromkeys(item.aliases)
        if alias
    ]
    stages = [
        SourceStageRow(
            source_key=str(item.source_key),
            stage=stage.stage,
            status=stage.status,
            freshness=stage.freshness,
            artifact_path=stage.artifact_path,
            summary=stage.summary,
            updated_at=now,
        )
        for stage in item.stage_states
    ]
    artifacts = [
        SourceArtifactRow(
            source_key=str(item.source_key),
            artifact_kind=artifact.artifact_kind,
            path=artifact.path,
            fingerprint=artifact.fingerprint,
            exists=artifact.exists,
            updated_at=now,
        )
        for artifact in item.artifacts
    ]
    return SourceRegistryRecord(source=source, aliases=aliases, stages=stages, artifacts=artifacts)


def _alias_type_for(item: InventoryItem, alias: str) -> str:
    if alias == str(item.source_key):
        return "source_key"
    if alias == item.source_id:
        return "source_id"
    if alias == item.external_id:
        return "external_id"
    if item.canonical_page_path and alias == Path(item.canonical_page_path).stem:
        return "page_id"
    if item.lane == "books" and alias == (item.external_id or "").removeprefix("audible-"):
        return "upstream_id"
    if item.lane == "youtube" and alias == (item.external_id or "").removeprefix("youtube-"):
        return "upstream_id"
    if item.lane == "substack" and alias == (item.external_id or "").removeprefix("substack-"):
        return "upstream_id"
    return "alias"


def rebuild_source_registry(*, repo_root: Path, phase_callback: Callable[[str], None] | None = None) -> tuple[SourceRegistry, int]:
    registry = SourceRegistry.for_repo_root(repo_root)
    records: dict[SourceKey, SourceRegistryRecord] = {}
    for lane in ("books", "youtube", "articles", "substack"):
        inventory = build_inventory(
            InventoryRequest(lane=lane),
            repo_root=repo_root,
            use_registry=False,
            phase_callback=phase_callback,
        )
        for item in inventory.items:
            records[item.source_key] = inventory_item_to_registry_record(item)
    if phase_callback is not None:
        phase_callback("writing source registry rows")
    registry.replace_all(list(records.values()))
    return registry, len(records)


def refresh_registry_for_inventory(
    inventory: InventoryResult,
    *,
    repo_root: Path,
) -> SourceRegistry:
    registry = SourceRegistry.for_repo_root(repo_root)
    for item in inventory.items:
        registry.upsert_record(inventory_item_to_registry_record(item))
    return registry


def reconcile_source_registry(
    request: InventoryRequest,
    *,
    repo_root: Path,
    phase_callback: Callable[[str], None] | None = None,
) -> ReconcileResult:
    adapter = ADAPTERS[request.lane]
    registry = SourceRegistry.for_repo_root(repo_root)
    if phase_callback is not None:
        phase_callback(f"scanning upstream {request.lane}")
    upstream_items = adapter.enumerate_upstream(request, repo_root)
    upstream_keys = {SourceKey(adapter.upstream_aliases(item)[0]) for item in upstream_items}
    live_inventory = build_inventory(request, repo_root=repo_root, use_registry=True, phase_callback=phase_callback)

    prior_rows = {SourceKey(row.source_key): row for row in registry.list_sources(lane=request.lane)}
    changed = 0
    new = 0
    for item in live_inventory.items:
        existing = prior_rows.get(item.source_key)
        if existing is None:
            new += 1
        elif existing.status != item.status or existing.canonical_page_path != item.canonical_page_path:
            changed += 1
        registry.upsert_record(inventory_item_to_registry_record(item))

    if phase_callback is not None:
        phase_callback(f"scanning drift for {request.lane}")
    page_candidates = adapter.enumerate_page_backed(repo_root)
    cache_candidates = adapter.enumerate_cache_only(repo_root)
    registry_keys = set(prior_rows.keys())
    matched_inventory_items = [item for item in live_inventory.items if item.source_key in upstream_keys]
    registry_matched_count = sum(1 for item in matched_inventory_items if item.registry_status is not None or item.source_key in registry_keys)
    page_matched_count = sum(1 for item in matched_inventory_items if item.canonical_page_path)

    registry_only_rows = [row for key, row in prior_rows.items() if key not in upstream_keys]
    page_only_rows = [candidate for candidate in page_candidates if candidate.source_key not in registry_keys]
    cache_only_rows = [candidate for candidate in cache_candidates if candidate.source_key not in upstream_keys]

    return ReconcileResult(
        request=request,
        refreshed_count=len(live_inventory.items),
        changed_count=changed,
        new_count=new,
        removed_count=len(registry_only_rows),
        upstream_selected_count=len(upstream_keys),
        registry_matched_count=registry_matched_count,
        page_matched_count=page_matched_count,
        registry_only_count=len(registry_only_rows),
        page_only_count=len(page_only_rows),
        cache_only_count=len(cache_only_rows),
        registry_only_samples=tuple(row.title for row in registry_only_rows[:5]),
        page_only_samples=tuple(candidate.title for candidate in page_only_rows[:5]),
        cache_only_samples=tuple(candidate.title for candidate in cache_only_rows[:5]),
        inventory=live_inventory,
    )


def execute_books_plan(
    plan: PlanResult,
    *,
    repo_root: Path,
    force_deep: bool = False,
    phase_callback: Callable[[str], None] | None = None,
) -> PlanExecutionResult:
    if plan.request.lane != "books":
        raise ValueError("books execution only supports the books lane")
    blocked_samples = tuple(plan.blocked_samples)
    executed = 0
    failed = 0
    failed_items: list[str] = []
    completed_items: list[CompletedExecutionItem] = []
    page_ids: list[str] = []
    inventory_by_key = {item.source_key: item for item in plan.inventory.items}
    actionable = [item for item in plan.items if item.action.startswith("resume_from_") or item.action.startswith("refresh_stale_from_")]
    if phase_callback is not None and actionable:
        phase_callback(f"executing {len(actionable)} planned actions")
    for item in plan.items:
        if not (item.action.startswith("resume_from_") or item.action.startswith("refresh_stale_from_")):
            continue
        inventory_item = inventory_by_key.get(item.source_key)
        if inventory_item is None or inventory_item.source_id is None:
            failed += 1
            failed_items.append(f"{item.title}: missing source_id alias for execution")
            continue
        try:
            if item.start_stage == "acquire":
                lifecycle = books_enrich.run_book_record_lifecycle(
                    inventory_item.payload,
                    repo_root=repo_root,
                    today=plan.request.today or _utc_now_string()[:10],
                    force_deep=force_deep,
                )
                if lifecycle is None:
                    raise ValueError("book lifecycle returned no result")
                executed += 1
                completed_item = CompletedExecutionItem(
                    source_key=item.source_key,
                    title=item.title,
                    source_id=item.source_id,
                    materialized_paths=dict(getattr(lifecycle, "materialized", None) or {}),
                    propagate=dict(getattr(lifecycle, "propagate", None) or {}),
                )
                completed_items.append(completed_item)
                if lifecycle.materialized and lifecycle.materialized.get("book"):
                    page_ids.append(Path(str(lifecycle.materialized["book"])).stem)
                continue

            result = reingest_service.run_reingest(
                reingest_service.ReingestRequest(
                    lane="books",
                    path=plan.request.path,
                    stage=item.start_stage or "acquire",
                    through=item.through_stage or plan.request.through,
                    source_ids=(item.source_id,),
                    dry_run=False,
                    force_deep=force_deep,
                ),
                repo_root=repo_root,
            )
            if result.exit_code != 0:
                detail = next((entry.detail for entry in result.results if entry.status == "failed"), "failed")
                failed += 1
                failed_items.append(f"{item.title}: {detail}")
                continue
            executed += 1
            completed = next((entry for entry in result.results if entry.status == "completed"), None)
            completed_item = CompletedExecutionItem(
                source_key=item.source_key,
                title=item.title,
                source_id=item.source_id,
                materialized_paths=dict(getattr(completed, "materialized_paths", {}) or {}),
                propagate=dict(getattr(completed, "propagate", {}) or {}),
            )
            completed_items.append(completed_item)
            if completed_item.materialized_paths.get("book"):
                page_ids.append(Path(completed_item.materialized_paths["book"]).stem)
        except Exception as exc:
            failed += 1
            failed_items.append(f"{item.title}: {type(exc).__name__}: {exc}")

    refreshed_inventory = build_inventory(
        InventoryRequest(
            lane="books",
            path=plan.request.path,
            today=plan.request.today,
            source_ids=plan.request.source_ids,
            external_ids=plan.request.external_ids,
            selection=plan.request.selection,
            limit=plan.request.limit,
        ),
        repo_root=repo_root,
        use_registry=False,
        phase_callback=phase_callback,
    )
    if phase_callback is not None:
        phase_callback("refreshing source registry")
    refresh_registry_for_inventory(refreshed_inventory, repo_root=repo_root)
    return PlanExecutionResult(
        plan=plan,
        executed_count=executed,
        failed_count=failed,
        page_ids=tuple(dict.fromkeys(page_ids)),
        blocked_samples=blocked_samples,
        failed_items=tuple(failed_items),
        completed_items=tuple(completed_items),
    )


def _actionable_plan_items(plan: PlanResult) -> list[PlanItem]:
    return [
        item
        for item in plan.items
        if item.action.startswith("resume_from_") or item.action.startswith("refresh_stale_from_")
    ]


def _refreshed_inventory_for_plan(
    plan: PlanResult,
    *,
    repo_root: Path,
    phase_callback: Callable[[str], None] | None = None,
) -> InventoryResult:
    return build_inventory(
        InventoryRequest(
            lane=plan.request.lane,
            path=plan.request.path,
            today=plan.request.today,
            source_ids=plan.request.source_ids,
            external_ids=plan.request.external_ids,
            selection=plan.request.selection,
            limit=plan.request.limit,
            lane_options=plan.request.lane_options,
        ),
        repo_root=repo_root,
        use_registry=False,
        phase_callback=phase_callback,
    )


def execute_youtube_plan(
    plan: PlanResult,
    *,
    repo_root: Path,
    default_duration_minutes: float = 30.0,
    phase_callback: Callable[[str], None] | None = None,
    item_callback: Callable[[PlanItem, str, str, int, int], None] | None = None,
) -> PlanExecutionResult:
    if plan.request.lane != "youtube":
        raise ValueError("youtube execution only supports the youtube lane")
    blocked_samples = tuple(plan.blocked_samples)
    executed = 0
    failed = 0
    failed_items: list[str] = []
    completed_items: list[CompletedExecutionItem] = []
    page_ids: list[str] = []
    inventory_by_key = {item.source_key: item for item in plan.inventory.items}
    actionable = _actionable_plan_items(plan)
    if phase_callback is not None and actionable:
        phase_callback(f"executing {len(actionable)} planned actions")

    for index, item in enumerate(actionable, start=1):
        inventory_item = inventory_by_key.get(item.source_key)
        if inventory_item is None or inventory_item.source_id is None:
            detail = "missing source_id alias for execution"
            failed += 1
            failed_items.append(f"{item.title}: {detail}")
            if item_callback is not None:
                item_callback(item, "failed", detail, index, len(actionable))
            continue
        try:
            if item.start_stage == "acquire":
                lifecycle = youtube_enrich.run_youtube_record_lifecycle(
                    inventory_item.payload,
                    repo_root=repo_root,
                    default_duration_minutes=float(default_duration_minutes),
                    today=plan.request.today or _utc_now_string()[:10],
                )
                if lifecycle is None:
                    raise ValueError("youtube lifecycle returned no result")
                executed += 1
                completed_item = CompletedExecutionItem(
                    source_key=item.source_key,
                    title=item.title,
                    source_id=item.source_id,
                    materialized_paths=dict(getattr(lifecycle, "materialized", None) or {}),
                    propagate=dict(getattr(lifecycle, "propagate", None) or {}),
                )
                completed_items.append(completed_item)
                if completed_item.materialized_paths.get("video"):
                    page_ids.append(Path(completed_item.materialized_paths["video"]).stem)
                if item_callback is not None:
                    item_callback(item, "completed", "ok", index, len(actionable))
                continue

            result = reingest_service.run_reingest(
                reingest_service.ReingestRequest(
                    lane="youtube",
                    path=plan.request.path,
                    stage=item.start_stage or "acquire",
                    through=item.through_stage or plan.request.through,
                    today=plan.request.today,
                    source_ids=(item.source_id,),
                    dry_run=False,
                    youtube_default_duration_minutes=float(default_duration_minutes),
                ),
                repo_root=repo_root,
            )
            if result.exit_code != 0:
                detail = next((entry.detail for entry in result.results if entry.status == "failed"), "failed")
                failed += 1
                failed_items.append(f"{item.title}: {detail}")
                if item_callback is not None:
                    item_callback(item, "failed", detail, index, len(actionable))
                continue
            executed += 1
            completed = next((entry for entry in result.results if entry.status == "completed"), None)
            completed_item = CompletedExecutionItem(
                source_key=item.source_key,
                title=item.title,
                source_id=item.source_id,
                materialized_paths=dict(getattr(completed, "materialized_paths", {}) or {}),
                propagate=dict(getattr(completed, "propagate", {}) or {}),
            )
            completed_items.append(completed_item)
            if completed_item.materialized_paths.get("video"):
                page_ids.append(Path(completed_item.materialized_paths["video"]).stem)
            if item_callback is not None:
                item_callback(item, "completed", "ok", index, len(actionable))
        except NoCaptionsAvailable as exc:
            detail = str(exc)
            failed += 1
            failed_items.append(f"{item.title}: {detail}")
            if item_callback is not None:
                item_callback(item, "failed", detail, index, len(actionable))
        except Exception as exc:
            detail = f"{type(exc).__name__}: {exc}"
            failed += 1
            failed_items.append(f"{item.title}: {detail}")
            if item_callback is not None:
                item_callback(item, "failed", detail, index, len(actionable))

    refreshed_inventory = _refreshed_inventory_for_plan(plan, repo_root=repo_root, phase_callback=phase_callback)
    if phase_callback is not None:
        phase_callback("refreshing source registry")
    refresh_registry_for_inventory(refreshed_inventory, repo_root=repo_root)
    return PlanExecutionResult(
        plan=plan,
        executed_count=executed,
        failed_count=failed,
        page_ids=tuple(dict.fromkeys(page_ids)),
        blocked_samples=blocked_samples,
        failed_items=tuple(failed_items),
        completed_items=tuple(completed_items),
    )


def execute_substack_plan(
    plan: PlanResult,
    *,
    repo_root: Path,
    client: Any | None = None,
    saved_urls: set[str] | None = None,
    phase_callback: Callable[[str], None] | None = None,
    item_callback: Callable[[PlanItem, str, str, int, int], None] | None = None,
) -> tuple[PlanExecutionResult, tuple[str, ...]]:
    if plan.request.lane != "substack":
        raise ValueError("substack execution only supports the substack lane")
    blocked_samples = tuple(plan.blocked_samples)
    executed = 0
    failed = 0
    paywalled_entries: list[str] = []
    failed_items: list[str] = []
    completed_items: list[CompletedExecutionItem] = []
    page_ids: list[str] = []
    inventory_by_key = {item.source_key: item for item in plan.inventory.items}
    actionable = _actionable_plan_items(plan)
    shared_client = client or substack_auth.build_client()
    shared_saved_urls = saved_urls if saved_urls is not None else set()
    if phase_callback is not None and actionable:
        phase_callback(f"executing {len(actionable)} planned actions")

    for index, item in enumerate(actionable, start=1):
        inventory_item = inventory_by_key.get(item.source_key)
        if inventory_item is None or inventory_item.source_id is None:
            detail = "missing source_id alias for execution"
            failed += 1
            failed_items.append(f"{item.title}: {detail}")
            if item_callback is not None:
                item_callback(item, "failed", detail, index, len(actionable))
            continue
        try:
            if item.start_stage == "acquire":
                lifecycle = substack_enrich.run_substack_record_lifecycle(
                    inventory_item.payload,
                    client=shared_client,
                    repo_root=repo_root,
                    today=plan.request.today or _utc_now_string()[:10],
                    saved_urls=shared_saved_urls,
                )
                if lifecycle is None:
                    raise ValueError("substack lifecycle returned no result")
                executed += 1
                completed_item = CompletedExecutionItem(
                    source_key=item.source_key,
                    title=item.title,
                    source_id=item.source_id,
                    materialized_paths=dict(lifecycle.materialized or {}),
                    propagate=dict(lifecycle.propagate or {}),
                )
                completed_items.append(completed_item)
                article_path = completed_item.materialized_paths.get("article")
                if article_path:
                    page_ids.append(Path(article_path).stem)
                if item_callback is not None:
                    item_callback(item, "completed", "ok", index, len(actionable))
                continue

            result = reingest_service.run_reingest(
                reingest_service.ReingestRequest(
                    lane="substack",
                    path=plan.request.path,
                    stage=item.start_stage or "acquire",
                    through=item.through_stage or plan.request.through,
                    today=plan.request.today,
                    source_ids=(item.source_id,),
                    dry_run=False,
                ),
                repo_root=repo_root,
            )
            if result.exit_code != 0:
                detail = next((entry.detail for entry in result.results if entry.status == "failed"), "failed")
                failed += 1
                failed_items.append(f"{item.title}: {detail}")
                if item_callback is not None:
                    item_callback(item, "failed", detail, index, len(actionable))
                continue
            executed += 1
            completed = next((entry for entry in result.results if entry.status == "completed"), None)
            completed_item = CompletedExecutionItem(
                source_key=item.source_key,
                title=item.title,
                source_id=item.source_id,
                materialized_paths=dict(getattr(completed, "materialized_paths", {}) or {}),
                propagate=dict(getattr(completed, "propagate", {}) or {}),
            )
            completed_items.append(completed_item)
            article_path = completed_item.materialized_paths.get("article")
            if article_path:
                page_ids.append(Path(article_path).stem)
            if item_callback is not None:
                item_callback(item, "completed", "ok", index, len(actionable))
        except substack_enrich.Paywalled as exc:
            paywalled_entries.append(
                f"- {getattr(inventory_item.payload, 'id', item.source_id)} — {item.title} — {getattr(inventory_item.payload, 'url', str(exc))}\n"
            )
            if item_callback is not None:
                item_callback(item, "paywalled", str(exc), index, len(actionable))
        except Exception as exc:
            detail = f"{type(exc).__name__}: {exc}"
            failed += 1
            failed_items.append(f"{item.title}: {detail}")
            if item_callback is not None:
                item_callback(item, "failed", detail, index, len(actionable))

    refreshed_inventory = _refreshed_inventory_for_plan(plan, repo_root=repo_root, phase_callback=phase_callback)
    if phase_callback is not None:
        phase_callback("refreshing source registry")
    refresh_registry_for_inventory(refreshed_inventory, repo_root=repo_root)
    return (
        PlanExecutionResult(
            plan=plan,
            executed_count=executed,
            failed_count=failed,
            page_ids=tuple(dict.fromkeys(page_ids)),
            blocked_samples=blocked_samples,
            failed_items=tuple(failed_items),
            completed_items=tuple(completed_items),
        ),
        tuple(paywalled_entries),
    )
