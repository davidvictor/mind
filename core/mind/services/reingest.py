from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
from typing import Any, Callable, Iterable, Sequence

from mind.services.content_policy import (
    compatibility_category,
    normalize_book_classification,
    normalize_youtube_classification,
    should_materialize,
)
from mind.services.ingest_contract import (
    LifecycleHandlers,
    NormalizedSource,
    make_enrichment_envelope,
    normalize_lifecycle_stage,
    run_ingestion_window,
)
from mind.services.llm_cache import LLMCacheIdentity, load_llm_cache, write_llm_cache
from mind.services.llm_service import get_llm_service
from mind.services.quality_receipts import write_quality_receipt
from scripts.atoms.pass_d import PASS_D_PROMPT_VERSION, PASS_D_TASK_CLASS, _parse_pass_d_result, pass_d_cache_identities, pass_d_cache_path
from scripts.articles import enrich as articles_enrich
from scripts.articles.fetch import (
    ArticleFetchFailure,
    ArticleFetchResult,
    fetch_article,
    is_supported_article_url,
)
from scripts.articles.parse import ArticleDropEntry
from scripts.articles.pipeline import iter_drop_entries
from scripts.articles.write_pages import slugify_url as article_slugify_url
from scripts.books import enrich as books_enrich
from scripts.books.parse import BookRecord
from scripts.common.drop_queue import filter_article_links_for_queue
from scripts.common.vault import Vault, raw_path
from scripts.substack import enrich as substack_enrich
from scripts.substack.parse import SubstackRecord
from scripts.youtube import enrich as youtube_enrich
from scripts.youtube import filter as youtube_filter
from scripts.youtube.parse import YouTubeRecord
from scripts.youtube.write_pages import channel_page_path as youtube_channel_page_path
from scripts.youtube.write_pages import video_page_path as youtube_video_page_path


LANES: tuple[str, ...] = ("youtube", "books", "articles", "substack")
PERSONALIZATION_REPAIR_LANES: tuple[str, ...] = ("youtube", "books")
REINGEST_STAGE_ORDER: tuple[str, ...] = ("acquire", "pass_a", "pass_b", "pass_c", "pass_d", "materialize", "propagate")
REINGEST_STAGE_ALIASES: dict[str, str] = {
    "summary": "pass_a",
    "personalization": "pass_b",
    "stance": "pass_c",
}
_DOWNSTREAM_LLM_STAGES: tuple[str, ...] = ("pass_a", "pass_b", "pass_c", "pass_d")


@dataclass(frozen=True)
class ReingestRequest:
    lane: str
    path: Path | None = None
    stage: str = "acquire"
    through: str = "propagate"
    today: str | None = None
    limit: int | None = None
    source_ids: tuple[str, ...] = ()
    dry_run: bool = True
    force_deep: bool = False
    youtube_default_duration_minutes: float | None = None


@dataclass(frozen=True)
class ReingestItemPlan:
    source_id: str
    label: str
    reusable_acquisition: bool
    reusable_downstream_stages: tuple[str, ...]
    forced_refresh_stages: tuple[str, ...]
    blocked_reasons: tuple[str, ...]
    projected_rewrites: int
    excluded_reason: str | None = None


@dataclass(frozen=True)
class ReingestPlan:
    lane: str
    stage: str
    through: str
    source_label: str
    selected_items: tuple[ReingestItemPlan, ...]

    @property
    def selected_count(self) -> int:
        return len(self.selected_items)

    @property
    def reusable_acquisition_count(self) -> int:
        return sum(1 for item in self.selected_items if item.reusable_acquisition)

    @property
    def reusable_downstream_count(self) -> int:
        return sum(1 for item in self.selected_items if item.reusable_downstream_stages)

    @property
    def forced_refresh_count(self) -> int:
        return sum(1 for item in self.selected_items if item.forced_refresh_stages)

    @property
    def blocked_count(self) -> int:
        return sum(1 for item in self.selected_items if item.blocked_reasons)

    @property
    def projected_rewrites(self) -> int:
        return sum(item.projected_rewrites for item in self.selected_items)


@dataclass(frozen=True)
class ReingestItemResult:
    source_id: str
    status: str
    detail: str
    materialized_paths: dict[str, str] | None = None
    propagate: dict[str, Any] | None = None


@dataclass(frozen=True)
class CacheProbe:
    reusable: bool
    stale: bool = False


@dataclass(frozen=True)
class ReingestRunResult:
    plan: ReingestPlan
    applied: bool
    results: tuple[ReingestItemResult, ...] = ()

    @property
    def exit_code(self) -> int:
        if any(result.status == "failed" for result in self.results):
            return 1
        return 0


ReingestItemCallback = Callable[[ReingestItemPlan, ReingestItemResult, int, int], None]


@dataclass(frozen=True)
class ArticleRepairItem:
    source_id: str
    label: str
    url: str
    action: str
    start_stage: str
    detail: str
    missing_stages: tuple[str, ...] = ()
    stale_stages: tuple[str, ...] = ()
    blocked_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class ArticleRepairPlan:
    source_label: str
    items: tuple[ArticleRepairItem, ...]

    @property
    def ready_count(self) -> int:
        return sum(1 for item in self.items if item.action == "ready")

    @property
    def reacquire_count(self) -> int:
        return sum(1 for item in self.items if item.action == "refresh_acquisition")

    @property
    def recompute_count(self) -> int:
        return sum(1 for item in self.items if item.action == "recompute_downstream")

    @property
    def blocked_count(self) -> int:
        return sum(1 for item in self.items if item.action == "blocked")


@dataclass(frozen=True)
class ArticleRepairResult:
    plan: ArticleRepairPlan
    applied: bool
    results: tuple[ReingestItemResult, ...] = ()

    @property
    def exit_code(self) -> int:
        if any(result.status == "failed" for result in self.results):
            return 1
        if self.plan.blocked_count > 0:
            return 1
        return 0


@dataclass(frozen=True)
class PersonalizationLinkRepairItem:
    source_id: str
    label: str
    reusable_stages: tuple[str, ...]
    blocked_reasons: tuple[str, ...]
    projected_rewrites: int
    excluded_reason: str | None = None


@dataclass(frozen=True)
class PersonalizationLinkRepairReport:
    lane: str
    source_label: str
    applied: bool
    items: tuple[PersonalizationLinkRepairItem, ...]
    results: tuple[ReingestItemResult, ...] = ()

    @property
    def selected_count(self) -> int:
        return len(self.items)

    @property
    def ready_count(self) -> int:
        return sum(1 for item in self.items if item.excluded_reason is None and not item.blocked_reasons)

    @property
    def blocked_count(self) -> int:
        return sum(1 for item in self.items if item.blocked_reasons)

    @property
    def excluded_count(self) -> int:
        return sum(1 for item in self.items if item.excluded_reason is not None)

    @property
    def projected_rewrites(self) -> int:
        return sum(item.projected_rewrites for item in self.items)

    def render(self) -> str:
        lines = [
            f"repair personalization-links[{self.lane}]: mode={'apply' if self.applied else 'dry-run'} source={self.source_label}",
            (
                f"selected={self.selected_count} ready={self.ready_count} "
                f"blocked={self.blocked_count} excluded={self.excluded_count} "
                f"projected_rewrites={self.projected_rewrites}"
            ),
        ]
        for item in self.items[:12]:
            parts = [f"- {item.source_id}"]
            if item.reusable_stages:
                parts.append("reuse=" + ",".join(item.reusable_stages))
            if item.excluded_reason is not None:
                parts.append("excluded=" + item.excluded_reason)
            if item.blocked_reasons:
                parts.append("blocked=" + "; ".join(item.blocked_reasons))
            if item.projected_rewrites:
                parts.append(f"rewrites={item.projected_rewrites}")
            lines.append(" ".join(parts))
        if self.applied and self.results:
            completed = sum(1 for entry in self.results if entry.status == "completed")
            failed = sum(1 for entry in self.results if entry.status == "failed")
            lines.append(f"applied: completed={completed} failed={failed}")
        return "\n".join(lines)


def normalize_reingest_stage(stage: str) -> str:
    normalized = REINGEST_STAGE_ALIASES.get(stage, stage)
    if normalized not in REINGEST_STAGE_ORDER:
        raise ValueError(
            f"unsupported reingest stage {stage!r}; expected one of {', '.join(REINGEST_STAGE_ORDER)}"
        )
    return normalized


def run_reingest(
    request: ReingestRequest,
    *,
    repo_root: Path | None = None,
    item_callback: ReingestItemCallback | None = None,
) -> ReingestRunResult:
    repo = repo_root or Vault.load(Path.cwd()).root
    lane = _normalize_lane(request.lane)
    stage = normalize_reingest_stage(request.stage)
    through = normalize_reingest_stage(request.through)
    if REINGEST_STAGE_ORDER.index(stage) > REINGEST_STAGE_ORDER.index(through):
        raise ValueError(f"invalid reingest window: stage={stage!r} is after through={through!r}")

    normalized = ReingestRequest(
        lane=lane,
        path=request.path.resolve() if request.path is not None else None,
        stage=stage,
        through=through,
        today=request.today,
        limit=request.limit,
        source_ids=tuple(request.source_ids),
        dry_run=request.dry_run,
        force_deep=request.force_deep,
    )
    items = _inventory_items(normalized, repo)
    plan = _build_plan(normalized, repo, items)
    if normalized.dry_run:
        return ReingestRunResult(plan=plan, applied=False)

    results: list[ReingestItemResult] = []
    total = len(items)
    for index, item in enumerate(items, start=1):
        plan_item = next((candidate for candidate in plan.selected_items if candidate.source_id == item.source_id), None)
        if plan_item is None:
            continue
        if plan_item.excluded_reason:
            result = ReingestItemResult(
                source_id=item.source_id,
                status="completed",
                detail=plan_item.excluded_reason,
            )
            results.append(result)
            if item_callback is not None:
                item_callback(plan_item, result, index, total)
            continue
        if plan_item.blocked_reasons:
            result = ReingestItemResult(
                source_id=item.source_id,
                status="failed",
                detail="; ".join(plan_item.blocked_reasons),
            )
            results.append(result)
            if item_callback is not None:
                item_callback(plan_item, result, index, total)
            continue
        try:
            lifecycle = _execute_item(normalized, repo, item)
        except Exception as exc:
            result = ReingestItemResult(
                source_id=item.source_id,
                status="failed",
                detail=f"{type(exc).__name__}: {exc}",
            )
            results.append(result)
            if item_callback is not None:
                item_callback(plan_item, result, index, total)
            continue
        result = ReingestItemResult(
            source_id=item.source_id,
            status="completed",
            detail="ok",
            materialized_paths=dict(getattr(lifecycle, "materialized", None) or {}),
            propagate=dict(getattr(lifecycle, "propagate", None) or {}),
        )
        results.append(result)
        if item_callback is not None:
            item_callback(plan_item, result, index, total)

    return ReingestRunResult(plan=plan, applied=True, results=tuple(results))


def render_reingest_report(result: ReingestRunResult) -> str:
    plan = result.plan
    lines = [
        (
            f"reingest[{plan.lane}]: mode={'dry-run' if not result.applied else 'apply'} "
            f"stage={plan.stage} through={plan.through} source={plan.source_label}"
        ),
        (
            f"selected={plan.selected_count} reusable_acquisition={plan.reusable_acquisition_count} "
            f"reusable_downstream={plan.reusable_downstream_count} forced_refresh={plan.forced_refresh_count} "
            f"blocked={plan.blocked_count} projected_rewrites={plan.projected_rewrites}"
        ),
    ]
    for item in plan.selected_items[:10]:
        parts = [f"- {item.source_id}"]
        if item.reusable_acquisition:
            parts.append("reuse=acquire")
        if item.reusable_downstream_stages:
            parts.append("reuse_stages=" + ",".join(item.reusable_downstream_stages))
        if item.forced_refresh_stages:
            parts.append("refresh=" + ",".join(item.forced_refresh_stages))
        if item.excluded_reason:
            parts.append("excluded=" + item.excluded_reason)
        if item.blocked_reasons:
            parts.append("blocked=" + "; ".join(item.blocked_reasons))
        if item.projected_rewrites:
            parts.append(f"rewrites={item.projected_rewrites}")
        lines.append(" ".join(parts))
    if result.applied and result.results:
        completed = sum(1 for entry in result.results if entry.status == "completed")
        failed = sum(1 for entry in result.results if entry.status == "failed")
        lines.append(f"applied: completed={completed} failed={failed}")
    return "\n".join(lines)


def run_article_repair(
    *,
    repo_root: Path,
    path: Path | None = None,
    today: str | None = None,
    limit: int | None = None,
    source_ids: Sequence[str] = (),
    apply: bool = False,
) -> ArticleRepairResult:
    request = ReingestRequest(
        lane="articles",
        path=path.resolve() if path is not None else None,
        today=today,
        limit=limit,
        source_ids=tuple(source_ids),
        dry_run=True,
        stage="pass_d",
        through="materialize",
    )
    items = list(_filter_items(_inventory_article_items(request, repo_root), request))
    if request.limit is not None:
        items = items[: request.limit]
    plan = _build_article_repair_plan(items=items, repo_root=repo_root, source_label=(items[0].source_label if items else (request.path.name if request.path is not None else "all-drop-files")))
    if not apply:
        return ArticleRepairResult(plan=plan, applied=False)

    results: list[ReingestItemResult] = []
    for item in plan.items:
        if item.action == "ready":
            results.append(ReingestItemResult(source_id=item.source_id, status="completed", detail="already ready"))
            continue
        if item.action == "blocked":
            results.append(
                ReingestItemResult(
                    source_id=item.source_id,
                    status="failed",
                    detail=item.detail,
                )
            )
            continue
        if item.action == "recompute_downstream" and item.start_stage == "pass_d":
            adopted = _adopt_legacy_pass_d_cache(
                repo_root=repo_root,
                source_kind="article",
                source_id=item.source_id,
            )
            if adopted:
                results.append(
                    ReingestItemResult(
                        source_id=item.source_id,
                        status="completed",
                        detail="adopted legacy pass_d cache identity",
                    )
                )
                continue
        rerun = run_reingest(
            ReingestRequest(
                lane="articles",
                path=path.resolve() if path is not None else None,
                today=today,
                limit=None,
                source_ids=(item.source_id,),
                dry_run=False,
                stage=item.start_stage,
                through="propagate",
            ),
            repo_root=repo_root,
        )
        if rerun.exit_code != 0:
            detail = next((entry.detail for entry in rerun.results if entry.status == "failed"), render_reingest_report(rerun))
            results.append(ReingestItemResult(source_id=item.source_id, status="failed", detail=detail))
            continue
        results.append(ReingestItemResult(source_id=item.source_id, status="completed", detail=f"repaired via {item.start_stage}"))
    return ArticleRepairResult(plan=plan, applied=True, results=tuple(results))


def render_article_repair_report(result: ArticleRepairResult) -> str:
    plan = result.plan
    lines = [
        f"article-repair: mode={'apply' if result.applied else 'dry-run'} source={plan.source_label}",
        (
            f"ready={plan.ready_count} refresh_acquisition={plan.reacquire_count} "
            f"recompute_downstream={plan.recompute_count} blocked={plan.blocked_count}"
        ),
    ]
    for item in plan.items[:12]:
        parts = [f"- {item.source_id}", item.action]
        if item.start_stage:
            parts.append(f"start={item.start_stage}")
        if item.missing_stages:
            parts.append("missing=" + ",".join(item.missing_stages))
        if item.stale_stages:
            parts.append("stale=" + ",".join(item.stale_stages))
        if item.blocked_reasons:
            parts.append("blocked=" + "; ".join(item.blocked_reasons))
        parts.append(item.detail)
        lines.append(" ".join(parts))
    if result.applied and result.results:
        completed = sum(1 for entry in result.results if entry.status == "completed")
        failed = sum(1 for entry in result.results if entry.status == "failed")
        lines.append(f"applied: completed={completed} failed={failed}")
    return "\n".join(lines)


def run_personalization_link_repair(
    *,
    repo_root: Path,
    lane: str,
    path: Path | None = None,
    today: str | None = None,
    limit: int | None = None,
    source_ids: Sequence[str] = (),
    apply: bool = False,
) -> PersonalizationLinkRepairReport:
    normalized_lane = _normalize_personalization_repair_lane(lane)
    request = ReingestRequest(
        lane=normalized_lane,
        path=path.resolve() if path is not None else None,
        today=today,
        limit=limit,
        source_ids=tuple(source_ids),
        dry_run=True,
        stage="materialize",
        through="materialize",
    )
    items = _inventory_items(request, repo_root)
    source_label = items[0].source_label if items else (request.path.name if request.path is not None else "latest-export")
    planned_items = tuple(
        _build_personalization_link_repair_item(repo_root=repo_root, lane=normalized_lane, item=item)
        for item in items
    )
    if not apply:
        return PersonalizationLinkRepairReport(
            lane=normalized_lane,
            source_label=source_label,
            applied=False,
            items=planned_items,
        )

    results: list[ReingestItemResult] = []
    for item, planned in zip(items, planned_items):
        if planned.excluded_reason is not None:
            results.append(
                ReingestItemResult(
                    source_id=item.source_id,
                    status="completed",
                    detail=planned.excluded_reason,
                )
            )
            continue
        if planned.blocked_reasons:
            results.append(
                ReingestItemResult(
                    source_id=item.source_id,
                    status="failed",
                    detail="; ".join(planned.blocked_reasons),
                )
            )
            continue
        try:
            lifecycle = _execute_personalization_link_repair_item(
                lane=normalized_lane,
                repo_root=repo_root,
                item=item,
                today=today,
            )
        except Exception as exc:
            results.append(
                ReingestItemResult(
                    source_id=item.source_id,
                    status="failed",
                    detail=f"{type(exc).__name__}: {exc}",
                )
            )
            continue
        results.append(
            ReingestItemResult(
                source_id=item.source_id,
                status="completed",
                detail="refreshed pass_b and rematerialized",
                materialized_paths=dict(getattr(lifecycle, "materialized", None) or {}),
                propagate=dict(getattr(lifecycle, "propagate", None) or {}),
            )
        )

    return PersonalizationLinkRepairReport(
        lane=normalized_lane,
        source_label=source_label,
        applied=True,
        items=planned_items,
        results=tuple(results),
    )


def _adopt_legacy_pass_d_cache(
    *,
    repo_root: Path,
    source_kind: str,
    source_id: str,
) -> bool:
    path = pass_d_cache_path(repo_root=repo_root, source_kind=source_kind, source_id=source_id)
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    if not isinstance(payload, dict):
        return False
    data = payload.get("data")
    if not isinstance(data, dict):
        return False
    try:
        _parse_pass_d_result(data)
    except Exception:
        return False
    current = _pass_d_identities()[0]
    identity = payload.get("_llm")
    if isinstance(identity, dict):
        comparable = dict(identity)
        comparable["request_fingerprint"] = current.request_fingerprint
        comparable["timeout_seconds"] = current.timeout_seconds
        comparable["temperature"] = current.temperature
        comparable["max_tokens"] = current.max_tokens
        comparable["reasoning_effort"] = current.reasoning_effort
        if comparable == current.to_dict():
            write_llm_cache(path, identity=current, data=data)
            return True
        return False
    write_llm_cache(path, identity=_pass_d_identities()[0], data=data)
    return True


@dataclass(frozen=True)
class _LaneItem:
    lane: str
    source_id: str
    label: str
    payload: Any
    source_label: str


def _normalize_lane(lane: str) -> str:
    if lane not in LANES:
        raise ValueError(f"unsupported reingest lane {lane!r}; expected one of {', '.join(LANES)}")
    return lane


def _normalize_personalization_repair_lane(lane: str) -> str:
    if lane not in PERSONALIZATION_REPAIR_LANES:
        raise ValueError(
            "unsupported personalization-links lane "
            f"{lane!r}; expected one of {', '.join(PERSONALIZATION_REPAIR_LANES)}"
        )
    return lane


def _inventory_items(request: ReingestRequest, repo_root: Path) -> list[_LaneItem]:
    items = {
        "youtube": _inventory_youtube_items,
        "books": _inventory_book_items,
        "articles": _inventory_article_items,
        "substack": _inventory_substack_items,
    }[request.lane](request, repo_root)
    selected = list(_filter_items(items, request))
    deduped: list[_LaneItem] = []
    seen_source_ids: set[str] = set()
    for item in selected:
        if item.source_id in seen_source_ids:
            continue
        seen_source_ids.add(item.source_id)
        deduped.append(item)
    if request.limit is not None:
        deduped = deduped[: request.limit]
    return deduped


def _filter_items(items: Sequence[_LaneItem], request: ReingestRequest) -> Iterable[_LaneItem]:
    if not request.source_ids:
        yield from items
        return
    allowed = set(request.source_ids)
    for item in items:
        aliases = {item.source_id}
        payload = item.payload
        if item.lane == "youtube":
            aliases.add(payload.video_id)
        elif item.lane == "books":
            aliases.add(payload.title)
            if payload.asin:
                aliases.add(payload.asin)
        elif item.lane == "articles":
            aliases.add(payload.url)
        elif item.lane == "substack":
            aliases.add(payload.id)
            aliases.add(payload.url)
        if aliases & allowed:
            yield item


def _inventory_youtube_items(request: ReingestRequest, repo_root: Path) -> list[_LaneItem]:
    vault = Vault.load(repo_root)
    path = request.path or _latest_existing_path(vault.raw / "exports", ("youtube-recent-*.json",))
    if path is None:
        return []
    records = list(__import__("mind.commands.ingest", fromlist=["_iter_youtube_records"])._iter_youtube_records(path))
    return [
        _LaneItem(
            lane="youtube",
            source_id=f"youtube-{record.video_id}",
            label=record.title,
            payload=record,
            source_label=path.name,
        )
        for record in records
    ]


def _inventory_book_items(request: ReingestRequest, repo_root: Path) -> list[_LaneItem]:
    vault = Vault.load(repo_root)
    path = request.path or _latest_existing_path(
        vault.raw / "exports",
        ("audible-library-*.json", "goodreads-*.csv", "audible-*.csv"),
    )
    if path is None:
        return []
    books = list(__import__("mind.commands.ingest", fromlist=["_iter_books_from_path"])._iter_books_from_path(path))
    return [
        _LaneItem(
            lane="books",
            source_id=_book_source_id(book),
            label=book.title,
            payload=book,
            source_label=path.name,
        )
        for book in books
    ]


def _inventory_article_items(request: ReingestRequest, repo_root: Path) -> list[_LaneItem]:
    entries = list(iter_drop_entries(repo_root=repo_root, path=request.path, today=request.today))
    source_label = request.path.name if request.path is not None else (request.today or "all-drop-files")
    return [
        _LaneItem(
            lane="articles",
            source_id=f"article-{article_slugify_url(entry.url, entry.discovered_at)}",
            label=entry.anchor_text or entry.url,
            payload=entry,
            source_label=source_label,
        )
        for _, entry in entries
    ]


def _inventory_substack_items(request: ReingestRequest, repo_root: Path) -> list[_LaneItem]:
    vault = Vault.load(repo_root)
    path = request.path or _latest_existing_path(vault.raw / "exports", ("substack-saved-*.json",))
    if path is None:
        return []
    records = list(
        __import__("mind.commands.ingest", fromlist=["substack_parse"]).substack_parse.parse_export(
            json.loads(path.read_text(encoding="utf-8"))
        )
    )
    return [
        _LaneItem(
            lane="substack",
            source_id=f"substack-{record.id}",
            label=record.title,
            payload=record,
            source_label=path.name,
        )
        for record in records
    ]


def _latest_existing_path(root: Path, patterns: Sequence[str]) -> Path | None:
    if not root.exists():
        return None
    candidates: list[Path] = []
    for pattern in patterns:
        candidates.extend(sorted(root.glob(pattern)))
    return sorted(candidates)[-1] if candidates else None


def _book_source_id(book: BookRecord) -> str:
    author_slug = books_enrich.slugify(book.author[0]) if book.author else "unknown"
    title_slug = books_enrich.slugify(book.title)
    return f"book-{author_slug}-{title_slug}"


def _build_plan(request: ReingestRequest, repo_root: Path, items: Sequence[_LaneItem]) -> ReingestPlan:
    planners = {
        "youtube": _plan_youtube_item,
        "books": _plan_book_item,
        "articles": _plan_article_item,
        "substack": _plan_substack_item,
    }
    selected_items = tuple(planners[request.lane](repo_root, request, item) for item in items)
    source_label = items[0].source_label if items else (request.path.name if request.path is not None else "none")
    return ReingestPlan(
        lane=request.lane,
        stage=request.stage,
        through=request.through,
        source_label=source_label,
        selected_items=selected_items,
    )


def _build_personalization_link_repair_item(
    *,
    repo_root: Path,
    lane: str,
    item: _LaneItem,
) -> PersonalizationLinkRepairItem:
    if lane == "youtube":
        return _plan_youtube_personalization_link_repair_item(repo_root, item)
    if lane == "books":
        return _plan_book_personalization_link_repair_item(repo_root, item)
    raise ValueError(f"unsupported personalization repair lane {lane!r}")


def _plan_youtube_personalization_link_repair_item(
    repo_root: Path,
    item: _LaneItem,
) -> PersonalizationLinkRepairItem:
    record: YouTubeRecord = item.payload
    if youtube_filter.should_skip_record(record):
        return PersonalizationLinkRepairItem(
            source_id=item.source_id,
            label=item.label,
            reusable_stages=(),
            blocked_reasons=(),
            projected_rewrites=0,
            excluded_reason="excluded by cheap YouTube filter",
        )
    classification = _load_youtube_classification_cache(repo_root, record) or {}
    if classification and not should_materialize(classification):
        reason = classification.get("category") or classification.get("retention") or "exclude"
        return PersonalizationLinkRepairItem(
            source_id=item.source_id,
            label=item.label,
            reusable_stages=(),
            blocked_reasons=(),
            projected_rewrites=0,
            excluded_reason=f"excluded by content policy ({reason})",
        )
    acquisition_probe = _combine_probes(
        _probe_youtube_classification_cache(repo_root, record),
        _probe_raw_file(youtube_enrich.raw_transcript_path(repo_root, record.video_id)),
    )
    pass_a_probe = _probe_llm_cache(
        youtube_enrich.transcript_path(repo_root, record.video_id),
        _summary_identities(youtube_enrich.SUMMARIZE_TRANSCRIPT_PROMPT_VERSION),
    )
    pass_c_probe = _probe_youtube_pass_c(repo_root, record)
    reusable: list[str] = []
    blocked: list[str] = []
    if acquisition_probe.reusable:
        reusable.append("acquire")
    else:
        blocked.append("missing acquisition cache")
    if pass_a_probe.reusable:
        reusable.append("pass_a")
    else:
        blocked.append("missing or stale pass_a cache")
    if pass_c_probe.reusable:
        reusable.append("pass_c")
    else:
        blocked.append("missing or stale pass_c cache")
    projected_rewrites = _projected_youtube_rewrites(
        repo_root,
        ReingestRequest(lane="youtube", stage="materialize", through="materialize", dry_run=True),
        record,
    ) if not blocked else 0
    return PersonalizationLinkRepairItem(
        source_id=item.source_id,
        label=item.label,
        reusable_stages=tuple(reusable),
        blocked_reasons=tuple(blocked),
        projected_rewrites=projected_rewrites,
    )


def _plan_book_personalization_link_repair_item(
    repo_root: Path,
    item: _LaneItem,
) -> PersonalizationLinkRepairItem:
    book: BookRecord = item.payload
    if str(book.status or "").strip().lower() == "to-read":
        return PersonalizationLinkRepairItem(
            source_id=item.source_id,
            label=item.label,
            reusable_stages=(),
            blocked_reasons=(),
            projected_rewrites=0,
            excluded_reason="book is still to-read",
        )
    classification = books_enrich.effective_book_classification(
        book,
        _load_book_classification_cache(repo_root, book) or {},
        force_deep=False,
    )
    if classification and not should_materialize(classification):
        reason = classification.get("category") or classification.get("retention") or "exclude"
        return PersonalizationLinkRepairItem(
            source_id=item.source_id,
            label=item.label,
            reusable_stages=(),
            blocked_reasons=(),
            projected_rewrites=0,
            excluded_reason=f"excluded by content policy ({reason})",
        )
    source_grounded_probe = _probe_book_source_grounded_cache(repo_root, book)
    acquisition_probe = _combine_probes(
        _probe_book_classification_cache(repo_root, book),
        _choose_probe(source_grounded_probe, _probe_book_deep_research_cache(repo_root, book)),
    )
    pass_a_probe = _probe_book_pass_a_cache(repo_root, book, source_grounded_probe)
    pass_c_probe = _probe_book_pass_c(repo_root, book)
    reusable: list[str] = []
    blocked: list[str] = []
    if acquisition_probe.reusable:
        reusable.append("acquire")
    else:
        blocked.append("missing acquisition cache")
    if pass_a_probe.reusable:
        reusable.append("pass_a")
    else:
        blocked.append("missing or stale pass_a cache")
    if pass_c_probe.reusable:
        reusable.append("pass_c")
    else:
        blocked.append("missing or stale pass_c cache")
    projected_rewrites = _projected_book_rewrites(
        repo_root,
        ReingestRequest(lane="books", stage="materialize", through="materialize", dry_run=True),
        book,
    ) if not blocked else 0
    return PersonalizationLinkRepairItem(
        source_id=item.source_id,
        label=item.label,
        reusable_stages=tuple(reusable),
        blocked_reasons=tuple(blocked),
        projected_rewrites=projected_rewrites,
    )


def _build_article_repair_plan(*, items: Sequence[_LaneItem], repo_root: Path, source_label: str) -> ArticleRepairPlan:
    plans: list[ArticleRepairItem] = []
    for item in items:
        entry: ArticleDropEntry = item.payload
        if not entry.url.strip():
            plans.append(
                ArticleRepairItem(
                    source_id=item.source_id,
                    label=item.label,
                    url=entry.url,
                    action="blocked",
                    start_stage="",
                    detail="missing source URL in drop entry",
                    blocked_reasons=("missing source URL in drop entry",),
                )
            )
            continue
        if not is_supported_article_url(entry.url):
            detail = (
                "non-article URL is intentionally excluded from article repair"
                if entry.source_type == "youtube-description"
                else "unsupported URL is intentionally excluded from article repair"
            )
            plans.append(
                ArticleRepairItem(
                    source_id=item.source_id,
                    label=item.label,
                    url=entry.url,
                    action="ready",
                    start_stage="",
                    detail=detail,
                )
            )
            continue
        fetch_probe = _probe_article_fetch_cache(repo_root, entry)
        probes = {
            "pass_a": _probe_llm_cache(articles_enrich.summary_cache_path(repo_root, entry), _summary_identities("articles.summary.v1")),
            "pass_b": _probe_llm_cache(articles_enrich.applied_cache_path(repo_root, entry), _personalization_identities()),
            "pass_c": _probe_article_pass_c(repo_root, entry),
            "pass_d": _probe_llm_cache(
                pass_d_cache_path(
                    repo_root=repo_root,
                    source_kind="article",
                    source_id=f"article-{article_slugify_url(entry.url, entry.discovered_at)}",
                ),
                _pass_d_identities(),
            ),
        }
        missing_stages = tuple(stage for stage, probe in probes.items() if not probe.reusable and not probe.stale)
        stale_stages = tuple(stage for stage, probe in probes.items() if probe.stale)
        if fetch_probe.reusable and not missing_stages and not stale_stages:
            plans.append(
                ArticleRepairItem(
                    source_id=item.source_id,
                    label=item.label,
                    url=entry.url,
                    action="ready",
                    start_stage="",
                    detail="all required acquisition and downstream caches are reusable",
                )
            )
            continue
        if not fetch_probe.reusable:
            blocked_reasons = ("missing or stale acquisition cache",) if fetch_probe.stale else ("missing acquisition cache",)
            plans.append(
                ArticleRepairItem(
                    source_id=item.source_id,
                    label=item.label,
                    url=entry.url,
                    action="refresh_acquisition",
                    start_stage="acquire",
                    detail="refresh cached fetch artifacts and downstream stages from source URL",
                    missing_stages=missing_stages,
                    stale_stages=stale_stages,
                    blocked_reasons=blocked_reasons,
                )
            )
            continue
        stages_to_refresh = tuple(stage for stage in ("pass_a", "pass_b", "pass_c", "pass_d") if stage in {*missing_stages, *stale_stages})
        start_stage = next((stage for stage in ("pass_a", "pass_b", "pass_c", "pass_d") if stage in stages_to_refresh), "pass_a")
        plans.append(
            ArticleRepairItem(
                source_id=item.source_id,
                label=item.label,
                url=entry.url,
                action="recompute_downstream",
                start_stage=start_stage,
                detail="recompute stale or missing downstream caches from reusable acquisition artifacts",
                missing_stages=missing_stages,
                stale_stages=stale_stages,
                blocked_reasons=tuple(f"missing or stale {stage} cache" for stage in stages_to_refresh),
            )
        )
    return ArticleRepairPlan(source_label=source_label, items=tuple(plans))


def _downstream_seed_requirements(stage: str) -> tuple[str, ...]:
    if stage == "acquire":
        return ()
    if stage == "pass_a":
        return ()
    if stage == "pass_b":
        return ("pass_a",)
    if stage == "pass_c":
        return ("pass_a", "pass_b")
    if stage == "pass_d":
        return ("pass_a", "pass_b", "pass_c")
    if stage == "materialize":
        return ("pass_a", "pass_b", "pass_c")
    if stage == "propagate":
        return ("pass_a", "pass_b", "pass_c", "pass_d")
    raise ValueError(stage)


def _selected_refresh_stages(request: ReingestRequest) -> tuple[str, ...]:
    start_index = REINGEST_STAGE_ORDER.index(request.stage)
    through_index = REINGEST_STAGE_ORDER.index(request.through)
    return tuple(stage for stage in REINGEST_STAGE_ORDER[start_index : through_index + 1] if stage in _DOWNSTREAM_LLM_STAGES)


def _plan_youtube_item(repo_root: Path, request: ReingestRequest, item: _LaneItem) -> ReingestItemPlan:
    record: YouTubeRecord = item.payload
    duration_override = request.youtube_default_duration_minutes
    if youtube_filter.should_skip_record(record, duration_minutes_override=duration_override):
        return ReingestItemPlan(
            source_id=item.source_id,
            label=item.label,
            reusable_acquisition=False,
            reusable_downstream_stages=(),
            forced_refresh_stages=(),
            blocked_reasons=(),
            projected_rewrites=0,
            excluded_reason="excluded by cheap YouTube filter",
        )
    classification_probe = _probe_youtube_classification_cache(repo_root, record)
    classification = _load_youtube_classification_cache(repo_root, record) or {}
    if classification and not should_materialize(classification):
        reason = classification.get("category") or classification.get("retention") or "exclude"
        return ReingestItemPlan(
            source_id=item.source_id,
            label=item.label,
            reusable_acquisition=classification_probe.reusable,
            reusable_downstream_stages=(),
            forced_refresh_stages=(),
            blocked_reasons=(),
            projected_rewrites=0,
            excluded_reason=f"excluded by content policy ({reason})",
        )
    transcript_probe = _probe_raw_file(youtube_enrich.raw_transcript_path(repo_root, record.video_id))
    pass_a_probe = _probe_llm_cache(
        youtube_enrich.transcript_path(repo_root, record.video_id),
        _summary_identities(youtube_enrich.SUMMARIZE_TRANSCRIPT_PROMPT_VERSION),
    )
    pass_b_probe = _probe_llm_cache(youtube_enrich.applied_path(repo_root, record.video_id), _personalization_identities())
    pass_c_probe = _probe_youtube_pass_c(repo_root, record)
    pass_d_probe = _probe_llm_cache(
        pass_d_cache_path(repo_root=repo_root, source_kind="youtube", source_id=f"youtube-{record.video_id}"),
        _pass_d_identities(),
    )
    reusable, blocked, forced = _plan_common_prereqs(
        request=request,
        acquisition_probe=_combine_probes(classification_probe, transcript_probe),
        pass_a_probe=pass_a_probe,
        pass_b_probe=pass_b_probe,
        pass_c_probe=pass_c_probe,
        pass_d_probe=pass_d_probe,
    )
    return ReingestItemPlan(
        source_id=item.source_id,
        label=item.label,
        reusable_acquisition=classification_probe.reusable and transcript_probe.reusable,
        reusable_downstream_stages=reusable,
        forced_refresh_stages=forced,
        blocked_reasons=blocked,
        projected_rewrites=_projected_youtube_rewrites(repo_root, request, record),
    )


def _plan_book_item(repo_root: Path, request: ReingestRequest, item: _LaneItem) -> ReingestItemPlan:
    book: BookRecord = item.payload
    if str(book.status or "").strip().lower() == "to-read":
        return ReingestItemPlan(
            source_id=item.source_id,
            label=item.label,
            reusable_acquisition=False,
            reusable_downstream_stages=(),
            forced_refresh_stages=(),
            blocked_reasons=(),
            projected_rewrites=0,
            excluded_reason="book is still to-read",
        )
    classification_probe = _probe_book_classification_cache(repo_root, book)
    classification = books_enrich.effective_book_classification(
        book,
        _load_book_classification_cache(repo_root, book) or {},
        force_deep=request.force_deep,
    )
    if classification and not should_materialize(classification):
        reason = classification.get("category") or classification.get("retention") or "exclude"
        return ReingestItemPlan(
            source_id=item.source_id,
            label=item.label,
            reusable_acquisition=classification_probe.reusable,
            reusable_downstream_stages=(),
            forced_refresh_stages=(),
            blocked_reasons=(),
            projected_rewrites=0,
            excluded_reason=f"excluded by content policy ({reason})",
        )
    source_grounded_probe = _probe_book_source_grounded_cache(repo_root, book)
    deep_research_probe = _probe_book_deep_research_cache(repo_root, book)
    pass_a_probe = _probe_book_pass_a_cache(repo_root, book, source_grounded_probe)
    pass_b_probe = _probe_llm_cache(books_enrich.applied_path(repo_root, book), _personalization_identities())
    pass_c_probe = _probe_book_pass_c(repo_root, book)
    pass_d_probe = _probe_llm_cache(
        pass_d_cache_path(repo_root=repo_root, source_kind="book", source_id=_book_source_id(book)),
        _pass_d_identities(),
    )
    reusable, blocked, forced = _plan_common_prereqs(
        request=request,
        acquisition_probe=_combine_probes(classification_probe, _choose_probe(source_grounded_probe, deep_research_probe)),
        pass_a_probe=pass_a_probe,
        pass_b_probe=pass_b_probe,
        pass_c_probe=pass_c_probe,
        pass_d_probe=pass_d_probe,
    )
    return ReingestItemPlan(
        source_id=item.source_id,
        label=item.label,
        reusable_acquisition=classification_probe.reusable and (source_grounded_probe.reusable or deep_research_probe.reusable),
        reusable_downstream_stages=reusable,
        forced_refresh_stages=forced,
        blocked_reasons=blocked,
        projected_rewrites=_projected_book_rewrites(repo_root, request, book),
    )


def _plan_article_item(repo_root: Path, request: ReingestRequest, item: _LaneItem) -> ReingestItemPlan:
    entry: ArticleDropEntry = item.payload
    if not is_supported_article_url(entry.url):
        excluded_reason = (
            "excluded non-article URL from YouTube description fanout"
            if entry.source_type == "youtube-description"
            else "excluded unsupported URL from article extraction"
        )
        return ReingestItemPlan(
            source_id=item.source_id,
            label=item.label,
            reusable_acquisition=False,
            reusable_downstream_stages=(),
            forced_refresh_stages=(),
            blocked_reasons=(),
            projected_rewrites=0,
            excluded_reason=excluded_reason,
        )
    fetch_probe = _probe_article_fetch_cache(repo_root, entry)
    pass_a_probe = _probe_llm_cache(articles_enrich.summary_cache_path(repo_root, entry), _summary_identities("articles.summary.v1"))
    pass_b_probe = _probe_llm_cache(articles_enrich.applied_cache_path(repo_root, entry), _personalization_identities())
    pass_c_probe = _probe_article_pass_c(repo_root, entry)
    pass_d_probe = _probe_llm_cache(
        pass_d_cache_path(
            repo_root=repo_root,
            source_kind="article",
            source_id=f"article-{article_slugify_url(entry.url, entry.discovered_at)}",
        ),
        _pass_d_identities(),
    )
    reusable, blocked, forced = _plan_common_prereqs(
        request=request,
        acquisition_probe=fetch_probe,
        pass_a_probe=pass_a_probe,
        pass_b_probe=pass_b_probe,
        pass_c_probe=pass_c_probe,
        pass_d_probe=pass_d_probe,
    )
    return ReingestItemPlan(
        source_id=item.source_id,
        label=item.label,
        reusable_acquisition=fetch_probe.reusable,
        reusable_downstream_stages=reusable,
        forced_refresh_stages=forced,
        blocked_reasons=blocked,
        projected_rewrites=_projected_article_rewrites(repo_root, request, entry),
    )


def _plan_substack_item(repo_root: Path, request: ReingestRequest, item: _LaneItem) -> ReingestItemPlan:
    record: SubstackRecord = item.payload
    acquisition_probe = _probe_substack_acquisition_cache(repo_root, record)
    pass_a_probe = _probe_substack_pass_a_cache(repo_root, record)
    pass_b_probe = _probe_llm_cache(substack_enrich.applied_cache_path(repo_root, record.id), _personalization_identities(substack=True))
    pass_c_probe = _probe_llm_cache(substack_enrich.stance.stance_cache_path(repo_root, record.id), _stance_identities())
    pass_d_probe = _probe_llm_cache(
        pass_d_cache_path(repo_root=repo_root, source_kind="substack", source_id=f"substack-{record.id}"),
        _pass_d_identities(),
    )
    reusable, blocked, forced = _plan_common_prereqs(
        request=request,
        acquisition_probe=acquisition_probe,
        pass_a_probe=pass_a_probe,
        pass_b_probe=pass_b_probe,
        pass_c_probe=pass_c_probe,
        pass_d_probe=pass_d_probe,
    )
    return ReingestItemPlan(
        source_id=item.source_id,
        label=item.label,
        reusable_acquisition=acquisition_probe.reusable,
        reusable_downstream_stages=reusable,
        forced_refresh_stages=forced,
        blocked_reasons=blocked,
        projected_rewrites=_projected_substack_rewrites(repo_root, request, record),
    )


def _plan_common_prereqs(
    *,
    request: ReingestRequest,
    acquisition_probe: CacheProbe,
    pass_a_probe: CacheProbe,
    pass_b_probe: CacheProbe,
    pass_c_probe: CacheProbe,
    pass_d_probe: CacheProbe,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    reusable: list[str] = []
    blocked: list[str] = []
    forced: list[str] = []
    required = _downstream_seed_requirements(request.stage)
    if "pass_a" in required:
        if acquisition_probe.reusable:
            reusable.append("acquire")
        elif acquisition_probe.stale:
            forced.append("acquire")
            blocked.append("missing or stale acquisition cache")
        else:
            blocked.append("missing acquisition cache")
        if pass_a_probe.reusable:
            reusable.append("pass_a")
        elif pass_a_probe.stale:
            forced.append("pass_a")
            blocked.append("missing or stale pass_a cache")
        else:
            blocked.append("missing or stale pass_a cache")
    if "pass_b" in required:
        if pass_b_probe.reusable:
            reusable.append("pass_b")
        elif pass_b_probe.stale:
            forced.append("pass_b")
            blocked.append("missing or stale pass_b cache")
        else:
            blocked.append("missing or stale pass_b cache")
    if "pass_c" in required:
        if pass_c_probe.reusable:
            reusable.append("pass_c")
        elif pass_c_probe.stale:
            forced.append("pass_c")
            blocked.append("missing or stale pass_c cache")
        else:
            blocked.append("missing or stale pass_c cache")
    if "pass_d" in required:
        if pass_d_probe.reusable:
            reusable.append("pass_d")
        elif pass_d_probe.stale:
            forced.append("pass_d")
            blocked.append("missing or stale pass_d cache")
        else:
            blocked.append("missing or stale pass_d cache")
    if request.stage in {"pass_a", "acquire"} and acquisition_probe.reusable:
        reusable = ["acquire", *reusable]
    return tuple(dict.fromkeys(reusable)), tuple(dict.fromkeys(blocked)), tuple(dict.fromkeys(forced))


def _summary_identities(prompt_version: str):
    return get_llm_service().cache_identities(task_class="summary", prompt_version=prompt_version)


def _personalization_identities(*, substack: bool = False):
    prompt = substack and substack_enrich.APPLIED_TO_POST_PROMPT_VERSION or youtube_enrich.APPLIED_TO_YOU_PROMPT_VERSION
    return get_llm_service().cache_identities(task_class="personalization", prompt_version=prompt)


def _stance_identities():
    return get_llm_service().cache_identities(task_class="stance", prompt_version=youtube_enrich.UPDATE_AUTHOR_STANCE_PROMPT_VERSION)


def _pass_d_identities():
    return pass_d_cache_identities(get_llm_service())


def _llm_cache_exists(path: Path, identities: Sequence[Any]) -> bool:
    return load_llm_cache(path, expected=list(identities)) is not None


def _probe_llm_cache(path: Path, identities: Sequence[Any]) -> CacheProbe:
    if load_llm_cache(path, expected=list(identities)) is not None:
        return CacheProbe(reusable=True)
    return CacheProbe(reusable=False, stale=path.exists())


def _probe_raw_file(path: Path) -> CacheProbe:
    return CacheProbe(reusable=path.exists())


def _combine_probes(*probes: CacheProbe) -> CacheProbe:
    return CacheProbe(
        reusable=all(probe.reusable for probe in probes),
        stale=any(probe.stale for probe in probes),
    )


def _choose_probe(*probes: CacheProbe) -> CacheProbe:
    if any(probe.reusable for probe in probes):
        return CacheProbe(reusable=True)
    if any(probe.stale for probe in probes):
        return CacheProbe(reusable=False, stale=True)
    return CacheProbe(reusable=False)


def _read_llm_cache_data(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    data = payload.get("data")
    return data if isinstance(data, dict) else None


def _load_youtube_classification_cache(repo_root: Path, record: YouTubeRecord) -> dict[str, Any] | None:
    try:
        cached = load_llm_cache(
            youtube_enrich.classification_path(repo_root, record.video_id),
            expected=get_llm_service().cache_identities(
                task_class="classification",
                prompt_version=youtube_enrich.CLASSIFY_VIDEO_PROMPT_VERSION,
            ),
        )
        return normalize_youtube_classification(cached) if isinstance(cached, dict) else None
    except Exception:
        return None


def _probe_youtube_classification_cache(repo_root: Path, record: YouTubeRecord) -> CacheProbe:
    path = youtube_enrich.classification_path(repo_root, record.video_id)
    return CacheProbe(reusable=_load_youtube_classification_cache(repo_root, record) is not None, stale=path.exists() and _load_youtube_classification_cache(repo_root, record) is None)


def _load_book_classification_cache(repo_root: Path, book: BookRecord) -> dict[str, Any] | None:
    try:
        cached = load_llm_cache(
            books_enrich.classification_path(repo_root, book),
            expected=get_llm_service().cache_identities(
                task_class="classification",
                prompt_version=books_enrich.CLASSIFY_BOOK_PROMPT_VERSION,
            ),
        )
        return normalize_book_classification(cached) if isinstance(cached, dict) else None
    except Exception:
        return None


def _probe_book_classification_cache(repo_root: Path, book: BookRecord) -> CacheProbe:
    path = books_enrich.classification_path(repo_root, book)
    cached = _load_book_classification_cache(repo_root, book)
    return CacheProbe(reusable=cached is not None, stale=path.exists() and cached is None)


def _load_book_source_grounded_cache(repo_root: Path, book: BookRecord) -> dict[str, Any] | None:
    path = books_enrich.source_research_path(repo_root, book)
    raw = _read_llm_cache_data(path)
    if not isinstance(raw, dict):
        return None
    source_kind = str(raw.get("source_kind") or "")
    if source_kind not in {"document", "audio"}:
        return None
    task_class = "document" if source_kind == "document" else "transcription"
    prompt_version = f"books.source-grounded.segmented.{source_kind}.v1"
    try:
        return load_llm_cache(
            path,
            expected=get_llm_service().cache_identities(task_class=task_class, prompt_version=prompt_version),
        )
    except Exception:
        return None


def _probe_book_source_grounded_cache(repo_root: Path, book: BookRecord) -> CacheProbe:
    path = books_enrich.source_research_path(repo_root, book)
    cached = _load_book_source_grounded_cache(repo_root, book)
    return CacheProbe(reusable=cached is not None, stale=path.exists() and cached is None)


def _load_book_deep_research_cache(repo_root: Path, book: BookRecord) -> dict[str, Any] | None:
    try:
        return load_llm_cache(
            books_enrich.deep_research_path(repo_root, book),
            expected=get_llm_service().cache_identities(
                task_class="research",
                prompt_version=books_enrich.RESEARCH_BOOK_DEEP_PROMPT_VERSION,
            ),
        )
    except Exception:
        return None


def _probe_book_deep_research_cache(repo_root: Path, book: BookRecord) -> CacheProbe:
    path = books_enrich.deep_research_path(repo_root, book)
    cached = _load_book_deep_research_cache(repo_root, book)
    return CacheProbe(reusable=cached is not None, stale=path.exists() and cached is None)


def _youtube_pass_a_cache(repo_root: Path, record: YouTubeRecord) -> bool:
    return _llm_cache_exists(
        youtube_enrich.transcript_path(repo_root, record.video_id),
        _summary_identities(youtube_enrich.SUMMARIZE_TRANSCRIPT_PROMPT_VERSION),
    )


def _book_pass_a_cache(repo_root: Path, book: BookRecord) -> bool:
    if _llm_cache_exists(books_enrich.summary_path(repo_root, book), _summary_identities(books_enrich.SUMMARIZE_BOOK_RESEARCH_PROMPT_VERSION)):
        return True
    payload = _load_book_source_grounded_cache(repo_root, book)
    return isinstance(payload, dict) and isinstance(payload.get("summary"), dict)


def _substack_pass_a_cache(repo_root: Path, record: SubstackRecord) -> bool:
    return (
        _llm_cache_exists(substack_enrich.links_cache_path(repo_root, record.id), get_llm_service().cache_identities(task_class="classification", prompt_version=substack_enrich.CLASSIFY_LINKS_PROMPT_VERSION))
        and _llm_cache_exists(substack_enrich.summary_cache_path(repo_root, record.id), _summary_identities(substack_enrich.SUMMARIZE_SUBSTACK_PROMPT_VERSION))
    )


def _probe_book_pass_a_cache(repo_root: Path, book: BookRecord, source_grounded_probe: CacheProbe) -> CacheProbe:
    summary_probe = _probe_llm_cache(
        books_enrich.summary_path(repo_root, book),
        _summary_identities(books_enrich.SUMMARIZE_BOOK_RESEARCH_PROMPT_VERSION),
    )
    if summary_probe.reusable:
        return summary_probe
    if source_grounded_probe.reusable:
        cached = _load_book_source_grounded_cache(repo_root, book) or {}
        if isinstance(cached.get("summary"), dict):
            return CacheProbe(reusable=True)
    return CacheProbe(reusable=False, stale=summary_probe.stale or source_grounded_probe.stale)


def _book_attribution_fallback(repo_root: Path, book: BookRecord) -> dict[str, Any] | None:
    author_name = book.author[0] if book.author else ""
    if not author_name:
        return {
            "status": "unsupported",
            "reason": "book creator target could not be resolved",
            "stance_change_note": "",
            "stance_context": "",
        }
    stance_context = books_enrich.load_stance_context(
        slug=books_enrich.slugify(author_name),
        kind="person",
        repo_root=repo_root,
    )
    if not stance_context:
        return {
            "status": "empty",
            "reason": "no prior author stance context exists yet",
            "stance_change_note": "",
            "stance_context": "",
        }
    return None


def _youtube_attribution_fallback(repo_root: Path, record: YouTubeRecord) -> dict[str, Any] | None:
    stance_context = youtube_enrich.load_stance_context(
        slug=youtube_enrich.slugify(record.channel),
        kind="channel",
        repo_root=repo_root,
    )
    if not stance_context:
        return {
            "status": "empty",
            "reason": "no prior channel stance context exists yet",
            "stance_change_note": "",
            "stance_context": "",
        }
    return None


def _probe_youtube_pass_c(repo_root: Path, record: YouTubeRecord) -> CacheProbe:
    path = youtube_enrich.attribute_cache_path(repo_root, record.video_id)
    cached = load_llm_cache(path, expected=_stance_identities())
    if isinstance(cached, dict):
        return CacheProbe(reusable=True)
    fallback = _youtube_attribution_fallback(repo_root, record)
    if fallback is not None:
        return CacheProbe(reusable=True)
    return CacheProbe(reusable=False, stale=path.exists())


def _probe_book_pass_c(repo_root: Path, book: BookRecord) -> CacheProbe:
    path = books_enrich.attribute_cache_path(repo_root, book)
    cached = load_llm_cache(path, expected=_stance_identities())
    if isinstance(cached, dict):
        return CacheProbe(reusable=True)
    fallback = _book_attribution_fallback(repo_root, book)
    if fallback is not None:
        return CacheProbe(reusable=True)
    return CacheProbe(reusable=False, stale=path.exists())


def _probe_article_fetch_cache(repo_root: Path, entry: ArticleDropEntry) -> CacheProbe:
    slug = article_slugify_url(entry.url, entry.discovered_at)
    cache = raw_path(repo_root, "transcripts", "articles", f"{slug}.html")
    metadata_cache = cache.with_suffix(".meta.json")
    reusable = _load_cached_article_fetch(repo_root, entry) is not None
    return CacheProbe(reusable=reusable, stale=(cache.exists() or metadata_cache.exists()) and not reusable)


def _probe_substack_acquisition_cache(repo_root: Path, record: SubstackRecord) -> CacheProbe:
    if record.body_html:
        return CacheProbe(reusable=True)
    return _probe_raw_file(substack_enrich.html_cache_path(repo_root, record.id))


def _probe_substack_pass_a_cache(repo_root: Path, record: SubstackRecord) -> CacheProbe:
    links_probe = _probe_llm_cache(
        substack_enrich.links_cache_path(repo_root, record.id),
        get_llm_service().cache_identities(task_class="classification", prompt_version=substack_enrich.CLASSIFY_LINKS_PROMPT_VERSION),
    )
    summary_probe = _probe_llm_cache(
        substack_enrich.summary_cache_path(repo_root, record.id),
        _summary_identities(substack_enrich.SUMMARIZE_SUBSTACK_PROMPT_VERSION),
    )
    return _combine_probes(links_probe, summary_probe)


def _article_attribution_fallback(
    repo_root: Path,
    entry: ArticleDropEntry,
    *,
    fetch_result: ArticleFetchResult | None,
    source: NormalizedSource | None = None,
) -> dict[str, Any] | None:
    if fetch_result is None:
        return None
    article_source = source or articles_enrich.normalize_article_source(entry, fetch_result=fetch_result)
    targets = articles_enrich._materialization_targets_from_source(article_source)
    creator_target = targets.creator_target
    if creator_target is None:
        return {
            "status": "unsupported",
            "reason": "article creator target could not be resolved",
            "stance_change_note": "",
            "stance_context": "",
        }
    if creator_target.page_type != "person":
        return {
            "status": "unsupported",
            "reason": "article company creators intentionally skip Pass C in Phase 3",
            "stance_change_note": "",
            "stance_context": "",
        }
    stance_context = articles_enrich.load_stance_context(
        slug=creator_target.resolved_page_id(),
        kind="person",
        repo_root=repo_root,
    )
    if not stance_context:
        return {
            "status": "empty",
            "reason": "no prior author stance context exists yet",
            "stance_change_note": "",
            "stance_context": "",
        }
    return None


def _probe_article_pass_c(repo_root: Path, entry: ArticleDropEntry) -> CacheProbe:
    path = articles_enrich.attribute_cache_path(repo_root, entry)
    cached = load_llm_cache(path, expected=_stance_identities())
    if isinstance(cached, dict):
        return CacheProbe(reusable=True)
    fetch_result = _load_cached_article_fetch(repo_root, entry)
    fallback = _article_attribution_fallback(repo_root, entry, fetch_result=fetch_result)
    if fallback is not None:
        return CacheProbe(reusable=True)
    return CacheProbe(reusable=False, stale=path.exists())


def _projected_youtube_rewrites(repo_root: Path, request: ReingestRequest, record: YouTubeRecord) -> int:
    if REINGEST_STAGE_ORDER.index(request.through) < REINGEST_STAGE_ORDER.index("materialize"):
        return 0
    return 2 + int(youtube_channel_page_path(repo_root, record).exists())


def _projected_book_rewrites(repo_root: Path, request: ReingestRequest, book: BookRecord) -> int:
    if REINGEST_STAGE_ORDER.index(request.through) < REINGEST_STAGE_ORDER.index("materialize"):
        return 0
    vault = Vault.load(repo_root)
    rewrites = 2
    author_slug = books_enrich.slugify(book.author[0]) if book.author else "unknown"
    if (vault.wiki / "people" / f"{author_slug}.md").exists():
        rewrites += 1
    if book.publisher and (vault.wiki / "companies" / f"{books_enrich.slugify(book.publisher)}.md").exists():
        rewrites += 1
    return rewrites


def _projected_article_rewrites(repo_root: Path, request: ReingestRequest, entry: ArticleDropEntry) -> int:
    if REINGEST_STAGE_ORDER.index(request.through) < REINGEST_STAGE_ORDER.index("materialize"):
        return 0
    return 2


def _projected_substack_rewrites(repo_root: Path, request: ReingestRequest, record: SubstackRecord) -> int:
    if REINGEST_STAGE_ORDER.index(request.through) < REINGEST_STAGE_ORDER.index("materialize"):
        return 0
    return 2


def _execute_item(request: ReingestRequest, repo_root: Path, item: _LaneItem):
    executors = {
        "youtube": _execute_youtube_item,
        "books": _execute_book_item,
        "articles": _execute_article_item,
        "substack": _execute_substack_item,
    }
    return executors[item.lane](request, repo_root, item)


def _execute_personalization_link_repair_item(
    *,
    lane: str,
    repo_root: Path,
    item: _LaneItem,
    today: str | None,
):
    executor = {
        "youtube": _execute_youtube_item,
        "books": _execute_book_item,
    }[lane]
    refresh_request = ReingestRequest(
        lane=lane,
        stage="pass_b",
        through="pass_b",
        today=today,
        limit=None,
        source_ids=(item.source_id,),
        dry_run=False,
    )
    executor(refresh_request, repo_root, item)
    materialize_request = ReingestRequest(
        lane=lane,
        stage="materialize",
        through="materialize",
        today=today,
        limit=None,
        source_ids=(item.source_id,),
        dry_run=False,
    )
    return executor(materialize_request, repo_root, item)


def _phase_window(request: ReingestRequest) -> tuple[str, str]:
    start = "pass_a" if request.stage == "acquire" else request.stage
    return normalize_lifecycle_stage(start), normalize_lifecycle_stage(request.through)


def _seed_envelope(source_id: str) -> dict[str, Any]:
    return make_enrichment_envelope(source_id=source_id)


def _read_cached_pass_d_payload(repo_root: Path, *, source_kind: str, source_id: str) -> dict[str, Any] | None:
    raw_payload = load_llm_cache(
        pass_d_cache_path(repo_root=repo_root, source_kind=source_kind, source_id=source_id),
        expected=_pass_d_identities(),
    )
    if not isinstance(raw_payload, dict):
        return None
    parsed = _parse_pass_d_result(raw_payload)
    return {
        "q1_matches": [entry.__dict__ for entry in parsed.q1_matches],
        "q2_candidates": [entry.__dict__ for entry in parsed.q2_candidates],
        "warnings": list(parsed.warnings),
        "dropped_q1_matches": parsed.dropped_q1_matches,
        "dropped_q2_candidates": parsed.dropped_q2_candidates,
    }


def _load_cached_article_fetch(repo_root: Path, entry: ArticleDropEntry) -> ArticleFetchResult | None:
    slug = article_slugify_url(entry.url, entry.discovered_at)
    cache = raw_path(repo_root, "transcripts", "articles", f"{slug}.html")
    metadata_cache = cache.with_suffix(".meta.json")
    if not cache.exists() or not metadata_cache.exists():
        return None
    meta = json.loads(metadata_cache.read_text(encoding="utf-8"))
    return ArticleFetchResult(
        body_text=cache.read_text(encoding="utf-8"),
        title=meta.get("title"),
        author=meta.get("author"),
        sitename=meta.get("sitename"),
        published=meta.get("published"),
        raw_html_path=cache,
    )


def _execute_youtube_item(request: ReingestRequest, repo_root: Path, item: _LaneItem) -> None:
    record: YouTubeRecord = item.payload
    source, handlers, seed = _prepare_youtube_execution(request, repo_root, record)
    start_stage, through_stage = _phase_window(request)
    result = run_ingestion_window(
        source=source,
        handlers=handlers,
        start_stage=start_stage,
        through_stage=through_stage,
        seed_envelope=seed,
    )
    write_quality_receipt(repo_root=repo_root, result=result, executed_at=request.today or item_date(record.watched_at))
    return result


def _prepare_youtube_execution(request: ReingestRequest, repo_root: Path, record: YouTubeRecord):
    duration_override = request.youtube_default_duration_minutes
    if youtube_filter.should_skip_record(record, duration_minutes_override=duration_override):
        raise ValueError("excluded by cheap YouTube filter")
    if REINGEST_STAGE_ORDER.index(request.stage) <= REINGEST_STAGE_ORDER.index("pass_a"):
        try:
            classification = normalize_youtube_classification(youtube_enrich.classify(record, description=record.description, tags=list(record.tags)))
        except TypeError:
            classification = normalize_youtube_classification(youtube_enrich.classify(record))
        if not should_materialize(classification):
            raise ValueError(f"excluded by content policy ({classification.get('category') or classification.get('retention') or 'exclude'})")
        transcription = youtube_enrich.fetch_transcription_result(record, repo_root=repo_root)
    else:
        classification = _load_youtube_classification_cache(repo_root, record) or {}
        if not should_materialize(classification):
            raise ValueError(f"excluded by content policy ({classification.get('category') or classification.get('retention') or 'exclude'})")
        transcript = youtube_enrich.raw_transcript_path(repo_root, record.video_id).read_text(encoding="utf-8").strip() if youtube_enrich.raw_transcript_path(repo_root, record.video_id).exists() else ""
        transcription = {
            "transcript": transcript,
            "transcription_path": "",
            "multimodal_error": "",
            "fallback_attempts": [],
        }
    transcript = str(transcription.get("transcript") or "").strip()
    if not transcript:
        raise ValueError("missing reusable transcript payload")
    source = youtube_enrich.normalize_youtube_source(record, classification=classification, transcription=transcription)
    handlers = _youtube_handlers(repo_root, record, classification, transcription, request.today or item_date(record.watched_at))
    seed = _seed_youtube_envelope(request, repo_root, record, source, classification, transcription)
    return source, handlers, seed


def item_date(value: str) -> str:
    return (value or "")[:10] or __import__("datetime").date.today().isoformat()


def _youtube_handlers(
    repo_root: Path,
    record: YouTubeRecord,
    classification: dict[str, Any],
    transcription: dict[str, Any],
    today: str,
) -> LifecycleHandlers:
    def understand(youtube_source: NormalizedSource, _envelope: dict[str, object]) -> dict[str, object]:
        prior_context = youtube_enrich._get_prior_channel_context(record, repo_root=repo_root)
        summary = youtube_enrich.summarize(record, youtube_source.primary_content)
        summary = youtube_enrich.verify_source_quotes(
            summary=summary,
            body_text=youtube_source.primary_content,
            source_id=record.video_id,
            source_kind="youtube",
            repo_root=repo_root,
        )
        return {
            "summary": summary,
            "classification": classification,
            "prior_context": prior_context,
            "verification": {
                "transcription_path": transcription.get("transcription_path", ""),
                "multimodal_error": transcription.get("multimodal_error", ""),
            },
            "materialization_hints": {
                "description_links": list(youtube_source.discovered_links or []),
            },
        }

    def personalize(_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        applied = youtube_enrich.apply_video_to_you(record, summary=(envelope.get("pass_a") or {}).get("summary", {}), repo_root=repo_root)
        status = "implemented" if any(applied.get(key) for key in ("applied_paragraph", "applied_bullets", "thread_links")) else "empty"
        return {"status": status, "applied": applied}

    def attribute(_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        return youtube_enrich.build_channel_attribution(record, summary=(envelope.get("pass_a") or {}).get("summary", {}), repo_root=repo_root)

    def distill(youtube_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        if not should_materialize(classification) or str(classification.get("synthesis_mode") or "") != "deep":
            return {
                "skipped": True,
                "reason": "synthesis_mode is not deep",
                "evidence_updates": 0,
                "probationary_updates": 0,
                "missing_atoms": [],
                "cache_reused": False,
            }
        return youtube_enrich.run_pass_d_for_youtube(
            record,
            transcript=youtube_source.primary_content,
            summary=(envelope.get("pass_a") or {}).get("summary", {}),
            classification=classification,
            applied=(envelope.get("pass_b") or {}).get("applied"),
            attribution=envelope.get("pass_c") or {},
            repo_root=repo_root,
            today=today,
            prior_source_context=str((envelope.get("pass_a") or {}).get("prior_context") or ""),
        )

    def materialize(youtube_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, str]:
        from scripts.youtube import write_pages

        summary = (envelope.get("pass_a") or {}).get("summary", {})
        source_classification = dict((envelope.get("pass_a") or {}).get("classification", {}) or {})
        category = compatibility_category(source_classification, lane="youtube")
        pass_b = envelope.get("pass_b") or {}
        pass_c = envelope.get("pass_c") or {}
        targets = youtube_enrich._materialization_targets_from_source(youtube_source)
        video = write_pages.write_video_page(
            record,
            summary,
            duration_minutes=youtube_filter.duration_minutes(record),
            category=category,
            policy=source_classification,
            applied=pass_b.get("applied"),
            stance_change_note=(pass_c.get("stance_change_note") or "").strip() or None,
            creator_target=targets.creator_target,
            force=True,
        )
        channel = write_pages.ensure_channel_page(
            record,
            repo_root=repo_root,
            creator_target=targets.creator_target,
            source_link=write_pages.canonical_page_id(repo_root, record),
        )
        materialized = {"video": str(video)}
        if channel is not None:
            materialized["channel"] = str(channel)
        return materialized

    def propagate(youtube_source: NormalizedSource, envelope: dict[str, object], materialized: dict[str, str] | None) -> dict[str, object]:
        from scripts.atoms import pass_d
        from scripts.youtube import write_pages

        pass_a = envelope.get("pass_a") or {}
        verification = envelope.get("verification") or {}
        description_links = list((pass_a.get("materialization_hints") or {}).get("description_links") or [])
        actionable_links = filter_article_links_for_queue(description_links)
        drop_path = youtube_enrich.append_article_links_to_drop_queue(
            repo_root=repo_root,
            today=today,
            source_id=record.video_id,
            source_url=record.title_url or f"https://www.youtube.com/watch?v={record.video_id}",
            source_type="youtube-description",
            discovered_at=record.watched_at,
            links=actionable_links,
            source_label="youtube-description",
        )
        logged_entities = youtube_enrich.log_source_entities(
            summary=pass_a.get("summary", {}),
            body_text=youtube_source.primary_content,
            repo_root=repo_root,
            today=today,
            source_link=write_pages.canonical_page_id(repo_root, record),
            inbox_kind="youtube-entities",
            stopwords=youtube_enrich._youtube_stopwords(record),
        )
        _ = materialized
        return {
            "pass_d": pass_d.stage_outcomes_from_payload(envelope.get("pass_d") or {}),
            "transcription_path": verification.get("transcription_path", ""),
            "multimodal_error": verification.get("multimodal_error", ""),
            "drop_path": str(drop_path),
            "logged_entities": logged_entities,
            "logged_entity_count": len(logged_entities),
            "propagate_discovered_count": len(description_links),
            "propagate_queued_count": len(actionable_links),
        }

    return LifecycleHandlers(
        understand=understand,
        personalize=personalize,
        attribute=attribute,
        distill=distill,
        materialize=materialize,
        propagate=propagate,
    )


def _seed_youtube_envelope(
    request: ReingestRequest,
    repo_root: Path,
    record: YouTubeRecord,
    source: NormalizedSource,
    classification: dict[str, Any],
    transcription: dict[str, Any],
) -> dict[str, Any]:
    seed = _seed_envelope(source.source_id)
    required = _downstream_seed_requirements(request.stage)
    if "pass_a" in required:
        summary = load_llm_cache(
            youtube_enrich.transcript_path(repo_root, record.video_id),
            expected=_summary_identities(youtube_enrich.SUMMARIZE_TRANSCRIPT_PROMPT_VERSION),
        )
        if not isinstance(summary, dict):
            raise ValueError("missing or stale pass_a cache")
        seed["pass_a"] = {
            "summary": summary,
            "classification": classification,
            "prior_context": youtube_enrich._get_prior_channel_context(record, repo_root=repo_root),
            "materialization_hints": {
                "description_links": list(source.discovered_links or []),
            },
        }
        seed["verification"] = {
            "transcription_path": transcription.get("transcription_path", ""),
            "multimodal_error": transcription.get("multimodal_error", ""),
        }
    if "pass_b" in required:
        applied = load_llm_cache(youtube_enrich.applied_path(repo_root, record.video_id), expected=_personalization_identities())
        if not isinstance(applied, dict):
            raise ValueError("missing or stale pass_b cache")
        seed["pass_b"] = {
            "status": "implemented" if any(applied.get(key) for key in ("applied_paragraph", "applied_bullets", "thread_links")) else "empty",
            "applied": applied,
        }
    if "pass_c" in required:
        attribution = load_llm_cache(youtube_enrich.attribute_cache_path(repo_root, record.video_id), expected=_stance_identities())
        if not isinstance(attribution, dict):
            attribution = _youtube_attribution_fallback(repo_root, record)
        if not isinstance(attribution, dict):
            raise ValueError("missing or stale pass_c cache")
        seed["pass_c"] = attribution
    if "pass_d" in required:
        payload = _read_cached_pass_d_payload(repo_root, source_kind="youtube", source_id=f"youtube-{record.video_id}")
        if payload is None:
            raise ValueError("missing or stale pass_d cache")
        seed["pass_d"] = payload
    return seed


def _execute_book_item(request: ReingestRequest, repo_root: Path, item: _LaneItem) -> None:
    book: BookRecord = item.payload
    source, handlers, seed = _prepare_book_execution(request, repo_root, book)
    start_stage, through_stage = _phase_window(request)
    result = run_ingestion_window(
        source=source,
        handlers=handlers,
        start_stage=start_stage,
        through_stage=through_stage,
        seed_envelope=seed,
    )
    write_quality_receipt(
        repo_root=repo_root,
        result=result,
        executed_at=request.today or item_date(book.finished_date or book.started_date),
    )
    return result


def _prepare_book_execution(request: ReingestRequest, repo_root: Path, book: BookRecord):
    if REINGEST_STAGE_ORDER.index(request.stage) <= REINGEST_STAGE_ORDER.index("pass_a"):
        classification = books_enrich.effective_book_classification(
            book,
            books_enrich.classify(book),
            force_deep=request.force_deep,
        )
        if not should_materialize(classification):
            raise ValueError(f"excluded by content policy ({classification.get('category') or classification.get('retention') or 'exclude'})")
        source_grounded = books_enrich.enrich_from_source(book)
        if source_grounded is not None:
            research = source_grounded["summary"]
            source = books_enrich.normalize_book_source(
                book,
                classification=classification,
                research=research,
                source_kind=str(source_grounded.get("source_kind") or "document"),
                source_text=str(source_grounded.get("source_text") or ""),
                source_asset_path=str(source_grounded.get("source_asset_path") or ""),
            )
        else:
            research = books_enrich.enrich_deep(book)
            source = books_enrich.normalize_book_source(book, classification=classification, research=research, source_kind="research")
    else:
        classification = books_enrich.effective_book_classification(
            book,
            _load_book_classification_cache(repo_root, book) or {},
            force_deep=request.force_deep,
        )
        if not should_materialize(classification):
            raise ValueError(f"excluded by content policy ({classification.get('category') or classification.get('retention') or 'exclude'})")
        source_grounded = _load_book_source_grounded_cache(repo_root, book)
        research = _load_book_deep_research_cache(repo_root, book) or {}
        if source_grounded is not None:
            source = books_enrich.normalize_book_source(
                book,
                classification=classification,
                research=source_grounded.get("summary", {}),
                source_kind=str(source_grounded.get("source_kind") or "document"),
                source_text=str(source_grounded.get("source_text") or ""),
                source_asset_path=str(source_grounded.get("source_asset_path") or ""),
            )
        else:
            source = books_enrich.normalize_book_source(book, classification=classification, research=research, source_kind="research")
    handlers = _book_handlers(
        repo_root,
        book,
        classification,
        source_grounded,
        research,
        request.today or item_date(book.finished_date or book.started_date),
    )
    seed = _seed_book_envelope(request, repo_root, book, source, classification)
    return source, handlers, seed


def _book_handlers(
    repo_root: Path,
    book: BookRecord,
    classification: dict[str, Any],
    source_grounded: dict[str, Any] | None,
    research_cache: dict[str, Any],
    today: str,
) -> LifecycleHandlers:
    def understand(book_source: NormalizedSource, _envelope: dict[str, object]) -> dict[str, object]:
        prior_context = books_enrich._get_prior_book_context(book, repo_root=repo_root)
        research_source_kind = str(book_source.provenance.get("source_kind") or "research")
        if research_source_kind in {"document", "audio"}:
            grounded_payload = source_grounded or {}
            research = grounded_payload.get("summary", {})
            summary = grounded_payload.get("summary", {})
            summary = books_enrich.verify_source_quotes(
                summary=summary,
                body_text=book_source.primary_content,
                source_id=book_source.source_id.replace("book-", "", 1),
                source_kind="book",
                repo_root=repo_root,
            )
            segment_count = int(grounded_payload.get("segment_count") or 0)
            segmentation_strategy = str(grounded_payload.get("segmentation_strategy") or "")
        else:
            research = research_cache or books_enrich.enrich_deep(book)
            summary = books_enrich.summarize_research(book, research)
            segment_count = 0
            segmentation_strategy = ""
        return {
            "classification": classification,
            "research": research,
            "summary": summary,
            "prior_context": prior_context,
            "research_source_kind": research_source_kind,
            "research_source_path": book_source.provenance.get("source_asset_path", ""),
            "segment_count": segment_count,
            "segmentation_strategy": segmentation_strategy,
        }

    def personalize(_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        applied = books_enrich.apply_to_you(book, (envelope.get("pass_a") or {}).get("summary", {}))
        status = "implemented" if any(applied.get(key) for key in ("applied_paragraph", "applied_bullets", "thread_links")) else "empty"
        return {"status": status, "applied": applied}

    def attribute(_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        return books_enrich.update_author_memory(book, summary_artifact=(envelope.get("pass_a") or {}).get("summary", {}), repo_root=repo_root)

    def distill(book_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        if not should_materialize(classification) or str(classification.get("synthesis_mode") or "") != "deep":
            return {
                "skipped": True,
                "reason": "synthesis_mode is not deep",
                "evidence_updates": 0,
                "probationary_updates": 0,
                "missing_atoms": [],
                "cache_reused": False,
            }
        return books_enrich.run_pass_d_for_book(
            book,
            body_or_transcript=book_source.primary_content,
            summary_artifact=(envelope.get("pass_a") or {}).get("summary", {}),
            classification=classification,
            applied=(envelope.get("pass_b") or {}).get("applied"),
            attribution=envelope.get("pass_c") or {},
            repo_root=repo_root,
            today=today,
            prior_source_context=str((envelope.get("pass_a") or {}).get("prior_context") or ""),
        )

    def materialize(book_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, str]:
        from scripts.books import write_pages

        pass_a = envelope.get("pass_a") or {}
        pass_b = envelope.get("pass_b") or {}
        pass_c = envelope.get("pass_c") or {}
        targets = books_enrich._materialization_targets_from_source(book_source)
        source_classification = dict(pass_a.get("classification", {}) or {})
        book_page = write_pages.write_book_page(
            book,
            pass_a.get("research", {}),
            category=compatibility_category(source_classification, lane="books"),
            policy=source_classification,
            applied=pass_b.get("applied"),
            stance_change_note=((pass_c.get("stance_change_note") or "").strip() or None),
            summary=pass_a.get("summary", {}),
            creator_target=targets.creator_target,
            publisher_target=targets.publisher_target,
            source_kind=str(pass_a.get("research_source_kind") or "research"),
            source_asset_path=str(pass_a.get("research_source_path") or ""),
            force=True,
        )
        materialized = {"book": str(book_page)}
        author_page = write_pages.ensure_author_page(
            book,
            repo_root=repo_root,
            creator_target=targets.creator_target,
            source_link=write_pages.canonical_page_id(repo_root, book),
        )
        if author_page is not None:
            materialized["author"] = str(author_page)
        publisher_page = write_pages.ensure_publisher_page(
            book,
            repo_root=repo_root,
            publisher_target=targets.publisher_target,
            source_link=write_pages.canonical_page_id(repo_root, book),
        )
        if publisher_page is not None:
            materialized["publisher"] = str(publisher_page)
        return materialized

    def propagate(book_source: NormalizedSource, envelope: dict[str, object], materialized: dict[str, str] | None) -> dict[str, object]:
        from scripts.atoms import pass_d

        logged_entities = books_enrich.log_source_entities(
            summary=(envelope.get("pass_a") or {}).get("summary", {}),
            body_text=book_source.primary_content,
            repo_root=repo_root,
            today=today,
            source_link=write_pages.canonical_page_id(repo_root, book),
            inbox_kind="book-entities",
            stopwords=books_enrich._book_stopwords(book),
        ) if str((envelope.get("pass_a") or {}).get("research_source_kind") or "") in {"document", "audio"} else []
        _ = materialized
        return {
            "pass_d": pass_d.stage_outcomes_from_payload(envelope.get("pass_d") or {}),
            "logged_entities": logged_entities,
            "logged_entity_count": len(logged_entities),
            "propagate_discovered_count": 0,
            "propagate_queued_count": 0,
        }

    return LifecycleHandlers(
        understand=understand,
        personalize=personalize,
        attribute=attribute,
        distill=distill,
        materialize=materialize,
        propagate=propagate,
    )


def _seed_book_envelope(
    request: ReingestRequest,
    repo_root: Path,
    book: BookRecord,
    source: NormalizedSource,
    classification: dict[str, Any],
) -> dict[str, Any]:
    seed = _seed_envelope(source.source_id)
    required = _downstream_seed_requirements(request.stage)
    if "pass_a" in required:
        summary = load_llm_cache(
            books_enrich.summary_path(repo_root, book),
            expected=_summary_identities(books_enrich.SUMMARIZE_BOOK_RESEARCH_PROMPT_VERSION),
        )
        source_grounded = _load_book_source_grounded_cache(repo_root, book)
        research = _load_book_deep_research_cache(repo_root, book) or {}
        if not isinstance(summary, dict) and not (isinstance(source_grounded, dict) and isinstance(source_grounded.get("summary"), dict)):
            raise ValueError("missing or stale pass_a cache")
        seed["pass_a"] = {
            "classification": classification,
            "research": source_grounded.get("summary", {}) if isinstance(source_grounded, dict) and source_grounded.get("summary") else research,
            "summary": summary if isinstance(summary, dict) else source_grounded.get("summary", {}),
            "prior_context": books_enrich._get_prior_book_context(book, repo_root=repo_root),
            "research_source_kind": str(source.provenance.get("source_kind") or "research"),
            "research_source_path": source.provenance.get("source_asset_path", ""),
            "segment_count": int((source_grounded or {}).get("segment_count") or 0),
            "segmentation_strategy": str((source_grounded or {}).get("segmentation_strategy") or ""),
        }
    if "pass_b" in required:
        applied = load_llm_cache(books_enrich.applied_path(repo_root, book), expected=_personalization_identities())
        if not isinstance(applied, dict):
            raise ValueError("missing or stale pass_b cache")
        seed["pass_b"] = {
            "status": "implemented" if any(applied.get(key) for key in ("applied_paragraph", "applied_bullets", "thread_links")) else "empty",
            "applied": applied,
        }
    if "pass_c" in required:
        attribution = load_llm_cache(books_enrich.attribute_cache_path(repo_root, book), expected=_stance_identities())
        if not isinstance(attribution, dict):
            attribution = _book_attribution_fallback(repo_root, book)
        if not isinstance(attribution, dict):
            raise ValueError("missing or stale pass_c cache")
        seed["pass_c"] = attribution
    if "pass_d" in required:
        payload = _read_cached_pass_d_payload(repo_root, source_kind="book", source_id=_book_source_id(book))
        if payload is None:
            raise ValueError("missing or stale pass_d cache")
        seed["pass_d"] = payload
    return seed


def _execute_article_item(request: ReingestRequest, repo_root: Path, item: _LaneItem) -> None:
    entry: ArticleDropEntry = item.payload
    source, handlers, seed = _prepare_article_execution(request, repo_root, entry)
    start_stage, through_stage = _phase_window(request)
    result = run_ingestion_window(
        source=source,
        handlers=handlers,
        start_stage=start_stage,
        through_stage=through_stage,
        seed_envelope=seed,
    )
    write_quality_receipt(repo_root=repo_root, result=result, executed_at=request.today or item_date(entry.discovered_at))
    return result


def _prepare_article_execution(request: ReingestRequest, repo_root: Path, entry: ArticleDropEntry):
    fetch_result = _load_cached_article_fetch(repo_root, entry)
    if fetch_result is None and REINGEST_STAGE_ORDER.index(request.stage) <= REINGEST_STAGE_ORDER.index("pass_a"):
        refreshed = fetch_article(entry, repo_root=repo_root)
        if isinstance(refreshed, ArticleFetchFailure):
            raise ValueError(f"article acquisition refresh failed: {refreshed.failure_kind}: {refreshed.detail}")
        fetch_result = refreshed
    if fetch_result is None:
        raise ValueError("missing acquisition cache")
    source = articles_enrich.normalize_article_source(entry, fetch_result=fetch_result)
    handlers = _article_handlers(repo_root, entry, fetch_result, request.today or item_date(entry.discovered_at))
    seed = _seed_article_envelope(request, repo_root, entry, source, fetch_result)
    return source, handlers, seed


def _article_handlers(repo_root: Path, entry: ArticleDropEntry, fetch_result: ArticleFetchResult, today: str) -> LifecycleHandlers:
    def understand(article_source: NormalizedSource, _envelope: dict[str, object]) -> dict[str, object]:
        summary = articles_enrich.summarize_article(entry, fetch_result=fetch_result, repo_root=repo_root)
        summary = articles_enrich.verify_source_quotes(
            summary=summary,
            body_text=article_source.primary_content,
            source_id=article_slugify_url(entry.url, entry.discovered_at),
            source_kind="article",
            repo_root=repo_root,
        )
        return {
            "summary": summary,
            "prior_context": articles_enrich._get_prior_article_context(fetch_result, repo_root=repo_root),
            "materialization_hints": {
                "additional_author_hints": list(article_source.source_metadata.get("additional_author_hints") or []),
                "discovered_links": list(article_source.discovered_links or []),
            },
        }

    def personalize(_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        applied = articles_enrich.apply_article_to_you(entry, fetch_result=fetch_result, summary=(envelope.get("pass_a") or {}).get("summary", {}), repo_root=repo_root)
        status = "implemented" if any(applied.get(key) for key in ("applied_paragraph", "applied_bullets", "thread_links")) else "empty"
        return {"status": status, "applied": applied}

    def attribute(article_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        return articles_enrich.build_article_attribution(
            entry,
            fetch_result=fetch_result,
            source=article_source,
            summary=(envelope.get("pass_a") or {}).get("summary", {}),
            repo_root=repo_root,
        )

    def distill(article_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        return articles_enrich.run_pass_d_for_article(
            entry,
            body_text=article_source.primary_content,
            summary=(envelope.get("pass_a") or {}).get("summary", {}),
            applied=(envelope.get("pass_b") or {}).get("applied"),
            attribution=envelope.get("pass_c") or {},
            repo_root=repo_root,
            today=today,
            prior_source_context=str((envelope.get("pass_a") or {}).get("prior_context") or ""),
        )

    def materialize(article_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, str]:
        from scripts.articles import write_pages

        pass_b = envelope.get("pass_b") or {}
        pass_c = envelope.get("pass_c") or {}
        targets = articles_enrich._materialization_targets_from_source(article_source)
        article = write_pages.write_article_page(
            entry,
            fetch_result=fetch_result,
            summary=(envelope.get("pass_a") or {}).get("summary", {}),
            repo_root=repo_root,
            applied=pass_b.get("applied"),
            stance_change_note=(pass_c.get("stance_change_note") or "").strip() or None,
            creator_target=targets.creator_target,
            publisher_target=targets.publisher_target,
            force=True,
        )
        materialized = {"article": str(article)}
        source_link = write_pages.canonical_page_id(repo_root, entry)
        author_page = write_pages.ensure_author_page(
            fetch_result=fetch_result,
            repo_root=repo_root,
            creator_target=targets.creator_target,
            publisher_target=targets.publisher_target,
            source_link=source_link,
        )
        if author_page is not None:
            materialized["author"] = str(author_page)
        publisher_page = write_pages.ensure_outlet_page(
            fetch_result=fetch_result,
            repo_root=repo_root,
            publisher_target=targets.publisher_target,
            source_link=source_link,
        )
        if publisher_page is not None:
            materialized["publisher"] = str(publisher_page)
        return materialized

    def propagate(article_source: NormalizedSource, envelope: dict[str, object], materialized: dict[str, str] | None) -> dict[str, object]:
        from scripts.atoms import pass_d

        pass_a = envelope.get("pass_a") or {}
        drop_path = articles_enrich.append_article_links_to_drop_queue(
            repo_root=repo_root,
            today=today,
            source_id=article_slugify_url(entry.url, entry.discovered_at),
            source_url=entry.url,
            source_type="article-link",
            source_label="article-link",
            discovered_at=entry.discovered_at,
            links=list((pass_a.get("materialization_hints") or {}).get("discovered_links") or []),
        )
        logged_entities = articles_enrich.log_source_entities(
            summary=pass_a.get("summary", {}),
            body_text=article_source.primary_content,
            repo_root=repo_root,
            today=today,
            source_link=write_pages.canonical_page_id(repo_root, entry),
            inbox_kind="article-entities",
            stopwords=articles_enrich._article_stopwords(fetch_result),
        )
        _ = materialized
        return {
            "pass_d": pass_d.stage_outcomes_from_payload(envelope.get("pass_d") or {}),
            "drop_path": str(drop_path),
            "logged_entities": logged_entities,
            "logged_entity_count": len(logged_entities),
            "propagate_discovered_count": len(list((pass_a.get("materialization_hints") or {}).get("discovered_links") or [])),
            "propagate_queued_count": len(list((pass_a.get("materialization_hints") or {}).get("discovered_links") or [])),
        }

    return LifecycleHandlers(
        understand=understand,
        personalize=personalize,
        attribute=attribute,
        distill=distill,
        materialize=materialize,
        propagate=propagate,
    )


def _seed_article_envelope(
    request: ReingestRequest,
    repo_root: Path,
    entry: ArticleDropEntry,
    source: NormalizedSource,
    fetch_result: ArticleFetchResult,
) -> dict[str, Any]:
    seed = _seed_envelope(source.source_id)
    required = _downstream_seed_requirements(request.stage)
    if "pass_a" in required:
        summary = load_llm_cache(
            articles_enrich.summary_cache_path(repo_root, entry),
            expected=_summary_identities("articles.summary.v1"),
        )
        if not isinstance(summary, dict):
            raise ValueError("missing or stale pass_a cache")
        seed["pass_a"] = {
            "summary": summary,
            "prior_context": articles_enrich._get_prior_article_context(fetch_result, repo_root=repo_root),
            "materialization_hints": {
                "additional_author_hints": list(source.source_metadata.get("additional_author_hints") or []),
                "discovered_links": list(source.discovered_links or []),
            },
        }
    if "pass_b" in required:
        applied = load_llm_cache(articles_enrich.applied_cache_path(repo_root, entry), expected=_personalization_identities())
        if not isinstance(applied, dict):
            raise ValueError("missing or stale pass_b cache")
        seed["pass_b"] = {
            "status": "implemented" if any(applied.get(key) for key in ("applied_paragraph", "applied_bullets", "thread_links")) else "empty",
            "applied": applied,
        }
    if "pass_c" in required:
        attribution = load_llm_cache(articles_enrich.attribute_cache_path(repo_root, entry), expected=_stance_identities())
        if not isinstance(attribution, dict):
            attribution = _article_attribution_fallback(
                repo_root,
                entry,
                fetch_result=fetch_result,
                source=source,
            )
        if not isinstance(attribution, dict):
            raise ValueError("missing or stale pass_c cache")
        seed["pass_c"] = attribution
    if "pass_d" in required:
        payload = _read_cached_pass_d_payload(
            repo_root,
            source_kind="article",
            source_id=f"article-{article_slugify_url(entry.url, entry.discovered_at)}",
        )
        if payload is None:
            raise ValueError("missing or stale pass_d cache")
        seed["pass_d"] = payload
    return seed


def _execute_substack_item(request: ReingestRequest, repo_root: Path, item: _LaneItem) -> None:
    record: SubstackRecord = item.payload
    source, handlers, seed = _prepare_substack_execution(request, repo_root, record)
    start_stage, through_stage = _phase_window(request)
    result = run_ingestion_window(
        source=source,
        handlers=handlers,
        start_stage=start_stage,
        through_stage=through_stage,
        seed_envelope=seed,
    )
    write_quality_receipt(repo_root=repo_root, result=result, executed_at=request.today or item_date(record.saved_at))
    return result


def _prepare_substack_execution(request: ReingestRequest, repo_root: Path, record: SubstackRecord):
    body_html = record.body_html or (substack_enrich.html_cache_path(repo_root, record.id).read_text(encoding="utf-8") if substack_enrich.html_cache_path(repo_root, record.id).exists() else "")
    if not body_html and REINGEST_STAGE_ORDER.index(request.stage) <= REINGEST_STAGE_ORDER.index("pass_a"):
        body_html = substack_enrich.fetch_body(
            record,
            client=__import__("scripts.substack.auth", fromlist=["build_client"]).build_client(),
            repo_root=repo_root,
        )
    if not body_html:
        raise ValueError("missing acquisition cache")
    body_md = __import__("scripts.substack.html_to_markdown", fromlist=["convert"]).convert(body_html)
    source = substack_enrich.normalize_substack_source(record, body_markdown=body_md, body_html=body_html)
    handlers = _substack_handlers(repo_root, record, request.today or item_date(record.saved_at))
    seed = _seed_substack_envelope(request, repo_root, record, source)
    return source, handlers, seed


def _substack_handlers(repo_root: Path, record: SubstackRecord, today: str) -> LifecycleHandlers:
    def understand(substack_source: NormalizedSource, _envelope: dict[str, object]) -> dict[str, object]:
        html = substack_source.source_metadata["body_html"]
        classified = substack_enrich.classify_post_links(record, body_html=html, repo_root=repo_root)
        prior_context = substack_enrich.get_prior_posts_context(record, repo_root)
        stance_context = substack_enrich.stance.load_stance_context(substack_enrich._slugify(record.author_name), repo_root)
        summary = substack_enrich.summarize_post(
            record,
            body_markdown=substack_source.primary_content,
            repo_root=repo_root,
            prior_posts_context=prior_context,
            stance_context=stance_context,
        )
        summary = substack_enrich.verify_quotes(summary, substack_source.primary_content, record, repo_root)
        return {
            "summary": summary,
            "classified_links": classified,
            "body_markdown": substack_source.primary_content,
            "prior_context": prior_context,
            "stance_context": stance_context,
        }

    def personalize(_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        applied = substack_enrich.apply_post_to_you(record, summary=(envelope.get("pass_a") or {}).get("summary", {}), repo_root=repo_root)
        return {"applied": applied}

    def attribute(_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        stance_change_note = substack_enrich.update_author_stance(record, summary=(envelope.get("pass_a") or {}).get("summary", {}), repo_root=repo_root)
        return {
            "status": "implemented" if (stance_change_note or "").strip() else "empty",
            "stance_change_note": stance_change_note,
        }

    def distill(substack_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        pass_a = envelope.get("pass_a") or {}
        return substack_enrich.run_pass_d_for_substack(
            record,
            body_markdown=pass_a.get("body_markdown", substack_source.primary_content),
            summary=pass_a.get("summary", {}),
            applied=(envelope.get("pass_b") or {}).get("applied"),
            stance_change_note=(envelope.get("pass_c") or {}).get("stance_change_note"),
            stance_context_text=pass_a.get("stance_context", ""),
            prior_context=pass_a.get("prior_context", ""),
            repo_root=repo_root,
            today=today,
        )

    def materialize(substack_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, str]:
        from scripts.substack import write_pages

        pass_a = envelope.get("pass_a") or {}
        pass_b = envelope.get("pass_b") or {}
        pass_c = envelope.get("pass_c") or {}
        targets = substack_enrich._materialization_targets_from_source(substack_source)
        article = write_pages.write_article_page(
            record,
            summary=pass_a.get("summary", {}),
            classified_links=pass_a.get("classified_links", {}),
            body_markdown=pass_a.get("body_markdown", substack_source.primary_content),
            repo_root=repo_root,
            applied=pass_b.get("applied"),
            stance_change_note=pass_c.get("stance_change_note"),
            creator_target=targets.creator_target,
            publisher_target=targets.publisher_target,
            force=True,
        )
        materialized = {"article": str(article)}
        author = write_pages.ensure_author_page(
            record,
            repo_root=repo_root,
            creator_target=targets.creator_target,
            publisher_target=targets.publisher_target,
        )
        publication = write_pages.ensure_publication_page(
            record,
            repo_root=repo_root,
            publisher_target=targets.publisher_target,
        )
        materialized["author"] = str(author)
        materialized["publication"] = str(publication)
        return materialized

    def propagate(substack_source: NormalizedSource, envelope: dict[str, object], materialized: dict[str, str] | None) -> dict[str, object]:
        from scripts.atoms import pass_d
        from scripts.substack import write_pages

        pass_a = envelope.get("pass_a") or {}
        classified = pass_a.get("classified_links", {})
        drop_path = write_pages.append_links_to_drop_queue(
            record,
            classified_links=classified,
            repo_root=repo_root,
            today=today,
        )
        logged_entities = substack_enrich.log_entities(
            record,
            summary=pass_a.get("summary", {}),
            body_markdown=pass_a.get("body_markdown", substack_source.primary_content),
            repo_root=repo_root,
            today=today,
        )
        external_classified = list(classified.get("external_classified") or [])
        _ = materialized
        return {
            "pass_d": pass_d.stage_outcomes_from_payload(envelope.get("pass_d") or {}),
            "drop_path": str(drop_path),
            "logged_entities": logged_entities,
            "logged_entity_count": len(logged_entities),
            "propagate_discovered_count": len(external_classified),
            "propagate_queued_count": len([link for link in external_classified if link.get("category") in ("business", "personal")]),
        }

    return LifecycleHandlers(
        understand=understand,
        personalize=personalize,
        attribute=attribute,
        distill=distill,
        materialize=materialize,
        propagate=propagate,
    )


def _seed_substack_envelope(
    request: ReingestRequest,
    repo_root: Path,
    record: SubstackRecord,
    source: NormalizedSource,
) -> dict[str, Any]:
    seed = _seed_envelope(source.source_id)
    required = _downstream_seed_requirements(request.stage)
    if "pass_a" in required:
        classified = load_llm_cache(
            substack_enrich.links_cache_path(repo_root, record.id),
            expected=get_llm_service().cache_identities(task_class="classification", prompt_version=substack_enrich.CLASSIFY_LINKS_PROMPT_VERSION),
        )
        summary = load_llm_cache(
            substack_enrich.summary_cache_path(repo_root, record.id),
            expected=_summary_identities(substack_enrich.SUMMARIZE_SUBSTACK_PROMPT_VERSION),
        )
        if not isinstance(classified, dict) or not isinstance(summary, dict):
            raise ValueError("missing or stale pass_a cache")
        seed["pass_a"] = {
            "summary": summary,
            "classified_links": classified,
            "body_markdown": source.primary_content,
            "prior_context": substack_enrich.get_prior_posts_context(record, repo_root),
            "stance_context": substack_enrich.stance.load_stance_context(substack_enrich._slugify(record.author_name), repo_root),
        }
    if "pass_b" in required:
        applied = load_llm_cache(substack_enrich.applied_cache_path(repo_root, record.id), expected=_personalization_identities(substack=True))
        if not isinstance(applied, dict):
            raise ValueError("missing or stale pass_b cache")
        seed["pass_b"] = {"applied": applied}
    if "pass_c" in required:
        cached = load_llm_cache(substack_enrich.stance.stance_cache_path(repo_root, record.id), expected=_stance_identities())
        if not isinstance(cached, dict):
            raise ValueError("missing or stale pass_c cache")
        seed["pass_c"] = {
            "status": "implemented" if (cached.get("change_note") or "").strip() else "empty",
            "stance_change_note": cached.get("change_note", ""),
        }
    if "pass_d" in required:
        payload = _read_cached_pass_d_payload(repo_root, source_kind="substack", source_id=f"substack-{record.id}")
        if payload is None:
            raise ValueError("missing or stale pass_d cache")
        seed["pass_d"] = payload
    return seed
