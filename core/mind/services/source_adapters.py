from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any, Protocol

import yaml

from mind.services import reingest as reingest_service
from mind.services.content_policy import should_materialize
from mind.services.llm_cache import load_llm_cache
from mind.services.quality_receipts import load_quality_receipt, quality_receipt_path
from mind.services.source_models import (
    InventoryItem,
    InventoryRequest,
    SourceArtifact,
    SourceKey,
    SourceStatus,
    StageFreshness,
    StageProbeState,
    StageStatus,
)
from mind.services.source_registry import SourceRegistry
from scripts.articles import enrich as articles_enrich
from scripts.articles import fetch as article_fetch
from scripts.articles.fetch import html_cache_path as article_html_cache_path
from scripts.articles.write_pages import slugify_url as article_slugify_url
from scripts.books import enrich as books_enrich
from scripts.common.vault import Vault, raw_path
from scripts.substack import enrich as substack_enrich
from scripts.substack import stance as substack_stance
from scripts.youtube import enrich as youtube_enrich
from scripts.youtube import filter as youtube_filter


@dataclass(frozen=True)
class PageBackedCandidate:
    source_key: SourceKey
    lane: str
    adapter: str
    title: str
    source_date: str
    aliases: tuple[str, ...]
    page_path: str
    source_id: str | None = None
    external_id: str | None = None


@dataclass(frozen=True)
class CacheOnlyCandidate:
    source_key: SourceKey
    lane: str
    adapter: str
    title: str
    source_date: str
    aliases: tuple[str, ...]
    source_id: str | None = None
    external_id: str | None = None
    stage_states: tuple[StageProbeState, ...] = ()
    artifacts: tuple[SourceArtifact, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


class SourceLaneAdapter(Protocol):
    lane: str
    adapter: str

    def enumerate_upstream(self, request: InventoryRequest, repo_root: Path) -> list[Any]:
        ...

    def enumerate_page_backed(self, repo_root: Path) -> list[PageBackedCandidate]:
        ...

    def enumerate_cache_only(self, repo_root: Path) -> list[CacheOnlyCandidate]:
        ...

    def build_inventory_from_upstream(
        self,
        lane_item: Any,
        *,
        request: InventoryRequest,
        page_candidate: PageBackedCandidate | None,
        cache_candidate: CacheOnlyCandidate | None,
        repo_root: Path,
        registry: SourceRegistry | None,
    ) -> InventoryItem:
        ...

    def build_inventory_from_page(
        self,
        candidate: PageBackedCandidate,
        *,
        repo_root: Path,
        registry: SourceRegistry | None,
    ) -> InventoryItem:
        ...

    def build_inventory_from_cache(
        self,
        candidate: CacheOnlyCandidate,
        *,
        repo_root: Path,
        registry: SourceRegistry | None,
    ) -> InventoryItem:
        ...


def _file_fingerprint(path: Path | None) -> str | None:
    if path is None or not path.exists():
        return None
    stat = path.stat()
    return f"{stat.st_size}:{stat.st_mtime_ns}"


def _frontmatter(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        return {}
    end = text.find("\n---\n", 4)
    if end == -1:
        return {}
    try:
        payload = yaml.safe_load(text[4:end]) or {}
    except yaml.YAMLError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _registry_status(registry: SourceRegistry | None, source_key: SourceKey) -> str | None:
    if registry is None:
        return None
    details = registry.get(str(source_key))
    return details.source.status if details is not None else None


def _title_from_slug(slug: str) -> str:
    return slug.replace("-", " ").title()


def _source_date_from_receipt(path: Path | None) -> str:
    if path is None or not path.exists():
        return ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return ""
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("source_date") or "")[:10]


def _artifacts(entries: list[tuple[str, Path | None]]) -> tuple[SourceArtifact, ...]:
    result: list[SourceArtifact] = []
    for kind, path in entries:
        if path is None:
            continue
        result.append(
            SourceArtifact(
                artifact_kind=kind,
                path=str(path),
                fingerprint=_file_fingerprint(path),
                exists=path.exists(),
            )
        )
    return tuple(result)


def _stage_from_probe(stage: str, probe: Any, *, artifact_path: Path | None = None, summary: str | None = None) -> StageProbeState:
    if probe.reusable:
        return StageProbeState(stage=stage, status="completed", freshness="fresh", artifact_path=str(artifact_path) if artifact_path else None, summary=summary)
    if probe.stale:
        return StageProbeState(stage=stage, status="missing", freshness="stale", artifact_path=str(artifact_path) if artifact_path else None, summary=summary)
    return StageProbeState(stage=stage, status="missing", freshness="missing", artifact_path=str(artifact_path) if artifact_path else None, summary=summary)


def _stage_from_cache(path: Path | None, *, expected: list[Any] | Any | None = None, summary: str | None = None) -> StageProbeState:
    raise RuntimeError("unused")


def _stage_completed(stage: str, *, artifact_path: Path | None = None, summary: str | None = None) -> StageProbeState:
    return StageProbeState(stage=stage, status="completed", freshness="fresh", artifact_path=str(artifact_path) if artifact_path else None, summary=summary)


def _stage_missing(stage: str, *, artifact_path: Path | None = None, freshness: StageFreshness = "missing", summary: str | None = None) -> StageProbeState:
    return StageProbeState(stage=stage, status="missing", freshness=freshness, artifact_path=str(artifact_path) if artifact_path else None, summary=summary)


def _materialize_stage(page_path: str | None) -> StageProbeState:
    if page_path and Path(page_path).exists():
        return _stage_completed("materialize", artifact_path=Path(page_path), summary="canonical page present")
    return _stage_missing("materialize")


def _propagate_stage(repo_root: Path, *, lane: str, source_id: str | None) -> StageProbeState:
    if not source_id:
        return _stage_missing("propagate")
    receipt_path = quality_receipt_path(repo_root=repo_root, lane=lane, source_id=source_id)
    receipt = load_quality_receipt(repo_root=repo_root, lane=lane, source_id=source_id)
    if receipt is not None:
        propagate_status = str(receipt.get("propagate_status") or "").strip().lower()
        if propagate_status == "error":
            return _stage_missing(
                "propagate",
                artifact_path=receipt_path,
                summary=str(receipt.get("propagate_detail") or "quality receipt recorded a propagate error"),
            )
        return _stage_completed("propagate", artifact_path=receipt_path, summary="quality receipt present")
    return _stage_missing("propagate", artifact_path=receipt_path)


def _earliest_resume_stage(stages: tuple[StageProbeState, ...]) -> str | None:
    stage_map = {stage.stage: stage for stage in stages}
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


def _classify_status(
    *,
    stages: tuple[StageProbeState, ...],
    page_path: str | None,
    excluded_reason: str | None = None,
    has_any_artifacts: bool,
) -> tuple[SourceStatus, str | None]:
    if excluded_reason:
        return "excluded", excluded_reason
    if any(stage.freshness == "stale" for stage in stages):
        return "stale", None
    materialize_stage = next((stage for stage in stages if stage.stage == "materialize"), None)
    propagate_stage = next((stage for stage in stages if stage.stage == "propagate"), None)
    if (
        materialize_stage is not None
        and materialize_stage.status == "completed"
        and propagate_stage is not None
        and propagate_stage.status != "completed"
        and propagate_stage.summary
    ):
        return "incomplete", propagate_stage.summary or "propagate stage is incomplete"
    if page_path and Path(page_path).exists():
        return "materialized", None
    if _earliest_resume_stage(stages) is not None:
        return "incomplete", None
    if has_any_artifacts:
        return "blocked", "missing required reusable artifacts"
    return "unseen", "missing acquisition cache"


def _filter_upstream_items(request: InventoryRequest, items: list[Any], *, matcher) -> list[Any]:
    if not request.source_ids and not request.external_ids:
        return items
    wanted_source_ids = set(request.source_ids)
    wanted_external_ids = set(request.external_ids)
    filtered: list[Any] = []
    for item in items:
        aliases = set(matcher(item))
        if wanted_source_ids and aliases & wanted_source_ids:
            filtered.append(item)
            continue
        if wanted_external_ids and aliases & wanted_external_ids:
            filtered.append(item)
    return filtered


def _read_cache_payload(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _probe_llm_cache_path(path: Path | None, expected: list[Any] | Any) -> tuple[StageStatus, StageFreshness]:
    if path is None or not path.exists():
        return "missing", "missing"
    cached = load_llm_cache(path, expected=expected)
    if cached is not None:
        return "completed", "fresh"
    return "missing", "stale"


def _probe_book_source_cache(path: Path | None) -> tuple[StageStatus, StageFreshness]:
    if path is None or not path.exists():
        return "missing", "missing"
    payload = _read_cache_payload(path)
    if not isinstance(payload, dict):
        return "missing", "stale"
    identity = payload.get("_llm")
    data = payload.get("data")
    if not isinstance(identity, dict) or not isinstance(data, dict):
        return "missing", "stale"
    source_kind = str(data.get("source_kind") or "")
    if source_kind not in {"document", "audio"}:
        return "missing", "stale"
    identities = reingest_service.get_llm_service().cache_identities(
        task_class="document" if source_kind == "document" else "transcription",
        prompt_version=f"books.source-grounded.segmented.{source_kind}.v1",
    )
    cached = load_llm_cache(path, expected=identities)
    if isinstance(cached, dict):
        return "completed", "fresh"
    return "missing", "stale"


def _stage_from_cache_probe(stage: str, status: StageStatus, freshness: StageFreshness, *, artifact_path: Path | None = None, summary: str | None = None) -> StageProbeState:
    return StageProbeState(stage=stage, status=status, freshness=freshness, artifact_path=str(artifact_path) if artifact_path else None, summary=summary)


def _book_aliases_from_slug(slug: str) -> tuple[str, ...]:
    return (str(SourceKey(f"book:slug:{slug}")), f"book-{slug}", slug)


class BooksAdapter:
    lane = "books"
    adapter = "books"
    receipt_lane = "book"

    def enumerate_upstream(self, request: InventoryRequest, repo_root: Path) -> list[Any]:
        normalized = reingest_service.ReingestRequest(
            lane="books",
            path=request.path.resolve() if request.path is not None else None,
            today=request.today,
            source_ids=(),
            limit=request.limit,
            dry_run=True,
        )
        items = reingest_service._inventory_book_items(normalized, repo_root)
        return _filter_upstream_items(request, items, matcher=lambda item: self.upstream_aliases(item))

    def enumerate_page_backed(self, repo_root: Path) -> list[PageBackedCandidate]:
        root = Vault.load(repo_root).wiki / "sources" / "books"
        if not root.exists():
            return []
        candidates: list[PageBackedCandidate] = []
        for path in sorted(root.rglob("*.md")):
            frontmatter = _frontmatter(path)
            page_stem = path.stem
            source_date = str(frontmatter.get("source_date") or frontmatter.get("ingested") or "")[:10]
            external_id = str(frontmatter.get("external_id") or "").strip()
            source_id = f"book-{page_stem}"
            source_key = SourceKey(f"book:audible:{external_id.removeprefix('audible-')}") if external_id.startswith("audible-") else SourceKey(f"book:slug:{page_stem}")
            aliases = tuple(
                alias
                for alias in dict.fromkeys([str(source_key), source_id, external_id, page_stem, str(frontmatter.get("id") or "").strip()])
                if alias
            )
            candidates.append(
                PageBackedCandidate(
                    source_key=source_key,
                    lane=self.lane,
                    adapter=self.adapter,
                    title=str(frontmatter.get("title") or page_stem),
                    source_date=source_date,
                    aliases=aliases,
                    page_path=str(path),
                    source_id=source_id,
                    external_id=external_id or None,
                )
            )
        return candidates

    def enumerate_cache_only(self, repo_root: Path) -> list[CacheOnlyCandidate]:
        research_root = raw_path(repo_root, "research", "books")
        transcript_root = raw_path(repo_root, "transcripts", "book")
        grouped: dict[str, dict[str, Path]] = {}

        def remember(slug: str, kind: str, path: Path) -> None:
            grouped.setdefault(slug, {})[kind] = path

        suffixes = {
            ".classification.json": "classification",
            ".source.json": "source",
            ".deep.json": "deep",
            ".summary.json": "summary",
            ".applied.json": "applied",
            ".stance.json": "stance",
        }
        if research_root.exists():
            for path in sorted(research_root.glob("*.json")):
                for suffix, kind in suffixes.items():
                    if path.name.endswith(suffix):
                        remember(path.name.removesuffix(suffix), kind, path)
                        break
        if transcript_root.exists():
            for path in sorted(transcript_root.glob("*.json")):
                if path.name.endswith(".quality.json"):
                    remember(path.name.removesuffix(".quality.json").removeprefix("book-"), "quality", path)
                elif path.name.endswith(".pass_d.json"):
                    remember(path.name.removesuffix(".pass_d.json").removeprefix("book-"), "pass_d", path)

        candidates: list[CacheOnlyCandidate] = []
        page_keys = {candidate.source_key for candidate in self.enumerate_page_backed(repo_root)}
        for slug, paths in grouped.items():
            source_key = SourceKey(f"book:slug:{slug}")
            if source_key in page_keys:
                continue
            aliases = _book_aliases_from_slug(slug)
            classification_status, classification_freshness = _probe_llm_cache_path(
                paths.get("classification"),
                reingest_service.get_llm_service().cache_identities(
                    task_class="classification",
                    prompt_version=books_enrich.CLASSIFY_BOOK_PROMPT_VERSION,
                ),
            ) if "classification" in paths else ("missing", "missing")
            source_status, source_freshness = _probe_book_source_cache(paths.get("source"))
            deep_status, deep_freshness = _probe_llm_cache_path(
                paths.get("deep"),
                reingest_service.get_llm_service().cache_identities(
                    task_class="research",
                    prompt_version=books_enrich.RESEARCH_BOOK_DEEP_PROMPT_VERSION,
                ),
            ) if "deep" in paths else ("missing", "missing")
            acquire_status = "completed" if classification_status == "completed" and (source_status == "completed" or deep_status == "completed") else "missing"
            acquire_freshness: StageFreshness = "fresh"
            if "stale" in {classification_freshness, source_freshness, deep_freshness}:
                acquire_freshness = "stale"
            elif classification_freshness == "missing" and source_freshness == "missing" and deep_freshness == "missing":
                acquire_freshness = "missing"
            summary_status, summary_freshness = _probe_llm_cache_path(
                paths.get("summary"),
                reingest_service.get_llm_service().cache_identities(
                    task_class="summary",
                    prompt_version=books_enrich.SUMMARIZE_BOOK_RESEARCH_PROMPT_VERSION,
                ),
            ) if "summary" in paths else ("missing", "missing")
            applied_status, applied_freshness = _probe_llm_cache_path(
                paths.get("applied"),
                reingest_service.get_llm_service().cache_identities(
                    task_class="personalization",
                    prompt_version=books_enrich.APPLIED_TO_YOU_PROMPT_VERSION,
                ),
            ) if "applied" in paths else ("missing", "missing")
            stance_status, stance_freshness = _probe_llm_cache_path(
                paths.get("stance"),
                reingest_service._stance_identities(),
            ) if "stance" in paths else ("missing", "missing")
            pass_d_status, pass_d_freshness = _probe_llm_cache_path(
                paths.get("pass_d"),
                reingest_service._pass_d_identities(),
            ) if "pass_d" in paths else ("missing", "missing")
            source_id = f"book-{slug}"
            stages = (
                _stage_from_cache_probe("acquire", acquire_status, acquire_freshness, artifact_path=paths.get("source") or paths.get("deep")),
                _stage_from_cache_probe("pass_a", summary_status, summary_freshness, artifact_path=paths.get("summary")),
                _stage_from_cache_probe("pass_b", applied_status, applied_freshness, artifact_path=paths.get("applied")),
                _stage_from_cache_probe("pass_c", stance_status, stance_freshness, artifact_path=paths.get("stance")),
                _stage_from_cache_probe("pass_d", pass_d_status, pass_d_freshness, artifact_path=paths.get("pass_d")),
                _materialize_stage(None),
                _propagate_stage(repo_root, lane=self.receipt_lane, source_id=source_id),
            )
            artifacts = _artifacts([(kind, path) for kind, path in paths.items()])
            source_date = _source_date_from_receipt(paths.get("quality"))
            candidates.append(
                CacheOnlyCandidate(
                    source_key=source_key,
                    lane=self.lane,
                    adapter=self.adapter,
                    title=_title_from_slug(slug),
                    source_date=source_date,
                    aliases=aliases,
                    source_id=source_id,
                    external_id=None,
                    stage_states=stages,
                    artifacts=artifacts,
                    metadata={"cache_slug": slug},
                )
            )
        return candidates

    def upstream_aliases(self, lane_item: Any) -> tuple[str, ...]:
        book = lane_item.payload
        asin = str(getattr(book, "asin", "") or "").strip()
        source_key = f"book:audible:{asin}" if asin else f"book:slug:{lane_item.source_id.removeprefix('book-')}"
        external_id = f"audible-{asin}" if asin else ""
        return tuple(
            alias
            for alias in dict.fromkeys([source_key, lane_item.source_id, external_id, asin, lane_item.source_id.removeprefix("book-")])
            if alias
        )

    def build_inventory_from_upstream(self, lane_item: Any, *, request: InventoryRequest, page_candidate: PageBackedCandidate | None, cache_candidate: CacheOnlyCandidate | None, repo_root: Path, registry: SourceRegistry | None) -> InventoryItem:
        book = lane_item.payload
        source_key = SourceKey(self.upstream_aliases(lane_item)[0])
        aliases = tuple(dict.fromkeys([*self.upstream_aliases(lane_item), *((page_candidate.aliases if page_candidate else ())), *((cache_candidate.aliases if cache_candidate else ())) ]))
        classification_probe = reingest_service._probe_book_classification_cache(repo_root, book)
        source_grounded_probe = reingest_service._probe_book_source_grounded_cache(repo_root, book)
        deep_probe = reingest_service._probe_book_deep_research_cache(repo_root, book)
        acquire_probe = reingest_service._combine_probes(classification_probe, reingest_service._choose_probe(source_grounded_probe, deep_probe))
        pass_a_probe = reingest_service._probe_book_pass_a_cache(repo_root, book, source_grounded_probe)
        pass_b_probe = reingest_service._probe_llm_cache(books_enrich.applied_path(repo_root, book), reingest_service._personalization_identities())
        pass_c_probe = reingest_service._probe_llm_cache(books_enrich.attribute_cache_path(repo_root, book), reingest_service._stance_identities())
        pass_d_probe = reingest_service._probe_llm_cache(
            reingest_service.pass_d_cache_path(repo_root=repo_root, source_kind="book", source_id=lane_item.source_id),
            reingest_service._pass_d_identities(),
        )
        page_path = page_candidate.page_path if page_candidate else None
        stages = (
            _stage_from_probe("acquire", acquire_probe, artifact_path=books_enrich.source_research_path(repo_root, book) if source_grounded_probe.reusable or source_grounded_probe.stale else books_enrich.deep_research_path(repo_root, book)),
            _stage_from_probe("pass_a", pass_a_probe, artifact_path=books_enrich.summary_path(repo_root, book)),
            _stage_from_probe("pass_b", pass_b_probe, artifact_path=books_enrich.applied_path(repo_root, book)),
            _stage_from_probe("pass_c", pass_c_probe, artifact_path=books_enrich.attribute_cache_path(repo_root, book)),
            _stage_from_probe("pass_d", pass_d_probe, artifact_path=reingest_service.pass_d_cache_path(repo_root=repo_root, source_kind="book", source_id=lane_item.source_id)),
            _materialize_stage(page_path),
            _propagate_stage(repo_root, lane=self.receipt_lane, source_id=lane_item.source_id),
        )
        classification = reingest_service._load_book_classification_cache(repo_root, book) or {}
        excluded_reason = None
        if classification and not should_materialize(classification):
            excluded_reason = f"excluded by content policy ({classification.get('category') or classification.get('retention') or 'exclude'})"
        artifacts = _artifacts(
            [
                ("classification", books_enrich.classification_path(repo_root, book)),
                ("source_research", books_enrich.source_research_path(repo_root, book)),
                ("deep_research", books_enrich.deep_research_path(repo_root, book)),
                ("summary", books_enrich.summary_path(repo_root, book)),
                ("applied", books_enrich.applied_path(repo_root, book)),
                ("stance", books_enrich.attribute_cache_path(repo_root, book)),
                ("pass_d_cache", reingest_service.pass_d_cache_path(repo_root=repo_root, source_kind="book", source_id=lane_item.source_id)),
                ("page", Path(page_path) if page_path else None),
                ("quality_receipt", quality_receipt_path(repo_root=repo_root, lane=self.receipt_lane, source_id=lane_item.source_id)),
            ]
        )
        status, blocked_reason = _classify_status(stages=stages, page_path=page_path, excluded_reason=excluded_reason, has_any_artifacts=any(a.exists for a in artifacts))
        return InventoryItem(
            source_key=source_key,
            lane=self.lane,
            adapter=self.adapter,
            title=lane_item.label,
            source_date=str(getattr(book, "finished_date", "") or getattr(book, "started_date", "") or "")[:10],
            status=status,
            aliases=aliases,
            canonical_page_path=page_path,
            stage_states=stages,
            artifacts=artifacts,
            source_id=lane_item.source_id,
            external_id=f"audible-{book.asin}" if getattr(book, "asin", "") else None,
            blocked_reason=blocked_reason,
            excluded_reason=excluded_reason,
            registry_status=_registry_status(registry, source_key),
            metadata={"source_label": lane_item.source_label},
            payload=book,
        )

    def build_inventory_from_page(self, candidate: PageBackedCandidate, *, repo_root: Path, registry: SourceRegistry | None) -> InventoryItem:
        receipt = load_quality_receipt(repo_root=repo_root, lane=self.receipt_lane, source_id=candidate.source_id or str(candidate.source_key))
        pass_d_path = raw_path(repo_root, "transcripts", self.receipt_lane, f"{candidate.source_id or candidate.source_key}.pass_d.json")
        stages = (
            _stage_missing("acquire"),
            _stage_missing("pass_a"),
            _stage_missing("pass_b"),
            _stage_missing("pass_c"),
            _stage_completed("pass_d", artifact_path=pass_d_path, summary="pass_d cache present") if pass_d_path.exists() else _stage_missing("pass_d", artifact_path=pass_d_path),
            _stage_completed("materialize", artifact_path=Path(candidate.page_path), summary="canonical page present"),
            _propagate_stage(repo_root, lane=self.receipt_lane, source_id=candidate.source_id or str(candidate.source_key)),
        )
        artifacts = [SourceArtifact("page", candidate.page_path, _file_fingerprint(Path(candidate.page_path)), True)]
        if receipt is not None:
            receipt_path = quality_receipt_path(repo_root=repo_root, lane=self.receipt_lane, source_id=candidate.source_id or str(candidate.source_key))
            artifacts.append(SourceArtifact("quality_receipt", str(receipt_path), _file_fingerprint(receipt_path), True))
        if pass_d_path.exists():
            artifacts.append(SourceArtifact("pass_d_cache", str(pass_d_path), _file_fingerprint(pass_d_path), True))
        status, blocked_reason = _classify_status(
            stages=stages,
            page_path=candidate.page_path,
            has_any_artifacts=any(artifact.exists for artifact in artifacts),
        )
        return InventoryItem(
            source_key=candidate.source_key,
            lane=self.lane,
            adapter=self.adapter,
            title=candidate.title,
            source_date=candidate.source_date,
            status=status,
            aliases=candidate.aliases,
            canonical_page_path=candidate.page_path,
            stage_states=stages,
            artifacts=tuple(artifacts),
            source_id=candidate.source_id,
            external_id=candidate.external_id,
            blocked_reason=blocked_reason,
            registry_status=_registry_status(registry, candidate.source_key),
            metadata={},
        )

    def build_inventory_from_cache(self, candidate: CacheOnlyCandidate, *, repo_root: Path, registry: SourceRegistry | None) -> InventoryItem:
        status, blocked_reason = _classify_status(
            stages=candidate.stage_states,
            page_path=None,
            has_any_artifacts=any(artifact.exists for artifact in candidate.artifacts),
        )
        return InventoryItem(
            source_key=candidate.source_key,
            lane=self.lane,
            adapter=self.adapter,
            title=candidate.title,
            source_date=candidate.source_date,
            status=status,
            aliases=candidate.aliases,
            canonical_page_path=None,
            stage_states=candidate.stage_states,
            artifacts=candidate.artifacts,
            source_id=candidate.source_id,
            external_id=candidate.external_id,
            blocked_reason=blocked_reason,
            registry_status=_registry_status(registry, candidate.source_key),
            metadata=candidate.metadata,
        )


class YouTubeAdapter:
    lane = "youtube"
    adapter = "youtube"
    receipt_lane = "youtube"

    def enumerate_upstream(self, request: InventoryRequest, repo_root: Path) -> list[Any]:
        normalized = reingest_service.ReingestRequest(
            lane="youtube",
            path=request.path.resolve() if request.path is not None else None,
            today=request.today,
            source_ids=(),
            limit=request.limit,
            dry_run=True,
        )
        items = reingest_service._inventory_youtube_items(normalized, repo_root)
        return _filter_upstream_items(request, items, matcher=lambda item: self.upstream_aliases(item))

    def enumerate_page_backed(self, repo_root: Path) -> list[PageBackedCandidate]:
        root = Vault.load(repo_root).wiki / "sources" / "youtube"
        if not root.exists():
            return []
        candidates: list[PageBackedCandidate] = []
        for path in sorted(root.rglob("*.md")):
            frontmatter = _frontmatter(path)
            external_id = str(frontmatter.get("external_id") or "").strip()
            page_stem = path.stem
            video_id = str(frontmatter.get("youtube_id") or "").strip() or external_id.removeprefix("youtube-")
            source_key = SourceKey(f"youtube:video:{video_id or page_stem}")
            source_id = f"youtube-{video_id}" if video_id else None
            aliases = tuple(alias for alias in dict.fromkeys([str(source_key), source_id or "", external_id, video_id, page_stem, str(frontmatter.get("id") or "").strip()]) if alias)
            candidates.append(PageBackedCandidate(source_key, self.lane, self.adapter, str(frontmatter.get("title") or page_stem), str(frontmatter.get("source_date") or frontmatter.get("ingested") or "")[:10], aliases, str(path), source_id, external_id or None))
        return candidates

    def enumerate_cache_only(self, repo_root: Path) -> list[CacheOnlyCandidate]:
        root = raw_path(repo_root, "transcripts", "youtube")
        if not root.exists():
            return []
        grouped: dict[str, dict[str, Path]] = {}
        def remember(video_id: str, kind: str, path: Path) -> None:
            grouped.setdefault(video_id, {})[kind] = path
        for path in sorted(root.glob("*")):
            name = path.name
            if name.endswith(".classification.json"):
                remember(name.removesuffix(".classification.json"), "classification", path)
            elif name.endswith(".transcript.txt"):
                remember(name.removesuffix(".transcript.txt"), "raw_transcript", path)
            elif name.endswith(".transcription.json"):
                remember(name.removesuffix(".transcription.json"), "transcription", path)
            elif name.endswith(".applied.json"):
                remember(name.removesuffix(".applied.json"), "applied", path)
            elif name.endswith(".stance.json"):
                remember(name.removesuffix(".stance.json"), "stance", path)
            elif name.endswith(".quality.json") and name.startswith("youtube-"):
                remember(name.removesuffix(".quality.json").removeprefix("youtube-"), "quality", path)
            elif name.endswith(".pass_d.json") and name.startswith("youtube-"):
                remember(name.removesuffix(".pass_d.json").removeprefix("youtube-"), "pass_d", path)
            elif name.endswith(".json") and not any(name.endswith(suffix) for suffix in (".classification.json", ".transcription.json", ".applied.json", ".stance.json", ".quality.json", ".pass_d.json")):
                remember(name.removesuffix(".json"), "summary", path)
        page_keys = {candidate.source_key for candidate in self.enumerate_page_backed(repo_root)}
        candidates: list[CacheOnlyCandidate] = []
        for video_id, paths in grouped.items():
            source_key = SourceKey(f"youtube:video:{video_id}")
            if source_key in page_keys:
                continue
            source_id = f"youtube-{video_id}"
            aliases = tuple(alias for alias in dict.fromkeys([str(source_key), source_id, video_id]) if alias)
            classification_status, classification_freshness = _probe_llm_cache_path(
                paths.get("classification"),
                reingest_service.get_llm_service().cache_identities(task_class="classification", prompt_version=youtube_enrich.CLASSIFY_VIDEO_PROMPT_VERSION),
            ) if "classification" in paths else ("missing", "missing")
            raw_transcript_path = paths.get("raw_transcript")
            transcript_status: StageStatus = "completed" if raw_transcript_path and raw_transcript_path.exists() else "missing"
            transcript_freshness: StageFreshness = "fresh" if transcript_status == "completed" else "missing"
            acquire_status: StageStatus = "completed" if classification_status == "completed" and transcript_status == "completed" else "missing"
            acquire_freshness: StageFreshness = "stale" if "stale" in {classification_freshness, transcript_freshness} else ("fresh" if acquire_status == "completed" else "missing")
            summary_status, summary_freshness = _probe_llm_cache_path(
                paths.get("summary"),
                reingest_service._summary_identities(youtube_enrich.SUMMARIZE_TRANSCRIPT_PROMPT_VERSION),
            ) if "summary" in paths else ("missing", "missing")
            applied_status, applied_freshness = _probe_llm_cache_path(paths.get("applied"), reingest_service._personalization_identities()) if "applied" in paths else ("missing", "missing")
            stance_status, stance_freshness = _probe_llm_cache_path(paths.get("stance"), reingest_service._stance_identities()) if "stance" in paths else ("missing", "missing")
            pass_d_status, pass_d_freshness = _probe_llm_cache_path(paths.get("pass_d"), reingest_service._pass_d_identities()) if "pass_d" in paths else ("missing", "missing")
            stages = (
                _stage_from_cache_probe("acquire", acquire_status, acquire_freshness, artifact_path=raw_transcript_path),
                _stage_from_cache_probe("pass_a", summary_status, summary_freshness, artifact_path=paths.get("summary")),
                _stage_from_cache_probe("pass_b", applied_status, applied_freshness, artifact_path=paths.get("applied")),
                _stage_from_cache_probe("pass_c", stance_status, stance_freshness, artifact_path=paths.get("stance")),
                _stage_from_cache_probe("pass_d", pass_d_status, pass_d_freshness, artifact_path=paths.get("pass_d")),
                _materialize_stage(None),
                _propagate_stage(repo_root, lane=self.receipt_lane, source_id=source_id),
            )
            candidates.append(CacheOnlyCandidate(source_key, self.lane, self.adapter, _title_from_slug(video_id), _source_date_from_receipt(paths.get("quality")), aliases, source_id, f"youtube-{video_id}", stages, _artifacts([(kind, path) for kind, path in paths.items()]), {"cache_video_id": video_id}))
        return candidates

    def upstream_aliases(self, lane_item: Any) -> tuple[str, ...]:
        video_id = str(getattr(lane_item.payload, "video_id", "") or "").strip()
        source_key = f"youtube:video:{video_id}"
        external_id = f"youtube-{video_id}" if video_id else ""
        return tuple(alias for alias in dict.fromkeys([source_key, lane_item.source_id, external_id, video_id]) if alias)

    def build_inventory_from_upstream(self, lane_item: Any, *, request: InventoryRequest, page_candidate: PageBackedCandidate | None, cache_candidate: CacheOnlyCandidate | None, repo_root: Path, registry: SourceRegistry | None) -> InventoryItem:
        record = lane_item.payload
        source_key = SourceKey(self.upstream_aliases(lane_item)[0])
        aliases = tuple(dict.fromkeys([*self.upstream_aliases(lane_item), *((page_candidate.aliases if page_candidate else ())), *((cache_candidate.aliases if cache_candidate else ())) ]))
        classification_probe = reingest_service._probe_youtube_classification_cache(repo_root, record)
        transcript_probe = reingest_service._probe_raw_file(youtube_enrich.raw_transcript_path(repo_root, record.video_id))
        pass_a_probe = reingest_service._probe_llm_cache(youtube_enrich.transcript_path(repo_root, record.video_id), reingest_service._summary_identities(youtube_enrich.SUMMARIZE_TRANSCRIPT_PROMPT_VERSION))
        pass_b_probe = reingest_service._probe_llm_cache(youtube_enrich.applied_path(repo_root, record.video_id), reingest_service._personalization_identities())
        pass_c_probe = reingest_service._probe_youtube_pass_c(repo_root, record)
        pass_d_probe = reingest_service._probe_llm_cache(reingest_service.pass_d_cache_path(repo_root=repo_root, source_kind="youtube", source_id=lane_item.source_id), reingest_service._pass_d_identities())
        page_path = page_candidate.page_path if page_candidate else None
        stages = (
            _stage_from_probe("acquire", reingest_service._combine_probes(classification_probe, transcript_probe), artifact_path=youtube_enrich.raw_transcript_path(repo_root, record.video_id)),
            _stage_from_probe("pass_a", pass_a_probe, artifact_path=youtube_enrich.transcript_path(repo_root, record.video_id)),
            _stage_from_probe("pass_b", pass_b_probe, artifact_path=youtube_enrich.applied_path(repo_root, record.video_id)),
            _stage_from_probe("pass_c", pass_c_probe, artifact_path=youtube_enrich.attribute_cache_path(repo_root, record.video_id)),
            _stage_from_probe("pass_d", pass_d_probe, artifact_path=reingest_service.pass_d_cache_path(repo_root=repo_root, source_kind="youtube", source_id=lane_item.source_id)),
            _materialize_stage(page_path),
            _propagate_stage(repo_root, lane=self.receipt_lane, source_id=lane_item.source_id),
        )
        classification = reingest_service._load_youtube_classification_cache(repo_root, record) or {}
        excluded_reason = None
        duration_override = request.lane_options.get("default_duration_minutes")
        if youtube_filter.should_skip_record(record, duration_minutes_override=duration_override):
            excluded_reason = "excluded by cheap YouTube filter"
        elif classification and not should_materialize(classification):
            excluded_reason = f"excluded by content policy ({classification.get('category') or classification.get('retention') or 'exclude'})"
        artifacts = _artifacts([
            ("classification", youtube_enrich.classification_path(repo_root, record.video_id)),
            ("raw_transcript", youtube_enrich.raw_transcript_path(repo_root, record.video_id)),
            ("transcription_payload", youtube_enrich.transcription_payload_path(repo_root, record.video_id)),
            ("summary", youtube_enrich.transcript_path(repo_root, record.video_id)),
            ("applied", youtube_enrich.applied_path(repo_root, record.video_id)),
            ("stance", youtube_enrich.attribute_cache_path(repo_root, record.video_id)),
            ("pass_d_cache", reingest_service.pass_d_cache_path(repo_root=repo_root, source_kind="youtube", source_id=lane_item.source_id)),
            ("page", Path(page_path) if page_path else None),
            ("quality_receipt", quality_receipt_path(repo_root=repo_root, lane=self.receipt_lane, source_id=lane_item.source_id)),
        ])
        status, blocked_reason = _classify_status(stages=stages, page_path=page_path, excluded_reason=excluded_reason, has_any_artifacts=any(a.exists for a in artifacts))
        return InventoryItem(source_key, self.lane, self.adapter, lane_item.label, str(getattr(record, "watched_at", "") or "")[:10], status, aliases, page_path, stages, artifacts, lane_item.source_id, f"youtube-{record.video_id}" if record.video_id else None, blocked_reason, excluded_reason, _registry_status(registry, source_key), {"source_label": lane_item.source_label}, record)

    def build_inventory_from_page(self, candidate: PageBackedCandidate, *, repo_root: Path, registry: SourceRegistry | None) -> InventoryItem:
        receipt = load_quality_receipt(repo_root=repo_root, lane=self.receipt_lane, source_id=candidate.source_id or str(candidate.source_key))
        pass_d_path = raw_path(repo_root, "transcripts", self.receipt_lane, f"{candidate.source_id or candidate.source_key}.pass_d.json")
        artifacts = [SourceArtifact("page", candidate.page_path, _file_fingerprint(Path(candidate.page_path)), True)]
        if receipt is not None:
            receipt_path = quality_receipt_path(repo_root=repo_root, lane=self.receipt_lane, source_id=candidate.source_id or str(candidate.source_key))
            artifacts.append(SourceArtifact("quality_receipt", str(receipt_path), _file_fingerprint(receipt_path), True))
        if pass_d_path.exists():
            artifacts.append(SourceArtifact("pass_d_cache", str(pass_d_path), _file_fingerprint(pass_d_path), True))
        stages = (
            _stage_missing("acquire"),
            _stage_missing("pass_a"),
            _stage_missing("pass_b"),
            _stage_missing("pass_c"),
            _stage_completed("pass_d", artifact_path=pass_d_path, summary="pass_d cache present") if pass_d_path.exists() else _stage_missing("pass_d"),
            _stage_completed("materialize", artifact_path=Path(candidate.page_path), summary="canonical page present"),
            _propagate_stage(repo_root, lane=self.receipt_lane, source_id=candidate.source_id or str(candidate.source_key)),
        )
        status, blocked_reason = _classify_status(
            stages=stages,
            page_path=candidate.page_path,
            has_any_artifacts=any(artifact.exists for artifact in artifacts),
        )
        return InventoryItem(candidate.source_key, self.lane, self.adapter, candidate.title, candidate.source_date, status, candidate.aliases, candidate.page_path, stages, tuple(artifacts), candidate.source_id, candidate.external_id, blocked_reason, None, _registry_status(registry, candidate.source_key), metadata={})

    def build_inventory_from_cache(self, candidate: CacheOnlyCandidate, *, repo_root: Path, registry: SourceRegistry | None) -> InventoryItem:
        status, blocked_reason = _classify_status(stages=candidate.stage_states, page_path=None, has_any_artifacts=any(a.exists for a in candidate.artifacts))
        return InventoryItem(candidate.source_key, self.lane, self.adapter, candidate.title, candidate.source_date, status, candidate.aliases, None, candidate.stage_states, candidate.artifacts, candidate.source_id, candidate.external_id, blocked_reason, None, _registry_status(registry, candidate.source_key), candidate.metadata)


class SubstackAdapter:
    lane = "substack"
    adapter = "substack"
    receipt_lane = "substack"

    def enumerate_upstream(self, request: InventoryRequest, repo_root: Path) -> list[Any]:
        normalized = reingest_service.ReingestRequest(lane="substack", path=request.path.resolve() if request.path is not None else None, today=request.today, source_ids=(), limit=request.limit, dry_run=True)
        items = reingest_service._inventory_substack_items(normalized, repo_root)
        return _filter_upstream_items(request, items, matcher=lambda item: self.upstream_aliases(item))

    def enumerate_page_backed(self, repo_root: Path) -> list[PageBackedCandidate]:
        root = Vault.load(repo_root).wiki / "sources" / "substack"
        if not root.exists():
            return []
        candidates: list[PageBackedCandidate] = []
        for path in sorted(root.rglob("*.md")):
            frontmatter = _frontmatter(path)
            external_id = str(frontmatter.get("external_id") or "").strip()
            page_stem = path.stem
            post_id = external_id.removeprefix("substack-") if external_id.startswith("substack-") else page_stem
            source_key = SourceKey(f"substack:post:{post_id}")
            source_id = f"substack-{post_id}"
            aliases = tuple(alias for alias in dict.fromkeys([str(source_key), source_id, external_id, post_id, page_stem, str(frontmatter.get("id") or "").strip()]) if alias)
            candidates.append(PageBackedCandidate(source_key, self.lane, self.adapter, str(frontmatter.get("title") or page_stem), str(frontmatter.get("source_date") or frontmatter.get("ingested") or "")[:10], aliases, str(path), source_id, external_id or None))
        return candidates

    def enumerate_cache_only(self, repo_root: Path) -> list[CacheOnlyCandidate]:
        root = raw_path(repo_root, "transcripts", "substack")
        if not root.exists():
            return []
        grouped: dict[str, dict[str, Path]] = {}
        def remember(post_id: str, kind: str, path: Path) -> None:
            grouped.setdefault(post_id, {})[kind] = path
        for path in sorted(root.glob("*")):
            name = path.name
            if name.endswith(".html"):
                remember(name.removesuffix(".html"), "html", path)
            elif name.endswith(".links.json"):
                remember(name.removesuffix(".links.json"), "links", path)
            elif name.endswith(".applied.json"):
                remember(name.removesuffix(".applied.json"), "applied", path)
            elif name.endswith(".stance.json"):
                remember(name.removesuffix(".stance.json"), "stance", path)
            elif name.endswith(".quality.json") and name.startswith("substack-"):
                remember(name.removesuffix(".quality.json").removeprefix("substack-"), "quality", path)
            elif name.endswith(".pass_d.json") and name.startswith("substack-"):
                remember(name.removesuffix(".pass_d.json").removeprefix("substack-"), "pass_d", path)
            elif name.endswith(".json") and not any(name.endswith(suffix) for suffix in (".links.json", ".applied.json", ".stance.json", ".quality.json", ".pass_d.json")):
                remember(name.removesuffix(".json"), "summary", path)
        page_keys = {candidate.source_key for candidate in self.enumerate_page_backed(repo_root)}
        candidates: list[CacheOnlyCandidate] = []
        for post_id, paths in grouped.items():
            source_key = SourceKey(f"substack:post:{post_id}")
            if source_key in page_keys:
                continue
            source_id = f"substack-{post_id}"
            aliases = tuple(alias for alias in dict.fromkeys([str(source_key), source_id, post_id]) if alias)
            html_status: StageStatus = "completed" if paths.get("html") and paths["html"].exists() else "missing"
            html_freshness: StageFreshness = "fresh" if html_status == "completed" else "missing"
            links_status, links_freshness = _probe_llm_cache_path(paths.get("links"), reingest_service.get_llm_service().cache_identities(task_class="classification", prompt_version=substack_enrich.CLASSIFY_LINKS_PROMPT_VERSION)) if "links" in paths else ("missing", "missing")
            summary_status, summary_freshness = _probe_llm_cache_path(paths.get("summary"), reingest_service._summary_identities(substack_enrich.SUMMARIZE_SUBSTACK_PROMPT_VERSION)) if "summary" in paths else ("missing", "missing")
            pass_a_status: StageStatus = "completed" if links_status == "completed" and summary_status == "completed" else "missing"
            pass_a_freshness: StageFreshness = "stale" if "stale" in {links_freshness, summary_freshness} else ("fresh" if pass_a_status == "completed" else "missing")
            applied_status, applied_freshness = _probe_llm_cache_path(paths.get("applied"), reingest_service._personalization_identities(substack=True)) if "applied" in paths else ("missing", "missing")
            stance_status, stance_freshness = _probe_llm_cache_path(paths.get("stance"), reingest_service._stance_identities()) if "stance" in paths else ("missing", "missing")
            pass_d_status, pass_d_freshness = _probe_llm_cache_path(paths.get("pass_d"), reingest_service._pass_d_identities()) if "pass_d" in paths else ("missing", "missing")
            stages = (
                _stage_from_cache_probe("acquire", html_status, html_freshness, artifact_path=paths.get("html")),
                _stage_from_cache_probe("pass_a", pass_a_status, pass_a_freshness, artifact_path=paths.get("summary")),
                _stage_from_cache_probe("pass_b", applied_status, applied_freshness, artifact_path=paths.get("applied")),
                _stage_from_cache_probe("pass_c", stance_status, stance_freshness, artifact_path=paths.get("stance")),
                _stage_from_cache_probe("pass_d", pass_d_status, pass_d_freshness, artifact_path=paths.get("pass_d")),
                _materialize_stage(None),
                _propagate_stage(repo_root, lane=self.receipt_lane, source_id=source_id),
            )
            candidates.append(CacheOnlyCandidate(source_key, self.lane, self.adapter, _title_from_slug(post_id), _source_date_from_receipt(paths.get("quality")), aliases, source_id, f"substack-{post_id}", stages, _artifacts([(kind, path) for kind, path in paths.items()]), {"cache_post_id": post_id}))
        return candidates

    def upstream_aliases(self, lane_item: Any) -> tuple[str, ...]:
        post_id = str(getattr(lane_item.payload, "id", "") or "").strip()
        url = str(getattr(lane_item.payload, "url", "") or "").strip()
        source_key = f"substack:post:{post_id}"
        return tuple(alias for alias in dict.fromkeys([source_key, lane_item.source_id, f"substack-{post_id}", post_id, url]) if alias)

    def build_inventory_from_upstream(self, lane_item: Any, *, request: InventoryRequest, page_candidate: PageBackedCandidate | None, cache_candidate: CacheOnlyCandidate | None, repo_root: Path, registry: SourceRegistry | None) -> InventoryItem:
        record = lane_item.payload
        source_key = SourceKey(self.upstream_aliases(lane_item)[0])
        aliases = tuple(dict.fromkeys([*self.upstream_aliases(lane_item), *((page_candidate.aliases if page_candidate else ())), *((cache_candidate.aliases if cache_candidate else ())) ]))
        acquisition_probe = reingest_service._probe_substack_acquisition_cache(repo_root, record)
        pass_a_probe = reingest_service._probe_substack_pass_a_cache(repo_root, record)
        pass_b_probe = reingest_service._probe_llm_cache(substack_enrich.applied_cache_path(repo_root, record.id), reingest_service._personalization_identities(substack=True))
        pass_c_probe = reingest_service._probe_llm_cache(substack_stance.stance_cache_path(repo_root, record.id), reingest_service._stance_identities())
        pass_d_probe = reingest_service._probe_llm_cache(reingest_service.pass_d_cache_path(repo_root=repo_root, source_kind="substack", source_id=lane_item.source_id), reingest_service._pass_d_identities())
        page_path = page_candidate.page_path if page_candidate else None
        stages = (
            _stage_from_probe("acquire", acquisition_probe, artifact_path=substack_enrich.html_cache_path(repo_root, record.id)),
            _stage_from_probe("pass_a", pass_a_probe, artifact_path=substack_enrich.summary_cache_path(repo_root, record.id)),
            _stage_from_probe("pass_b", pass_b_probe, artifact_path=substack_enrich.applied_cache_path(repo_root, record.id)),
            _stage_from_probe("pass_c", pass_c_probe, artifact_path=substack_stance.stance_cache_path(repo_root, record.id)),
            _stage_from_probe("pass_d", pass_d_probe, artifact_path=reingest_service.pass_d_cache_path(repo_root=repo_root, source_kind="substack", source_id=lane_item.source_id)),
            _materialize_stage(page_path),
            _propagate_stage(repo_root, lane=self.receipt_lane, source_id=lane_item.source_id),
        )
        artifacts = _artifacts([
            ("html", substack_enrich.html_cache_path(repo_root, record.id)),
            ("links", substack_enrich.links_cache_path(repo_root, record.id)),
            ("summary", substack_enrich.summary_cache_path(repo_root, record.id)),
            ("applied", substack_enrich.applied_cache_path(repo_root, record.id)),
            ("stance", substack_stance.stance_cache_path(repo_root, record.id)),
            ("pass_d_cache", reingest_service.pass_d_cache_path(repo_root=repo_root, source_kind="substack", source_id=lane_item.source_id)),
            ("page", Path(page_path) if page_path else None),
            ("quality_receipt", quality_receipt_path(repo_root=repo_root, lane=self.receipt_lane, source_id=lane_item.source_id)),
        ])
        status, blocked_reason = _classify_status(stages=stages, page_path=page_path, has_any_artifacts=any(a.exists for a in artifacts))
        return InventoryItem(source_key, self.lane, self.adapter, lane_item.label, str(getattr(record, "published_at", "") or getattr(record, "saved_at", "") or "")[:10], status, aliases, page_path, stages, artifacts, lane_item.source_id, f"substack-{record.id}" if getattr(record, "id", "") else None, blocked_reason, None, _registry_status(registry, source_key), {"source_label": lane_item.source_label}, record)

    def build_inventory_from_page(self, candidate: PageBackedCandidate, *, repo_root: Path, registry: SourceRegistry | None) -> InventoryItem:
        receipt = load_quality_receipt(repo_root=repo_root, lane=self.receipt_lane, source_id=candidate.source_id or str(candidate.source_key))
        pass_d_path = raw_path(repo_root, "transcripts", self.receipt_lane, f"{candidate.source_id or candidate.source_key}.pass_d.json")
        artifacts = [SourceArtifact("page", candidate.page_path, _file_fingerprint(Path(candidate.page_path)), True)]
        if receipt is not None:
            receipt_path = quality_receipt_path(repo_root=repo_root, lane=self.receipt_lane, source_id=candidate.source_id or str(candidate.source_key))
            artifacts.append(SourceArtifact("quality_receipt", str(receipt_path), _file_fingerprint(receipt_path), True))
        if pass_d_path.exists():
            artifacts.append(SourceArtifact("pass_d_cache", str(pass_d_path), _file_fingerprint(pass_d_path), True))
        stages = (
            _stage_missing("acquire"),
            _stage_missing("pass_a"),
            _stage_missing("pass_b"),
            _stage_missing("pass_c"),
            _stage_completed("pass_d", artifact_path=pass_d_path, summary="pass_d cache present") if pass_d_path.exists() else _stage_missing("pass_d"),
            _stage_completed("materialize", artifact_path=Path(candidate.page_path), summary="canonical page present"),
            _propagate_stage(repo_root, lane=self.receipt_lane, source_id=candidate.source_id or str(candidate.source_key)),
        )
        status, blocked_reason = _classify_status(
            stages=stages,
            page_path=candidate.page_path,
            has_any_artifacts=any(artifact.exists for artifact in artifacts),
        )
        return InventoryItem(candidate.source_key, self.lane, self.adapter, candidate.title, candidate.source_date, status, candidate.aliases, candidate.page_path, stages, tuple(artifacts), candidate.source_id, candidate.external_id, blocked_reason, None, _registry_status(registry, candidate.source_key), metadata={})

    def build_inventory_from_cache(self, candidate: CacheOnlyCandidate, *, repo_root: Path, registry: SourceRegistry | None) -> InventoryItem:
        status, blocked_reason = _classify_status(stages=candidate.stage_states, page_path=None, has_any_artifacts=any(a.exists for a in candidate.artifacts))
        return InventoryItem(candidate.source_key, self.lane, self.adapter, candidate.title, candidate.source_date, status, candidate.aliases, None, candidate.stage_states, candidate.artifacts, candidate.source_id, candidate.external_id, blocked_reason, None, _registry_status(registry, candidate.source_key), candidate.metadata)


class ArticlesAdapter:
    lane = "articles"
    adapter = "articles"
    receipt_lane = "article"

    def enumerate_upstream(self, request: InventoryRequest, repo_root: Path) -> list[Any]:
        normalized = reingest_service.ReingestRequest(lane="articles", path=request.path.resolve() if request.path is not None else None, today=request.today, source_ids=(), limit=request.limit, dry_run=True)
        items = reingest_service._inventory_article_items(normalized, repo_root)
        return _filter_upstream_items(request, items, matcher=lambda item: self.upstream_aliases(item))

    def enumerate_page_backed(self, repo_root: Path) -> list[PageBackedCandidate]:
        root = Vault.load(repo_root).wiki / "sources" / "articles"
        if not root.exists():
            return []
        candidates: list[PageBackedCandidate] = []
        for path in sorted(root.rglob("*.md")):
            frontmatter = _frontmatter(path)
            page_stem = path.stem
            source_key = SourceKey(f"article:url:{page_stem}")
            source_id = f"article-{page_stem}"
            aliases = tuple(alias for alias in dict.fromkeys([str(source_key), source_id, page_stem, str(frontmatter.get("id") or "").strip()]) if alias)
            candidates.append(PageBackedCandidate(source_key, self.lane, self.adapter, str(frontmatter.get("title") or page_stem), str(frontmatter.get("source_date") or frontmatter.get("ingested") or "")[:10], aliases, str(path), source_id, None))
        return candidates

    def enumerate_cache_only(self, repo_root: Path) -> list[CacheOnlyCandidate]:
        root = raw_path(repo_root, "transcripts", "articles")
        if not root.exists():
            return []
        grouped: dict[str, dict[str, Path]] = {}
        def remember(slug: str, kind: str, path: Path) -> None:
            grouped.setdefault(slug, {})[kind] = path
        for path in sorted(root.glob("*")):
            name = path.name
            if name.endswith(".html"):
                remember(name.removesuffix(".html"), "html", path)
            elif name.endswith(".meta.json"):
                remember(name.removesuffix(".meta.json"), "meta", path)
            elif name.endswith(".applied.json"):
                remember(name.removesuffix(".applied.json"), "applied", path)
            elif name.endswith(".stance.json"):
                remember(name.removesuffix(".stance.json"), "stance", path)
            elif name.endswith(".quality.json") and name.startswith("article-"):
                remember(name.removesuffix(".quality.json").removeprefix("article-"), "quality", path)
            elif name.endswith(".pass_d.json") and name.startswith("article-"):
                remember(name.removesuffix(".pass_d.json").removeprefix("article-"), "pass_d", path)
            elif name.endswith(".json") and not any(name.endswith(suffix) for suffix in (".meta.json", ".applied.json", ".stance.json", ".quality.json", ".pass_d.json")):
                remember(name.removesuffix(".json"), "summary", path)
        page_keys = {candidate.source_key for candidate in self.enumerate_page_backed(repo_root)}
        candidates: list[CacheOnlyCandidate] = []
        for slug, paths in grouped.items():
            source_key = SourceKey(f"article:url:{slug}")
            if source_key in page_keys:
                continue
            source_id = f"article-{slug}"
            aliases = tuple(alias for alias in dict.fromkeys([str(source_key), source_id, slug]) if alias)
            html_status: StageStatus = "completed" if paths.get("html") and paths["html"].exists() else "missing"
            html_freshness: StageFreshness = "fresh" if html_status == "completed" else "missing"
            summary_status, summary_freshness = _probe_llm_cache_path(paths.get("summary"), reingest_service._summary_identities("articles.summary.v1")) if "summary" in paths else ("missing", "missing")
            applied_status, applied_freshness = _probe_llm_cache_path(paths.get("applied"), reingest_service._personalization_identities()) if "applied" in paths else ("missing", "missing")
            stance_status, stance_freshness = _probe_llm_cache_path(paths.get("stance"), reingest_service._stance_identities()) if "stance" in paths else ("missing", "missing")
            pass_d_status, pass_d_freshness = _probe_llm_cache_path(paths.get("pass_d"), reingest_service._pass_d_identities()) if "pass_d" in paths else ("missing", "missing")
            stages = (
                _stage_from_cache_probe("acquire", html_status, html_freshness, artifact_path=paths.get("html")),
                _stage_from_cache_probe("pass_a", summary_status, summary_freshness, artifact_path=paths.get("summary")),
                _stage_from_cache_probe("pass_b", applied_status, applied_freshness, artifact_path=paths.get("applied")),
                _stage_from_cache_probe("pass_c", stance_status, stance_freshness, artifact_path=paths.get("stance")),
                _stage_from_cache_probe("pass_d", pass_d_status, pass_d_freshness, artifact_path=paths.get("pass_d")),
                _materialize_stage(None),
                _propagate_stage(repo_root, lane=self.receipt_lane, source_id=source_id),
            )
            candidates.append(CacheOnlyCandidate(source_key, self.lane, self.adapter, _title_from_slug(slug), _source_date_from_receipt(paths.get("quality")), aliases, source_id, None, stages, _artifacts([(kind, path) for kind, path in paths.items()]), {"cache_slug": slug}))
        return candidates

    def upstream_aliases(self, lane_item: Any) -> tuple[str, ...]:
        entry = lane_item.payload
        slug = lane_item.source_id.removeprefix("article-")
        return tuple(alias for alias in dict.fromkeys([f"article:url:{slug}", lane_item.source_id, str(getattr(entry, "url", "") or "").strip(), slug]) if alias)

    def build_inventory_from_upstream(self, lane_item: Any, *, request: InventoryRequest, page_candidate: PageBackedCandidate | None, cache_candidate: CacheOnlyCandidate | None, repo_root: Path, registry: SourceRegistry | None) -> InventoryItem:
        entry = lane_item.payload
        source_key = SourceKey(self.upstream_aliases(lane_item)[0])
        aliases = tuple(dict.fromkeys([*self.upstream_aliases(lane_item), *((page_candidate.aliases if page_candidate else ())), *((cache_candidate.aliases if cache_candidate else ())) ]))
        acquisition_probe = reingest_service._probe_article_fetch_cache(repo_root, entry)
        pass_a_probe = reingest_service._probe_llm_cache(articles_enrich.summary_cache_path(repo_root, entry), reingest_service._summary_identities("articles.summary.v1"))
        pass_b_probe = reingest_service._probe_llm_cache(articles_enrich.applied_cache_path(repo_root, entry), reingest_service._personalization_identities())
        pass_c_probe = reingest_service._probe_article_pass_c(repo_root, entry)
        pass_d_probe = reingest_service._probe_llm_cache(reingest_service.pass_d_cache_path(repo_root=repo_root, source_kind="article", source_id=lane_item.source_id), reingest_service._pass_d_identities())
        page_path = page_candidate.page_path if page_candidate else None
        slug = lane_item.source_id.removeprefix("article-")
        stages = (
            _stage_from_probe("acquire", acquisition_probe, artifact_path=article_html_cache_path(repo_root, slug)),
            _stage_from_probe("pass_a", pass_a_probe, artifact_path=articles_enrich.summary_cache_path(repo_root, entry)),
            _stage_from_probe("pass_b", pass_b_probe, artifact_path=articles_enrich.applied_cache_path(repo_root, entry)),
            _stage_from_probe("pass_c", pass_c_probe, artifact_path=articles_enrich.attribute_cache_path(repo_root, entry)),
            _stage_from_probe("pass_d", pass_d_probe, artifact_path=reingest_service.pass_d_cache_path(repo_root=repo_root, source_kind="article", source_id=lane_item.source_id)),
            _materialize_stage(page_path),
            _propagate_stage(repo_root, lane=self.receipt_lane, source_id=lane_item.source_id),
        )
        artifacts = _artifacts([
            ("html", article_html_cache_path(repo_root, slug)),
            ("summary", articles_enrich.summary_cache_path(repo_root, entry)),
            ("applied", articles_enrich.applied_cache_path(repo_root, entry)),
            ("stance", articles_enrich.attribute_cache_path(repo_root, entry)),
            ("pass_d_cache", reingest_service.pass_d_cache_path(repo_root=repo_root, source_kind="article", source_id=lane_item.source_id)),
            ("page", Path(page_path) if page_path else None),
            ("quality_receipt", quality_receipt_path(repo_root=repo_root, lane=self.receipt_lane, source_id=lane_item.source_id)),
        ])
        excluded_reason = None
        if not article_fetch.is_supported_article_url(entry.url):
            excluded_reason = (
                "excluded non-article URL from YouTube description fanout"
                if entry.source_type == "youtube-description"
                else "excluded unsupported URL from article extraction"
            )
        status, blocked_reason = _classify_status(
            stages=stages,
            page_path=page_path,
            excluded_reason=excluded_reason,
            has_any_artifacts=any(a.exists for a in artifacts),
        )
        return InventoryItem(source_key, self.lane, self.adapter, lane_item.label, str(getattr(entry, "discovered_at", "") or "")[:10], status, aliases, page_path, stages, artifacts, lane_item.source_id, None, blocked_reason, excluded_reason, _registry_status(registry, source_key), {"source_label": lane_item.source_label}, entry)

    def build_inventory_from_page(self, candidate: PageBackedCandidate, *, repo_root: Path, registry: SourceRegistry | None) -> InventoryItem:
        receipt = load_quality_receipt(repo_root=repo_root, lane=self.receipt_lane, source_id=candidate.source_id or str(candidate.source_key))
        pass_d_path = raw_path(repo_root, "transcripts", self.receipt_lane, f"{candidate.source_id or candidate.source_key}.pass_d.json")
        artifacts = [SourceArtifact("page", candidate.page_path, _file_fingerprint(Path(candidate.page_path)), True)]
        if receipt is not None:
            receipt_path = quality_receipt_path(repo_root=repo_root, lane=self.receipt_lane, source_id=candidate.source_id or str(candidate.source_key))
            artifacts.append(SourceArtifact("quality_receipt", str(receipt_path), _file_fingerprint(receipt_path), True))
        if pass_d_path.exists():
            artifacts.append(SourceArtifact("pass_d_cache", str(pass_d_path), _file_fingerprint(pass_d_path), True))
        stages = (
            _stage_missing("acquire"),
            _stage_missing("pass_a"),
            _stage_missing("pass_b"),
            _stage_missing("pass_c"),
            _stage_completed("pass_d", artifact_path=pass_d_path, summary="pass_d cache present") if pass_d_path.exists() else _stage_missing("pass_d"),
            _stage_completed("materialize", artifact_path=Path(candidate.page_path), summary="canonical page present"),
            _propagate_stage(repo_root, lane=self.receipt_lane, source_id=candidate.source_id or str(candidate.source_key)),
        )
        status, blocked_reason = _classify_status(
            stages=stages,
            page_path=candidate.page_path,
            has_any_artifacts=any(artifact.exists for artifact in artifacts),
        )
        return InventoryItem(candidate.source_key, self.lane, self.adapter, candidate.title, candidate.source_date, status, candidate.aliases, candidate.page_path, stages, tuple(artifacts), candidate.source_id, candidate.external_id, blocked_reason, None, _registry_status(registry, candidate.source_key), metadata={})

    def build_inventory_from_cache(self, candidate: CacheOnlyCandidate, *, repo_root: Path, registry: SourceRegistry | None) -> InventoryItem:
        status, blocked_reason = _classify_status(stages=candidate.stage_states, page_path=None, has_any_artifacts=any(a.exists for a in candidate.artifacts))
        return InventoryItem(candidate.source_key, self.lane, self.adapter, candidate.title, candidate.source_date, status, candidate.aliases, None, candidate.stage_states, candidate.artifacts, candidate.source_id, candidate.external_id, blocked_reason, None, _registry_status(registry, candidate.source_key), candidate.metadata)


ADAPTERS: dict[str, SourceLaneAdapter] = {
    "books": BooksAdapter(),
    "youtube": YouTubeAdapter(),
    "substack": SubstackAdapter(),
    "articles": ArticlesAdapter(),
}
