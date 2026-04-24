from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from scripts.chrome.scan import scan_chrome_profiles, write_scan_outputs
from scripts.articles.pipeline import drain_drop_queue
from scripts.audible.parse import parse_audible_library
from scripts.books import enrich as books_enrich
from scripts.books.parse import BookRecord, parse_csv, parse_markdown
from scripts.common.inbox_log import append_to_inbox_log
from scripts.common.vault import Vault
from scripts.substack import auth as substack_auth
from scripts.substack import enrich as substack_enrich
from scripts.substack import parse as substack_parse
from scripts.substack.pull import pull_saved
from scripts.search_signals.materialize import SearchSignalsIngestResult, ingest_search_signal_drop_files
from scripts.web_discovery.pipeline import (
    WebDiscoveryDrainResult,
    WebDiscoveryIngestResult,
    build_retained_search_signals,
    build_web_candidates,
    drain_web_discovery_drop_queue,
    write_search_signal_drop,
    write_web_discovery_drop,
)
from scripts.youtube import enrich as youtube_enrich
from scripts.youtube.parse import YouTubeRecord, parse_takeout
from mind.services.cli_progress import progress_for_args
from mind.services.document_text import extract_document_text
from mind.services.durable_write import write_contract_page
from mind.services.graph_registry import GraphRegistry
from mind.services.graph_resolution import patch_canonical_node, resolve_graph_document, write_ingest_review_artifact
from mind.services.ingest_contract import NormalizedSource, run_ingestion_lifecycle
from mind.services.ingest_readiness import run_ingest_readiness
from mind.services.reingest import (
    ReingestRequest,
    render_article_repair_report,
    render_reingest_report,
    run_article_repair,
    run_reingest,
)
from mind.services.rebuild_manifest import load_rebuild_manifest, write_rebuild_manifest
from mind.services.llm_service import get_llm_service
from mind.runtime_state import RuntimeState
from mind.services.source_planner import (
    InventoryRequest,
    PlanRequest,
    build_inventory,
    build_plan,
    execute_books_plan,
    execute_substack_plan,
    execute_youtube_plan,
    rebuild_source_registry,
    refresh_registry_for_inventory,
    reconcile_source_registry,
)
from mind.services.source_registry import SourceRegistry

from .common import append_changelog, ensure_index_entries, ingest_lane_hint, source_page_id, today_str, vault


@dataclass(frozen=True)
class BooksIngestResult:
    pages_written: int
    page_ids: list[str]
    selected_count: int = 0
    skipped_materialized: int = 0
    resumable: int = 0
    blocked: int = 0
    stale: int = 0
    executed: int = 0
    failed: int = 0
    blocked_samples: list[str] = field(default_factory=list)
    failed_items: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class YouTubeIngestResult:
    pages_written: int
    selected_count: int = 0
    skipped_count: int = 0
    skipped_materialized: int = 0
    resumable: int = 0
    blocked: int = 0
    stale: int = 0
    executed: int = 0
    failed: int = 0
    blocked_samples: list[str] = field(default_factory=list)
    failed_items: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SubstackIngestResult:
    posts_written: int
    paywalled: int
    failures: int
    unsaved_refs: int
    linked_articles_fetched: int
    export_path: Path
    linked_substack_followed: int = 0
    linked_substack_deferred: int = 0
    selected_count: int = 0
    skipped_materialized: int = 0
    resumable: int = 0
    blocked: int = 0
    stale: int = 0
    executed: int = 0
    blocked_samples: list[str] = field(default_factory=list)
    failed_items: list[str] = field(default_factory=list)

    @property
    def failed(self) -> int:
        return self.failures


def _update_global_index_and_changelog(
    *,
    v: Vault,
    action: str,
    source_name: str,
    page_ids: Iterable[str],
) -> list[str]:
    written = list(dict.fromkeys(page_id for page_id in page_ids if page_id))
    if not written:
        return []
    ensure_index_entries(v, written)
    append_changelog(
        v,
        f"{action} — {source_name}",
        [f"Created/updated: {', '.join(f'[[{entry}]]' for entry in written)}"],
    )
    return written


@dataclass(frozen=True)
class ChromeScanCommandResult:
    event_files: list[Path]
    query_files: list[Path]
    events_scanned: int


@dataclass(frozen=True)
class ChromeIngestCommandResult:
    raw_events_seen: int
    candidates_written: int
    search_signals_written: int
    candidate_drop_path: Path
    search_signal_drop_path: Path


@dataclass(frozen=True)
class FileIngestPreflight:
    source: NormalizedSource
    materialized_target: Path
    resolved: object | None
    details: dict[str, object]


def _extract_title_and_excerpt(path: Path) -> tuple[str, str]:
    if path.suffix.lower() == ".pdf":
        try:
            return path.stem.replace("-", " ").title(), extract_document_text(path)
        except Exception:
            return path.stem.replace("-", " ").title(), ""
    text = path.read_text(encoding="utf-8")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    title = path.stem.replace("-", " ").title()
    for line in lines:
        if line.startswith("# "):
            title = line[2:].strip()
            break
    excerpt = "\n".join(lines[:12]).strip()
    return title, excerpt


def _extract_full_file_text(path: Path) -> tuple[str, str]:
    if path.suffix.lower() == ".pdf":
        try:
            return path.stem.replace("-", " ").title(), extract_document_text(path)
        except Exception:
            return path.stem.replace("-", " ").title(), ""
    text = path.read_text(encoding="utf-8")
    title, _excerpt = _extract_title_and_excerpt(path)
    return title, text


def _normalize_file_source(
    path: Path,
    *,
    full_document: bool = False,
    source_id_override: str | None = None,
    source_metadata_extra: dict[str, object] | None = None,
) -> NormalizedSource:
    title, excerpt = _extract_full_file_text(path) if full_document else _extract_title_and_excerpt(path)
    source_kind = "document" if path.suffix.lower() == ".pdf" else ingest_lane_hint(path)
    source_metadata = {
        "source_path": str(path),
        "source_type": source_kind,
    }
    if source_metadata_extra:
        source_metadata.update(source_metadata_extra)
    return NormalizedSource(
        source_id=source_id_override or source_page_id(path),
        source_kind=source_kind,
        external_id="",
        canonical_url=str(path),
        title=title,
        creator_candidates=[],
        published_at=today_str(),
        discovered_at=today_str(),
        source_metadata=source_metadata,
        discovered_links=[],
        provenance={
            "adapter": "file",
            "ingest_command": "mind ingest file",
        },
        raw_text=excerpt or f"Binary {source_kind} source at {path}",
    )


def _raw_file_artifact_target(source: NormalizedSource) -> Path:
    return vault().raw / "files" / f"{source.source_id}.md"


def _understand_file_source(source: NormalizedSource, _envelope: dict[str, object]) -> dict[str, object]:
    if source.source_kind == "document":
        path = Path(source.canonical_url)
        response = get_llm_service().summarize_document(
            title=source.title,
            path_hint=str(path),
            document_bytes=path.read_bytes(),
            mime_type="application/pdf",
        )
        return {
            "excerpt": response.get("article", "") or response.get("tldr", ""),
            "source_type": source.source_kind,
            "document_summary": response,
        }
    return {
        "excerpt": source.primary_content,
        "source_type": source.source_kind,
    }


def _run_pass_d_for_file_source(source: NormalizedSource, *, summary: dict[str, object], repo_root: Path, today: str) -> dict[str, object]:
    from scripts.atoms import pass_d, working_set
    from scripts.atoms.replay import apply_pass_d_result

    ws = working_set.load_for_source(
        source_topics=list(summary.get("topics") or []),
        source_domains=["learning"],
        cap=300,
        repo_root=repo_root,
    )
    result = pass_d.run_pass_d(
        source_id=source.source_id,
        source_link=f"[[{source.source_id}]]",
        source_kind=source.source_kind,
        body_or_transcript=source.primary_content,
        summary=summary,
        applied=None,
        pass_c_delta=None,
        stance_context="",
        prior_source_context="",
        working_set=ws,
        repo_root=repo_root,
        today_str=today,
    )
    dispatch = apply_pass_d_result(
        result,
        evidence_date=today,
        recorded_on=today,
        source_link=f"[[{source.source_id}]]",
        repo_root=repo_root,
    )
    return {
        "evidence_updates": dispatch.evidence_updates,
        "probationary_updates": dispatch.probationary_updates,
        "missing_atoms": dispatch.missing_atoms,
    }


def _materialize_file_source(source: NormalizedSource, envelope: dict[str, object]) -> Path:
    v = vault()
    target = _raw_file_artifact_target(source)
    pass_a = envelope.get("pass_a") or {}
    excerpt = str((pass_a if isinstance(pass_a, dict) else {}).get("excerpt") or source.primary_content)
    source_path = Path(source.canonical_url)
    try:
        serialized_source_path = str(source_path.relative_to(v.root))
    except ValueError:
        serialized_source_path = source.canonical_url
    resolved_nodes_block = ""
    resolved_nodes = list(source.source_metadata.get("resolved_nodes") or [])
    if resolved_nodes:
        resolved_nodes_block = (
            "## Resolved Nodes\n\n"
            + "\n".join(f"- [[{node_id}]]" for node_id in resolved_nodes)
            + "\n\n"
        )
    body = (
        f"# {source.title}\n\n"
        "## Source\n\n"
        f"- Path: `{source.canonical_url}`\n"
        f"- Type: `{source.source_kind}`\n\n"
        + resolved_nodes_block
        + "## Summary\n\n"
        + f"{excerpt}\n"
    )
    if target.exists():
        try:
            read_page = __import__("mind.dream.common", fromlist=["read_page"]).read_page
            existing_frontmatter, _existing_body = read_page(target)
        except Exception:
            existing_frontmatter = {}
        existing_hash = str(existing_frontmatter.get("document_sha256") or "")
        if existing_hash and existing_hash == str(source.source_metadata.get("document_sha256") or ""):
            hints = dict(envelope.get("materialization_hints") or {})
            hints["artifact_preexisted"] = True
            envelope["materialization_hints"] = hints
            return target
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        (
            "---\n"
            f"id: {source.source_id}\n"
            "type: raw-file\n"
            f"title: {source.title}\n"
            f"source_path: {serialized_source_path}\n"
            f"source_type: {source.source_kind}\n"
            f"source_date: {source.discovered_at or today_str()}\n"
            f"ingested: {today_str()}\n"
            f"resolution_status: {str(source.source_metadata.get('resolution_status') or 'legacy')}\n"
            f"document_sha256: {str(source.source_metadata.get('document_sha256') or '')}\n"
            "resolved_nodes:\n"
            + "".join(f"  - {node}\n" for node in resolved_nodes)
            + "---\n\n"
            + body.rstrip()
            + "\n"
        ),
        encoding="utf-8",
    )
    return target


def _propagate_file_source(_source: NormalizedSource, _envelope: dict[str, object], materialized: Path | None) -> dict[str, object]:
    return {"index_updated": False, "changelog_updated": False}


def _candidate_preview(resolved: object | None) -> list[str]:
    if resolved is None:
        return []
    rows: list[str] = []
    decisions = [resolved.primary_decision, *resolved.related_decisions]
    for decision in decisions:
        for candidate in decision.candidates[:3]:
            rows.append(
                f"{decision.mention_text}: {candidate.page_id} "
                f"({candidate.match_kind}, {candidate.score:.2f})"
            )
        for candidate in decision.shadow_vector_candidates[:2]:
            rows.append(
                f"{decision.mention_text}: {candidate.page_id} "
                f"(shadow-vector, {candidate.score:.2f})"
            )
    deduped: list[str] = []
    for row in rows:
        if row not in deduped:
            deduped.append(row)
    return deduped[:8]


def _review_reasons(resolved: object | None) -> list[str]:
    if resolved is None:
        return []
    reasons: list[str] = []
    for decision in [resolved.primary_decision, *resolved.related_decisions]:
        if decision.resolution_kind in {"review_ambiguous", "no_match"} or not decision.resolved_node_id:
            reason = f"{decision.mention_text}: {decision.rationale}"
            if reason not in reasons:
                reasons.append(reason)
    return reasons[:6]


def _read_existing_document_hash(target: Path) -> str:
    if not target.exists():
        return ""
    try:
        read_page = __import__("mind.dream.common", fromlist=["read_page"]).read_page
        frontmatter, _body = read_page(target)
    except Exception:
        return ""
    return str(frontmatter.get("document_sha256") or "")


def _materialized_source_ref(target: Path) -> str:
    v = vault()
    try:
        return str(target.relative_to(v.root))
    except ValueError:
        return str(target)


def preflight_file_ingest(
    path: Path,
    *,
    graph_aware: bool = False,
    graph_registry: GraphRegistry | None = None,
) -> FileIngestPreflight:
    source = _normalize_file_source(path)
    resolved = None
    registry = graph_registry
    if graph_aware:
        registry = registry or GraphRegistry.for_repo_root(vault().root)
        registry.ensure_built()
        resolved = resolve_graph_document(path=path, registry=registry)
        source = _normalize_file_source(
            path,
            full_document=True,
            source_id_override=resolved.artifact_id,
            source_metadata_extra={
                "resolved_nodes": [
                    decision.resolved_node_id
                    for decision in [resolved.primary_decision, *resolved.related_decisions]
                    if decision.resolved_node_id
                ],
                "resolved_registry_nodes": [
                    decision.resolved_registry_node_id
                    for decision in [resolved.primary_decision, *resolved.related_decisions]
                    if decision.resolved_registry_node_id
                ],
                "resolution_status": resolved.primary_decision.resolution_kind,
                "document_sha256": resolved.doc_id.replace("doc-", ""),
            },
        )
    materialized_target = _raw_file_artifact_target(source)
    existing_hash = _read_existing_document_hash(materialized_target)
    document_hash = str(source.source_metadata.get("document_sha256") or "")
    would_reuse_artifact = bool(existing_hash and existing_hash == document_hash)
    canonical_page_target = ""
    if graph_aware and resolved is not None and not resolved.review_required and resolved.primary_decision.resolved_registry_node_id:
        registry_node = registry.get_node(resolved.primary_decision.resolved_registry_node_id) if registry is not None else None
        if registry_node is not None:
            canonical_page_target = str(vault().resolve_logical_path(registry_node.path))
    details: dict[str, object] = {
        "source_id": source.source_id,
        "canonical_page_target": canonical_page_target,
        "raw_artifact_target": str(materialized_target),
        "review_required": bool(getattr(resolved, "review_required", False)),
        "resolved_nodes": list(source.source_metadata.get("resolved_nodes") or []),
        "resolved_registry_nodes": list(source.source_metadata.get("resolved_registry_nodes") or []),
        "resolution_status": str(source.source_metadata.get("resolution_status") or ("legacy" if not graph_aware else "")),
        "document_sha256": document_hash,
        "would_patch_existing_node": bool(
            graph_aware
            and resolved is not None
            and not resolved.review_required
            and resolved.primary_decision.resolved_registry_node_id
        ),
        "would_create_canonical_page": False,
        "would_reuse_canonical_page": bool(canonical_page_target),
        "would_reuse_raw_artifact": would_reuse_artifact,
        "review_reasons": _review_reasons(resolved),
        "candidate_summaries": _candidate_preview(resolved),
    }
    return FileIngestPreflight(
        source=source,
        materialized_target=materialized_target,
        resolved=resolved,
        details=details,
    )


def ingest_file_with_details(
    path: Path,
    *,
    graph_aware: bool = False,
    graph_registry: GraphRegistry | None = None,
) -> tuple[Path, dict[str, object]]:
    registry = graph_registry or (GraphRegistry.for_repo_root(vault().root) if graph_aware else None)
    preflight = preflight_file_ingest(
        path,
        graph_aware=graph_aware,
        graph_registry=registry,
    )
    source = preflight.source
    resolved = preflight.resolved
    result = run_ingestion_lifecycle(
        source=source,
        understand=_understand_file_source,
        distill=(
            None
            if graph_aware
            else lambda source, envelope: _run_pass_d_for_file_source(
                source,
                summary=(envelope.get("pass_a") or {}).get("document_summary") or {
                    "tldr": (envelope.get("pass_a") or {}).get("excerpt", ""),
                    "topics": [],
                },
                repo_root=vault().memory_root,
                today=today_str(),
            )
        ),
        materialize=_materialize_file_source,
        propagate=_propagate_file_source,
    )
    assert isinstance(result.materialized, Path)
    details: dict[str, object] = dict(preflight.details)
    if graph_aware and resolved is not None and registry is not None:
        if resolved.review_required:
            review_json, review_md = write_ingest_review_artifact(repo_root=vault().root, resolved=resolved)
            details["review_required"] = True
            details["review_artifacts"] = [str(review_json), str(review_md)]
        else:
            patch_canonical_node(
                repo_root=vault().root,
                registry=registry,
                resolved=resolved,
                source_ref=_materialized_source_ref(result.materialized),
            )
        candidate_rows: list[dict[str, object]] = []
        for decision in [resolved.primary_decision, *resolved.related_decisions]:
            for candidate in decision.candidates:
                candidate_rows.append(
                    {
                        "mention_text": decision.mention_text,
                        "candidate_node_id": candidate.page_id,
                        "candidate_registry_node_id": candidate.registry_node_id,
                        "score": candidate.score,
                        "match_kind": candidate.match_kind,
                        "debug_payload": {
                            "title": candidate.title,
                            "primary_type": candidate.primary_type,
                            "path": candidate.path,
                            "aliases": candidate.aliases,
                        },
                    }
                )
            for candidate in decision.shadow_vector_candidates:
                candidate_rows.append(
                    {
                        "mention_text": decision.mention_text,
                        "candidate_node_id": candidate.page_id,
                        "candidate_registry_node_id": candidate.registry_node_id,
                        "score": candidate.score,
                        "match_kind": candidate.match_kind,
                        "debug_payload": {
                            "title": candidate.title,
                            "primary_type": candidate.primary_type,
                            "path": candidate.path,
                            "aliases": candidate.aliases,
                        },
                    }
                )
        registry.record_document(
            doc_id=resolved.doc_id,
            path=path,
            title=resolved.title,
            source_kind=resolved.source_kind,
            ingest_lane="dropbox-file" if graph_aware else "file",
            body=resolved.body,
            resolutions=[
                {
                    "mention_text": decision.mention_text,
                    "resolved_node_id": decision.resolved_node_id,
                    "resolved_registry_node_id": decision.resolved_registry_node_id,
                    "resolution_kind": decision.resolution_kind,
                    "confidence": decision.confidence,
                    "rationale": decision.rationale,
                }
                for decision in [resolved.primary_decision, *resolved.related_decisions]
            ],
            candidates=candidate_rows,
            document_targets=list(source.source_metadata.get("resolved_registry_nodes") or []),
        )
        details["resolved_nodes"] = list(source.source_metadata.get("resolved_nodes") or [])
        details["resolution_status"] = resolved.primary_decision.resolution_kind
    return result.materialized, details


def ingest_file(
    path: Path,
    *,
    graph_aware: bool = False,
    graph_registry: GraphRegistry | None = None,
) -> Path:
    target, _details = ingest_file_with_details(
        path,
        graph_aware=graph_aware,
        graph_registry=graph_registry,
    )
    return target


def cmd_ingest_file(args: argparse.Namespace) -> int:
    target = ingest_file(Path(args.path).resolve())
    print(target)
    return 0


def _iter_books_from_path(path: Path) -> Iterable[BookRecord]:
    name = path.name.lower()
    if path.suffix.lower() == ".pdf":
        return [BookRecord(title=path.stem.replace("-", " ").title(), author=[], format="ebook", status="finished", document_path=str(path.resolve()))]
    if path.suffix.lower() in {".mp3", ".m4a", ".mp4", ".wav", ".webm"}:
        return [BookRecord(title=path.stem.replace("-", " ").title(), author=[], format="audiobook", status="finished", audio_path=str(path.resolve()))]
    if name.startswith("goodreads-") and path.suffix == ".csv":
        return parse_csv(path, flavor="goodreads")
    if name.startswith("audible-library-") and path.suffix == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        return parse_audible_library(data)
    if name.startswith("audible-") and path.suffix == ".csv":
        return parse_csv(path, flavor="openaudible")
    if path.suffix == ".md":
        return parse_markdown(path)
    if path.suffix == ".csv":
        try:
            return parse_csv(path, flavor="goodreads")
        except Exception:
            return parse_csv(path, flavor="openaudible")
    raise ValueError(f"unsupported books export path: {path}")


def _ingest_books_export_direct(path: Path, *, force_deep: bool = False) -> BooksIngestResult:
    path = path.resolve()
    v = vault()
    written: list[str] = []
    for book in _iter_books_from_path(path):
        lifecycle = books_enrich.run_book_record_lifecycle(
            book,
            repo_root=v.memory_root,
            today=today_str(),
            force_deep=force_deep,
        )
        if lifecycle is None:
            continue
        pass_d_outcomes = (lifecycle.propagate or {}).get("pass_d") if lifecycle.propagate else None
        for pass_d_outcome in pass_d_outcomes or []:
            append_to_inbox_log(
                target=v.wiki / "inbox" / f"books-failures-{today_str()}.md",
                kind="books-failures",
                entry=f"- {book.title} — stage={pass_d_outcome['stage']} — {pass_d_outcome['summary']}\n",
                date=today_str(),
            )
        for fanout_outcome in ((lifecycle.propagate or {}).get("fanout_outcomes") or []):
            append_to_inbox_log(
                target=v.wiki / "inbox" / f"books-failures-{today_str()}.md",
                kind="books-failures",
                entry=f"- {book.title} — stage={fanout_outcome['stage']} — {fanout_outcome['summary']}\n",
                date=today_str(),
            )
        written.extend([
            Path(lifecycle.materialized["book"]).stem,
        ])
    written = _update_global_index_and_changelog(
        v=v,
        action="ingest-books",
        source_name=path.name,
        page_ids=written,
    )
    return BooksIngestResult(pages_written=len(written), page_ids=written)


def _postprocess_books_completed_items(*, path: Path, completed_items: Sequence[object]) -> list[str]:
    v = vault()
    written: list[str] = []
    for completed in completed_items:
        title = str(getattr(completed, "title", "") or "Unknown book")
        propagate = dict(getattr(completed, "propagate", None) or {})
        for pass_d_outcome in propagate.get("pass_d") or []:
            append_to_inbox_log(
                target=v.wiki / "inbox" / f"books-failures-{today_str()}.md",
                kind="books-failures",
                entry=f"- {title} — stage={pass_d_outcome['stage']} — {pass_d_outcome['summary']}\n",
                date=today_str(),
            )
        for fanout_outcome in propagate.get("fanout_outcomes") or []:
            append_to_inbox_log(
                target=v.wiki / "inbox" / f"books-failures-{today_str()}.md",
                kind="books-failures",
                entry=f"- {title} — stage={fanout_outcome['stage']} — {fanout_outcome['summary']}\n",
                date=today_str(),
            )
        materialized = dict(getattr(completed, "materialized_paths", None) or {})
        book_path = str(materialized.get("book") or "").strip()
        if book_path:
            written.append(Path(book_path).stem)
    return _update_global_index_and_changelog(
        v=v,
        action="ingest-books",
        source_name=path.name,
        page_ids=written,
    )


def ingest_books_export(
    path: Path,
    *,
    force_deep: bool = False,
    resume: bool = True,
    skip_materialized: bool = True,
    refresh_stale: bool = False,
    recompute_missing: bool = False,
    from_stage: str | None = None,
    through: str = "propagate",
    source_ids: tuple[str, ...] = (),
    external_ids: tuple[str, ...] = (),
    selection: tuple[str, ...] = ("all",),
    phase_callback=None,
) -> BooksIngestResult:
    path = path.resolve()
    repo_root = vault().memory_root
    inventory = build_inventory(
        InventoryRequest(
            lane="books",
            path=path,
            source_ids=source_ids,
            external_ids=external_ids,
            selection=selection,
        ),
        repo_root=repo_root,
        use_registry=True,
        phase_callback=phase_callback,
    )
    refresh_registry_for_inventory(inventory, repo_root=repo_root)
    plan = build_plan(
        PlanRequest(
            lane="books",
            path=path,
            source_ids=source_ids,
            external_ids=external_ids,
            selection=selection,
            resume=resume,
            skip_materialized=skip_materialized,
            refresh_stale=refresh_stale,
            recompute_missing=recompute_missing,
            from_stage=from_stage,
            through=through,
        ),
        repo_root=repo_root,
        use_registry=True,
        phase_callback=phase_callback,
    )
    execution = execute_books_plan(plan, repo_root=repo_root, force_deep=force_deep, phase_callback=phase_callback)
    written = _postprocess_books_completed_items(path=path, completed_items=execution.completed_items)
    return BooksIngestResult(
        pages_written=len(written),
        page_ids=written,
        selected_count=plan.selected_count,
        skipped_materialized=plan.skipped_materialized_count,
        resumable=plan.resumable_count,
        blocked=plan.blocked_count,
        stale=plan.stale_count,
        executed=execution.executed_count,
        failed=execution.failed_count,
        blocked_samples=list(execution.blocked_samples),
        failed_items=list(getattr(execution, "failed_items", ())),
    )


def render_books_ingest_result(result: BooksIngestResult) -> str:
    lines = [(
        "ingest-books: "
        f"selected={result.selected_count} "
        f"skipped_materialized={result.skipped_materialized} "
        f"resumable={result.resumable} "
        f"blocked={result.blocked} "
        f"stale={result.stale} "
        f"executed={result.executed} "
        f"failed={result.failed} "
        f"pages_written={result.pages_written}"
    )]
    if result.blocked_samples:
        lines.append("blocked_samples:")
        lines.extend(f"- {sample}" for sample in result.blocked_samples[:3])
    if result.failed_items:
        lines.append("failed_samples:")
        lines.extend(f"- {sample}" for sample in result.failed_items[:5])
    return "\n".join(lines)


def cmd_ingest_books(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="ingesting books", default=True) as progress:
        progress.phase("inventorying selected books")
        result = ingest_books_export(
            Path(args.path),
            force_deep=bool(args.force_deep),
            resume=bool(args.resume),
            skip_materialized=bool(args.skip_materialized),
            refresh_stale=bool(args.refresh_stale),
            recompute_missing=bool(args.recompute_missing),
            from_stage=args.from_stage,
            through=args.through,
            source_ids=tuple(args.source_ids or ()),
            external_ids=tuple(args.external_ids or ()),
            selection=tuple(args.selection or ("all",)),
            phase_callback=progress.phase,
        )
        progress.clear()
        print(render_books_ingest_result(result))
        targeted = bool(args.source_ids or args.external_ids)
        if result.failed > 0:
            return 1
        if targeted and result.blocked > 0:
            return 1
        return 0


def _start_tracked_ingest_run(*, kind: str, holder: str, metadata: dict[str, object]) -> tuple[RuntimeState, int]:
    state = RuntimeState.for_repo_root(vault().root)
    run_id = state.create_run(kind=kind, holder=holder, metadata=metadata)
    state.add_run_event(run_id, stage="ingest", event_type="started", message=f"{holder} started", payload=metadata)
    return state, run_id


def _finish_tracked_ingest_run(
    state: RuntimeState,
    run_id: int,
    *,
    status: str,
    notes: str,
    metadata: dict[str, object],
) -> None:
    event_type = "completed" if status == "completed" else "failed"
    state.add_run_event(run_id, stage="ingest", event_type=event_type, message=notes, payload=metadata)
    if status != "completed":
        state.add_error(run_id=run_id, stage="ingest", error_type="CommandFailed", message=notes, payload=metadata)
    state.finish_run(run_id, status=status, notes=notes, metadata=metadata)


def _fail_tracked_ingest_run(
    state: RuntimeState,
    run_id: int,
    *,
    holder: str,
    exc: Exception,
    metadata: dict[str, object],
) -> None:
    state.add_run_event(
        run_id,
        stage="ingest",
        event_type="failed",
        message=f"{holder} failed: {type(exc).__name__}",
        payload=metadata,
    )
    state.add_error(
        run_id=run_id,
        stage="ingest",
        error_type=type(exc).__name__,
        message=str(exc),
        payload=metadata,
    )
    state.finish_run(run_id, status="failed", notes=str(exc), metadata=metadata)


def _tracked_phase_callback(phase_callback, state: RuntimeState, run_id: int):
    def _phase(message: str) -> None:
        if phase_callback is not None:
            phase_callback(message)
        state.add_run_event(run_id, stage="phase", event_type="progress", message=message)

    return _phase


def _planner_skipped_count(plan: object) -> int:
    items = list(getattr(plan, "items", ()) or ())
    return int(getattr(plan, "skipped_materialized_count", 0) or 0) + sum(1 for item in items if getattr(item, "action", "") == "excluded")


def _planner_counter_payload(*, result: object, pages_written: int, extra: dict[str, object] | None = None) -> dict[str, object]:
    payload = {
        "selected_count": int(getattr(result, "selected_count", 0) or 0),
        "skipped_materialized": int(getattr(result, "skipped_materialized", 0) or 0),
        "resumable": int(getattr(result, "resumable", 0) or 0),
        "blocked": int(getattr(result, "blocked", 0) or 0),
        "stale": int(getattr(result, "stale", 0) or 0),
        "executed": int(getattr(result, "executed", 0) or 0),
        "failed": int(getattr(result, "failed", 0) or 0),
        "pages_written": pages_written,
    }
    if extra:
        payload.update(extra)
    return payload


def _record_plan_item_progress(
    state: RuntimeState,
    run_id: int,
    *,
    item: object,
    status: str,
    detail: str,
    index: int,
    total: int,
) -> None:
    source_id = str(getattr(item, "source_id", "") or "")
    stage = str(getattr(item, "start_stage", "") or "unknown")
    title = str(getattr(item, "title", "") or "")
    state.add_run_event(
        run_id,
        stage=stage,
        event_type=status,
        message=f"{index}/{total} {source_id or title} {status}",
        payload={
            "lane": str(getattr(item, "lane", "") or ""),
            "source_id": source_id,
            "title": title,
            "current_stage": stage,
            "detail": detail,
            "index": index,
            "total": total,
        },
    )


def _planner_item_callback(
    state: RuntimeState,
    run_id: int,
    *,
    today: str,
    failure_logger: Callable[[str, object, str], None] | None = None,
):
    def _callback(item, status, detail, index, total) -> None:
        _record_plan_item_progress(
            state,
            run_id,
            item=item,
            status=status,
            detail=detail,
            index=index,
            total=total,
        )
        if status == "failed" and failure_logger is not None:
            failure_logger(today, item, detail)

    return _callback


def _log_youtube_failure(today: str, entry: str) -> None:
    append_to_inbox_log(
        target=vault().wiki / "inbox" / f"youtube-failures-{today}.md",
        kind="youtube-failures",
        entry=entry,
        date=today,
    )


def _log_youtube_execution_failure(today: str, item: object, detail: str) -> None:
    source_id = str(getattr(item, "source_id", "") or "").removeprefix("youtube-")
    title = str(getattr(item, "title", "") or source_id or "Unknown video")
    stage = str(getattr(item, "start_stage", "") or "unknown")
    _log_youtube_failure(today, f"- {source_id or title} — {title} — stage={stage} — {detail}\n")


def _postprocess_youtube_completed_items(*, completed_items: Sequence[object], today: str) -> list[str]:
    written: list[str] = []
    for completed in completed_items:
        title = str(getattr(completed, "title", "") or "Unknown video")
        source_id = str(getattr(completed, "source_id", "") or "").removeprefix("youtube-")
        propagate = dict(getattr(completed, "propagate", None) or {})
        if propagate.get("multimodal_error"):
            _log_youtube_failure(
                today,
                (
                    f"- {source_id or title} — {title} — stage=multimodal-fallback"
                    f" — path={propagate.get('transcription_path') or 'unknown'}"
                    f" — {propagate['multimodal_error']}\n"
                ),
            )
        for pass_d_outcome in [
            outcome
            for outcome in (propagate.get("pass_d") or [])
            if isinstance(outcome, dict) and _youtube_outcome_is_failure(outcome)
        ]:
            _log_youtube_failure(
                today,
                f"- {source_id or title} — {title} — stage={pass_d_outcome['stage']} — {pass_d_outcome['summary']}\n",
            )
        for fanout_outcome in [
            outcome
            for outcome in (propagate.get("fanout_outcomes") or [])
            if isinstance(outcome, dict) and _youtube_outcome_is_failure(outcome)
        ]:
            _log_youtube_failure(
                today,
                f"- {source_id or title} — {title} — stage={fanout_outcome['stage']} — {fanout_outcome['summary']}\n",
            )
        materialized = dict(getattr(completed, "materialized_paths", None) or {})
        video_path = str(materialized.get("video") or "").strip()
        if video_path:
            written.append(Path(video_path).stem)
    return list(dict.fromkeys(written))


def render_youtube_ingest_result(result: YouTubeIngestResult) -> str:
    lines = [(
        "ingest-youtube: "
        f"selected={getattr(result, 'selected_count', 0)} "
        f"skipped_materialized={getattr(result, 'skipped_materialized', 0)} "
        f"resumable={getattr(result, 'resumable', 0)} "
        f"blocked={getattr(result, 'blocked', 0)} "
        f"stale={getattr(result, 'stale', 0)} "
        f"executed={getattr(result, 'executed', 0)} "
        f"failed={getattr(result, 'failed', 0)} "
        f"pages_written={getattr(result, 'pages_written', 0)}"
    )]
    blocked_samples = list(getattr(result, "blocked_samples", []) or [])
    if blocked_samples:
        lines.append("blocked_samples:")
        lines.extend(f"- {sample}" for sample in blocked_samples[:3])
    failed_items = list(getattr(result, "failed_items", []) or [])
    if failed_items:
        lines.append("failed_samples:")
        lines.extend(f"- {sample}" for sample in failed_items[:5])
    return "\n".join(lines)


def _iter_youtube_records(path: Path) -> list[YouTubeRecord]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if data and isinstance(data[0], dict) and "titleUrl" in data[0]:
        return list(parse_takeout(data))
    records: list[YouTubeRecord] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        raw_categories = entry.get("categories") or ()
        if isinstance(raw_categories, str):
            categories = (raw_categories,) if raw_categories.strip() else ()
        else:
            categories = tuple(str(value) for value in raw_categories if str(value))
        records.append(
            YouTubeRecord(
                video_id=str(entry.get("video_id", "")),
                title=str(entry.get("title", "")),
                channel=str(entry.get("channel", "")),
                watched_at=str(entry.get("watched_at") or entry.get("time") or ""),
                duration_seconds=_coerce_optional_int(entry.get("duration_seconds") or entry.get("duration")),
                description=str(entry.get("description", "") or ""),
                tags=tuple(entry.get("tags") or ()),
                category=str(entry.get("category", "") or (categories[0] if categories else "") or ""),
                categories=categories,
                title_url=str(entry.get("title_url") or entry.get("url") or ""),
                channel_url=str(entry.get("channel_url") or ""),
                channel_id=str(entry.get("channel_id") or ""),
                published_at=str(entry.get("published_at") or entry.get("published") or ""),
                thumbnail_url=str(entry.get("thumbnail_url") or entry.get("thumbnail") or ""),
            )
        )
    return records


def _coerce_optional_int(value: object) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _youtube_outcome_is_failure(outcome: dict[str, Any]) -> bool:
    status = str(outcome.get("status") or "").strip().lower()
    if status == "error":
        return True
    if status == "warning":
        return False
    stage = str(outcome.get("stage") or "").strip().lower()
    summary = str(outcome.get("summary") or "").strip()
    if stage in {"propagate", "pass_d.dispatch"}:
        return True
    return any(token in summary for token in ("Error:", "Exception:", "RuntimeError", "TypeError", "ValueError", "NameError"))


def ingest_youtube_export(
    path: Path,
    *,
    default_duration_minutes: float = 30.0,
    resume: bool = True,
    skip_materialized: bool = True,
    refresh_stale: bool = False,
    recompute_missing: bool = False,
    from_stage: str | None = None,
    through: str = "propagate",
    source_ids: tuple[str, ...] = (),
    external_ids: tuple[str, ...] = (),
    selection: tuple[str, ...] = ("all",),
    phase_callback=None,
) -> YouTubeIngestResult:
    path = path.resolve()
    v = vault()
    repo_root = v.memory_root
    effective_today = date.today().isoformat()
    initial_metadata: dict[str, object] = {
        "lane": "youtube",
        "export_path": str(path),
        "today": effective_today,
        "default_duration_minutes": float(default_duration_minutes),
    }
    state, run_id = _start_tracked_ingest_run(
        kind="youtube.ingest",
        holder="mind-youtube-ingest",
        metadata=initial_metadata,
    )
    tracked_phase = _tracked_phase_callback(phase_callback, state, run_id)
    try:
        tracked_phase("processing selected videos")
        lane_options = {"default_duration_minutes": float(default_duration_minutes)}
        inventory = build_inventory(
            InventoryRequest(
                lane="youtube",
                path=path,
                today=effective_today,
                source_ids=source_ids,
                external_ids=external_ids,
                selection=selection,
                lane_options=lane_options,
            ),
            repo_root=repo_root,
            use_registry=True,
            phase_callback=tracked_phase,
        )
        refresh_registry_for_inventory(inventory, repo_root=repo_root)
        plan = build_plan(
            PlanRequest(
                lane="youtube",
                path=path,
                today=effective_today,
                source_ids=source_ids,
                external_ids=external_ids,
                selection=selection,
                resume=resume,
                skip_materialized=skip_materialized,
                refresh_stale=refresh_stale,
                recompute_missing=recompute_missing,
                from_stage=from_stage,
                through=through,
                lane_options=lane_options,
            ),
            repo_root=repo_root,
            use_registry=True,
            phase_callback=tracked_phase,
        )
        execution = execute_youtube_plan(
            plan,
            repo_root=repo_root,
            default_duration_minutes=float(default_duration_minutes),
            phase_callback=tracked_phase,
            item_callback=_planner_item_callback(
                state,
                run_id,
                today=effective_today,
                failure_logger=_log_youtube_execution_failure,
            ),
        )
        written = _postprocess_youtube_completed_items(completed_items=execution.completed_items, today=effective_today)
        written = _update_global_index_and_changelog(
            v=v,
            action="ingest-youtube",
            source_name=path.name,
            page_ids=written,
        )
        result = YouTubeIngestResult(
            pages_written=len(written),
            selected_count=plan.selected_count,
            skipped_count=_planner_skipped_count(plan),
            skipped_materialized=plan.skipped_materialized_count,
            resumable=plan.resumable_count,
            blocked=plan.blocked_count,
            stale=plan.stale_count,
            executed=execution.executed_count,
            failed=execution.failed_count,
            blocked_samples=list(execution.blocked_samples),
            failed_items=list(getattr(execution, "failed_items", ())),
        )
        final_metadata = {
            **initial_metadata,
            **_planner_counter_payload(result=result, pages_written=len(written)),
        }
        notes = (
            f"selected={result.selected_count} blocked={result.blocked} "
            f"executed={result.executed} failed={result.failed} pages_written={result.pages_written}"
        )
        _finish_tracked_ingest_run(
            state,
            run_id,
            status="completed" if result.failed == 0 else "failed",
            notes=notes,
            metadata=final_metadata,
        )
        return result
    except Exception as exc:
        _fail_tracked_ingest_run(state, run_id, holder="mind-youtube-ingest", exc=exc, metadata=initial_metadata)
        raise


def cmd_ingest_youtube(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="ingesting YouTube export", default=True) as progress:
        progress.phase("loading export")
        result = ingest_youtube_export(
            Path(args.path),
            default_duration_minutes=float(args.default_duration_minutes),
            resume=bool(args.resume),
            skip_materialized=bool(args.skip_materialized),
            refresh_stale=bool(args.refresh_stale),
            recompute_missing=bool(args.recompute_missing),
            from_stage=args.from_stage,
            through=args.through,
            source_ids=tuple(args.source_ids or ()),
            external_ids=tuple(args.external_ids or ()),
            selection=tuple(args.selection or ("all",)),
            phase_callback=progress.phase,
        )
        progress.clear()
        print(render_youtube_ingest_result(result))
        targeted = bool(args.source_ids or args.external_ids)
        if result.failed > 0:
            return 1
        if targeted and result.blocked > 0:
            return 1
        return 0


def ingest_articles_queue(*, today: str | None = None, repo_root: Path | None = None):
    effective_root = repo_root or vault().memory_root
    result = drain_drop_queue(today_str=today or date.today().isoformat(), repo_root=effective_root)
    v = Vault.load(effective_root)
    _update_global_index_and_changelog(
        v=v,
        action="ingest-articles",
        source_name=today or date.today().isoformat(),
        page_ids=(path.stem for path in getattr(result, "new_page_paths", ()) or ()),
    )
    return result


def cmd_ingest_articles(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="ingesting article queue", default=True) as progress:
        progress.phase("draining article queue")
        result = ingest_articles_queue(today=args.today)
        progress.phase("summarizing fetched articles")
        print(
            f"ingest-articles: {result.drop_files_processed} drop files -> "
            f"{result.fetched_summarized} fetched, {result.failed} failed"
        )
        return 0 if result.failed == 0 else 1


def cmd_ingest_reingest(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="replaying cached ingest work", default=True) as progress:
        progress.phase(f"replaying {args.lane}")
        manifest_ids: tuple[str, ...] = ()
        if args.manifest:
            manifest_ids = load_rebuild_manifest(Path(args.manifest), lane=args.lane)
        request = ReingestRequest(
            lane=args.lane,
            path=Path(args.path) if args.path else None,
            stage=args.stage,
            through=args.through,
            today=args.today,
            limit=args.limit,
            source_ids=tuple(dict.fromkeys([*(args.source_ids or ()), *manifest_ids])),
            dry_run=bool(args.dry_run),
            force_deep=bool(getattr(args, "force_deep", False)),
        )
        report_path: Path | None = None
        item_callback = None
        if not request.dry_run:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
            report_path = vault().raw / "reports" / "ingest-review" / f"reingest-{args.lane}-{timestamp}.jsonl"
            report_path.parent.mkdir(parents=True, exist_ok=True)
            progress.update(f"{request.lane} report: {report_path.name}")
            print(f"reingest-start: lane={request.lane} report={report_path}", flush=True)
            report_path.write_text(
                json.dumps(
                    {
                        "event": "start",
                        "lane": request.lane,
                        "stage": request.stage,
                        "through": request.through,
                        "path": str(request.path) if request.path is not None else "",
                        "limit": request.limit,
                        "source_ids": list(request.source_ids),
                        "started_at": timestamp,
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )

            def _item_callback(plan_item, item_result, index, total) -> None:
                progress.update(f"{request.lane} {index}/{total}: {item_result.source_id} {item_result.status}")
                if report_path is None:
                    return
                payload = {
                    "event": "item",
                    "index": index,
                    "total": total,
                    "source_id": item_result.source_id,
                    "status": item_result.status,
                    "detail": item_result.detail,
                    "label": plan_item.label,
                    "blocked_reasons": list(plan_item.blocked_reasons),
                    "excluded_reason": plan_item.excluded_reason or "",
                    "materialized_paths": item_result.materialized_paths or {},
                }
                with report_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

            item_callback = _item_callback

        result = run_reingest(request, repo_root=vault().memory_root, item_callback=item_callback)
        if report_path is not None:
            with report_path.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        {
                            "event": "complete",
                            "exit_code": result.exit_code,
                            "completed": sum(1 for entry in result.results if entry.status == "completed"),
                            "failed": sum(1 for entry in result.results if entry.status == "failed"),
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
        print(render_reingest_report(result))
        if report_path is not None:
            print(f"reingest-report: {report_path}")
        return result.exit_code


def cmd_ingest_rebuild_manifest(args: argparse.Namespace) -> int:
    output_path = Path(args.output).resolve() if args.output else (vault().reports_root / "ingest-rebuild-manifest.json")
    manifest = write_rebuild_manifest(repo_root=vault().root, output_path=output_path)
    counts = {lane: len(items) for lane, items in manifest.lanes.items()}
    print(
        "ingest-rebuild-manifest: "
        f"output={output_path} "
        f"books={counts.get('books', 0)} "
        f"youtube={counts.get('youtube', 0)} "
        f"substack={counts.get('substack', 0)} "
        f"articles={counts.get('articles', 0)}"
    )
    return 0


def _inventory_request_from_args(args: argparse.Namespace) -> InventoryRequest:
    return InventoryRequest(
        lane=args.lane,
        path=Path(args.path).resolve() if getattr(args, "path", None) else None,
        today=getattr(args, "today", None),
        source_ids=tuple(args.source_ids or ()),
        external_ids=tuple(args.external_ids or ()),
        selection=tuple(args.selection or ("all",)),
        limit=getattr(args, "limit", None),
    )


def _plan_request_from_args(args: argparse.Namespace) -> PlanRequest:
    return PlanRequest(
        lane=args.lane,
        path=Path(args.path).resolve() if getattr(args, "path", None) else None,
        today=getattr(args, "today", None),
        source_ids=tuple(args.source_ids or ()),
        external_ids=tuple(args.external_ids or ()),
        selection=tuple(args.selection or ("all",)),
        limit=getattr(args, "limit", None),
        resume=bool(getattr(args, "resume", True)),
        skip_materialized=bool(getattr(args, "skip_materialized", True)),
        refresh_stale=bool(getattr(args, "refresh_stale", False)),
        recompute_missing=bool(getattr(args, "recompute_missing", False)),
        from_stage=getattr(args, "from_stage", None),
        through=getattr(args, "through", "propagate"),
    )


def cmd_ingest_registry_rebuild(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="rebuilding source registry", default=True) as progress:
        progress.phase("rebuilding source registry")
        registry, count = rebuild_source_registry(repo_root=vault().root, phase_callback=progress.phase)
        print(f"ingest-registry-rebuild: sources={count}")
        print(registry.status().render())
        return 0


def cmd_ingest_registry_status(_args: argparse.Namespace) -> int:
    print(SourceRegistry.for_repo_root(vault().root).status().render())
    return 0


def cmd_ingest_inventory(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="inventorying sources", default=True) as progress:
        progress.phase(f"inventorying {args.lane}")
        result = build_inventory(
            _inventory_request_from_args(args),
            repo_root=vault().root,
            use_registry=True,
            phase_callback=progress.phase,
        )
        if bool(getattr(args, "json", False)):
            print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
        else:
            print(result.render())
        return 0


def cmd_ingest_plan(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="planning ingest actions", default=True) as progress:
        progress.phase(f"planning {args.lane}")
        result = build_plan(
            _plan_request_from_args(args),
            repo_root=vault().root,
            use_registry=True,
            phase_callback=progress.phase,
        )
        if bool(getattr(args, "json", False)):
            print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
        else:
            print(result.render())
        targeted = bool(args.source_ids or args.external_ids)
        if targeted and result.blocked_count > 0:
            return 1
        return 0


def cmd_ingest_source_show(args: argparse.Namespace) -> int:
    details = SourceRegistry.for_repo_root(vault().root).get(args.identifier)
    if details is None:
        print(f"ingest-source: not found for {args.identifier}")
        return 1
    print(f"source_key: {details.source.source_key}")
    print(f"lane: {details.source.lane}")
    print(f"adapter: {details.source.adapter}")
    print(f"title: {details.source.title}")
    print(f"source_date: {details.source.source_date or '-'}")
    print(f"status: {details.source.status}")
    print(f"page: {details.source.canonical_page_path or '-'}")
    print(f"excluded_reason: {details.source.excluded_reason or '-'}")
    print(f"blocked_reason: {details.source.blocked_reason or '-'}")
    print("aliases:")
    if not details.aliases:
        print("  (none)")
    else:
        for alias in details.aliases:
            print(f"  {alias.alias_type}\t{alias.alias}")
    print("stages:")
    if not details.stages:
        print("  (none)")
    else:
        for stage in details.stages:
            print(
                f"  {stage.stage}\tstatus={stage.status}\tfreshness={stage.freshness}\t"
                f"artifact={stage.artifact_path or '-'}\tsummary={stage.summary or '-'}"
            )
    print("artifacts:")
    if not details.artifacts:
        print("  (none)")
    else:
        for artifact in details.artifacts:
            print(
                f"  {artifact.artifact_kind}\t{artifact.path}\texists={artifact.exists}\t"
                f"fingerprint={artifact.fingerprint or '-'}"
            )
    return 0


def cmd_ingest_reconcile(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="reconciling source registry", default=True) as progress:
        progress.phase(f"reconciling {args.lane}")
        result = reconcile_source_registry(
            _inventory_request_from_args(args),
            repo_root=vault().root,
            phase_callback=progress.phase,
        )
        if bool(getattr(args, "json", False)):
            print(
                json.dumps(
                    {
                        "refreshed_count": result.refreshed_count,
                        "changed_count": result.changed_count,
                        "new_count": result.new_count,
                        "removed_count": result.removed_count,
                        "upstream_selected_count": result.upstream_selected_count,
                        "registry_matched_count": result.registry_matched_count,
                        "page_matched_count": result.page_matched_count,
                        "registry_only_count": result.registry_only_count,
                        "page_only_count": result.page_only_count,
                        "cache_only_count": result.cache_only_count,
                        "registry_only_samples": list(result.registry_only_samples),
                        "page_only_samples": list(result.page_only_samples),
                        "cache_only_samples": list(result.cache_only_samples),
                        "inventory": result.inventory.to_dict(),
                    },
                    indent=2,
                    ensure_ascii=False,
                )
            )
        else:
            print(result.render())
        return 0


def cmd_ingest_readiness(args: argparse.Namespace) -> int:
    result = run_ingest_readiness(
        repo_root=vault().root,
        dropbox_limit=args.dropbox_limit,
        lane_limit=args.lane_limit,
        include_promotion_gate=bool(args.include_promotion_gate),
    )
    print(result.render())
    return 0 if result.passed else 1


def cmd_ingest_repair_articles(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="repairing article caches", default=True) as progress:
        progress.phase("repairing article caches")
        result = run_article_repair(
            repo_root=vault().root,
            path=Path(args.path).resolve() if args.path else None,
            today=args.today,
            limit=args.limit,
            source_ids=tuple(args.source_ids or ()),
            apply=bool(args.apply),
        )
        print(render_article_repair_report(result))
        return result.exit_code


def _log_substack_failure(today: str, entry: str, *, kind: str = "substack-failures") -> None:
    append_to_inbox_log(
        target=vault().wiki / "inbox" / f"{kind}-{today}.md",
        kind=kind,
        entry=entry,
        date=today,
    )


def _log_substack_execution_failure(today: str, item: object, detail: str) -> None:
    source_id = str(getattr(item, "source_id", "") or "").removeprefix("substack-")
    title = str(getattr(item, "title", "") or source_id or "Unknown post")
    stage = str(getattr(item, "start_stage", "") or "unknown")
    _log_substack_failure(today, f"- {source_id or title} — {title} — stage={stage} — {detail}\n")


def _postprocess_substack_completed_items(*, completed_items: Sequence[object], today: str) -> tuple[list[str], int]:
    written: list[str] = []
    unsaved_refs = 0
    for completed in completed_items:
        title = str(getattr(completed, "title", "") or "Unknown post")
        source_id = str(getattr(completed, "source_id", "") or "").removeprefix("substack-")
        propagate = dict(getattr(completed, "propagate", None) or {})
        for pass_d_outcome in propagate.get("pass_d") or []:
            _log_substack_failure(
                today,
                f"- {source_id or title} — {title} — stage={pass_d_outcome['stage']} — {pass_d_outcome['summary']}\n",
            )
        for fanout_outcome in propagate.get("fanout_outcomes") or []:
            _log_substack_failure(
                today,
                f"- {source_id or title} — {title} — stage={fanout_outcome['stage']} — {fanout_outcome['summary']}\n",
            )
        unsaved_refs += int(propagate.get("unsaved_refs") or 0)
        materialized = dict(getattr(completed, "materialized_paths", None) or {})
        article_path = str(materialized.get("article") or "").strip()
        if article_path:
            written.append(Path(article_path).stem)
    return list(dict.fromkeys(written)), unsaved_refs


def ingest_substack_export(
    *,
    export_path: Path | None = None,
    today: str | None = None,
    drain_articles: bool = True,
    resume: bool = True,
    skip_materialized: bool = True,
    refresh_stale: bool = False,
    recompute_missing: bool = False,
    from_stage: str | None = None,
    through: str = "propagate",
    source_ids: tuple[str, ...] = (),
    external_ids: tuple[str, ...] = (),
    selection: tuple[str, ...] = ("all",),
    phase_callback=None,
) -> SubstackIngestResult:
    v = vault()
    effective_today = today or date.today().isoformat()
    if export_path is None:
        export_path = pull_saved(
            client=substack_auth.build_client(),
            out_dir=v.raw / "exports",
            today=effective_today,
        )
    else:
        export_path = export_path.resolve()
    initial_metadata: dict[str, object] = {
        "lane": "substack",
        "export_path": str(export_path),
        "today": effective_today,
        "drain_articles": bool(drain_articles),
    }
    state, run_id = _start_tracked_ingest_run(
        kind="substack.ingest",
        holder="mind-substack-ingest",
        metadata=initial_metadata,
    )
    tracked_phase = _tracked_phase_callback(phase_callback, state, run_id)
    try:
        tracked_phase("processing selected substack posts")
        inventory = build_inventory(
            InventoryRequest(
                lane="substack",
                path=export_path,
                today=effective_today,
                source_ids=source_ids,
                external_ids=external_ids,
                selection=selection,
            ),
            repo_root=v.memory_root,
            use_registry=True,
            phase_callback=tracked_phase,
        )
        refresh_registry_for_inventory(inventory, repo_root=v.memory_root)
        plan = build_plan(
            PlanRequest(
                lane="substack",
                path=export_path,
                today=effective_today,
                source_ids=source_ids,
                external_ids=external_ids,
                selection=selection,
                resume=resume,
                skip_materialized=skip_materialized,
                refresh_stale=refresh_stale,
                recompute_missing=recompute_missing,
                from_stage=from_stage,
                through=through,
            ),
            repo_root=v.memory_root,
            use_registry=True,
            phase_callback=tracked_phase,
        )
        execution, paywalled_entries = execute_substack_plan(
            plan,
            repo_root=v.memory_root,
            client=substack_auth.build_client(),
            saved_urls=set(),
            phase_callback=tracked_phase,
            item_callback=_planner_item_callback(
                state,
                run_id,
                today=effective_today,
                failure_logger=_log_substack_execution_failure,
            ),
        )
        written, unsaved_refs = _postprocess_substack_completed_items(
            completed_items=execution.completed_items,
            today=effective_today,
        )
        written = _update_global_index_and_changelog(
            v=v,
            action="ingest-substack",
            source_name=export_path.name,
            page_ids=written,
        )
        for entry in paywalled_entries:
            _log_substack_failure(effective_today, entry, kind="substack-paywalled")
        marker = v.wiki / "sources" / "substack" / f".ingested-{export_path.name}"
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text("", encoding="utf-8")
        linked_articles_fetched = 0
        article_failures = 0
        article_failed_items: list[str] = []
        if drain_articles:
            tracked_phase("draining linked article queue")
            article_result = ingest_articles_queue(today=effective_today, repo_root=v.memory_root)
            linked_articles_fetched = int(getattr(article_result, "fetched_summarized", 0) or 0)
            article_failures = int(getattr(article_result, "failed", 0) or 0)
            if article_failures > 0:
                article_failed_items.append(f"linked article queue: failed={article_failures}")
            state.add_run_event(
                run_id,
                stage="articles",
                event_type="completed" if article_failures == 0 else "failed",
                message=(
                    f"drop_files={getattr(article_result, 'drop_files_processed', 0)} "
                    f"fetched={getattr(article_result, 'fetched_summarized', 0)} "
                    f"failed={article_failures}"
                ),
                payload={
                    "drop_files_processed": int(getattr(article_result, "drop_files_processed", 0) or 0),
                    "linked_articles_fetched": linked_articles_fetched,
                    "failed": article_failures,
                },
            )
        result = SubstackIngestResult(
            posts_written=len(written),
            paywalled=len(paywalled_entries),
            failures=execution.failed_count + article_failures,
            unsaved_refs=unsaved_refs,
            linked_articles_fetched=linked_articles_fetched,
            export_path=export_path,
            linked_substack_followed=0,
            linked_substack_deferred=0,
            selected_count=plan.selected_count,
            skipped_materialized=plan.skipped_materialized_count,
            resumable=plan.resumable_count,
            blocked=plan.blocked_count,
            stale=plan.stale_count,
            executed=execution.executed_count,
            blocked_samples=list(execution.blocked_samples),
            failed_items=[*list(getattr(execution, "failed_items", ())), *article_failed_items],
        )
        final_metadata = {
            **initial_metadata,
            **_planner_counter_payload(
                result=result,
                pages_written=result.posts_written,
                extra={
                    "paywalled": result.paywalled,
                    "posts_written": result.posts_written,
                    "linked_articles_fetched": result.linked_articles_fetched,
                },
            ),
        }
        notes = (
            f"selected={result.selected_count} paywalled={result.paywalled} "
            f"blocked={result.blocked} executed={result.executed} "
            f"failed={result.failed} posts_written={result.posts_written}"
        )
        _finish_tracked_ingest_run(
            state,
            run_id,
            status="completed" if result.failed == 0 else "failed",
            notes=notes,
            metadata=final_metadata,
        )
        return result
    except Exception as exc:
        _fail_tracked_ingest_run(state, run_id, holder="mind-substack-ingest", exc=exc, metadata=initial_metadata)
        raise


def render_substack_ingest_result(result: SubstackIngestResult) -> str:
    lines = [(
        "ingest-substack: "
        f"selected={getattr(result, 'selected_count', 0)} "
        f"skipped_materialized={getattr(result, 'skipped_materialized', 0)} "
        f"resumable={getattr(result, 'resumable', 0)} "
        f"blocked={getattr(result, 'blocked', 0)} "
        f"stale={getattr(result, 'stale', 0)} "
        f"executed={getattr(result, 'executed', 0)} "
        f"failed={getattr(result, 'failed', getattr(result, 'failures', 0))} "
        f"paywalled={getattr(result, 'paywalled', 0)} "
        f"posts_written={getattr(result, 'posts_written', 0)}"
    )]
    blocked_samples = list(getattr(result, "blocked_samples", []) or [])
    if blocked_samples:
        lines.append("blocked_samples:")
        lines.extend(f"- {sample}" for sample in blocked_samples[:3])
    failed_items = list(getattr(result, "failed_items", []) or [])
    if failed_items:
        lines.append("failed_samples:")
        lines.extend(f"- {sample}" for sample in failed_items[:5])
    return "\n".join(lines)


def cmd_ingest_substack(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="ingesting Substack export", default=True) as progress:
        progress.phase("ingesting Substack export")
        result = ingest_substack_export(
            export_path=Path(args.path).resolve() if args.path else None,
            today=args.today,
            resume=bool(args.resume),
            skip_materialized=bool(args.skip_materialized),
            refresh_stale=bool(args.refresh_stale),
            recompute_missing=bool(args.recompute_missing),
            from_stage=args.from_stage,
            through=args.through,
            source_ids=tuple(args.source_ids or ()),
            external_ids=tuple(args.external_ids or ()),
            selection=tuple(args.selection or ("all",)),
            phase_callback=progress.phase,
        )
        progress.clear()
        print(render_substack_ingest_result(result))
        targeted = bool(args.source_ids or args.external_ids)
        if result.failed > 0:
            return 1
        if targeted and result.blocked > 0:
            return 1
        return 0


def ingest_audible_library(
    *,
    library_only: bool = False,
    sleep: float | None = None,
    force_deep: bool = False,
    resume: bool = True,
    skip_materialized: bool = True,
    refresh_stale: bool = False,
    recompute_missing: bool = False,
    from_stage: str | None = None,
    through: str = "propagate",
    source_ids: tuple[str, ...] = (),
    external_ids: tuple[str, ...] = (),
    selection: tuple[str, ...] = ("all",),
    phase_callback=None,
) -> BooksIngestResult:
    from scripts.audible import pull as audible_pull

    argv: list[str] = []
    if library_only:
        argv.append("--library-only")
    if sleep is not None:
        argv.extend(["--sleep", str(sleep)])
    if phase_callback is not None:
        phase_callback("pulling Audible library export")
    rc = audible_pull.main(argv)
    if rc != 0:
        raise RuntimeError(f"audible pull failed with exit_code={rc}")
    export_candidates = sorted(vault().raw.glob("exports/audible-library-*.json"))
    if not export_candidates:
        raise RuntimeError("mind ingest audible: no audible-library export found after pull")
    latest = export_candidates[-1]
    return ingest_books_export(
        latest,
        force_deep=force_deep,
        resume=resume,
        skip_materialized=skip_materialized,
        refresh_stale=refresh_stale,
        recompute_missing=recompute_missing,
        from_stage=from_stage,
        through=through,
        source_ids=source_ids,
        external_ids=external_ids,
        selection=selection,
        phase_callback=phase_callback,
    )


def cmd_ingest_audible(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="ingesting Audible library", default=True) as progress:
        progress.phase("pulling Audible library export")
        result = ingest_audible_library(
            library_only=bool(args.library_only),
            sleep=args.sleep,
            force_deep=bool(args.force_deep),
            resume=bool(args.resume),
            skip_materialized=bool(args.skip_materialized),
            refresh_stale=bool(args.refresh_stale),
            recompute_missing=bool(args.recompute_missing),
            from_stage=args.from_stage,
            through=args.through,
            source_ids=tuple(args.source_ids or ()),
            external_ids=tuple(args.external_ids or ()),
            selection=tuple(args.selection or ("all",)),
            phase_callback=progress.phase,
        )
        progress.clear()
        print(render_books_ingest_result(result).replace("ingest-books:", "ingest-audible:", 1))
        targeted = bool(args.source_ids or args.external_ids)
        if result.failed > 0:
            return 1
        if targeted and result.blocked > 0:
            return 1
        return 0


def import_links(
    path: Path,
    *,
    today: str | None = None,
    ingest: bool = False,
):
    from scripts.links.importer import append_links_drop, load_links

    root = vault().memory_root
    effective_today = today or date.today().isoformat()
    links = load_links(path)
    append_links_drop(root, links=links, today_str=effective_today)
    if ingest:
        result = ingest_articles_queue(today=effective_today, repo_root=root)
        return len(links), result
    return len(links), None


def cmd_ingest_links(args: argparse.Namespace) -> int:
    imported, result = import_links(
        Path(args.path),
        today=args.today,
        ingest=bool(args.ingest),
    )
    if result is not None:
        print(f"links-ingest: {imported} links imported -> {result.fetched_summarized} fetched")
        return 0 if result.failed == 0 else 1
    print(f"links-import: {imported} links")
    return 0


def scan_chrome(
    *,
    today: str | None = None,
    repo_root: Path | None = None,
    selected_profiles: list[str] | None = None,
    since_days: int | None = None,
) -> ChromeScanCommandResult:
    root = repo_root or vault().memory_root
    effective_today = today or date.today().isoformat()
    scan_result = scan_chrome_profiles(
        repo_root=root,
        selected_profiles=selected_profiles,
        since_days=since_days,
    )
    cfg = Vault.load(root).config
    event_files, query_files = write_scan_outputs(
        root,
        scan_result,
        today_str=effective_today,
        raw_query_retention_days=cfg.chrome.raw_query_retention_days,
    )
    return ChromeScanCommandResult(
        event_files=event_files,
        query_files=query_files,
        events_scanned=len(scan_result.events),
    )


def ingest_chrome(
    *,
    today: str | None = None,
    repo_root: Path | None = None,
    selected_profiles: list[str] | None = None,
    since_days: int | None = None,
) -> ChromeIngestCommandResult:
    root = repo_root or vault().memory_root
    effective_today = today or date.today().isoformat()
    scan_result = scan_chrome_profiles(
        repo_root=root,
        selected_profiles=selected_profiles,
        since_days=since_days,
    )
    cfg = Vault.load(root).config
    write_scan_outputs(
        root,
        scan_result,
        today_str=effective_today,
        raw_query_retention_days=cfg.chrome.raw_query_retention_days,
    )
    candidates = build_web_candidates(scan_result.events, repo_root=root)
    search_signals = build_retained_search_signals(scan_result.events)
    candidate_drop_path = write_web_discovery_drop(root, candidates=candidates, today_str=effective_today)
    search_signal_drop_path = write_search_signal_drop(root, search_signals=search_signals, today_str=effective_today)
    return ChromeIngestCommandResult(
        raw_events_seen=len(scan_result.events),
        candidates_written=len(candidates),
        search_signals_written=len(search_signals),
        candidate_drop_path=candidate_drop_path,
        search_signal_drop_path=search_signal_drop_path,
    )


def ingest_search_signals(
    *,
    today: str | None = None,
    repo_root: Path | None = None,
) -> SearchSignalsIngestResult:
    root = repo_root or vault().memory_root
    effective_today = today or date.today().isoformat()
    return ingest_search_signal_drop_files(root, today_str=effective_today)


def drain_web_discovery(
    *,
    today: str | None = None,
    repo_root: Path | None = None,
) -> WebDiscoveryDrainResult:
    root = repo_root or vault().memory_root
    effective_today = today or date.today().isoformat()
    return drain_web_discovery_drop_queue(repo_root=root, today_str=effective_today)
