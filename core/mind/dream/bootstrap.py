from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import math
import re

from mind.services.content_policy import working_set_domains
from scripts.articles.enrich import (
    apply_article_to_you,
    build_article_attribution,
    normalize_article_source,
    slugify_url,
    run_pass_d_for_article,
)
from scripts.articles.fetch import ArticleFetchResult
from scripts.articles.parse import ArticleDropEntry
from scripts.atoms.pass_d import pass_d_cache_exists
from scripts.books.enrich import apply_to_you as apply_book_to_you
from scripts.books.enrich import normalize_book_source, run_pass_d_for_book, update_author_memory
from scripts.books.parse import BookRecord
from scripts.common.stance import load_stance_context
from scripts.common.vault import Vault
from scripts.substack.enrich import apply_post_to_you, get_prior_posts_context, run_pass_d_for_substack, update_author_stance
from scripts.substack.parse import SubstackRecord
from scripts.youtube.enrich import apply_video_to_you, build_channel_attribution, run_pass_d_for_youtube
from scripts.youtube.parse import YouTubeRecord

from .common import (
    DreamPreconditionError,
    DreamResult,
    dream_run,
    ensure_dream_enabled,
    ensure_onboarded,
    extract_wikilinks,
    maybe_locked,
    read_page,
    runtime_state,
    section_body,
    source_pages,
    summary_snippet,
    today_str,
    vault,
)
from .deep import run_deep
from .quality import evaluate_and_persist_quality, lane_state_for_frontmatter

from scripts.common.wikilinks import WIKILINK_DISPLAY_RE as WIKILINK_RE

BOOTSTRAP_ADAPTER = "dream.bootstrap"
BOOTSTRAP_REPORT_DIR = ("raw", "reports", "dream", "bootstrap")


@dataclass(frozen=True)
class BootstrapSource:
    summary_id: str
    source_kind: str
    source_date: str
    summary_path: Path
    source_page_path: Path | None = None


def _normalize_source_kind(value: str) -> str:
    kind = (value or "").strip().lower()
    if kind == "video":
        return "youtube"
    return kind


def _resolve_source_page(v: Vault, frontmatter: dict, body: str) -> Path | None:
    source_path = str(frontmatter.get("source_path") or "").strip()
    if source_path:
        candidate = v.resolve_logical_path(source_path).resolve()
        try:
            candidate.relative_to(v.wiki / "sources")
        except ValueError:
            pass
        else:
            if candidate.is_file():
                return candidate
    for link in frontmatter.get("relates_to") or []:
        for page_id in extract_wikilinks(str(link)):
            matches = sorted((v.wiki / "sources").rglob(f"{page_id}.md"))
            if matches:
                return matches[0]
    return None


def _sort_key(source: BootstrapSource) -> tuple[str, str]:
    return (source.source_date or "9999-12-31", source.summary_id)


def enumerate_bootstrap_sources(v: Vault) -> list[BootstrapSource]:
    sources: list[BootstrapSource] = []
    for path in source_pages(v):
        if not path.is_file():
            continue
        frontmatter, body = read_page(path)
        kind = _normalize_source_kind(str(frontmatter.get("source_type") or ""))
        if kind in {"", "onboarding"}:
            continue
        summary_id = str(frontmatter.get("id") or path.stem)
        source_date = str(frontmatter.get("source_date") or frontmatter.get("created") or "")
        sources.append(
            BootstrapSource(
                summary_id=summary_id,
                source_kind=kind,
                source_date=source_date,
                summary_path=path,
                source_page_path=path,
            )
        )
    return sorted(sources, key=_sort_key)


def _summary_topics(frontmatter: dict, body: str) -> list[str]:
    topics = set()
    topics.update(str(item).strip() for item in frontmatter.get("topics") or [] if str(item).strip())
    for key in ("concepts", "entities", "relates_to"):
        for link in frontmatter.get(key) or []:
            topics.update(extract_wikilinks(str(link)))
    topics_section = section_body(body, "Topics")
    topics.update(extract_wikilinks(topics_section))
    for line in topics_section.splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            topics.add(stripped[2:].strip())
    return sorted(item for item in topics if item)


def _source_domains(frontmatter: dict) -> list[str]:
    return working_set_domains(frontmatter)


def _summary_payload(frontmatter: dict, body: str) -> dict[str, object]:
    return {
        "id": str(frontmatter.get("id") or ""),
        "title": str(frontmatter.get("title") or ""),
        "tldr": section_body(body, "TL;DR") or summary_snippet(body),
        "topics": _summary_topics(frontmatter, body),
    }


def _read_primary_content(source: BootstrapSource) -> tuple[dict, str]:
    if source.source_page_path is not None and source.source_page_path.exists():
        return read_page(source.source_page_path)
    return read_page(source.summary_path)


def _plain_text(value: object) -> str:
    text = str(value or "").strip()
    match = WIKILINK_RE.fullmatch(text)
    if match:
        return (match.group(2) or match.group(1) or "").strip()
    return text


def _metadata_line(body: str, label: str) -> str:
    prefix = f"**{label}:**"
    for line in body.splitlines():
        if line.startswith(prefix):
            return _plain_text(line.split(prefix, 1)[1].strip())
    return ""


def _slugify_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")


def _replay_article_source(
    source: BootstrapSource,
    *,
    summary_frontmatter: dict,
    summary_body: str,
    source_frontmatter: dict,
    source_body: str,
    repo_root: Path,
    execution_date: str,
    force_pass_d: bool,
) -> dict[str, int | bool]:
    entry = ArticleDropEntry(
        url=str(source_frontmatter.get("source_url") or ""),
        source_post_id=source.summary_id,
        source_post_url=str(source_frontmatter.get("source_url") or ""),
        anchor_text=str(source_frontmatter.get("title") or summary_frontmatter.get("title") or ""),
        context_snippet=summary_snippet(summary_body),
        category="personal",
        discovered_at=str(summary_frontmatter.get("ingested") or summary_frontmatter.get("created") or execution_date),
        source_type="bootstrap-replay",
        source_label="bootstrap",
    )
    fetch_result = ArticleFetchResult(
        body_text=source_body,
        title=str(source_frontmatter.get("title") or "") or None,
        author=_metadata_line(source_body, "Author") or _plain_text(source_frontmatter.get("author")),
        sitename=_metadata_line(source_body, "Publication") or _plain_text(source_frontmatter.get("outlet")),
        published=str(source_frontmatter.get("published") or summary_frontmatter.get("source_date") or "") or None,
        raw_html_path=source.source_page_path or source.summary_path,
    )
    article_source = normalize_article_source(entry, fetch_result=fetch_result)
    summary_payload = _summary_payload(summary_frontmatter, summary_body)
    applied = apply_article_to_you(entry, fetch_result=fetch_result, summary=summary_payload, repo_root=repo_root)
    attribution = build_article_attribution(
        entry,
        fetch_result=fetch_result,
        source=article_source,
        summary=summary_payload,
        repo_root=repo_root,
    )
    return run_pass_d_for_article(
        entry,
        body_text=source_body,
        summary=summary_payload,
        applied=applied,
        attribution=attribution,
        repo_root=repo_root,
        today=execution_date,
        cache_mode="bootstrap",
        evidence_date=source.source_date or execution_date,
        force_refresh=force_pass_d,
    )


def _replay_book_source(
    source: BootstrapSource,
    *,
    summary_frontmatter: dict,
    summary_body: str,
    repo_root: Path,
    execution_date: str,
    force_pass_d: bool,
) -> dict[str, int | bool]:
    authors = []
    for item in summary_frontmatter.get("author") or []:
        text = _plain_text(item)
        if text:
            authors.append(text)
    book = BookRecord(
        title=str(summary_frontmatter.get("title") or source.summary_id).replace("Book: ", "", 1),
        author=authors,
        publisher=_plain_text(summary_frontmatter.get("publisher")),
        finished_date=str(summary_frontmatter.get("source_date") or ""),
        started_date=str(summary_frontmatter.get("source_date") or ""),
        format="audiobook" if str(summary_frontmatter.get("external_id") or "").startswith("audible-") else "ebook",
        asin=str(summary_frontmatter.get("external_id") or "").removeprefix("audible-"),
    )
    summary_payload = _summary_payload(summary_frontmatter, summary_body)
    apply_payload = apply_book_to_you(book, summary_artifact=summary_payload)
    attribution = update_author_memory(book, summary_artifact=summary_payload, repo_root=repo_root)
    book_source = normalize_book_source(book, classification={"category": "personal"}, research=summary_payload)
    return run_pass_d_for_book(
        book,
        body_or_transcript=book_source.primary_content,
        summary_artifact=summary_payload,
        applied=apply_payload,
        attribution=attribution,
        repo_root=repo_root,
        today=execution_date,
        cache_mode="bootstrap",
        evidence_date=source.source_date or execution_date,
        force_refresh=force_pass_d,
    )


def _replay_substack_source(
    source: BootstrapSource,
    *,
    summary_frontmatter: dict,
    summary_body: str,
    source_frontmatter: dict,
    source_body: str,
    repo_root: Path,
    execution_date: str,
    force_pass_d: bool,
) -> dict[str, int | bool]:
    record = SubstackRecord(
        id=str(summary_frontmatter.get("external_id") or "").removeprefix("substack-") or source.summary_id.removeprefix("summary-substack-"),
        title=str(source_frontmatter.get("title") or summary_frontmatter.get("title") or "").replace("Summary — ", "", 1),
        subtitle=None,
        slug=str(source.source_page_path.stem if source.source_page_path is not None else source.summary_id),
        published_at=str(source_frontmatter.get("published") or summary_frontmatter.get("source_date") or ""),
        saved_at=str(source_frontmatter.get("saved_at") or summary_frontmatter.get("ingested") or execution_date),
        url=str(source_frontmatter.get("source_url") or ""),
        author_name=_metadata_line(source_body, "Author") or _plain_text(source_frontmatter.get("author")),
        author_id="",
        publication_name=_metadata_line(source_body, "Publication") or _plain_text(source_frontmatter.get("outlet")),
        publication_slug=str(source.source_page_path.parent.name if source.source_page_path is not None else ""),
        body_html=None,
        is_paywalled=False,
    )
    summary_payload = _summary_payload(summary_frontmatter, summary_body)
    applied = apply_post_to_you(record, summary=summary_payload, repo_root=repo_root)
    stance_change_note = update_author_stance(record, summary=summary_payload, repo_root=repo_root)
    prior_context = get_prior_posts_context(record, repo_root)
    return run_pass_d_for_substack(
        record,
        body_markdown=source_body,
        summary=summary_payload,
        applied=applied,
        stance_change_note=stance_change_note,
        stance_context_text=load_stance_context(
            slug=_slugify_name(_plain_text(source_frontmatter.get("author")) or record.author_name),
            kind="person",
            repo_root=repo_root,
        ),
        prior_context=prior_context,
        repo_root=repo_root,
        today=execution_date,
        cache_mode="bootstrap",
        evidence_date=source.source_date or execution_date,
        force_refresh=force_pass_d,
    )


def _replay_youtube_source(
    source: BootstrapSource,
    *,
    summary_frontmatter: dict,
    summary_body: str,
    source_body: str,
    repo_root: Path,
    execution_date: str,
    force_pass_d: bool,
) -> dict[str, int | bool]:
    video_id = str(summary_frontmatter.get("external_id") or "").removeprefix("youtube-") or source.summary_id.removeprefix("summary-yt-")
    record = YouTubeRecord(
        video_id=video_id,
        title=str(summary_frontmatter.get("title") or source.summary_id).replace("YouTube: ", "", 1),
        channel=_plain_text(summary_frontmatter.get("channel")),
        watched_at=str(summary_frontmatter.get("source_date") or execution_date),
    )
    summary_payload = _summary_payload(summary_frontmatter, summary_body)
    apply_payload = apply_video_to_you(record, summary=summary_payload, repo_root=repo_root)
    attribution = build_channel_attribution(record, summary=summary_payload, repo_root=repo_root)
    return run_pass_d_for_youtube(
        record,
        transcript=source_body,
        summary=summary_payload,
        applied=apply_payload,
        attribution=attribution,
        repo_root=repo_root,
        today=execution_date,
        cache_mode="bootstrap",
        evidence_date=source.source_date or execution_date,
        force_refresh=force_pass_d,
    )


def replay_bootstrap_source(
    source: BootstrapSource,
    *,
    repo_root: Path,
    force_pass_d: bool,
    execution_date: str,
) -> dict[str, int | bool]:
    summary_frontmatter, summary_body = read_page(source.summary_path)
    source_frontmatter, source_body = _read_primary_content(source)
    if source.source_kind == "article":
        return _replay_article_source(
            source,
            summary_frontmatter=summary_frontmatter,
            summary_body=summary_body,
            source_frontmatter=source_frontmatter,
            source_body=source_body,
            repo_root=repo_root,
            execution_date=execution_date,
            force_pass_d=force_pass_d,
        )
    if source.source_kind == "book":
        return _replay_book_source(
            source,
            summary_frontmatter=summary_frontmatter,
            summary_body=summary_body,
            repo_root=repo_root,
            execution_date=execution_date,
            force_pass_d=force_pass_d,
        )
    if source.source_kind == "substack":
        return _replay_substack_source(
            source,
            summary_frontmatter=summary_frontmatter,
            summary_body=summary_body,
            source_frontmatter=source_frontmatter,
            source_body=source_body,
            repo_root=repo_root,
            execution_date=execution_date,
            force_pass_d=force_pass_d,
        )
    return _replay_youtube_source(
        source,
        summary_frontmatter=summary_frontmatter,
        summary_body=summary_body,
        source_body=source_body,
        repo_root=repo_root,
        execution_date=execution_date,
        force_pass_d=force_pass_d,
    )


def _bootstrap_pass_d_source_id(
    source: BootstrapSource,
    *,
    summary_frontmatter: dict,
    source_frontmatter: dict,
) -> str:
    if source.source_kind == "article":
        discovered_at = str(summary_frontmatter.get("ingested") or summary_frontmatter.get("created") or source.source_date or "")
        slug = slugify_url(str(source_frontmatter.get("source_url") or ""), discovered_at)
        return f"article-{slug}"
    if source.source_kind == "book":
        authors = []
        for item in summary_frontmatter.get("author") or []:
            text = _plain_text(item)
            if text:
                authors.append(text)
        author_slug = _slugify_name(authors[0]) if authors else "unknown"
        title_slug = _slugify_name(str(summary_frontmatter.get("title") or source.summary_id).replace("Book: ", "", 1))
        return f"book-{author_slug}-{title_slug}"
    if source.source_kind == "substack":
        record_id = str(summary_frontmatter.get("external_id") or "").removeprefix("substack-") or source.summary_id.removeprefix("summary-substack-")
        return f"substack-{record_id}"
    video_id = str(summary_frontmatter.get("external_id") or "").removeprefix("youtube-") or source.summary_id.removeprefix("summary-yt-")
    return f"youtube-{video_id}"


def run_bootstrap_checkpoint(*, dry_run: bool) -> DreamResult:
    return run_deep(
        dry_run=dry_run,
        acquire_lock=False,
        write_digest=False,
        update_runtime_state=False,
    )


def _select_sources(
    *,
    all_sources: list[BootstrapSource],
    progress: dict | None,
    limit: int | None,
    resume: bool,
) -> list[BootstrapSource]:
    if resume:
        if not progress or not progress.get("planned_source_ids"):
            raise DreamPreconditionError("mind dream bootstrap --resume: no resumable bootstrap state found")
        planned_source_ids = [str(source_id) for source_id in progress.get("planned_source_ids") or []]
        by_id = {source.summary_id: source for source in all_sources}
        if str(progress.get("status") or "") != "completed":
            return [by_id[source_id] for source_id in planned_source_ids if source_id in by_id]
        batch_size = limit if limit is not None else len(planned_source_ids)
        last_source_id = planned_source_ids[-1]
        if batch_size <= 0 or last_source_id not in by_id:
            raise DreamPreconditionError("mind dream bootstrap --resume: no remaining bootstrap sources")
        start_index = next(
            index + 1 for index, source in enumerate(all_sources) if source.summary_id == last_source_id
        )
        selected = all_sources[start_index : start_index + batch_size]
        if not selected:
            raise DreamPreconditionError("mind dream bootstrap --resume: no remaining bootstrap sources")
        return selected
    if limit is None:
        return all_sources
    return all_sources[:limit]


def _checkpoint_plan(source_count: int, checkpoint_every: int | None) -> int:
    if not checkpoint_every or checkpoint_every <= 0 or source_count <= checkpoint_every:
        return 0
    return math.floor((source_count - 1) / checkpoint_every)


def _resume_starts_new_batch(*, progress: dict | None, selected_sources: list[BootstrapSource], resume: bool) -> bool:
    if not resume or not progress:
        return False
    if str(progress.get("status") or "") != "completed":
        return False
    selected_ids = [source.summary_id for source in selected_sources]
    previous_ids = [str(source_id) for source_id in progress.get("planned_source_ids") or []]
    return selected_ids != previous_ids


def _progress_payload(
    *,
    sources: list[BootstrapSource],
    checkpoint_every: int | None,
    force_pass_d: bool,
    report_path: str | None = None,
) -> dict:
    return {
        "status": "running",
        "planned_source_ids": [source.summary_id for source in sources],
        "completed_source_ids": [],
        "checkpoint_every": checkpoint_every,
        "force_pass_d": force_pass_d,
        "cache_reuses": 0,
        "evidence_updates": 0,
        "probationary_updates": 0,
        "evidence_updates_by_state": {},
        "probationary_updates_by_state": {},
        "checkpoints": [],
        "report_path": report_path,
    }


def _write_bootstrap_report(
    v: Vault,
    *,
    run_id: int,
    execution_date: str,
    processed_sources: int,
    cache_reuses: int,
    evidence_updates: int,
    probationary_updates: int,
    checkpoints: list[str],
    lane_counts: dict[str, int],
    blocked_sources: list[str],
    quality_report_path: str | None,
    evidence_updates_by_state: dict[str, int],
    probationary_updates_by_state: dict[str, int],
) -> Path:
    report_dir = v.root.joinpath(*BOOTSTRAP_REPORT_DIR)
    report_dir.mkdir(parents=True, exist_ok=True)
    target = report_dir / f"{execution_date}-bootstrap-report-run-{run_id}.md"
    lines = [
        "# Dream Bootstrap Report",
        "",
        f"- Run date: {execution_date}",
        f"- Sources replayed: {processed_sources}",
        f"- Bootstrap cache reuses: {cache_reuses}",
        f"- Evidence updates: {evidence_updates}",
        f"- Probationary updates: {probationary_updates}",
        f"- Trusted sources replayed: {lane_counts.get('trusted', 0)}",
        f"- Partial-fidelity sources replayed: {lane_counts.get('partial-fidelity', 0)}",
        f"- Bootstrap-only sources replayed: {lane_counts.get('bootstrap-only', 0)}",
        f"- Blocked sources skipped: {len(blocked_sources)}",
        f"- Quality report: {quality_report_path or 'not persisted'}",
        "",
        "## Update Quality Split",
        "",
        f"- Trusted evidence updates: {evidence_updates_by_state.get('trusted', 0)}",
        f"- Degraded evidence updates: {evidence_updates_by_state.get('partial-fidelity', 0) + evidence_updates_by_state.get('bootstrap-only', 0)}",
        f"- Trusted probationary updates: {probationary_updates_by_state.get('trusted', 0)}",
        f"- Degraded probationary updates: {probationary_updates_by_state.get('partial-fidelity', 0) + probationary_updates_by_state.get('bootstrap-only', 0)}",
        "",
        "## Checkpoints",
        "",
    ]
    if checkpoints:
        lines.extend(f"- {item}" for item in checkpoints)
    else:
        lines.append("- None")
    lines.extend(["", "## Blocked Sources", ""])
    if blocked_sources:
        lines.extend(f"- {item}" for item in blocked_sources)
    else:
        lines.append("- None")
    target.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return target


def run_bootstrap(
    *,
    dry_run: bool,
    force_pass_d: bool,
    checkpoint_every: int | None,
    resume: bool,
    limit: int | None,
) -> DreamResult:
    ensure_dream_enabled()
    ensure_onboarded()
    v = vault()
    state = runtime_state()
    execution_date = today_str()
    progress = state.get_adapter_state(BOOTSTRAP_ADAPTER)
    effective_checkpoint_every = checkpoint_every if checkpoint_every is not None else (
        int(progress.get("checkpoint_every")) if resume and progress and progress.get("checkpoint_every") else None
    )
    effective_force_pass_d = force_pass_d
    all_sources = enumerate_bootstrap_sources(v)
    selected_sources = _select_sources(all_sources=all_sources, progress=progress, limit=limit, resume=resume)
    quality = evaluate_and_persist_quality(persist=not dry_run, report_key="bootstrap")
    checkpoint_count = _checkpoint_plan(len(selected_sources), effective_checkpoint_every)
    mutations: list[str] = []
    warnings: list[str] = []
    blocked_sources: list[str] = []
    lane_counts = {"trusted": 0, "partial-fidelity": 0, "bootstrap-only": 0}
    source_states: dict[str, str] = {}

    gated_sources: list[BootstrapSource] = []
    for source in selected_sources:
        summary_frontmatter, _summary_body = read_page(source.summary_path)
        lane_state = lane_state_for_frontmatter(summary_frontmatter, quality)
        if lane_state == "blocked":
            blocked_sources.append(source.summary_id)
            warnings.append(f"bootstrap skipped blocked lane source {source.summary_id}")
            continue
        if lane_state in lane_counts:
            lane_counts[lane_state] += 1
        else:
            lane_counts["trusted"] += 1
        source_states[source.summary_id] = lane_state
        if lane_state == "bootstrap-only":
            warnings.append(f"bootstrap replay explicitly included bootstrap-only lane source {source.summary_id}")
        gated_sources.append(source)
    selected_sources = gated_sources

    if dry_run:
        cache_reuses = 0
        for source in selected_sources:
            summary_frontmatter, _summary_body = read_page(source.summary_path)
            source_frontmatter, _source_body = _read_primary_content(source)
            cache_exists = pass_d_cache_exists(
                repo_root=v.root,
                source_kind=source.source_kind,
                source_id=_bootstrap_pass_d_source_id(
                    source,
                    summary_frontmatter=summary_frontmatter,
                    source_frontmatter=source_frontmatter,
                ),
                cache_mode="bootstrap",
            )
            if cache_exists:
                cache_reuses += 1
        summary = (
            f"Bootstrap rehearsal planned for {len(selected_sources)} sources; "
            f"checkpoint plan={checkpoint_count}, cache mode="
            f"{'force-refresh' if effective_force_pass_d else 'reuse-if-present'}."
        )
        mutations.extend(
            [
                f"would replay {len(selected_sources)} sources in chronological order",
                f"would run {checkpoint_count} Deep checkpoints",
                (
                    f"would bypass {cache_reuses} existing Pass D caches"
                    if effective_force_pass_d
                    else f"would reuse up to {cache_reuses} existing Pass D caches"
                ),
            ]
        )
        return DreamResult(stage="bootstrap", dry_run=True, summary=summary, mutations=mutations, warnings=warnings)

    if not selected_sources:
        raise DreamPreconditionError("mind dream bootstrap: all selected canonical lane sources are blocked by quality gates")

    if not resume or _resume_starts_new_batch(progress=progress, selected_sources=selected_sources, resume=resume):
        progress = _progress_payload(
            sources=selected_sources,
            checkpoint_every=effective_checkpoint_every,
            force_pass_d=effective_force_pass_d,
        )
    else:
        progress = progress or _progress_payload(
            sources=selected_sources,
            checkpoint_every=effective_checkpoint_every,
            force_pass_d=effective_force_pass_d,
        )
    completed_ids = set(progress.get("completed_source_ids") or [])
    progress["planned_source_ids"] = [source.summary_id for source in selected_sources]
    progress["checkpoint_every"] = effective_checkpoint_every
    progress["force_pass_d"] = effective_force_pass_d
    processed_sources = len(completed_ids)
    evidence_updates = int(progress.get("evidence_updates") or 0)
    probationary_updates = int(progress.get("probationary_updates") or 0)
    cache_reuses = int(progress.get("cache_reuses") or 0)
    checkpoints = list(progress.get("checkpoints") or [])
    evidence_updates_by_state = {state: int((progress.get("evidence_updates_by_state") or {}).get(state) or 0) for state in ("trusted", "partial-fidelity", "bootstrap-only")}
    probationary_updates_by_state = {state: int((progress.get("probationary_updates_by_state") or {}).get(state) or 0) for state in ("trusted", "partial-fidelity", "bootstrap-only")}

    with dream_run("bootstrap", dry_run=False) as (runtime, run_id):
        runtime.add_run_event(
            run_id,
            stage="bootstrap",
            event_type="selected",
            message=f"{len(selected_sources)} sources planned",
        )
        with maybe_locked("bootstrap", dry_run=False):
            state.upsert_adapter_state(adapter=BOOTSTRAP_ADAPTER, state=progress)
            try:
                for source in selected_sources:
                    if source.summary_id in completed_ids:
                        continue
                    result = replay_bootstrap_source(
                        source,
                        repo_root=v.root,
                        force_pass_d=effective_force_pass_d,
                        execution_date=execution_date,
                    )
                    if result.get("error"):
                        warnings.append(
                            f"{source.summary_id}: {result.get('error_stage') or 'replay'} {result['error']}"
                        )
                    processed_sources += 1
                    evidence_updates += int(result["evidence_updates"])
                    probationary_updates += int(result["probationary_updates"])
                    lane_state = source_states.get(source.summary_id, "trusted")
                    if lane_state not in evidence_updates_by_state:
                        lane_state = "trusted"
                    evidence_updates_by_state[lane_state] += int(result["evidence_updates"])
                    probationary_updates_by_state[lane_state] += int(result["probationary_updates"])
                    cache_reuses += int(bool(result["cache_reused"]))
                    progress["cache_reuses"] = cache_reuses
                    progress["evidence_updates"] = evidence_updates
                    progress["probationary_updates"] = probationary_updates
                    progress["evidence_updates_by_state"] = evidence_updates_by_state
                    progress["probationary_updates_by_state"] = probationary_updates_by_state
                    completed_ids.add(source.summary_id)
                    progress["completed_source_ids"] = sorted(completed_ids)
                    runtime.add_run_event(
                        run_id,
                        stage="bootstrap",
                        event_type="replayed",
                        message=source.summary_id,
                        payload={
                            "cache_reused": bool(result["cache_reused"]),
                            "evidence_updates": int(result["evidence_updates"]),
                            "probationary_updates": int(result["probationary_updates"]),
                        },
                    )
                    if (
                        effective_checkpoint_every
                        and processed_sources < len(selected_sources)
                        and processed_sources % effective_checkpoint_every == 0
                    ):
                        checkpoint_result = run_bootstrap_checkpoint(dry_run=False)
                        checkpoints.append(checkpoint_result.summary)
                        progress["checkpoints"] = checkpoints
                        runtime.add_run_event(
                            run_id,
                            stage="bootstrap",
                            event_type="checkpoint",
                            message=checkpoint_result.summary,
                        )
                    state.upsert_adapter_state(adapter=BOOTSTRAP_ADAPTER, state=progress)
            except Exception as exc:
                progress["status"] = "interrupted"
                progress["last_error"] = str(exc)
                state.upsert_adapter_state(adapter=BOOTSTRAP_ADAPTER, state=progress)
                raise

            report_path = _write_bootstrap_report(
                v,
                run_id=run_id,
                execution_date=execution_date,
                processed_sources=processed_sources,
                cache_reuses=cache_reuses,
                evidence_updates=evidence_updates,
                probationary_updates=probationary_updates,
                checkpoints=checkpoints,
                lane_counts=lane_counts,
                blocked_sources=blocked_sources,
                quality_report_path=str(quality.get("report_path") or ""),
                evidence_updates_by_state=evidence_updates_by_state,
                probationary_updates_by_state=probationary_updates_by_state,
            )
            progress["status"] = "completed"
            progress["report_path"] = v.logical_path(report_path)
            state.upsert_adapter_state(adapter=BOOTSTRAP_ADAPTER, state=progress)
            mutations.append(f"wrote {v.logical_path(report_path)}")
    summary = (
        f"Bootstrap replay processed {processed_sources} sources, "
        f"{evidence_updates} evidence updates, {probationary_updates} probationary updates, "
        f"{len(blocked_sources)} blocked skips, and {len(checkpoints)} Deep checkpoints."
    )
    return DreamResult(stage="bootstrap", dry_run=False, summary=summary, mutations=mutations, warnings=warnings)
