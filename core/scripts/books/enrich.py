"""Enrich a BookRecord through the routed LLM service layer.

Cached: writes raw/research/books/<slug>.json on first run, returns from cache on subsequent runs.
"""
from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
import subprocess
from typing import Any

from mind.services.content_policy import (
    canonical_policy_fields,
    compatibility_category,
    content_policy_from_classification,
    normalize_book_classification,
    should_materialize,
    should_run_deep_synthesis,
    working_set_domains,
)
from mind.services.document_text import extract_document_text
from mind.services.ingest_contract import IngestionLifecycleResult, NormalizedSource, run_ingestion_lifecycle
from mind.services.quality_receipts import write_quality_receipt
from mind.services.llm_cache import load_llm_cache, write_llm_cache
from mind.services.llm_service import get_llm_service
from mind.services.materialization import MaterializationCandidate, select_primary_targets
from mind.services.providers.base import LLMInputPart
from mind.services.prompt_builders import (
    APPLIED_TO_YOU_PROMPT_VERSION,
    CLASSIFY_BOOK_PROMPT_VERSION,
    RESEARCH_BOOK_DEEP_PROMPT_VERSION,
    RESEARCH_BOOK_PROMPT_VERSION,
    SUMMARIZE_BOOK_RESEARCH_PROMPT_VERSION,
    UPDATE_AUTHOR_STANCE_PROMPT_VERSION,
)
from mind.services.llm_service import get_llm_service as _get_llm_service
from scripts.common import env
from scripts.common.entity_log import log_entities as log_source_entities
from scripts.common.profile import load_profile_context
from scripts.common.quote_verify import verify_quotes as verify_source_quotes
from scripts.common.source_context import build_prior_context, strip_wiki_link
from scripts.common.stance import load_stance_context
from scripts.common.vault import raw_path, wiki_path
from scripts.books.parse import BookRecord

def slugify(text: str, max_len: int = 80) -> str:
    """Thin wrapper around scripts.common.slugify."""
    from scripts.common.slugify import slugify as _slugify
    return _slugify(text, max_len=max_len)


def research_path(repo_root: Path, book: BookRecord) -> Path:
    author_slug = slugify(book.author[0]) if book.author else "unknown"
    title_slug = slugify(book.title)
    return raw_path(repo_root, "research", "books", f"{author_slug}-{title_slug}.json")


def summary_path(repo_root: Path, book: BookRecord) -> Path:
    author_slug = slugify(book.author[0]) if book.author else "unknown"
    title_slug = slugify(book.title)
    return raw_path(repo_root, "research", "books", f"{author_slug}-{title_slug}.summary.json")


def classification_path(repo_root: Path, book: BookRecord) -> Path:
    author_slug = slugify(book.author[0]) if book.author else "unknown"
    title_slug = slugify(book.title)
    return raw_path(repo_root, "research", "books", f"{author_slug}-{title_slug}.classification.json")


def classify(book: BookRecord) -> dict[str, Any]:
    """Classify a book into the canonical content policy shape. Cached on disk.

    Mirror of scripts.youtube.enrich.classify().
    """
    cfg = env.load()
    target = classification_path(cfg.repo_root, book)
    identities = get_llm_service().cache_identities(task_class="classification", prompt_version=CLASSIFY_BOOK_PROMPT_VERSION)
    identity = identities[0]
    cached = load_llm_cache(target, expected=identities)
    if isinstance(cached, dict):
        return normalize_book_classification(cached)
    author = book.author[0] if book.author else "unknown"
    result_or_tuple = get_llm_service().classify_book(title=book.title, author=author, with_meta=True)
    if isinstance(result_or_tuple, tuple):
        response, identity = result_or_tuple
    else:
        response = result_or_tuple
    normalized = normalize_book_classification(response)
    write_llm_cache(target, identity=identity, data=normalized)
    return normalized


def force_deep_classification(classification: dict[str, Any] | None) -> dict[str, Any]:
    """Override a book classification to materialize with deep synthesis.

    This preserves the existing domain/category hinting where possible, but
    forces retention to ``keep`` and synthesis to ``deep`` for the current run.
    """
    payload = dict(classification or {})
    policy = content_policy_from_classification(payload)
    domains = list(policy.domains) or ["personal"]
    category = str(payload.get("category") or "").strip().lower()
    if category == "ignore":
        category = "business" if "business" in domains else "personal"
    normalized = {
        **payload,
        "retention": "keep",
        "synthesis_mode": "deep",
        "domains": domains,
        "category": category or ("business" if "business" in domains else "personal"),
        "confidence": str(payload.get("confidence") or "medium").strip().lower() or "medium",
        "reasoning": str(payload.get("reasoning") or "").strip(),
        "subcategory": payload.get("subcategory"),
    }
    if normalized["category"] != "personal":
        normalized["subcategory"] = None
    return normalized


def is_audiobook_record(book: BookRecord) -> bool:
    """Return whether the current book input should default to deep synthesis."""
    format_name = str(getattr(book, "format", "") or "").strip().lower()
    return (
        format_name == "audiobook"
        or bool(str(getattr(book, "audio_path", "") or "").strip())
        or bool(str(getattr(book, "asin", "") or "").strip())
    )


def effective_book_classification(
    book: BookRecord,
    classification: dict[str, Any] | None,
    *,
    force_deep: bool = False,
) -> dict[str, Any]:
    """Resolve the runtime classification for a book ingest or reingest run."""
    normalized = normalize_book_classification(classification)
    if force_deep or is_audiobook_record(book):
        return force_deep_classification(normalized)
    return normalized


def enrich(book: BookRecord) -> dict[str, Any]:
    """Legacy thin enrichment. Prefer enrich_deep() + apply_to_you()."""
    cfg = env.load()
    target = research_path(cfg.repo_root, book)
    identities = get_llm_service().cache_identities(task_class="research", prompt_version=RESEARCH_BOOK_PROMPT_VERSION)
    identity = identities[0]
    cached = load_llm_cache(target, expected=identities)
    if isinstance(cached, dict):
        return cached
    author = book.author[0] if book.author else "unknown"
    result_or_tuple = get_llm_service().research_book(title=book.title, author=author, with_meta=True)
    if isinstance(result_or_tuple, tuple):
        response, identity = result_or_tuple
    else:
        response = result_or_tuple
    write_llm_cache(target, identity=identity, data=response)
    return response


def source_research_path(repo_root: Path, book: BookRecord) -> Path:
    author_slug = slugify(book.author[0]) if book.author else "unknown"
    title_slug = slugify(book.title)
    return raw_path(repo_root, "research", "books", f"{author_slug}-{title_slug}.source.json")


def _book_source_payload(book: BookRecord) -> tuple[str, list[LLMInputPart]] | None:
    document_path = _resolve_book_asset_path(book.document_path, book=book, kind="document")
    if document_path is not None:
            return (
                "document",
                [
                    LLMInputPart.metadata_part({"title": book.title, "author": book.author}),
                    LLMInputPart.pdf_part(
                        document_path.read_bytes(),
                        file_name=document_path.name,
                        metadata={"path": str(document_path)},
                    ),
                ],
            )
    audio_path = _resolve_book_asset_path(book.audio_path, book=book, kind="audio")
    if audio_path is not None:
            clip_windows = [
                {
                    "start_seconds": getattr(clip, "start_seconds", 0.0),
                    "end_seconds": getattr(clip, "end_seconds", 0.0),
                    "note": getattr(clip, "note", ""),
                }
                for clip in (book.clips or [])
            ]
            return (
                "audio",
                [
                    LLMInputPart.metadata_part(
                        {
                            "title": book.title,
                            "author": book.author,
                            "clips": clip_windows,
                        }
                    ),
                    LLMInputPart.audio_part(
                        audio_path.read_bytes(),
                        mime_type=_book_audio_mime_type(audio_path.suffix),
                        file_name=audio_path.name,
                        metadata={"path": str(audio_path)},
                    ),
                ],
            )
    return None


def _resolve_book_asset_path(explicit_path: str, *, book: BookRecord, kind: str) -> Path | None:
    if explicit_path:
        candidate = Path(explicit_path).expanduser()
        if candidate.exists():
            return candidate
    cfg = env.load()
    raw_root = Path(getattr(cfg, "raw_root", Path(getattr(cfg, "repo_root", ".")) / "raw"))
    author_slug = slugify(book.author[0]) if book.author else "unknown"
    title_slug = slugify(book.title)
    stems = [f"{author_slug}-{title_slug}", title_slug]
    if book.asin:
        stems.insert(0, book.asin)
    search_roots = [
        raw_root / "books" / ("documents" if kind == "document" else "audio"),
        raw_root / "audible" / ("documents" if kind == "document" else "audio"),
        raw_root / "exports",
    ]
    suffixes = [".pdf"] if kind == "document" else [".mp3", ".m4a", ".mp4", ".wav", ".webm"]
    for root in search_roots:
        if not root.exists():
            continue
        for stem in stems:
            for suffix in suffixes:
                candidate = root / f"{stem}{suffix}"
                if candidate.exists():
                    return candidate
    return None


def enrich_from_source(book: BookRecord) -> dict[str, Any] | None:
    document_path = _resolve_book_asset_path(book.document_path, book=book, kind="document")
    audio_path = _resolve_book_asset_path(book.audio_path, book=book, kind="audio")
    if document_path is not None:
        source_kind = "document"
        asset_path = document_path
    elif audio_path is not None:
        source_kind = "audio"
        asset_path = audio_path
    else:
        return None

    cfg = env.load()
    target = source_research_path(cfg.repo_root, book)
    author = book.author[0] if book.author else "unknown"
    identities = get_llm_service().cache_identities(
        task_class="document" if source_kind == "document" else "transcription",
        prompt_version=f"books.source-grounded.segmented.{source_kind}.v1",
    )
    cached = load_llm_cache(target, expected=identities)
    cache_meta = _book_source_cache_metadata(asset_path=asset_path, source_kind=source_kind)
    if (
        isinstance(cached, dict)
        and isinstance(cached.get("summary"), dict)
        and str(cached.get("source_text") or "").strip()
        and cached.get("cache_meta") == cache_meta
    ):
        return cached

    if source_kind == "document":
        source_text = extract_document_text(asset_path)
        segment_payloads = _segment_document_source_text(source_text)
        segment_summaries = [
            get_llm_service().summarize_book_source_text(
                title=book.title,
                author=author,
                source_kind="document",
                excerpt=segment["text"],
                segment_label=segment["label"],
            )
            for segment in segment_payloads
        ]
        segmentation_strategy = "fixed-window" if len(segment_payloads) > 1 else "single-segment"
    else:
        audio_segments, segmentation_strategy = _segment_audio_asset(
            asset_path,
            clip_windows=[
                {
                    "start_seconds": getattr(clip, "start_seconds", 0.0),
                    "end_seconds": getattr(clip, "end_seconds", 0.0),
                    "note": getattr(clip, "note", ""),
                }
                for clip in (book.clips or [])
            ],
        )
        segment_payloads = [{"label": segment["label"], "text": ""} for segment in audio_segments]
        segment_summaries = []
        for segment in audio_segments:
            summary = get_llm_service().summarize_book_source(
                title=book.title,
                author=author,
                input_parts=[
                    LLMInputPart.metadata_part(
                        {
                            "title": book.title,
                            "author": book.author,
                            "segment_label": segment["label"],
                        }
                    ),
                    LLMInputPart.audio_part(
                        segment["bytes"],
                        mime_type=segment["mime_type"],
                        file_name=segment["file_name"],
                        metadata={"path": str(asset_path), "segment_label": segment["label"]},
                    ),
                ],
                source_kind="audio",
            )
            segment_summaries.append(summary)
        source_text = "\n\n".join(
            str(segment.get("transcript") or "").strip()
            for segment in segment_summaries
            if str(segment.get("transcript") or "").strip()
        ).strip()
        for payload, summary in zip(segment_payloads, segment_summaries):
            payload["text"] = str(summary.get("transcript") or "").strip()

    if not source_text:
        return None

    summary = _merge_book_segment_summaries(
        source_kind=source_kind,
        segment_summaries=segment_summaries,
        source_text=source_text,
    )
    payload = {
        "source_kind": source_kind,
        "source_asset_path": str(asset_path),
        "source_text": source_text,
        "source_segments": [payload["text"] for payload in segment_payloads if payload["text"]],
        "segment_count": len(segment_payloads),
        "segmentation_strategy": segmentation_strategy,
        "summary": summary,
        "cache_meta": cache_meta,
    }
    write_llm_cache(target, identity=identities[0], data=payload)
    return payload


def deep_research_path(repo_root: Path, book: BookRecord) -> Path:
    author_slug = slugify(book.author[0]) if book.author else "unknown"
    title_slug = slugify(book.title)
    return raw_path(repo_root, "research", "books", f"{author_slug}-{title_slug}.deep.json")


def applied_path(repo_root: Path, book: BookRecord) -> Path:
    author_slug = slugify(book.author[0]) if book.author else "unknown"
    title_slug = slugify(book.title)
    return raw_path(repo_root, "research", "books", f"{author_slug}-{title_slug}.applied.json")


def attribute_cache_path(repo_root: Path, book: BookRecord) -> Path:
    author_slug = slugify(book.author[0]) if book.author else "unknown"
    title_slug = slugify(book.title)
    return raw_path(repo_root, "research", "books", f"{author_slug}-{title_slug}.stance.json")


def enrich_deep(book: BookRecord) -> dict[str, Any]:
    """Deep sectioned book research. Cached on disk at <slug>.deep.json."""
    cfg = env.load()
    target = deep_research_path(cfg.repo_root, book)
    identities = get_llm_service().cache_identities(task_class="research", prompt_version=RESEARCH_BOOK_DEEP_PROMPT_VERSION)
    identity = identities[0]
    cached = load_llm_cache(target, expected=identities)
    if isinstance(cached, dict):
        return cached
    author = book.author[0] if book.author else "unknown"
    result_or_tuple = get_llm_service().research_book_deep(title=book.title, author=author, with_meta=True)
    if isinstance(result_or_tuple, tuple):
        response, identity = result_or_tuple
    else:
        response = result_or_tuple
    write_llm_cache(target, identity=identity, data=response)
    return response


def summarize_research(book: BookRecord, research: dict[str, Any]) -> dict[str, Any]:
    """Compact summary artifact derived from the rich research artifact."""
    cfg = env.load()
    target = summary_path(cfg.repo_root, book)
    identities = get_llm_service().cache_identities(task_class="summary", prompt_version=SUMMARIZE_BOOK_RESEARCH_PROMPT_VERSION)
    identity = identities[0]
    cached = load_llm_cache(target, expected=identities)
    if isinstance(cached, dict):
        return cached
    author = book.author[0] if book.author else "unknown"
    result_or_tuple = get_llm_service().summarize_book_research(
        title=book.title,
        author=author,
        research=research,
        with_meta=True,
    )
    if isinstance(result_or_tuple, tuple):
        response, identity = result_or_tuple
    else:
        response = result_or_tuple
    write_llm_cache(target, identity=identity, data=response)
    return response



def apply_to_you(
    book: BookRecord,
    summary_artifact: dict[str, Any] | None = None,
    *,
    deep_research: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Pass B: write a personal advisory note tying the book to the owner.

    Cached on disk at <slug>.applied.json so re-running is free.
    """
    cfg = env.load()
    target = applied_path(cfg.repo_root, book)
    identities = get_llm_service().cache_identities(task_class="personalization", prompt_version=APPLIED_TO_YOU_PROMPT_VERSION)
    identity = identities[0]
    cached = load_llm_cache(target, expected=identities)
    if isinstance(cached, dict):
        return cached
    summary_payload = summary_artifact or deep_research or {}
    profile_ctx = load_profile_context(repo_root=cfg.repo_root)
    if not profile_ctx:
        # No profile context — return empty stub so the renderer skips the section
        return {"applied_paragraph": "", "applied_bullets": [], "thread_links": []}
    author = book.author[0] if book.author else "unknown"
    result_or_tuple = get_llm_service().applied_to_you(
        title=book.title,
        author=author,
        profile_context=profile_ctx,
        research=summary_payload,
        with_meta=True,
    )
    if isinstance(result_or_tuple, tuple):
        response, identity = result_or_tuple
    else:
        response = result_or_tuple
    write_llm_cache(target, identity=identity, data=response)
    return response


def update_author_memory(
    book: BookRecord,
    *,
    summary_artifact: dict[str, Any],
    repo_root: Path,
) -> dict[str, Any]:
    """Pass C: derive a book author-memory delta when prior stance exists."""
    author_name = book.author[0] if book.author else ""
    if not author_name:
        return {
            "status": "unsupported",
            "reason": "book creator target could not be resolved",
            "stance_change_note": "",
            "stance_context": "",
        }

    author_slug = slugify(author_name)
    stance_context = load_stance_context(
        slug=author_slug,
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

    target = attribute_cache_path(repo_root, book)
    identities = get_llm_service().cache_identities(
        task_class="stance",
        prompt_version=UPDATE_AUTHOR_STANCE_PROMPT_VERSION,
    )
    identity = identities[0]
    cached = load_llm_cache(target, expected=identities)
    if isinstance(cached, dict):
        return cached

    result_or_tuple = _get_llm_service().update_author_stance(
        author=author_name,
        title=book.title,
        post_slug=__import__("scripts.books.write_pages", fromlist=["canonical_page_id"]).canonical_page_id(repo_root, book),
        current_stance=stance_context,
        summary=summary_artifact,
    )
    if isinstance(result_or_tuple, tuple):
        response, identity = result_or_tuple
    else:
        response = result_or_tuple

    payload = {
        "status": "implemented" if (response.get("change_note") or "").strip() else "empty",
        "stance_change_note": response.get("change_note", ""),
        "stance_context": stance_context,
    }
    write_llm_cache(target, identity=identity, data=payload)
    return payload


def normalize_book_source(
    book: BookRecord,
    *,
    classification: dict[str, Any],
    research: dict[str, Any] | None = None,
    deep_research: dict[str, Any] | None = None,
    source_kind: str = "research",
    source_text: str = "",
    source_asset_path: str = "",
) -> NormalizedSource:
    """Normalize a book into the shared source boundary."""
    research_payload = research or deep_research or {}
    author_name = book.author[0] if book.author else "unknown"
    author_slug = slugify(author_name)
    title_slug = slugify(book.title)
    creator_candidates = []
    if author_name and author_name != "unknown":
        creator_candidates.append(
            MaterializationCandidate(
                page_type="person",
                name=author_name,
                role="creator",
                confidence=0.99,
                deterministic=True,
                source="book",
                page_id=author_slug,
            )
        )
    if book.publisher:
        creator_candidates.append(
            MaterializationCandidate(
                page_type="company",
                name=book.publisher,
                role="publisher",
                confidence=0.95,
                deterministic=True,
                source="book",
                page_id=slugify(book.publisher),
            )
        )
    content_field: dict[str, str]
    if source_kind == "document":
        content_field = {"raw_text": source_text}
    elif source_kind == "audio":
        content_field = {"transcript_text": source_text}
    else:
        content_field = {"raw_text": json.dumps(research_payload, ensure_ascii=False)}

    return NormalizedSource(
        source_id=f"book-{author_slug}-{title_slug}",
        source_kind="book",
        external_id=f"audible-{book.asin}" if book.asin else "",
        canonical_url="",
        title=book.title,
        creator_candidates=[asdict(candidate) for candidate in creator_candidates],
        published_at=book.finished_date or book.started_date,
        discovered_at=book.finished_date or book.started_date,
        source_metadata={
            "book": book,
            "classification": classification,
            "content_policy": canonical_policy_fields(classification),
            "additional_author_hints": list(book.author[1:]),
        },
        discovered_links=[],
        provenance={
            "adapter": "books",
            "format": book.format,
            "asin": book.asin,
            "source_kind": source_kind,
            "source_asset_path": source_asset_path,
        },
        **content_field,
    )


def _materialization_targets_from_source(source: NormalizedSource):
    candidates = [MaterializationCandidate(**candidate) for candidate in source.creator_candidates]
    return select_primary_targets(candidates)


def _book_audio_mime_type(suffix: str) -> str:
    normalized = suffix.lower()
    if normalized == ".mp3":
        return "audio/mpeg"
    if normalized in {".m4a", ".mp4"}:
        return "audio/mp4"
    if normalized == ".wav":
        return "audio/wav"
    if normalized == ".webm":
        return "audio/webm"
    return "application/octet-stream"


def run_pass_d_for_book(
    book: BookRecord,
    *,
    body_or_transcript: str,
    summary_artifact: dict,
    classification: dict[str, Any] | None = None,
    applied: dict[str, Any] | None,
    attribution: dict[str, Any] | None,
    repo_root: Path,
    today: str,
    prior_source_context: str = "",
    cache_mode: str = "default",
    evidence_date: str | None = None,
    force_refresh: bool = False,
) -> dict:
    """Run shared Pass D for a book source and dispatch evidence/probationary writes."""
    from scripts.atoms import pass_d, working_set
    from scripts.atoms.replay import apply_pass_d_result

    if classification is not None and not should_run_deep_synthesis(classification):
        return {
            "skipped": True,
            "reason": "synthesis_mode is not deep",
            "evidence_updates": 0,
            "probationary_updates": 0,
            "missing_atoms": [],
            "cache_reused": False,
        }

    ws = working_set.load_for_source(
        source_topics=list(summary_artifact.get("topics") or []),
        source_domains=working_set_domains(classification),
        cap=300,
        repo_root=repo_root,
    )
    author_slug = slugify(book.author[0]) if book.author else "unknown"
    title_slug = slugify(book.title)
    source_suffix = f"{author_slug}-{title_slug}"
    source_page_id = __import__("scripts.books.write_pages", fromlist=["canonical_page_id"]).canonical_page_id(repo_root, book)
    cache_reused = pass_d.pass_d_cache_exists(
        repo_root=repo_root,
        source_kind="book",
        source_id=f"book-{source_suffix}",
        cache_mode=cache_mode,
    ) and not force_refresh
    try:
        result = pass_d.run_pass_d(
            source_id=f"book-{source_suffix}",
            source_link=f"[[{source_page_id}]]",
            source_kind="book",
            body_or_transcript=body_or_transcript,
            summary=summary_artifact,
            applied=applied,
            pass_c_delta=(attribution or {}).get("stance_change_note") or None,
            stance_context=(attribution or {}).get("stance_context", ""),
            prior_source_context=prior_source_context,
            working_set=ws,
            repo_root=repo_root,
            today_str=today,
            cache_mode=cache_mode,
            force_refresh=force_refresh,
        )
    except Exception as exc:
        return {
            "cache_reused": cache_reused,
            "error": f"{type(exc).__name__}: {exc}",
            "error_stage": "pass_d.parse",
            "evidence_updates": 0,
            "probationary_updates": 0,
            "missing_atoms": [],
        }

    payload = {
        "q1_matches": [asdict(match) for match in result.q1_matches],
        "q2_candidates": [asdict(candidate) for candidate in result.q2_candidates],
        "warnings": list(getattr(result, "warnings", [])),
        "dropped_q1_matches": int(getattr(result, "dropped_q1_matches", 0)),
        "dropped_q2_candidates": int(getattr(result, "dropped_q2_candidates", 0)),
        "cache_reused": cache_reused,
    }
    try:
        dispatch = apply_pass_d_result(
            result,
            dedupe_by_source=cache_mode != "default",
            evidence_date=evidence_date or today,
            recorded_on=today,
            source_link=f"[[{source_page_id}]]",
            repo_root=repo_root,
            source_id=f"book-{source_suffix}",
            source_kind="book",
            source_date=evidence_date or today,
            creator_id=str(getattr(book, "author", "") or ""),
            topics=[str(item) for item in list((summary or {}).get("topics") or [])],
            entities=[str(item) for item in list((summary or {}).get("entities") or [])],
        )
    except Exception as exc:
        payload.update(
            {
                "error": f"{type(exc).__name__}: {exc}",
                "error_stage": "pass_d.dispatch",
                "evidence_updates": 0,
                "probationary_updates": 0,
                "missing_atoms": [],
            }
        )
        return payload

    payload.update(
        {
            "evidence_updates": dispatch.evidence_updates,
            "probationary_updates": dispatch.probationary_updates,
            "missing_atoms": dispatch.missing_atoms,
        }
    )
    return payload


def run_book_record_lifecycle(
    book: BookRecord,
    *,
    repo_root: Path,
    today: str,
    force_deep: bool = False,
) -> IngestionLifecycleResult | None:
    """Run one book through the shared ingestion lifecycle."""
    if book.status == "to-read":
        return None

    classification = effective_book_classification(book, classify(book), force_deep=force_deep)
    if not should_materialize(classification):
        return None
    source_grounded = enrich_from_source(book)
    if source_grounded is not None:
        research = source_grounded["summary"]
        summary_artifact = source_grounded["summary"]
        source = normalize_book_source(
            book,
            classification=classification,
            research=research,
            source_kind=str(source_grounded.get("source_kind") or "document"),
            source_text=str(source_grounded.get("source_text") or ""),
            source_asset_path=str(source_grounded.get("source_asset_path") or ""),
        )
    else:
        research = enrich_deep(book)
        from scripts.common.normalize import normalize_book_research
        research = normalize_book_research(research)
        summary_artifact = summarize_research(book, research)
        summary_artifact = normalize_book_research(summary_artifact)
        source = normalize_book_source(book, classification=classification, research=research, source_kind="research")

    def understand(_book_source: NormalizedSource, _envelope: dict[str, object]) -> dict[str, object]:
        prior_context = _get_prior_book_context(book, repo_root=repo_root)
        summary = summary_artifact
        if source.provenance.get("source_kind") in {"document", "audio"}:
            summary = verify_source_quotes(
                summary=summary,
                body_text=_book_source.primary_content,
                source_id=_book_source.source_id.replace("book-", "", 1),
                source_kind="book",
                repo_root=repo_root,
            )
        return {
            "classification": classification,
            "research": research,
            "summary": summary,
            "prior_context": prior_context,
            "research_source_kind": source.provenance.get("source_kind", "research"),
            "research_source_path": source.provenance.get("source_asset_path", ""),
            "segment_count": (source_grounded or {}).get("segment_count", 0),
            "segmentation_strategy": (source_grounded or {}).get("segmentation_strategy", ""),
        }

    def personalize(_book_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        applied = apply_to_you(book, (envelope.get("pass_a") or {}).get("summary", {}))
        status = "implemented" if any(applied.get(key) for key in ("applied_paragraph", "applied_bullets", "thread_links")) else "empty"
        return {
            "status": status,
            "applied": applied,
        }

    def attribute(_book_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        summary = (envelope.get("pass_a") or {}).get("summary", {})
        return update_author_memory(book, summary_artifact=summary, repo_root=repo_root)

    def distill(book_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        if not should_run_deep_synthesis(classification):
            return {
                "skipped": True,
                "reason": "synthesis_mode is not deep",
                "evidence_updates": 0,
                "probationary_updates": 0,
                "missing_atoms": [],
                "cache_reused": False,
            }
        summary = (envelope.get("pass_a") or {}).get("summary", {})
        pass_b = envelope.get("pass_b") or {}
        pass_c = envelope.get("pass_c") or {}
        try:
            return run_pass_d_for_book(
                book,
                body_or_transcript=book_source.primary_content,
                summary_artifact=summary,
                classification=classification,
                applied=pass_b.get("applied"),
                attribution=pass_c,
                repo_root=repo_root,
                today=today,
                prior_source_context=str((envelope.get("pass_a") or {}).get("prior_context") or ""),
            )
        except Exception as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}

    def materialize(book_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, str]:
        from scripts.books import write_pages

        pass_a = envelope.get("pass_a") or {}
        summary = pass_a.get("summary", {})
        research = pass_a.get("research", {})
        applied = (envelope.get("pass_b") or {}).get("applied")
        stance_change_note = ((envelope.get("pass_c") or {}).get("stance_change_note") or "").strip() or None
        source_classification = dict(pass_a.get("classification", {}) or {})
        category = compatibility_category(source_classification, lane="books")
        research_source_path = str(pass_a.get("research_source_path") or "").strip()
        targets = _materialization_targets_from_source(book_source)
        book_page = write_pages.write_book_page(
            book,
            research,
            category=category,  # type: ignore[arg-type]
            policy=source_classification,
            applied=applied,
            stance_change_note=stance_change_note,
            summary=summary,
            creator_target=targets.creator_target,
            publisher_target=targets.publisher_target,
            source_kind=str(pass_a.get("research_source_kind") or "research"),
            source_asset_path=research_source_path,
            force=True,
        )
        author_page = write_pages.ensure_author_page(
            book,
            repo_root=repo_root,
            creator_target=targets.creator_target,
            source_link=write_pages.canonical_page_id(repo_root, book),
        )
        materialized = {"book": str(book_page)}
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

    def propagate(_book_source: NormalizedSource, envelope: dict[str, object], _materialized: dict[str, str]) -> dict[str, object]:
        from scripts.atoms import pass_d

        pass_a = envelope.get("pass_a") or {}
        if not should_run_deep_synthesis((pass_a.get("classification") or {})):
            return {
                "pass_d": pass_d.stage_outcomes_from_payload(envelope.get("pass_d") or {}),
                "source_kind": pass_a.get("research_source_kind", "research"),
                "source_asset_path": pass_a.get("research_source_path", ""),
                "segment_count": pass_a.get("segment_count", 0),
                "segmentation_strategy": pass_a.get("segmentation_strategy", ""),
                "logged_entities": [],
                "logged_entity_count": 0,
                "propagate_discovered_count": 0,
                "propagate_queued_count": 0,
                "skipped": True,
                "reason": "synthesis_mode is not deep",
            }
        logged_entities = log_source_entities(
            summary=pass_a.get("summary", {}),
            body_text=_book_source.primary_content,
            repo_root=repo_root,
            today=today,
            source_link=write_pages.canonical_page_id(repo_root, book),
            inbox_kind="book-entities",
            stopwords=_book_stopwords(book),
        ) if source.provenance.get("source_kind") in {"document", "audio"} else []
        return {
            "pass_d": pass_d.stage_outcomes_from_payload(envelope.get("pass_d") or {}),
            "source_kind": pass_a.get("research_source_kind", "research"),
            "source_asset_path": pass_a.get("research_source_path", ""),
            "segment_count": pass_a.get("segment_count", 0),
            "segmentation_strategy": pass_a.get("segmentation_strategy", ""),
            "logged_entities": logged_entities,
            "logged_entity_count": len(logged_entities),
            "propagate_discovered_count": 0,
            "propagate_queued_count": 0,
        }

    result = run_ingestion_lifecycle(
        source=source,
        understand=understand,
        personalize=personalize,
        attribute=attribute,
        distill=distill,
        materialize=materialize,
        propagate=propagate,
    )
    write_quality_receipt(repo_root=repo_root, result=result, executed_at=today)
    return result


def _segment_source_text(source_text: str, *, clip_windows: list[dict[str, Any]] | None = None, max_chars: int = 12000) -> list[str]:
    text = source_text.strip()
    if not text:
        return []
    if clip_windows:
        segments = [
            clip.get("note", "").strip()
            for clip in clip_windows
            if str(clip.get("note", "")).strip()
        ]
        if segments:
            return segments
    return [text[index:index + max_chars].strip() for index in range(0, len(text), max_chars) if text[index:index + max_chars].strip()]


def _segment_document_source_text(source_text: str, max_chars: int = 12000) -> list[dict[str, str]]:
    segments = _segment_source_text(source_text, max_chars=max_chars)
    return [
        {"label": f"segment-{index + 1}", "text": segment}
        for index, segment in enumerate(segments)
    ]


def _book_source_cache_metadata(*, asset_path: Path, source_kind: str) -> dict[str, Any]:
    stat = asset_path.stat()
    return {
        "source_kind": source_kind,
        "source_asset_path": str(asset_path),
        "source_asset_size": stat.st_size,
        "source_asset_mtime_ns": stat.st_mtime_ns,
    }


def _audio_duration_seconds(path: Path) -> float:
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return 0.0
    try:
        return float(result.stdout.strip() or 0.0)
    except ValueError:
        return 0.0


def _extract_audio_segment_bytes(path: Path, *, start_seconds: float | None, end_seconds: float | None) -> bytes:
    cmd = ["ffmpeg", "-v", "error"]
    if start_seconds is not None:
        cmd.extend(["-ss", str(start_seconds)])
    cmd.extend(["-i", str(path)])
    if start_seconds is not None and end_seconds is not None and end_seconds > start_seconds:
        cmd.extend(["-t", str(end_seconds - start_seconds)])
    cmd.extend(["-ac", "1", "-ar", "16000", "-f", "wav", "pipe:1"])
    result = subprocess.run(cmd, capture_output=True, check=False)
    if result.returncode != 0 or not result.stdout:
        raise RuntimeError(f"ffmpeg audio segmentation failed for {path}: {result.stderr.decode(errors='ignore')[:200]}")
    return result.stdout


def _segment_audio_asset(
    path: Path,
    *,
    clip_windows: list[dict[str, Any]],
    segment_seconds: int = 900,
) -> tuple[list[dict[str, Any]], str]:
    valid_clips = [
        clip
        for clip in clip_windows
        if float(clip.get("end_seconds") or 0.0) > float(clip.get("start_seconds") or 0.0)
    ]
    segments: list[dict[str, Any]] = []
    if valid_clips:
        for index, clip in enumerate(valid_clips):
            segments.append(
                {
                    "label": clip.get("note") or f"clip-{index + 1}",
                    "bytes": _extract_audio_segment_bytes(
                        path,
                        start_seconds=float(clip["start_seconds"]),
                        end_seconds=float(clip["end_seconds"]),
                    ),
                    "mime_type": "audio/wav",
                    "file_name": f"{path.stem}-clip-{index + 1}.wav",
                }
            )
        return segments, "audible-clips"

    duration = _audio_duration_seconds(path)
    if duration <= 0 or duration <= segment_seconds:
        return [
            {
                "label": "segment-1",
                "bytes": _extract_audio_segment_bytes(path, start_seconds=None, end_seconds=None),
                "mime_type": "audio/wav",
                "file_name": f"{path.stem}-segment-1.wav",
            }
        ], "single-segment"

    start = 0.0
    index = 1
    while start < duration:
        end = min(duration, start + segment_seconds)
        segments.append(
            {
                "label": f"segment-{index}",
                "bytes": _extract_audio_segment_bytes(path, start_seconds=start, end_seconds=end),
                "mime_type": "audio/wav",
                "file_name": f"{path.stem}-segment-{index}.wav",
            }
        )
        start = end
        index += 1
    return segments, "fixed-window"


def _merge_book_segment_summaries(
    *,
    source_kind: str,
    segment_summaries: list[dict[str, Any]],
    source_text: str,
) -> dict[str, Any]:
    merged: dict[str, Any] = {
        "tldr": " ".join(_unique_preserve_order(
            str(summary.get("tldr") or "").strip()
            for summary in segment_summaries
            if str(summary.get("tldr") or "").strip()
        )[:3]),
        "key_claims": [],
        "frameworks_introduced": [],
        "in_conversation_with": [],
        "notable_quotes": [],
        "topics": [],
    }
    transcripts: list[str] = []
    for summary in segment_summaries:
        for item in summary.get("key_claims") or summary.get("key_ideas") or []:
            if item not in merged["key_claims"]:
                merged["key_claims"].append(item)
        merged["frameworks_introduced"] = _unique_preserve_order(
            [*merged["frameworks_introduced"], *(summary.get("frameworks_introduced") or [])]
        )
        merged["in_conversation_with"] = _unique_preserve_order(
            [*merged["in_conversation_with"], *(summary.get("in_conversation_with") or [])]
        )
        merged["notable_quotes"] = _unique_preserve_order(
            [*merged["notable_quotes"], *(summary.get("notable_quotes") or [])]
        )[:12]
        merged["topics"] = _unique_preserve_order(
            [*merged["topics"], *(summary.get("topics") or [])]
        )
        transcript = str(summary.get("transcript") or "").strip()
        if transcript:
            transcripts.append(transcript)
    if source_kind == "audio":
        merged["transcript"] = "\n\n".join(transcripts).strip()
    merged["source_text"] = source_text
    return merged


def _unique_preserve_order(values):
    seen: set[str] = set()
    result: list[Any] = []
    for value in values:
        marker = json.dumps(value, sort_keys=True, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)
        if not marker or marker in seen:
            continue
        seen.add(marker)
        result.append(value)
    return result


def _book_stopwords(book: BookRecord) -> set[str]:
    values = {book.title.strip(), (book.publisher or "").strip()}
    values.update(author.strip() for author in (book.author or []) if author.strip())
    for text in [book.title, book.publisher or "", *(book.author or [])]:
        values.update(token.strip() for token in text.split() if len(token.strip()) >= 2)
    return {value for value in values if value}


def _get_prior_book_context(book: BookRecord, *, repo_root: Path) -> str:
    author_name = (book.author[0] if book.author else "").strip().lower()
    publisher_name = (book.publisher or "").strip().lower()
    return build_prior_context(
        root=wiki_path(repo_root, "sources", "books"),
        heading="## Prior book sources in your wiki",
        matcher=lambda fm: (
            (author_name and any(strip_wiki_link(str(value)).lower() == author_name for value in _coerce_list(fm.get("author", ""))))
            or (publisher_name and strip_wiki_link(str(fm.get("publisher", ""))).lower() == publisher_name)
        ),
    )


def _coerce_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    text = str(value or "").strip()
    return [text] if text else []
