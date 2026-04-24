"""Three-stage YouTube enrichment with a shared lifecycle wrapper.

Stage 1 — classify (metadata-only routed LLM call)
  Input:  YouTubeRecord (title + channel)
  Output: {category, confidence, reasoning}
  Cache:  raw/transcripts/youtube/<id>.classification.json

Stage 2 — fetch transcript (no API call)
  Input:  video_id
  Output: plain text transcript
  Tool:   youtube-transcript-api → yt-dlp fallback (see scripts.youtube.transcript)
  Cache:  raw/transcripts/youtube/<id>.transcript.txt

Stage 3 — summarize (text-only routed LLM call against the transcript)
  Input:  YouTubeRecord + transcript text
  Output: {tldr, key_claims, notable_quotes, takeaways, topics, article}
  Cache:  raw/transcripts/youtube/<id>.json

The shared lifecycle wraps these cached stages and handles materialization and
Pass D propagation. The stage functions remain independently callable for focused
tests and simple one-off flows.

Every stage is idempotent: if the cache file exists, the work is skipped.
"""
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from mind.services.content_policy import (
    canonical_policy_fields,
    compatibility_category,
    normalize_youtube_classification,
    should_materialize,
    should_run_deep_synthesis,
    working_set_domains,
)
from mind.services.ingest_contract import IngestionLifecycleResult, NormalizedSource, run_ingestion_lifecycle
from mind.services.quality_receipts import write_quality_receipt
from mind.services.llm_cache import LLMCacheIdentity, load_llm_cache, write_llm_cache
from mind.services.llm_service import get_llm_service
from mind.services.materialization import MaterializationCandidate, select_primary_targets
from mind.services.providers.base import LLMInputPart
from mind.services.prompt_builders import (
    APPLIED_TO_YOU_PROMPT_VERSION,
    CLASSIFY_VIDEO_PROMPT_VERSION,
    SUMMARIZE_TRANSCRIPT_PROMPT_VERSION,
    UPDATE_AUTHOR_STANCE_PROMPT_VERSION,
)
from mind.services.llm_service import get_llm_service as _get_llm_service
from scripts.common import env
from scripts.common.drop_queue import (
    append_article_links_to_drop_queue,
    extract_urls_with_context,
    filter_article_links_for_queue,
)
from scripts.common.entity_log import log_entities as log_source_entities
from scripts.common.profile import load_profile_context
from scripts.common.slugify import slugify
from scripts.common.source_context import build_prior_context, strip_wiki_link
from scripts.common.stance import load_stance_context
from scripts.common.vault import raw_path, wiki_path
from scripts.common.quote_verify import verify_quotes as verify_source_quotes
from scripts.youtube import write_pages
from scripts.youtube import filter as youtube_filter
from scripts.youtube.parse import YouTubeRecord
from scripts.youtube.transcript import NoCaptionsAvailable, download_audio, fetch_with_metadata


# ---------------------------------------------------------------------------
# Cache paths
# ---------------------------------------------------------------------------


def transcript_path(repo_root: Path, video_id: str) -> Path:
    """Path to the structured transcript summary (Stage 3 output)."""
    return raw_path(repo_root, "transcripts", "youtube", f"{video_id}.json")


def classification_path(repo_root: Path, video_id: str) -> Path:
    return raw_path(repo_root, "transcripts", "youtube", f"{video_id}.classification.json")


def raw_transcript_path(repo_root: Path, video_id: str) -> Path:
    """Path to the raw text transcript (Stage 2 output)."""
    return raw_path(repo_root, "transcripts", "youtube", f"{video_id}.transcript.txt")


def transcription_payload_path(repo_root: Path, video_id: str) -> Path:
    return raw_path(repo_root, "transcripts", "youtube", f"{video_id}.transcription.json")


def applied_path(repo_root: Path, video_id: str) -> Path:
    return raw_path(repo_root, "transcripts", "youtube", f"{video_id}.applied.json")


def attribute_cache_path(repo_root: Path, video_id: str) -> Path:
    return raw_path(repo_root, "transcripts", "youtube", f"{video_id}.stance.json")


YOUTUBE_TRANSCRIPTION_PAYLOAD_SCHEMA_VERSION = 2


def _empty_applied_payload() -> dict[str, Any]:
    return {
        "applied_paragraph": "",
        "applied_bullets": [],
        "thread_links": [],
    }


def _persist_raw_transcript_cache(repo_root: Path, video_id: str, transcript: str) -> None:
    text = str(transcript or "").strip()
    if not text:
        return
    target = raw_transcript_path(repo_root, video_id)
    if target.exists() and target.read_text(encoding="utf-8").strip() == text:
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


# ---------------------------------------------------------------------------
# Stage 1 — classify
# ---------------------------------------------------------------------------


def classify(
    record: YouTubeRecord,
    *,
    description: str = "",
    tags: list[str] | None = None,
) -> dict[str, Any]:
    """Classify a video into the canonical content policy shape. Cached on disk.

    Metadata-only — title + channel + description + tags are sent to the routed LLM.
    No frame decoding, no transcript needed. Works for any video length.

    The description is the highest-signal field for ambiguous middle-30%
    videos (AI hot-takes vs substantive talks, geopolitics analysis vs news
    cycles). Pass it whenever it's available from the puller.
    """
    cfg = env.load()
    target = classification_path(cfg.repo_root, record.video_id)
    identities = get_llm_service().cache_identities(task_class="classification", prompt_version=CLASSIFY_VIDEO_PROMPT_VERSION)
    identity = identities[0]
    cached = load_llm_cache(target, expected=identities)
    if isinstance(cached, dict):
        return normalize_youtube_classification(cached)
    response, identity = get_llm_service().classify_video(
        title=record.title,
        channel=record.channel,
        description=description,
        tags=tags,
        with_meta=True,
    )
    normalized = normalize_youtube_classification(response)
    write_llm_cache(target, identity=identity, data=normalized)
    return normalized


# ---------------------------------------------------------------------------
# Stage 2 — fetch transcript
# ---------------------------------------------------------------------------


def fetch_transcript(record: YouTubeRecord) -> str:
    """Fetch the raw transcript text for a video. Cached on disk.

    Raises NoCaptionsAvailable if neither the transcript-api path nor the
    yt-dlp fallback returns text. Caller should log to inbox and skip.
    """
    cfg = env.load()
    target = raw_transcript_path(cfg.repo_root, record.video_id)
    if target.exists():
        return target.read_text(encoding="utf-8")
    payload = fetch_transcription_result(record, repo_root=cfg.repo_root)
    text = str(payload.get("transcript") or "").strip()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")
    return text


def fetch_transcription_result(record: YouTubeRecord, *, repo_root: Path) -> dict[str, Any]:
    payload_target = transcription_payload_path(repo_root, record.video_id)
    service = get_llm_service()
    youtube_url = record.title_url or f"https://www.youtube.com/watch?v={record.video_id}"
    base_input_parts = [
        LLMInputPart.url_part(youtube_url, metadata={"source": "youtube"}),
        LLMInputPart.metadata_part({"title": record.title, "channel": record.channel}),
    ]
    identities = service.cache_identities_for_parts(
        task_class="transcription",
        instructions=(
            "Transcribe this YouTube source into JSON with these exact keys: "
            "transcript (string), summary (string), topics (array of lowercase-hyphenated strings), "
            "confidence (one of high|medium|low). Preserve speaker wording where possible. "
            "If only the URL is usable, infer from the accessible media or metadata rather than inventing details. "
            "Output JSON only."
        ),
        input_parts=base_input_parts,
        prompt_version="youtube.transcription.v1",
        input_mode="media",
        request_metadata={
            "youtube_url": youtube_url,
            "has_audio_bytes": False,
            "audio_mime_type": "",
        },
    )
    cached_payload, cached_identity = _load_cached_transcription_payload(payload_target, identities)
    if isinstance(cached_payload, dict) and str(cached_payload.get("transcript") or "").strip():
        normalized_cached = _normalize_transcription_payload(
            cached_payload,
            youtube_url=youtube_url,
            audio_mime_type=str(cached_payload.get("audio_mime_type") or ""),
            audio_download_error=str(cached_payload.get("audio_download_error") or ""),
        )
        _persist_raw_transcript_cache(repo_root, record.video_id, str(normalized_cached.get("transcript") or ""))
        if cached_identity is not None and normalized_cached != cached_payload:
            write_llm_cache(payload_target, identity=cached_identity, data=normalized_cached)
        return normalized_cached

    raw_transcript = raw_transcript_path(repo_root, record.video_id)
    if raw_transcript.exists():
        payload = _normalize_transcription_payload(
            {
                "transcript": raw_transcript.read_text(encoding="utf-8"),
                "summary": "",
                "topics": [],
                "confidence": "low",
                "transcription_path": "raw-transcript-cache",
                "youtube_url": youtube_url,
                "audio_mime_type": "",
                "audio_download_error": "",
                "multimodal_error": "",
                "fallback_attempts": [],
            },
            youtube_url=youtube_url,
            audio_mime_type="",
            audio_download_error="",
        )
        write_llm_cache(payload_target, identity=identities[0], data=payload)
        _persist_raw_transcript_cache(repo_root, record.video_id, str(payload.get("transcript") or ""))
        return payload

    cache_identity = identities[0]
    multimodal_error = ""
    audio_download_error = ""
    try:
        transcript, fallback_path, attempts = fetch_with_metadata(record.video_id)
    except NoCaptionsAvailable as exc:
        attempts = list(exc.attempts)
    else:
        payload = _normalize_transcription_payload(
            {
                "transcript": transcript,
                "summary": "",
                "topics": [],
                "confidence": "low",
                "transcription_path": fallback_path,
                "youtube_url": youtube_url,
                "audio_mime_type": "",
                "audio_download_error": "",
                "multimodal_error": "",
                "fallback_attempts": attempts,
            },
            youtube_url=youtube_url,
            audio_mime_type="",
            audio_download_error="",
        )
        write_llm_cache(payload_target, identity=cache_identity, data=payload)
        _persist_raw_transcript_cache(repo_root, record.video_id, str(payload.get("transcript") or ""))
        return payload

    try:
        response, identity = service.transcribe_youtube(
            title=record.title,
            channel=record.channel,
            youtube_url=youtube_url,
            audio_bytes=None,
            audio_mime_type="audio/mp4",
            with_meta=True,
        )
        transcript = str(response.get("transcript") or "").strip()
        if transcript:
            payload = _normalize_transcription_payload(
                {
                    **response,
                    "transcription_path": "url-multimodal",
                    "youtube_url": youtube_url,
                    "audio_mime_type": "",
                    "audio_download_error": "",
                    "multimodal_error": "",
                    "fallback_attempts": attempts,
                },
                youtube_url=youtube_url,
                audio_mime_type="",
                audio_download_error="",
            )
            write_llm_cache(payload_target, identity=identity, data=payload)
            _persist_raw_transcript_cache(repo_root, record.video_id, str(payload.get("transcript") or ""))
            return payload
        multimodal_error = "multimodal-empty-transcript"
        cache_identity = identity
    except Exception as exc:
        multimodal_error = (multimodal_error + "; " if multimodal_error else "") + f"{type(exc).__name__}: {exc}"

    try:
        audio_bytes, audio_mime_type = download_audio(record.video_id)
    except Exception as exc:
        audio_bytes, audio_mime_type = None, "audio/mp4"
        audio_download_error = f"audio-download: {type(exc).__name__}: {exc}"
    if audio_bytes is not None:
        try:
            response, identity = service.transcribe_youtube(
                title=record.title,
                channel=record.channel,
                youtube_url=youtube_url,
                audio_bytes=audio_bytes,
                audio_mime_type=audio_mime_type,
                with_meta=True,
            )
            transcript = str(response.get("transcript") or "").strip()
            if transcript:
                payload = _normalize_transcription_payload(
                    {
                        **response,
                        "transcription_path": "audio-multimodal",
                        "youtube_url": youtube_url,
                        "audio_mime_type": audio_mime_type,
                        "audio_download_error": audio_download_error,
                        "multimodal_error": multimodal_error,
                        "fallback_attempts": attempts,
                    },
                    youtube_url=youtube_url,
                    audio_mime_type=audio_mime_type,
                    audio_download_error=audio_download_error,
                )
                write_llm_cache(payload_target, identity=identity, data=payload)
                _persist_raw_transcript_cache(repo_root, record.video_id, str(payload.get("transcript") or ""))
                return payload
            multimodal_error = (multimodal_error + "; " if multimodal_error else "") + "audio-multimodal-empty-transcript"
            cache_identity = identity
        except Exception as exc:
            multimodal_error = (multimodal_error + "; " if multimodal_error else "") + f"{type(exc).__name__}: {exc}"

    raise NoCaptionsAvailable(record.video_id, attempts + ([multimodal_error] if multimodal_error else []))


def _transcribe_with_route(record: YouTubeRecord, *, repo_root: Path) -> str:
    payload = fetch_transcription_result(record, repo_root=repo_root)
    transcript = str(payload.get("transcript") or "").strip()
    if transcript:
        return transcript
    attempts = list(payload.get("fallback_attempts") or [])
    multimodal_error = str(payload.get("multimodal_error") or "").strip()
    if multimodal_error:
        attempts.append(multimodal_error)
    raise NoCaptionsAvailable(record.video_id, attempts or ["empty-transcription-result"])


# ---------------------------------------------------------------------------
# Stage 3 — summarize
# ---------------------------------------------------------------------------


def summarize(
    record: YouTubeRecord,
    transcript: str,
    stance_context: str = "",
    prior_sources_context: str = "",
) -> dict[str, Any]:
    """Summarize a transcript into the structured article dict. Cached on disk."""
    cfg = env.load()
    target = transcript_path(cfg.repo_root, record.video_id)
    identities = get_llm_service().cache_identities(task_class="summary", prompt_version=SUMMARIZE_TRANSCRIPT_PROMPT_VERSION)
    identity = identities[0]
    cached = load_llm_cache(target, expected=identities)
    if isinstance(cached, dict):
        return cached
    response, identity = get_llm_service().summarize_transcript(
        title=record.title,
        channel=record.channel,
        transcript=transcript,
        stance_context=stance_context,
        prior_sources_context=prior_sources_context,
        with_meta=True,
    )
    write_llm_cache(target, identity=identity, data=response)
    return response


def normalize_youtube_source(
    record: YouTubeRecord,
    *,
    classification: dict[str, Any],
    transcription: dict[str, Any],
) -> NormalizedSource:
    """Normalize a YouTube record into the shared source boundary."""
    creator_candidates = [
        MaterializationCandidate(
            page_type="channel",
            name=record.channel,
            role="creator",
            confidence=0.99,
            deterministic=True,
            source="youtube",
        )
    ]
    return NormalizedSource(
        source_id=f"youtube-{record.video_id}",
        source_kind="youtube",
        external_id=f"youtube-{record.video_id}",
        canonical_url=f"https://www.youtube.com/watch?v={record.video_id}",
        title=record.title,
        creator_candidates=[asdict(candidate) for candidate in creator_candidates],
        published_at=record.published_at,
        discovered_at=record.watched_at,
        source_metadata={
            "record": record,
            "classification": classification,
            "content_policy": canonical_policy_fields(classification),
            "transcription": transcription,
            "description": record.description,
            "tags": list(record.tags),
            "category": record.category,
            "categories": list(record.categories),
            "published_at": record.published_at,
            "channel_id": record.channel_id,
            "channel_url": record.channel_url,
            "thumbnail_url": record.thumbnail_url,
        },
        discovered_links=[
            {
                **link,
                "category": compatibility_category(classification, lane="youtube"),
            }
            for link in extract_urls_with_context(record.description)
        ],
        provenance={
            "adapter": "youtube",
            "channel": record.channel,
            "youtube_url": record.title_url or f"https://www.youtube.com/watch?v={record.video_id}",
            "channel_url": record.channel_url,
            "channel_id": record.channel_id,
            "published_at": record.published_at,
            "transcription_path": transcription.get("transcription_path", ""),
            "audio_mime_type": transcription.get("audio_mime_type", ""),
        },
        transcript_text=str(transcription.get("transcript") or ""),
    )


def _materialization_targets_from_source(source: NormalizedSource):
    candidates = [MaterializationCandidate(**candidate) for candidate in source.creator_candidates]
    return select_primary_targets(candidates)


def _resolved_duration_minutes(record: YouTubeRecord, default_duration_minutes: float) -> float | None:
    duration = youtube_filter.duration_minutes(record)
    if duration is not None:
        return duration
    return float(default_duration_minutes)


def apply_video_to_you(
    record: YouTubeRecord,
    *,
    summary: dict[str, Any],
    repo_root: Path,
) -> dict[str, Any]:
    """Pass B: create an applied-to-you note for a YouTube video."""
    target = applied_path(repo_root, record.video_id)
    identities = get_llm_service().cache_identities(
        task_class="personalization",
        prompt_version=APPLIED_TO_YOU_PROMPT_VERSION,
    )
    identity = identities[0]
    cached = load_llm_cache(target, expected=identities)
    if isinstance(cached, dict):
        return cached

    profile_ctx = load_profile_context(repo_root=repo_root)
    if not profile_ctx:
        return _empty_applied_payload()

    result_or_tuple = get_llm_service().applied_to_you(
        title=record.title,
        author=record.channel,
        profile_context=profile_ctx,
        research=summary,
        with_meta=True,
    )
    if isinstance(result_or_tuple, tuple):
        response, identity = result_or_tuple
    else:
        response = result_or_tuple
    write_llm_cache(target, identity=identity, data=response)
    return response


def build_channel_attribution(
    record: YouTubeRecord,
    *,
    summary: dict[str, Any],
    repo_root: Path,
) -> dict[str, Any]:
    """Pass C: derive a channel-memory delta when prior stance context exists."""
    channel_slug = slugify(record.channel)
    stance_context = load_stance_context(
        slug=channel_slug,
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

    target = attribute_cache_path(repo_root, record.video_id)
    identities = get_llm_service().cache_identities(
        task_class="stance",
        prompt_version=UPDATE_AUTHOR_STANCE_PROMPT_VERSION,
    )
    identity = identities[0]
    cached = load_llm_cache(target, expected=identities)
    if isinstance(cached, dict):
        return cached

    result_or_tuple = _get_llm_service().update_author_stance(
        author=record.channel,
        title=record.title,
        post_slug=__import__("scripts.youtube.write_pages", fromlist=["canonical_page_id"]).canonical_page_id(repo_root, record),
        current_stance=stance_context,
        summary=summary,
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


def run_pass_d_for_youtube(
    record: YouTubeRecord,
    *,
    transcript: str,
    summary: dict,
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
    """Run shared Pass D for a YouTube source and dispatch evidence/probationary writes."""
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
        source_topics=list(summary.get("topics") or []),
        source_domains=working_set_domains(classification),
        cap=300,
        repo_root=repo_root,
    )
    source_page_id = __import__("scripts.youtube.write_pages", fromlist=["canonical_page_id"]).canonical_page_id(repo_root, record)
    cache_reused = pass_d.pass_d_cache_exists(
        repo_root=repo_root,
        source_kind="youtube",
        source_id=f"youtube-{record.video_id}",
        cache_mode=cache_mode,
    ) and not force_refresh
    try:
        result = pass_d.run_pass_d(
            source_id=f"youtube-{record.video_id}",
            source_link=f"[[{source_page_id}]]",
            source_kind="youtube",
            body_or_transcript=transcript,
            summary=summary,
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


def run_youtube_record_lifecycle(
    record: YouTubeRecord,
    *,
    repo_root: Path,
    default_duration_minutes: float,
    today: str,
) -> IngestionLifecycleResult | None:
    """Run one YouTube record through the shared ingestion lifecycle."""
    if youtube_filter.should_skip_record(record, duration_minutes_override=float(default_duration_minutes)):
        return None
    try:
        classification = classify(record, description=record.description, tags=list(record.tags))
    except TypeError:  # pragma: no cover - compatibility with narrow fakes in tests
        classification = normalize_youtube_classification(classify(record))
    if not should_materialize(classification):
        return None
    transcription = fetch_transcription_result(record, repo_root=repo_root)
    transcript = str(transcription.get("transcript") or "").strip()
    if not transcript:
        raise NoCaptionsAvailable(record.video_id, list(transcription.get("fallback_attempts") or []))
    source = normalize_youtube_source(record, classification=classification, transcription=transcription)

    def understand(youtube_source: NormalizedSource, _envelope: dict[str, object]) -> dict[str, object]:
        prior_context = _get_prior_channel_context(record, repo_root=repo_root)
        channel_stance = load_stance_context(
            slug=slugify(record.channel), kind="channel", repo_root=repo_root,
        )
        summary = summarize(
            record, youtube_source.primary_content,
            stance_context=channel_stance,
            prior_sources_context=prior_context,
        )
        summary = verify_source_quotes(
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

    def personalize(_youtube_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        summary = (envelope.get("pass_a") or {}).get("summary", {})
        applied = apply_video_to_you(record, summary=summary, repo_root=repo_root)
        status = "implemented" if any(applied.get(key) for key in ("applied_paragraph", "applied_bullets", "thread_links")) else "empty"
        return {
            "status": status,
            "applied": applied,
        }

    def attribute(_youtube_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        summary = (envelope.get("pass_a") or {}).get("summary", {})
        return build_channel_attribution(record, summary=summary, repo_root=repo_root)

    def distill(youtube_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
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
            return run_pass_d_for_youtube(
                record,
                transcript=youtube_source.primary_content,
                summary=summary,
                classification=classification,
                applied=pass_b.get("applied"),
                attribution=pass_c,
                repo_root=repo_root,
                today=today,
                prior_source_context=str((envelope.get("pass_a") or {}).get("prior_context") or ""),
            )
        except Exception as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}

    def materialize(youtube_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, str]:
        summary = (envelope.get("pass_a") or {}).get("summary", {})
        pass_b = envelope.get("pass_b") or {}
        pass_c = envelope.get("pass_c") or {}
        targets = _materialization_targets_from_source(youtube_source)
        source_classification = dict((envelope.get("pass_a") or {}).get("classification", {}) or {})
        category = compatibility_category(source_classification, lane="youtube")
        video = write_pages.write_video_page(
            record,
            summary,
            duration_minutes=_resolved_duration_minutes(record, default_duration_minutes),
            category=category,  # type: ignore[arg-type]
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

    def propagate(_youtube_source: NormalizedSource, envelope: dict[str, object], _materialized: dict[str, str]) -> dict[str, object]:
        from scripts.atoms import pass_d

        verification = envelope.get("verification") or {}
        pass_a = envelope.get("pass_a") or {}
        if not should_run_deep_synthesis((pass_a.get("classification") or {})):
            return {
                "pass_d": pass_d.stage_outcomes_from_payload(envelope.get("pass_d") or {}),
                "transcription_path": verification.get("transcription_path", ""),
                "multimodal_error": verification.get("multimodal_error", ""),
                "drop_path": "",
                "logged_entities": [],
                "logged_entity_count": 0,
                "propagate_discovered_count": 0,
                "propagate_queued_count": 0,
                "skipped": True,
                "reason": "synthesis_mode is not deep",
            }
        description_links = list((pass_a.get("materialization_hints") or {}).get("description_links") or [])
        actionable_links = filter_article_links_for_queue(description_links)
        drop_path = append_article_links_to_drop_queue(
            repo_root=repo_root,
            today=today,
            source_id=record.video_id,
            source_url=record.title_url or f"https://www.youtube.com/watch?v={record.video_id}",
            source_type="youtube-description",
            discovered_at=record.watched_at,
            links=actionable_links,
            source_label="youtube-description",
        )
        logged_entities = log_source_entities(
            summary=pass_a.get("summary", {}),
            body_text=source.transcript_text,
            repo_root=repo_root,
            today=today,
            source_link=write_pages.canonical_page_id(repo_root, record),
            inbox_kind="youtube-entities",
            stopwords=_youtube_stopwords(record),
        )
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


# ---------------------------------------------------------------------------
# Convenience: run all three stages for a single record
# ---------------------------------------------------------------------------


def enrich(record: YouTubeRecord) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Run the full 3-stage pipeline for one record.

    Returns:
      None if the video was classified as 'ignore' or has no captions.
      (classification, summary) tuple otherwise.

    The orchestrator workflow can call this directly for simple cases, or
    call the individual stages for finer-grained control.
    """
    try:
        classification = classify(record, description=record.description, tags=list(record.tags))
    except TypeError:  # pragma: no cover - compatibility with narrow fakes in tests
        classification = classify(record)
    if classification.get("category") == "ignore":
        return None
    try:
        transcript = fetch_transcript(record)
    except NoCaptionsAvailable:
        return None
    summary = summarize(record, transcript)
    return classification, summary


# Re-export for convenience
__all__ = [
    "classify",
    "fetch_transcript",
    "fetch_transcription_result",
    "summarize",
    "enrich",
    "transcript_path",
    "classification_path",
    "raw_transcript_path",
    "NoCaptionsAvailable",
]


def _load_cached_transcription_payload(
    path: Path,
    identities: list[LLMCacheIdentity],
) -> tuple[dict[str, Any] | None, LLMCacheIdentity | None]:
    cached = load_llm_cache(path, expected=identities)
    if not isinstance(cached, dict):
        return None, None
    try:
        envelope = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return cached, None
    raw_identity = envelope.get("_llm")
    if not isinstance(raw_identity, dict):
        return cached, None
    try:
        return cached, LLMCacheIdentity(**raw_identity)
    except TypeError:
        return cached, None


def _normalize_topics(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    topics: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            topics.append(text)
    return topics


def _normalize_confidence(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"high", "medium", "low"}:
        return text
    return "low"


def _derive_audio_download_status(audio_mime_type: str, audio_download_error: str, transcription_path: str) -> str:
    if audio_download_error:
        return "failed"
    if audio_mime_type:
        return "downloaded"
    if transcription_path == "audio-multimodal":
        return "downloaded"
    return "not_attempted"


def _normalize_transcription_payload(
    payload: dict[str, Any],
    *,
    youtube_url: str,
    audio_mime_type: str,
    audio_download_error: str,
) -> dict[str, Any]:
    transcription_path = str(
        payload.get("acquisition_path_used")
        or payload.get("transcription_path")
        or ""
    ).strip()
    normalized_audio_mime_type = str(payload.get("audio_mime_type") or audio_mime_type).strip()
    normalized_audio_download_error = str(payload.get("audio_download_error") or audio_download_error).strip()
    fallback_attempts_raw = payload.get("fallback_attempts") or []
    if isinstance(fallback_attempts_raw, str):
        fallback_attempts_iterable = [fallback_attempts_raw]
    elif isinstance(fallback_attempts_raw, (list, tuple, set)):
        fallback_attempts_iterable = fallback_attempts_raw
    else:
        fallback_attempts_iterable = []
    return {
        "schema_version": YOUTUBE_TRANSCRIPTION_PAYLOAD_SCHEMA_VERSION,
        "transcript": str(payload.get("transcript") or "").strip(),
        "summary": str(payload.get("summary") or "").strip(),
        "topics": _normalize_topics(payload.get("topics")),
        "confidence": _normalize_confidence(payload.get("confidence")),
        "transcription_path": transcription_path,
        "acquisition_path_used": transcription_path,
        "youtube_url": str(payload.get("youtube_url") or youtube_url).strip(),
        "audio_mime_type": normalized_audio_mime_type,
        "audio_download_status": _derive_audio_download_status(
            normalized_audio_mime_type,
            normalized_audio_download_error,
            transcription_path,
        ),
        "audio_download_error": normalized_audio_download_error,
        "multimodal_error": str(payload.get("multimodal_error") or "").strip(),
        "fallback_attempts": [
            str(attempt).strip()
            for attempt in fallback_attempts_iterable
            if str(attempt).strip()
        ],
    }


def _youtube_stopwords(record: YouTubeRecord) -> set[str]:
    words = {record.channel.strip(), record.title.strip()}
    words.update(token.strip() for token in record.channel.split() if len(token.strip()) >= 2)
    words.update(token.strip() for token in record.title.split() if len(token.strip()) >= 2)
    return {word for word in words if word}


def _get_prior_channel_context(record: YouTubeRecord, *, repo_root: Path) -> str:
    channel_slug = slugify(record.channel)
    channel_name = record.channel.strip().lower()
    return build_prior_context(
        root=wiki_path(repo_root, "sources", "youtube"),
        heading="## Prior YouTube sources in your wiki",
        matcher=lambda fm: (
            strip_wiki_link(str(fm.get("channel", ""))) == channel_slug
            or str(fm.get("channel", "")).strip().lower() == channel_name
        ),
    )


def _audio_extension(mime_type: str) -> str:
    if mime_type == "audio/mpeg":
        return "mp3"
    if mime_type == "audio/wav":
        return "wav"
    if mime_type == "audio/mp4":
        return "m4a"
    if mime_type == "audio/webm":
        return "webm"
    return "bin"
