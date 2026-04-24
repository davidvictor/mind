from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from mind.commands.ingest import _iter_youtube_records
from mind.services.llm_cache import LLMCacheIdentity, write_llm_cache
from scripts.youtube import enrich, write_pages
from scripts.common.vault import Vault
from scripts.youtube.parse import YouTubeRecord
from scripts.youtube.transcript import NoCaptionsAvailable
from tests.support import fake_env_config, write_repo_config


def _record(**overrides) -> YouTubeRecord:
    data = {
        "video_id": "abc123xyz00",
        "title": "Test Video Title",
        "channel": "Test Channel",
        "watched_at": "2026-04-01T10:00:00Z",
    }
    data.update(overrides)
    return YouTubeRecord(**data)


def _cache_identity() -> LLMCacheIdentity:
    return LLMCacheIdentity(
        task_class="transcription",
        provider="test",
        model="test-model",
        transport="gateway",
        api_family="responses",
        input_mode="media",
        prompt_version="youtube.transcription.v1",
        request_fingerprint={"kind": "media"},
        temperature=None,
        max_tokens=None,
        timeout_seconds=None,
        reasoning_effort=None,
    )


def test_normalize_youtube_source_uses_channel_as_primary_creator() -> None:
    source = enrich.normalize_youtube_source(
        _record(description="See https://example.com/essay for notes"),
        classification={"category": "business"},
        transcription={"transcript": "hello world", "transcription_path": "url-multimodal"},
    )
    assert source.source_kind == "youtube"
    assert source.creator_candidates[0]["page_type"] == "channel"
    assert source.creator_candidates[0]["role"] == "creator"
    assert source.creator_candidates[0]["deterministic"] is True
    assert source.discovered_links[0]["url"] == "https://example.com/essay"
    assert source.discovered_links[0]["category"] == "business"


def test_iter_youtube_records_preserves_duration_seconds(tmp_path: Path) -> None:
    export = tmp_path / "youtube.json"
    export.write_text(
        json.dumps(
            [
                {
                    "video_id": "abc123xyz00",
                    "title": "Test Video Title",
                    "channel": "Test Channel",
                    "watched_at": "2026-04-01T10:00:00Z",
                    "duration_seconds": 894,
                }
            ]
        ),
        encoding="utf-8",
    )

    records = _iter_youtube_records(export)

    assert len(records) == 1
    assert records[0].duration_seconds == 894


def test_iter_youtube_records_preserves_provider_metadata_shape(tmp_path: Path) -> None:
    export = tmp_path / "youtube.json"
    export.write_text(
        json.dumps(
            [
                {
                    "video_id": "abc123xyz00",
                    "title": "Test Video Title",
                    "channel": "Test Channel",
                    "watched_at": "",
                    "published_at": "2026-04-02T12:34:56Z",
                    "channel_id": "channel-123",
                    "channel_url": "https://www.youtube.com/@test-channel",
                    "thumbnail_url": "https://img.youtube.com/test.jpg",
                    "categories": ["Science & Technology", "Education"],
                }
            ]
        ),
        encoding="utf-8",
    )

    records = _iter_youtube_records(export)

    assert len(records) == 1
    assert records[0].published_at == "2026-04-02T12:34:56Z"
    assert records[0].category == "Science & Technology"
    assert records[0].categories == ("Science & Technology", "Education")
    assert records[0].channel_id == "channel-123"
    assert records[0].channel_url == "https://www.youtube.com/@test-channel"
    assert records[0].thumbnail_url == "https://img.youtube.com/test.jpg"


def test_fetch_transcription_result_records_url_only_multimodal_success(monkeypatch, tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    monkeypatch.setattr("scripts.common.env.load", lambda: fake_env_config(tmp_path))
    monkeypatch.setattr(
        enrich,
        "fetch_with_metadata",
        lambda video_id: (_ for _ in ()).throw(NoCaptionsAvailable(video_id, ["transcript-api: RuntimeError: boom"])),
    )
    monkeypatch.setattr(enrich, "download_audio", lambda video_id: (_ for _ in ()).throw(AssertionError("audio download should not run")))
    identity = _cache_identity()
    monkeypatch.setattr(
        "scripts.youtube.enrich.get_llm_service",
        lambda: type(
            "FakeService",
            (),
            {
                "cache_identities_for_parts": staticmethod(lambda **kwargs: [identity]),
                "transcribe_youtube": staticmethod(
                    lambda **kwargs: (
                        {"transcript": "hello world", "topics": [], "confidence": "high"},
                        identity,
                    )
                ),
            },
        )(),
    )

    payload = enrich.fetch_transcription_result(_record(), repo_root=tmp_path)

    assert payload["schema_version"] == enrich.YOUTUBE_TRANSCRIPTION_PAYLOAD_SCHEMA_VERSION
    assert payload["transcription_path"] == "url-multimodal"
    assert payload["acquisition_path_used"] == "url-multimodal"
    assert payload["multimodal_error"] == ""
    assert payload["summary"] == ""
    assert payload["topics"] == []
    assert payload["confidence"] == "high"
    assert payload["audio_download_status"] == "not_attempted"
    assert payload["audio_download_error"] == ""


def test_fetch_transcription_result_prefers_caption_path_before_download(monkeypatch, tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    monkeypatch.setattr("scripts.common.env.load", lambda: fake_env_config(tmp_path))
    monkeypatch.setattr(enrich, "download_audio", lambda video_id: (_ for _ in ()).throw(AssertionError("audio download should not run")))
    identity = _cache_identity()
    monkeypatch.setattr(
        "scripts.youtube.enrich.get_llm_service",
        lambda: type(
            "FakeService",
            (),
            {
                "cache_identities_for_parts": staticmethod(lambda **kwargs: [identity]),
                "transcribe_youtube": staticmethod(lambda **kwargs: (_ for _ in ()).throw(AssertionError("multimodal should not run"))),
            },
        )(),
    )
    monkeypatch.setattr(enrich, "fetch_with_metadata", lambda video_id: ("fallback transcript", "yt-dlp", ["transcript-api: RuntimeError: boom"]))

    payload = enrich.fetch_transcription_result(_record(), repo_root=tmp_path)

    assert payload["schema_version"] == enrich.YOUTUBE_TRANSCRIPTION_PAYLOAD_SCHEMA_VERSION
    assert payload["transcription_path"] == "yt-dlp"
    assert payload["acquisition_path_used"] == "yt-dlp"
    assert payload["transcript"] == "fallback transcript"
    assert payload["audio_download_status"] == "not_attempted"
    assert payload["fallback_attempts"] == ["transcript-api: RuntimeError: boom"]
    assert payload["multimodal_error"] == ""
    assert enrich.raw_transcript_path(tmp_path, "abc123xyz00").read_text(encoding="utf-8") == "fallback transcript"


def test_fetch_transcription_result_uses_audio_only_after_caption_and_url_paths_fail(monkeypatch, tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    monkeypatch.setattr("scripts.common.env.load", lambda: fake_env_config(tmp_path))
    monkeypatch.setattr(
        enrich,
        "fetch_with_metadata",
        lambda video_id: (_ for _ in ()).throw(NoCaptionsAvailable(video_id, ["transcript-api: RuntimeError: boom"])),
    )
    monkeypatch.setattr(enrich, "download_audio", lambda video_id: (b"audio", "audio/mpeg"))
    identity = _cache_identity()

    def _transcribe(**kwargs):
        if kwargs.get("audio_bytes") is None:
            raise RuntimeError("gateway down")
        return (
            {"transcript": "audio transcript", "topics": ["ai"], "confidence": "medium"},
            identity,
        )

    monkeypatch.setattr(
        "scripts.youtube.enrich.get_llm_service",
        lambda: type(
            "FakeService",
            (),
            {
                "cache_identities_for_parts": staticmethod(lambda **kwargs: [identity]),
                "transcribe_youtube": staticmethod(_transcribe),
            },
        )(),
    )

    payload = enrich.fetch_transcription_result(_record(), repo_root=tmp_path)

    assert payload["transcription_path"] == "audio-multimodal"
    assert payload["audio_download_status"] == "downloaded"
    assert payload["transcript"] == "audio transcript"
    assert "gateway down" in payload["multimodal_error"]


def test_fetch_transcription_result_upgrades_legacy_cached_payload(monkeypatch, tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    monkeypatch.setattr("scripts.common.env.load", lambda: fake_env_config(tmp_path))
    monkeypatch.setattr(enrich, "download_audio", lambda video_id: (b"audio", "audio/mpeg"))

    identity = _cache_identity()
    service = type(
        "FakeService",
        (),
        {
            "cache_identities_for_parts": staticmethod(lambda **kwargs: [identity]),
            "transcribe_youtube": staticmethod(lambda **kwargs: (_ for _ in ()).throw(AssertionError("cache should be used"))),
        },
    )()
    monkeypatch.setattr("scripts.youtube.enrich.get_llm_service", lambda: service)

    cache_path = enrich.transcription_payload_path(tmp_path, "abc123xyz00")
    write_llm_cache(
        cache_path,
        identity=identity,
        data={
            "transcript": "legacy transcript",
            "summary": "legacy summary",
            "topics": ["systems"],
            "confidence": "HIGH",
            "transcription_path": "audio-multimodal",
            "youtube_url": "https://www.youtube.com/watch?v=abc123xyz00",
            "audio_mime_type": "audio/mpeg",
            "fallback_attempts": "transcript-api: RuntimeError: boom",
        },
    )

    payload = enrich.fetch_transcription_result(_record(), repo_root=tmp_path)

    assert payload["schema_version"] == enrich.YOUTUBE_TRANSCRIPTION_PAYLOAD_SCHEMA_VERSION
    assert payload["acquisition_path_used"] == "audio-multimodal"
    assert payload["audio_download_status"] == "downloaded"
    assert payload["confidence"] == "high"
    assert payload["fallback_attempts"] == ["transcript-api: RuntimeError: boom"]

    cached = json.loads(cache_path.read_text(encoding="utf-8"))
    assert cached["data"]["schema_version"] == enrich.YOUTUBE_TRANSCRIPTION_PAYLOAD_SCHEMA_VERSION
    assert cached["data"]["acquisition_path_used"] == "audio-multimodal"
    assert cached["data"]["audio_download_status"] == "downloaded"


def test_fetch_transcription_result_does_not_poison_cached_audio_metadata_on_new_download_failure(monkeypatch, tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    monkeypatch.setattr("scripts.common.env.load", lambda: fake_env_config(tmp_path))
    monkeypatch.setattr(enrich, "download_audio", lambda video_id: (_ for _ in ()).throw(RuntimeError("temporary failure")))

    identity = _cache_identity()
    monkeypatch.setattr(
        "scripts.youtube.enrich.get_llm_service",
        lambda: type(
            "FakeService",
            (),
            {
                "cache_identities_for_parts": staticmethod(lambda **kwargs: [identity]),
                "transcribe_youtube": staticmethod(lambda **kwargs: (_ for _ in ()).throw(AssertionError("cache should be used"))),
            },
        )(),
    )
    cache_path = enrich.transcription_payload_path(tmp_path, "abc123xyz00")
    write_llm_cache(
        cache_path,
        identity=identity,
        data={
            "transcript": "cached transcript",
            "summary": "",
            "topics": [],
            "confidence": "high",
            "transcription_path": "audio-multimodal",
            "acquisition_path_used": "audio-multimodal",
        },
    )

    payload = enrich.fetch_transcription_result(_record(), repo_root=tmp_path)

    assert payload["audio_download_status"] == "downloaded"
    assert payload["audio_download_error"] == ""


def test_write_pages_materialize_deterministic_channel_pages_without_people_pages(tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    record = _record(channel="My Channel")
    summary = {"tldr": "x", "key_claims": [], "notable_quotes": [], "takeaways": [], "topics": [], "article": ""}
    source = enrich.normalize_youtube_source(
        record,
        classification={"category": "business"},
        transcription={"transcript": "hello", "transcription_path": "audio-multimodal"},
    )
    targets = enrich._materialization_targets_from_source(source)
    with patch("scripts.common.env.load", return_value=fake_env_config(tmp_path)):
        path = write_pages.ensure_channel_page(
            record,
            repo_root=tmp_path,
            creator_target=targets.creator_target,
            source_link="summary-yt-abc123xyz00",
        )
    assert path == Vault.load(tmp_path).wiki / "channels" / "my-channel.md"
    assert path.exists()
    assert not (Vault.load(tmp_path).wiki / "people").exists()
