from __future__ import annotations

import subprocess
from pathlib import Path

from scripts.youtube import pull


def test_normalize_entries_emits_canonical_ingest_shape() -> None:
    records = pull.normalize_entries(
        [
            {
                "id": "abc123xyz00",
                "title": "Test Video",
                "uploader": "Test Channel",
                "duration": 894,
                "description": "A useful video",
                "tags": ["ai", "agents"],
                "categories": ["Science & Technology", "Education"],
                "webpage_url": "https://www.youtube.com/watch?v=abc123xyz00",
                "uploader_url": "https://www.youtube.com/@test-channel",
                "channel_id": "channel-123",
                "thumbnail": "https://img.youtube.com/test.jpg",
                "timestamp": 1776207369,
            }
        ]
    )

    assert records == [
        {
            "video_id": "abc123xyz00",
            "title": "Test Video",
            "channel": "Test Channel",
            "watched_at": "",
            "published_at": "2026-04-14T22:56:09Z",
            "duration_seconds": 894,
            "description": "A useful video",
            "tags": ["ai", "agents"],
            "category": "Science & Technology",
            "categories": ["Science & Technology", "Education"],
            "title_url": "https://www.youtube.com/watch?v=abc123xyz00",
            "url": "https://www.youtube.com/watch?v=abc123xyz00",
            "channel_url": "https://www.youtube.com/@test-channel",
            "channel_id": "channel-123",
            "thumbnail_url": "https://img.youtube.com/test.jpg",
        }
    ]


def test_fetch_via_yt_dlp_returns_timeout_failure(monkeypatch) -> None:
    def _timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=["yt-dlp"], timeout=pull.YT_DLP_PULL_TIMEOUT_SECONDS)

    monkeypatch.setattr(subprocess, "run", _timeout)

    result = pull.fetch_via_yt_dlp("chrome", 5)

    assert result.exit_code == 1
    assert result.timed_out is True
    assert "timed out" in result.stderr


def test_run_fails_when_yt_dlp_exits_nonzero(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        pull,
        "fetch_via_yt_dlp",
        lambda browser, limit: pull.FetchResult(entries=[], exit_code=1, stderr="ERROR: HTTP Error 403"),
    )

    result = pull.run(browser="chrome", raw_root=tmp_path / "raw", limit=5, dry_run=True)

    assert result.exit_code == 1
    assert "HTTP Error 403" in result.detail
    assert result.export_path is None


def test_run_fails_when_auth_looks_broken_even_with_zero_exit(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        pull,
        "fetch_via_yt_dlp",
        lambda browser, limit: pull.FetchResult(entries=[], exit_code=0, stderr="Sign in to confirm you're not a bot"),
    )

    result = pull.run(browser="chrome", raw_root=tmp_path / "raw", limit=5, dry_run=True)

    assert result.exit_code == 1
    assert "bot" in result.detail.lower()


def test_run_dry_run_reports_watch_item_count(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        pull,
        "fetch_via_yt_dlp",
        lambda browser, limit: pull.FetchResult(
            entries=[
                {
                    "id": "abc123xyz00",
                    "title": "Test Video",
                    "uploader": "Test Channel",
                    "webpage_url": "https://www.youtube.com/watch?v=abc123xyz00",
                }
            ],
            exit_code=0,
            stderr="",
        ),
    )

    result = pull.run(browser="chrome", raw_root=tmp_path / "raw", limit=5, dry_run=True)

    assert result.exit_code == 0
    assert result.detail == "found 1 watch item"
    assert result.export_path is None
