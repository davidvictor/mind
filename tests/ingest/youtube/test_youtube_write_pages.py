"""Tests for scripts/youtube/write_pages.py id alignment."""
from datetime import date as _date
import pytest

from scripts.common import env
from scripts.youtube.parse import YouTubeRecord
from scripts.youtube import write_pages
from tests.support import write_repo_config


def _fake_cfg(tmp_path):
    class FakeCfg:
        gemini_api_key = "fake"
        llm_model = "fake"
        browser_for_cookies = "chrome"
        repo_root = tmp_path
        wiki_root = tmp_path / "memory"
        raw_root = tmp_path / "raw"
        substack_session_cookie = ""
    return FakeCfg()


@pytest.fixture(autouse=True)
def _configured_repo(tmp_path, monkeypatch):
    write_repo_config(tmp_path)
    monkeypatch.setattr(env, "load", lambda: _fake_cfg(tmp_path))


def _make_record(**overrides) -> YouTubeRecord:
    defaults = dict(
        video_id="abc123xyz00",
        title="Test Video Title",
        channel="Test Channel",
        watched_at="2026-04-01T10:00:00Z",
    )
    defaults.update(overrides)
    return YouTubeRecord(**defaults)


FAKE_APPLIED = {
    "applied_paragraph": "This video reinforces Example Owner's current work.",
    "applied_bullets": [{"claim": "Practice synthesis publicly", "why_it_matters": "It clarifies thinking", "action": "Ship a synthesis note"}],
    "thread_links": ["knowledge-systems"],
}

EMPTY_APPLIED = {"applied_paragraph": "", "applied_bullets": [], "thread_links": []}

FAKE_STANCE_CHANGE = "The channel is leaning harder into systems thinking for knowledge work."


def test_write_video_page_uses_filename_slug_as_id(tmp_path):
    record = _make_record()
    enriched = {"tldr": "summary", "key_claims": [], "notable_quotes": [], "takeaways": [], "topics": [], "article": ""}
    path = write_pages.write_video_page(
        record,
        enriched,
        duration_minutes=10,
        category="business",
        policy={"retention": "keep", "domains": ["business"], "synthesis_mode": "deep"},
    )
    content = path.read_text(encoding="utf-8")
    assert f"id: {path.stem}" in content
    assert "external_id: youtube-abc123xyz00" in content
    assert "retention: keep" in content
    assert "domains:\n  - business" in content
    assert "synthesis_mode: deep" in content
    assert "\nid: youtube-abc123xyz00" not in content


def test_write_summary_page_uses_filename_slug_as_id(tmp_path):
    record = _make_record()
    enriched = {"tldr": "summary", "key_claims": [], "notable_quotes": [], "topics": []}
    path = write_pages.write_summary_page(
        record,
        enriched,
        category="business",
        policy={"retention": "keep", "domains": ["business"], "synthesis_mode": "deep"},
    )
    content = path.read_text(encoding="utf-8")
    assert f"id: {path.stem}" in content
    assert "external_id: youtube-abc123xyz00" in content
    assert "domains:\n  - business" in content

def test_write_video_page_emits_three_tag_axes(tmp_path):
    record = _make_record()
    enriched = {"tldr": "summary", "key_claims": [], "notable_quotes": [], "takeaways": [], "topics": ["ai-agents"], "article": ""}
    path = write_pages.write_video_page(
        record,
        enriched,
        duration_minutes=10,
        category="business",
        policy={"retention": "keep", "domains": ["business"], "synthesis_mode": "deep"},
    )
    content = path.read_text(encoding="utf-8")
    assert "domain/" in content
    assert "function/" in content
    assert "signal/" in content
    assert "  - ai-agents" in content
    assert "  - youtube" not in content


def test_write_summary_video_page_emits_three_tag_axes(tmp_path):
    record = _make_record()
    enriched = {"tldr": "summary", "key_claims": [], "notable_quotes": [], "topics": ["ai-agents"]}
    path = write_pages.write_summary_page(
        record,
        enriched,
        category="business",
        policy={"retention": "keep", "domains": ["business"], "synthesis_mode": "deep"},
    )
    content = path.read_text(encoding="utf-8")
    assert "domain/" in content
    assert "function/" in content
    assert "signal/" in content
    assert "  - ai-agents" in content
    assert "  - youtube" not in content


def test_write_pages_follow_flattened_memory_layout(tmp_path):
    record = _make_record()
    enriched = {"tldr": "summary", "key_claims": [], "notable_quotes": [], "takeaways": [], "topics": [], "article": ""}

    video_path = write_pages.write_video_page(record, enriched, duration_minutes=10, category="business")
    summary_path = write_pages.write_summary_page(record, enriched, category="business")

    assert video_path == tmp_path / "memory" / "sources" / "youtube" / "business" / "test-video-title.md"
    assert summary_path == video_path

    video_content = video_path.read_text(encoding="utf-8")
    assert "transcript_path: ../../../../raw/transcripts/youtube/abc123xyz00.transcript.txt" in video_content
    assert "source_path: ../../../../raw/transcripts/youtube/abc123xyz00.transcription.json" in video_content
    assert "summary_path: ../../../../raw/transcripts/youtube/abc123xyz00.json" in video_content
    assert "summary-test-video-title" in video_content


def test_write_video_page_uses_published_date_when_watch_date_is_unknown(tmp_path):
    record = _make_record(
        watched_at="",
        published_at="2026-04-02T12:34:56Z",
        channel_id="channel-123",
        channel_url="https://www.youtube.com/@test-channel",
        thumbnail_url="https://img.youtube.com/test.jpg",
    )
    enriched = {"tldr": "summary", "key_claims": [], "notable_quotes": [], "takeaways": [], "topics": [], "article": ""}

    video_path = write_pages.write_video_page(record, enriched, duration_minutes=10, category="business")

    video_content = video_path.read_text(encoding="utf-8")
    assert "source_date: 2026-04-02" in video_content
    assert "published: 2026-04-02" in video_content
    assert "channel_id: channel-123" in video_content
    assert 'channel_url: "https://www.youtube.com/@test-channel"' in video_content
    assert 'thumbnail_url: "https://img.youtube.com/test.jpg"' in video_content


def test_write_video_page_renders_phase3_sections_when_present(tmp_path):
    record = _make_record()
    enriched = {"tldr": "summary", "key_claims": [], "notable_quotes": [], "takeaways": [], "topics": [], "article": ""}
    path = write_pages.write_video_page(
        record,
        enriched,
        duration_minutes=10,
        category="business",
        applied=FAKE_APPLIED,
        stance_change_note=FAKE_STANCE_CHANGE,
    )
    content = path.read_text(encoding="utf-8")
    assert "## Applied to You" in content
    assert "This video reinforces Example Owner's current work." in content
    assert "knowledge-systems" in content
    assert "## Channel Memory Delta" in content
    assert FAKE_STANCE_CHANGE in content


def test_write_summary_page_renders_phase3_sections_when_present(tmp_path):
    record = _make_record()
    enriched = {"tldr": "summary", "key_claims": [], "notable_quotes": [], "topics": []}
    path = write_pages.write_summary_page(
        record,
        enriched,
        category="business",
        applied=FAKE_APPLIED,
        stance_change_note=FAKE_STANCE_CHANGE,
    )
    assert path == write_pages.video_page_path(tmp_path, record, "business")
    content = path.read_text(encoding="utf-8")
    assert "## Applied to You" in content
    assert "## Channel Memory Delta" in content
    assert FAKE_STANCE_CHANGE in content


def test_write_pages_omit_empty_phase3_sections_cleanly(tmp_path):
    record = _make_record()
    video = write_pages.write_video_page(
        record,
        {"tldr": "summary", "key_claims": [], "notable_quotes": [], "takeaways": [], "topics": [], "article": ""},
        duration_minutes=10,
        category="business",
        applied=EMPTY_APPLIED,
        stance_change_note=None,
    )
    summary = write_pages.write_summary_page(
        record,
        {"tldr": "summary", "key_claims": [], "notable_quotes": [], "topics": []},
        category="business",
        applied=EMPTY_APPLIED,
        stance_change_note="",
    )
    video_content = video.read_text(encoding="utf-8")
    summary_content = summary.read_text(encoding="utf-8")
    assert "## Applied to You" not in video_content
    assert "## Channel Memory Delta" not in video_content
    assert "## Applied to You" not in summary_content
    assert "## Channel Memory Delta" not in summary_content


def test_write_video_page_force_rewrite_preserves_created_and_ingested(tmp_path, monkeypatch):
    record = _make_record()
    enriched = {"tldr": "summary", "key_claims": [], "notable_quotes": [], "takeaways": [], "topics": [], "article": ""}

    class _OldDate(_date):
        @classmethod
        def today(cls):
            return cls(2026, 4, 17)

    class _NewDate(_date):
        @classmethod
        def today(cls):
            return cls(2026, 4, 18)

    monkeypatch.setattr(write_pages, "date", _OldDate)
    path = write_pages.write_video_page(
        record,
        enriched,
        duration_minutes=10,
        category="business",
    )
    monkeypatch.setattr(write_pages, "date", _NewDate)
    write_pages.write_video_page(
        record,
        {**enriched, "article": "Updated"},
        duration_minutes=10,
        category="business",
        force=True,
    )
    content = path.read_text(encoding="utf-8")
    assert "created: 2026-04-17" in content
    assert "ingested: 2026-04-17" in content
    assert "last_updated: 2026-04-18" in content
