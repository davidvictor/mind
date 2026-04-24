from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scripts.articles import pipeline
from scripts.articles.fetch import ArticleFetchFailure, ArticleFetchResult


@pytest.fixture(autouse=True)
def _stub_stance(monkeypatch):
    """Stance context loading requires wiki dirs that don't exist in test tmp_paths."""
    monkeypatch.setattr("scripts.articles.enrich.load_stance_context", lambda *a, **kw: "")


def _seed_drop_file(repo_root: Path, name: str, lines: list[dict]):
    drops = repo_root / "raw" / "drops"
    drops.mkdir(parents=True, exist_ok=True)
    target = drops / name
    target.write_text(
        "\n".join(json.dumps(L, ensure_ascii=False) for L in lines) + "\n",
        encoding="utf-8",
    )
    return target


def _fake_success_fetch(entry, repo_root):
    return ArticleFetchResult(
        body_text="Body text from fake fetch.",
        title="Fake Title",
        author="Fake Author",
        sitename="Fake Outlet",
        published="2024-05-15",
        raw_html_path=repo_root / "raw" / "transcripts" / "articles" / "fake.html",
    )


def _fake_success_lifecycle(*, entry, repo_root: Path):
    article_dir = repo_root / "wiki" / "sources" / "articles"
    article_dir.mkdir(parents=True, exist_ok=True)
    article_path = article_dir / f"{entry.anchor_text}.md"
    article_path.write_text("# Article\n", encoding="utf-8")
    return type(
        "Lifecycle",
        (),
        {
            "materialized": {"article": str(article_path)},
            "propagate": {},
        },
    )()


def test_drain_drop_queue_processes_all_entries(tmp_path):
    _seed_drop_file(tmp_path, "articles-from-substack-2026-04-07.jsonl", [
        {"url": "https://a.com/x", "source_post_id": "1", "source_post_url": "u",
         "anchor_text": "a", "context_snippet": "c", "category": "business",
         "discovered_at": "2026-04-07T00:00:00Z", "source_type": "substack-link"},
        {"url": "https://b.com/y", "source_post_id": "2", "source_post_url": "u",
         "anchor_text": "b", "context_snippet": "c", "category": "personal",
         "discovered_at": "2026-04-07T00:00:00Z", "source_type": "substack-link"},
    ])
    with patch("scripts.articles.pipeline.fetch_article", side_effect=_fake_success_fetch), \
         patch(
             "scripts.articles.pipeline.run_article_entry_lifecycle",
             side_effect=lambda entry, fetch_result, repo_root, today, summarize_override=None: _fake_success_lifecycle(entry=entry, repo_root=repo_root),
         ):
        result = pipeline.drain_drop_queue(today_str="2026-04-07", repo_root=tmp_path)

    assert result.urls_in_queue == 2
    assert result.fetched_summarized == 2
    assert result.new_pages_written == 2
    assert result.paywalled == 0
    assert result.failed == 0
    # Marker file written
    marker = tmp_path / "wiki" / "sources" / "articles" / ".ingested-articles-from-substack-2026-04-07.jsonl"
    assert marker.exists()
    # Article pages written
    article_dir = tmp_path / "wiki" / "sources" / "articles"
    md_files = list(article_dir.glob("*.md"))
    assert len(md_files) == 2


def test_drain_drop_queue_skips_marker_files(tmp_path):
    _seed_drop_file(tmp_path, "articles-from-substack-2026-04-07.jsonl", [
        {"url": "https://a.com/x", "source_post_id": "1", "source_post_url": "u",
         "anchor_text": "a", "context_snippet": "c", "category": "business",
         "discovered_at": "2026-04-07T00:00:00Z", "source_type": "substack-link"},
    ])
    # Pre-create marker
    marker_dir = tmp_path / "wiki" / "sources" / "articles"
    marker_dir.mkdir(parents=True, exist_ok=True)
    (marker_dir / ".ingested-articles-from-substack-2026-04-07.jsonl").touch()

    with patch("scripts.articles.pipeline.fetch_article") as mock_fetch:
        result = pipeline.drain_drop_queue(today_str="2026-04-07", repo_root=tmp_path)

    mock_fetch.assert_not_called()
    assert result.drop_files_processed == 0


def test_drain_drop_queue_logs_paywall_to_inbox(tmp_path):
    _seed_drop_file(tmp_path, "articles-from-substack-2026-04-07.jsonl", [
        {"url": "https://paywall.com/x", "source_post_id": "1", "source_post_url": "u",
         "anchor_text": "a", "context_snippet": "c", "category": "business",
         "discovered_at": "2026-04-07T00:00:00Z", "source_type": "substack-link"},
    ])
    with patch(
        "scripts.articles.pipeline.fetch_article",
        return_value=ArticleFetchFailure(failure_kind="paywalled", detail="member-only", url="https://paywall.com/x"),
    ):
        result = pipeline.drain_drop_queue(today_str="2026-04-07", repo_root=tmp_path)

    assert result.paywalled == 1
    assert result.fetched_summarized == 0
    inbox = tmp_path / "wiki" / "inbox" / "articles-paywalled-2026-04-07.md"
    assert inbox.exists()
    assert "https://paywall.com/x" in inbox.read_text()


def test_drain_drop_queue_logs_fetch_failure_kind_to_inbox(tmp_path):
    _seed_drop_file(tmp_path, "articles-from-substack-2026-04-07.jsonl", [
        {"url": "https://a.com/x", "source_post_id": "1", "source_post_url": "u",
         "anchor_text": "a", "context_snippet": "c", "category": "business",
         "discovered_at": "2026-04-07T00:00:00Z", "source_type": "substack-link"},
    ])
    with patch(
        "scripts.articles.pipeline.fetch_article",
        return_value=ArticleFetchFailure(failure_kind="unsupported_format", detail="unsupported host", url="https://a.com/x"),
    ):
        result = pipeline.drain_drop_queue(today_str="2026-04-07", repo_root=tmp_path)

    assert result.failed == 1
    inbox = tmp_path / "wiki" / "inbox" / "articles-failures-2026-04-07.md"
    assert inbox.exists()
    text = inbox.read_text()
    assert "stage=fetch" in text
    assert "kind=unsupported_format" in text


def test_drain_drop_queue_logs_summarize_failure_to_inbox(tmp_path):
    _seed_drop_file(tmp_path, "articles-from-substack-2026-04-07.jsonl", [
        {"url": "https://a.com/x", "source_post_id": "1", "source_post_url": "u",
         "anchor_text": "a", "context_snippet": "c", "category": "business",
         "discovered_at": "2026-04-07T00:00:00Z", "source_type": "substack-link"},
    ])
    with patch("scripts.articles.pipeline.fetch_article", side_effect=_fake_success_fetch), \
         patch("scripts.articles.pipeline.summarize_article",
               side_effect=RuntimeError("gemini transient")):
        result = pipeline.drain_drop_queue(today_str="2026-04-07", repo_root=tmp_path)

    assert result.failed == 1
    assert result.fetched_summarized == 0
    inbox = tmp_path / "wiki" / "inbox" / "articles-failures-2026-04-07.md"
    assert inbox.exists()
    assert "https://a.com/x" in inbox.read_text()


def test_drain_drop_queue_logs_propagate_failure_after_materialization(tmp_path):
    _seed_drop_file(tmp_path, "articles-from-substack-2026-04-07.jsonl", [
        {"url": "https://a.com/x", "source_post_id": "1", "source_post_url": "u",
         "anchor_text": "a", "context_snippet": "c", "category": "business",
         "discovered_at": "2026-04-07T00:00:00Z", "source_type": "substack-link"},
    ])
    lifecycle = type("Lifecycle", (), {
        "materialized": {"article": str(tmp_path / "wiki" / "sources" / "articles" / "x.md")},
        "propagate": {"pass_d": [{"stage": "propagate", "summary": "RuntimeError: boom"}]},
    })()
    (tmp_path / "wiki" / "sources" / "articles").mkdir(parents=True, exist_ok=True)
    (tmp_path / "wiki" / "sources" / "articles" / "x.md").write_text("x", encoding="utf-8")

    with patch("scripts.articles.pipeline.fetch_article", side_effect=_fake_success_fetch), \
         patch("scripts.articles.pipeline.run_article_entry_lifecycle", return_value=lifecycle):
        result = pipeline.drain_drop_queue(today_str="2026-04-07", repo_root=tmp_path)

    assert result.fetched_summarized == 1
    inbox = tmp_path / "wiki" / "inbox" / "articles-failures-2026-04-07.md"
    assert inbox.exists()
    assert "stage=propagate" in inbox.read_text()


def test_drain_drop_queue_skips_non_article_youtube_description_entries(tmp_path):
    _seed_drop_file(tmp_path, "articles-from-youtube-description-2026-04-07.jsonl", [
        {"url": "https://www.youtube.com/@science.revolution", "source_post_id": "1", "source_post_url": "u",
         "anchor_text": "a", "context_snippet": "c", "category": "business",
         "discovered_at": "2026-04-07T00:00:00Z", "source_type": "youtube-description"},
    ])

    with patch("scripts.articles.pipeline.fetch_article") as mock_fetch:
        result = pipeline.drain_drop_queue(today_str="2026-04-07", repo_root=tmp_path)

    mock_fetch.assert_not_called()
    assert result.skipped_existing == 1
    inbox = tmp_path / "wiki" / "inbox" / "articles-failures-2026-04-07.md"
    assert not inbox.exists()


def test_drain_drop_queue_idempotent_second_run(tmp_path):
    """Re-run on the same drop file does no work."""
    _seed_drop_file(tmp_path, "articles-from-substack-2026-04-07.jsonl", [
        {"url": "https://a.com/x", "source_post_id": "1", "source_post_url": "u",
         "anchor_text": "a", "context_snippet": "c", "category": "business",
         "discovered_at": "2026-04-07T00:00:00Z", "source_type": "substack-link"},
    ])
    lifecycle_mock = MagicMock(
        side_effect=lambda entry, fetch_result, repo_root, today, summarize_override=None: _fake_success_lifecycle(entry=entry, repo_root=repo_root)
    )
    with patch("scripts.articles.pipeline.fetch_article", side_effect=_fake_success_fetch), \
         patch("scripts.articles.pipeline.run_article_entry_lifecycle", lifecycle_mock):
        pipeline.drain_drop_queue(today_str="2026-04-07", repo_root=tmp_path)
        result2 = pipeline.drain_drop_queue(today_str="2026-04-07", repo_root=tmp_path)
    # Marker now exists; second run skips entirely
    assert result2.drop_files_processed == 0
    assert lifecycle_mock.call_count == 1


def test_drain_drop_queue_processes_links_drop_files(tmp_path):
    _seed_drop_file(tmp_path, "articles-from-links-2026-04-08.jsonl", [
        {"url": "https://a.com/x", "source_post_id": "links-import", "source_post_url": "",
         "anchor_text": "a", "context_snippet": "c", "category": "personal",
         "discovered_at": "2026-04-08T00:00:00Z", "source_type": "links-import",
         "source_label": "links"},
    ])
    with patch("scripts.articles.pipeline.fetch_article", side_effect=_fake_success_fetch), \
         patch(
             "scripts.articles.pipeline.run_article_entry_lifecycle",
             side_effect=lambda entry, fetch_result, repo_root, today, summarize_override=None: _fake_success_lifecycle(entry=entry, repo_root=repo_root),
         ):
        result = pipeline.drain_drop_queue(today_str="2026-04-08", repo_root=tmp_path)

    assert result.urls_in_queue == 1
    assert result.fetched_summarized == 1
    marker = tmp_path / "wiki" / "sources" / "articles" / ".ingested-articles-from-links-2026-04-08.jsonl"
    assert marker.exists()
