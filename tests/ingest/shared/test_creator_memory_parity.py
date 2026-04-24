from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mind.services.llm_cache import LLMCacheIdentity
from scripts.articles import enrich as articles_enrich
from scripts.articles.fetch import ArticleFetchResult
from scripts.articles.parse import ArticleDropEntry
from scripts.books import enrich as books_enrich
from scripts.books.parse import BookRecord
from scripts.substack import enrich as substack_enrich
from scripts.substack.parse import SubstackRecord
from scripts.youtube import enrich as youtube_enrich
from scripts.youtube.parse import YouTubeRecord
from tests.support import write_repo_config


_FAKE_IDENTITY = LLMCacheIdentity(
    task_class="stance",
    provider="google",
    model="gemini-test",
    transport="direct",
    api_family="genai",
    input_mode="text",
    prompt_version="test.v1",
)


def _make_stance_svc(return_value):
    """Build a mock LLMService whose update_author_stance returns the given value."""
    svc = MagicMock()
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    svc.update_author_stance.return_value = return_value
    return svc


def _write_stance_doc(path: Path, title: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"# {title}\n\n## Core beliefs\n\nPrior stance.\n",
        encoding="utf-8",
    )


def test_article_person_bylines_produce_real_creator_memory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    entry = ArticleDropEntry(
        url="https://example.com/article",
        source_post_id="1",
        source_post_url="https://example.com/source",
        anchor_text="article",
        context_snippet="ctx",
        category="business",
        discovered_at="2026-04-09T00:00:00Z",
        source_type="substack-link",
    )
    fetch_result = ArticleFetchResult(
        body_text="Body text",
        title="Title",
        author="Ben Thompson",
        sitename="Stratechery",
        published="2024-05-15",
        raw_html_path=tmp_path / "raw" / "transcripts" / "articles" / "fake.html",
    )
    source = articles_enrich.normalize_article_source(entry, fetch_result=fetch_result)
    _write_stance_doc(tmp_path / "memory" / "people" / "ben-thompson-stance.md", "Ben Thompson — Current Stance")
    svc = _make_stance_svc({"change_note": "Ben Thompson now emphasizes distribution over audience capture."})
    monkeypatch.setattr("scripts.articles.enrich._get_llm_service", lambda: svc)
    monkeypatch.setattr("scripts.articles.enrich.get_llm_service", lambda: svc)

    payload = articles_enrich.build_article_attribution(
        entry,
        fetch_result=fetch_result,
        source=source,
        summary={"tldr": "x", "topics": []},
        repo_root=tmp_path,
    )

    assert payload["status"] == "implemented"
    assert "distribution" in payload["stance_change_note"]
    assert "Prior stance" in payload["stance_context"]


def test_article_company_creators_are_explicitly_unsupported(tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    entry = ArticleDropEntry(
        url="https://example.com/article",
        source_post_id="1",
        source_post_url="https://example.com/source",
        anchor_text="article",
        context_snippet="ctx",
        category="business",
        discovered_at="2026-04-09T00:00:00Z",
        source_type="substack-link",
    )
    fetch_result = ArticleFetchResult(
        body_text="Body text",
        title="Title",
        author=None,
        sitename="Example Outlet",
        published="2024-05-15",
        raw_html_path=tmp_path / "raw" / "transcripts" / "articles" / "fake.html",
    )
    source = articles_enrich.normalize_article_source(entry, fetch_result=fetch_result)

    payload = articles_enrich.build_article_attribution(
        entry,
        fetch_result=fetch_result,
        source=source,
        summary={"tldr": "x", "topics": []},
        repo_root=tmp_path,
    )

    assert payload["status"] == "unsupported"
    assert "Phase 3" in payload["reason"]


def test_youtube_channels_produce_real_creator_memory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    record = YouTubeRecord(
        video_id="abc123xyz00",
        title="Test Video",
        channel="Test Channel",
        watched_at="2026-04-01T10:00:00Z",
    )
    _write_stance_doc(tmp_path / "memory" / "channels" / "test-channel-stance.md", "Test Channel — Current Stance")
    svc = _make_stance_svc({"change_note": "The channel is leaning harder into operating systems for knowledge work."})
    monkeypatch.setattr("scripts.youtube.enrich._get_llm_service", lambda: svc)
    monkeypatch.setattr("scripts.youtube.enrich.get_llm_service", lambda: svc)

    payload = youtube_enrich.build_channel_attribution(
        record,
        summary={"tldr": "x", "topics": []},
        repo_root=tmp_path,
    )

    assert payload["status"] == "implemented"
    assert "knowledge work" in payload["stance_change_note"]


def test_books_authors_produce_real_creator_memory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    book = BookRecord(
        title="Designing Data-Intensive Applications",
        author=["Martin Kleppmann"],
        status="finished",
        finished_date="2026-03-15",
        format="ebook",
    )
    _write_stance_doc(tmp_path / "memory" / "people" / "martin-kleppmann-stance.md", "Martin Kleppmann — Current Stance")
    svc = _make_stance_svc({"change_note": "Martin Kleppmann is now more explicit about tradeoffs between consistency and operability."})
    monkeypatch.setattr("scripts.books.enrich._get_llm_service", lambda: svc)
    monkeypatch.setattr("scripts.books.enrich.get_llm_service", lambda: svc)

    payload = books_enrich.update_author_memory(
        book,
        summary_artifact={"tldr": "x", "topics": []},
        repo_root=tmp_path,
    )

    assert payload["status"] == "implemented"
    assert "tradeoffs" in payload["stance_change_note"]


def test_substack_creator_memory_surfaces_explicit_error_instead_of_silent_none(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    record = SubstackRecord(
        id="140000001",
        title="On Trust",
        subtitle="Why the internet runs on it",
        slug="on-trust",
        published_at="2026-03-15T09:00:00Z",
        saved_at="2026-04-02T18:00:00Z",
        url="https://thegeneralist.substack.com/p/on-trust",
        author_name="Mario Gabriele",
        author_id="9001",
        publication_name="The Generalist",
        publication_slug="thegeneralist",
        body_html="<p>Trust is the root.</p>",
        is_paywalled=False,
    )
    captured: dict[str, object] = {}

    def fake_run_ingestion_lifecycle(**kwargs):
        captured.update(kwargs)
        return type("Result", (), {"envelope": {}, "materialized": {}, "propagate": {}})()

    monkeypatch.setattr(substack_enrich, "fetch_body", lambda *_args, **_kwargs: "<p>Trust is the root.</p>")
    monkeypatch.setattr("scripts.substack.html_to_markdown.convert", lambda _html: "# On Trust\n\nTrust is the root.")
    monkeypatch.setattr(substack_enrich, "run_ingestion_lifecycle", fake_run_ingestion_lifecycle)
    monkeypatch.setattr(
        "scripts.substack.enrich.update_author_stance",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    substack_enrich.run_substack_record_lifecycle(
        record,
        client=object(),
        repo_root=tmp_path,
        today="2026-04-09",
        saved_urls={record.url},
    )

    payload = captured["attribute"](captured["source"], {"pass_a": {"summary": {}}})  # type: ignore[misc]
    assert payload["status"] == "error"
    assert payload["stance_change_note"] is None
    assert "RuntimeError: boom" == payload["error"]
