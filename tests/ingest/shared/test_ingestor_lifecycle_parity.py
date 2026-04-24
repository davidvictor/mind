from __future__ import annotations

from pathlib import Path

import pytest

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


ATTRIBUTION_STATUS_MATRIX = {
    "substack_error": "error",
    "articles_person": "implemented",
    "articles_company": "unsupported",
    "youtube_empty": "empty",
    "books_empty": "empty",
}

EMPTY_APPLIED = {"applied_paragraph": "", "applied_bullets": [], "thread_links": []}


def _article_entry() -> ArticleDropEntry:
    return ArticleDropEntry(
        url="https://example.com/article",
        source_post_id="1",
        source_post_url="https://example.com/source",
        anchor_text="article",
        context_snippet="ctx",
        category="business",
        discovered_at="2026-04-09T00:00:00Z",
        source_type="substack-link",
    )


def _article_fetch(**overrides) -> ArticleFetchResult:
    data = {
        "body_text": "Body text",
        "title": "Title",
        "author": "Author Name",
        "sitename": "Example Outlet",
        "published": "2024-05-15",
        "raw_html_path": Path("/tmp/fake.html"),
    }
    data.update(overrides)
    return ArticleFetchResult(**data)


def _youtube_record() -> YouTubeRecord:
    return YouTubeRecord(
        video_id="abc123xyz00",
        title="Test Video",
        channel="Test Channel",
        watched_at="2026-04-01T10:00:00Z",
    )


def _book() -> BookRecord:
    return BookRecord(
        title="Designing Data-Intensive Applications",
        author=["Martin Kleppmann"],
        status="finished",
        finished_date="2026-03-15",
        format="ebook",
    )


def _substack_record() -> SubstackRecord:
    return SubstackRecord(
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


def _write_stance_doc(path: Path, title: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"# {title}\n\n## Core beliefs\n\nPrior stance.\n", encoding="utf-8")


def _capture_articles(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> dict[str, object]:
    captured: dict[str, object] = {}

    def fake_run_ingestion_lifecycle(**kwargs):
        captured.update(kwargs)
        return type("Result", (), {"envelope": {}, "materialized": {}, "propagate": {}})()

    monkeypatch.setattr(articles_enrich, "run_ingestion_lifecycle", fake_run_ingestion_lifecycle)
    articles_enrich.run_article_entry_lifecycle(
        _article_entry(),
        fetch_result=_article_fetch(),
        repo_root=tmp_path,
        today="2026-04-09",
    )
    return captured


def _capture_youtube(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> dict[str, object]:
    captured: dict[str, object] = {}

    def fake_run_ingestion_lifecycle(**kwargs):
        captured.update(kwargs)
        return type("Result", (), {"envelope": {}, "materialized": {}, "propagate": {}})()

    monkeypatch.setattr(youtube_enrich, "classify", lambda record: {"category": "business"})
    monkeypatch.setattr(
        youtube_enrich,
        "fetch_transcription_result",
        lambda record, repo_root: {
            "transcript": "hello world",
            "transcription_path": "transcript-api",
            "multimodal_error": "",
            "fallback_attempts": [],
        },
    )
    monkeypatch.setattr(youtube_enrich, "run_ingestion_lifecycle", fake_run_ingestion_lifecycle)
    youtube_enrich.run_youtube_record_lifecycle(
        _youtube_record(),
        repo_root=tmp_path,
        default_duration_minutes=10.0,
        today="2026-04-09",
    )
    return captured


def _capture_books(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> dict[str, object]:
    captured: dict[str, object] = {}

    def fake_run_ingestion_lifecycle(**kwargs):
        captured.update(kwargs)
        return type("Result", (), {"envelope": {}, "materialized": {}, "propagate": {}})()

    monkeypatch.setattr(books_enrich, "classify", lambda book: {"category": "business"})
    monkeypatch.setattr(books_enrich, "enrich_deep", lambda book: {"tldr": "x", "topics": []})
    monkeypatch.setattr(books_enrich, "summarize_research", lambda book, research: {"tldr": "x", "topics": []})
    monkeypatch.setattr(books_enrich, "run_ingestion_lifecycle", fake_run_ingestion_lifecycle)
    books_enrich.run_book_record_lifecycle(
        _book(),
        repo_root=tmp_path,
        today="2026-04-09",
    )
    return captured


def _capture_substack(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> dict[str, object]:
    captured: dict[str, object] = {}

    def fake_run_ingestion_lifecycle(**kwargs):
        captured.update(kwargs)
        return type("Result", (), {"envelope": {}, "materialized": {}, "propagate": {}})()

    monkeypatch.setattr(substack_enrich, "fetch_body", lambda *_args, **_kwargs: "<p>Trust is the root.</p>")
    monkeypatch.setattr("scripts.substack.html_to_markdown.convert", lambda _html: "# On Trust\n\nTrust is the root.")
    monkeypatch.setattr(substack_enrich, "run_ingestion_lifecycle", fake_run_ingestion_lifecycle)
    substack_enrich.run_substack_record_lifecycle(
        _substack_record(),
        client=object(),
        repo_root=tmp_path,
        today="2026-04-09",
        saved_urls={"https://thegeneralist.substack.com/p/on-trust"},
    )
    return captured


@pytest.mark.parametrize(
    ("capture_fn", "expected_kind"),
    [
        (_capture_substack, "substack"),
        (_capture_articles, "article"),
        (_capture_youtube, "youtube"),
        (_capture_books, "book"),
    ],
)
def test_primary_lanes_wire_full_shared_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capture_fn,
    expected_kind: str,
) -> None:
    captured = capture_fn(monkeypatch, tmp_path)

    source = captured["source"]
    assert source.source_kind == expected_kind  # type: ignore[attr-defined]
    assert captured["understand"] is not None
    assert captured["personalize"] is not None
    assert captured["attribute"] is not None
    assert captured["distill"] is not None
    assert captured["materialize"] is not None
    assert captured["propagate"] is not None


def test_articles_fanout_defers_pass_d_logging_to_pipeline(
    tmp_path: Path,
) -> None:
    monkeypatch = pytest.MonkeyPatch()
    captured = _capture_articles(monkeypatch, tmp_path)
    source = captured["source"]
    propagate = captured["propagate"]

    result = propagate(
        source,
        {
            "pass_d": {
                "warnings": ["q2_candidates[0]: unsupported type 'note'"],
                "dropped_q1_matches": 0,
                "dropped_q2_candidates": 1,
            }
        },
        {},
    )  # type: ignore[misc]
    assert result["pass_d"] == [
        {
            "status": "warning",
            "stage": "pass_d.parse",
            "summary": "1 warning(s); dropped 0 q1 match(es) and 1 q2 candidate(s); first=q2_candidates[0]: unsupported type 'note'",
            "warnings": ["q2_candidates[0]: unsupported type 'note'"],
            "dropped_q1_matches": 0,
            "dropped_q2_candidates": 1,
        }
    ]
    assert result["logged_entities"] == []
    assert str(result["drop_path"]).endswith(".jsonl")
    monkeypatch.undo()


def test_empty_personalization_paths_use_explicit_payloads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    write_repo_config(tmp_path)

    article_payload = articles_enrich.apply_article_to_you(
        _article_entry(),
        fetch_result=_article_fetch(),
        summary={"tldr": "x", "topics": []},
        repo_root=tmp_path,
    )
    assert article_payload == EMPTY_APPLIED

    youtube_payload = youtube_enrich.apply_video_to_you(
        _youtube_record(),
        summary={"tldr": "x", "topics": []},
        repo_root=tmp_path,
    )
    assert youtube_payload == EMPTY_APPLIED

    class FakeCfg:
        repo_root = tmp_path
        wiki_root = tmp_path / "memory"
        raw_root = tmp_path / "raw"
        gemini_api_key = "fake"
        llm_model = "fake"
        browser_for_cookies = "chrome"
        substack_session_cookie = ""

    monkeypatch.setattr("scripts.books.enrich.env.load", lambda: FakeCfg())
    books_payload = books_enrich.apply_to_you(_book(), {"tldr": "x", "topics": []})
    assert books_payload == EMPTY_APPLIED


def test_attribution_status_matrix_is_explicit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    write_repo_config(tmp_path)

    article_entry = _article_entry()
    article_fetch = _article_fetch(author="Ben Thompson", sitename="Stratechery")
    article_source = articles_enrich.normalize_article_source(article_entry, fetch_result=article_fetch)
    _write_stance_doc(tmp_path / "memory" / "people" / "ben-thompson-stance.md", "Ben Thompson — Current Stance")
    class FakeArticleLLMService:
        def cache_identities(self, *, task_class: str, prompt_version: str):
            return [type("Identity", (), {"to_dict": lambda self: {
                "task_class": task_class,
                "provider": "test",
                "model": "test-model",
                "transport": "ai_gateway",
                "api_family": "responses",
                "input_mode": "text",
                "prompt_version": prompt_version,
                "request_fingerprint": None,
                "temperature": None,
                "max_tokens": None,
                "timeout_seconds": None,
                "reasoning_effort": None,
            }})()]

        def update_author_stance(self, **kwargs):
            return {"change_note": "Ben Thompson now emphasizes distribution over audience capture."}

    monkeypatch.setattr("scripts.articles.enrich.get_llm_service", lambda: FakeArticleLLMService())
    monkeypatch.setattr("scripts.articles.enrich._get_llm_service", lambda: FakeArticleLLMService())
    article_person_status = articles_enrich.build_article_attribution(
        article_entry,
        fetch_result=article_fetch,
        source=article_source,
        summary={"tldr": "x", "topics": []},
        repo_root=tmp_path,
    )["status"]

    company_fetch = _article_fetch(author=None, sitename="Example Outlet")
    company_source = articles_enrich.normalize_article_source(article_entry, fetch_result=company_fetch)
    article_company_status = articles_enrich.build_article_attribution(
        article_entry,
        fetch_result=company_fetch,
        source=company_source,
        summary={"tldr": "x", "topics": []},
        repo_root=tmp_path,
    )["status"]

    youtube_status = youtube_enrich.build_channel_attribution(
        _youtube_record(),
        summary={"tldr": "x", "topics": []},
        repo_root=tmp_path,
    )["status"]

    books_status = books_enrich.update_author_memory(
        _book(),
        summary_artifact={"tldr": "x", "topics": []},
        repo_root=tmp_path,
    )["status"]

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
        _substack_record(),
        client=object(),
        repo_root=tmp_path,
        today="2026-04-09",
        saved_urls={"https://thegeneralist.substack.com/p/on-trust"},
    )
    substack_status = captured["attribute"](captured["source"], {"pass_a": {"summary": {}}})["status"]  # type: ignore[misc]

    assert {
        "substack_error": substack_status,
        "articles_person": article_person_status,
        "articles_company": article_company_status,
        "youtube_empty": youtube_status,
        "books_empty": books_status,
    } == ATTRIBUTION_STATUS_MATRIX
