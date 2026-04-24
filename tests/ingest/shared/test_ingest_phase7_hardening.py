from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from mind.services.llm_cache import LLMCacheIdentity, write_llm_cache
from mind.services.reingest import ReingestRequest, _LaneItem, _plan_book_item, _prepare_book_execution, _prepare_youtube_execution, run_reingest
from scripts.articles import enrich as articles_enrich
from scripts.articles.fetch import ArticleFetchResult
from scripts.articles.parse import ArticleDropEntry
from scripts.books import enrich as books_enrich
from scripts.books.parse import BookRecord
from scripts.common.vault import Vault
from mind.commands.ingest import ingest_articles_queue, ingest_substack_export
from scripts.substack import enrich as substack_enrich
from scripts.substack.parse import SubstackRecord
from scripts.youtube import enrich as youtube_enrich
from scripts.youtube.parse import YouTubeRecord
from tests.support import write_repo_config


def _cache_identity(task_class: str, prompt_version: str) -> LLMCacheIdentity:
    return LLMCacheIdentity(
        task_class=task_class,
        provider="test",
        model="test/model",
        transport="ai_gateway",
        api_family="responses",
        input_mode="text",
        prompt_version=prompt_version,
        request_fingerprint={"kind": "test-cache"},
    )


class _FakeCacheService:
    def cache_identities(self, task_class: str, prompt_version: str):
        return [_cache_identity(task_class, prompt_version)]


def test_substack_rerun_is_idempotent_for_existing_pages_and_actors(tmp_path: Path) -> None:
    wiki_root = Vault.load(tmp_path).wiki
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
    summary = {
        "tldr": "Trust matters.",
        "core_argument": "",
        "argument_graph": {},
        "key_claims": [],
        "memorable_examples": [],
        "notable_quotes": [],
        "steelman": "",
        "strongest_rebuttal": "",
        "would_change_mind_if": "",
        "in_conversation_with": [],
        "relates_to_prior": [],
        "topics": [],
    }

    with (
        patch("scripts.substack.enrich.fetch_body", return_value="<p>Trust is the root.</p>"),
        patch("scripts.substack.html_to_markdown.convert", return_value="# On Trust\n\nTrust is the root."),
        patch("scripts.substack.enrich.classify_post_links", return_value={"external_classified": [], "substack_internal": []}),
        patch("scripts.substack.enrich.get_prior_posts_context", return_value=""),
        patch("scripts.substack.stance.load_stance_context", return_value=""),
        patch("scripts.substack.enrich.summarize_post", return_value=summary),
        patch("scripts.substack.enrich.verify_quotes", side_effect=lambda summary, *_args, **_kwargs: summary),
        patch("scripts.substack.enrich.apply_post_to_you", return_value={"applied_paragraph": "", "applied_bullets": [], "socratic_questions": [], "thread_links": []}),
        patch("scripts.substack.enrich.update_author_stance", return_value=None),
        patch("scripts.substack.enrich.run_pass_d_for_substack", return_value={}),
    ):
        substack_enrich.run_substack_record_lifecycle(record, client=object(), repo_root=tmp_path, today="2026-04-09", saved_urls={record.url})
        substack_enrich.run_substack_record_lifecycle(record, client=object(), repo_root=tmp_path, today="2026-04-09", saved_urls={record.url})

    assert len(list((wiki_root / "people").glob("*.md"))) == 1
    assert len(list((wiki_root / "companies").glob("*.md"))) == 1
    assert len(list((wiki_root / "sources" / "substack" / "thegeneralist").glob("*.md"))) == 1
    assert len(list((wiki_root / "summaries").glob("summary-substack-*.md"))) == 0


def test_substack_command_rerun_same_export_stays_idempotent(tmp_path: Path) -> None:
    wiki_root = Vault.load(tmp_path).wiki
    export_path = tmp_path / "raw" / "exports" / "substack-saved-2026-04-09.json"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    export_path.write_text('{"posts":[{"id":"1"}]}', encoding="utf-8")
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
    summary = {
        "tldr": "Trust matters.",
        "core_argument": "",
        "argument_graph": {},
        "key_claims": [],
        "memorable_examples": [],
        "notable_quotes": [],
        "steelman": "",
        "strongest_rebuttal": "",
        "would_change_mind_if": "",
        "in_conversation_with": [],
        "relates_to_prior": [],
        "topics": [],
    }

    with (
        patch("mind.commands.ingest.vault", lambda: Vault.load(tmp_path)),
        patch("mind.commands.ingest.substack_auth.build_client", lambda: object()),
        patch("mind.commands.ingest.substack_parse.parse_export", lambda data: [record]),
        patch("scripts.substack.enrich.fetch_body", return_value="<p>Trust is the root.</p>"),
        patch("scripts.substack.html_to_markdown.convert", return_value="# On Trust\n\nTrust is the root."),
        patch("scripts.substack.enrich.classify_post_links", return_value={"external_classified": [], "substack_internal": []}),
        patch("scripts.substack.enrich.get_prior_posts_context", return_value=""),
        patch("scripts.substack.stance.load_stance_context", return_value=""),
        patch("scripts.substack.enrich.summarize_post", return_value=summary),
        patch("scripts.substack.enrich.verify_quotes", side_effect=lambda summary, *_args, **_kwargs: summary),
        patch("scripts.substack.enrich.apply_post_to_you", return_value={"applied_paragraph": "", "applied_bullets": [], "socratic_questions": [], "thread_links": []}),
        patch("scripts.substack.enrich.update_author_stance", return_value=None),
        patch("scripts.substack.enrich.run_pass_d_for_substack", return_value={}),
    ):
        ingest_substack_export(export_path=export_path, today="2026-04-09", drain_articles=False)
        ingest_substack_export(export_path=export_path, today="2026-04-09", drain_articles=False)

    assert len(list((wiki_root / "people").glob("*.md"))) == 1
    assert len(list((wiki_root / "companies").glob("*.md"))) == 1
    assert len(list((wiki_root / "sources" / "substack" / "thegeneralist").glob("*.md"))) == 1
    assert len(list((wiki_root / "summaries").glob("summary-substack-*.md"))) == 0


def test_article_rerun_is_idempotent_for_existing_pages_and_actors(tmp_path: Path) -> None:
    wiki_root = Vault.load(tmp_path).wiki
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
    fetch = ArticleFetchResult(
        body_text="Body text",
        title="Title",
        author="Author Name",
        sitename="Example Outlet",
        published="2024-05-15",
        raw_html_path=tmp_path / "raw" / "transcripts" / "articles" / "fake.html",
    )
    summary = {"tldr": "x", "key_claims": [], "notable_quotes": [], "takeaways": [], "topics": [], "article": ""}

    with patch("scripts.articles.enrich.summarize_article", return_value=summary), patch("scripts.articles.enrich.run_pass_d_for_article", return_value={}):
        articles_enrich.run_article_entry_lifecycle(entry, fetch_result=fetch, repo_root=tmp_path, today="2026-04-09")
        articles_enrich.run_article_entry_lifecycle(entry, fetch_result=fetch, repo_root=tmp_path, today="2026-04-09")

    assert len(list((wiki_root / "people").glob("*.md"))) == 1
    assert len(list((wiki_root / "companies").glob("*.md"))) == 1
    assert len(list((wiki_root / "sources" / "articles").glob("*.md"))) == 1
    assert len(list((wiki_root / "summaries").glob("summary-article-*.md"))) == 0


def test_articles_command_rerun_requires_marker_removal_and_stays_idempotent(tmp_path: Path) -> None:
    wiki_root = Vault.load(tmp_path).wiki
    drop_dir = tmp_path / "raw" / "drops"
    drop_dir.mkdir(parents=True, exist_ok=True)
    drop_file = drop_dir / "articles-from-substack-2026-04-09.jsonl"
    drop_file.write_text(
        '{"url":"https://example.com/article","source_post_id":"1","source_post_url":"https://example.com/source","anchor_text":"article","context_snippet":"ctx","category":"business","discovered_at":"2026-04-09T00:00:00Z","source_type":"substack-link"}\n',
        encoding="utf-8",
    )
    fetch = ArticleFetchResult(
        body_text="Body text",
        title="Title",
        author="Author Name",
        sitename="Example Outlet",
        published="2024-05-15",
        raw_html_path=tmp_path / "raw" / "transcripts" / "articles" / "fake.html",
    )
    summary = {"tldr": "x", "key_claims": [], "notable_quotes": [], "takeaways": [], "topics": [], "article": ""}

    with (
        patch("mind.commands.ingest.vault", lambda: Vault.load(tmp_path)),
        patch("scripts.articles.pipeline.fetch_article", return_value=fetch),
        patch("scripts.articles.pipeline.summarize_article", return_value=summary),
        patch("scripts.articles.enrich.run_pass_d_for_article", return_value={}),
    ):
        first = ingest_articles_queue(today="2026-04-09", repo_root=tmp_path)
        second = ingest_articles_queue(today="2026-04-09", repo_root=tmp_path)
        marker = wiki_root / "sources" / "articles" / ".ingested-articles-from-substack-2026-04-09.jsonl"
        marker.unlink()
        third = ingest_articles_queue(today="2026-04-09", repo_root=tmp_path)

    assert first.fetched_summarized == 1
    assert second.drop_files_processed == 0
    assert third.fetched_summarized == 1
    assert len(list((wiki_root / "people").glob("*.md"))) == 1
    assert len(list((wiki_root / "companies").glob("*.md"))) == 1
    assert len(list((wiki_root / "sources" / "articles").glob("*.md"))) == 1
    assert len(list((wiki_root / "summaries").glob("summary-article-*.md"))) == 0


def test_youtube_rerun_does_not_broaden_materialization(tmp_path: Path) -> None:
    wiki_root = Vault.load(tmp_path).wiki
    record = YouTubeRecord(
        video_id="abc123xyz00",
        title="Test Video",
        channel="Test Channel",
        watched_at="2026-04-01T10:00:00Z",
    )
    summary = {"tldr": "x", "key_claims": [], "notable_quotes": [], "takeaways": [], "topics": [], "article": ""}

    with (
        patch("scripts.common.env.load", return_value=SimpleNamespace(repo_root=tmp_path, gemini_api_key="fake", llm_model="fake", browser_for_cookies="chrome", substack_session_cookie="")),
        patch("scripts.youtube.enrich.classify", return_value={"category": "business"}),
        patch(
            "scripts.youtube.enrich.fetch_transcription_result",
            return_value={"transcript": "hello world", "transcription_path": "transcript-api", "multimodal_error": "", "fallback_attempts": []},
        ),
        patch("scripts.youtube.enrich.summarize", return_value=summary),
        patch("scripts.youtube.enrich.run_pass_d_for_youtube", return_value={}),
    ):
        youtube_enrich.run_youtube_record_lifecycle(record, repo_root=tmp_path, default_duration_minutes=10.0, today="2026-04-09")
        youtube_enrich.run_youtube_record_lifecycle(record, repo_root=tmp_path, default_duration_minutes=10.0, today="2026-04-09")

    assert not (wiki_root / "people").exists()
    assert len(list((wiki_root / "channels").glob("*.md"))) == 1
    assert len(list((wiki_root / "sources" / "youtube" / "business").glob("*.md"))) == 1
    assert len(list((wiki_root / "summaries").glob("summary-yt-*.md"))) == 0


def test_books_rerun_is_idempotent_for_existing_pages_and_actors(tmp_path: Path) -> None:
    wiki_root = Vault.load(tmp_path).wiki
    book = BookRecord(
        title="Designing Data-Intensive Applications",
        author=["Martin Kleppmann", "Co Author"],
        publisher="Addison-Wesley",
        status="finished",
        finished_date="2026-03-15",
        format="ebook",
    )
    deep = {"tldr": "x", "topics": [], "key_ideas": [], "frameworks_introduced": [], "in_conversation_with": [], "notable_quotes": [], "takeaways": []}

    with (
        patch("scripts.common.env.load", return_value=SimpleNamespace(repo_root=tmp_path, gemini_api_key="fake", llm_model="fake", browser_for_cookies="chrome", substack_session_cookie="")),
        patch("scripts.books.enrich.classify", return_value={"category": "business"}),
        patch("scripts.books.enrich.enrich_deep", return_value=deep),
        patch("scripts.books.enrich.summarize_research", return_value=deep),
        patch("scripts.books.enrich.apply_to_you", return_value={"applied_paragraph": "", "applied_bullets": [], "thread_links": []}),
        patch("scripts.books.enrich.run_pass_d_for_book", return_value={}),
    ):
        books_enrich.run_book_record_lifecycle(book, repo_root=tmp_path, today="2026-04-09")
        books_enrich.run_book_record_lifecycle(book, repo_root=tmp_path, today="2026-04-09")

    assert len(list((wiki_root / "people").glob("*.md"))) == 1
    assert len(list((wiki_root / "companies").glob("*.md"))) == 1
    assert len(list((wiki_root / "sources" / "books" / "business").glob("*.md"))) == 1
    assert len(list((wiki_root / "summaries").glob("summary-book-*.md"))) == 0


def test_youtube_reingest_prepare_rejects_ignored_category(tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    record = YouTubeRecord(
        video_id="ngLQfhJZ7Rs",
        title="Car video",
        channel="Doug DeMuro",
        watched_at="2026-04-01T10:00:00Z",
    )

    with (
        patch("scripts.youtube.enrich.classify", return_value={"category": "ignore"}),
        patch("scripts.youtube.enrich.fetch_transcription_result", return_value={"transcript": "car transcript"}),
    ):
        with pytest.raises(ValueError, match="excluded by content policy \\(ignore\\)"):
            _prepare_youtube_execution(
                ReingestRequest(lane="youtube", stage="pass_a", through="materialize", dry_run=False),
                tmp_path,
                record,
            )


def test_youtube_reingest_plan_treats_content_policy_exclusions_as_nonblocking(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path)
    monkeypatch.setattr("mind.services.reingest.get_llm_service", lambda: _FakeCacheService())

    export = tmp_path / "raw" / "exports" / "youtube-recent-2026-04-09.json"
    export.parent.mkdir(parents=True, exist_ok=True)
    export.write_text(
        json.dumps(
            [
                {
                    "video_id": "ngLQfhJZ7Rs",
                    "title": "Car video",
                    "channel": "Doug DeMuro",
                    "watched_at": "2026-04-01T10:00:00Z",
                }
            ]
        ),
        encoding="utf-8",
    )
    write_llm_cache(
        youtube_enrich.classification_path(tmp_path, "ngLQfhJZ7Rs"),
        identity=_cache_identity("classification", youtube_enrich.CLASSIFY_VIDEO_PROMPT_VERSION),
        data={"retention": "exclude", "domains": ["personal"], "synthesis_mode": "none", "category": "ignore"},
    )

    result = run_reingest(
        ReingestRequest(lane="youtube", stage="acquire", through="propagate", dry_run=True),
        repo_root=tmp_path,
    )

    assert result.plan.selected_count == 1
    assert result.plan.blocked_count == 0
    assert result.plan.projected_rewrites == 0
    assert result.plan.selected_items[0].excluded_reason == "excluded by content policy (ignore)"


def test_books_reingest_prepare_rejects_ignored_category(tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    book = BookRecord(
        title="Some ignored book",
        author=["Author"],
        status="finished",
        finished_date="2026-03-15",
        format="ebook",
    )

    with patch("scripts.books.enrich.classify", return_value={"category": "ignore"}):
        with pytest.raises(ValueError, match="excluded by content policy \\(ignore\\)"):
            _prepare_book_execution(
                ReingestRequest(lane="books", stage="pass_a", through="materialize", dry_run=False),
                tmp_path,
                book,
            )


def test_books_reingest_plan_treats_skipped_books_as_nonblocking(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path)
    monkeypatch.setattr("mind.services.reingest.get_llm_service", lambda: _FakeCacheService())
    book = BookRecord(
        title="Some ignored book",
        author=["Author"],
        status="finished",
        finished_date="2026-03-15",
        format="ebook",
    )
    write_llm_cache(
        books_enrich.classification_path(tmp_path, book),
        identity=_cache_identity("classification", books_enrich.CLASSIFY_BOOK_PROMPT_VERSION),
        data={"retention": "exclude", "domains": ["personal"], "synthesis_mode": "none", "category": "ignore"},
    )

    plan = _plan_book_item(
        tmp_path,
        ReingestRequest(lane="books", stage="summary", through="propagate", dry_run=True),
        _LaneItem(
            lane="books",
            source_id="book-author-some-ignored-book",
            label=book.title,
            payload=book,
            source_label="books.csv",
        ),
    )

    assert plan.blocked_reasons == ()
    assert plan.projected_rewrites == 0
    assert plan.excluded_reason == "excluded by content policy (ignore)"


def test_books_reingest_prepare_force_deep_overrides_light_synthesis(tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    book = BookRecord(
        title="Some personal book",
        author=["Author"],
        status="finished",
        finished_date="2026-03-15",
        format="ebook",
    )
    seen: dict[str, object] = {}

    def fake_normalize_book_source(_book, *, classification, research, source_kind, source_text="", source_asset_path=""):
        seen["classification"] = dict(classification)
        return SimpleNamespace(
            source_id="book-author-some-personal-book",
            creator_candidates=[],
            provenance={"source_kind": source_kind, "source_asset_path": source_asset_path},
            primary_content="body text",
            source_metadata={},
            discovered_links=[],
        )

    with (
        patch("scripts.books.enrich.classify", return_value={"retention": "keep", "domains": ["personal"], "synthesis_mode": "light", "category": "personal"}),
        patch("scripts.books.enrich.enrich_from_source", return_value=None),
        patch("scripts.books.enrich.enrich_deep", return_value={"tldr": "x"}),
        patch("scripts.books.enrich.normalize_book_source", side_effect=fake_normalize_book_source),
        patch("mind.services.reingest._book_handlers", return_value="handlers"),
        patch("mind.services.reingest._seed_book_envelope", return_value={"pass_a": {"classification": {}}}),
    ):
        _prepare_book_execution(
            ReingestRequest(lane="books", stage="pass_a", through="materialize", dry_run=False, force_deep=True),
            tmp_path,
            book,
        )

    classification = seen["classification"]
    assert isinstance(classification, dict)
    assert classification["synthesis_mode"] == "deep"
    assert classification["retention"] == "keep"
    assert classification["category"] == "personal"


def test_books_reingest_prepare_audiobooks_default_to_deep_synthesis(tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    book = BookRecord(
        title="Some audiobook",
        author=["Author"],
        status="finished",
        finished_date="2026-03-15",
        format="audiobook",
        asin="B00AUDIO123",
    )
    seen: dict[str, object] = {}

    def fake_normalize_book_source(_book, *, classification, research, source_kind, source_text="", source_asset_path=""):
        seen["classification"] = dict(classification)
        return SimpleNamespace(
            source_id="book-author-some-audiobook",
            creator_candidates=[],
            provenance={"source_kind": source_kind, "source_asset_path": source_asset_path},
            primary_content="body text",
            source_metadata={},
            discovered_links=[],
        )

    with (
        patch("scripts.books.enrich.classify", return_value={"retention": "keep", "domains": ["personal"], "synthesis_mode": "light", "category": "personal"}),
        patch("scripts.books.enrich.enrich_from_source", return_value=None),
        patch("scripts.books.enrich.enrich_deep", return_value={"tldr": "x"}),
        patch("scripts.books.enrich.normalize_book_source", side_effect=fake_normalize_book_source),
        patch("mind.services.reingest._book_handlers", return_value="handlers"),
        patch("mind.services.reingest._seed_book_envelope", return_value={"pass_a": {"classification": {}}}),
    ):
        _prepare_book_execution(
            ReingestRequest(lane="books", stage="pass_a", through="materialize", dry_run=False),
            tmp_path,
            book,
        )

    classification = seen["classification"]
    assert isinstance(classification, dict)
    assert classification["synthesis_mode"] == "deep"
    assert classification["retention"] == "keep"
    assert classification["category"] == "personal"


def test_youtube_light_policy_materializes_without_aggressive_fanout(tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    record = YouTubeRecord(
        video_id="light123456",
        title="A history lecture",
        channel="History Channel",
        watched_at="2026-04-01T10:00:00Z",
    )
    summary = {"tldr": "x", "key_claims": [], "notable_quotes": [], "takeaways": [], "topics": [], "article": ""}

    with (
        patch("scripts.common.env.load", return_value=SimpleNamespace(repo_root=tmp_path, gemini_api_key="fake", llm_model="fake", browser_for_cookies="chrome", substack_session_cookie="")),
        patch("scripts.youtube.enrich.classify", return_value={"retention": "keep", "domains": ["personal"], "synthesis_mode": "light", "category": "personal"}),
        patch("scripts.youtube.enrich.fetch_transcription_result", return_value={"transcript": "hello world", "transcription_path": "transcript-api", "multimodal_error": "", "fallback_attempts": []}),
        patch("scripts.youtube.enrich.summarize", return_value=summary),
        patch("scripts.youtube.enrich.run_pass_d_for_youtube", side_effect=AssertionError("pass_d should not run for light sources")),
        patch("scripts.youtube.enrich.append_article_links_to_drop_queue", side_effect=AssertionError("fanout queue should not run for light sources")),
        patch("scripts.youtube.enrich.log_source_entities", side_effect=AssertionError("entity logging should not run for light sources")),
    ):
        result = youtube_enrich.run_youtube_record_lifecycle(record, repo_root=tmp_path, default_duration_minutes=10.0, today="2026-04-09")

    assert result is not None
    assert result.propagate["skipped"] is True
    assert result.propagate["reason"] == "synthesis_mode is not deep"
    assert (tmp_path / "memory" / "sources" / "youtube" / "personal").exists()
    assert not (tmp_path / "memory" / "summaries" / "summary-yt-light123456.md").exists()


def test_books_light_policy_materializes_without_aggressive_fanout(tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    book = BookRecord(
        title="History of Cities",
        author=["Author"],
        status="finished",
        finished_date="2026-03-15",
        format="ebook",
    )
    summary = {"tldr": "x", "topics": [], "key_ideas": [], "frameworks_introduced": [], "in_conversation_with": [], "notable_quotes": []}

    with (
        patch("scripts.common.env.load", return_value=SimpleNamespace(repo_root=tmp_path, gemini_api_key="fake", llm_model="fake", browser_for_cookies="chrome", substack_session_cookie="")),
        patch("scripts.books.enrich.classify", return_value={"retention": "keep", "domains": ["personal"], "synthesis_mode": "light", "category": "personal", "subcategory": "history"}),
        patch("scripts.books.enrich.enrich_deep", return_value=summary),
        patch("scripts.books.enrich.summarize_research", return_value=summary),
        patch("scripts.books.enrich.run_pass_d_for_book", side_effect=AssertionError("pass_d should not run for light books")),
        patch("scripts.books.enrich.log_source_entities", side_effect=AssertionError("entity logging should not run for light books")),
    ):
        result = books_enrich.run_book_record_lifecycle(book, repo_root=tmp_path, today="2026-04-09")

    assert result is not None
    assert result.propagate["skipped"] is True
    assert result.propagate["reason"] == "synthesis_mode is not deep"
    assert (tmp_path / "memory" / "sources" / "books" / "personal").exists()
    assert not (tmp_path / "memory" / "summaries" / "summary-book-author-history-of-cities.md").exists()


def test_articles_reingest_inventory_ignores_marker_files(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_exports=True)
    drop_dir = tmp_path / "raw" / "drops"
    drop_dir.mkdir(parents=True, exist_ok=True)
    drop_file = drop_dir / "articles-from-substack-2026-04-09.jsonl"
    drop_file.write_text(
        '{"url":"https://example.com/article","source_post_id":"1","source_post_url":"https://example.com/source","anchor_text":"article","context_snippet":"ctx","category":"business","discovered_at":"2026-04-09T00:00:00Z","source_type":"substack-link"}\n',
        encoding="utf-8",
    )
    marker = tmp_path / "memory" / "sources" / "articles" / ".ingested-articles-from-substack-2026-04-09.jsonl"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("", encoding="utf-8")

    slug = "2026-04-09-example-com-article"
    cache = tmp_path / "raw" / "transcripts" / "articles" / f"{slug}.html"
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text("Body text", encoding="utf-8")
    cache.with_suffix(".meta.json").write_text(
        '{"title":"Title","author":"Author Name","sitename":"Example Outlet","published":"2024-05-15"}',
        encoding="utf-8",
    )

    identity = LLMCacheIdentity(
        task_class="summary",
        provider="test",
        model="test",
        transport="ai_gateway",
        api_family="responses",
        input_mode="text",
        prompt_version="articles.summary.v1",
    )
    write_llm_cache(
        tmp_path / "raw" / "transcripts" / "articles" / f"{slug}.json",
        identity=identity,
        data={"tldr": "x", "topics": [], "key_claims": [], "notable_quotes": [], "takeaways": [], "article": ""},
    )

    class FakeService:
        def cache_identities(self, task_class: str, prompt_version: str):
            return [
                LLMCacheIdentity(
                    task_class=task_class,
                    provider="test",
                    model="test",
                    transport="ai_gateway",
                    api_family="responses",
                    input_mode="text",
                    prompt_version=prompt_version,
                )
            ]

    monkeypatch.setattr("mind.services.reingest.get_llm_service", lambda: FakeService())

    result = run_reingest(
        ReingestRequest(
            lane="articles",
            stage="pass_a",
            through="materialize",
            dry_run=True,
        ),
        repo_root=tmp_path,
    )

    assert result.plan.selected_count == 1
    assert result.plan.selected_items[0].source_id == f"article-{slug}"


def test_articles_reingest_apply_materialize_skips_upstream_llm_stages(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_exports=True)
    drop_dir = tmp_path / "raw" / "drops"
    drop_dir.mkdir(parents=True, exist_ok=True)
    drop_file = drop_dir / "articles-from-substack-2026-04-09.jsonl"
    drop_file.write_text(
        '{"url":"https://example.com/article","source_post_id":"1","source_post_url":"https://example.com/source","anchor_text":"article","context_snippet":"ctx","category":"business","discovered_at":"2026-04-09T00:00:00Z","source_type":"substack-link"}\n',
        encoding="utf-8",
    )

    slug = "2026-04-09-example-com-article"
    cache = tmp_path / "raw" / "transcripts" / "articles" / f"{slug}.html"
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text("Body text", encoding="utf-8")
    cache.with_suffix(".meta.json").write_text(
        '{"title":"Title","author":"Author Name","sitename":"Example Outlet","published":"2024-05-15"}',
        encoding="utf-8",
    )

    def _identity(task_class: str, prompt_version: str) -> LLMCacheIdentity:
        return LLMCacheIdentity(
            task_class=task_class,
            provider="test",
            model="test",
            transport="ai_gateway",
            api_family="responses",
            input_mode="text",
            prompt_version=prompt_version,
        )

    write_llm_cache(
        tmp_path / "raw" / "transcripts" / "articles" / f"{slug}.json",
        identity=_identity("summary", "articles.summary.v1"),
        data={"tldr": "x", "topics": [], "key_claims": [], "notable_quotes": [], "takeaways": [], "article": ""},
    )
    write_llm_cache(
        tmp_path / "raw" / "transcripts" / "articles" / f"{slug}.applied.json",
        identity=_identity("personalization", youtube_enrich.APPLIED_TO_YOU_PROMPT_VERSION),
        data={"applied_paragraph": "", "applied_bullets": [], "thread_links": []},
    )
    write_llm_cache(
        tmp_path / "raw" / "transcripts" / "articles" / f"{slug}.stance.json",
        identity=_identity("stance", youtube_enrich.UPDATE_AUTHOR_STANCE_PROMPT_VERSION),
        data={"status": "empty", "stance_change_note": "", "stance_context": ""},
    )

    class FakeService:
        def cache_identities(self, task_class: str, prompt_version: str):
            return [_identity(task_class, prompt_version)]

    monkeypatch.setattr("mind.services.reingest.get_llm_service", lambda: FakeService())
    monkeypatch.setattr("scripts.articles.enrich.summarize_article", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pass_a should not run")))
    monkeypatch.setattr("scripts.articles.enrich.apply_article_to_you", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pass_b should not run")))
    monkeypatch.setattr("scripts.articles.enrich.build_article_attribution", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pass_c should not run")))

    seen = {"article": 0}
    monkeypatch.setattr("scripts.articles.enrich.run_pass_d_for_article", lambda *args, **kwargs: {})
    monkeypatch.setattr("scripts.articles.write_pages.write_article_page", lambda *args, **kwargs: seen.__setitem__("article", seen["article"] + 1) or (tmp_path / "memory" / "sources" / "articles" / f"{slug}.md"))
    monkeypatch.setattr("scripts.articles.write_pages.ensure_author_page", lambda *args, **kwargs: None)
    monkeypatch.setattr("scripts.articles.write_pages.ensure_outlet_page", lambda *args, **kwargs: None)

    result = run_reingest(
        ReingestRequest(
            lane="articles",
            stage="materialize",
            through="materialize",
            dry_run=False,
        ),
        repo_root=tmp_path,
    )

    assert result.exit_code == 0
    assert seen == {"article": 1}
