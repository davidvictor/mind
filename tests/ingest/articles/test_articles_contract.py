from __future__ import annotations

from pathlib import Path

from scripts.articles import enrich, write_pages
from scripts.articles.fetch import ArticleFetchResult
from scripts.articles.parse import ArticleDropEntry


def _entry(**overrides) -> ArticleDropEntry:
    defaults = dict(
        url="https://example.com/article",
        source_post_id="190000001",
        source_post_url="https://example.com/source",
        anchor_text="article",
        context_snippet="ctx",
        category="business",
        discovered_at="2026-04-02T18:00:00Z",
        source_type="substack-link",
    )
    defaults.update(overrides)
    return ArticleDropEntry(**defaults)


def _fetch(**overrides) -> ArticleFetchResult:
    defaults = dict(
        body_text="Body text",
        title="Title",
        author="Author Name",
        sitename="Example Outlet",
        published="2024-05-15",
        raw_html_path=Path("/tmp/x.html"),
    )
    defaults.update(overrides)
    return ArticleFetchResult(**defaults)


def test_normalize_article_source_human_byline_creates_person_and_company_candidates() -> None:
    source = enrich.normalize_article_source(
        _entry(),
        fetch_result=_fetch(author="Ben Thompson", sitename="Stratechery", body_text="See https://example.com/report for background."),
    )
    roles = {(candidate["role"], candidate["page_type"]) for candidate in source.creator_candidates}
    assert ("creator", "person") in roles
    assert ("publisher", "company") in roles
    assert source.discovered_links[0]["url"] == "https://example.com/report"


def test_normalize_article_source_no_byline_falls_back_to_company_creator() -> None:
    source = enrich.normalize_article_source(
        _entry(),
        fetch_result=_fetch(author=None, sitename="Stratechery"),
    )
    assert source.creator_candidates[0]["role"] == "creator"
    assert source.creator_candidates[0]["page_type"] == "company"
    assert source.creator_candidates[1]["role"] == "publisher"
    assert source.creator_candidates[1]["page_type"] == "company"


def test_normalize_article_source_org_author_falls_back_to_company_creator() -> None:
    source = enrich.normalize_article_source(
        _entry(),
        fetch_result=_fetch(author="Example Media", sitename="Example Media"),
    )
    assert source.creator_candidates[0]["page_type"] == "company"
    assert source.creator_candidates[1]["page_type"] == "company"


def test_normalize_article_source_multiple_authors_uses_primary_person_only() -> None:
    source = enrich.normalize_article_source(
        _entry(),
        fetch_result=_fetch(author="Alice Author, Bob Writer", sitename="Example Outlet"),
    )
    assert source.creator_candidates[0]["page_type"] == "person"
    assert source.creator_candidates[0]["name"] == "Alice Author"
    assert source.source_metadata["additional_author_hints"] == ["Bob Writer"]


def test_normalize_article_source_unclear_publication_hosted_content_uses_company() -> None:
    source = enrich.normalize_article_source(
        _entry(),
        fetch_result=_fetch(author="Editorial Team", sitename="Example Outlet"),
    )
    assert source.creator_candidates[0]["page_type"] == "company"
    assert source.creator_candidates[1]["page_type"] == "company"


def test_write_pages_preserve_low_confidence_strings_when_no_targets(tmp_path: Path) -> None:
    entry = _entry()
    fetch_result = _fetch(author="Unclear Byline", sitename="Example Outlet")
    summary = {
        "tldr": "x",
        "key_claims": [],
        "notable_quotes": [],
        "takeaways": [],
        "topics": [],
        "article": "",
    }
    article = write_pages.write_article_page(
        entry,
        fetch_result=fetch_result,
        summary=summary,
        repo_root=tmp_path,
    )
    text = article.read_text(encoding="utf-8")
    assert "author: Unclear Byline" in text
    assert "outlet: Example Outlet" in text


def test_write_article_page_force_rewrites_existing_flat_page(tmp_path: Path) -> None:
    entry = _entry()
    fetch_result = _fetch(author="Ben Thompson", sitename="Stratechery")
    path = write_pages.article_page_path(tmp_path, entry)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("---\nid: legacy\ntype: article\n---\n\n# Legacy\n", encoding="utf-8")

    targets = enrich._materialization_targets_from_source(
        enrich.normalize_article_source(entry, fetch_result=fetch_result)
    )

    summary = {
        "tldr": "x",
        "key_claims": [],
        "notable_quotes": [],
        "takeaways": [],
        "topics": [],
        "article": "",
    }
    rewritten = write_pages.write_article_page(
        entry,
        fetch_result=fetch_result,
        summary=summary,
        repo_root=tmp_path,
        creator_target=targets.creator_target,
        publisher_target=targets.publisher_target,
        force=True,
    )
    text = rewritten.read_text(encoding="utf-8")
    assert "legacy" not in text
    assert 'author: "[[ben-thompson]]"' in text
