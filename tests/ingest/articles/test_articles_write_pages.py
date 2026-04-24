from pathlib import Path

import pytest

from scripts.articles import write_pages
from scripts.articles.parse import ArticleDropEntry
from scripts.articles.fetch import ArticleFetchResult
from tests.support import write_repo_config


@pytest.fixture(autouse=True)
def _repo_config(tmp_path):
    write_repo_config(tmp_path)


def _make_entry(**overrides) -> ArticleDropEntry:
    defaults = dict(
        url="https://stratechery.com/2024/aggregators-and-jobs-to-be-done",
        source_post_id="190000001",
        source_post_url="https://thegeneralist.substack.com/p/on-trust",
        anchor_text="aggregators",
        context_snippet="ctx",
        category="business",
        discovered_at="2026-04-02T18:00:00Z",
        source_type="substack-link",
    )
    defaults.update(overrides)
    return ArticleDropEntry(**defaults)


def _make_fetch_result(**overrides) -> ArticleFetchResult:
    defaults = dict(
        body_text="# Header\n\nBody text here.",
        title="Aggregators and Jobs-To-Be-Done",
        author="Ben Thompson",
        sitename="Stratechery",
        published="2024-05-15",
        raw_html_path=Path("/tmp/fake.html"),
    )
    defaults.update(overrides)
    return ArticleFetchResult(**defaults)


def test_slugify_url_basic():
    slug = write_pages.slugify_url(
        "https://stratechery.com/2024/aggregators-and-jobs-to-be-done",
        "2026-04-02T18:00:00Z",
    )
    # Date prefix + hostname + first non-empty path segment
    assert slug.startswith("2026-04-02-")
    assert "stratechery" in slug


def test_slugify_url_strips_www():
    slug = write_pages.slugify_url(
        "https://www.example.com/article",
        "2026-04-02T18:00:00Z",
    )
    assert "www" not in slug


def test_slugify_url_handles_query_strings():
    slug = write_pages.slugify_url(
        "https://news.ycombinator.com/item?id=12345",
        "2026-04-02T18:00:00Z",
    )
    # query strings dropped
    assert "12345" not in slug or slug.startswith("2026-04-02-news-ycombinator")


def test_slugify_url_truncates_long_slugs():
    long_path = "/" + "very-long-segment-" * 20
    slug = write_pages.slugify_url(
        f"https://example.com{long_path}",
        "2026-04-02T18:00:00Z",
    )
    # date prefix is 11 chars, total should be bounded
    assert len(slug) <= 75


def test_article_page_path(tmp_path):
    e = _make_entry()
    p = write_pages.article_page_path(tmp_path, e)
    assert p.parent == tmp_path / "memory" / "sources" / "articles"
    assert p.name.endswith(".md")
    assert p.name.startswith("2026-04-02-")


def test_summary_page_path(tmp_path):
    e = _make_entry()
    p = write_pages.summary_page_path(tmp_path, e)
    assert p.parent == tmp_path / "memory" / "summaries"
    assert p.name.startswith("summary-")
    assert p.name.endswith(".md")


FAKE_SUMMARY = {
    "tldr": "Aggregators dominate the internet.",
    "key_claims": ["Claim A", "Claim B"],
    "notable_quotes": ["Quote 1"],
    "takeaways": ["Action 1", "Action 2"],
    "topics": ["aggregators", "platforms"],
    "article": "Para 1.\n\nPara 2.",
}

FAKE_APPLIED = {
    "applied_paragraph": "This article matters to Example Owner right now.",
    "applied_bullets": [
        {
            "claim": "Use aggregation leverage",
            "why_it_matters": "It compounds distribution",
            "action": "Write with aggregation in mind",
        }
    ],
    "thread_links": ["distribution-leverage"],
}

EMPTY_APPLIED = {"applied_paragraph": "", "applied_bullets": [], "thread_links": []}

FAKE_STANCE_CHANGE = "The author is more explicit about distribution as a strategic moat."


def test_write_article_page_creates_file_with_frontmatter(tmp_path):
    e = _make_entry()
    fr = _make_fetch_result()
    path = write_pages.write_article_page(
        e, fetch_result=fr, summary=FAKE_SUMMARY, repo_root=tmp_path,
    )
    assert path.exists()
    content = path.read_text()
    assert content.startswith("---\n")
    assert "type: article" in content
    assert "Aggregators and Jobs-To-Be-Done" in content
    assert 'discovered_via: "https://thegeneralist.substack.com/p/on-trust"' in content
    assert "[the linking Substack post](https://thegeneralist.substack.com/p/on-trust)" in content
    assert "https://stratechery.com/2024/aggregators-and-jobs-to-be-done" in content
    assert "Aggregators dominate the internet." in content   # tldr in body
    assert "## Key Claims" in content
    assert "- Claim A" in content
    assert "## Notable Quotes" in content
    assert "> Quote 1" in content


def test_write_article_page_prefers_local_substack_provenance_when_available(tmp_path):
    e = _make_entry(source_page_id="2026-04-02-on-trust")
    fr = _make_fetch_result()
    source_page = tmp_path / "memory" / "sources" / "substack" / "thegeneralist" / "2026-04-02-on-trust.md"
    source_page.parent.mkdir(parents=True, exist_ok=True)
    source_page.write_text(
        "---\n"
        "id: 2026-04-02-on-trust\n"
        "type: article\n"
        "title: On Trust\n"
        "status: active\n"
        "created: 2026-04-02\n"
        "last_updated: 2026-04-02\n"
        "aliases: []\n"
        "tags:\n"
        "  - domain/learning\n"
        "  - function/source\n"
        "  - signal/canon\n"
        "domains:\n"
        "  - learning\n"
        "relates_to: []\n"
        "sources: []\n"
        "source_type: substack\n"
        "source_date: 2026-04-02\n"
        "ingested: 2026-04-02\n"
        "author: \"[[mario-gabriele]]\"\n"
        "outlet: \"[[thegeneralist]]\"\n"
        "published: 2026-04-02\n"
        "source_url: \"https://thegeneralist.substack.com/p/on-trust\"\n"
        "saved_at: \"2026-04-02T18:00:00Z\"\n"
        "---\n\n"
        "# On Trust\n",
        encoding="utf-8",
    )
    path = write_pages.write_article_page(
        e, fetch_result=fr, summary=FAKE_SUMMARY, repo_root=tmp_path,
    )
    content = path.read_text(encoding="utf-8")
    assert 'sources:\n  - "[[2026-04-02-on-trust]]"' in content
    assert "_Discovered via [[2026-04-02-on-trust|the linking Substack post]]._" in content
    assert 'discovered_via: "https://thegeneralist.substack.com/p/on-trust"' in content


def test_write_article_page_idempotent_skips_existing(tmp_path):
    e = _make_entry()
    fr = _make_fetch_result()
    path1 = write_pages.write_article_page(
        e, fetch_result=fr, summary=FAKE_SUMMARY, repo_root=tmp_path,
    )
    original = path1.read_text()
    # Mutate summary; second call must NOT rewrite
    different = dict(FAKE_SUMMARY)
    different["tldr"] = "DIFFERENT"
    path2 = write_pages.write_article_page(
        e, fetch_result=fr, summary=different, repo_root=tmp_path,
    )
    assert path1 == path2
    assert path2.read_text() == original


def test_write_article_page_handles_missing_metadata(tmp_path):
    e = _make_entry(url="https://news.ycombinator.com/item?id=12345")
    fr = _make_fetch_result(title=None, author=None, sitename=None, published=None)
    path = write_pages.write_article_page(
        e, fetch_result=fr, summary=FAKE_SUMMARY, repo_root=tmp_path,
    )
    content = path.read_text()
    assert "type: article" in content
    # Title should fall back to anchor text or hostname
    assert "type: article" in content
    # discovered_via still present
    assert "discovered_via:" in content


def test_write_summary_page_creates_file(tmp_path):
    e = _make_entry()
    fr = _make_fetch_result()
    path = write_pages.write_summary_page(
        e, fetch_result=fr, summary=FAKE_SUMMARY, repo_root=tmp_path,
    )
    assert path.exists()
    content = path.read_text()
    assert "type: article" in content
    assert "source_type: article" in content
    assert "source_date:" in content
    assert "summary-2026-04-02-stratechery-com-2024-aggregators-and-jobs-to-be-done" in content
    assert "Aggregators dominate the internet." in content
    assert "- Claim A" in content


def test_write_article_page_renders_phase3_sections_when_present(tmp_path):
    e = _make_entry()
    fr = _make_fetch_result()
    path = write_pages.write_article_page(
        e,
        fetch_result=fr,
        summary=FAKE_SUMMARY,
        repo_root=tmp_path,
        applied=FAKE_APPLIED,
        stance_change_note=FAKE_STANCE_CHANGE,
    )
    content = path.read_text(encoding="utf-8")
    assert "## Applied to You" in content
    assert "This article matters to Example Owner right now." in content
    assert "distribution-leverage" in content
    assert "## Author Stance Update" in content
    assert FAKE_STANCE_CHANGE in content


def test_write_summary_page_renders_phase3_sections_when_present(tmp_path):
    e = _make_entry()
    fr = _make_fetch_result()
    path = write_pages.write_summary_page(
        e,
        fetch_result=fr,
        summary=FAKE_SUMMARY,
        repo_root=tmp_path,
        applied=FAKE_APPLIED,
        stance_change_note=FAKE_STANCE_CHANGE,
    )
    content = path.read_text(encoding="utf-8")
    assert "## Applied to You" in content
    assert "This article matters to Example Owner right now." in content
    assert "## Author Stance Update" in content
    assert FAKE_STANCE_CHANGE in content


def test_write_pages_omit_empty_phase3_sections_cleanly(tmp_path):
    e = _make_entry()
    fr = _make_fetch_result()
    article_path = write_pages.write_article_page(
        e,
        fetch_result=fr,
        summary=FAKE_SUMMARY,
        repo_root=tmp_path,
        applied=EMPTY_APPLIED,
        stance_change_note=None,
    )
    summary_path = write_pages.write_summary_page(
        e,
        fetch_result=fr,
        summary=FAKE_SUMMARY,
        repo_root=tmp_path,
        applied=EMPTY_APPLIED,
        stance_change_note="",
    )
    article_content = article_path.read_text(encoding="utf-8")
    summary_content = summary_path.read_text(encoding="utf-8")
    assert "## Applied to You" not in article_content
    assert "## Author Stance Update" not in article_content
    assert "## Applied to You" not in summary_content
    assert "## Author Stance Update" not in summary_content


def test_write_summary_page_idempotent(tmp_path):
    e = _make_entry()
    fr = _make_fetch_result()
    p1 = write_pages.write_summary_page(
        e, fetch_result=fr, summary=FAKE_SUMMARY, repo_root=tmp_path,
    )
    original = p1.read_text()
    different = dict(FAKE_SUMMARY)
    different["tldr"] = "DIFFERENT"
    write_pages.write_summary_page(
        e, fetch_result=fr, summary=different, repo_root=tmp_path,
    )
    assert p1.read_text() == original


def test_write_summary_page_omits_empty_sections(tmp_path):
    e = _make_entry(url="https://example.com/minimal", source_post_id="999")
    fr = _make_fetch_result()
    minimal = {"tldr": "Just tldr.", "key_claims": [], "notable_quotes": [],
               "takeaways": [], "topics": [], "article": ""}
    path = write_pages.write_summary_page(
        e, fetch_result=fr, summary=minimal, repo_root=tmp_path,
    )
    content = path.read_text()
    assert "## Key Claims" not in content
    assert "## Notable Quotes" not in content
    assert "## Takeaways" not in content
    assert "## TL;DR" in content


def test_write_article_page_uses_filename_slug_as_id(tmp_path):
    """After Plan 02, article page id matches the filename slug (no 'article-' prefix)."""
    e = _make_entry()
    fr = _make_fetch_result()
    path = write_pages.write_article_page(
        e, fetch_result=fr, summary=FAKE_SUMMARY, repo_root=tmp_path,
    )
    content = path.read_text(encoding="utf-8")
    assert f"id: {path.stem}" in content
    assert "id: article-" not in content


def test_write_summary_page_uses_filename_slug_as_id(tmp_path):
    e = _make_entry()
    fr = _make_fetch_result()
    path = write_pages.write_summary_page(
        e, fetch_result=fr, summary=FAKE_SUMMARY, repo_root=tmp_path,
    )
    content = path.read_text(encoding="utf-8")
    assert f"id: {path.stem}" in content


def test_write_article_page_emits_three_tag_axes(tmp_path):
    e = _make_entry()
    fr = _make_fetch_result()
    path = write_pages.write_article_page(
        e, fetch_result=fr, summary=FAKE_SUMMARY, repo_root=tmp_path,
    )
    content = path.read_text(encoding="utf-8")
    assert "domain/" in content
    assert "function/" in content
    assert "signal/" in content
    assert "  - aggregators" in content
    assert "  - article" not in content
    assert "  - web" not in content


def test_write_summary_article_page_emits_three_tag_axes(tmp_path):
    e = _make_entry()
    fr = _make_fetch_result()
    path = write_pages.write_summary_page(
        e, fetch_result=fr, summary=FAKE_SUMMARY, repo_root=tmp_path,
    )
    content = path.read_text(encoding="utf-8")
    assert "domain/" in content
    assert "function/" in content
    assert "signal/" in content
    assert "  - aggregators" in content
    assert "  - article" not in content


def test_write_summary_page_emits_required_fields(tmp_path):
    """Articles summary pages must carry source_type, source_date, ingested for /lint."""
    e = _make_entry()
    fr = _make_fetch_result()
    path = write_pages.write_summary_page(
        e, fetch_result=fr, summary=FAKE_SUMMARY, repo_root=tmp_path,
    )
    content = path.read_text(encoding="utf-8")
    assert "source_type:" in content
    assert "source_date:" in content
    assert "ingested:" in content
    assert "source_type: article" in content


def test_write_article_page_handles_non_substack_discovery(tmp_path):
    e = _make_entry(
        source_post_id="links-import",
        source_post_url="",
        source_type="links-import",
        source_label="links",
    )
    fr = _make_fetch_result()
    path = write_pages.write_article_page(
        e, fetch_result=fr, summary=FAKE_SUMMARY, repo_root=tmp_path,
    )
    content = path.read_text(encoding="utf-8")
    assert "discovered_via: links" in content
    assert "_Discovered via links._" in content
