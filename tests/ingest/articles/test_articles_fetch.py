from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.articles import fetch
from scripts.articles.parse import ArticleDropEntry


def _make_entry(**overrides) -> ArticleDropEntry:
    defaults = dict(
        url="https://stratechery.com/2024/aggregators",
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


# Mock returns a dict — fetch.py's normalizer treats dicts pass-through.
FAKE_BARE_EXTRACTION = {
    "text": "# Aggregators\n\nThe internet is dominated by aggregators.\n\nThis matters because...",
    "title": "Aggregators and Jobs-To-Be-Done",
    "author": "Ben Thompson",
    "sitename": "Stratechery",
    "date": "2024-05-15",
    "url": "https://stratechery.com/2024/aggregators",
}


def test_fetch_article_success(tmp_path):
    e = _make_entry()
    with patch("scripts.articles.fetch.trafilatura.fetch_url", return_value="<html>...</html>"), \
         patch("scripts.articles.fetch.trafilatura.bare_extraction", return_value=FAKE_BARE_EXTRACTION):
        result = fetch.fetch_article(e, repo_root=tmp_path)
    assert result is not None
    assert "Aggregators" in result.body_text
    assert result.title == "Aggregators and Jobs-To-Be-Done"
    assert result.author == "Ben Thompson"
    assert result.sitename == "Stratechery"
    assert result.published == "2024-05-15"
    assert result.raw_html_path.exists()
    assert "Aggregators" in result.raw_html_path.read_text(encoding="utf-8")


def test_fetch_article_caches_on_disk(tmp_path):
    e = _make_entry()
    with patch("scripts.articles.fetch.trafilatura.fetch_url", return_value="<html>...</html>") as mock_fetch, \
         patch("scripts.articles.fetch.trafilatura.bare_extraction", return_value=FAKE_BARE_EXTRACTION) as mock_extract:
        fetch.fetch_article(e, repo_root=tmp_path)
        fetch.fetch_article(e, repo_root=tmp_path)
    assert mock_fetch.call_count == 1
    assert mock_extract.call_count == 1


def test_fetch_article_returns_network_failure_on_fetch_failure(tmp_path):
    e = _make_entry()
    with patch("scripts.articles.fetch.trafilatura.fetch_url", return_value=None):
        result = fetch.fetch_article(e, repo_root=tmp_path)
    assert result.failure_kind == "network_failed"
    cache_dir = tmp_path / "raw" / "transcripts" / "articles"
    if cache_dir.exists():
        assert not any(cache_dir.iterdir())


def test_fetch_article_returns_extract_failure_on_extract_failure(tmp_path):
    """trafilatura.bare_extraction returning None means no extractable body."""
    e = _make_entry()
    with patch("scripts.articles.fetch.trafilatura.fetch_url", return_value="<html>junk</html>"), \
         patch("scripts.articles.fetch.trafilatura.bare_extraction", return_value=None):
        result = fetch.fetch_article(e, repo_root=tmp_path)
    assert result.failure_kind == "extraction_failed"


def test_fetch_article_returns_empty_body_failure_on_empty_body(tmp_path):
    """trafilatura returning empty text means we can't summarize."""
    e = _make_entry()
    with patch("scripts.articles.fetch.trafilatura.fetch_url", return_value="<html>...</html>"), \
         patch("scripts.articles.fetch.trafilatura.bare_extraction",
               return_value={"text": "", "title": "Empty", "author": None, "sitename": None, "date": None}):
        result = fetch.fetch_article(e, repo_root=tmp_path)
    assert result.failure_kind == "empty_body"


def test_fetch_article_detects_unsupported_format_hosts(tmp_path):
    e = _make_entry(url="https://twitter.com/someone")
    result = fetch.fetch_article(e, repo_root=tmp_path)
    assert result.failure_kind == "unsupported_format"


def test_fetch_article_detects_root_landing_urls_as_unsupported(tmp_path):
    e = _make_entry(url="https://agent.minimax.io/")
    result = fetch.fetch_article(e, repo_root=tmp_path)
    assert result.failure_kind == "unsupported_format"


def test_fetch_article_handles_missing_metadata_gracefully(tmp_path):
    """If author/sitename/date are missing, the result is still valid."""
    e = _make_entry(url="https://news.ycombinator.com/item?id=1")
    with patch("scripts.articles.fetch.trafilatura.fetch_url", return_value="<html>...</html>"), \
         patch("scripts.articles.fetch.trafilatura.bare_extraction",
               return_value={"text": "Some discussion text.", "title": "HN Discussion",
                             "author": None, "sitename": None, "date": None}):
        result = fetch.fetch_article(e, repo_root=tmp_path)
    assert result is not None
    assert result.body_text == "Some discussion text."
    assert result.title == "HN Discussion"
    assert result.author is None
    assert result.sitename is None
    assert result.published is None
