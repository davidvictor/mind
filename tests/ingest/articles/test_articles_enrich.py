from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scripts.articles import enrich
from scripts.articles.parse import ArticleDropEntry
from scripts.articles.fetch import ArticleFetchResult
from mind.services.llm_cache import LLMCacheIdentity


def _make_entry(**overrides):
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


def _make_fetch_result():
    return ArticleFetchResult(
        body_text="Body text",
        title="Title",
        author="Author",
        sitename="Outlet",
        published="2024-05-15",
        raw_html_path=Path("/tmp/x.html"),
    )


FAKE_SUMMARY = {
    "tldr": "x", "key_claims": [], "notable_quotes": [],
    "takeaways": [], "topics": [], "article": "",
}


_FAKE_IDENTITY = LLMCacheIdentity(
    task_class="summary",
    provider="google",
    model="gemini-test",
    transport="direct",
    api_family="genai",
    input_mode="text",
    prompt_version="articles.summary.v2",
)


def _mock_llm_service(summarize_return=FAKE_SUMMARY):
    """Build a mock LLMService whose summarize_article_text returns the given value."""
    svc = MagicMock()
    svc.summarize_article_text.return_value = summarize_return
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    return svc


def test_summary_cache_path(tmp_path):
    e = _make_entry()
    p = enrich.summary_cache_path(tmp_path, e)
    assert p.parent == tmp_path / "raw" / "transcripts" / "articles"
    assert p.name.endswith(".json")


def test_summarize_article_caches_result(tmp_path):
    e = _make_entry()
    fr = _make_fetch_result()
    svc = _mock_llm_service()
    with patch("scripts.articles.enrich._get_llm_service", return_value=svc), \
         patch("scripts.articles.enrich.get_llm_service", return_value=svc):
        result = enrich.summarize_article(e, fetch_result=fr, repo_root=tmp_path)
    assert result == FAKE_SUMMARY
    assert enrich.summary_cache_path(tmp_path, e).exists()
    svc.summarize_article_text.assert_called_once()


def test_summarize_article_reads_cache_on_second_call(tmp_path):
    e = _make_entry()
    fr = _make_fetch_result()
    svc = _mock_llm_service()
    with patch("scripts.articles.enrich._get_llm_service", return_value=svc), \
         patch("scripts.articles.enrich.get_llm_service", return_value=svc):
        enrich.summarize_article(e, fetch_result=fr, repo_root=tmp_path)
        enrich.summarize_article(e, fetch_result=fr, repo_root=tmp_path)
    assert svc.summarize_article_text.call_count == 1


def test_summarize_article_passes_fields_to_llm_service(tmp_path):
    e = _make_entry()
    fr = _make_fetch_result()
    svc = _mock_llm_service()
    with patch("scripts.articles.enrich._get_llm_service", return_value=svc), \
         patch("scripts.articles.enrich.get_llm_service", return_value=svc):
        enrich.summarize_article(e, fetch_result=fr, repo_root=tmp_path)
    kwargs = svc.summarize_article_text.call_args.kwargs
    assert kwargs["title"] == "Title"
    assert kwargs["url"] == "https://stratechery.com/2024/aggregators"
    assert kwargs["body_markdown"] == "Body text"
    assert kwargs["sitename"] == "Outlet"
