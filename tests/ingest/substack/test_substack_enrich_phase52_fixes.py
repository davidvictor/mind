"""Regression tests for Task 5.2 code review fixes.

Three defensive-behavior edge cases for classify_post_links:
  1. Classifier returns explicit `category: None` — must fall back to "ignore"
  2. Classifier hallucinates a URL not in the input — must be dropped
  3. Classifier omits an entry entirely — missing URL gets default "ignore"
"""
from unittest.mock import MagicMock, patch

from mind.services.llm_cache import LLMCacheIdentity
from scripts.substack import enrich
from scripts.substack.parse import SubstackRecord


_FAKE_IDENTITY = LLMCacheIdentity(
    task_class="classification",
    provider="google",
    model="gemini-test",
    transport="direct",
    api_family="genai",
    input_mode="text",
    prompt_version="test.v1",
)


def _make_record(**overrides) -> SubstackRecord:
    defaults = dict(
        id="140000099",
        title="Regression Post",
        subtitle=None,
        slug="regression-post",
        published_at="2026-03-15T09:00:00Z",
        saved_at="2026-04-02T18:00:00Z",
        url="https://example.substack.com/p/regression-post",
        author_name="Regression Tester",
        author_id="99",
        publication_name="Example",
        publication_slug="example",
        body_html=None,
        is_paywalled=False,
    )
    defaults.update(overrides)
    return SubstackRecord(**defaults)


def test_classify_post_links_handles_explicit_none_category(tmp_path):
    """Gemini JSON mode can return category=None; must fall back to 'ignore'."""
    r = _make_record(body_html='<p><a href="https://a.com/x">x</a></p>')
    fake = [{"url": "https://a.com/x", "category": None, "reason": None}]
    svc = MagicMock()
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    svc.classify_links_batch.return_value = fake
    with patch("scripts.substack.enrich._get_llm_service", return_value=svc):
        body = enrich.fetch_body(r, client=MagicMock(), repo_root=tmp_path)
        result = enrich.classify_post_links(r, body_html=body, repo_root=tmp_path)
    entry = result["external_classified"][0]
    assert entry["category"] == "ignore"
    assert entry["reason"] == ""


def test_classify_post_links_drops_hallucinated_classifier_urls(tmp_path):
    """Classifier returning a URL not in the input must not appear in output."""
    r = _make_record(body_html='<p><a href="https://a.com/x">x</a></p>')
    fake = [
        {"url": "https://a.com/x", "category": "business", "reason": "real"},
        {"url": "https://hallucinated.com/fake", "category": "business", "reason": "not in input"},
    ]
    svc = MagicMock()
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    svc.classify_links_batch.return_value = fake
    with patch("scripts.substack.enrich._get_llm_service", return_value=svc):
        body = enrich.fetch_body(r, client=MagicMock(), repo_root=tmp_path)
        result = enrich.classify_post_links(r, body_html=body, repo_root=tmp_path)
    urls = [e["url"] for e in result["external_classified"]]
    assert urls == ["https://a.com/x"]
    assert not any("hallucinated" in u for u in urls)


def test_classify_post_links_defaults_missing_classifications(tmp_path):
    """If classifier returns fewer entries than input, missing ones default to ignore."""
    r = _make_record(body_html='<p><a href="https://a.com/x">x</a> <a href="https://b.com/y">y</a></p>')
    # Classifier only returns one entry
    fake = [{"url": "https://a.com/x", "category": "business", "reason": "real"}]
    svc = MagicMock()
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    svc.classify_links_batch.return_value = fake
    with patch("scripts.substack.enrich._get_llm_service", return_value=svc):
        body = enrich.fetch_body(r, client=MagicMock(), repo_root=tmp_path)
        result = enrich.classify_post_links(r, body_html=body, repo_root=tmp_path)
    by_url = {e["url"]: e for e in result["external_classified"]}
    assert by_url["https://a.com/x"]["category"] == "business"
    assert by_url["https://b.com/y"]["category"] == "ignore"
