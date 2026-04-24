from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.articles import enrich as articles_enrich
from scripts.articles.fetch import ArticleFetchResult
from scripts.articles.parse import ArticleDropEntry


def _make_entry() -> ArticleDropEntry:
    return ArticleDropEntry(
        url="https://example.com/article",
        source_post_id="190000001",
        source_post_url="https://example.com/source",
        anchor_text="article",
        context_snippet="ctx",
        category="business",
        discovered_at="2026-04-02T18:00:00Z",
        source_type="substack-link",
    )


def _make_fetch_result() -> ArticleFetchResult:
    return ArticleFetchResult(
        body_text="Body text",
        title="Title",
        author="Author",
        sitename="Outlet",
        published="2024-05-15",
        raw_html_path=Path("/tmp/x.html"),
    )


@pytest.mark.parametrize(
    ("provider", "model"),
    [
        ("gemini", "google/gemini-3.1-flash-lite-preview"),
        ("openai", "openai/gpt-5.4"),
        ("anthropic", "anthropic/claude-sonnet-4.6"),
    ],
)
def test_articles_path_switches_provider_from_config(tmp_path, monkeypatch, provider, model):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: SimpleNamespace(
            llm_provider=provider,
            llm_model=model,
            llm_routes={},
            llm_backup=None,
            llm_transport_mode="ai_gateway",
            repo_root=tmp_path,
            browser_for_cookies="chrome",
            substack_session_cookie="",
            ai_gateway_api_key="gateway",
        ),
    )
    called: dict[str, object] = {}

    def fake_execute(self, request):
        called["provider"] = provider
        called["input_parts"] = request.input_parts
        return '{"tldr":"x","key_claims":[],"notable_quotes":[],"takeaways":[],"topics":[],"article":""}'

    monkeypatch.setattr("mind.services.providers.gateway.GatewayProviderClient.execute", fake_execute)

    result = articles_enrich.summarize_article(
        _make_entry(),
        fetch_result=_make_fetch_result(),
        repo_root=tmp_path,
    )

    assert result["tldr"] == "x"
    assert called["provider"] == provider
    text_part = called["input_parts"][0]
    assert "Title" in (text_part.text or "")
