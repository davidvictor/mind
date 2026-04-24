"""Scenario-level smoketest for the articles pipeline.

This keeps one true end-to-end assertion path for the lane while avoiding
paying for repeated copies of the same expensive pipeline run just to inspect
different artifacts.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.articles import pipeline
from scripts.articles.fetch import ArticleFetchFailure, ArticleFetchResult
from scripts.common.vault import Vault
from tests.support import write_repo_config


@pytest.fixture(autouse=True)
def _stub_expensive_article_side_effects(monkeypatch):
    """Bypass unrelated personalization/Pass D work for this lane smoke."""
    monkeypatch.setattr("scripts.articles.enrich.load_stance_context", lambda *a, **kw: "")
    monkeypatch.setattr("scripts.articles.enrich._get_prior_article_context", lambda *a, **kw: "")
    monkeypatch.setattr(
        "scripts.articles.enrich.verify_source_quotes",
        lambda summary, **_kwargs: summary,
    )
    monkeypatch.setattr(
        "scripts.articles.enrich.apply_article_to_you",
        lambda *a, **kw: {"applied_paragraph": "", "applied_bullets": [], "thread_links": []},
    )
    monkeypatch.setattr(
        "scripts.articles.enrich.build_article_attribution",
        lambda *a, **kw: {
            "status": "empty",
            "reason": "smoketest",
            "stance_change_note": "",
            "stance_context": "",
        },
    )
    monkeypatch.setattr("scripts.articles.enrich.run_pass_d_for_article", lambda *a, **kw: {})
    monkeypatch.setattr("scripts.articles.enrich.log_source_entities", lambda *a, **kw: [])
    monkeypatch.setattr(
        "scripts.articles.enrich.append_article_links_to_drop_queue",
        lambda **kwargs: kwargs["repo_root"] / "raw" / "drops" / "articles-from-article-link-2026-04-07.jsonl",
    )
    monkeypatch.setattr("scripts.articles.enrich.write_quality_receipt", lambda **_kwargs: None)


FAKE_SUMMARY = {
    "tldr": "Aggregators dominate the internet.",
    "key_claims": ["Claim A", "Claim B"],
    "notable_quotes": ["The world runs on aggregation."],
    "takeaways": ["Build aggregators"],
    "topics": ["aggregators", "platforms"],
    "article": "Para 1.\n\nPara 2.",
}


SAMPLE_DROP_LINES = [
    {
        "url": "https://stratechery.com/2024/aggregators",
        "source_post_id": "190000001",
        "source_post_url": "https://thegeneralist.substack.com/p/on-trust",
        "source_page_id": "2026-04-02-on-trust",
        "anchor_text": "aggregators",
        "context_snippet": "ctx",
        "category": "business",
        "discovered_at": "2026-04-02T18:00:00Z",
        "source_type": "substack-link",
    },
    {
        "url": "https://example.com/paywalled",
        "source_post_id": "190000001",
        "source_post_url": "https://thegeneralist.substack.com/p/on-trust",
        "anchor_text": "paywalled paper",
        "context_snippet": "ctx",
        "category": "personal",
        "discovered_at": "2026-04-02T18:00:00Z",
        "source_type": "substack-link",
    },
    {
        "url": "https://news.ycombinator.com/item?id=12345",
        "source_post_id": "190000002",
        "source_post_url": "https://stratechery.com/2026/why-aggregators-win",
        "anchor_text": "HN thread",
        "context_snippet": "ctx",
        "category": "personal",
        "discovered_at": "2026-04-03T09:00:00Z",
        "source_type": "substack-link",
    },
]


def _seed(repo_root: Path) -> None:
    write_repo_config(repo_root)
    drops = repo_root / "raw" / "drops"
    drops.mkdir(parents=True, exist_ok=True)
    target = drops / "articles-from-substack-2026-04-07.jsonl"
    target.write_text(
        "\n".join(json.dumps(line, ensure_ascii=False) for line in SAMPLE_DROP_LINES) + "\n",
        encoding="utf-8",
    )
    source_page = repo_root / "memory" / "sources" / "substack" / "thegeneralist" / "2026-04-02-on-trust.md"
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
        "# On Trust\n\n"
        "## Referenced Links\n\n"
        "### Business\n\n"
        "- [aggregators](https://stratechery.com/2024/aggregators)\n",
        encoding="utf-8",
    )


def _fake_fetch(entry, repo_root):
    """Success for Stratechery + HN; paywall failure for example.com."""
    if "example.com/paywalled" in entry.url:
        return ArticleFetchFailure(
            failure_kind="paywalled",
            detail="member-only",
            url=entry.url,
        )

    cache = repo_root / "raw" / "transcripts" / "articles"
    cache.mkdir(parents=True, exist_ok=True)
    fake = cache / "fake.html"
    fake.write_text("body", encoding="utf-8")
    return ArticleFetchResult(
        body_text=f"Real body for {entry.url}",
        title=f"Title for {entry.url}",
        author="Author",
        sitename="Outlet",
        published="2024-05-15",
        raw_html_path=fake,
    )


@pytest.fixture
def _mocked():
    with (
        patch("scripts.articles.pipeline.fetch_article", side_effect=_fake_fetch),
        patch("scripts.articles.pipeline.summarize_article", return_value=FAKE_SUMMARY),
    ):
        yield


def _run_smoketest(repo_root: Path):
    wiki_root = Vault.load(repo_root).wiki
    result = pipeline.drain_drop_queue(today_str="2026-04-07", repo_root=repo_root)
    return result, wiki_root


def _content_snapshot(paths: list[Path]) -> dict[Path, str]:
    return {path: path.read_text(encoding="utf-8") for path in paths}


def test_smoketest_processes_drop_file_and_writes_expected_artifacts(tmp_path, _mocked):
    _seed(tmp_path)
    result, wiki_root = _run_smoketest(tmp_path)

    assert result.fetched_summarized == 2
    assert result.paywalled == 1
    assert result.failed == 0
    assert result.new_pages_written == 2

    inbox = wiki_root / "inbox" / "articles-paywalled-2026-04-07.md"
    assert inbox.exists()
    assert "https://example.com/paywalled" in inbox.read_text(encoding="utf-8")

    marker = wiki_root / "sources" / "articles" / ".ingested-articles-from-substack-2026-04-07.jsonl"
    assert marker.exists()

    pages = sorted((wiki_root / "sources" / "articles").glob("*.md"))
    assert len(pages) == 2
    for page in pages:
        content = page.read_text(encoding="utf-8")
        assert "discovered_via:" in content
        assert "linking Substack post" in content

    source_page = wiki_root / "sources" / "substack" / "thegeneralist" / "2026-04-02-on-trust.md"
    source_content = source_page.read_text(encoding="utf-8")
    assert "## Materialized Linked Pages" in source_content
    assert "### Articles" in source_content
    assert "[[2026-04-02-stratechery-com-2024-aggregators]]" in source_content

    summaries = list((wiki_root / "summaries").glob("summary-article-*.md"))
    assert len(summaries) == 0


def test_smoketest_idempotent_rerun(tmp_path, _mocked):
    _seed(tmp_path)
    _, wiki_root = _run_smoketest(tmp_path)
    pages = sorted((wiki_root / "sources" / "articles").glob("*.md"))
    baseline = _content_snapshot(pages)

    result2 = pipeline.drain_drop_queue(today_str="2026-04-07", repo_root=tmp_path)

    assert result2.drop_files_processed == 0
    assert _content_snapshot(pages) == baseline
