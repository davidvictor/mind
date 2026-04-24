"""End-to-end smoketest for the substack pipeline.

Uses the fixture at raw/exports/substack-saved-smoketest.json. Mocks:
  - requests session (no real network)
  - gemini.summarize_substack_post + gemini.classify_links_batch

Verifies:
  - Article pages written for accessible posts with correct paths
  - Summary pages written
  - Author and publication stubs created
  - Paywalled post is NOT written as a page, and IS surfaced via Paywalled exception
  - Link drop queue contains non-ignored external links only
  - Substack-internal links to unsaved posts are captured
  - Re-running the pipeline is a fast no-op
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from mind.services.llm_cache import LLMCacheIdentity
from scripts.common.vault import Vault
from scripts.substack import enrich, parse, write_pages
from scripts.substack.enrich import FetchFailed, Paywalled
from scripts.substack.html_to_markdown import convert as html_to_md
from tests.paths import FIXTURES_ROOT
from tests.support import write_repo_config


_FAKE_IDENTITY = LLMCacheIdentity(
    task_class="summary",
    provider="google",
    model="gemini-test",
    transport="direct",
    api_family="genai",
    input_mode="text",
    prompt_version="test.v1",
)


EXPORT = FIXTURES_ROOT / "substack" / "substack-saved-smoketest.json"


FAKE_SUMMARY_TRUST = {
    "tldr": "Trust is the root.",
    "core_argument": "Trust is foundational.",
    "argument_graph": {"premises": [], "inferences": [], "conclusion": ""},
    "key_claims": [
        {
            "claim": "Claim A",
            "evidence_quote": "trust is the root",
            "evidence_context": "",
            "quote_unverified": False,
        }
    ],
    "memorable_examples": [],
    "notable_quotes": ["Trust is everything."],
    "steelman": "",
    "strongest_rebuttal": "",
    "would_change_mind_if": "",
    "in_conversation_with": [],
    "relates_to_prior": [],
    "topics": ["trust"],
    "article": "Body.",
    "schema_version": 2,
}
FAKE_SUMMARY_AGG = {
    "tldr": "Aggregators win.",
    "core_argument": "",
    "argument_graph": {"premises": [], "inferences": [], "conclusion": ""},
    "key_claims": [
        {
            "claim": "Claim B",
            "evidence_quote": "",
            "evidence_context": "",
            "quote_unverified": False,
        }
    ],
    "memorable_examples": [],
    "notable_quotes": [],
    "steelman": "",
    "strongest_rebuttal": "",
    "would_change_mind_if": "",
    "in_conversation_with": [],
    "relates_to_prior": [],
    "topics": ["aggregators"],
    "article": "Body.",
    "schema_version": 2,
}
FAKE_CLASSIFICATIONS_TRUST = [
    {"url": "https://stratechery.com/2024/aggregators", "category": "business", "reason": "analysis"},
    {"url": "https://twitter.com/patrickc", "category": "ignore", "reason": "social"},
]


def _fake_summarize(*, title, publication, author, body_markdown,
                    prior_posts_context="", stance_context=""):
    if "Trust" in title:
        return FAKE_SUMMARY_TRUST
    return FAKE_SUMMARY_AGG


def _fake_classify(*, post_title, publication, links):
    if not links:
        return []
    if "Trust" in post_title:
        return FAKE_CLASSIFICATIONS_TRUST
    return []


def _run_pipeline_once(repo_root: Path, today: str = "2026-04-07"):
    """Run the full pipeline once against the fixture export in repo_root."""
    write_repo_config(repo_root)
    export_data = json.loads(EXPORT.read_text())
    records = list(parse.parse_export(export_data))

    client = MagicMock()

    new_pages: list[Path] = []
    paywalled: list[str] = []
    inbox_unsaved: list[str] = []

    # Build set of saved post URLs so we can detect referenced-but-unsaved
    saved_urls = {r.url for r in records}

    for r in records:
        if r.is_paywalled:
            paywalled.append(r.id)
            continue
        try:
            body_html = enrich.fetch_body(r, client=client, repo_root=repo_root)
        except Paywalled:
            paywalled.append(r.id)
            continue

        body_md = html_to_md(body_html)

        classified = enrich.classify_post_links(r, body_html=body_html, repo_root=repo_root)
        summary = enrich.summarize_post(r, body_markdown=body_md, repo_root=repo_root)

        p = write_pages.write_article_page(
            r, summary=summary, classified_links=classified,
            body_markdown=body_md, repo_root=repo_root,
        )
        new_pages.append(p)
        write_pages.write_summary_page(r, summary=summary, repo_root=repo_root)
        write_pages.ensure_author_page(r, repo_root=repo_root)
        write_pages.ensure_publication_page(r, repo_root=repo_root)
        write_pages.append_links_to_drop_queue(
            r, classified_links=classified, repo_root=repo_root, today=today,
        )

        for L in classified.get("substack_internal") or []:
            if L["url"] not in saved_urls:
                inbox_unsaved.append(L["url"])

    return {"new_pages": new_pages, "paywalled": paywalled, "inbox_unsaved": inbox_unsaved}


@pytest.fixture
def _mocked_gemini():
    svc = MagicMock()
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    svc.summarize_substack_post.side_effect = _fake_summarize
    svc.classify_links_batch.side_effect = _fake_classify
    with patch("scripts.substack.enrich._get_llm_service", return_value=svc):
        yield


def test_smoketest_writes_expected_pages(tmp_path, _mocked_gemini):
    result = _run_pipeline_once(tmp_path)
    wiki_root = Vault.load(tmp_path).wiki
    assert len(result["new_pages"]) == 2  # 2 accessible, 1 paywalled
    assert (wiki_root / "sources" / "substack" / "thegeneralist"
            / "2026-03-15-on-trust.md").exists()
    assert (wiki_root / "sources" / "substack" / "stratechery"
            / "2026-03-20-why-aggregators-win.md").exists()


def test_smoketest_creates_author_and_publication_stubs(tmp_path, _mocked_gemini):
    _run_pipeline_once(tmp_path)
    wiki_root = Vault.load(tmp_path).wiki
    assert (wiki_root / "people" / "mario-gabriele.md").exists()
    assert (wiki_root / "people" / "ben-thompson.md").exists()
    assert (wiki_root / "companies" / "thegeneralist.md").exists()
    assert (wiki_root / "companies" / "stratechery.md").exists()


def test_smoketest_paywalled_post_reported(tmp_path, _mocked_gemini):
    result = _run_pipeline_once(tmp_path)
    wiki_root = Vault.load(tmp_path).wiki
    assert "190000003" in result["paywalled"]
    # No wiki page should exist for the paywalled post
    paywalled_dir = wiki_root / "sources" / "substack" / "example"
    assert not paywalled_dir.exists() or not any(paywalled_dir.iterdir())


def test_smoketest_drops_non_ignored_external_links(tmp_path, _mocked_gemini):
    _run_pipeline_once(tmp_path)
    drop = tmp_path / "raw" / "drops" / "articles-from-substack-2026-04-07.jsonl"
    assert drop.exists()
    lines = [L for L in drop.read_text().splitlines() if L.strip()]
    entries = [json.loads(L) for L in lines]
    urls = {e["url"] for e in entries}
    assert "https://stratechery.com/2024/aggregators" in urls
    # ignored link must NOT be queued
    assert "https://twitter.com/patrickc" not in urls


def test_smoketest_idempotent_second_run_does_not_duplicate_anything(tmp_path, _mocked_gemini):
    """Content-hash comparison across all generated files, not just mtimes.

    mtime can collide on fast filesystems within the same second. Hashing the
    content of every article/summary/author/publication page AND the drop queue
    line count catches duplication that mtime alone would miss.
    """
    _run_pipeline_once(tmp_path)
    wiki_root = Vault.load(tmp_path).wiki

    def _snapshot() -> dict[str, str]:
        out: dict[str, str] = {}
        for p in sorted(wiki_root.rglob("*.md")):
            out[str(p.relative_to(tmp_path))] = hashlib.sha256(p.read_bytes()).hexdigest()
        drop = tmp_path / "raw" / "drops" / "articles-from-substack-2026-04-07.jsonl"
        if drop.exists():
            out["drop-queue"] = hashlib.sha256(drop.read_bytes()).hexdigest()
            out["drop-queue-line-count"] = str(
                len([L for L in drop.read_text().splitlines() if L.strip()])
            )
        return out

    snap1 = _snapshot()
    _run_pipeline_once(tmp_path)
    snap2 = _snapshot()

    assert snap1 == snap2, "Second run changed wiki pages or drop queue contents"
    # Specifically: drop queue line count must not grow on re-run
    assert snap1.get("drop-queue-line-count") == snap2.get("drop-queue-line-count")


def test_smoketest_logs_unsaved_substack_internal_links(tmp_path, _mocked_gemini):
    result = _run_pipeline_once(tmp_path)
    # The fixture links from on-trust to /p/marketplaces which is NOT in the saved list
    assert any("marketplaces" in u for u in result["inbox_unsaved"])


# ---------------------------------------------------------------------------
# HTTP-path coverage — exercise fetch_body's network branch with a fake client
# ---------------------------------------------------------------------------


def _mock_response(text: str, status: int = 200) -> MagicMock:
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status
    resp.text = text
    resp.raise_for_status = MagicMock()
    if status >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(
            str(status), response=resp
        )
    return resp


def _make_record_needs_fetch():
    """A SubstackRecord with body_html=None so fetch_body actually hits the client."""
    return parse.SubstackRecord(
        id="290000001",
        title="Needs Fetch",
        subtitle=None,
        slug="needs-fetch",
        published_at="2026-03-10T09:00:00Z",
        saved_at="2026-04-05T12:00:00Z",
        url="https://fetchme.substack.com/p/needs-fetch",
        author_name="Fetch Author",
        author_id="12345",
        publication_name="Fetchme",
        publication_slug="fetchme",
        body_html=None,
        is_paywalled=False,
    )


def test_fetch_body_network_path_extracts_body_markup(tmp_path):
    """fetch_body must call client.get, extract div.body.markup, and cache it."""
    r = _make_record_needs_fetch()
    client = MagicMock()
    client.get.return_value = _mock_response(
        "<html><body><nav>junk</nav>"
        "<div class='body markup'><h1>Real Title</h1><p>Real body.</p></div>"
        "<footer>footer junk</footer></body></html>"
    )
    html = enrich.fetch_body(r, client=client, repo_root=tmp_path)
    client.get.assert_called_once_with(r.url, timeout=30, allow_redirects=True)
    assert "<h1>Real Title</h1>" in html
    assert "<p>Real body.</p>" in html
    # junk outside the body.markup div must be excluded
    assert "nav" not in html.lower() or "junk" not in html
    assert "footer" not in html.lower() or "junk" not in html
    # Cache must exist on disk
    cached = enrich.html_cache_path(tmp_path, r.id)
    assert cached.exists()
    assert "Real body." in cached.read_text(encoding="utf-8")


def test_fetch_body_network_path_403_raises_paywalled(tmp_path):
    """A real 403 response from Substack must raise Paywalled."""
    r = _make_record_needs_fetch()
    client = MagicMock()
    client.get.return_value = _mock_response("Access Denied", status=403)
    with pytest.raises(Paywalled):
        enrich.fetch_body(r, client=client, repo_root=tmp_path)
    assert not enrich.html_cache_path(tmp_path, r.id).exists()


def test_fetch_body_network_path_missing_body_selector_raises_fetch_failed(tmp_path):
    """If the response lacks div.body.markup, fetch_body raises rather than polluting cache."""
    r = _make_record_needs_fetch()
    client = MagicMock()
    client.get.return_value = _mock_response(
        "<html><body><main><p>No body class here.</p></main></body></html>"
    )
    with pytest.raises(FetchFailed, match="body selector"):
        enrich.fetch_body(r, client=client, repo_root=tmp_path)
    assert not enrich.html_cache_path(tmp_path, r.id).exists()
