import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from mind.services.llm_cache import LLMCacheIdentity
from mind.services.llm_service import get_llm_service
from mind.services.prompt_builders import SUMMARIZE_SUBSTACK_PROMPT_VERSION
from scripts.substack import enrich
from scripts.substack.parse import SubstackRecord


_FAKE_IDENTITY = LLMCacheIdentity(
    task_class="summary",
    provider="google",
    model="gemini-test",
    transport="direct",
    api_family="genai",
    input_mode="text",
    prompt_version="test.v1",
)


def _mock_llm_service(**method_returns):
    """Build a mock LLMService with specified method return values and cache_identities stub."""
    svc = MagicMock()
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    for method, retval in method_returns.items():
        getattr(svc, method).return_value = retval
    return svc


def _patch_llm_service(svc):
    """Return a combined context manager that patches both _get_llm_service and get_llm_service."""
    return patch("scripts.substack.enrich._get_llm_service", return_value=svc)


def _make_record(**overrides) -> SubstackRecord:
    defaults = dict(
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
    defaults.update(overrides)
    return SubstackRecord(**defaults)


def test_cache_paths(tmp_path):
    r = _make_record()
    assert enrich.html_cache_path(tmp_path, r.id).name == "140000001.html"
    assert enrich.summary_cache_path(tmp_path, r.id).name == "140000001.json"
    assert enrich.links_cache_path(tmp_path, r.id).name == "140000001.links.json"
    # All three live under raw/transcripts/substack/
    assert "raw/transcripts/substack" in str(enrich.html_cache_path(tmp_path, r.id)).replace("\\", "/")


def test_fetch_body_uses_body_html_from_record_when_present(tmp_path):
    r = _make_record(body_html="<p>Inline body</p>")
    client = MagicMock()
    html = enrich.fetch_body(r, client=client, repo_root=tmp_path)
    assert html == "<p>Inline body</p>"
    client.get.assert_not_called()
    assert enrich.html_cache_path(tmp_path, r.id).exists()
    assert enrich.html_cache_path(tmp_path, r.id).read_text() == "<p>Inline body</p>"


def test_fetch_body_fetches_when_record_body_missing(tmp_path):
    r = _make_record(body_html=None)
    client = MagicMock()
    resp = MagicMock(spec=requests.Response)
    resp.status_code = 200
    resp.text = "<html><body><div class='body markup'><p>Fetched</p></div></body></html>"
    resp.raise_for_status = MagicMock()
    client.get.return_value = resp

    html = enrich.fetch_body(r, client=client, repo_root=tmp_path)
    assert "Fetched" in html
    client.get.assert_called_once()
    assert enrich.html_cache_path(tmp_path, r.id).exists()


def test_fetch_body_reads_cache_on_second_call(tmp_path):
    r = _make_record(body_html=None)
    client = MagicMock()
    resp = MagicMock(spec=requests.Response)
    resp.status_code = 200
    resp.text = "<html><body><div class='body markup'><p>Fetched</p></div></body></html>"
    resp.raise_for_status = MagicMock()
    client.get.return_value = resp

    enrich.fetch_body(r, client=client, repo_root=tmp_path)
    enrich.fetch_body(r, client=client, repo_root=tmp_path)
    assert client.get.call_count == 1


def test_fetch_body_403_raises_paywalled(tmp_path):
    r = _make_record(body_html=None)
    client = MagicMock()
    resp = MagicMock(spec=requests.Response)
    resp.status_code = 403
    resp.raise_for_status.side_effect = requests.HTTPError(
        "403", response=resp
    )
    client.get.return_value = resp
    with pytest.raises(enrich.Paywalled):
        enrich.fetch_body(r, client=client, repo_root=tmp_path)


def test_fetch_body_401_raises_paywalled(tmp_path):
    r = _make_record(body_html=None)
    client = MagicMock()
    resp = MagicMock(spec=requests.Response)
    resp.status_code = 401
    resp.raise_for_status.side_effect = requests.HTTPError(
        "401", response=resp
    )
    client.get.return_value = resp
    with pytest.raises(enrich.Paywalled):
        enrich.fetch_body(r, client=client, repo_root=tmp_path)


def test_fetch_body_500_raises_fetch_failed(tmp_path):
    r = _make_record(body_html=None)
    client = MagicMock()
    resp = MagicMock(spec=requests.Response)
    resp.status_code = 500
    resp.raise_for_status.side_effect = requests.HTTPError(
        "500", response=resp
    )
    client.get.return_value = resp
    with pytest.raises(enrich.FetchFailed):
        enrich.fetch_body(r, client=client, repo_root=tmp_path)


def test_fetch_body_network_error_raises_fetch_failed(tmp_path):
    r = _make_record(body_html=None)
    client = MagicMock()
    client.get.side_effect = requests.ConnectTimeout("boom")
    with pytest.raises(enrich.FetchFailed):
        enrich.fetch_body(r, client=client, repo_root=tmp_path)


def test_fetch_body_raises_fetch_failed_when_body_selector_not_found(tmp_path):
    """If Substack DOM changes and the body selector doesn't match, raise FetchFailed
    rather than silently caching the whole page HTML."""
    r = _make_record(body_html=None)
    client = MagicMock()
    resp = MagicMock(spec=requests.Response)
    resp.status_code = 200
    resp.text = "<html><body><main><p>No body class here.</p></main></body></html>"
    resp.raise_for_status = MagicMock()
    client.get.return_value = resp
    with pytest.raises(enrich.FetchFailed, match="body selector"):
        enrich.fetch_body(r, client=client, repo_root=tmp_path)
    # And importantly, no cache file should have been written.
    assert not enrich.html_cache_path(tmp_path, r.id).exists()


def test_classify_post_links_caches_result(tmp_path):
    r = _make_record(body_html="""
        <p>See <a href="https://stratechery.com/x">x</a> and
        <a href="https://twitter.com/y">@y</a>.</p>
    """)

    fake_classifications = [
        {"url": "https://stratechery.com/x", "category": "business", "reason": "analysis"},
        {"url": "https://twitter.com/y", "category": "ignore", "reason": "social"},
    ]

    svc = _mock_llm_service(classify_links_batch=fake_classifications)
    with _patch_llm_service(svc):
        body_html = enrich.fetch_body(r, client=MagicMock(), repo_root=tmp_path)
        result = enrich.classify_post_links(r, body_html=body_html, repo_root=tmp_path)

    assert result["external_classified"][0]["category"] == "business"
    assert result["external_classified"][1]["category"] == "ignore"
    assert enrich.links_cache_path(tmp_path, r.id).exists()
    svc.classify_links_batch.assert_called_once()


def test_classify_post_links_reads_cache_on_second_call(tmp_path):
    r = _make_record(body_html="""<p><a href="https://example.com/x">x</a></p>""")
    fake = [{"url": "https://example.com/x", "category": "personal", "reason": "ok"}]

    svc = _mock_llm_service(classify_links_batch=fake)
    with _patch_llm_service(svc):
        body_html = enrich.fetch_body(r, client=MagicMock(), repo_root=tmp_path)
        enrich.classify_post_links(r, body_html=body_html, repo_root=tmp_path)
        enrich.classify_post_links(r, body_html=body_html, repo_root=tmp_path)

    assert svc.classify_links_batch.call_count == 1


def test_classify_post_links_empty_body_returns_empty(tmp_path):
    r = _make_record(body_html="<p>No links here at all.</p>")
    svc = _mock_llm_service()
    with _patch_llm_service(svc):
        body_html = enrich.fetch_body(r, client=MagicMock(), repo_root=tmp_path)
        result = enrich.classify_post_links(r, body_html=body_html, repo_root=tmp_path)
    assert result["external_classified"] == []
    assert result["substack_internal"] == []
    svc.classify_links_batch.assert_not_called()


def test_classify_post_links_merges_classifications_onto_original_links(tmp_path):
    """Each output entry should have anchor_text/context_snippet from extractor
    AND category/reason from classifier, merged into one dict per URL."""
    r = _make_record(body_html="""
        <p>Check out <a href="https://a.com/post">this analysis</a> on AI trends.</p>
    """)
    fake = [{"url": "https://a.com/post", "category": "business", "reason": "industry"}]

    svc = _mock_llm_service(classify_links_batch=fake)
    with _patch_llm_service(svc):
        body_html = enrich.fetch_body(r, client=MagicMock(), repo_root=tmp_path)
        result = enrich.classify_post_links(r, body_html=body_html, repo_root=tmp_path)

    entry = result["external_classified"][0]
    assert entry["url"] == "https://a.com/post"
    assert entry["anchor_text"] == "this analysis"
    assert "context_snippet" in entry
    assert entry["category"] == "business"
    assert entry["reason"] == "industry"


def test_classify_post_links_preserves_substack_internal_unclassified(tmp_path):
    """substack_internal links (e.g. thegeneralist.substack.com/p/foo) are returned
    as-is without going through the classifier."""
    r = _make_record(body_html="""
        <p>See <a href="https://thegeneralist.substack.com/p/marketplaces">this</a>.</p>
    """)
    svc = _mock_llm_service()
    with _patch_llm_service(svc):
        body_html = enrich.fetch_body(r, client=MagicMock(), repo_root=tmp_path)
        result = enrich.classify_post_links(r, body_html=body_html, repo_root=tmp_path)
    assert len(result["substack_internal"]) == 1
    assert result["substack_internal"][0]["url"] == "https://thegeneralist.substack.com/p/marketplaces"
    assert result["external_classified"] == []
    # Classifier should not be called since there are no external links
    svc.classify_links_batch.assert_not_called()


FAKE_SUMMARY = {
    "tldr": "Trust matters.",
    "key_claims": ["A", "B"],
    "notable_quotes": [],
    "takeaways": ["X"],
    "topics": ["trust"],
    "article": "Body text.",
}


def test_summarize_post_caches_result(tmp_path):
    r = _make_record()
    body_md = "# On Trust\n\nTrust is the root."
    svc = _mock_llm_service(summarize_substack_post=FAKE_SUMMARY)
    with _patch_llm_service(svc):
        result = enrich.summarize_post(r, body_markdown=body_md, repo_root=tmp_path)
    assert result == FAKE_SUMMARY
    assert enrich.summary_cache_path(tmp_path, r.id).exists()
    svc.summarize_substack_post.assert_called_once()


def test_summarize_post_reads_cache_on_second_call(tmp_path):
    r = _make_record()
    body_md = "# On Trust\n\nTrust is the root."
    svc = _mock_llm_service(summarize_substack_post=FAKE_SUMMARY)
    with _patch_llm_service(svc):
        enrich.summarize_post(r, body_markdown=body_md, repo_root=tmp_path)
        enrich.summarize_post(r, body_markdown=body_md, repo_root=tmp_path)
    assert svc.summarize_substack_post.call_count == 1


def test_summarize_post_passes_record_fields_to_gemini(tmp_path):
    """Verify title/publication/author from record are forwarded to the summarizer."""
    r = _make_record(title="Custom Title", publication_name="Custom Pub", author_name="Custom Author")
    svc = _mock_llm_service(summarize_substack_post=FAKE_SUMMARY)
    with _patch_llm_service(svc):
        enrich.summarize_post(r, body_markdown="body", repo_root=tmp_path)
    kwargs = svc.summarize_substack_post.call_args.kwargs
    assert kwargs["title"] == "Custom Title"
    assert kwargs["publication"] == "Custom Pub"
    assert kwargs["author"] == "Custom Author"
    assert kwargs["body_markdown"] == "body"


# ---------------------------------------------------------------------------
# Phase 1d — schema versioning
# ---------------------------------------------------------------------------

def test_summarize_post_writes_schema_version_into_cache(tmp_path):
    r = _make_record()
    svc = _mock_llm_service(summarize_substack_post=dict(FAKE_SUMMARY))
    with _patch_llm_service(svc):
        enrich.summarize_post(r, body_markdown="body", repo_root=tmp_path)
    cached = json.loads(enrich.summary_cache_path(tmp_path, r.id).read_text())
    assert cached["data"].get("schema_version") == enrich.SUMMARY_CACHE_SCHEMA_VERSION


def test_summarize_post_treats_cache_without_schema_version_as_miss(tmp_path):
    """A cache file without schema_version (v1 format) is deleted and regenerated."""
    r = _make_record()
    cache_path = enrich.summary_cache_path(tmp_path, r.id)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    # Write a v1-style cache (no schema_version)
    cache_path.write_text(json.dumps({"tldr": "stale"}), encoding="utf-8")

    new_summary = {**FAKE_SUMMARY, "tldr": "fresh"}
    svc = _mock_llm_service(summarize_substack_post=dict(new_summary))
    with _patch_llm_service(svc):
        result = enrich.summarize_post(r, body_markdown="body", repo_root=tmp_path)

    svc.summarize_substack_post.assert_called_once()
    assert result["tldr"] == "fresh"
    # Verify schema_version is now written
    assert result.get("schema_version") == enrich.SUMMARY_CACHE_SCHEMA_VERSION


def test_summarize_post_treats_schema_version_1_cache_as_miss(tmp_path):
    """A cache file with schema_version=1 is deleted and regenerated."""
    r = _make_record()
    cache_path = enrich.summary_cache_path(tmp_path, r.id)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps({"tldr": "old", "schema_version": 1}), encoding="utf-8")

    new_summary = {**FAKE_SUMMARY, "tldr": "regenerated"}
    svc = _mock_llm_service(summarize_substack_post=dict(new_summary))
    with _patch_llm_service(svc):
        result = enrich.summarize_post(r, body_markdown="body", repo_root=tmp_path)

    svc.summarize_substack_post.assert_called_once()
    assert result["tldr"] == "regenerated"
    assert result.get("schema_version") == enrich.SUMMARY_CACHE_SCHEMA_VERSION


def test_summarize_post_hits_cache_when_schema_version_current(tmp_path):
    """A cache file with the current schema_version is used as-is (no Gemini call)."""
    r = _make_record()
    real_svc = get_llm_service()
    real_identity = real_svc.cache_identity(
        task_class="summary",
        prompt_version=SUMMARIZE_SUBSTACK_PROMPT_VERSION,
    )
    cache_path = enrich.summary_cache_path(tmp_path, r.id)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cached_data = {**FAKE_SUMMARY, "schema_version": enrich.SUMMARY_CACHE_SCHEMA_VERSION}
    cache_path.write_text(
        json.dumps(
            {
                "_llm": real_identity.to_dict(),
                "data": cached_data,
            }
        ),
        encoding="utf-8",
    )

    svc = MagicMock()
    svc.cache_identities.return_value = [real_identity]
    with _patch_llm_service(svc):
        result = enrich.summarize_post(r, body_markdown="body", repo_root=tmp_path)

    svc.summarize_substack_post.assert_not_called()
    assert result["schema_version"] == enrich.SUMMARY_CACHE_SCHEMA_VERSION


def test_summarize_post_passes_context_kwargs_to_gemini(tmp_path):
    """prior_posts_context and stance_context flow through to gemini.summarize_substack_post."""
    r = _make_record()
    prior = "## Prior posts in your wiki\n- [[foo]] \"Foo\" — something\n"
    stance = "## What this author believed last time you read them\n\nBullish on trust.\n"

    svc = _mock_llm_service(summarize_substack_post=dict(FAKE_SUMMARY))
    with _patch_llm_service(svc):
        enrich.summarize_post(
            r,
            body_markdown="body",
            repo_root=tmp_path,
            prior_posts_context=prior,
            stance_context=stance,
        )

    kwargs = svc.summarize_substack_post.call_args.kwargs
    assert kwargs["prior_posts_context"] == prior
    assert kwargs["stance_context"] == stance


# ---------------------------------------------------------------------------
# Phase 1b — get_prior_posts_context
# ---------------------------------------------------------------------------

def _write_substack_wiki_page(
    tmp_path: Path,
    *,
    pub_slug: str,
    filename: str,
    author_slug: str,
    outlet_slug: str,
    last_updated: str,
    title: str,
    tldr_body: str = "",
) -> Path:
    """Write a fake substack wiki source page into the correct directory."""
    dest = tmp_path / "wiki" / "sources" / "substack" / pub_slug / filename
    dest.parent.mkdir(parents=True, exist_ok=True)
    fm = (
        f"---\n"
        f"id: substack-test\n"
        f"type: article\n"
        f"title: {title}\n"
        f"last_updated: {last_updated}\n"
        f"author: [[{author_slug}]]\n"
        f"outlet: [[{outlet_slug}]]\n"
        f"---\n"
    )
    body = f"## TL;DR\n\n{tldr_body}\n" if tldr_body else "Some content here.\n"
    dest.write_text(fm + body, encoding="utf-8")
    return dest


def test_get_prior_posts_context_no_matches_returns_empty_string(tmp_path):
    r = _make_record(author_name="Unknown Person", publication_slug="unknown-pub")
    # Write a post from a completely different author + outlet
    _write_substack_wiki_page(
        tmp_path,
        pub_slug="thegeneralist",
        filename="2026-01-01-some-post.md",
        author_slug="mario-gabriele",
        outlet_slug="thegeneralist",
        last_updated="2026-01-01",
        title="Some Post",
        tldr_body="Some tldr.",
    )
    result = enrich.get_prior_posts_context(r, tmp_path)
    assert result == ""


def test_get_prior_posts_context_single_match_returns_correct_format(tmp_path):
    r = _make_record(author_name="Mario Gabriele", publication_slug="thegeneralist")
    _write_substack_wiki_page(
        tmp_path,
        pub_slug="thegeneralist",
        filename="2026-01-15-on-focus.md",
        author_slug="mario-gabriele",
        outlet_slug="thegeneralist",
        last_updated="2026-01-15",
        title="On Focus",
        tldr_body="Focus is rare and valuable.",
    )
    result = enrich.get_prior_posts_context(r, tmp_path)
    assert result.startswith("## Prior posts in your wiki\n")
    assert "On Focus" in result
    assert "Focus is rare and valuable." in result


def test_get_prior_posts_context_six_matches_returns_top_5_by_recency(tmp_path):
    r = _make_record(author_name="Mario Gabriele", publication_slug="thegeneralist")
    for i in range(1, 7):  # 6 posts
        _write_substack_wiki_page(
            tmp_path,
            pub_slug="thegeneralist",
            filename=f"2026-0{i}-01-post.md",
            author_slug="mario-gabriele",
            outlet_slug="thegeneralist",
            last_updated=f"2026-0{i}-01",
            title=f"Post {i}",
            tldr_body=f"Tldr for post {i}.",
        )
    result = enrich.get_prior_posts_context(r, tmp_path)
    assert result.startswith("## Prior posts in your wiki\n")
    # Top 5: posts 6, 5, 4, 3, 2 (sorted descending)
    assert "Post 6" in result
    assert "Post 5" in result
    assert "Post 4" in result
    assert "Post 3" in result
    assert "Post 2" in result
    # Post 1 (oldest) should be excluded
    assert "Post 1" not in result


def test_get_prior_posts_context_budget_guard_truncates_long_tldrs(tmp_path):
    """When the block exceeds 2000 chars at top 5, budget guard fires and truncates.

    Each entry with a 300-char title + 200-char tldr = ~530 chars per line.
    5 entries = ~2650 chars + header, well over the 2000-char budget.
    After truncating to top 3, ~1590 chars + header — fits. Guard should return 3.
    """
    r = _make_record(author_name="Mario Gabriele", publication_slug="thegeneralist")
    # 300-char title: no cap in frontmatter. 200-char tldr: capped by _extract_tldr.
    long_title = "X" * 300
    long_tldr = "A" * 200
    for i in range(1, 6):
        _write_substack_wiki_page(
            tmp_path,
            pub_slug="thegeneralist",
            filename=f"2026-0{i}-01-post.md",
            author_slug="mario-gabriele",
            outlet_slug="thegeneralist",
            last_updated=f"2026-0{i}-01",
            title=long_title,
            tldr_body=long_tldr,
        )
    result = enrich.get_prior_posts_context(r, tmp_path)
    assert "## Prior posts in your wiki" in result
    entry_count = result.count("- [[")
    # With ~530 chars per entry, 5 entries exceed 2000 chars.
    # Budget guard should reduce to top 3 (or fewer).
    assert entry_count <= 3


def test_get_prior_posts_context_matches_by_outlet_only(tmp_path):
    """A post from the same outlet but different author should still match."""
    r = _make_record(author_name="New Writer", publication_slug="thegeneralist")
    _write_substack_wiki_page(
        tmp_path,
        pub_slug="thegeneralist",
        filename="2026-02-01-older-post.md",
        author_slug="mario-gabriele",   # different author
        outlet_slug="thegeneralist",    # same outlet
        last_updated="2026-02-01",
        title="Older Post",
        tldr_body="From the same publication.",
    )
    result = enrich.get_prior_posts_context(r, tmp_path)
    assert "Older Post" in result


def test_get_prior_posts_context_mismatched_author_and_outlet_no_match(tmp_path):
    """A post with a different author slug AND different outlet slug produces no match."""
    r = _make_record(author_name="Person A", publication_slug="pub-a")
    _write_substack_wiki_page(
        tmp_path,
        pub_slug="pub-b",
        filename="2026-01-01-unrelated.md",
        author_slug="person-b",
        outlet_slug="pub-b",
        last_updated="2026-01-01",
        title="Unrelated Post",
        tldr_body="Nothing to do with person-a.",
    )
    result = enrich.get_prior_posts_context(r, tmp_path)
    assert result == ""


def test_get_prior_posts_context_no_substack_wiki_dir_returns_empty_string(tmp_path):
    """When wiki/sources/substack doesn't exist, returns empty string without error."""
    r = _make_record()
    result = enrich.get_prior_posts_context(r, tmp_path)
    assert result == ""


# ---------------------------------------------------------------------------
# Phase 1e — verify_quotes
# ---------------------------------------------------------------------------

_NOW = "2026-04-07T12:00:00Z"

_BODY = (
    "The quick brown fox jumps over the lazy dog. "
    "Trust is the foundation of all relationships. "
    "Innovation requires both creativity and discipline."
)


def _make_summary(claims: list[dict]) -> dict:
    return {
        "schema_version": 2,
        "tldr": "A post about trust.",
        "core_argument": "Trust matters.",
        "key_claims": claims,
    }


def test_verify_quotes_all_verified(tmp_path):
    """All 3 claims have verbatim quotes from the body; result unchanged, no sidecar."""
    r = _make_record()
    claims = [
        {"claim": "Fox jumps.", "evidence_quote": "The quick brown fox jumps over the lazy dog."},
        {"claim": "Trust.", "evidence_quote": "Trust is the foundation of all relationships."},
        {"claim": "Innovation.", "evidence_quote": "Innovation requires both creativity and discipline."},
    ]
    summary = _make_summary(claims)
    result = enrich.verify_quotes(summary, _BODY, r, tmp_path, _now=_NOW)

    assert result is summary
    for claim in result["key_claims"]:
        assert "quote_unverified" not in claim
    assert not enrich.quote_warnings_path(tmp_path, r.id).exists()


def test_verify_quotes_some_unverified(tmp_path):
    """2 of 3 claims verbatim, 1 paraphrased; paraphrased one marked, sidecar has 1 entry."""
    r = _make_record()
    claims = [
        {"claim": "Fox jumps.", "evidence_quote": "The quick brown fox jumps over the lazy dog."},
        {"claim": "Trust.", "evidence_quote": "Trust is the foundation of all relationships."},
        {"claim": "Innovation.", "evidence_quote": "Innovation is about bold new ideas."},  # paraphrase
    ]
    summary = _make_summary(claims)
    result = enrich.verify_quotes(summary, _BODY, r, tmp_path, _now=_NOW)

    assert "quote_unverified" not in result["key_claims"][0]
    assert "quote_unverified" not in result["key_claims"][1]
    assert result["key_claims"][2]["quote_unverified"] is True

    sidecar_path = enrich.quote_warnings_path(tmp_path, r.id)
    assert sidecar_path.exists()
    sidecar = json.loads(sidecar_path.read_text())
    assert len(sidecar["unverified_claims"]) == 1
    assert sidecar["unverified_claims"][0]["index"] == 2


def test_verify_quotes_all_unverified(tmp_path):
    """All claims paraphrased; all marked unverified, sidecar has all entries."""
    r = _make_record()
    claims = [
        {"claim": "Fox claim.", "evidence_quote": "A fox leapt swiftly."},
        {"claim": "Trust claim.", "evidence_quote": "Trust underpins everything."},
        {"claim": "Innovation claim.", "evidence_quote": "Creativity drives progress."},
    ]
    summary = _make_summary(claims)
    result = enrich.verify_quotes(summary, _BODY, r, tmp_path, _now=_NOW)

    for claim in result["key_claims"]:
        assert claim["quote_unverified"] is True

    sidecar = json.loads(enrich.quote_warnings_path(tmp_path, r.id).read_text())
    assert len(sidecar["unverified_claims"]) == 3


def test_verify_quotes_missing_evidence_quote_field(tmp_path):
    """A claim with no evidence_quote key is marked unverified."""
    r = _make_record()
    claims = [
        {"claim": "Fox jumps.", "evidence_quote": "The quick brown fox jumps over the lazy dog."},
        {"claim": "Missing quote."},  # no evidence_quote key
    ]
    summary = _make_summary(claims)
    result = enrich.verify_quotes(summary, _BODY, r, tmp_path, _now=_NOW)

    assert "quote_unverified" not in result["key_claims"][0]
    assert result["key_claims"][1]["quote_unverified"] is True
    assert enrich.quote_warnings_path(tmp_path, r.id).exists()


def test_verify_quotes_empty_evidence_quote(tmp_path):
    """A claim with evidence_quote="" is marked unverified."""
    r = _make_record()
    claims = [
        {"claim": "Verified.", "evidence_quote": "Trust is the foundation of all relationships."},
        {"claim": "Empty quote.", "evidence_quote": ""},
    ]
    summary = _make_summary(claims)
    result = enrich.verify_quotes(summary, _BODY, r, tmp_path, _now=_NOW)

    assert "quote_unverified" not in result["key_claims"][0]
    assert result["key_claims"][1]["quote_unverified"] is True
    assert enrich.quote_warnings_path(tmp_path, r.id).exists()


def test_verify_quotes_no_key_claims(tmp_path):
    """Summary with no key_claims key returns unchanged, no sidecar."""
    r = _make_record()
    summary = {"schema_version": 2, "tldr": "No claims here."}
    result = enrich.verify_quotes(summary, _BODY, r, tmp_path, _now=_NOW)

    assert result is summary
    assert not enrich.quote_warnings_path(tmp_path, r.id).exists()


def test_verify_quotes_empty_key_claims(tmp_path):
    """Summary with key_claims=[] returns unchanged, no sidecar."""
    r = _make_record()
    summary = _make_summary([])
    result = enrich.verify_quotes(summary, _BODY, r, tmp_path, _now=_NOW)

    assert result is summary
    assert not enrich.quote_warnings_path(tmp_path, r.id).exists()


def test_verify_quotes_case_insensitive_and_whitespace_normalized(tmp_path):
    """Quote with different casing and extra whitespace still matches."""
    r = _make_record()
    # Different casing + extra internal whitespace
    claims = [
        {
            "claim": "Fox claim.",
            "evidence_quote": "THE QUICK  BROWN  FOX  JUMPS   OVER THE LAZY  DOG.",
        }
    ]
    summary = _make_summary(claims)
    result = enrich.verify_quotes(summary, _BODY, r, tmp_path, _now=_NOW)

    assert "quote_unverified" not in result["key_claims"][0]
    assert not enrich.quote_warnings_path(tmp_path, r.id).exists()


def test_verify_quotes_sidecar_structure(tmp_path):
    """Sidecar has exact {post_id, verified_at, unverified_claims: [{index, claim, evidence_quote}]}."""
    r = _make_record()
    claims = [
        {"claim": "Some claim.", "evidence_quote": "This text is not in the body at all."},
    ]
    summary = _make_summary(claims)
    enrich.verify_quotes(summary, _BODY, r, tmp_path, _now=_NOW)

    sidecar_path = enrich.quote_warnings_path(tmp_path, r.id)
    assert sidecar_path.exists()
    sidecar = json.loads(sidecar_path.read_text())

    assert sidecar["post_id"] == r.id
    assert sidecar["verified_at"] == _NOW
    assert isinstance(sidecar["unverified_claims"], list)
    assert len(sidecar["unverified_claims"]) == 1

    entry = sidecar["unverified_claims"][0]
    assert entry["index"] == 0
    assert entry["claim"] == "Some claim."
    assert entry["evidence_quote"] == "This text is not in the body at all."


def test_verify_quotes_mutates_summary_and_returns(tmp_path):
    """Function returns the same dict object passed in (identity check), not a copy."""
    r = _make_record()
    claims = [
        {"claim": "Paraphrased.", "evidence_quote": "Something not in the body."},
    ]
    summary = _make_summary(claims)
    result = enrich.verify_quotes(summary, _BODY, r, tmp_path, _now=_NOW)

    assert result is summary
    # Mutation: the original dict's claim was marked unverified
    assert summary["key_claims"][0]["quote_unverified"] is True


# ---------------------------------------------------------------------------
# Phase 2 — apply_post_to_you
# ---------------------------------------------------------------------------

FAKE_APPLIED_RESPONSE = {
    "applied_paragraph": "Example Owner should consider this.",
    "applied_bullets": [
        {"claim": "Trust matters.", "why_it_matters": "Relevant to Example Health App.", "action": "Review the onboarding flow."},
    ],
    "socratic_questions": ["Are you building trust or assuming it on Example Health App?"],
    "thread_links": ["Example Health App"],
}

FAKE_APPLY_SUMMARY = {
    "tldr": "Trust matters.",
    "core_argument": "The central argument is about trust.",
    "key_claims": [
        {"claim": "Trust is rare.", "evidence_quote": "Trust is rare.", "evidence_context": "..."},
    ],
    "topics": ["trust", "culture"],
    "article": "Long body text here...",
    "entities": {"people": [], "companies": [], "tools": [], "concepts": []},
}


def _reset_profile_cache():
    """Reset the module-level profile cache between tests."""
    import scripts.common.profile as profile_mod
    profile_mod._PROFILE_CACHE = None


def _write_profile_files(wiki_me_dir: Path) -> None:
    """Write minimal wiki/me/ profile files for testing."""
    wiki_me_dir.mkdir(parents=True, exist_ok=True)
    (wiki_me_dir / "profile.md").write_text("# Profile\n\nExample Owner is a builder.", encoding="utf-8")
    (wiki_me_dir / "positioning.md").write_text("# Positioning\n\nFocused on Example Health App.", encoding="utf-8")
    (wiki_me_dir / "values.md").write_text("# Values\n\nClarity and directness.", encoding="utf-8")
    (wiki_me_dir / "open-inquiries.md").write_text("# Open Inquiries\n\nExample Health App, Brain wiki.", encoding="utf-8")


def test_apply_post_to_you_empty_profile_returns_stub_no_gemini(tmp_path, monkeypatch):
    """No wiki/me/ files → stub returned, Gemini NOT called."""
    _reset_profile_cache()

    # Point env.load() at tmp_path so profile files are looked up there
    import scripts.common.env as env_mod
    fake_cfg = MagicMock()
    fake_cfg.repo_root = tmp_path
    monkeypatch.setattr(env_mod, "load", lambda: fake_cfg)

    svc = MagicMock()
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    svc.applied_to_post.side_effect = AssertionError("applied_to_post must NOT be called when profile is empty")
    monkeypatch.setattr("scripts.substack.enrich._get_llm_service", lambda: svc)

    r = _make_record()
    result = enrich.apply_post_to_you(r, summary=FAKE_APPLY_SUMMARY, repo_root=tmp_path)

    assert result == {
        "applied_paragraph": "",
        "applied_bullets": [],
        "socratic_questions": [],
        "thread_links": [],
    }
    _reset_profile_cache()


def test_apply_post_to_you_caches_result(tmp_path, monkeypatch):
    """First call writes cache; second call reads it without calling Gemini."""
    _reset_profile_cache()

    import scripts.common.env as env_mod
    fake_cfg = MagicMock()
    fake_cfg.repo_root = tmp_path
    monkeypatch.setattr(env_mod, "load", lambda: fake_cfg)

    wiki_me_dir = tmp_path / "wiki" / "me"
    _write_profile_files(wiki_me_dir)

    svc = MagicMock()
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    svc.applied_to_post.return_value = FAKE_APPLIED_RESPONSE
    monkeypatch.setattr("scripts.substack.enrich._get_llm_service", lambda: svc)

    r = _make_record()
    result1 = enrich.apply_post_to_you(r, summary=FAKE_APPLY_SUMMARY, repo_root=tmp_path)
    result2 = enrich.apply_post_to_you(r, summary=FAKE_APPLY_SUMMARY, repo_root=tmp_path)

    assert svc.applied_to_post.call_count == 1
    assert result1 == FAKE_APPLIED_RESPONSE
    assert result2 == FAKE_APPLIED_RESPONSE
    assert enrich.applied_cache_path(tmp_path, r.id).exists()
    _reset_profile_cache()


def test_apply_post_to_you_passes_correct_args_to_gemini(tmp_path, monkeypatch):
    """Verify title, publication, author, profile_context, and summary fields reach gemini."""
    _reset_profile_cache()

    import scripts.common.env as env_mod
    fake_cfg = MagicMock()
    fake_cfg.repo_root = tmp_path
    monkeypatch.setattr(env_mod, "load", lambda: fake_cfg)

    wiki_me_dir = tmp_path / "wiki" / "me"
    _write_profile_files(wiki_me_dir)

    svc = MagicMock()
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    svc.applied_to_post.return_value = FAKE_APPLIED_RESPONSE
    monkeypatch.setattr("scripts.substack.enrich._get_llm_service", lambda: svc)

    r = _make_record(
        title="On Trust",
        publication_name="The Generalist",
        author_name="Mario Gabriele",
    )
    enrich.apply_post_to_you(r, summary=FAKE_APPLY_SUMMARY, repo_root=tmp_path)

    kwargs = svc.applied_to_post.call_args.kwargs
    assert kwargs["title"] == "On Trust"
    assert kwargs["publication"] == "The Generalist"
    assert kwargs["author"] == "Mario Gabriele"
    # Profile context should contain all four file headers
    assert "### profile.md" in kwargs["profile_context"]
    assert "### positioning.md" in kwargs["profile_context"]
    assert "### values.md" in kwargs["profile_context"]
    assert "### open-inquiries.md" in kwargs["profile_context"]
    # Summary passed through as the full dict
    assert kwargs["summary"] is FAKE_APPLY_SUMMARY
    _reset_profile_cache()


def test_apply_post_to_you_empty_profile_does_not_write_cache(tmp_path, monkeypatch):
    """Empty profile → no cache file written."""
    _reset_profile_cache()

    import scripts.common.env as env_mod
    fake_cfg = MagicMock()
    fake_cfg.repo_root = tmp_path
    monkeypatch.setattr(env_mod, "load", lambda: fake_cfg)

    svc = MagicMock()
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    svc.applied_to_post.return_value = FAKE_APPLIED_RESPONSE
    monkeypatch.setattr("scripts.substack.enrich._get_llm_service", lambda: svc)

    r = _make_record()
    enrich.apply_post_to_you(r, summary=FAKE_APPLY_SUMMARY, repo_root=tmp_path)

    assert not enrich.applied_cache_path(tmp_path, r.id).exists()
    _reset_profile_cache()


# ---------------------------------------------------------------------------
# Phase 3 — update_author_stance orchestrator
# ---------------------------------------------------------------------------

FAKE_STANCE_RESPONSE = {
    "stance_delta_md": (
        "## Core beliefs\n\n- Trust is the foundation of networks.\n\n"
        "## Open questions\n\n- How do you rebuild trust?\n\n"
        "## Recent shifts\n\n- (none)\n\n"
        "## Contradictions observed\n\n- (none)\n"
    ),
    "change_note": "Extended Core beliefs with trust foundation claim.",
}

FAKE_SUMMARY_V2 = {
    "schema_version": 2,
    "tldr": "Trust matters.",
    "core_argument": "Trust is foundational to durable networks.",
    "key_claims": [
        {"claim": "Trust is rare.", "evidence_quote": "Trust is rare.", "evidence_context": "..."},
    ],
    "topics": ["trust", "networks"],
    "article": "Long article body...",
}


def test_update_author_stance_first_ingest_creates_stance_doc(tmp_path, monkeypatch):
    """No prior stance → orchestrator calls Gemini and creates the stance doc."""
    svc = MagicMock()
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    svc.update_author_stance.return_value = FAKE_STANCE_RESPONSE
    monkeypatch.setattr("scripts.substack.enrich._get_llm_service", lambda: svc)
    r = _make_record()
    change_note = enrich.update_author_stance(r, summary=FAKE_SUMMARY_V2, repo_root=tmp_path)

    from scripts.substack.stance import stance_page_path
    stance_path = stance_page_path(tmp_path, "mario-gabriele")
    assert stance_path.exists()
    assert change_note == "Extended Core beliefs with trust foundation claim."


def test_update_author_stance_caches_result(tmp_path, monkeypatch):
    """Second call with same post → no Gemini call, returns cached change_note."""
    svc = MagicMock()
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    svc.update_author_stance.return_value = FAKE_STANCE_RESPONSE
    monkeypatch.setattr("scripts.substack.enrich._get_llm_service", lambda: svc)

    r = _make_record()
    note1 = enrich.update_author_stance(r, summary=FAKE_SUMMARY_V2, repo_root=tmp_path)
    note2 = enrich.update_author_stance(r, summary=FAKE_SUMMARY_V2, repo_root=tmp_path)

    assert svc.update_author_stance.call_count == 1
    assert note1 == note2 == "Extended Core beliefs with trust foundation claim."


def test_update_author_stance_cache_miss_calls_gemini_and_writes_both_cache_and_doc(
    tmp_path, monkeypatch
):
    """Cache miss: Gemini is called, cache file and stance doc are both written."""
    svc = MagicMock()
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    svc.update_author_stance.return_value = FAKE_STANCE_RESPONSE
    monkeypatch.setattr("scripts.substack.enrich._get_llm_service", lambda: svc)
    r = _make_record()
    enrich.update_author_stance(r, summary=FAKE_SUMMARY_V2, repo_root=tmp_path)

    from scripts.substack.stance import stance_cache_path, stance_page_path

    assert stance_cache_path(tmp_path, r.id).exists()
    assert stance_page_path(tmp_path, "mario-gabriele").exists()


def test_update_author_stance_subsequent_ingest_reads_current_stance(
    tmp_path, monkeypatch
):
    """Seed a stance doc first, then call orchestrator with a different post;
    verify Gemini received the seeded stance body as current_stance."""
    svc = MagicMock()
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    svc.update_author_stance.return_value = FAKE_STANCE_RESPONSE
    monkeypatch.setattr("scripts.substack.enrich._get_llm_service", lambda: svc)

    # Seed a stance doc manually
    from scripts.substack.stance import stance_page_path
    people_dir = tmp_path / "wiki" / "people"
    people_dir.mkdir(parents=True, exist_ok=True)
    seeded_body = "## Core beliefs\n\n- Seeded belief.\n\n## Changelog\n\n- 2026-01-01 — seeded.\n"
    seeded_doc = people_dir / "mario-gabriele.md"
    seeded_doc.write_text(
        f"---\nid: mario-gabriele\ntype: person\n---\n# Mario Gabriele\n\n{seeded_body}",
        encoding="utf-8",
    )

    r = _make_record(
        id="140000002",
        title="On Capital",
        slug="on-capital",
        published_at="2026-04-01T09:00:00Z",
        saved_at="2026-04-10T18:00:00Z",
    )
    enrich.update_author_stance(r, summary=FAKE_SUMMARY_V2, repo_root=tmp_path)

    # The mock received the seeded content as current_stance
    kwargs = svc.update_author_stance.call_args.kwargs
    assert "Seeded belief." in kwargs["current_stance"]


def test_update_author_stance_uses_bounded_snapshot_for_large_docs(tmp_path, monkeypatch):
    svc = MagicMock()
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    svc.update_author_stance.return_value = FAKE_STANCE_RESPONSE
    monkeypatch.setattr("scripts.substack.enrich._get_llm_service", lambda: svc)

    people_dir = tmp_path / "wiki" / "people"
    people_dir.mkdir(parents=True, exist_ok=True)
    large_beliefs = "\n\n".join(f"- belief {index}" for index in range(1, 40))
    (people_dir / "mario-gabriele.md").write_text(
        f"---\nid: mario-gabriele\ntype: person\n---\n# Mario Gabriele\n\n## Core beliefs\n\n{large_beliefs}\n\n## Changelog\n\n- seeded\n",
        encoding="utf-8",
    )

    enrich.update_author_stance(_make_record(), summary=FAKE_SUMMARY_V2, repo_root=tmp_path)

    kwargs = svc.update_author_stance.call_args.kwargs
    assert "belief 1" in kwargs["current_stance"]
    assert "belief 39" not in kwargs["current_stance"]
    assert "seeded" not in kwargs["current_stance"]


def test_update_author_stance_returns_change_note(tmp_path, monkeypatch):
    """Return value is the change_note from Gemini response."""
    custom_note = "Added contradiction between trust and control."
    svc = MagicMock()
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    svc.update_author_stance.return_value = {
        "stance_delta_md": FAKE_STANCE_RESPONSE["stance_delta_md"],
        "change_note": custom_note,
    }
    monkeypatch.setattr("scripts.substack.enrich._get_llm_service", lambda: svc)
    r = _make_record()
    result = enrich.update_author_stance(r, summary=FAKE_SUMMARY_V2, repo_root=tmp_path)
    assert result == custom_note


# ---------------------------------------------------------------------------
# Phase 4 — log_entities
# ---------------------------------------------------------------------------

_TODAY = "2026-04-07"

_ENTITIES_BLOCK = {
    "people": ["Sam Altman"],
    "companies": ["OpenAI"],
    "tools": ["ChatGPT"],
    "concepts": ["Alignment"],
}

_BODY_WITH_ENTITIES = (
    "Sam Altman is the CEO of OpenAI. "
    "ChatGPT is the flagship product. "
    "Alignment is the core research challenge."
)

_SUMMARY_WITH_ENTITIES = {
    "tldr": "AI matters.",
    "entities": _ENTITIES_BLOCK,
}


def _inbox_path(tmp_path: Path, today: str = _TODAY) -> Path:
    return tmp_path / "wiki" / "inbox" / f"substack-entities-{today}.md"


def test_log_entities_happy_path(tmp_path):
    """4 entities across categories, none exist as pages; all logged with correct format."""
    r = _make_record()
    logged = enrich.log_entities(
        r,
        summary=_SUMMARY_WITH_ENTITIES,
        body_markdown=_BODY_WITH_ENTITIES,
        repo_root=tmp_path,
        today=_TODAY,
    )
    assert set(logged) == {"Sam Altman", "OpenAI", "ChatGPT", "Alignment"}

    content = _inbox_path(tmp_path).read_text(encoding="utf-8")
    assert "**Sam Altman** (people)" in content
    assert "**OpenAI** (companies)" in content
    assert "**ChatGPT** (tools)" in content
    assert "**Alignment** (concepts)" in content
    # All lines reference the post slug
    from scripts.substack.write_pages import article_slug
    slug = article_slug(tmp_path, r)
    assert f"[[{slug}]]" in content


def test_log_entities_skips_existing_wiki_pages(tmp_path):
    """Pre-create wiki/people/sam-altman.md; 'Sam Altman' is skipped, others logged."""
    people_dir = tmp_path / "wiki" / "people"
    people_dir.mkdir(parents=True, exist_ok=True)
    (people_dir / "sam-altman.md").write_text("# Sam Altman\n", encoding="utf-8")

    r = _make_record()
    logged = enrich.log_entities(
        r,
        summary=_SUMMARY_WITH_ENTITIES,
        body_markdown=_BODY_WITH_ENTITIES,
        repo_root=tmp_path,
        today=_TODAY,
    )
    assert "Sam Altman" not in logged
    assert "OpenAI" in logged
    assert "ChatGPT" in logged
    assert "Alignment" in logged

    content = _inbox_path(tmp_path).read_text(encoding="utf-8")
    # Should not appear as a logged entity (the bold entity form)
    assert "**Sam Altman**" not in content


def test_log_entities_stopwords_filtered(tmp_path):
    """Entities matching stopwords or the author's own name are filtered out."""
    summary = {
        "entities": {
            "people": ["the", "Mario Gabriele", "Mario"],
            "companies": ["Substack", "OpenAI"],
            "tools": [],
            "concepts": [],
        }
    }
    r = _make_record(author_name="Mario Gabriele")
    logged = enrich.log_entities(
        r,
        summary=summary,
        body_markdown="OpenAI is interesting.",
        repo_root=tmp_path,
        today=_TODAY,
    )
    # "the" → static stopword; "Mario Gabriele" / "Mario" → author tokens
    # "Substack" → static stopword ("substack")
    assert "the" not in logged
    assert "Mario Gabriele" not in logged
    assert "Mario" not in logged
    assert "Substack" not in logged
    assert "OpenAI" in logged


def test_log_entities_filters_sales_chrome_and_writes_audit(tmp_path):
    summary = {
        "entities": {
            "people": [],
            "companies": ["Athletic Greens", "OpenAI"],
            "tools": [],
            "concepts": [],
        }
    }
    body = (
        "This episode is sponsored by Athletic Greens. Use code LEX30 for a discount. "
        "OpenAI released a new model."
    )
    r = _make_record()
    logged = enrich.log_entities(
        r,
        summary=summary,
        body_markdown=body,
        repo_root=tmp_path,
        today=_TODAY,
    )
    assert "Athletic Greens" not in logged
    assert "OpenAI" in logged

    audit_path = tmp_path / "wiki" / "inbox" / f"substack-anti-sales-audit-{_TODAY}.md"
    assert audit_path.exists()
    audit = audit_path.read_text(encoding="utf-8")
    assert "Athletic Greens" in audit
    assert "filtered sales chrome" in audit


def test_log_entities_dedup_across_categories(tmp_path):
    """'OpenAI' in both companies and concepts → logged once with category=companies."""
    summary = {
        "entities": {
            "people": [],
            "companies": ["OpenAI"],
            "tools": [],
            "concepts": ["OpenAI"],
        }
    }
    r = _make_record()
    logged = enrich.log_entities(
        r,
        summary=summary,
        body_markdown="OpenAI is an AI lab.",
        repo_root=tmp_path,
        today=_TODAY,
    )
    assert logged.count("OpenAI") == 1

    content = _inbox_path(tmp_path).read_text(encoding="utf-8")
    assert content.count("**OpenAI**") == 1
    assert "(companies)" in content
    assert "(concepts)" not in content


def test_log_entities_length_threshold(tmp_path):
    """Entity 'a' (length 1) is skipped."""
    summary = {
        "entities": {
            "people": ["a", "Bob"],
            "companies": [],
            "tools": [],
            "concepts": [],
        }
    }
    r = _make_record()
    logged = enrich.log_entities(
        r,
        summary=summary,
        body_markdown="Bob is here.",
        repo_root=tmp_path,
        today=_TODAY,
    )
    assert "a" not in logged
    assert "Bob" in logged


def test_log_entities_cap_30(tmp_path):
    """40 unique entities → only 30 logged."""
    entities = [f"Entity{i}" for i in range(40)]
    summary = {
        "entities": {
            "people": entities,
            "companies": [],
            "tools": [],
            "concepts": [],
        }
    }
    r = _make_record()
    body = " ".join(f"Entity{i} appears here." for i in range(40))
    logged = enrich.log_entities(
        r,
        summary=summary,
        body_markdown=body,
        repo_root=tmp_path,
        today=_TODAY,
    )
    assert len(logged) == 30


def test_log_entities_missing_entities_field(tmp_path):
    """summary has no 'entities' key → returns [], no inbox file created."""
    r = _make_record()
    logged = enrich.log_entities(
        r,
        summary={"tldr": "no entities here"},
        body_markdown="Some body.",
        repo_root=tmp_path,
        today=_TODAY,
    )
    assert logged == []
    assert not _inbox_path(tmp_path).exists()


def test_log_entities_empty_categories(tmp_path):
    """All categories empty → returns [], no inbox file created."""
    r = _make_record()
    logged = enrich.log_entities(
        r,
        summary={"entities": {"people": [], "companies": [], "tools": [], "concepts": []}},
        body_markdown="Some body.",
        repo_root=tmp_path,
        today=_TODAY,
    )
    assert logged == []
    assert not _inbox_path(tmp_path).exists()


def test_log_entities_context_sentence_extraction(tmp_path):
    """Entity 'OpenAI' found in body → context line contains the enclosing sentence."""
    body = "Sam Altman is the CEO of OpenAI. This is a test sentence."
    summary = {
        "entities": {
            "people": [],
            "companies": ["OpenAI"],
            "tools": [],
            "concepts": [],
        }
    }
    r = _make_record()
    enrich.log_entities(
        r,
        summary=summary,
        body_markdown=body,
        repo_root=tmp_path,
        today=_TODAY,
    )
    content = _inbox_path(tmp_path).read_text(encoding="utf-8")
    # The enclosing sentence containing "OpenAI" should appear in the context
    assert "OpenAI" in content
    assert "CEO" in content  # part of the enclosing sentence


def test_log_entities_context_sentence_not_found(tmp_path):
    """Entity not in body → context is '(no direct quote in body)'."""
    summary = {
        "entities": {
            "people": ["Satya Nadella"],
            "companies": [],
            "tools": [],
            "concepts": [],
        }
    }
    r = _make_record()
    enrich.log_entities(
        r,
        summary=summary,
        body_markdown="This body mentions nobody by that name.",
        repo_root=tmp_path,
        today=_TODAY,
    )
    content = _inbox_path(tmp_path).read_text(encoding="utf-8")
    assert "(no direct quote in body)" in content


def test_log_entities_appends_to_existing_file(tmp_path):
    """Call twice with different entities on the same day → file has header once + both sets."""
    r1 = _make_record(id="140000001", title="Post One", slug="post-one")
    r2 = _make_record(id="140000002", title="Post Two", slug="post-two")

    summary1 = {"entities": {"people": ["Alice"], "companies": [], "tools": [], "concepts": []}}
    summary2 = {"entities": {"people": ["Bob"], "companies": [], "tools": [], "concepts": []}}

    enrich.log_entities(r1, summary=summary1, body_markdown="Alice is here.", repo_root=tmp_path, today=_TODAY)
    enrich.log_entities(r2, summary=summary2, body_markdown="Bob is there.", repo_root=tmp_path, today=_TODAY)

    content = _inbox_path(tmp_path).read_text(encoding="utf-8")
    # Frontmatter fence should appear exactly twice (one open, one close) — not duplicated
    assert content.count("---\n") == 2
    assert content.count("kind: substack-entities") == 1
    # Both entities should be present
    assert "**Alice**" in content
    assert "**Bob**" in content


def test_log_entities_slugifies_post_wikilink(tmp_path):
    """The [[...]] wiki-link in each entry uses the article_slug helper output."""
    from scripts.substack.write_pages import article_slug
    r = _make_record(
        published_at="2026-03-15T09:00:00Z",
        slug="on-trust",
    )
    summary = {
        "entities": {
            "people": ["Elon Musk"],
            "companies": [],
            "tools": [],
            "concepts": [],
        }
    }
    enrich.log_entities(
        r,
        summary=summary,
        body_markdown="Elon Musk is mentioned here.",
        repo_root=tmp_path,
        today=_TODAY,
    )
    expected_slug = article_slug(tmp_path, r)
    content = _inbox_path(tmp_path).read_text(encoding="utf-8")
    assert f"[[{expected_slug}]]" in content


def test_log_entities_header_written_once(tmp_path):
    """Even with many entities, frontmatter block appears exactly once."""
    entities = [f"Person{i}" for i in range(10)]
    summary = {
        "entities": {
            "people": entities,
            "companies": [],
            "tools": [],
            "concepts": [],
        }
    }
    r = _make_record()
    body = " ".join(f"Person{i} is here." for i in range(10))
    enrich.log_entities(
        r,
        summary=summary,
        body_markdown=body,
        repo_root=tmp_path,
        today=_TODAY,
    )
    content = _inbox_path(tmp_path).read_text(encoding="utf-8")
    assert content.count("---\n") == 2
    assert content.count("kind: substack-entities") == 1


def test_audit_sales_chrome_links_writes_operator_visible_audit(tmp_path):
    r = _make_record()
    audited = enrich._audit_sales_chrome_links(
        record=r,
        classified_links=[
            {
                "url": "https://nordvpn.com/lex30",
                "anchor_text": "NordVPN",
                "context_snippet": "This episode is sponsored by NordVPN, use code LEX30.",
                "category": "ignore",
                "reason": "sponsor read",
            },
            {
                "url": "https://example.com/essay",
                "anchor_text": "essay",
                "context_snippet": "A substantive essay citation.",
                "category": "business",
                "reason": "analysis",
            },
        ],
        repo_root=tmp_path,
        today=_TODAY,
    )
    assert audited == ["https://nordvpn.com/lex30"]

    audit_path = tmp_path / "wiki" / "inbox" / f"substack-anti-sales-audit-{_TODAY}.md"
    assert audit_path.exists()
    audit = audit_path.read_text(encoding="utf-8")
    assert "NordVPN" in audit
    assert "sponsor read" in audit
    assert "https://example.com/essay" not in audit
