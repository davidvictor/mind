import inspect
from pathlib import Path

from scripts.substack import write_pages
from scripts.substack.parse import SubstackRecord


def _make_record(**overrides) -> SubstackRecord:
    defaults = dict(
        id="140000001",
        title="On Trust",
        subtitle="Why it matters",
        slug="on-trust",
        published_at="2026-03-15T09:00:00Z",
        saved_at="2026-04-02T18:00:00Z",
        url="https://thegeneralist.substack.com/p/on-trust",
        author_name="Mario Gabriele",
        author_id="9001",
        publication_name="The Generalist",
        publication_slug="thegeneralist",
        body_html="<p>body</p>",
        is_paywalled=False,
    )
    defaults.update(overrides)
    return SubstackRecord(**defaults)


def test_slugify_basic():
    assert write_pages.slugify("On Trust") == "on-trust"
    assert write_pages.slugify("Why We're All Wrong!") == "why-we-re-all-wrong"
    assert write_pages.slugify("Multiple   spaces") == "multiple-spaces"


def test_slugify_empty_returns_untitled():
    assert write_pages.slugify("") == "untitled"
    assert write_pages.slugify("!!!") == "untitled"


def test_slugify_truncates_long_text():
    long_title = "A" * 200
    assert len(write_pages.slugify(long_title)) <= 60


def test_article_page_path(tmp_path):
    r = _make_record()
    p = write_pages.article_page_path(tmp_path, r)
    assert p == tmp_path / "wiki" / "sources" / "substack" / "thegeneralist" / "2026-03-15-on-trust.md"


def test_summary_page_path(tmp_path):
    r = _make_record()
    p = write_pages.summary_page_path(tmp_path, r)
    assert p == tmp_path / "wiki" / "summaries" / "summary-2026-03-15-on-trust.md"


def test_author_page_path(tmp_path):
    r = _make_record()
    p = write_pages.author_page_path(tmp_path, r)
    assert p == tmp_path / "wiki" / "people" / "mario-gabriele.md"


def test_publication_page_path(tmp_path):
    r = _make_record()
    p = write_pages.publication_page_path(tmp_path, r)
    assert p == tmp_path / "wiki" / "companies" / "thegeneralist.md"


def test_article_page_path_uses_record_slug_if_present(tmp_path):
    r = _make_record(slug="custom-slug")
    p = write_pages.article_page_path(tmp_path, r)
    assert "2026-03-15-custom-slug.md" in str(p)


def test_article_page_path_falls_back_to_slugified_title_if_slug_empty(tmp_path):
    r = _make_record(slug="", title="Fallback Title")
    p = write_pages.article_page_path(tmp_path, r)
    assert "2026-03-15-fallback-title.md" in str(p)


def test_write_article_page_creates_file_with_frontmatter(tmp_path):
    r = _make_record()
    summary = {
        "tldr": "Trust matters.",
        "key_claims": ["A", "B"],
        "notable_quotes": ["Quote 1"],
        "takeaways": ["X"],
        "topics": ["trust", "systems"],
        "article": "The body of the article.",
    }
    classified_links = {
        "external_classified": [
            {"url": "https://stratechery.com/x", "anchor_text": "x", "context_snippet": "see x",
             "category": "business", "reason": "analysis"},
            {"url": "https://twitter.com/y", "anchor_text": "@y", "context_snippet": "h/t y",
             "category": "ignore", "reason": "social"},
        ],
        "substack_internal": [],
    }
    body_markdown = "# On Trust\n\nTrust is the root of everything.\n"

    path = write_pages.write_article_page(
        r, summary=summary, classified_links=classified_links,
        body_markdown=body_markdown, repo_root=tmp_path,
    )

    assert path.exists()
    content = path.read_text()
    assert content.startswith("---\n")
    assert "type: article" in content
    assert "title: On Trust" in content
    assert "# On Trust" in content  # body heading
    assert "Trust matters." in content  # tldr
    assert "## Referenced Links" in content
    assert "### Business" in content
    assert "https://stratechery.com/x" in content
    assert "  - trust" in content
    assert "  - systems" in content
    assert "  - substack" not in content
    assert "  - compounding" not in content
    assert "  - thegeneralist" not in content
    # Ignored links should be hidden from the page
    assert "twitter.com/y" not in content


def test_write_article_page_idempotent_skips_existing(tmp_path):
    r = _make_record()
    summary = {"tldr": "x", "key_claims": [], "notable_quotes": [], "takeaways": [],
               "topics": [], "article": ""}
    classified_links = {"external_classified": [], "substack_internal": []}

    path1 = write_pages.write_article_page(r, summary=summary, classified_links=classified_links,
                                            body_markdown="", repo_root=tmp_path)
    original = path1.read_text()
    # Overwrite summary to prove it's skipped
    summary["tldr"] = "DIFFERENT"
    path2 = write_pages.write_article_page(r, summary=summary, classified_links=classified_links,
                                            body_markdown="", repo_root=tmp_path)
    assert path1 == path2
    assert path2.read_text() == original


def test_write_article_page_omits_referenced_links_section_when_no_links(tmp_path):
    r = _make_record(id="140000002", slug="no-links")
    summary = {"tldr": "x", "key_claims": [], "notable_quotes": [], "takeaways": [],
               "topics": [], "article": ""}
    classified_links = {"external_classified": [], "substack_internal": []}
    path = write_pages.write_article_page(r, summary=summary, classified_links=classified_links,
                                           body_markdown="", repo_root=tmp_path)
    content = path.read_text()
    assert "## Referenced Links" not in content


def test_write_article_page_renders_personal_and_substack_link_sections(tmp_path):
    r = _make_record(id="140000003", slug="with-links")
    summary = {"tldr": "x", "key_claims": [], "notable_quotes": [], "takeaways": [],
               "topics": [], "article": ""}
    classified_links = {
        "external_classified": [
            {"url": "https://hobby.com", "anchor_text": "hobby", "context_snippet": "",
             "category": "personal", "reason": ""},
        ],
        "substack_internal": [
            {"url": "https://other.substack.com/p/foo", "anchor_text": "foo",
             "context_snippet": ""},
        ],
    }
    path = write_pages.write_article_page(r, summary=summary, classified_links=classified_links,
                                           body_markdown="", repo_root=tmp_path)
    content = path.read_text()
    assert "### Personal" in content
    assert "https://hobby.com" in content
    assert "### Substack (internal)" in content
    assert "https://other.substack.com/p/foo" in content


def test_write_summary_page_creates_file(tmp_path):
    r = _make_record()
    summary = {
        "tldr": "Trust matters.",
        "key_claims": [
            {"claim": "Claim A", "evidence_quote": "", "evidence_context": "", "quote_unverified": False},
            {"claim": "Claim B", "evidence_quote": "", "evidence_context": "", "quote_unverified": False},
        ],
        "notable_quotes": ["Quote"],
        "topics": ["trust"],
        "article": "Body.",
        "schema_version": 2,
    }
    path = write_pages.write_summary_page(r, summary=summary, repo_root=tmp_path)
    assert path.exists()
    content = path.read_text()
    assert "type: article" in content
    assert "source_type: substack" in content
    assert "source_date:" in content
    assert "Trust matters." in content
    assert "**Claim A**" in content  # key claims rendered as bold in new format
    assert "summary-substack-140000001" in content
    assert "  - substack" not in content


def test_write_summary_page_idempotent(tmp_path):
    r = _make_record()
    summary = {"tldr": "x", "key_claims": [], "notable_quotes": [],
               "takeaways": [], "topics": [], "article": ""}
    p1 = write_pages.write_summary_page(r, summary=summary, repo_root=tmp_path)
    original = p1.read_text()
    summary["tldr"] = "DIFFERENT"
    p2 = write_pages.write_summary_page(r, summary=summary, repo_root=tmp_path)
    assert p2.read_text() == original


def test_write_summary_page_omits_empty_sections(tmp_path):
    r = _make_record(id="140000011", slug="minimal")
    summary = {"tldr": "Just tldr.", "key_claims": [], "notable_quotes": [],
               "topics": [], "article": ""}
    path = write_pages.write_summary_page(r, summary=summary, repo_root=tmp_path)
    content = path.read_text()
    assert "## Key Claims (with receipts)" not in content
    assert "## Notable Quotes" not in content
    assert "## Takeaways" not in content  # removed from v2 schema — should never appear
    assert "## TL;DR" in content


def test_write_summary_page_notable_quotes_use_blockquote_format(tmp_path):
    r = _make_record(id="140000012", slug="quotes")
    summary = {"tldr": "x", "key_claims": [], "notable_quotes": ["Trust is everything."],
               "takeaways": [], "topics": [], "article": ""}
    path = write_pages.write_summary_page(r, summary=summary, repo_root=tmp_path)
    content = path.read_text()
    assert "> Trust is everything." in content


def test_ensure_author_page_creates_stub_if_missing(tmp_path):
    r = _make_record()
    path = write_pages.ensure_author_page(r, repo_root=tmp_path)
    assert path.exists()
    content = path.read_text()
    assert "type: person" in content
    assert "name: Mario Gabriele" in content
    assert "substack_author_id:" in content
    assert "Substack author at [[thegeneralist|The Generalist]]." in content
    assert "## Core beliefs" in content
    assert "## Changelog" in content


def test_ensure_author_page_preserves_existing(tmp_path):
    r = _make_record()
    path = write_pages.author_page_path(tmp_path, r)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("---\ntype: person\n---\n\n# Custom content\n")
    result = write_pages.ensure_author_page(r, repo_root=tmp_path)
    assert result == path
    assert "Custom content" in path.read_text()
    assert "Stub created by" not in path.read_text()


def test_ensure_publication_page_creates_stub_if_missing(tmp_path):
    r = _make_record()
    path = write_pages.ensure_publication_page(r, repo_root=tmp_path)
    assert path.exists()
    content = path.read_text()
    assert "type: company" in content
    assert "name: The Generalist" in content
    assert "substack_publication_slug: thegeneralist" in content


def test_ensure_publication_page_preserves_existing(tmp_path):
    r = _make_record()
    path = write_pages.publication_page_path(tmp_path, r)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("---\ntype: company\n---\n\n# Custom\n")
    write_pages.ensure_publication_page(r, repo_root=tmp_path)
    assert "Custom" in path.read_text()


def test_write_summary_page_emits_required_fields(tmp_path):
    """Summary pages must carry source_type, source_date, ingested for /lint."""
    r = _make_record()
    summary = {"tldr": "x", "key_claims": [], "notable_quotes": [],
               "takeaways": [], "topics": [], "article": ""}
    path = write_pages.write_summary_page(r, summary=summary, repo_root=tmp_path)
    content = path.read_text()
    assert "source_type:" in content
    assert "source_date:" in content
    assert "ingested:" in content
    assert "source_type: substack" in content


def test_ensure_author_page_emits_title_and_sources(tmp_path):
    """Person stubs must carry title and sources for /lint."""
    r = _make_record()
    path = write_pages.ensure_author_page(r, repo_root=tmp_path)
    content = path.read_text()
    assert "title:" in content
    assert "sources:" in content


def test_ensure_publication_page_emits_title_and_sources(tmp_path):
    """Company stubs must carry title and sources for /lint."""
    r = _make_record()
    path = write_pages.ensure_publication_page(r, repo_root=tmp_path)
    content = path.read_text()
    assert "title:" in content
    assert "sources:" in content


import json as _json


def test_append_links_to_drop_queue_writes_jsonl(tmp_path):
    r = _make_record()
    classified = {
        "external_classified": [
            {"url": "https://a.com/x", "anchor_text": "a", "context_snippet": "ctx a",
             "category": "business", "reason": "r"},
            {"url": "https://b.com/y", "anchor_text": "b", "context_snippet": "ctx b",
             "category": "personal", "reason": "r"},
            {"url": "https://c.com/z", "anchor_text": "c", "context_snippet": "ctx c",
             "category": "ignore", "reason": "r"},
        ],
        "substack_internal": [],
    }
    path = write_pages.append_links_to_drop_queue(
        r, classified_links=classified, repo_root=tmp_path, today="2026-04-07"
    )
    assert path.name == "articles-from-substack-2026-04-07.jsonl"
    lines = [L for L in path.read_text().splitlines() if L.strip()]
    assert len(lines) == 2  # ignore excluded
    entries = [_json.loads(L) for L in lines]
    urls = {e["url"] for e in entries}
    assert urls == {"https://a.com/x", "https://b.com/y"}
    for e in entries:
        assert e["source_post_id"] == "140000001"
        assert e["source_page_id"] == "2026-03-15-on-trust"
        assert e["category"] in {"business", "personal"}
        assert "discovered_at" in e


def test_append_links_to_drop_queue_appends_not_overwrites(tmp_path):
    r = _make_record()
    classified_a = {
        "external_classified": [
            {"url": "https://a.com/x", "anchor_text": "a", "context_snippet": "",
             "category": "business", "reason": ""},
        ],
        "substack_internal": [],
    }
    classified_b = {
        "external_classified": [
            {"url": "https://b.com/y", "anchor_text": "b", "context_snippet": "",
             "category": "personal", "reason": ""},
        ],
        "substack_internal": [],
    }
    write_pages.append_links_to_drop_queue(r, classified_links=classified_a,
                                            repo_root=tmp_path, today="2026-04-07")
    write_pages.append_links_to_drop_queue(
        _make_record(id="140000002"),
        classified_links=classified_b, repo_root=tmp_path, today="2026-04-07",
    )
    path = tmp_path / "raw" / "drops" / "articles-from-substack-2026-04-07.jsonl"
    lines = [L for L in path.read_text().splitlines() if L.strip()]
    assert len(lines) == 2


def test_append_links_to_drop_queue_no_links_creates_empty_file(tmp_path):
    r = _make_record()
    classified = {"external_classified": [], "substack_internal": []}
    path = write_pages.append_links_to_drop_queue(
        r, classified_links=classified, repo_root=tmp_path, today="2026-04-07"
    )
    assert path.exists()
    assert path.read_text() == ""


def test_append_links_to_drop_queue_all_ignored_produces_empty_file(tmp_path):
    r = _make_record()
    classified = {
        "external_classified": [
            {"url": "https://a.com/x", "anchor_text": "a", "context_snippet": "",
             "category": "ignore", "reason": "spam"},
        ],
        "substack_internal": [],
    }
    path = write_pages.append_links_to_drop_queue(
        r, classified_links=classified, repo_root=tmp_path, today="2026-04-07"
    )
    assert path.exists()
    assert path.read_text() == ""


def test_add_materialized_link_to_source_page_creates_deduped_sections(tmp_path):
    record = _make_record()
    page = write_pages.write_article_page(
        record,
        summary={"tldr": "x", "key_claims": [], "notable_quotes": [], "takeaways": [], "topics": [], "article": ""},
        classified_links={"external_classified": [], "substack_internal": []},
        body_markdown="",
        repo_root=tmp_path,
    )
    assert page.exists()

    changed = write_pages.add_materialized_link_to_source_page(
        repo_root=tmp_path,
        source_page_id="2026-03-15-on-trust",
        target_page_id="2026-04-02-dol-framework",
        target_kind="article",
    )
    assert changed is True
    changed_again = write_pages.add_materialized_link_to_source_page(
        repo_root=tmp_path,
        source_page_id="2026-03-15-on-trust",
        target_page_id="2026-04-02-dol-framework",
        target_kind="article",
    )
    assert changed_again is False
    write_pages.add_materialized_link_to_source_page(
        repo_root=tmp_path,
        source_page_id="2026-03-15-on-trust",
        target_page_id="2026-02-13-what-is-critical-ai-literacy",
        target_kind="substack",
    )

    content = page.read_text(encoding="utf-8")
    assert "## Materialized Linked Pages" in content
    assert "### Articles" in content
    assert "- [[2026-04-02-dol-framework]]" in content
    assert "### Substack Posts" in content
    assert "- [[2026-02-13-what-is-critical-ai-literacy]]" in content


# ---------------------------------------------------------------------------
# Phase 5 — Deep enrichment section tests (tests 1–17 per spec)
# ---------------------------------------------------------------------------


def _full_v2_summary() -> dict:
    """A summary dict with every v2 field populated."""
    return {
        "tldr": "Trust is the foundation of durable relationships.",
        "core_argument": "Without trust there is no coordination and no compounding.",
        "argument_graph": {
            "premises": ["Coordination requires predictability.", "Trust enables predictability."],
            "inferences": ["Therefore trust is a prerequisite for coordination."],
            "conclusion": "Invest in trust early.",
        },
        "key_claims": [
            {
                "claim": "Trust compounds over time",
                "evidence_quote": "trust compounds just like interest",
                "evidence_context": "Said in reference to long-term partnerships.",
                "quote_unverified": False,
            },
        ],
        "memorable_examples": [
            {
                "title": "The Airline Example",
                "story": "Airlines lose luggage but loyal customers return anyway.",
                "lesson": "Trust persists through individual failures.",
            }
        ],
        "notable_quotes": ["Trust is everything."],
        "steelman": "A world without trust would require adversarial contracts for every interaction.",
        "strongest_rebuttal": "Trust can be exploited by bad actors.",
        "would_change_mind_if": "Evidence that low-trust societies outperform high-trust ones.",
        "in_conversation_with": ["Francis Fukuyama (Trust, 1995)", "Robert Putnam"],
        "relates_to_prior": [
            {
                "post_id": "99001",
                "post_title": "On Coordination",
                "relation": "extends",
                "note": "This post deepens the coordination argument.",
            }
        ],
        "topics": ["trust", "systems"],
        "article": "Body text.",
        "schema_version": 2,
    }


def _full_applied() -> dict:
    return {
        "applied_paragraph": "You can apply these trust principles daily.",
        "applied_bullets": [
            {
                "claim": "Show up consistently",
                "why_it_matters": "Predictability builds trust",
                "action": "Block 30 min daily for follow-ups",
            }
        ],
        "socratic_questions": [
            "Where in your life is trust the bottleneck?",
            "What would you do differently if trust were abundant?",
        ],
        "thread_links": [],
    }


def _empty_classified() -> dict:
    return {"external_classified": [], "substack_internal": []}


# Test 1
def test_write_article_page_renders_all_14_sections(tmp_path):
    r = _make_record(id="200000001", slug="all-sections")
    summary = _full_v2_summary()
    applied = _full_applied()
    path = write_pages.write_article_page(
        r,
        summary=summary,
        classified_links=_empty_classified(),
        body_markdown="## Full body content\n\nSome text.",
        repo_root=tmp_path,
        applied=applied,
        stance_change_note="Author now emphasizes systemic trust over individual trust.",
    )
    content = path.read_text()

    # Section headings in order
    headings = [
        "## TL;DR",
        "## Core Argument",
        "## Argument Structure",
        "## Key Claims (with receipts)",
        "## Memorable Examples",
        "## Notable Quotes",
        "## The Strongest Fight",
        "## In Conversation With",
        "## Applied to You",
        "## Questions This Raises for You",
        "## Author Stance Update",
        "## Full Body",
    ]
    for h in headings:
        assert h in content, f"Missing heading: {h}"

    # Verify canonical ordering by position
    positions = [content.index(h) for h in headings]
    assert positions == sorted(positions), "Headings not in canonical order"

    # Spot-check content
    assert "Trust is the foundation" in content  # tldr
    assert "Without trust there is no coordination" in content  # core_argument
    assert "**Premises:**" in content
    assert "trust compounds just like interest" in content  # evidence_quote
    assert "The Airline Example" in content
    assert "> Trust is everything." in content
    assert "**Steelman:**" in content
    assert "Francis Fukuyama" in content
    assert "You can apply these trust principles daily." in content
    assert "Where in your life is trust the bottleneck?" in content
    assert "Author now emphasizes systemic trust" in content
    assert "[[mario-gabriele]]" in content
    assert "## Full Body" in content


# Test 2
def test_write_article_page_skips_empty_sections(tmp_path):
    r = _make_record(id="200000002", slug="minimal-v2")
    summary = {"tldr": "Just a tldr.", "topics": [], "schema_version": 2}
    path = write_pages.write_article_page(
        r,
        summary=summary,
        classified_links=_empty_classified(),
        body_markdown="",
        repo_root=tmp_path,
    )
    content = path.read_text()
    assert "## TL;DR" in content
    absent = [
        "## Core Argument",
        "## Argument Structure",
        "## Key Claims",
        "## Memorable Examples",
        "## Notable Quotes",
        "## The Strongest Fight",
        "## In Conversation With",
        "## Applied to You",
        "## Questions This Raises for You",
        "## Author Stance Update",
        "## Full Body",
        "## Referenced Links",
    ]
    for h in absent:
        assert h not in content, f"Should be absent: {h}"


# Test 3
def test_write_article_page_unverified_claim_gets_warning_prefix(tmp_path):
    r = _make_record(id="200000003", slug="unverified-claim")
    summary = {
        "tldr": "x",
        "key_claims": [
            {
                "claim": "The sky is green",
                "evidence_quote": "sky is famously green",
                "evidence_context": "",
                "quote_unverified": True,
            }
        ],
        "topics": [],
    }
    path = write_pages.write_article_page(
        r,
        summary=summary,
        classified_links=_empty_classified(),
        body_markdown="",
        repo_root=tmp_path,
    )
    content = path.read_text()
    assert "⚠️ **The sky is green** (quote unverified)" in content


# Test 4
def test_write_article_page_argument_graph_partial_premises_only(tmp_path):
    r = _make_record(id="200000004", slug="premises-only")
    summary = {
        "tldr": "x",
        "argument_graph": {
            "premises": ["P1", "P2"],
            "inferences": [],
            "conclusion": "",
        },
        "topics": [],
    }
    path = write_pages.write_article_page(
        r,
        summary=summary,
        classified_links=_empty_classified(),
        body_markdown="",
        repo_root=tmp_path,
    )
    content = path.read_text()
    assert "## Argument Structure" in content
    assert "**Premises:**" in content
    assert "- P1" in content
    assert "**Inferences:**" not in content
    assert "**Conclusion:**" not in content


# Test 5
def test_write_article_page_applied_empty_paragraph_renders_bullets_only(tmp_path):
    r = _make_record(id="200000005", slug="bullets-only")
    summary = {"tldr": "x", "topics": []}
    applied = {
        "applied_paragraph": "",
        "applied_bullets": [
            {"claim": "Do X", "why_it_matters": "because Y", "action": "act Z"},
        ],
        "socratic_questions": [],
    }
    path = write_pages.write_article_page(
        r,
        summary=summary,
        classified_links=_empty_classified(),
        body_markdown="",
        repo_root=tmp_path,
        applied=applied,
    )
    content = path.read_text()
    assert "## Applied to You" in content
    assert "**Do X**" in content
    # No blank paragraph above bullets — paragraph text absent
    assert "because Y" in content
    # Ensure no leading blank line between heading and first bullet
    idx_heading = content.index("## Applied to You")
    section_fragment = content[idx_heading:idx_heading + 200]
    # No double-blank-line followed by just a blank line before bullet
    assert "\n\n\n" not in section_fragment


# Test 6
def test_write_article_page_empty_applied_skips_section(tmp_path):
    r = _make_record(id="200000006", slug="no-applied")
    summary = {"tldr": "x", "topics": []}
    # applied=None (default)
    path = write_pages.write_article_page(
        r,
        summary=summary,
        classified_links=_empty_classified(),
        body_markdown="",
        repo_root=tmp_path,
    )
    content = path.read_text()
    assert "## Applied to You" not in content


# Test 7
def test_write_article_page_empty_applied_skips_socratic_section(tmp_path):
    r = _make_record(id="200000007", slug="no-socratic")
    summary = {"tldr": "x", "topics": []}
    path = write_pages.write_article_page(
        r,
        summary=summary,
        classified_links=_empty_classified(),
        body_markdown="",
        repo_root=tmp_path,
    )
    content = path.read_text()
    assert "## Questions This Raises for You" not in content


# Test 8
def test_write_article_page_stance_change_note_none_skips_section(tmp_path):
    r = _make_record(id="200000008", slug="no-stance")
    summary = {"tldr": "x", "topics": []}
    path = write_pages.write_article_page(
        r,
        summary=summary,
        classified_links=_empty_classified(),
        body_markdown="",
        repo_root=tmp_path,
        stance_change_note=None,
    )
    content = path.read_text()
    assert "## Author Stance Update" not in content


# Test 9
def test_write_article_page_stance_change_note_renders_with_wiki_link(tmp_path):
    r = _make_record(id="200000009", slug="with-stance")
    summary = {"tldr": "x", "topics": []}
    path = write_pages.write_article_page(
        r,
        summary=summary,
        classified_links=_empty_classified(),
        body_markdown="",
        repo_root=tmp_path,
        stance_change_note="Author shifted toward empiricism.",
    )
    content = path.read_text()
    assert "## Author Stance Update" in content
    assert "Author shifted toward empiricism." in content
    assert "[[mario-gabriele]]" in content


# Test 10
def test_write_article_page_in_conversation_with_no_relates_to_prior(tmp_path):
    r = _make_record(id="200000010", slug="conv-no-prior")
    summary = {
        "tldr": "x",
        "in_conversation_with": ["Adam Smith", "Milton Friedman"],
        "relates_to_prior": [],
        "topics": [],
    }
    path = write_pages.write_article_page(
        r,
        summary=summary,
        classified_links=_empty_classified(),
        body_markdown="",
        repo_root=tmp_path,
    )
    content = path.read_text()
    assert "## In Conversation With" in content
    assert "- Adam Smith" in content
    assert "- Milton Friedman" in content
    assert "Prior posts in your wiki" not in content


# Test 11
def test_write_article_page_in_conversation_with_both_sources(tmp_path):
    r = _make_record(id="200000011", slug="conv-both")
    summary = {
        "tldr": "x",
        "in_conversation_with": ["Jane Jacobs"],
        "relates_to_prior": [
            {
                "post_id": "99002",
                "post_title": "On Cities",
                "relation": "contrasts",
                "note": "Different take on density.",
            }
        ],
        "topics": [],
    }
    path = write_pages.write_article_page(
        r,
        summary=summary,
        classified_links=_empty_classified(),
        body_markdown="",
        repo_root=tmp_path,
    )
    content = path.read_text()
    assert "## In Conversation With" in content
    assert "- Jane Jacobs" in content
    assert "**Prior posts in your wiki:**" in content
    assert "on-cities" in content
    assert "(contrasts)" in content
    assert "Different take on density." in content


# Test 12
def test_write_article_page_referenced_links_section_position_12_of_14(tmp_path):
    r = _make_record(id="200000012", slug="section-order")
    summary = _full_v2_summary()
    applied = _full_applied()
    classified = {
        "external_classified": [
            {"url": "https://example.com", "anchor_text": "ex", "context_snippet": "",
             "category": "business", "reason": ""},
        ],
        "substack_internal": [],
    }
    path = write_pages.write_article_page(
        r,
        summary=summary,
        classified_links=classified,
        body_markdown="Some body.",
        repo_root=tmp_path,
        applied=applied,
        stance_change_note="Small shift.",
    )
    content = path.read_text()
    pos_questions = content.index("## Questions This Raises for You")
    pos_refs = content.index("## Referenced Links")
    pos_stance = content.index("## Author Stance Update")
    pos_full_body = content.index("## Full Body")
    assert pos_questions < pos_refs < pos_stance < pos_full_body


# Test 13
def test_write_article_page_render_referenced_links_signature_unchanged(tmp_path):
    sig = inspect.signature(write_pages._render_referenced_links)
    params = list(sig.parameters.keys())
    # First positional parameter must be classified_links — no other required params
    assert params[0] == "classified_links"
    # Must NOT have link_promotion_map as a required param (it's reserved for Plan B)
    for name, param in sig.parameters.items():
        if name != "classified_links":
            assert param.default is not inspect.Parameter.empty, (
                f"Parameter '{name}' has no default — breaks Plan B compatibility"
            )


# Test 14
def test_write_summary_page_renders_deep_sections_without_full_body(tmp_path):
    r = _make_record(id="200000014", slug="summary-deep")
    summary = _full_v2_summary()
    applied = _full_applied()
    path = write_pages.write_summary_page(
        r,
        summary=summary,
        repo_root=tmp_path,
        applied=applied,
        stance_change_note="Big shift.",
    )
    content = path.read_text()

    present = [
        "## Core Argument",
        "## Argument Structure",
        "## Key Claims (with receipts)",
        "## Applied to You",
        "## Questions This Raises for You",
        "## Author Stance Update",
    ]
    for h in present:
        assert h in content, f"Missing on summary page: {h}"

    absent = ["## Full Body", "## Referenced Links"]
    for h in absent:
        assert h not in content, f"Should be absent on summary page: {h}"


# Test 15
def test_write_summary_page_topics_section_still_rendered(tmp_path):
    r = _make_record(id="200000015", slug="summary-topics")
    summary = {
        "tldr": "x",
        "topics": ["trust", "systems", "coordination"],
        "schema_version": 2,
    }
    path = write_pages.write_summary_page(r, summary=summary, repo_root=tmp_path)
    content = path.read_text()
    assert "tags:" in content
    assert "  - trust" in content
    assert "  - systems" in content
    assert "  - coordination" in content


# Test 16
def test_write_article_page_backward_compat_applied_none_and_stance_none_default(tmp_path):
    r = _make_record(id="200000016", slug="backward-compat")
    summary = {
        "tldr": "Backward compat check.",
        "key_claims": [],
        "notable_quotes": [],
        "topics": [],
    }
    classified_links = {"external_classified": [], "substack_internal": []}
    # Call without applied or stance_change_note kwargs — must not raise
    path = write_pages.write_article_page(
        r,
        summary=summary,
        classified_links=classified_links,
        body_markdown="",
        repo_root=tmp_path,
    )
    assert path.exists()
    content = path.read_text()
    assert "type: article" in content
    assert "Backward compat check." in content


# Test 17
def test_write_article_page_memorable_examples_renders_title_story_lesson(tmp_path):
    r = _make_record(id="200000017", slug="examples-format")
    summary = {
        "tldr": "x",
        "memorable_examples": [
            {
                "title": "The Rocket Ship",
                "story": "SpaceX landed a booster in 2015.",
                "lesson": "Audacious goals inspire audacious execution.",
            }
        ],
        "topics": [],
    }
    path = write_pages.write_article_page(
        r,
        summary=summary,
        classified_links=_empty_classified(),
        body_markdown="",
        repo_root=tmp_path,
    )
    content = path.read_text()
    assert "## Memorable Examples" in content
    assert "### The Rocket Ship" in content
    assert "SpaceX landed a booster in 2015." in content
    assert "**Lesson:** Audacious goals inspire audacious execution." in content


def test_write_article_page_uses_filename_slug_as_id(tmp_path):
    """After Plan 02, article page id matches the filename slug, and the
    legacy substack-XXX value moves to external_id."""
    r = _make_record(id="187231142", slug="ai-fluency-trap", published_at="2026-02-12T10:00:00Z")
    summary = {"tldr": "x", "key_claims": [], "notable_quotes": [], "topics": [], "article": ""}
    classified_links = {"external_classified": [], "substack_internal": []}
    path = write_pages.write_article_page(
        r, summary=summary, classified_links=classified_links,
        body_markdown="", repo_root=tmp_path,
    )
    content = path.read_text(encoding="utf-8")
    expected_id = path.stem
    assert f"id: {expected_id}" in content, f"id should be filename slug, got:\n{content[:300]}"
    assert "external_id: substack-187231142" in content
    assert "\nid: substack-187231142" not in content


def test_write_summary_page_uses_filename_slug_as_id(tmp_path):
    r = _make_record(id="187231142", slug="ai-fluency-trap")
    summary = {"tldr": "x", "key_claims": [], "notable_quotes": [], "topics": [], "article": ""}
    path = write_pages.write_summary_page(r, summary=summary, repo_root=tmp_path)
    content = path.read_text(encoding="utf-8")
    expected_id = path.stem
    assert f"id: {expected_id}" in content
    assert "external_id: substack-187231142" in content


def test_ensure_author_page_id_is_slug_without_prefix(tmp_path):
    r = _make_record()
    path = write_pages.ensure_author_page(r, repo_root=tmp_path)
    content = path.read_text(encoding="utf-8")
    assert "id: mario-gabriele" in content
    assert "id: person-mario-gabriele" not in content
    assert "external_id:" not in content


def test_ensure_publication_page_id_is_slug_without_prefix(tmp_path):
    r = _make_record()
    path = write_pages.ensure_publication_page(r, repo_root=tmp_path)
    content = path.read_text(encoding="utf-8")
    assert "id: thegeneralist" in content
    assert "id: company-thegeneralist" not in content
    assert "external_id:" not in content


def test_write_article_page_emits_three_tag_axes(tmp_path):
    r = _make_record()
    summary = {
        "tldr": "Trust matters.",
        "key_claims": ["A"],
        "notable_quotes": [],
        "takeaways": [],
        "topics": ["trust"],
        "article": "body",
    }
    classified_links = {"external_classified": [], "substack_internal": []}
    path = write_pages.write_article_page(
        r, summary=summary, classified_links=classified_links,
        body_markdown="# Title\n\nBody.\n", repo_root=tmp_path,
    )
    content = path.read_text(encoding="utf-8")
    assert "domain/" in content
    assert "function/" in content
    assert "signal/" in content


def test_write_summary_substack_page_emits_three_tag_axes(tmp_path):
    r = _make_record()
    summary = {
        "tldr": "Trust matters.",
        "key_claims": ["A"],
        "notable_quotes": [],
        "takeaways": [],
        "topics": [],
        "article": "body",
    }
    path = write_pages.write_summary_page(
        r, summary=summary, applied=None, repo_root=tmp_path, stance_change_note=None,
    )
    content = path.read_text(encoding="utf-8")
    assert "domain/" in content
    assert "function/" in content
    assert "signal/" in content


def test_ensure_author_page_emits_three_tag_axes(tmp_path):
    r = _make_record()
    path = write_pages.ensure_author_page(r, repo_root=tmp_path)
    content = path.read_text(encoding="utf-8")
    assert "domain/" in content
    assert "function/" in content
    assert "signal/" in content


def test_ensure_publication_page_emits_three_tag_axes(tmp_path):
    r = _make_record()
    path = write_pages.ensure_publication_page(r, repo_root=tmp_path)
    content = path.read_text(encoding="utf-8")
    assert "domain/" in content
    assert "function/" in content
    assert "signal/" in content
