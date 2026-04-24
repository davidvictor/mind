"""Substack prompt-builder tests.

These exercise the pure prompt-construction path in
``mind.services.prompt_builders`` and the underlying templates in
``scripts.substack.prompts``. The transport layer (LLMService / AI Gateway) is
covered elsewhere.
"""
from mind.services.prompt_builders import (
    build_applied_to_post_prompt,
    build_classify_links_prompt,
    build_summarize_substack_prompt,
    build_update_author_stance_prompt,
)
from scripts.substack.prompts import APPLIED_TO_POST, SUMMARIZE_POST, UPDATE_STANCE


# ---------------------------------------------------------------------------
# SUMMARIZE_POST — wrapper + template behaviour
# ---------------------------------------------------------------------------

def test_summarize_substack_prompt_builds_prompt_with_metadata():
    prompt = build_summarize_substack_prompt(
        title="On Trust",
        publication="The Generalist",
        author="Mario Gabriele",
        body_markdown="Trust is the root of everything.",
    )
    assert "On Trust" in prompt
    assert "The Generalist" in prompt
    assert "Mario Gabriele" in prompt
    assert "Trust is the root of everything." in prompt
    assert "%%BODY%%" not in prompt


def test_summarize_substack_prompt_context_blocks_appear_verbatim():
    prior = "## Prior posts in your wiki\n- [[on-focus]] \"On Focus\" — Focus is rare.\n"
    stance = "## What this author believed last time you read them\n\nBullish on decentralisation.\n"

    prompt = build_summarize_substack_prompt(
        title="On Trust",
        publication="The Generalist",
        author="Mario Gabriele",
        body_markdown="Trust is the root of everything.",
        prior_posts_context=prior,
        stance_context=stance,
    )
    assert "Prior posts in your wiki" in prompt
    assert "On Focus" in prompt
    assert "Focus is rare." in prompt
    assert "What this author believed last time you read them" in prompt
    assert "Bullish on decentralisation." in prompt
    assert "Trust is the root of everything." in prompt
    assert "%%BODY%%" not in prompt


# ---------------------------------------------------------------------------
# SUMMARIZE_POST template smoke tests
# ---------------------------------------------------------------------------

def _render(prior_posts_context="", stance_context="", body=""):
    rendered = SUMMARIZE_POST.format(
        title="On Trust",
        publication="The Generalist",
        author="Mario Gabriele",
    )
    rendered = rendered.replace("%%PRIOR_POSTS%%", prior_posts_context)
    rendered = rendered.replace("%%STANCE_CONTEXT%%", stance_context)
    rendered = rendered.replace("%%BODY%%", body)
    return rendered


def test_v2_template_renders_with_minimum_input_no_key_error():
    rendered = _render()
    assert "On Trust" in rendered
    assert "The Generalist" in rendered
    assert "Mario Gabriele" in rendered
    assert "%%BODY%%" not in rendered
    assert "%%PRIOR_POSTS%%" not in rendered
    assert "%%STANCE_CONTEXT%%" not in rendered


def test_v2_template_renders_with_full_context_blocks_no_key_error():
    prior = "## Prior posts in your wiki\n- [[foo]] \"Foo\" (Mario Gabriele, 2026-01-01) — something\n"
    stance = "## Author stance\nBullish on trust.\n"
    rendered = _render(prior_posts_context=prior, stance_context=stance)
    assert "Prior posts in your wiki" in rendered
    assert "Author stance" in rendered
    assert "Bullish on trust." in rendered


def test_v2_template_contains_required_schema_fields():
    rendered = _render()
    for field in [
        "schema_version",
        "evidence_quote",
        "argument_graph",
        "steelman",
        "strongest_rebuttal",
        "would_change_mind_if",
        "entities",
        "relates_to_prior",
        "memorable_examples",
        "in_conversation_with",
    ]:
        assert field in rendered, f"Missing field in prompt: {field}"


def test_summarize_substack_prompt_handles_braces_in_body():
    """Body markdown with literal braces must not break str.format() AND must appear verbatim."""
    tricky_body = "Here is code: `Vec<T>` and a JSX snippet `<Foo bar={baz} />` and {template_var}"
    prompt = build_summarize_substack_prompt(
        title="T",
        publication="P",
        author="A",
        body_markdown=tricky_body,
    )
    assert "Vec<T>" in prompt
    assert "<Foo bar={baz} />" in prompt
    assert "{template_var}" in prompt
    assert "%%BODY%%" not in prompt


def test_v2_template_empty_prior_posts_context_does_not_produce_header():
    rendered = _render(prior_posts_context="")
    assert "Prior posts in your wiki" not in rendered


def test_summarize_post_prompt_includes_anti_sales_rule():
    out = build_summarize_substack_prompt(
        publication="Test Pub",
        author="Test Author",
        title="Test Title",
        body_markdown="hello world",
    )
    assert "%%ANTI_SALES%%" not in out, "placeholder should be substituted"
    assert "ANTI-SALES RULE" in out
    assert "sponsor reads" in out.lower()


def test_summarize_substack_prompt_handles_braces_in_prior_posts_context():
    tricky_context = "## Prior posts in your wiki\n- [[foo]] talks about {template_var} and `Vec<T>`"
    prompt = build_summarize_substack_prompt(
        title="T",
        publication="P",
        author="A",
        body_markdown="body",
        prior_posts_context=tricky_context,
    )
    assert "{template_var}" in prompt
    assert "Vec<T>" in prompt
    assert "%%PRIOR_POSTS%%" not in prompt


def test_summarize_substack_prompt_handles_braces_in_stance_context():
    tricky_stance = (
        "## What this author believed last time you read them\n\n"
        "- believes {something}\n- uses `{jsx_expr}`"
    )
    prompt = build_summarize_substack_prompt(
        title="T",
        publication="P",
        author="A",
        body_markdown="body",
        stance_context=tricky_stance,
    )
    assert "{something}" in prompt
    assert "{jsx_expr}" in prompt
    assert "%%STANCE_CONTEXT%%" not in prompt


# ---------------------------------------------------------------------------
# classify_links_batch prompt builder
# ---------------------------------------------------------------------------

def test_classify_links_prompt_builds_prompt_with_urls():
    links = [
        {"url": "https://stratechery.com/2024/aggregators", "anchor_text": "aggregators", "context_snippet": "as discussed in aggregators"},
        {"url": "https://twitter.com/someone", "anchor_text": "@someone", "context_snippet": "h/t @someone"},
    ]
    prompt = build_classify_links_prompt(
        post_title="On Trust",
        publication="The Generalist",
        links=links,
    )
    assert "stratechery.com" in prompt
    assert "twitter.com/someone" in prompt


def test_classify_links_prompt_handles_braces_in_anchor_text():
    links = [
        {"url": "https://react.dev/reference/useState",
         "anchor_text": "useState({foo: bar})",
         "context_snippet": "See the example: const [x, setX] = useState({count: 0})"},
    ]
    # Should not crash even with literal braces in anchor/context
    prompt = build_classify_links_prompt(
        post_title="React hooks",
        publication="Some Blog",
        links=links,
    )
    assert "react.dev" in prompt


# ---------------------------------------------------------------------------
# APPLIED_TO_POST template + wrapper
# ---------------------------------------------------------------------------

def test_applied_to_post_template_renders_with_all_placeholders():
    rendered = APPLIED_TO_POST.format(title="T", publication="P", author="A")
    assert isinstance(rendered, str)
    assert "T" in rendered
    assert "P" in rendered
    assert "A" in rendered


def test_applied_to_post_template_marker_slots_present():
    rendered = APPLIED_TO_POST.format(title="T", publication="P", author="A")
    assert "%%PROFILE%%" in rendered
    assert "%%SUMMARY%%" in rendered


def test_applied_to_post_template_schema_fields():
    rendered = APPLIED_TO_POST.format(title="T", publication="P", author="A")
    for field in ("applied_paragraph", "applied_bullets", "socratic_questions", "thread_links"):
        assert field in rendered, f"Missing schema field: {field}"


def test_applied_to_post_template_schema_fields_include_claim_matter_action():
    rendered = APPLIED_TO_POST.format(title="T", publication="P", author="A")
    for field in ("claim", "why_it_matters", "action"):
        assert field in rendered, f"Missing applied_bullets sub-field: {field}"


def test_applied_to_post_wrapper_replaces_markers():
    profile_body = "PROFILE BODY with {brace} safe"
    summary = {
        "tldr": "x",
        "core_argument": "y",
        "key_claims": [{"claim": "c1", "evidence_quote": "...", "evidence_context": "..."}],
        "topics": ["t1"],
        "article": "Long article text that should be dropped.",
        "entities": {"people": ["Someone"]},
    }

    prompt = build_applied_to_post_prompt(
        title="T",
        publication="P",
        author="A",
        profile_context=profile_body,
        summary=summary,
    )

    assert "%%PROFILE%%" not in prompt
    assert "%%SUMMARY%%" not in prompt
    assert profile_body in prompt
    assert "x" in prompt  # tldr value


def test_applied_to_post_wrapper_compresses_summary():
    summary = {
        "tldr": "Short tldr here",
        "core_argument": "Central thesis paragraph.",
        "key_claims": [
            {"claim": "Claim alpha", "evidence_quote": "...", "evidence_context": "..."},
            {"claim": "Claim beta", "evidence_quote": "...", "evidence_context": "..."},
        ],
        "topics": ["topic-one", "topic-two"],
        "article": "FULL ARTICLE BODY — MUST NOT APPEAR IN COMPRESSED PROMPT",
        "entities": {"people": ["Alice", "Bob"], "companies": ["ACME"]},
        "notable_quotes": ["A long verbatim quote that should be dropped."],
    }

    prompt = build_applied_to_post_prompt(
        title="T",
        publication="P",
        author="A",
        profile_context="My profile",
        summary=summary,
    )

    assert "Short tldr here" in prompt
    assert "Central thesis paragraph." in prompt
    assert "topic-one" in prompt
    assert "Claim alpha" in prompt
    assert "FULL ARTICLE BODY" not in prompt
    assert "MUST NOT APPEAR" not in prompt
    assert "evidence_quote" not in prompt
    assert "evidence_context" not in prompt


# ---------------------------------------------------------------------------
# UPDATE_STANCE template + wrapper
# ---------------------------------------------------------------------------

def test_update_stance_template_renders_with_minimum_placeholders():
    rendered = UPDATE_STANCE.format(author="Mario Gabriele", post_slug="2026-03-15-on-trust")
    assert isinstance(rendered, str)
    assert "Mario Gabriele" in rendered
    assert "2026-03-15-on-trust" in rendered


def test_update_stance_template_has_marker_slots():
    rendered = UPDATE_STANCE.format(author="A", post_slug="s")
    assert "%%POST_TITLE%%" in rendered
    assert "%%CURRENT_STANCE%%" in rendered
    assert "%%SUMMARY%%" in rendered


def test_update_stance_template_schema_fields():
    rendered = UPDATE_STANCE.format(author="A", post_slug="s")
    for field in ("stance_delta_md", "change_note"):
        assert field in rendered, f"Missing schema field: {field}"
    for section in ("Core beliefs", "Open questions", "Recent shifts", "Contradictions observed"):
        assert section in rendered, f"Missing section heading: {section}"


def test_update_author_stance_prompt_replaces_all_markers():
    tricky_title = "On {braces} and <angles> in titles"
    tricky_stance = "## Core beliefs\n\n- Believes in {config: value} patterns.\n"
    summary = {
        "tldr": "Short tldr.",
        "core_argument": "Main argument here.",
        "key_claims": [{"claim": "Claim A", "evidence_quote": "...", "evidence_context": "..."}],
        "topics": ["trust", "networks"],
        "article": "LONG ARTICLE SHOULD NOT APPEAR",
        "entities": {"people": ["Alice"]},
    }

    prompt = build_update_author_stance_prompt(
        author="Mario Gabriele",
        title=tricky_title,
        post_slug="2026-03-15-on-trust",
        current_stance=tricky_stance,
        summary=summary,
    )

    assert "%%POST_TITLE%%" not in prompt
    assert "%%CURRENT_STANCE%%" not in prompt
    assert "%%SUMMARY%%" not in prompt
    assert tricky_title in prompt
    assert "Believes in {config: value} patterns." in prompt


def test_update_author_stance_prompt_compresses_summary():
    summary = {
        "tldr": "Short tldr here",
        "core_argument": "Central thesis.",
        "key_claims": [
            {"claim": "Claim alpha", "evidence_quote": "...", "evidence_context": "..."},
        ],
        "topics": ["trust"],
        "article": "FULL ARTICLE BODY — MUST NOT APPEAR IN COMPRESSED PROMPT",
        "entities": {"people": ["Alice"], "companies": ["ACME"]},
        "notable_quotes": ["A long verbatim quote that should be dropped."],
    }

    prompt = build_update_author_stance_prompt(
        author="Author",
        title="Title",
        post_slug="slug",
        current_stance="",
        summary=summary,
    )

    assert "Short tldr here" in prompt
    assert "Central thesis." in prompt
    assert "Claim alpha" in prompt
    assert "FULL ARTICLE BODY" not in prompt
    assert "MUST NOT APPEAR" not in prompt
    assert "evidence_quote" not in prompt
    assert "evidence_context" not in prompt
