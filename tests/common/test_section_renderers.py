"""Tests for scripts.common.section_renderers."""
from __future__ import annotations

from scripts.common.section_renderers import (
    render_tldr,
    render_core_argument,
    render_argument_structure,
    render_key_claims,
    render_memorable_examples,
    render_notable_quotes,
    render_strongest_fight,
    render_in_conversation_with,
    render_entities,
    render_applied_to_you,
    render_socratic_questions,
)


# ---------------------------------------------------------------------------
# render_tldr
# ---------------------------------------------------------------------------

def test_render_tldr_present():
    assert "## TL;DR" in render_tldr({"tldr": "Short summary."})


def test_render_tldr_empty():
    assert render_tldr({}) == ""
    assert render_tldr({"tldr": ""}) == ""


# ---------------------------------------------------------------------------
# render_core_argument
# ---------------------------------------------------------------------------

def test_render_core_argument_present():
    assert "## Core Argument" in render_core_argument({"core_argument": "Central thesis."})


def test_render_core_argument_empty():
    assert render_core_argument({}) == ""


# ---------------------------------------------------------------------------
# render_argument_structure
# ---------------------------------------------------------------------------

def test_render_argument_structure():
    summary = {"argument_graph": {"premises": ["P1"], "inferences": ["I1"], "conclusion": "C"}}
    result = render_argument_structure(summary)
    assert "## Argument Structure" in result
    assert "P1" in result
    assert "I1" in result
    assert "C" in result


def test_render_argument_structure_empty():
    assert render_argument_structure({}) == ""
    assert render_argument_structure({"argument_graph": {}}) == ""


# ---------------------------------------------------------------------------
# render_key_claims — both formats
# ---------------------------------------------------------------------------

def test_render_key_claims_bare_strings():
    """Legacy format: bare string claims."""
    summary = {"key_claims": ["Claim one", "Claim two"]}
    result = render_key_claims(summary)
    assert "## Key Claims" in result
    assert "Claim one" in result
    assert "with receipts" not in result  # bare format uses simpler heading


def test_render_key_claims_structured_dicts():
    """New format: structured claims with evidence."""
    summary = {"key_claims": [
        {"claim": "AI will eat software", "evidence_quote": "verbatim quote", "evidence_context": "context"},
    ]}
    result = render_key_claims(summary)
    assert "## Key Claims (with receipts)" in result
    assert "**AI will eat software**" in result
    assert "> verbatim quote" in result


def test_render_key_claims_empty():
    assert render_key_claims({}) == ""
    assert render_key_claims({"key_claims": []}) == ""


# ---------------------------------------------------------------------------
# render_memorable_examples
# ---------------------------------------------------------------------------

def test_render_memorable_examples():
    summary = {"memorable_examples": [{"title": "The EpiPen story", "story": "Mylan raised prices.", "lesson": "PBMs matter."}]}
    result = render_memorable_examples(summary)
    assert "### The EpiPen story" in result
    assert "Mylan raised prices." in result


def test_render_memorable_examples_empty():
    assert render_memorable_examples({}) == ""


# ---------------------------------------------------------------------------
# render_notable_quotes
# ---------------------------------------------------------------------------

def test_render_notable_quotes():
    result = render_notable_quotes({"notable_quotes": ["Stay hungry, stay foolish."]})
    assert "> Stay hungry, stay foolish." in result


def test_render_notable_quotes_empty():
    assert render_notable_quotes({}) == ""


# ---------------------------------------------------------------------------
# render_strongest_fight
# ---------------------------------------------------------------------------

def test_render_strongest_fight():
    summary = {"steelman": "Best version.", "strongest_rebuttal": "Counter.", "would_change_mind_if": "Data X."}
    result = render_strongest_fight(summary)
    assert "## The Strongest Fight" in result
    assert "Best version." in result


def test_render_strongest_fight_empty():
    assert render_strongest_fight({}) == ""


# ---------------------------------------------------------------------------
# render_in_conversation_with
# ---------------------------------------------------------------------------

def test_render_in_conversation_with():
    result = render_in_conversation_with({"in_conversation_with": ["Thinker A", "Book B"]})
    assert "- Thinker A" in result


def test_render_in_conversation_with_empty():
    assert render_in_conversation_with({}) == ""


# ---------------------------------------------------------------------------
# render_entities
# ---------------------------------------------------------------------------

def test_render_entities():
    summary = {"entities": {"people": ["Alice"], "companies": ["Acme"], "tools": [], "concepts": ["moats"]}}
    result = render_entities(summary)
    assert "**People:** Alice" in result
    assert "**Concepts:** moats" in result
    assert "Tools" not in result  # empty list skipped


def test_render_entities_empty():
    assert render_entities({}) == ""


# ---------------------------------------------------------------------------
# render_applied_to_you
# ---------------------------------------------------------------------------

def test_render_applied_to_you():
    applied = {
        "applied_paragraph": "This book matters because...",
        "applied_bullets": [{"claim": "Do X", "why_it_matters": "Because Y", "action": "Try Z"}],
        "thread_links": ["Brain wiki"],
    }
    result = render_applied_to_you(applied)
    assert "## Applied to You" in result
    assert "**Do X**" in result
    assert "[[Brain wiki]]" in result


def test_render_applied_to_you_empty():
    assert render_applied_to_you(None) == ""
    assert render_applied_to_you({}) == ""


# ---------------------------------------------------------------------------
# render_socratic_questions
# ---------------------------------------------------------------------------

def test_render_socratic_questions():
    applied = {"socratic_questions": ["Why not?", "What if?"]}
    result = render_socratic_questions(applied)
    assert "## Questions This Raises for You" in result
    assert "1. Why not?" in result
    assert "2. What if?" in result


def test_render_socratic_questions_empty():
    assert render_socratic_questions(None) == ""
    assert render_socratic_questions({}) == ""
