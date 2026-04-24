"""Shared section renderers for wiki page generation.

Extracted from the Substack pipeline's write_pages.py so that YouTube,
articles, and books can produce the same rich sections. Each renderer
returns either "" (skip) or a markdown chunk with its own ## heading
and trailing newline.

All renderers are defensive: they handle missing keys, empty arrays,
bare-string entries (legacy cached format), and structured-dict entries
(new schema) gracefully.
"""
from __future__ import annotations

from typing import Any


def render_tldr(summary: dict[str, Any]) -> str:
    tldr = (summary.get("tldr") or "").strip()
    if not tldr:
        return ""
    return f"## TL;DR\n\n{tldr}\n"


def render_core_argument(summary: dict[str, Any]) -> str:
    core = (summary.get("core_argument") or "").strip()
    if not core:
        return ""
    return f"## Core Argument\n\n{core}\n"


def render_argument_structure(summary: dict[str, Any]) -> str:
    ag = summary.get("argument_graph")
    if not ag:
        return ""

    premises = ag.get("premises") or []
    inferences = ag.get("inferences") or []
    conclusion = (ag.get("conclusion") or "").strip()

    if not premises and not inferences and not conclusion:
        return ""

    parts = ["## Argument Structure\n"]
    if premises:
        parts.append("**Premises:**\n")
        for p in premises:
            parts.append(f"- {p}")
        parts.append("")
    if inferences:
        parts.append("**Inferences:**\n")
        for i in inferences:
            parts.append(f"- {i}")
        parts.append("")
    if conclusion:
        parts.append(f"**Conclusion:** {conclusion}\n")

    return "\n".join(parts).rstrip() + "\n"


def render_key_claims(summary: dict[str, Any]) -> str:
    """Render key claims — handles both bare-string and structured dict formats.

    Bare-string format (legacy cached): ["claim text", ...]
    Structured dict format (new schema): [{"claim": ..., "evidence_quote": ..., "evidence_context": ...}, ...]
    """
    key_claims = summary.get("key_claims") or []
    if not key_claims:
        return ""

    # Detect format: if first non-None entry is a string, use bare format
    has_structured = any(isinstance(c, dict) for c in key_claims)

    if has_structured:
        parts = ["## Key Claims (with receipts)\n"]
        for c in key_claims:
            if isinstance(c, str):
                # Mixed format — bare string in a structured list
                c = c.strip()
                if c:
                    parts.append(f"- {c}")
                    parts.append("")
                continue
            if not isinstance(c, dict):
                continue
            claim = (c.get("claim") or "").strip()
            if not claim:
                continue
            evidence_quote = (c.get("evidence_quote") or "").strip()
            evidence_context = (c.get("evidence_context") or "").strip()
            quote_unverified = bool(c.get("quote_unverified"))

            if quote_unverified:
                parts.append(f"- ⚠️ **{claim}** (quote unverified)")
            else:
                parts.append(f"- **{claim}**")

            if evidence_quote:
                parts.append(f"  > {evidence_quote}")
                parts.append("")
            if evidence_context:
                parts.append(f"  {evidence_context}")
            parts.append("")
    else:
        parts = ["## Key Claims\n"]
        for c in key_claims:
            if isinstance(c, str) and c.strip():
                parts.append(f"- {c.strip()}")
        parts.append("")

    rendered = "\n".join(parts).rstrip()
    heading = "## Key Claims (with receipts)" if has_structured else "## Key Claims"
    if rendered == heading:
        return ""
    return rendered + "\n"


def render_memorable_examples(summary: dict[str, Any]) -> str:
    examples = summary.get("memorable_examples") or []
    if not examples:
        return ""

    parts = ["## Memorable Examples\n"]
    for ex in examples:
        if not isinstance(ex, dict):
            continue
        title = (ex.get("title") or "").strip()
        story = (ex.get("story") or "").strip()
        lesson = (ex.get("lesson") or "").strip()
        if not title and not story:
            continue
        if title:
            parts.append(f"### {title}\n")
        if story:
            parts.append(f"{story}\n")
        if lesson:
            parts.append(f"**Lesson:** {lesson}\n")

    rendered = "\n".join(parts).rstrip()
    if rendered == "## Memorable Examples":
        return ""
    return rendered + "\n"


def render_notable_quotes(summary: dict[str, Any]) -> str:
    quotes = summary.get("notable_quotes") or []
    if not quotes:
        return ""

    parts = ["## Notable Quotes\n"]
    for q in quotes:
        parts.append(f"> {q}")
        parts.append("")

    return "\n".join(parts).rstrip() + "\n"


def render_strongest_fight(summary: dict[str, Any]) -> str:
    steelman = (summary.get("steelman") or "").strip()
    strongest_rebuttal = (summary.get("strongest_rebuttal") or "").strip()
    would_change_mind_if = (summary.get("would_change_mind_if") or "").strip()

    if not steelman and not strongest_rebuttal and not would_change_mind_if:
        return ""

    parts = ["## The Strongest Fight\n"]
    if steelman:
        parts.append(f"**Steelman:** {steelman}\n")
    if strongest_rebuttal:
        parts.append(f"**Strongest rebuttal:** {strongest_rebuttal}\n")
    if would_change_mind_if:
        parts.append(f"**Would change your mind if:** {would_change_mind_if}\n")

    return "\n".join(parts).rstrip() + "\n"


def render_in_conversation_with(summary: dict[str, Any]) -> str:
    in_conv = summary.get("in_conversation_with") or []
    if not in_conv:
        return ""

    parts = ["## In Conversation With\n"]
    for entry in in_conv:
        parts.append(f"- {entry}")
    parts.append("")

    return "\n".join(parts).rstrip() + "\n"


def render_entities(summary: dict[str, Any]) -> str:
    entities = summary.get("entities")
    if not entities or not isinstance(entities, dict):
        return ""

    people = entities.get("people") or []
    companies = entities.get("companies") or []
    tools = entities.get("tools") or []
    concepts = entities.get("concepts") or []

    if not people and not companies and not tools and not concepts:
        return ""

    parts = ["## Entities\n"]
    if people:
        parts.append(f"**People:** {', '.join(people)}")
    if companies:
        parts.append(f"**Companies:** {', '.join(companies)}")
    if tools:
        parts.append(f"**Tools:** {', '.join(tools)}")
    if concepts:
        parts.append(f"**Concepts:** {', '.join(concepts)}")
    parts.append("")

    return "\n".join(parts).rstrip() + "\n"


def render_applied_to_you(applied: dict[str, Any] | None) -> str:
    if not applied:
        return ""

    applied_paragraph = (applied.get("applied_paragraph") or "").strip()
    applied_bullets = applied.get("applied_bullets") or []

    if not applied_paragraph and not applied_bullets:
        return ""

    parts = ["## Applied to You\n"]
    if applied_paragraph:
        parts.append(f"{applied_paragraph}\n")

    if applied_bullets:
        for b in applied_bullets:
            if not isinstance(b, dict):
                parts.append(f"- {b}")
                continue
            claim = (b.get("claim") or "").strip()
            why = (b.get("why_it_matters") or "").strip()
            action = (b.get("action") or "").strip()
            if not claim:
                continue
            line = f"- **{claim}**"
            if why:
                line += f" — {why}"
            if action:
                line += f" ({action})"
            parts.append(line)
        parts.append("")

    thread_links = [str(link).strip() for link in applied.get("thread_links") or [] if str(link).strip()]
    if thread_links:
        parts.append("_Touches:_ " + ", ".join(f"[[{link}]]" for link in thread_links))
        parts.append("")

    return "\n".join(parts).rstrip() + "\n"


def render_socratic_questions(applied: dict[str, Any] | None) -> str:
    if not applied:
        return ""

    questions = applied.get("socratic_questions") or []
    if not questions:
        return ""

    parts = ["## Questions This Raises for You\n"]
    for i, q in enumerate(questions, 1):
        parts.append(f"{i}. {q}")
    parts.append("")

    return "\n".join(parts).rstrip() + "\n"
