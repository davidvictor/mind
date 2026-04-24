from __future__ import annotations

from datetime import date

import pytest

from scripts.atoms.synthesis import build_active_synthesis_prompt, parse_active_synthesis_result


def test_parse_active_synthesis_result_for_concept() -> None:
    result = parse_active_synthesis_result(
        {
            "intro": "A mature concept about integrated judgment.",
            "tldr": "Integrated judgment is the moat.",
            "why_it_matters": "It changes how the work compounds.",
            "mechanism": "The same person holds product and technical judgment together.",
            "examples": ["Example Studio", "Example Product"],
            "in_conversation_with": ["one-senior-builder-augmented-by-ai"],
            "typed_relations": {
                "extends": ["one-senior-builder-augmented-by-ai"],
                "adjacent_to": ["design-and-engineering-compression"],
            },
        },
        atom_type="concept",
    )

    assert result.tldr == "Integrated judgment is the moat."
    assert result.examples == ["Example Studio", "Example Product"]
    assert result.typed_relations["extends"] == ["one-senior-builder-augmented-by-ai"]


def test_parse_active_synthesis_result_rejects_missing_required_fields() -> None:
    with pytest.raises(ValueError, match="missing question"):
        parse_active_synthesis_result(
            {
                "intro": "An inquiry intro.",
                "tldr": "A short inquiry summary.",
                "why_it_matters": "It shapes product bets.",
                "current_hypotheses": ["Maybe X", "Maybe Y"],
                "what_would_resolve_it": ["A trusted outside benchmark."],
            },
            atom_type="inquiry",
        )


def test_build_active_synthesis_prompt_serializes_date_frontmatter() -> None:
    prompt = build_active_synthesis_prompt(
        atom_type="concept",
        atom_id="local-first-systems",
        title="Local-First Systems",
        frontmatter={
            "created": date(2026, 4, 20),
            "last_updated": date(2026, 4, 20),
            "relates_to": ["[[user-owned-ai]]"],
        },
        body="# Local-First Systems\n\nDurable state should stay visible.\n",
        evidence_log=["- [[source-a]] — Durable state should stay visible."],
        typed_neighbors=[{"atom_id": "user-owned-ai", "type": "concept", "tldr": "Users should own the durable record."}],
        generic_neighbors=[],
        contradiction_signals=[],
        cooccurrence_signals=[],
    )

    assert '"created": "2026-04-20"' in prompt
    assert '"last_updated": "2026-04-20"' in prompt
