from __future__ import annotations

from scripts.atoms.canonical import canonicalize_atom_page


def test_canonicalize_playbook_uses_candidate_metadata_and_normalizes_sections() -> None:
    rendered = canonicalize_atom_page(
        frontmatter={
            "id": "incident-runbook",
            "type": "playbook",
            "title": "Incident Runbook",
            "status": "active",
            "created": "2026-04-16",
            "last_updated": "2026-04-16",
            "aliases": [],
            "tags": ["domain/meta", "function/playbook", "signal/working"],
            "domains": ["meta"],
            "relates_to": [],
            "sources": ["[[summary-a]]"],
            "derived_from": [],
            "applied_by_owner": False,
            "lifecycle_state": "probationary",
            "last_evidence_date": "2026-04-16",
            "evidence_count": 1,
            "last_dream_pass": "2026-04-16",
        },
        body="# A thin stub\n\n## Evidence log\n\n- 2026-04-16 — [[summary-a]] — first signal\n",
        candidate={
            "type": "playbook",
            "proposed_id": "incident-runbook",
            "title": "Incident Runbook",
            "description": "Write the runbook before the incident happens.",
            "tldr": "Write the runbook before the incident happens.",
            "domains": ["work", "craft"],
            "in_conversation_with": ["source-to-atom-promotion"],
            "steps": ["Triage the incident.", "Send the first stakeholder update."],
        },
    )

    assert rendered.frontmatter["domains"] == ["work", "craft"]
    assert rendered.frontmatter["relates_to"] == ["[[source-to-atom-promotion]]"]
    assert rendered.body.startswith("# Incident Runbook\n\nWrite the runbook before the incident happens.\n")
    assert "## Steps\n\n- Triage the incident.\n- Send the first stakeholder update.\n" in rendered.body
    assert "\n\n\n" not in rendered.body[:80]


def test_canonicalize_stance_preserves_extra_sections_and_migrates_legacy_position() -> None:
    rendered = canonicalize_atom_page(
        frontmatter={
            "id": "file-first-record",
            "type": "stance",
            "title": "Local-First Knowledge Should Stay File-First",
            "status": "active",
            "created": "2026-04-14",
            "last_updated": "2026-04-14",
            "aliases": [],
            "tags": ["domain/meta", "function/stance", "signal/working"],
            "domains": ["meta", "work"],
            "relates_to": [],
            "sources": [],
            "confidence": "starter",
        },
        body=(
            "# Local-First Knowledge Should Stay File-First\n\n"
            "## Position\n\n"
            "Keep the durable record visible in files.\n\n"
            "## Why This Matters\n\n"
            "It keeps the graph inspectable.\n"
        ),
    )

    assert rendered.frontmatter["position"] == "Keep the durable record visible in files."
    assert "## TL;DR" in rendered.body
    assert "## Evidence log" in rendered.body
    assert "## Contradictions" in rendered.body
    assert "## Why It Matters" in rendered.body
    assert "## Position" in rendered.body


def test_canonicalize_inquiry_uses_legacy_question_and_preserves_resolution_section() -> None:
    rendered = canonicalize_atom_page(
        frontmatter={
            "id": "how-should-the-system-evolve",
            "type": "inquiry",
            "title": "How should the system evolve",
            "status": "active",
            "created": "2026-04-14",
            "last_updated": "2026-04-14",
            "aliases": [],
            "tags": ["domain/meta", "function/inquiry", "signal/working"],
            "domains": ["meta", "work"],
            "relates_to": [],
            "sources": [],
            "question": "How should the system evolve",
        },
        body=(
            "# How Should the System Evolve\n\n"
            "## The Question\n\n"
            "What additional structure would make the graph more useful without making it harder to operate?\n\n"
            "## What Would Resolve It\n\n"
            "- Clear evidence that a hub is overloaded.\n"
        ),
    )

    assert rendered.frontmatter["question"] == (
        "What additional structure would make the graph more useful without making it harder to operate?"
    )
    assert "## Evidence log" in rendered.body
    assert "## What Would Resolve It" in rendered.body
    assert "## The Question" not in rendered.body
