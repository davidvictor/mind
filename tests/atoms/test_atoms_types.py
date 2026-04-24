"""Tests for scripts.atoms.types — Atom, Q1Match, Q2Candidate, PassDResult dataclasses.

The dataclasses are immutable (frozen=True) so they can be safely shared
across the Pass D pipeline without aliasing bugs.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from scripts.atoms.types import (
    Atom,
    PassDResult,
    Q1Match,
    Q2Candidate,
)


def test_atom_construction():
    a = Atom(
        id="iteration-loop",
        type="concept",
        path=Path("wiki/concepts/iteration-loop.md"),
        lifecycle_state="active",
        domains=["work", "creative"],
        topics=["feedback-loops", "iteration"],
        last_evidence_date="2026-04-05",
        evidence_count=17,
        tldr="Becoming over being",
    )
    assert a.id == "iteration-loop"
    assert a.evidence_count == 17


def test_atom_is_frozen():
    a = Atom(
        id="x",
        type="concept",
        path=Path("x.md"),
        lifecycle_state="active",
        domains=[],
        topics=[],
        last_evidence_date="2026-04-08",
        evidence_count=0,
        tldr="",
    )
    with pytest.raises((AttributeError, TypeError)):
        a.id = "y"  # type: ignore[misc]


def test_q1_match_construction():
    m = Q1Match(
        atom_id="iteration-loop",
        atom_type="concept",
        snippet="becoming as iterative process",
        polarity="for",
        confidence="high",
    )
    assert m.polarity == "for"
    assert m.confidence == "high"


def test_q2_candidate_construction():
    c = Q2Candidate(
        type="stance",
        proposed_id="managers-should-do-IC-work",
        title="Managers should do IC work",
        description="Engineering managers retain credibility by shipping code",
        tldr="Managers keep credibility by shipping code.",
        snippet="ICs respect IC managers",
        polarity="for",
        rationale="Recurrent across two essays this week",
        domains=["work"],
        in_conversation_with=["credibility-as-output"],
        position="Managers should continue shipping code.",
    )
    assert c.proposed_id == "managers-should-do-IC-work"
    assert c.type == "stance"
    assert c.tldr == "Managers keep credibility by shipping code."


def test_pass_d_result_holds_lists():
    r = PassDResult(q1_matches=[], q2_candidates=[], warnings=["warn"], dropped_q1_matches=1, dropped_q2_candidates=2)
    assert r.q1_matches == []
    assert r.q2_candidates == []
    assert r.warnings == ["warn"]
    assert r.dropped_q1_matches == 1
    assert r.dropped_q2_candidates == 2

    m = Q1Match(
        atom_id="x",
        atom_type="concept",
        snippet="...",
        polarity="neutral",
        confidence="low",
    )
    r2 = PassDResult(q1_matches=[m], q2_candidates=[])
    assert len(r2.q1_matches) == 1


def test_atom_lifecycle_state_enum_values_accepted():
    """The Literal type allows the four valid lifecycle states."""
    for state in ("probationary", "active", "declining", "dormant"):
        a = Atom(
            id="x",
            type="concept",
            path=Path("x"),
            lifecycle_state=state,  # type: ignore[arg-type]
            domains=[],
            topics=[],
            last_evidence_date="2026-04-08",
            evidence_count=0,
            tldr="",
        )
        assert a.lifecycle_state == state


def test_atom_type_enum_values_accepted():
    for atom_type in ("concept", "playbook", "stance", "inquiry"):
        a = Atom(
            id="x",
            type=atom_type,  # type: ignore[arg-type]
            path=Path("x"),
            lifecycle_state="active",
            domains=[],
            topics=[],
            last_evidence_date="2026-04-08",
            evidence_count=0,
            tldr="",
        )
        assert a.type == atom_type
