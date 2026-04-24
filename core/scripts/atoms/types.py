"""Atom, Q1Match, Q2Candidate, PassDResult dataclasses.

All dataclasses are frozen (immutable) so they can be safely shared across
the Pass D pipeline without aliasing bugs.

No I/O. No Gemini calls. Pure data containers.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


AtomType = Literal["concept", "playbook", "stance", "inquiry"]
LifecycleState = Literal["probationary", "active", "declining", "dormant"]
Polarity = Literal["for", "against", "neutral"]
Confidence = Literal["low", "medium", "high"]


@dataclass(frozen=True)
class Atom:
    """In-memory representation of an atom, loaded from the brain-state cache.

    Mirrors the entry shape in wiki/.brain-state.json's `atoms.index` block.
    See scripts/atoms/cache.py for the cache lifecycle.
    """
    id: str
    type: AtomType
    path: Path
    lifecycle_state: LifecycleState
    domains: list[str]
    topics: list[str]
    last_evidence_date: str
    evidence_count: int
    tldr: str  # one-line description for prompt serialization
    last_dream_pass: str = ""


@dataclass(frozen=True)
class Q1Match:
    """A single Pass D Q1 match: 'this source provides evidence for an existing atom'."""
    atom_id: str
    atom_type: AtomType
    snippet: str
    polarity: Polarity
    confidence: Confidence


@dataclass(frozen=True)
class Q2Candidate:
    """A single Pass D Q2 candidate: 'this source surfaces a new probationary atom'."""
    type: AtomType
    proposed_id: str
    title: str
    description: str
    tldr: str
    snippet: str
    polarity: Polarity
    rationale: str
    domains: list[str] = field(default_factory=list)
    in_conversation_with: list[str] = field(default_factory=list)
    steps: list[str] = field(default_factory=list)
    position: str = ""
    question: str = ""


@dataclass(frozen=True)
class PassDResult:
    """Structured output of a single Pass D Gemini call.

    The caller (an ingestor's pipeline) consumes this and dispatches:
      - q1_matches → scripts.atoms.evidence_writer.append_evidence
      - q2_candidates → scripts.atoms.probationary.create_or_extend
    """
    q1_matches: list[Q1Match] = field(default_factory=list)
    q2_candidates: list[Q2Candidate] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    dropped_q1_matches: int = 0
    dropped_q2_candidates: int = 0
