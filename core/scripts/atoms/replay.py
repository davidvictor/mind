from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from scripts.atoms.evidence_writer import append_evidence
from scripts.atoms.probationary import create_or_extend
from scripts.atoms.types import PassDResult


@dataclass(frozen=True)
class PassDDispatchSummary:
    evidence_updates: int = 0
    probationary_updates: int = 0
    missing_atoms: int = 0


def apply_pass_d_result(
    result: PassDResult,
    *,
    dedupe_by_source: bool = False,
    evidence_date: str,
    recorded_on: str | None,
    source_link: str,
    repo_root: Path,
) -> PassDDispatchSummary:
    evidence_updates = 0
    probationary_updates = 0
    missing_atoms = 0
    for match in result.q1_matches:
        try:
            appended = append_evidence(
                atom_id=match.atom_id,
                atom_type=match.atom_type,
                date=evidence_date,
                dedupe_by_source=dedupe_by_source,
                recorded_on=recorded_on,
                source_link=source_link,
                snippet=match.snippet,
                polarity=match.polarity,
                repo_root=repo_root,
            )
        except FileNotFoundError:
            missing_atoms += 1
            continue
        if appended:
            evidence_updates += 1
    for candidate in result.q2_candidates:
        create_or_extend(
            type=candidate.type,
            proposed_id=candidate.proposed_id,
            title=candidate.title,
            description=candidate.description,
            tldr=candidate.tldr,
            snippet=candidate.snippet,
            polarity=candidate.polarity,
            rationale=candidate.rationale,
            domains=candidate.domains,
            in_conversation_with=candidate.in_conversation_with,
            steps=candidate.steps,
            position=candidate.position,
            question=candidate.question,
            date=evidence_date,
            dedupe_by_source=dedupe_by_source,
            recorded_on=recorded_on,
            source_link=source_link,
            repo_root=repo_root,
        )
        probationary_updates += 1
    return PassDDispatchSummary(
        evidence_updates=evidence_updates,
        probationary_updates=probationary_updates,
        missing_atoms=missing_atoms,
    )
