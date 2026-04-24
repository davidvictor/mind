from __future__ import annotations

from pathlib import Path

from mind.services.durable_write import DurableLinkTarget
from mind.services.materialization import (
    MaterializationCandidate,
    materialize_primary_target,
    select_primary_targets,
)


def test_select_primary_targets_separates_creator_and_publisher() -> None:
    candidates = [
        MaterializationCandidate(
            page_type="person",
            name="Alice Author",
            role="creator",
            confidence=0.95,
            deterministic=True,
            source="article",
        ),
        MaterializationCandidate(
            page_type="company",
            name="Example Media",
            role="publisher",
            confidence=0.99,
            deterministic=True,
            source="article",
        ),
    ]
    targets = select_primary_targets(candidates)
    assert targets.creator_target is not None
    assert targets.creator_target.page_type == "person"
    assert targets.publisher_target is not None
    assert targets.publisher_target.page_type == "company"


def test_select_primary_targets_prefers_best_eligible_candidate() -> None:
    candidates = [
        MaterializationCandidate(
            page_type="person",
            name="Weak First",
            role="creator",
            confidence=0.2,
            deterministic=False,
            source="article",
        ),
        MaterializationCandidate(
            page_type="person",
            name="Strong Second",
            role="creator",
            confidence=0.95,
            deterministic=True,
            source="article",
        ),
    ]
    targets = select_primary_targets(candidates)
    assert targets.creator_target is not None
    assert targets.creator_target.name == "Strong Second"


def test_incidental_person_does_not_materialize(tmp_path: Path) -> None:
    candidate = MaterializationCandidate(
        page_type="person",
        name="Quoted Analyst",
        role="creator",
        confidence=0.2,
        deterministic=False,
        source="article",
    )
    out = materialize_primary_target(
        candidate,
        repo_root=tmp_path,
        source_link=DurableLinkTarget(page_type="summary", page_id="summary-x"),
        today="2026-04-09",
    )
    assert out is None


def test_quoted_third_party_company_does_not_materialize(tmp_path: Path) -> None:
    candidate = MaterializationCandidate(
        page_type="company",
        name="Third Party Co",
        role="publisher",
        confidence=0.4,
        deterministic=False,
        source="article",
    )
    out = materialize_primary_target(
        candidate,
        repo_root=tmp_path,
        source_link=DurableLinkTarget(page_type="summary", page_id="summary-x"),
        today="2026-04-09",
    )
    assert out is None


def test_sponsor_tool_does_not_materialize_without_central_subject(tmp_path: Path) -> None:
    candidate = MaterializationCandidate(
        page_type="tool",
        name="Sponsored Tool",
        role="tool",
        confidence=0.99,
        deterministic=True,
        source="video",
        central_subject=False,
    )
    out = materialize_primary_target(
        candidate,
        repo_root=tmp_path,
        source_link=DurableLinkTarget(page_type="summary", page_id="summary-x"),
        today="2026-04-09",
    )
    assert out is None


def test_primary_creator_materializes_when_high_confidence(tmp_path: Path) -> None:
    candidate = MaterializationCandidate(
        page_type="person",
        name="Alice Author",
        role="creator",
        confidence=0.95,
        deterministic=True,
        source="substack",
    )
    out = materialize_primary_target(
        candidate,
        repo_root=tmp_path,
        source_link=DurableLinkTarget(page_type="summary", page_id="summary-x"),
        today="2026-04-09",
    )
    assert out is not None
    assert out.exists()
    text = out.read_text(encoding="utf-8")
    assert "type: person" in text
    assert "sources:" in text
    assert "[[summary-x]]" in text
    assert "writer" not in text
    assert "substack" not in text
    assert "domain/relationships" in text
    assert "domains:\n  - relationships" in text
