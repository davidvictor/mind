from __future__ import annotations

import pytest
from pydantic import ValidationError

from mind.dream.v2.contracts import (
    ApplyPlan,
    ApplyPlanAction,
    KeneArrangementPlan,
    KeneAtomSnapshot,
    KeneCritique,
    KeneGroup,
    KeneInputBundle,
    KeneRelationChange,
    KeneRelationDiff,
    KeneRenderPackage,
)


def test_kene_contracts_accept_valid_shadow_payloads() -> None:
    atom = KeneAtomSnapshot(
        atom_id="local-first-systems",
        atom_type="concept",
        title="Local-first systems",
        path="memory/concepts/local-first-systems.md",
        lifecycle_state="active",
        domains=["meta"],
        relation_ids=["user-owned-ai"],
        evidence_count=3,
    )
    bundle = KeneInputBundle(
        run_id="run-1",
        generated_at="2026-04-24T00:00:00Z",
        atoms=[atom],
        source_ids=["summary-example-seed"],
    )
    group = KeneGroup(
        group_id="group-meta-concept",
        title="Meta Concept",
        member_atom_ids=["local-first-systems"],
    )
    arrangement = KeneArrangementPlan(run_id="run-1", groups=[group])
    relation_diff = KeneRelationDiff(
        run_id="run-1",
        changes=[
            KeneRelationChange(
                change_id="review-1",
                action="review_only",
                source_atom_id="local-first-systems",
                target_atom_id="user-owned-ai",
            )
        ],
    )
    critique = KeneCritique(run_id="run-1", findings=["shadow only"])
    render = KeneRenderPackage(
        run_id="run-1",
        markdown_target_path="memory/dreams/kene/2026-04-24-run-1.md",
        title="Kene Map",
    )

    assert bundle.stage == "kene"
    assert arrangement.groups == [group]
    assert relation_diff.changes[0].review_only is True
    assert critique.findings == ["shadow only"]
    assert render.markdown_target_path.startswith("memory/dreams/kene/")


def test_kene_contracts_reject_invalid_ids_and_unsafe_paths() -> None:
    with pytest.raises(ValidationError):
        KeneAtomSnapshot(
            atom_id="Bad ID",
            atom_type="concept",
            title="Bad",
            path="memory/concepts/bad.md",
            lifecycle_state="active",
        )

    with pytest.raises(ValidationError):
        KeneRenderPackage(
            run_id="run-1",
            markdown_target_path="../memory/dreams/kene/bad.md",
            title="Bad",
        )


def test_apply_plan_rejects_unsafe_write_mode_actions() -> None:
    action = ApplyPlanAction(
        action_id="write-kene-map",
        action_type="write_markdown",
        target_path="memory/dreams/kene/map.md",
        safe_to_apply=False,
    )

    with pytest.raises(ValidationError):
        ApplyPlan(run_id="run-1", stage="kene", mode="write", actions=[action])

    shadow = ApplyPlan(run_id="run-1", stage="kene", mode="shadow", actions=[action])
    assert shadow.actions[0].safe_to_apply is False
