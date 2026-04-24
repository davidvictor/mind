from __future__ import annotations

import pytest
from pydantic import ValidationError

from mind.dream.v2.contracts import ApplyManifest, ApplyManifestEntry, ApplyPlan, ApplyPlanAction


def test_kene_apply_plan_is_shadow_blocked_by_default() -> None:
    plan = ApplyPlan(
        run_id="run-1",
        stage="kene",
        mode="shadow",
        actions=[
            ApplyPlanAction(
                action_id="write-kene-map",
                action_type="write_markdown",
                target_path="memory/dreams/kene/map.md",
                safe_to_apply=False,
                rationale="blocked until apply mode is approved",
            )
        ],
    )
    manifest = ApplyManifest(
        run_id="run-1",
        stage="kene",
        mode="shadow",
        entries=[
            ApplyManifestEntry(
                action_id="write-kene-map",
                status="blocked",
                target_path="memory/dreams/kene/map.md",
                notes=["shadow-only"],
            )
        ],
        write_count=0,
        warning_count=1,
    )

    assert plan.actions[0].safe_to_apply is False
    assert manifest.entries[0].status == "blocked"
    assert manifest.write_count == 0


def test_kene_write_mode_rejects_unsafe_apply_actions() -> None:
    with pytest.raises(ValidationError):
        ApplyPlan(
            run_id="run-1",
            stage="kene",
            mode="write",
            actions=[
                ApplyPlanAction(
                    action_id="write-kene-map",
                    action_type="write_markdown",
                    target_path="memory/dreams/kene/map.md",
                    safe_to_apply=False,
                )
            ],
        )
