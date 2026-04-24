from __future__ import annotations

import json
from pathlib import Path

from mind.dream.v2.apply import build_apply_manifest_from_plan, build_weave_apply_plan
from mind.dream.v2.artifacts import build_layout, write_run_manifest, write_stage_json
from mind.dream.v2.contracts import (
    DreamRunManifest,
    ReviewNudge,
    SafeClusterRefUpdate,
    StageRunSummary,
    WeaveClusterReport,
    WeaveClusterReportsArtifact,
    WeaveCritiqueArtifact,
    WeaveStructuralActionsArtifact,
)


def test_dream_v2_artifact_layout_writes_stage_and_run_files(tmp_path: Path) -> None:
    layout = build_layout(
        repo_root=tmp_path,
        artifact_root="raw/reports/dream/v2",
        run_id="run-7",
        stage="weave",
    )

    candidate_path = write_stage_json(layout, "candidate-set.json", {"candidate_count": 3}, dry_run=False)
    manifest_path = write_run_manifest(
        layout,
        DreamRunManifest(
            run_id="run-7",
            started_at="2026-04-21T00:00:00Z",
            completed_at="2026-04-21T00:00:01Z",
            mode="shadow",
            artifact_root="raw/reports/dream/v2/runs/run-7",
            stages=[StageRunSummary(stage="weave", status="completed", candidate_count=3)],
        ),
        dry_run=False,
    )

    assert candidate_path == "raw/reports/dream/v2/runs/run-7/stage-weave/candidate-set.json"
    assert manifest_path == "raw/reports/dream/v2/runs/run-7/manifest.json"
    assert json.loads((tmp_path / candidate_path).read_text(encoding="utf-8")) == {"candidate_count": 3}
    manifest = json.loads((tmp_path / manifest_path).read_text(encoding="utf-8"))
    assert manifest["run_id"] == "run-7"
    assert manifest["stages"][0]["stage"] == "weave"


def test_dream_v2_apply_manifest_renders_expected_shadow_entries() -> None:
    reports = WeaveClusterReportsArtifact(
        reports=[
            WeaveClusterReport(
                cluster_id="weave-alpha",
                title="Alpha",
                thesis="Alpha thesis",
                why_now="Why now",
            )
        ]
    )
    actions = WeaveStructuralActionsArtifact(
        safe_cluster_ref_updates=[
            SafeClusterRefUpdate(
                cluster_id="weave-alpha",
                atom_ids=["alpha", "beta"],
                cluster_ref="[[weave-alpha]]",
            )
        ],
        review_nudges=[
            ReviewNudge(
                nudge_id="nudge-alpha",
                title="Review Alpha",
                body="Check alpha",
                target_path="memory/inbox/nudges/nudge-alpha.md",
            )
        ],
    )
    critique = WeaveCritiqueArtifact(approved_cluster_ids=["weave-alpha"])

    plan = build_weave_apply_plan(
        run_id="run-9",
        mode="shadow",
        reports=reports,
        actions=actions,
        critique=critique,
    )
    manifest = build_apply_manifest_from_plan(run_id="run-9", mode="shadow", plan=plan)

    assert [action.action_type for action in plan.actions] == [
        "write_markdown",
        "update_frontmatter",
        "emit_nudge",
    ]
    assert [
        (entry.action_id, entry.status, entry.target_path)
        for entry in manifest.entries
    ] == [
        ("write-weave-alpha", "skipped", "memory/dreams/weave/weave-alpha.md"),
        ("cluster-ref-weave-alpha", "skipped", "memory/dreams/weave/weave-alpha.md"),
        ("nudge-alpha", "skipped", "memory/inbox/nudges/nudge-alpha.md"),
    ]
