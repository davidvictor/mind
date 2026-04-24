from __future__ import annotations

from pathlib import Path

from mind.dream.v2.compare import build_weave_compare_artifact
from mind.dream.v2.contracts import CandidateSet, ReconciledCluster, WeaveClusterReportsArtifact, WeaveCritiqueArtifact, WeaveLocalProposalArtifact
from scripts.common.vault import Vault
from tests.support import write_repo_config


def _write_v1_weave_page(root: Path, *, cluster_id: str, member_ids: list[str]) -> None:
    target = root / "memory" / "dreams" / "weave" / f"{cluster_id}.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    member_block = "".join(f"  - {item}\n" for item in member_ids)
    target.write_text(
        "---\n"
        f"id: {cluster_id}\n"
        "type: note\n"
        f'title: "{cluster_id}"\n'
        "status: active\n"
        "created: 2026-04-21\n"
        "last_updated: 2026-04-21\n"
        "aliases: []\n"
        "tags: []\n"
        "domains: []\n"
        "relates_to: []\n"
        "sources: []\n"
        f"member_atom_ids:\n{member_block}"
        "---\n\n"
        f"# {cluster_id}\n",
        encoding="utf-8",
    )


def test_build_weave_compare_artifact_uses_v1_baseline_and_v2_metrics(tmp_path: Path) -> None:
    write_repo_config(tmp_path, dream_enabled=True, create_me=True)
    _write_v1_weave_page(tmp_path, cluster_id="weave-alpha", member_ids=["alpha", "beta", "gamma"])
    compare = build_weave_compare_artifact(
        vault=Vault.load(tmp_path),
        run_id="run-4",
        candidate_set=CandidateSet(run_id="run-4", stage="weave", generated_at="2026-04-21T00:00:00Z", mode="shadow"),
        local_proposals=[
            WeaveLocalProposalArtifact(
                window_id="window-001-alpha",
                seed_atom_id="alpha",
                clusters=[],
                leftover_atom_ids=[],
                bridge_candidates=[],
                window_observations=[],
            )
        ],
        reconciled_clusters=[
            ReconciledCluster(
                cluster_id="window-001-alpha-cluster-01-alpha-beta",
                source_cluster_ids=["window-001-alpha-cluster-01-alpha-beta"],
                cluster_title="Alpha cluster",
                cluster_thesis="Alpha thesis",
                member_atom_ids=["alpha", "beta"],
                confidence=0.8,
                rationale="relation",
                why_now="now",
            )
        ],
        critique=WeaveCritiqueArtifact(approved_cluster_ids=["window-001-alpha-cluster-01-alpha-beta"]),
        reports=WeaveClusterReportsArtifact(reports=[]),
    )

    assert compare.baseline_available is True
    assert compare.v1_cluster_count == 1
    assert compare.v1_largest_cluster_size == 3
    assert compare.v2_reconciled_cluster_count == 1
