from __future__ import annotations

from mind.dream.common import read_page
from scripts.common.vault import Vault

from .contracts import (
    CandidateSet,
    ReconciledCluster,
    WeaveClusterReportsArtifact,
    WeaveCompareArtifact,
    WeaveCritiqueArtifact,
    WeaveLocalProposalArtifact,
)


def build_weave_compare_artifact(
    *,
    vault: Vault,
    run_id: str,
    candidate_set: CandidateSet,
    local_proposals: list[WeaveLocalProposalArtifact],
    reconciled_clusters: list[ReconciledCluster],
    critique: WeaveCritiqueArtifact,
    reports: WeaveClusterReportsArtifact,
) -> WeaveCompareArtifact:
    v1_pages = _load_v1_weave_pages(vault)
    return WeaveCompareArtifact(
        run_id=run_id,
        baseline_available=bool(v1_pages),
        baseline_source="memory/dreams/weave" if v1_pages else "unavailable",
        v1_cluster_count=len(v1_pages),
        v2_local_cluster_count=sum(len(proposal.clusters) for proposal in local_proposals),
        v2_reconciled_cluster_count=len(reconciled_clusters),
        v1_largest_cluster_size=max((page["member_count"] for page in v1_pages), default=0),
        v2_largest_cluster_size=max((len(cluster.member_atom_ids) for cluster in reconciled_clusters), default=0),
        explicit_exclusion_count=sum(len(cluster.excluded_atom_ids) for cluster in reconciled_clusters),
        bridge_candidate_count=sum(len(cluster.bridge_candidate_ids) for cluster in reconciled_clusters),
        parent_concept_candidate_count=len(critique.parent_concept_candidates),
        notes=[
            f"candidate atoms={len(candidate_set.atom_snapshots)}",
            f"report count={len(reports.reports)}",
        ],
    )


def _load_v1_weave_pages(vault: Vault) -> list[dict[str, object]]:
    root = vault.wiki / "dreams" / "weave"
    if not root.exists():
        return []
    pages: list[dict[str, object]] = []
    for path in sorted(root.glob("weave-*.md")):
        frontmatter, _body = read_page(path)
        member_ids = frontmatter.get("member_atom_ids") or []
        pages.append(
            {
                "path": path,
                "cluster_id": path.stem,
                "member_count": len(member_ids) if isinstance(member_ids, list) else 0,
            }
        )
    return pages
