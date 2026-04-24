from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Callable

from pydantic import BaseModel

from .contracts import (
    CandidateSet,
    NeighborhoodWindow,
    ReconciledCluster,
    WeaveClusterReportsArtifact,
    WeaveCritiqueArtifact,
    WeaveLocalProposalResponse,
    WeaveReconcileArtifact,
    WeaveStructuralActionsArtifact,
)

WEAVE_LOCAL_CLUSTER_PROMPT_VERSION = "dream.weave.local-cluster.v2"
WEAVE_RECONCILE_PROMPT_VERSION = "dream.weave.reconcile.v2"
WEAVE_CRITIQUE_PROMPT_VERSION = "dream.weave.critique.v2"
WEAVE_REPORT_WRITER_PROMPT_VERSION = "dream.weave.report-writer.v2"
WEAVE_STRUCTURAL_ACTIONS_PROMPT_VERSION = "dream.weave.structural-actions.v2"


@dataclass(frozen=True)
class PromptSpec:
    family: str
    task_class: str
    prompt_version: str
    response_model: type[BaseModel]
    render: Callable[..., str]


def get_prompt_spec(family: str) -> PromptSpec:
    try:
        return _PROMPT_REGISTRY[family]
    except KeyError as exc:
        raise KeyError(f"unknown Dream v2 prompt family: {family}") from exc


def _render_weave_local_cluster_prompt(
    *,
    window: NeighborhoodWindow,
    candidate_set: CandidateSet,
    max_clusters: int,
) -> str:
    snapshots_by_id = {snapshot.atom_id: snapshot for snapshot in candidate_set.atom_snapshots}
    atoms = [_atom_prompt_payload(snapshots_by_id[atom_id]) for atom_id in window.atom_ids if atom_id in snapshots_by_id]
    prompt_payload = {
        "window_id": window.window_id,
        "seed_atom_id": window.seed_atom_id,
        "allowed_atom_ids": list(window.atom_ids),
        "max_clusters": max_clusters,
        "rules": [
            "Use only the provided atom ids and evidence.",
            "Every atom id in member_atom_ids, member_roles, borderline_atom_ids, excluded_atom_ids, leftover_atom_ids, and bridge candidates must come from allowed_atom_ids.",
            "If a relevant atom is missing from allowed_atom_ids, omit it instead of inventing or paraphrasing it.",
            "Prefer exclusion over over-clustering.",
            "Cite atom ids explicitly in member lists and role entries.",
            "Do not invent atom ids, bridge ids, or structural claims.",
            "Return JSON only.",
        ],
        "atoms": atoms,
    }
    return (
        "You are running Brain Dream v2 weave.local_cluster.\n"
        "Inspect this bounded neighborhood and propose 0-3 coherent clusters.\n"
        "A cluster should have a real thesis, explicit boundaries, and conservative membership.\n"
        "Leave ambiguous atoms in leftover_atom_ids instead of forcing a join.\n\n"
        f"Prompt payload:\n{json.dumps(prompt_payload, indent=2, sort_keys=True, ensure_ascii=True)}"
    )


def _render_weave_reconcile_prompt(
    *,
    candidate_set: CandidateSet,
    local_proposals: list[WeaveLocalProposalArtifact],
) -> str:
    prompt_payload = {
        "rules": [
            "Use only provided cluster ids and atom ids.",
            "Prefer keeping distinct clusters separate when evidence is weak.",
            "Do not invent atom ids or cluster ids.",
            "Return JSON only.",
        ],
        "candidate_count": len(candidate_set.atom_snapshots),
        "local_proposals": [proposal.model_dump(mode="json") for proposal in local_proposals],
    }
    return (
        "You are running Brain Dream v2 weave.reconcile.\n"
        "Merge or keep apart overlapping local cluster proposals conservatively.\n\n"
        f"Prompt payload:\n{json.dumps(prompt_payload, indent=2, sort_keys=True, ensure_ascii=True)}"
    )


def _render_weave_critique_prompt(
    *,
    candidate_set: CandidateSet,
    reconciled: WeaveReconcileArtifact,
) -> str:
    prompt_payload = {
        "rules": [
            "Look for giant umbrella clusters, false joins, and missing parent concepts.",
            "Approve only clusters that are coherent and bounded.",
            "Return JSON only.",
        ],
        "candidate_count": len(candidate_set.atom_snapshots),
        "reconciled_clusters": reconciled.model_dump(mode="json"),
    }
    return (
        "You are running Brain Dream v2 weave.critique.\n"
        "Challenge the reconciled cluster map and surface boundary problems.\n\n"
        f"Prompt payload:\n{json.dumps(prompt_payload, indent=2, sort_keys=True, ensure_ascii=True)}"
    )


def _render_weave_report_writer_prompt(
    *,
    candidate_set: CandidateSet,
    reconciled_clusters: list[ReconciledCluster],
    critique: WeaveCritiqueArtifact,
) -> str:
    approved_ids = set(critique.approved_cluster_ids)
    prompt_payload = {
        "rules": [
            "Write structured report payloads only for approved clusters.",
            "Cite atom ids explicitly.",
            "Do not invent evidence anchors beyond the provided snapshots.",
            "Return JSON only.",
        ],
        "clusters": [
            cluster.model_dump(mode="json")
            for cluster in reconciled_clusters
            if cluster.cluster_id in approved_ids
        ],
        "candidate_atoms": [_atom_prompt_payload(snapshot) for snapshot in candidate_set.atom_snapshots],
        "critique": critique.model_dump(mode="json"),
    }
    return (
        "You are running Brain Dream v2 weave.report_writer.\n"
        "Turn approved reconciled clusters into structured report payloads.\n\n"
        f"Prompt payload:\n{json.dumps(prompt_payload, indent=2, sort_keys=True, ensure_ascii=True)}"
    )


def _render_weave_structural_actions_prompt(
    *,
    reports: WeaveClusterReportsArtifact,
    critique: WeaveCritiqueArtifact,
) -> str:
    prompt_payload = {
        "rules": [
            "Separate safe cluster-ref updates from report-only merge/split suggestions.",
            "Emit review nudges when critique flags require human review.",
            "Return JSON only.",
        ],
        "reports": reports.model_dump(mode="json"),
        "critique": critique.model_dump(mode="json"),
    }
    return (
        "You are running Brain Dream v2 weave.structural_actions.\n"
        "Classify structural actions into safe updates versus report-only outputs.\n\n"
        f"Prompt payload:\n{json.dumps(prompt_payload, indent=2, sort_keys=True, ensure_ascii=True)}"
    )


def _atom_prompt_payload(snapshot) -> dict[str, object]:
    return {
        "atom_id": snapshot.atom_id,
        "atom_type": snapshot.atom_type,
        "title": snapshot.title,
        "tldr": snapshot.tldr,
        "generic_relation_ids": snapshot.generic_relation_ids,
        "typed_relation_ids": snapshot.typed_relation_ids,
        "evidence_source_ids": [ref.source_id for ref in snapshot.evidence_refs[:6]],
        "prior_cluster_refs": snapshot.prior_cluster_refs,
        "life_mentions": snapshot.life_mentions,
        "changed_since_last_weave": snapshot.changed_since_last_weave,
        "hotness_features": snapshot.hotness_features.model_dump(mode="json"),
    }


_PROMPT_REGISTRY: dict[str, PromptSpec] = {
    "weave.local_cluster": PromptSpec(
        family="weave.local_cluster",
        task_class="dream_decision",
        prompt_version=WEAVE_LOCAL_CLUSTER_PROMPT_VERSION,
        response_model=WeaveLocalProposalResponse,
        render=_render_weave_local_cluster_prompt,
    ),
    "weave.reconcile": PromptSpec(
        family="weave.reconcile",
        task_class="dream_decision",
        prompt_version=WEAVE_RECONCILE_PROMPT_VERSION,
        response_model=WeaveReconcileArtifact,
        render=_render_weave_reconcile_prompt,
    ),
    "weave.critique": PromptSpec(
        family="weave.critique",
        task_class="dream_decision",
        prompt_version=WEAVE_CRITIQUE_PROMPT_VERSION,
        response_model=WeaveCritiqueArtifact,
        render=_render_weave_critique_prompt,
    ),
    "weave.report_writer": PromptSpec(
        family="weave.report_writer",
        task_class="dream_writer",
        prompt_version=WEAVE_REPORT_WRITER_PROMPT_VERSION,
        response_model=WeaveClusterReportsArtifact,
        render=_render_weave_report_writer_prompt,
    ),
    "weave.structural_actions": PromptSpec(
        family="weave.structural_actions",
        task_class="dream_decision",
        prompt_version=WEAVE_STRUCTURAL_ACTIONS_PROMPT_VERSION,
        response_model=WeaveStructuralActionsArtifact,
        render=_render_weave_structural_actions_prompt,
    ),
}
