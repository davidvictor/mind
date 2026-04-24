from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class DreamV2Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class HotnessFeatures(DreamV2Model):
    relation_degree: int = 0
    recent_evidence_count: int = 0
    evidence_count: int = 0
    life_mentions: int = 0
    rem_carryover_bonus: int = 0
    hot_score: int = 0


class SourceEvidenceRef(DreamV2Model):
    source_id: str
    observed_at: str
    snippet: str = ""


class AtomSnapshot(DreamV2Model):
    atom_id: str
    atom_type: str
    path: str
    title: str
    frontmatter: dict[str, Any] = Field(default_factory=dict)
    tldr: str
    evidence_refs: list[SourceEvidenceRef] = Field(default_factory=list)
    generic_relation_ids: list[str] = Field(default_factory=list)
    typed_relation_ids: list[str] = Field(default_factory=list)
    lifecycle_state: str = "active"
    last_updated: str = ""
    last_dream_pass: str | None = None
    life_mentions: int = 0
    prior_cluster_refs: list[str] = Field(default_factory=list)
    hotness_features: HotnessFeatures = Field(default_factory=HotnessFeatures)
    embedding_ref: str | None = None
    changed_since_last_weave: bool = False

    @property
    def relation_ids(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys([*self.generic_relation_ids, *self.typed_relation_ids]))


class NeighborhoodWindow(DreamV2Model):
    window_id: str
    seed_atom_id: str
    atom_ids: list[str] = Field(default_factory=list)
    ranked_atom_ids: list[str] = Field(default_factory=list)
    rationale: list[str] = Field(default_factory=list)


class CandidateSet(DreamV2Model):
    run_id: str
    stage: Literal["light", "deep", "weave", "rem"]
    generated_at: str
    mode: Literal["shadow", "write"]
    atom_snapshots: list[AtomSnapshot] = Field(default_factory=list)
    windows: list[NeighborhoodWindow] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class WeaveClusterMember(DreamV2Model):
    cluster_id: str = ""
    atom_id: str
    role: Literal["hub", "core", "bridge", "boundary", "counterpoint"]
    why_included: str
    primary_signals: list[str] = Field(default_factory=list)


class WeaveBridgeCandidate(DreamV2Model):
    bridge_id: str = ""
    source_atom_id: str
    target_atom_id: str
    bridge_type: str
    why_it_matters: str
    confidence: float = Field(ge=0.0, le=1.0)


class WeaveClusterCandidate(DreamV2Model):
    cluster_id: str = ""
    source_window_id: str = ""
    cluster_title: str
    cluster_thesis: str
    member_atom_ids: list[str] = Field(default_factory=list)
    member_roles: list[WeaveClusterMember] = Field(default_factory=list)
    borderline_atom_ids: list[str] = Field(default_factory=list)
    excluded_atom_ids: list[str] = Field(default_factory=list)
    bridge_candidate_ids: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str = ""
    why_now: str


class WeavePromptClusterMember(DreamV2Model):
    atom_id: str
    role: Literal["hub", "core", "bridge", "boundary", "counterpoint"]
    why_included: str
    primary_signals: list[str] = Field(default_factory=list)


class WeavePromptBridgeCandidate(DreamV2Model):
    source_atom_id: str
    target_atom_id: str
    bridge_type: str
    why_it_matters: str
    confidence: float = Field(ge=0.0, le=1.0)


class WeavePromptClusterCandidate(DreamV2Model):
    cluster_title: str
    cluster_thesis: str
    member_atom_ids: list[str] = Field(default_factory=list)
    member_roles: list[WeavePromptClusterMember] = Field(default_factory=list)
    borderline_atom_ids: list[str] = Field(default_factory=list)
    excluded_atom_ids: list[str] = Field(default_factory=list)
    bridge_candidate_ids: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str = ""
    why_now: str


class WeaveLocalProposalResponse(DreamV2Model):
    clusters: list[WeavePromptClusterCandidate] = Field(default_factory=list)
    leftover_atom_ids: list[str] = Field(default_factory=list)
    bridge_candidates: list[WeavePromptBridgeCandidate] = Field(default_factory=list)
    window_observations: list[str] = Field(default_factory=list)


class WeaveLocalProposalArtifact(DreamV2Model):
    window_id: str
    seed_atom_id: str
    clusters: list[WeaveClusterCandidate] = Field(default_factory=list)
    leftover_atom_ids: list[str] = Field(default_factory=list)
    bridge_candidates: list[WeaveBridgeCandidate] = Field(default_factory=list)
    window_observations: list[str] = Field(default_factory=list)


class ReconciledCluster(DreamV2Model):
    cluster_id: str
    source_cluster_ids: list[str] = Field(default_factory=list)
    cluster_title: str
    cluster_thesis: str
    member_atom_ids: list[str] = Field(default_factory=list)
    member_roles: list[WeaveClusterMember] = Field(default_factory=list)
    borderline_atom_ids: list[str] = Field(default_factory=list)
    excluded_atom_ids: list[str] = Field(default_factory=list)
    bridge_candidate_ids: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str
    why_now: str


class ReconcileSplitInstruction(DreamV2Model):
    cluster_id: str
    target_cluster_ids: list[str] = Field(default_factory=list)
    reason: str


class HierarchyEdge(DreamV2Model):
    parent_cluster_id: str
    child_cluster_id: str
    relation_type: str = "parent"


class WeaveReconcileArtifact(DreamV2Model):
    merged_clusters: list[ReconciledCluster] = Field(default_factory=list)
    discarded_cluster_ids: list[str] = Field(default_factory=list)
    split_instructions: list[ReconcileSplitInstruction] = Field(default_factory=list)
    hierarchy_edges: list[HierarchyEdge] = Field(default_factory=list)
    global_observations: list[str] = Field(default_factory=list)


class ClusterBoundaryTrim(DreamV2Model):
    cluster_id: str
    atom_ids: list[str] = Field(default_factory=list)
    reason: str


class ParentConceptCandidate(DreamV2Model):
    candidate_id: str
    title: str
    supporting_cluster_ids: list[str] = Field(default_factory=list)
    rationale: str
    confidence: float = Field(ge=0.0, le=1.0)


class ReviewFlag(DreamV2Model):
    cluster_id: str
    flag_type: str
    summary: str


class WeaveCritiqueArtifact(DreamV2Model):
    approved_cluster_ids: list[str] = Field(default_factory=list)
    clusters_requiring_split: list[str] = Field(default_factory=list)
    clusters_requiring_boundary_trim: list[ClusterBoundaryTrim] = Field(default_factory=list)
    parent_concept_candidates: list[ParentConceptCandidate] = Field(default_factory=list)
    review_flags: list[ReviewFlag] = Field(default_factory=list)


class ClusterMemberSection(DreamV2Model):
    atom_id: str
    role: str
    summary: str


class BridgeSection(DreamV2Model):
    bridge_id: str
    summary: str


class TensionSection(DreamV2Model):
    summary: str
    atom_ids: list[str] = Field(default_factory=list)


class WeaveClusterReport(DreamV2Model):
    cluster_id: str
    title: str
    thesis: str
    why_now: str
    member_sections: list[ClusterMemberSection] = Field(default_factory=list)
    bridge_sections: list[BridgeSection] = Field(default_factory=list)
    tension_sections: list[TensionSection] = Field(default_factory=list)
    parent_concept_candidates: list[str] = Field(default_factory=list)
    evidence_anchors: list[str] = Field(default_factory=list)


class WeaveClusterReportsArtifact(DreamV2Model):
    reports: list[WeaveClusterReport] = Field(default_factory=list)


class SafeClusterRefUpdate(DreamV2Model):
    cluster_id: str
    atom_ids: list[str] = Field(default_factory=list)
    cluster_ref: str


class ReportOnlyMerge(DreamV2Model):
    source_cluster_id: str
    target_cluster_id: str
    reason: str


class ReportOnlySplit(DreamV2Model):
    cluster_id: str
    reason: str


class ReviewNudge(DreamV2Model):
    nudge_id: str
    title: str
    body: str
    target_path: str


class WeaveStructuralActionsArtifact(DreamV2Model):
    safe_cluster_ref_updates: list[SafeClusterRefUpdate] = Field(default_factory=list)
    report_only_merges: list[ReportOnlyMerge] = Field(default_factory=list)
    report_only_splits: list[ReportOnlySplit] = Field(default_factory=list)
    review_nudges: list[ReviewNudge] = Field(default_factory=list)


class PromptReceipt(DreamV2Model):
    prompt_family: str
    prompt_version: str
    task_class: str
    provider: str
    model: str
    input_mode: str
    request_fingerprint: dict[str, Any] = Field(default_factory=dict)
    request_metadata: dict[str, Any] = Field(default_factory=dict)
    response_metadata: dict[str, Any] = Field(default_factory=dict)
    repaired: bool = False


class DecisionArtifactEnvelope(DreamV2Model):
    stage: Literal["light", "deep", "weave", "rem"]
    artifact_name: str
    prompt_receipt: PromptReceipt
    payload: dict[str, Any] = Field(default_factory=dict)


class ApplyPlanAction(DreamV2Model):
    action_id: str
    action_type: Literal["write_markdown", "update_frontmatter", "emit_nudge", "skip"]
    target_path: str
    safe_to_apply: bool = False
    rationale: str = ""
    atom_ids: list[str] = Field(default_factory=list)


class ApplyPlan(DreamV2Model):
    run_id: str
    stage: Literal["light", "deep", "weave", "rem"]
    mode: Literal["shadow", "write"]
    actions: list[ApplyPlanAction] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class ApplyManifestEntry(DreamV2Model):
    action_id: str
    status: Literal["written", "skipped", "blocked"]
    target_path: str
    notes: list[str] = Field(default_factory=list)


class ApplyManifest(DreamV2Model):
    run_id: str
    stage: Literal["light", "deep", "weave", "rem"]
    mode: Literal["shadow", "write"]
    entries: list[ApplyManifestEntry] = Field(default_factory=list)
    write_count: int = 0
    warning_count: int = 0
    notes: list[str] = Field(default_factory=list)


class WeaveCompareArtifact(DreamV2Model):
    run_id: str
    baseline_available: bool
    baseline_source: str
    v1_cluster_count: int = 0
    v2_local_cluster_count: int = 0
    v2_reconciled_cluster_count: int = 0
    v1_largest_cluster_size: int = 0
    v2_largest_cluster_size: int = 0
    explicit_exclusion_count: int = 0
    bridge_candidate_count: int = 0
    parent_concept_candidate_count: int = 0
    notes: list[str] = Field(default_factory=list)


class StageRunSummary(DreamV2Model):
    stage: Literal["light", "deep", "weave", "rem"]
    status: Literal["pending", "completed", "failed"]
    candidate_count: int = 0
    decision_artifact_count: int = 0
    write_count: int = 0
    warning_count: int = 0


class DreamRunManifest(DreamV2Model):
    run_id: str
    started_at: str
    completed_at: str | None = None
    mode: Literal["shadow", "write"]
    engine_version: Literal["v2"] = "v2"
    shadow_mode: bool = True
    config_snapshot: dict[str, Any] = Field(default_factory=dict)
    artifact_root: str
    stages: list[StageRunSummary] = Field(default_factory=list)
