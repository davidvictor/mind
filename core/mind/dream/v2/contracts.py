from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


DreamStage = Literal["light", "deep", "rem"]


class DreamV2Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


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
    stage: DreamStage
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
    stage: DreamStage
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
    stage: DreamStage
    mode: Literal["shadow", "write"]
    entries: list[ApplyManifestEntry] = Field(default_factory=list)
    write_count: int = 0
    warning_count: int = 0
    notes: list[str] = Field(default_factory=list)


class StageRunSummary(DreamV2Model):
    stage: DreamStage
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
