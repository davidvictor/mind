from __future__ import annotations

from pathlib import PurePosixPath
import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


DreamStage = Literal["light", "deep", "rem", "kene"]
KENE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")


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

    @field_validator("action_id")
    @classmethod
    def _validate_action_id(cls, value: str) -> str:
        return _validate_kene_id(value, field_name="action_id")

    @field_validator("target_path")
    @classmethod
    def _validate_target_path(cls, value: str) -> str:
        return _validate_relative_path(value, field_name="target_path")

    @field_validator("atom_ids")
    @classmethod
    def _validate_action_atom_ids(cls, value: list[str]) -> list[str]:
        return [_validate_kene_id(item, field_name="atom_id") for item in value]


class ApplyPlan(DreamV2Model):
    run_id: str
    stage: DreamStage
    mode: Literal["shadow", "write"]
    actions: list[ApplyPlanAction] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_write_mode_actions(self) -> "ApplyPlan":
        if self.mode != "write":
            return self
        unsafe = [
            action.action_id
            for action in self.actions
            if action.action_type != "skip" and not action.safe_to_apply
        ]
        if unsafe:
            raise ValueError(f"unsafe apply actions in write mode: {', '.join(unsafe)}")
        return self


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


class KeneArtifactReference(DreamV2Model):
    stage: Literal["light", "deep", "rem"]
    artifact_name: str
    artifact_path: str

    @field_validator("artifact_path")
    @classmethod
    def _validate_artifact_path(cls, value: str) -> str:
        return _validate_relative_path(value, field_name="artifact_path")


class KeneAtomSnapshot(DreamV2Model):
    atom_id: str
    atom_type: Literal["concept", "playbook", "stance", "inquiry"]
    title: str
    path: str
    lifecycle_state: str
    domains: list[str] = Field(default_factory=list)
    relation_ids: list[str] = Field(default_factory=list)
    evidence_count: int = 0
    last_evidence_date: str = ""

    @field_validator("atom_id")
    @classmethod
    def _validate_atom_id(cls, value: str) -> str:
        return _validate_kene_id(value, field_name="atom_id")

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        return _validate_relative_path(value, field_name="path")

    @field_validator("relation_ids")
    @classmethod
    def _validate_relation_ids(cls, value: list[str]) -> list[str]:
        return [_validate_kene_id(item, field_name="relation_id") for item in value]


class KeneInputBundle(DreamV2Model):
    run_id: str
    stage: Literal["kene"] = "kene"
    generated_at: str
    mode: Literal["shadow"] = "shadow"
    prior_stages: list[Literal["light", "deep", "rem"]] = Field(default_factory=lambda: ["light", "deep", "rem"])
    atoms: list[KeneAtomSnapshot] = Field(default_factory=list)
    source_ids: list[str] = Field(default_factory=list)
    prior_artifacts: list[KeneArtifactReference] = Field(default_factory=list)

    @field_validator("source_ids")
    @classmethod
    def _validate_source_ids(cls, value: list[str]) -> list[str]:
        return [_validate_kene_id(item, field_name="source_id") for item in value]


class KenePriorOutputMap(DreamV2Model):
    run_id: str
    stage: Literal["kene"] = "kene"
    outputs_by_stage: dict[Literal["light", "deep", "rem"], list[KeneArtifactReference]] = Field(default_factory=dict)


class KeneGroup(DreamV2Model):
    group_id: str
    title: str
    member_atom_ids: list[str] = Field(default_factory=list)
    rationale: str = ""
    provenance: list[str] = Field(default_factory=list)

    @field_validator("group_id")
    @classmethod
    def _validate_group_id(cls, value: str) -> str:
        return _validate_kene_id(value, field_name="group_id")

    @field_validator("member_atom_ids")
    @classmethod
    def _validate_member_atom_ids(cls, value: list[str]) -> list[str]:
        return [_validate_kene_id(item, field_name="member_atom_id") for item in value]


class KeneMove(DreamV2Model):
    move_id: str
    action: Literal["move", "merge", "split", "rename", "reframe", "reorder", "review_only"]
    atom_ids: list[str] = Field(default_factory=list)
    from_group_id: str | None = None
    to_group_id: str | None = None
    target_path: str | None = None
    rationale: str = ""
    provenance: list[str] = Field(default_factory=list)

    @field_validator("move_id")
    @classmethod
    def _validate_move_id(cls, value: str) -> str:
        return _validate_kene_id(value, field_name="move_id")

    @field_validator("atom_ids")
    @classmethod
    def _validate_move_atom_ids(cls, value: list[str]) -> list[str]:
        return [_validate_kene_id(item, field_name="atom_id") for item in value]

    @field_validator("from_group_id", "to_group_id")
    @classmethod
    def _validate_optional_group_id(cls, value: str | None) -> str | None:
        if value is None:
            return value
        return _validate_kene_id(value, field_name="group_id")

    @field_validator("target_path")
    @classmethod
    def _validate_optional_target_path(cls, value: str | None) -> str | None:
        if value is None:
            return value
        return _validate_relative_path(value, field_name="target_path")


class KeneArrangementPlan(DreamV2Model):
    run_id: str
    stage: Literal["kene"] = "kene"
    mode: Literal["shadow"] = "shadow"
    groups: list[KeneGroup] = Field(default_factory=list)
    moves: list[KeneMove] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class KeneRelationChange(DreamV2Model):
    change_id: str
    action: Literal["add", "weaken", "remove", "review_only"]
    source_atom_id: str
    target_atom_id: str
    relation_type: str = "adjacent_to"
    review_only: bool = True
    rationale: str = ""
    provenance: list[str] = Field(default_factory=list)

    @field_validator("change_id")
    @classmethod
    def _validate_change_id(cls, value: str) -> str:
        return _validate_kene_id(value, field_name="change_id")

    @field_validator("source_atom_id", "target_atom_id")
    @classmethod
    def _validate_relation_atom_id(cls, value: str) -> str:
        return _validate_kene_id(value, field_name="atom_id")

    @model_validator(mode="after")
    def _validate_relation_change(self) -> "KeneRelationChange":
        if self.action != "review_only" and self.review_only:
            raise ValueError("non-review-only relation changes must clear review_only")
        return self


class KeneRelationDiff(DreamV2Model):
    run_id: str
    stage: Literal["kene"] = "kene"
    mode: Literal["shadow"] = "shadow"
    changes: list[KeneRelationChange] = Field(default_factory=list)
    blocked_write_count: int = 0
    warnings: list[str] = Field(default_factory=list)


class KeneCritique(DreamV2Model):
    run_id: str
    stage: Literal["kene"] = "kene"
    findings: list[str] = Field(default_factory=list)
    blocked_reasons: list[str] = Field(default_factory=list)


class KeneRenderPackage(DreamV2Model):
    run_id: str
    stage: Literal["kene"] = "kene"
    markdown_target_path: str
    title: str
    sections: list[dict[str, Any]] = Field(default_factory=list)

    @field_validator("markdown_target_path")
    @classmethod
    def _validate_markdown_target_path(cls, value: str) -> str:
        return _validate_relative_path(value, field_name="markdown_target_path")


def _validate_kene_id(value: str, *, field_name: str) -> str:
    cleaned = str(value).strip()
    if not cleaned or not KENE_ID_RE.match(cleaned):
        raise ValueError(f"{field_name} must use lowercase ASCII slug syntax")
    return cleaned


def _validate_relative_path(value: str, *, field_name: str) -> str:
    cleaned = str(value).strip()
    path = PurePosixPath(cleaned)
    if not cleaned or path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{field_name} must be a relative logical path")
    return cleaned
