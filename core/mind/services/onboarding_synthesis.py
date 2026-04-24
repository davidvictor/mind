from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import mimetypes
from pathlib import Path
from typing import Any, Callable, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
import yaml

from mind.services.embedding_service import get_embedding_service
from mind.services.graph_registry import GraphRegistry, ResolutionCandidate
from mind.services.llm_rate_limiter import normalize_concurrency
from mind.services.llm_repair import compact_validation_errors, repair_once
from mind.services.llm_schema import prepare_strict_schema
from mind.services.llm_service import get_llm_service
from mind.services.llm_cache import LLMCacheIdentity
from mind.services.onboarding_chunker import (
    CHUNK_SIZE,
    assemble_graph_chunks,
    assemble_merge_chunks,
    chunk_graph_entities,
    chunk_merge_nodes,
    kept_nodes_for_relationships,
    relationship_edges_for_kept_nodes,
)
from mind.services.onboarding_plan_builder import build_materialization_plan
from mind.services.onboarding_state import (
    ChunkPhase,
    ChunkState,
    acquire_chunk_lease,
    ensure_chunk_state,
    iter_runnable_states,
    load_chunk_states,
    load_chunk_result,
    mark_chunk_done,
    mark_chunk_failed,
    next_retry_not_before,
    prune_chunk_phase,
    summarize_chunk_phase,
)
from mind.services.llm_routing import resolve_route
from mind.services.prompt_builders import (
    ONBOARDING_GRAPH_CHUNK_PROMPT_VERSION,
    ONBOARDING_GRAPH_PROMPT_VERSION,
    ONBOARDING_MERGE_CHUNK_PROMPT_VERSION,
    ONBOARDING_MERGE_RELATIONSHIPS_PROMPT_VERSION,
    ONBOARDING_MERGE_PROMPT_VERSION,
    ONBOARDING_SYNTHESIS_PROMPT_VERSION,
    ONBOARDING_VERIFY_PROMPT_VERSION,
    build_onboarding_graph_chunk_prompt,
    build_onboarding_graph_prompt,
    build_onboarding_merge_chunk_prompt,
    build_onboarding_merge_prompt,
    build_onboarding_merge_relationships_prompt,
    build_onboarding_synthesis_instructions,
    build_onboarding_verify_prompt,
)
from mind.services.vector_index import select_vector_backend
from scripts.common.default_tags import default_tags
from scripts.common.section_rewriter import (
    ParsedMarkdownBody,
    SectionOperation,
    apply_section_operations,
    parse_markdown_body,
)
from scripts.common.slugify import slugify
from scripts.common.vault import Vault
from scripts.common.wiki_writer import write_page


ARTIFACT_FILE_NAMES = {
    "semantic": "synthesis-semantic.json",
    "graph": "synthesis-graph.json",
    "candidate_context": "merge-candidate-context.json",
    "merge": "merge-decisions.json",
    "verify": "verify-report.json",
    "materialization_plan": "materialization-plan.json",
}
MARKDOWN_FILE_NAMES = {
    "semantic": "synthesis-semantic.md",
    "graph": "synthesis-graph.md",
    "candidate_context": "merge-candidate-context.md",
    "merge": "merge-decisions.md",
    "verify": "verify-report.md",
    "materialization_plan": "materialization-plan.md",
}
PATCH_REVIEW_DIR = "patch-reviews"
TEXT_MIME_PREFIXES = ("text/",)
TEXT_MIME_TYPES = {
    "application/json",
    "application/ld+json",
    "application/x-ndjson",
    "application/x-yaml",
    "application/yaml",
}
CANONICAL_PAGE_DIRS = {
    "person": "people",
    "project": "projects",
    "concept": "concepts",
    "playbook": "playbooks",
    "stance": "stances",
    "inquiry": "inquiries",
    "company": "companies",
    "channel": "channels",
    "source": "sources",
}
SUMMARY_KINDS = {"overview", "profile", "values", "positioning", "open-inquiries"}
FIXED_TARGET_KINDS = {
    "owner_profile",
    "owner_values",
    "owner_positioning",
    "owner_open_inquiries",
    "owner_person",
    "decision",
}


@dataclass(frozen=True)
class SectionRule:
    heading: str
    allowed_modes: tuple[str, ...]


@dataclass(frozen=True)
class PagePatchSchema:
    page_type: str
    intro_editable: bool
    section_order: tuple[str, ...]
    section_rules: dict[str, SectionRule]


PATCH_SCHEMAS: dict[str, PagePatchSchema] = {
    "profile": PagePatchSchema(
        page_type="profile",
        intro_editable=True,
        section_order=("## Snapshot",),
        section_rules={
            "## Snapshot": SectionRule("## Snapshot", ("replace", "append", "union", "preserve")),
        },
    ),
    "note": PagePatchSchema(
        page_type="note",
        intro_editable=True,
        section_order=(
            "## Operating Principles",
            "## Positioning Narrative",
            "## Work Priorities",
            "## Life Priorities",
            "## Constraints",
            "## Active Inquiries",
        ),
        section_rules={
            "## Operating Principles": SectionRule("## Operating Principles", ("replace", "append", "union", "preserve")),
            "## Positioning Narrative": SectionRule("## Positioning Narrative", ("replace", "append", "preserve")),
            "## Work Priorities": SectionRule("## Work Priorities", ("replace", "append", "union", "preserve")),
            "## Life Priorities": SectionRule("## Life Priorities", ("replace", "append", "union", "preserve")),
            "## Constraints": SectionRule("## Constraints", ("replace", "append", "union", "preserve")),
            "## Active Inquiries": SectionRule("## Active Inquiries", ("replace", "append", "union", "preserve")),
        },
    ),
    "person": PagePatchSchema(
        page_type="person",
        intro_editable=True,
        section_order=("## Snapshot", "## Relationships", "## Notes"),
        section_rules={
            "## Snapshot": SectionRule("## Snapshot", ("replace", "append", "union", "preserve")),
            "## Relationships": SectionRule("## Relationships", ("replace", "append", "union", "preserve")),
            "## Notes": SectionRule("## Notes", ("replace", "append", "preserve")),
        },
    ),
    "project": PagePatchSchema(
        page_type="project",
        intro_editable=True,
        section_order=("## Project Priorities", "## Constraints", "## Notes"),
        section_rules={
            "## Project Priorities": SectionRule("## Project Priorities", ("replace", "append", "union", "preserve")),
            "## Constraints": SectionRule("## Constraints", ("replace", "append", "union", "preserve")),
            "## Notes": SectionRule("## Notes", ("replace", "append", "preserve")),
        },
    ),
    "concept": PagePatchSchema(
        page_type="concept",
        intro_editable=True,
        section_order=("## TL;DR", "## Why It Matters", "## Mechanism", "## Examples", "## In Conversation With", "## Evidence log"),
        section_rules={
            "## TL;DR": SectionRule("## TL;DR", ("replace", "append", "preserve")),
            "## Why It Matters": SectionRule("## Why It Matters", ("replace", "append", "preserve")),
            "## Mechanism": SectionRule("## Mechanism", ("replace", "append", "preserve")),
            "## Examples": SectionRule("## Examples", ("replace", "append", "union", "preserve")),
            "## In Conversation With": SectionRule("## In Conversation With", ("replace", "append", "union", "preserve")),
            "## Evidence log": SectionRule("## Evidence log", ("append", "union", "preserve")),
        },
    ),
    "playbook": PagePatchSchema(
        page_type="playbook",
        intro_editable=True,
        section_order=("## TL;DR", "## When To Use", "## Prerequisites", "## Steps", "## Failure Modes", "## Evidence log"),
        section_rules={
            "## TL;DR": SectionRule("## TL;DR", ("replace", "append", "preserve")),
            "## When To Use": SectionRule("## When To Use", ("replace", "append", "preserve")),
            "## Prerequisites": SectionRule("## Prerequisites", ("replace", "append", "union", "preserve")),
            "## Steps": SectionRule("## Steps", ("replace", "append", "union", "preserve")),
            "## Failure Modes": SectionRule("## Failure Modes", ("replace", "append", "union", "preserve")),
            "## Evidence log": SectionRule("## Evidence log", ("append", "union", "preserve")),
        },
    ),
    "stance": PagePatchSchema(
        page_type="stance",
        intro_editable=True,
        section_order=("## TL;DR", "## Position", "## Why", "## Best Evidence For", "## Strongest Counterevidence", "## What Would Change My Mind", "## Evidence log", "## Contradictions"),
        section_rules={
            "## TL;DR": SectionRule("## TL;DR", ("replace", "append", "preserve")),
            "## Position": SectionRule("## Position", ("replace", "append", "preserve")),
            "## Why": SectionRule("## Why", ("replace", "append", "preserve")),
            "## Best Evidence For": SectionRule("## Best Evidence For", ("replace", "append", "union", "preserve")),
            "## Strongest Counterevidence": SectionRule("## Strongest Counterevidence", ("replace", "append", "union", "preserve")),
            "## What Would Change My Mind": SectionRule("## What Would Change My Mind", ("replace", "append", "union", "preserve")),
            "## Evidence log": SectionRule("## Evidence log", ("append", "union", "preserve")),
            "## Contradictions": SectionRule("## Contradictions", ("append", "union", "preserve")),
        },
    ),
    "inquiry": PagePatchSchema(
        page_type="inquiry",
        intro_editable=True,
        section_order=("## TL;DR", "## Question", "## Why This Matters", "## Current Hypotheses", "## What Would Resolve It", "## Evidence log"),
        section_rules={
            "## TL;DR": SectionRule("## TL;DR", ("replace", "append", "preserve")),
            "## Question": SectionRule("## Question", ("replace", "append", "preserve")),
            "## Why This Matters": SectionRule("## Why This Matters", ("replace", "append", "preserve")),
            "## Current Hypotheses": SectionRule("## Current Hypotheses", ("replace", "append", "union", "preserve")),
            "## What Would Resolve It": SectionRule("## What Would Resolve It", ("replace", "append", "union", "preserve")),
            "## Evidence log": SectionRule("## Evidence log", ("append", "union", "preserve")),
        },
    ),
    "decision": PagePatchSchema(
        page_type="decision",
        intro_editable=True,
        section_order=(),
        section_rules={},
    ),
    "summary": PagePatchSchema(
        page_type="summary",
        intro_editable=True,
        section_order=(),
        section_rules={},
    ),
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_timestamp_readable() -> str:
    return _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _today() -> str:
    return _utc_now().date().isoformat()


def _coerce_str_list(value: Any) -> Any:
    """Coerce LLM output where a ``list[str]`` field arrived as something else.

    Models occasionally emit a plain string, or even a dict keyed by
    note-category, for fields like ``synthesis_notes`` / ``notes`` when they
    have only one remark or want to group their commentary. Normalise these
    into ``list[str]`` so the schema does not fail validation. Leave lists
    untouched.
    """
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, dict):
        items: list[str] = []
        for key, inner in value.items():
            if isinstance(inner, str):
                items.append(f"{key}: {inner}" if inner else str(key))
            elif isinstance(inner, list):
                for entry in inner:
                    items.append(str(entry))
            elif inner is None:
                items.append(str(key))
            else:
                items.append(f"{key}: {inner}")
        return items
    if isinstance(value, list):
        return [str(item) if not isinstance(item, str) else item for item in value]
    return value


def _text_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EvidenceValue(StrictModel):
    text: str
    evidence_refs: list[str]


class PositioningArtifact(StrictModel):
    summary: str
    work_priorities: list[str]
    life_priorities: list[str]
    constraints: list[str]
    evidence_refs: list[str]


class InquiryArtifact(StrictModel):
    slug: str
    question: str
    evidence_refs: list[str]


class SemanticEntity(StrictModel):
    proposal_id: str
    family: Literal["projects", "people", "concepts", "playbooks", "stances", "inquiries"]
    title: str
    slug: str
    summary: str
    domains: list[str] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)
    evidence_refs: list[str] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)


class SemanticRelationship(StrictModel):
    source_ref: str
    target_ref: str
    relation_type: str
    rationale: str
    evidence_refs: list[str] = Field(default_factory=list)


class SemanticOwner(StrictModel):
    name: str
    role: str = ""
    location: str = ""
    summary: str
    values: list[EvidenceValue] = Field(default_factory=list)
    positioning: PositioningArtifact
    open_inquiries: list[InquiryArtifact] = Field(default_factory=list)


class SemanticArtifact(StrictModel):
    bundle_id: str
    owner: SemanticOwner
    entities: list[SemanticEntity] = Field(default_factory=list)
    relationships: list[SemanticRelationship] = Field(default_factory=list)
    synthesis_notes: list[str] = Field(default_factory=list)

    _coerce_notes = field_validator("synthesis_notes", mode="before")(_coerce_str_list)


class GraphNodeProposal(StrictModel):
    proposal_id: str
    page_type: Literal["project", "person", "concept", "playbook", "stance", "inquiry"]
    slug: str
    title: str
    summary: str
    domains: list[str] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)
    evidence_refs: list[str] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)
    relates_to_refs: list[str] = Field(default_factory=list)


class GraphEdgeProposal(StrictModel):
    source_ref: str
    target_ref: str
    relation_type: str
    rationale: str
    evidence_refs: list[str] = Field(default_factory=list)


class GraphArtifact(StrictModel):
    bundle_id: str
    node_proposals: list[GraphNodeProposal] = Field(default_factory=list)
    edge_proposals: list[GraphEdgeProposal] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)

    _coerce_notes = field_validator("notes", mode="before")(_coerce_str_list)


class GraphChunkArtifact(StrictModel):
    bundle_id: str
    node_proposals: list[GraphNodeProposal] = Field(default_factory=list)
    edge_proposals: list[GraphEdgeProposal] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)

    _coerce_notes = field_validator("notes", mode="before")(_coerce_str_list)


class MergeDecision(StrictModel):
    proposal_id: str
    source_proposal_id: str
    action: Literal["create", "update", "merge"]
    title: str
    slug: str
    summary: str
    page_type: Literal["project", "person", "concept", "playbook", "stance", "inquiry"]
    domains: list[str] = Field(default_factory=list)
    relates_to: list[str] = Field(default_factory=list)
    target_page_id: str | None = None
    target_page_type: str | None = None
    target_path: str | None = None
    rationale: str
    evidence_refs: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_target_fields(self) -> "MergeDecision":
        if self.source_proposal_id != self.proposal_id:
            raise ValueError("source_proposal_id must match proposal_id for merge decisions")
        if not self.title.strip():
            raise ValueError("merge decisions require title")
        if not self.slug.strip():
            raise ValueError("merge decisions require slug")
        needs_target = self.action in {"update", "merge"}
        if needs_target and not (self.target_page_id and self.target_page_type and self.target_path):
            raise ValueError("update/merge decisions require target_page_id, target_page_type, and target_path")
        if not needs_target and any(value is not None for value in (self.target_page_id, self.target_page_type, self.target_path)):
            raise ValueError("create decisions must not include target fields")
        return self


class RelationshipDecision(StrictModel):
    source_ref: str
    target_ref: str
    action: Literal["keep", "drop"]
    rationale: str
    evidence_refs: list[str] = Field(default_factory=list)


class MergeArtifact(StrictModel):
    bundle_id: str
    decisions: list[MergeDecision] = Field(default_factory=list)
    relationship_decisions: list[RelationshipDecision] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)

    _coerce_notes = field_validator("notes", mode="before")(_coerce_str_list)


class MergeNodeChunkArtifact(StrictModel):
    bundle_id: str
    decisions: list[MergeDecision] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)

    _coerce_notes = field_validator("notes", mode="before")(_coerce_str_list)


class RelationshipDecisionArtifact(StrictModel):
    bundle_id: str
    relationship_decisions: list[RelationshipDecision] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)

    _coerce_notes = field_validator("notes", mode="before")(_coerce_str_list)


class VerifyArtifact(StrictModel):
    bundle_id: str
    approved: bool
    blocking_issues: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)

    _coerce_strlist = field_validator(
        "blocking_issues", "warnings", "notes", mode="before"
    )(_coerce_str_list)

    @model_validator(mode="after")
    def _validate_blocking_issues(self) -> "VerifyArtifact":
        if self.approved and self.blocking_issues:
            raise ValueError("approved verifier result cannot include blocking issues")
        if not self.approved and not self.blocking_issues:
            raise ValueError("rejected verifier result must include blocking issues")
        return self


class MaterializationPagePlan(StrictModel):
    plan_id: str
    target_kind: Literal[
        "owner_profile",
        "owner_values",
        "owner_positioning",
        "owner_open_inquiries",
        "owner_person",
        "canonical",
        "summary",
        "decision",
    ]
    write_mode: Literal["create", "update"]
    page_type: str
    slug: str
    title: str
    body_markdown: str | None = None
    intro_mode: Literal["preserve", "replace", "append"] = "preserve"
    intro_markdown: str | None = None
    section_operations: list[dict[str, Any]] = Field(default_factory=list)
    domains: list[str] = Field(default_factory=list)
    relates_to: list[str] = Field(default_factory=list)
    sources: list[str] = Field(default_factory=list)
    extra_frontmatter: dict[str, Any] = Field(default_factory=dict)
    target_path: str | None = None
    summary_kind: Literal["overview", "profile", "values", "positioning", "open-inquiries"] | None = None

    @model_validator(mode="after")
    def _validate_plan(self) -> "MaterializationPagePlan":
        if self.target_kind == "summary":
            if self.page_type != "summary":
                raise ValueError("summary target_kind must use page_type=summary")
            if self.summary_kind is None:
                raise ValueError("summary target_kind requires summary_kind")
        else:
            if self.summary_kind is not None:
                raise ValueError("summary_kind is only valid for summary target_kind")
        if self.write_mode == "update" and not self.target_path:
            raise ValueError("update page plans require target_path")
        if self.write_mode == "create" and self.target_path is not None:
            raise ValueError("create page plans must not set target_path")
        if self.write_mode == "create" and not self.body_markdown:
            raise ValueError("create page plans require body_markdown")
        if self.write_mode == "update" and self.body_markdown is not None:
            raise ValueError("update page plans must not set body_markdown")
        if self.write_mode == "update" and self.intro_mode != "preserve" and self.intro_markdown is None:
            raise ValueError("non-preserve intro updates require intro_markdown")
        return self


class PatchReviewArtifact(StrictModel):
    bundle_id: str
    target_path: str
    page_type: str
    expected_schema: dict[str, Any]
    discovered_headings: list[str]
    reason: str
    intro_mode: str
    intro_markdown: str | None = None
    section_operations: list[dict[str, Any]] = Field(default_factory=list)
    backup_path: str


class PatchReviewRequiredError(RuntimeError):
    def __init__(self, reviews: list[dict[str, Any]]):
        super().__init__("semantic patch review required")
        self.reviews = reviews


class MaterializationPlan(StrictModel):
    bundle_id: str
    pages: list[MaterializationPagePlan] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_required_targets(self) -> "MaterializationPlan":
        seen_targets = {page.target_kind for page in self.pages}
        missing = sorted(FIXED_TARGET_KINDS - seen_targets)
        if missing:
            raise ValueError(f"missing required fixed target kinds: {', '.join(missing)}")
        return self


@dataclass(frozen=True)
class PipelineArtifacts:
    semantic: dict[str, Any]
    graph: dict[str, Any]
    candidate_context: dict[str, Any]
    merge: dict[str, Any]
    verify: dict[str, Any] | None = None
    materialization_plan: dict[str, Any] | None = None
    artifact_paths: dict[str, str] | None = None


def artifact_paths(bundle_dir: Path) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for key, name in ARTIFACT_FILE_NAMES.items():
        paths[key] = bundle_dir / name
    for key, name in MARKDOWN_FILE_NAMES.items():
        paths[f"{key}_markdown"] = bundle_dir / name
    return paths


def _dump_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _render_markdown(name: str, data: dict[str, Any]) -> str:
    return (
        f"# {name.replace('-', ' ').title()}\n\n"
        f"```json\n{json.dumps(data, indent=2, ensure_ascii=False)}\n```\n"
    )


def _write_artifact(
    path: Path,
    markdown_path: Path,
    *,
    data: dict[str, Any],
    identity: LLMCacheIdentity | None = None,
    repair_count: int = 0,
) -> dict[str, Any]:
    payload = {"data": data}
    if identity is not None:
        payload["_llm"] = {**identity.to_dict(), "repair_count": repair_count}
    _dump_json(path, payload)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(_render_markdown(path.stem, payload), encoding="utf-8")
    return payload


def _read_artifact_data(path: Path) -> dict[str, Any]:
    payload = _load_json(path)
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return dict(payload["data"])
    return payload


def _schema_for_page(page: MaterializationPagePlan, *, target_path: Path | None = None) -> PagePatchSchema:
    if page.target_kind == "owner_values" or page.target_kind == "owner_positioning" or page.target_kind == "owner_open_inquiries":
        return PATCH_SCHEMAS["note"]
    if page.target_kind == "owner_profile":
        return PATCH_SCHEMAS["profile"]
    if page.target_kind == "owner_person":
        return PATCH_SCHEMAS["person"]
    if page.target_kind == "summary":
        return PATCH_SCHEMAS["summary"]
    if page.target_kind == "decision":
        return PATCH_SCHEMAS["decision"]
    schema = PATCH_SCHEMAS.get(page.page_type)
    if schema is None:
        raise RuntimeError(f"no patch schema defined for page type {page.page_type}")
    return schema


def _section_operations_from_plan(page: MaterializationPagePlan) -> list[SectionOperation]:
    operations: list[SectionOperation] = []
    for item in page.section_operations:
        if not isinstance(item, dict):
            raise RuntimeError(f"invalid section operation for {page.plan_id}: expected object")
        heading = str(item.get("heading") or "").strip()
        mode = str(item.get("mode") or "").strip()
        content = str(item.get("content") or "")
        insert_after = item.get("insert_after")
        if not heading or not heading.startswith("## "):
            raise RuntimeError(f"invalid section operation heading for {page.plan_id}: {heading!r}")
        operations.append(
            SectionOperation(
                heading=heading,
                mode=mode,
                content=content,
                insert_after=str(insert_after) if insert_after else None,
            )
        )
    return operations


def _default_insert_after(schema: PagePatchSchema, heading: str, existing_headings: list[str]) -> str | None:
    try:
        index = schema.section_order.index(heading)
    except ValueError:
        return None
    for prior in reversed(schema.section_order[:index]):
        if prior in existing_headings:
            return prior
    return None


def _review_artifact_paths(*, bundle_dir: Path, plan_id: str) -> tuple[Path, Path]:
    safe = slugify(plan_id) or "patch-review"
    root = bundle_dir / PATCH_REVIEW_DIR
    return root / f"{safe}.json", root / f"{safe}.md"


def _coerce_upload_part(upload: dict[str, Any]):
    from mind.services.providers.base import LLMInputPart

    raw_path = Path(str(upload.get("path") or ""))
    if not raw_path.exists():
        raise FileNotFoundError(f"missing onboarding upload: {raw_path}")
    mime_type = str(upload.get("media_type") or mimetypes.guess_type(raw_path.name)[0] or "application/octet-stream")
    if mime_type.startswith(TEXT_MIME_PREFIXES) or mime_type in TEXT_MIME_TYPES:
        try:
            text = raw_path.read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            raise RuntimeError(f"unable to decode onboarding upload {raw_path.name} as utf-8 text") from exc
        return [
            LLMInputPart.metadata_part(
                {
                    "upload_id": upload.get("id"),
                    "file_name": raw_path.name,
                    "media_type": mime_type,
                    "evidence_refs": upload.get("evidence_refs") or [],
                }
            ),
            LLMInputPart.text_part(
                f"Upload file: {raw_path.name}\nMedia type: {mime_type}\n\n{text}"
            ),
        ]
    kind = "pdf_bytes" if mime_type == "application/pdf" else "image_bytes" if mime_type.startswith("image/") else "audio_bytes" if mime_type.startswith("audio/") else "file_bytes"
    return [
        LLMInputPart.metadata_part(
            {
                "upload_id": upload.get("id"),
                "file_name": raw_path.name,
                "media_type": mime_type,
                "evidence_refs": upload.get("evidence_refs") or [],
            }
        ),
        LLMInputPart.file_bytes_part(
            raw_path.read_bytes(),
            mime_type=mime_type,
            file_name=raw_path.name,
            kind=kind,  # type: ignore[arg-type]
            metadata={"path": raw_path.as_posix()},
        ),
    ]


def build_synthesis_input_parts(
    *,
    bundle_id: str,
    bundle: dict[str, Any],
    transcript_path: Path,
) -> list[Any]:
    from mind.services.providers.base import LLMInputPart

    if not transcript_path.exists():
        raise FileNotFoundError(f"missing onboarding transcript for {bundle_id}: {transcript_path}")
    parts: list[LLMInputPart] = [
        LLMInputPart.metadata_part(
            {
                "bundle_id": bundle_id,
                "upload_count": len(bundle.get("uploads") or []),
            }
        ),
        LLMInputPart.text_part(
            "Normalized onboarding evidence bundle JSON\n\n"
            + json.dumps(bundle, ensure_ascii=False, indent=2)
        ),
        LLMInputPart.text_part(
            "Onboarding interview transcript JSONL\n\n"
            + transcript_path.read_text(encoding="utf-8")
        ),
    ]
    for upload in bundle.get("uploads") or []:
        parts.extend(_coerce_upload_part(upload))
    return parts


def _candidate_excerpt(repo_root: Path, candidate: ResolutionCandidate) -> str:
    path = Vault.load(repo_root).resolve_logical_path(candidate.path)
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8")
    if text.startswith("---\n"):
        marker = text.find("\n---\n", 4)
        if marker != -1:
            text = text[marker + 5 :]
    lines = [line.strip() for line in text.splitlines() if line.strip() and not line.strip().startswith("#")]
    return " ".join(lines[:4])[:400]


def build_merge_candidate_context(
    repo_root: Path,
    *,
    graph_artifact: dict[str, Any],
) -> dict[str, Any]:
    registry = GraphRegistry.for_repo_root(repo_root)
    registry.ensure_built()
    route = resolve_route("embedding")
    vector_backend = select_vector_backend(Vault.load(repo_root).vector_db)
    embedding_service = get_embedding_service()
    contexts: list[dict[str, Any]] = []
    for proposal in graph_artifact.get("node_proposals") or []:
        title = str(proposal.get("title") or "")
        exact_candidates = registry.resolve_candidates(title, limit=5)
        try:
            vector_candidates = registry.resolve_vector_candidates(
                title,
                embedding_service=embedding_service,
                vector_backend=vector_backend,
                model=route.model,
                limit=5,
            )
        except Exception:
            vector_candidates = []
        contexts.append(
            {
                "proposal_id": proposal.get("proposal_id"),
                "title": title,
                "page_type": proposal.get("page_type"),
                "exact_candidates": [
                    {
                        "registry_node_id": candidate.registry_node_id,
                        "page_id": candidate.page_id,
                        "title": candidate.title,
                        "primary_type": candidate.primary_type,
                        "path": candidate.path,
                        "score": candidate.score,
                        "match_kind": candidate.match_kind,
                        "aliases": candidate.aliases,
                        "excerpt": _candidate_excerpt(repo_root, candidate),
                    }
                    for candidate in exact_candidates
                ],
                "vector_candidates": [
                    {
                        "registry_node_id": candidate.registry_node_id,
                        "page_id": candidate.page_id,
                        "title": candidate.title,
                        "primary_type": candidate.primary_type,
                        "path": candidate.path,
                        "score": candidate.score,
                        "match_kind": candidate.match_kind,
                        "aliases": candidate.aliases,
                        "excerpt": _candidate_excerpt(repo_root, candidate),
                    }
                    for candidate in vector_candidates
                ],
            }
        )
    return {
        "bundle_id": graph_artifact.get("bundle_id"),
        "candidates": contexts,
        "embedding_model": route.model,
    }


RepairCallback = Callable[[list[dict[str, Any]], dict[str, Any]], dict[str, Any]]


def _validate_payload(
    model_type: type[StrictModel],
    data: dict[str, Any],
    *,
    repair_callback: RepairCallback | None = None,
) -> tuple[dict[str, Any], bool]:
    try:
        return model_type.model_validate(data).model_dump(mode="json"), False
    except ValidationError as exc:
        if repair_callback is None:
            raise RuntimeError(str(exc)) from exc
        repair_errors = compact_validation_errors(exc.errors())
        try:
            repaired_payload = repair_callback(repair_errors, data)
        except Exception as repair_exc:
            raise RuntimeError(f"{exc}\n\nrepair attempted and failed: {repair_exc}") from repair_exc
        try:
            return model_type.model_validate(repaired_payload).model_dump(mode="json"), True
        except ValidationError as repaired_exc:
            raise RuntimeError(f"{exc}\n\nrepair attempted and failed: {repaired_exc}") from repaired_exc


def _build_repair_callback(
    *,
    executor: Any,
    model_type: type[StrictModel],
    request_builder: Callable[[], Any],
    prompt_version: str,
) -> RepairCallback:
    schema = prepare_strict_schema(model_type)

    def _callback(validation_errors: list[dict[str, Any]], invalid_payload: dict[str, Any]) -> dict[str, Any]:
        request = request_builder()
        return repair_once(
            executor,
            original_request=request,
            prompt_version=prompt_version,
            response_schema=schema,
            validation_errors=validation_errors,
            invalid_payload=invalid_payload,
        )

    return _callback


def _configured_chunk_workers(task_class: str) -> int:
    route = resolve_route(task_class)
    from scripts.common import env

    cfg = env.load()
    configured = getattr(cfg, "llm_concurrency", {}).get(route.provider)
    return normalize_concurrency(configured)


def _check_gateway_balance(task_class: str) -> str | None:
    from scripts.common import env
    from mind.services.providers.gateway import GatewayProviderClient

    cfg = env.load()
    route = resolve_route(task_class)
    client = GatewayProviderClient(api_key=getattr(cfg, "ai_gateway_api_key", ""), model=route.model)
    try:
        payload = client.get_credits()
    except Exception as exc:
        return f"warning: unable to verify AI Gateway credits before chunk dispatch: {exc}"
    balance = _extract_credit_balance(payload)
    if balance is None:
        return None
    if balance < float(getattr(cfg, "llm_min_balance_usd", 1.0)):
        raise RuntimeError(
            f"AI Gateway balance ${balance:.2f} is below configured minimum ${float(getattr(cfg, 'llm_min_balance_usd', 1.0)):.2f}"
        )
    return None


def _extract_credit_balance(payload: dict[str, Any]) -> float | None:
    for key in ("balance", "remaining_balance", "credits", "available_credits"):
        value = payload.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    data = payload.get("data")
    if isinstance(data, dict):
        return _extract_credit_balance(data)
    return None


def _run_chunk_phase(
    *,
    bundle_dir: Path,
    bundle_id: str,
    phase: ChunkPhase,
    chunks: list[dict[str, Any]],
    max_workers: int,
    runner: Callable[[dict[str, Any]], tuple[dict[str, Any], str | None]],
) -> list[dict[str, Any]]:
    if not chunks:
        return []
    chunk_by_id = {str(chunk["chunk_id"]): chunk for chunk in chunks}
    prune_chunk_phase(bundle_dir, phase=phase, keep_chunk_ids=set(chunk_by_id))
    for chunk in chunks:
        ensure_chunk_state(bundle_dir, bundle_id=bundle_id, phase=phase, chunk_id=str(chunk["chunk_id"]))

    while True:
        states = {
            state.chunk_id: state
            for state in load_chunk_states(bundle_dir, phase=phase)
            if state.chunk_id in chunk_by_id
        }
        if states and all(state.status == "done" for state in states.values()):
            break
        failed = [state for state in states.values() if state.status == "failed" and state.attempts >= 3]
        if failed:
            raise RuntimeError(f"{phase} failed for chunks: {', '.join(sorted(state.chunk_id for state in failed))}")

        runnable_states = [state for state in iter_runnable_states(bundle_dir, phase=phase) if state.chunk_id in chunk_by_id]
        if not runnable_states:
            retry_not_before = next_retry_not_before(bundle_dir, phase=phase)
            if retry_not_before is not None:
                raise RuntimeError(f"{phase} is waiting for retry window until {retry_not_before}")
            waiting = [state.chunk_id for state in states.values() if state.status != "done"]
            raise RuntimeError(f"{phase} is waiting on in-flight chunks with no local owner: {', '.join(sorted(waiting))}")

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(
                    _execute_chunk,
                    bundle_dir=bundle_dir,
                    bundle_id=bundle_id,
                    phase=phase,
                    chunk=chunk_by_id[state.chunk_id],
                    runner=runner,
                )
                for state in runnable_states
            ]
            for future in as_completed(futures):
                future.result()

    payloads: list[dict[str, Any]] = []
    for chunk in chunks:
        result = load_chunk_result(bundle_dir, phase=phase, chunk_id=str(chunk["chunk_id"]))
        payloads.append(dict(result.get("data") or {}))
    return payloads


def _execute_chunk(
    *,
    bundle_dir: Path,
    bundle_id: str,
    phase: ChunkPhase,
    chunk: dict[str, Any],
    runner: Callable[[dict[str, Any]], tuple[dict[str, Any], str | None]],
) -> None:
    acquired, state = acquire_chunk_lease(
        bundle_dir,
        bundle_id=bundle_id,
        phase=phase,
        chunk_id=str(chunk["chunk_id"]),
    )
    if not acquired:
        return
    try:
        payload, generation_id = runner(chunk)
        mark_chunk_done(bundle_dir, state=state, payload={"data": payload}, generation_id=generation_id)
    except Exception as exc:
        retry_after = getattr(exc, "retry_after_seconds", None)
        mark_chunk_failed(bundle_dir, state=state, error_message=str(exc), retry_after_seconds=retry_after)


def _run_chunked_graph_stage(
    *,
    bundle_dir: Path,
    bundle: dict[str, Any],
    semantic: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    llm = get_llm_service()
    schema = prepare_strict_schema(GraphChunkArtifact)
    chunks = chunk_graph_entities(semantic)
    warning = _check_gateway_balance("onboarding_synthesis")
    payloads = _run_chunk_phase(
        bundle_dir=bundle_dir,
        bundle_id=str(bundle.get("bundle_id") or bundle_dir.name),
        phase="graph_nodes",
        chunks=chunks,
        max_workers=_configured_chunk_workers("onboarding_synthesis"),
        runner=lambda chunk: _run_graph_chunk(
            llm=llm,
            bundle=bundle,
            chunk=chunk,
            schema=schema,
        ),
    )
    graph = assemble_graph_chunks(str(bundle.get("bundle_id") or bundle_dir.name), payloads)
    validated = GraphArtifact.model_validate(graph).model_dump(mode="json")
    warnings = [warning] if warning else []
    return validated, warnings


def _run_graph_chunk(
    *,
    llm: Any,
    bundle: dict[str, Any],
    chunk: dict[str, Any],
    schema: dict[str, Any],
) -> tuple[dict[str, Any], str | None]:
    semantic_chunk = {
        "bundle_id": bundle["bundle_id"],
        "entities": list(chunk.get("entities") or []),
        "relationships": list(chunk.get("relationships") or []),
    }
    result = llm.shape_onboarding_graph_chunk(
        bundle=bundle,
        semantic_chunk=semantic_chunk,
        response_schema=schema,
    )
    payload_data, generation_id = _payload_and_generation(result)
    payload, _ = _validate_payload(
        GraphChunkArtifact,
        payload_data,
        repair_callback=_build_repair_callback(
            executor=llm.executor,
            model_type=GraphChunkArtifact,
            prompt_version=ONBOARDING_GRAPH_CHUNK_PROMPT_VERSION,
            request_builder=lambda: llm.executor.build_prompt_request(
                task_class="onboarding_synthesis",
                prompt=build_onboarding_graph_chunk_prompt(bundle=bundle, semantic_chunk=semantic_chunk),
                output_mode="json",
                response_schema=schema,
                request_metadata={
                    "bundle_id": bundle["bundle_id"],
                    "phase": "graph_nodes",
                    "chunk_id": chunk["chunk_id"],
                },
            ),
        ) if hasattr(llm, "executor") else None,
    )
    return payload, generation_id


def _run_chunked_merge_stage(
    *,
    repo_root: Path,
    bundle_dir: Path,
    bundle: dict[str, Any],
    graph: dict[str, Any],
    candidate_context: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    llm = get_llm_service()
    node_schema = prepare_strict_schema(MergeNodeChunkArtifact)
    relationship_schema = prepare_strict_schema(RelationshipDecisionArtifact)
    chunks = chunk_merge_nodes(graph, candidate_context)
    warning = _check_gateway_balance("onboarding_merge")
    node_payloads = _run_chunk_phase(
        bundle_dir=bundle_dir,
        bundle_id=str(bundle.get("bundle_id") or bundle_dir.name),
        phase="merge_nodes",
        chunks=chunks,
        max_workers=_configured_chunk_workers("onboarding_merge"),
        runner=lambda chunk: _run_merge_node_chunk(
            llm=llm,
            bundle=bundle,
            chunk=chunk,
            schema=node_schema,
        ),
    )
    merge_decisions = [decision for payload in node_payloads for decision in payload.get("decisions") or []]
    relationship_payloads = _run_chunk_phase(
        bundle_dir=bundle_dir,
        bundle_id=str(bundle.get("bundle_id") or bundle_dir.name),
        phase="merge_relationships",
        chunks=[{"chunk_id": "merge-relationships-global"}],
        max_workers=1,
        runner=lambda _chunk: _run_relationship_merge(
            llm=llm,
            bundle=bundle,
            graph=graph,
            merge_decisions=merge_decisions,
            schema=relationship_schema,
        ),
    )
    relationship_decisions = list((relationship_payloads[0].get("relationship_decisions") if relationship_payloads else []) or [])
    merge = assemble_merge_chunks(
        str(bundle.get("bundle_id") or bundle_dir.name),
        node_payloads,
        relationship_decisions,
        graph_artifact=graph,
    )
    validated = MergeArtifact.model_validate(merge).model_dump(mode="json")
    warnings = [warning] if warning else []
    return validated, warnings


def _run_merge_node_chunk(
    *,
    llm: Any,
    bundle: dict[str, Any],
    chunk: dict[str, Any],
    schema: dict[str, Any],
) -> tuple[dict[str, Any], str | None]:
    graph_chunk = {
        "bundle_id": bundle["bundle_id"],
        "node_proposals": list(chunk.get("node_proposals") or []),
        "candidates": list(chunk.get("candidates") or []),
    }
    result = llm.merge_onboarding_graph_chunk(
        bundle=bundle,
        graph_chunk=graph_chunk,
        response_schema=schema,
    )
    payload_data, generation_id = _payload_and_generation(result)
    payload, _ = _validate_payload(
        MergeNodeChunkArtifact,
        payload_data,
        repair_callback=_build_repair_callback(
            executor=llm.executor,
            model_type=MergeNodeChunkArtifact,
            prompt_version=ONBOARDING_MERGE_CHUNK_PROMPT_VERSION,
            request_builder=lambda: llm.executor.build_prompt_request(
                task_class="onboarding_merge",
                prompt=build_onboarding_merge_chunk_prompt(bundle=bundle, graph_chunk=graph_chunk),
                output_mode="json",
                response_schema=schema,
                request_metadata={
                    "bundle_id": bundle["bundle_id"],
                    "phase": "merge_nodes",
                    "chunk_id": chunk["chunk_id"],
                },
            ),
        ) if hasattr(llm, "executor") else None,
    )
    return payload, generation_id


def _run_relationship_merge(
    *,
    llm: Any,
    bundle: dict[str, Any],
    graph: dict[str, Any],
    merge_decisions: list[dict[str, Any]],
    schema: dict[str, Any],
) -> tuple[dict[str, Any], str | None]:
    kept_nodes = kept_nodes_for_relationships(graph, merge_decisions)
    edge_proposals = relationship_edges_for_kept_nodes(graph, merge_decisions)
    result = llm.merge_onboarding_relationships(
        bundle=bundle,
        kept_nodes=kept_nodes,
        edge_proposals=edge_proposals,
        response_schema=schema,
    )
    payload_data, generation_id = _payload_and_generation(result)
    payload, _ = _validate_payload(
        RelationshipDecisionArtifact,
        payload_data,
        repair_callback=_build_repair_callback(
            executor=llm.executor,
            model_type=RelationshipDecisionArtifact,
            prompt_version=ONBOARDING_MERGE_RELATIONSHIPS_PROMPT_VERSION,
            request_builder=lambda: llm.executor.build_prompt_request(
                task_class="onboarding_merge",
                prompt=build_onboarding_merge_relationships_prompt(
                    bundle=bundle,
                    kept_nodes=kept_nodes,
                    edge_proposals=edge_proposals,
                ),
                output_mode="json",
                response_schema=schema,
                request_metadata={
                    "bundle_id": bundle["bundle_id"],
                    "phase": "merge_relationships",
                    "chunk_id": "merge-relationships-global",
                },
            ),
        ) if hasattr(llm, "executor") else None,
    )
    return payload, generation_id


def _payload_and_generation(result: Any) -> tuple[dict[str, Any], str | None]:
    if hasattr(result, "data"):
        payload = dict(getattr(result, "data") or {})
        metadata = getattr(result, "response_metadata", {}) or {}
        generation_id = _text_or_none(metadata.get("generation_id")) if isinstance(metadata, dict) else None
        return payload, generation_id
    if isinstance(result, dict):
        return dict(result), None
    raise RuntimeError(f"unsupported chunk result type: {type(result).__name__}")


def synthesize_bundle(
    repo_root: Path,
    *,
    bundle_dir: Path,
    bundle: dict[str, Any],
    transcript_path: Path,
) -> PipelineArtifacts:
    bundle_id = str(bundle.get("bundle_id") or bundle_dir.name)
    paths = artifact_paths(bundle_dir)
    llm = get_llm_service()
    if paths["semantic"].exists():
        semantic = _read_artifact_data(paths["semantic"])
    else:
        semantic_schema = prepare_strict_schema(SemanticArtifact)
        semantic_input_parts = build_synthesis_input_parts(bundle_id=bundle_id, bundle=bundle, transcript_path=transcript_path)
        semantic_repair_callback = (
            _build_repair_callback(
                executor=llm.executor,
                model_type=SemanticArtifact,
                prompt_version=ONBOARDING_SYNTHESIS_PROMPT_VERSION,
                request_builder=lambda: llm.executor.build_parts_request(
                    task_class="onboarding_synthesis",
                    instructions=build_onboarding_synthesis_instructions(bundle_id=bundle_id),
                    input_parts=semantic_input_parts,
                    output_mode="json",
                    input_mode="file",
                    request_metadata={"bundle_id": bundle_id},
                    response_schema=semantic_schema,
                ),
            )
            if hasattr(llm, "executor")
            else None
        )
        semantic_raw, semantic_identity = llm.synthesize_onboarding_semantics(
            bundle_id=bundle_id,
            input_parts=semantic_input_parts,
            with_meta=True,
            response_schema=semantic_schema,
        )
        semantic, semantic_repaired = _validate_payload(
            SemanticArtifact,
            semantic_raw,
            repair_callback=semantic_repair_callback,
        )
        _write_artifact(
            paths["semantic"],
            paths["semantic_markdown"],
            data=semantic,
            identity=semantic_identity,
            repair_count=int(semantic_repaired),
        )

    graph, graph_warnings = _run_chunked_graph_stage(
        bundle_dir=bundle_dir,
        bundle=bundle,
        semantic=semantic,
    )
    _write_artifact(
        paths["graph"],
        paths["graph_markdown"],
        data=graph,
        identity=LLMCacheIdentity(
            task_class="onboarding_synthesis",
            provider="chunked",
            model="chunked",
            transport="deterministic",
            api_family="deterministic",
            input_mode="text",
            prompt_version="onboarding.synthesis.graph.chunked.v1",
            request_fingerprint={"kind": "chunk-assembly"},
        ),
    )

    candidate_context = build_merge_candidate_context(repo_root, graph_artifact=graph)
    _write_artifact(paths["candidate_context"], paths["candidate_context_markdown"], data=candidate_context)

    merge, merge_warnings = _run_chunked_merge_stage(
        repo_root=repo_root,
        bundle_dir=bundle_dir,
        bundle=bundle,
        graph=graph,
        candidate_context=candidate_context,
    )
    _write_artifact(
        paths["merge"],
        paths["merge_markdown"],
        data=merge,
        identity=LLMCacheIdentity(
            task_class="onboarding_merge",
            provider="chunked",
            model="chunked",
            transport="deterministic",
            api_family="deterministic",
            input_mode="text",
            prompt_version="onboarding.merge.chunked.v1",
            request_fingerprint={"kind": "chunk-assembly"},
        ),
    )

    if graph_warnings or merge_warnings:
        for artifact_path in (paths["semantic"], paths["graph"], paths["merge"]):
            if not artifact_path.exists():
                continue
            payload = _load_json(artifact_path)
            payload.setdefault("_pipeline_warnings", [])
            payload["_pipeline_warnings"].extend(graph_warnings + merge_warnings)
            _dump_json(artifact_path, payload)

    return PipelineArtifacts(
        semantic=semantic,
        graph=graph,
        candidate_context=candidate_context,
        merge=merge,
        artifact_paths={key: value.as_posix() for key, value in paths.items()},
    )


def verify_bundle(
    *,
    bundle_dir: Path,
    bundle: dict[str, Any],
    semantic: dict[str, Any],
    graph: dict[str, Any],
    merge: dict[str, Any],
) -> PipelineArtifacts:
    paths = artifact_paths(bundle_dir)
    llm = get_llm_service()
    verify_schema = prepare_strict_schema(VerifyArtifact)
    verify_repair_callback = (
        _build_repair_callback(
            executor=llm.executor,
            model_type=VerifyArtifact,
            prompt_version=ONBOARDING_VERIFY_PROMPT_VERSION,
            request_builder=lambda: llm.executor.build_prompt_request(
                task_class="onboarding_verify",
                prompt=build_onboarding_verify_prompt(
                    bundle=bundle,
                    semantic_artifact=semantic,
                    graph_artifact=graph,
                    merge_artifact=merge,
                ),
                output_mode="json",
                response_schema=verify_schema,
            ),
        )
        if hasattr(llm, "executor")
        else None
    )
    verify_raw, verify_identity = llm.verify_onboarding_graph(
        bundle=bundle,
        semantic_artifact=semantic,
        graph_artifact=graph,
        merge_artifact=merge,
        with_meta=True,
        response_schema=verify_schema,
    )
    verify, verify_repaired = _validate_payload(
        VerifyArtifact,
        verify_raw,
        repair_callback=verify_repair_callback,
    )
    _write_artifact(
        paths["verify"],
        paths["verify_markdown"],
        data=verify,
        identity=verify_identity,
        repair_count=int(verify_repaired),
    )

    materialization_plan: dict[str, Any] | None = None
    if verify.get("approved"):
        plan_raw = build_materialization_plan(
            bundle_id=str(bundle.get("bundle_id") or bundle_dir.name),
            bundle=bundle,
            semantic=semantic,
            graph=graph,
            merge=merge,
            verify=verify,
        )
        materialization_plan = MaterializationPlan.model_validate(plan_raw).model_dump(mode="json")
        _write_artifact(
            paths["materialization_plan"],
            paths["materialization_plan_markdown"],
            data=materialization_plan,
            identity=LLMCacheIdentity(
                task_class="onboarding_materialization",
                provider="deterministic",
                model="deterministic",
                transport="deterministic",
                api_family="responses",
                input_mode="text",
                prompt_version="onboarding.materialization.deterministic.v2",
                request_fingerprint={"kind": "deterministic-plan"},
            ),
        )
    return PipelineArtifacts(
        semantic=semantic,
        graph=graph,
        candidate_context=_read_artifact_data(paths["candidate_context"]) if paths["candidate_context"].exists() else {},
        merge=merge,
        verify=verify,
        materialization_plan=materialization_plan,
        artifact_paths={key: value.as_posix() for key, value in paths.items()},
    )


def load_pipeline_artifacts(bundle_dir: Path) -> PipelineArtifacts:
    paths = artifact_paths(bundle_dir)
    missing = [name for name in ("semantic", "graph", "merge") if not paths[name].exists()]
    if missing:
        raise FileNotFoundError(f"missing onboarding synthesis artifact(s): {', '.join(missing)}")
    verify = _read_artifact_data(paths["verify"]) if paths["verify"].exists() else None
    materialization_plan = _read_artifact_data(paths["materialization_plan"]) if paths["materialization_plan"].exists() else None
    return PipelineArtifacts(
        semantic=_read_artifact_data(paths["semantic"]),
        graph=_read_artifact_data(paths["graph"]),
        candidate_context=_read_artifact_data(paths["candidate_context"]) if paths["candidate_context"].exists() else {},
        merge=_read_artifact_data(paths["merge"]),
        verify=verify,
        materialization_plan=materialization_plan,
        artifact_paths={key: value.as_posix() for key, value in paths.items()},
    )


def _resolve_existing_target(repo_root: Path, target_path: str) -> Path:
    raw = Path(target_path)
    if raw.is_absolute():
        raise RuntimeError(f"materialization target_path must be repo-relative: {target_path}")
    resolved = repo_root / raw
    try:
        resolved.relative_to(repo_root)
    except ValueError as exc:
        raise RuntimeError(f"materialization target_path escapes repo root: {target_path}") from exc
    if not resolved.exists():
        raise RuntimeError(f"materialization target_path does not exist: {target_path}")
    return resolved


def _validate_patch_plan(
    *,
    page: MaterializationPagePlan,
    target: Path,
    schema: PagePatchSchema,
    parsed: ParsedMarkdownBody,
) -> str | None:
    if not schema.intro_editable and page.intro_mode != "preserve":
        return f"page type {page.page_type} does not allow intro updates"
    operations = _section_operations_from_plan(page)
    existing_headings = [section.heading for section in parsed.sections]
    for operation in operations:
        rule = schema.section_rules.get(operation.heading)
        if rule is None:
            return f"unsupported section heading {operation.heading!r} for page type {page.page_type}"
        if operation.mode not in rule.allowed_modes:
            return (
                f"operation {operation.mode!r} is not allowed for {operation.heading!r} "
                f"on page type {page.page_type}"
            )
        if operation.mode == "union" and not operation.content.strip():
            return f"union operation for {operation.heading!r} requires bullet-list content"
    return None


def _resolve_materialization_target(
    repo_root: Path,
    vault: Vault,
    *,
    bundle_id: str,
    page: MaterializationPagePlan,
) -> Path:
    if page.write_mode == "update" and page.target_path:
        return _resolve_existing_target(repo_root, page.target_path)
    if page.target_kind == "owner_profile":
        return vault.owner_profile
    if page.target_kind == "owner_values":
        return vault.values_path
    if page.target_kind == "owner_positioning":
        return vault.positioning_path
    if page.target_kind == "owner_open_inquiries":
        return vault.wiki / "me" / "open-inquiries.md"
    if page.target_kind == "owner_person":
        return vault.wiki / "people" / f"{page.slug}.md"
    if page.target_kind == "summary":
        return vault.wiki / "summaries" / f"{page.slug}.md"
    if page.target_kind == "decision":
        return vault.wiki / "decisions" / f"onboarding-{bundle_id}.md"
    if page.target_kind == "canonical":
        if page.page_type not in CANONICAL_PAGE_DIRS:
            raise RuntimeError(f"unsupported canonical page_type for onboarding materialization: {page.page_type}")
        return vault.wiki / CANONICAL_PAGE_DIRS[page.page_type] / f"{page.slug}.md"
    raise RuntimeError(f"unsupported materialization target kind: {page.target_kind}")


def _frontmatter_for_page(page: MaterializationPagePlan) -> dict[str, Any]:
    extra = dict(page.extra_frontmatter or {})
    aliases = list(extra.pop("aliases", [])) if isinstance(extra.get("aliases"), list) else []
    tags = list(extra.pop("tags", [])) if isinstance(extra.get("tags"), list) and extra.get("tags") else default_tags(page.page_type)
    frontmatter = {
        "id": _page_id_for_target(page),
        "type": page.page_type,
        "title": page.title,
        "status": "active",
        "created": _today(),
        "last_updated": _today(),
        "aliases": aliases,
        "tags": tags,
        "domains": list(page.domains),
        "relates_to": list(page.relates_to),
        "sources": list(page.sources),
    }
    frontmatter.update(extra)
    return frontmatter


def _page_id_for_target(page: MaterializationPagePlan) -> str:
    if page.target_kind == "owner_profile":
        return "profile"
    if page.target_kind == "owner_values":
        return "values"
    if page.target_kind == "owner_positioning":
        return "positioning"
    if page.target_kind == "owner_open_inquiries":
        return "open-inquiries"
    return page.slug


def _split_frontmatter(text: str) -> tuple[dict[str, Any], str]:
    if not text.startswith("---\n"):
        return {}, text
    marker = text.find("\n---\n", 4)
    if marker == -1:
        return {}, text
    try:
        frontmatter = yaml.safe_load(text[4:marker]) or {}
    except yaml.YAMLError:
        frontmatter = {}
    if not isinstance(frontmatter, dict):
        frontmatter = {}
    return frontmatter, text[marker + 5 :]


def _read_existing_page(target: Path) -> tuple[dict[str, Any], str]:
    text = target.read_text(encoding="utf-8")
    return _split_frontmatter(text)


def _ordered_union(left: list[Any], right: list[Any]) -> list[Any]:
    ordered: list[Any] = []
    seen: set[str] = set()
    for item in [*left, *right]:
        key = json.dumps(item, sort_keys=True, ensure_ascii=False) if not isinstance(item, str) else item
        if key in seen:
            continue
        ordered.append(item)
        seen.add(key)
    return ordered


def _filter_obsolete_source_refs(values: list[Any]) -> list[Any]:
    filtered: list[Any] = []
    for value in values:
        text = str(value)
        if text.startswith("[[summary-file-"):
            continue
        if text.startswith("[[") and text.endswith("]]") and any(
            text.endswith(suffix)
            for suffix in (
                "-overview]]",
                "-profile-summary]]",
                "-values-summary]]",
                "-positioning-summary]]",
                "-open-inquiries-summary]]",
            )
        ):
            continue
        filtered.append(value)
    return filtered


def _merge_frontmatter(existing: dict[str, Any], proposed: dict[str, Any]) -> dict[str, Any]:
    merged = dict(proposed)
    if existing.get("created"):
        merged["created"] = existing["created"]
    merged["last_updated"] = _today()
    for key in ("aliases", "tags", "domains", "relates_to", "sources"):
        existing_values = existing.get(key)
        proposed_values = proposed.get(key)
        if isinstance(existing_values, list) or isinstance(proposed_values, list):
            merged[key] = _ordered_union(
                list(existing_values or []) if isinstance(existing_values, list) else [],
                list(proposed_values or []) if isinstance(proposed_values, list) else [],
            )
    if isinstance(merged.get("sources"), list):
        merged["sources"] = _filter_obsolete_source_refs(list(merged["sources"]))
    return merged


def _snapshot_existing_target(*, repo_root: Path, bundle_id: str, target: Path) -> str:
    relative = target.relative_to(repo_root)
    backup = (
        Vault.load(repo_root).onboarding_bundles_root
        / bundle_id
        / "pre-materialization"
        / relative
    )
    backup.parent.mkdir(parents=True, exist_ok=True)
    backup.write_text(target.read_text(encoding="utf-8"), encoding="utf-8")
    return backup.as_posix()


def _write_patch_review_artifact(
    *,
    bundle_dir: Path,
    review: dict[str, Any],
) -> tuple[str, str]:
    plan_id = str(review.get("plan_id") or review.get("target_path") or "patch-review")
    json_path, markdown_path = _review_artifact_paths(bundle_dir=bundle_dir, plan_id=plan_id)
    _dump_json(json_path, review)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(_render_markdown(json_path.stem, review), encoding="utf-8")
    return json_path.as_posix(), markdown_path.as_posix()


def apply_materialization_plan(
    repo_root: Path,
    *,
    bundle_id: str,
    plan: dict[str, Any],
    force: bool = False,
) -> dict[str, list[str] | str | None]:
    validated_plan = MaterializationPlan.model_validate(plan)
    vault = Vault.load(repo_root)
    bundle_dir = vault.onboarding_bundles_root / bundle_id
    written: list[Path] = []
    summary_paths: list[Path] = []
    decision_page: Path | None = None
    backup_paths: list[str] = []
    blocked_patch_targets: list[str] = []
    review_artifacts: list[str] = []

    blocked_reviews: list[dict[str, Any]] = []
    for page in validated_plan.pages:
        if page.write_mode != "update":
            continue
        target = _resolve_materialization_target(repo_root, vault, bundle_id=bundle_id, page=page)
        schema = _schema_for_page(page, target_path=target)
        existing_frontmatter, _existing_body = _read_existing_page(target)
        existing_type = str(existing_frontmatter.get("type") or "").strip()
        parsed = parse_markdown_body(target.read_text(encoding="utf-8"))
        reason = None
        if existing_type and existing_type != page.page_type:
            reason = f"materialization target type mismatch: existing={existing_type} planned={page.page_type}"
        else:
            reason = _validate_patch_plan(page=page, target=target, schema=schema, parsed=parsed)
        if reason is None:
            continue
        backup_path = _snapshot_existing_target(repo_root=repo_root, bundle_id=bundle_id, target=target)
        review = PatchReviewArtifact(
            bundle_id=bundle_id,
            target_path=target.relative_to(repo_root).as_posix(),
            page_type=page.page_type,
            expected_schema={
                "intro_editable": schema.intro_editable,
                "section_order": list(schema.section_order),
                "section_rules": {
                    heading: list(rule.allowed_modes)
                    for heading, rule in schema.section_rules.items()
                },
            },
            discovered_headings=[section.heading for section in parsed.sections],
            reason=reason,
            intro_mode=page.intro_mode,
            intro_markdown=page.intro_markdown,
            section_operations=list(page.section_operations),
            backup_path=backup_path,
        ).model_dump(mode="json")
        blocked_reviews.append(review)

    if blocked_reviews:
        for review in blocked_reviews:
            blocked_patch_targets.append(str(review["target_path"]))
            json_path, markdown_path = _write_patch_review_artifact(bundle_dir=bundle_dir, review=review)
            review_artifacts.extend([json_path, markdown_path])
        raise PatchReviewRequiredError(blocked_reviews)

    for page in validated_plan.pages:
        target = _resolve_materialization_target(repo_root, vault, bundle_id=bundle_id, page=page)
        frontmatter = _frontmatter_for_page(page)
        write_force = force or page.write_mode == "update"
        if page.write_mode == "update":
            existing_frontmatter, _existing_body = _read_existing_page(target)
            backup_paths.append(_snapshot_existing_target(repo_root=repo_root, bundle_id=bundle_id, target=target))
            frontmatter = _merge_frontmatter(existing_frontmatter, frontmatter)
            parsed = parse_markdown_body(target.read_text(encoding="utf-8"))
            schema = _schema_for_page(page, target_path=target)
            operations: list[SectionOperation] = []
            for operation in _section_operations_from_plan(page):
                insert_after = operation.insert_after or _default_insert_after(
                    schema,
                    operation.heading,
                    [section.heading for section in parsed.sections],
                )
                operations.append(
                    SectionOperation(
                        heading=operation.heading,
                        mode=operation.mode,
                        content=operation.content,
                        insert_after=insert_after,
                    )
                )
            patched_text = apply_section_operations(
                text=target.read_text(encoding="utf-8"),
                intro_mode=page.intro_mode,
                intro_content=page.intro_markdown or "",
                section_operations=operations,
            )
            _patched_frontmatter, patched_body = _split_frontmatter(patched_text)
            write_page(
                target,
                frontmatter=frontmatter,
                body=patched_body,
                force=write_force,
            )
        else:
            if page.target_kind in FIXED_TARGET_KINDS and target.exists():
                backup_paths.append(_snapshot_existing_target(repo_root=repo_root, bundle_id=bundle_id, target=target))
                existing_frontmatter, _existing_body = _read_existing_page(target)
                frontmatter = _merge_frontmatter(existing_frontmatter, frontmatter)
                write_force = True
            write_page(
                target,
                frontmatter=frontmatter,
                body=page.body_markdown or "",
                force=write_force,
            )
        written.append(target)
        if page.target_kind == "summary":
            summary_paths.append(target)
        if page.target_kind == "decision":
            decision_page = target
    _write_changelog(vault, written)
    materialized = [path.as_posix() for path in written if path not in summary_paths and path != decision_page]
    return {
        "materialized_pages": materialized,
        "summary_pages": [path.as_posix() for path in summary_paths],
        "decision_page": decision_page.as_posix() if decision_page else None,
        "backup_paths": backup_paths,
        "blocked_patch_targets": blocked_patch_targets,
        "review_artifacts": review_artifacts,
    }


def _write_changelog(vault: Vault, paths: list[Path]) -> None:
    if not vault.changelog.exists():
        vault.changelog.write_text("# CHANGELOG\n", encoding="utf-8")
    with vault.changelog.open("a", encoding="utf-8") as handle:
        handle.write(f"\n## {_today()} — onboard\n")
        handle.write("- Materialized onboarding bundle\n")
        for path in paths:
            handle.write(f"- [[{path.stem}]]\n")
