from __future__ import annotations

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field, model_validator


class AuthRequest(BaseModel):
    auth_token: Optional[str] = None
    session_id: Optional[str] = None


class RuntimeStatusResponse(BaseModel):
    db_path: str
    schema_version: str
    active_locks: int
    run_count: int
    queue_entries: int
    tracked_skills: int
    last_light: Optional[str]
    last_deep: Optional[str]
    last_rem: Optional[str]
    light_passes_since_deep: int
    deep_passes_since_rem: int


class RunSummary(BaseModel):
    id: int
    kind: str
    status: str
    holder: Optional[str]
    started_at: str
    finished_at: Optional[str]
    notes: Optional[str]
    queue_name: Optional[str] = None
    item_ref: Optional[str] = None
    retry_count: int = 0
    next_attempt_at: Optional[str] = None


class RunEventView(BaseModel):
    id: int
    stage: str
    event_type: str
    message: Optional[str]
    created_at: str


class ErrorView(BaseModel):
    id: int
    stage: Optional[str]
    error_type: str
    message: str
    created_at: str


class RunDetailsResponse(BaseModel):
    run: RunSummary
    events: List[RunEventView]
    errors: List[ErrorView]


class MemoryMatch(BaseModel):
    page_id: str
    title: str
    path: str
    score: float
    snippet: str


class SkillListItem(BaseModel):
    skill_id: str
    path: str
    usage_count: int = 0
    artifact_count: int = 0
    last_used_at: Optional[str] = None


class SkillReadResponse(BaseModel):
    skill_id: str
    path: str
    content: str


class QueueItemResponse(BaseModel):
    name: str
    status: str
    pending_count: int
    last_item_ref: Optional[str]
    last_run_id: Optional[int]
    updated_at: str


class EnqueueResponse(BaseModel):
    run_id: int
    queue_name: str
    status: str = "queued"


class SearchMemoryRequest(AuthRequest):
    query: str
    limit: int = Field(default=8, ge=1, le=50)


class ReadSkillRequest(AuthRequest):
    skill_id: str


class EnqueueLinksRequest(AuthRequest):
    links: List[Dict[str, str]]
    today: Optional[str] = None


class StartIngestRequest(AuthRequest):
    kind: Literal["file", "youtube", "books", "substack", "audible", "articles", "links"]
    path: Optional[str] = None
    today: Optional[str] = None
    options: Optional[Dict[str, Optional[object]]] = None

    @model_validator(mode="after")
    def validate_required_path(self) -> "StartIngestRequest":
        if self.kind in {"file", "youtube", "books", "links"} and not (self.path or "").strip():
            raise ValueError(f"start_ingest path is required for kind={self.kind}")
        return self


class StartReingestRequest(AuthRequest):
    lane: Literal["youtube", "books", "articles", "substack"]
    path: Optional[str] = None
    today: Optional[str] = None
    stage: str = "acquire"
    through: str = "propagate"
    limit: Optional[int] = None
    source_ids: List[str] = Field(default_factory=list)
    dry_run: bool = True

    @model_validator(mode="after")
    def validate_stages(self) -> "StartReingestRequest":
        aliases = {"summary": "pass_a", "personalization": "pass_b", "stance": "pass_c"}
        order = ("acquire", "pass_a", "pass_b", "pass_c", "pass_d", "materialize", "propagate")
        stage = aliases.get(self.stage, self.stage)
        through = aliases.get(self.through, self.through)
        if stage not in order:
            raise ValueError(f"unsupported reingest stage: {self.stage}")
        if through not in order:
            raise ValueError(f"unsupported reingest through stage: {self.through}")
        if order.index(stage) > order.index(through):
            raise ValueError(f"invalid reingest stage window: {self.stage} -> {self.through}")
        return self


class GraphHealthRequest(AuthRequest):
    skip_promotion_gate: bool = False


class IngestReadinessRequest(AuthRequest):
    dropbox_limit: Optional[int] = Field(default=None, ge=1)
    lane_limit: Optional[int] = Field(default=None, ge=1)
    include_promotion_gate: bool = False


class StartArticleRepairRequest(AuthRequest):
    path: Optional[str] = None
    today: Optional[str] = None
    limit: Optional[int] = Field(default=None, ge=1)
    source_ids: List[str] = Field(default_factory=list)
    apply: bool = False


class StartDreamRequest(AuthRequest):
    dry_run: bool = False


class StartDreamBootstrapRequest(AuthRequest):
    dry_run: bool = False
    force_pass_d: bool = False
    checkpoint_every: Optional[int] = None
    resume: bool = False
    limit: Optional[int] = None


class GenerateSkillRequest(AuthRequest):
    prompt: str
    name: Optional[str] = None
    description: Optional[str] = None
    context: str = ""


class SetSkillStatusRequest(AuthRequest):
    skill_id: str
    status: str


class RetryQueueItemRequest(AuthRequest):
    run_id: int = Field(ge=1)


class ClearStaleLockRequest(AuthRequest):
    lock_name: str = "brain"


class RunOnboardRequest(AuthRequest):
    input_path: str
    force: bool = False
