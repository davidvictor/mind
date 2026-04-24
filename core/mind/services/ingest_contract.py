"""Shared ingestion boundary types and lifecycle runner.

This module intentionally owns only the boundary/orchestration layer:
- a stable normalized source object
- a top-level-uniform enrichment envelope
- a shared lifecycle runner

Shared durable writes and Pass D execution live in separate Phase 2 modules.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional


ContentField = str
EnvelopeDict = dict[str, Any]
PhaseHandler = Callable[["NormalizedSource", EnvelopeDict], Optional[Mapping[str, Any]]]
MaterializeHandler = Callable[["NormalizedSource", EnvelopeDict], Any]
PropagateHandler = Callable[["NormalizedSource", EnvelopeDict, Any], Any]
LifecycleStage = str

_CONTENT_FIELDS: tuple[ContentField, ...] = ("raw_text", "body_markdown", "transcript_text")
_ENVELOPE_KEYS: tuple[str, ...] = (
    "schema_version",
    "source_id",
    "pass_a",
    "pass_b",
    "pass_c",
    "pass_d",
    "verification",
    "materialization_hints",
)
_LIFECYCLE_STAGE_ORDER: tuple[LifecycleStage, ...] = (
    "pass_a",
    "pass_b",
    "pass_c",
    "pass_d",
    "materialize",
    "propagate",
)
_LIFECYCLE_STAGE_ALIASES: dict[str, LifecycleStage] = {
    "summary": "pass_a",
    "personalization": "pass_b",
    "stance": "pass_c",
    "creator-memory": "pass_c",
    "creator_memory": "pass_c",
    "substrate": "pass_d",
    "fanout": "propagate",
}


@dataclass(frozen=True)
class NormalizedSource:
    """Stable orchestration boundary for ingestion phases."""

    source_id: str
    source_kind: str
    external_id: str
    canonical_url: str
    title: str
    creator_candidates: list[dict[str, Any]] = field(default_factory=list)
    published_at: str = ""
    discovered_at: str = ""
    source_metadata: dict[str, Any] = field(default_factory=dict)
    discovered_links: list[dict[str, Any]] = field(default_factory=list)
    provenance: dict[str, Any] = field(default_factory=dict)
    raw_text: str = ""
    body_markdown: str = ""
    transcript_text: str = ""

    def __post_init__(self) -> None:
        populated = [name for name in _CONTENT_FIELDS if getattr(self, name)]
        if len(populated) != 1:
            raise ValueError(
                "NormalizedSource requires exactly one populated primary content field "
                f"from {_CONTENT_FIELDS}; got {populated or 'none'}"
            )

    @property
    def primary_content(self) -> str:
        for name in _CONTENT_FIELDS:
            value = getattr(self, name)
            if value:
                return str(value)
        raise RuntimeError("NormalizedSource has no primary content")


def make_enrichment_envelope(*, source_id: str, schema_version: int = 1) -> EnvelopeDict:
    """Return a minimal valid top-level enrichment envelope."""

    return {
        "schema_version": schema_version,
        "source_id": source_id,
        "pass_a": {},
        "pass_b": {},
        "pass_c": {},
        "pass_d": {},
        "verification": {},
        "materialization_hints": {},
    }


def parse_enrichment_envelope(
    data: Mapping[str, Any],
    *,
    expected_source_id: str | None = None,
) -> EnvelopeDict:
    """Validate and normalize the top-level enrichment envelope shape."""

    normalized = dict(data)
    missing = [key for key in _ENVELOPE_KEYS if key not in normalized]
    if missing:
        raise ValueError(f"EnrichmentEnvelope missing required keys: {missing}")
    if not isinstance(normalized["schema_version"], int):
        raise ValueError("EnrichmentEnvelope schema_version must be an int")
    if not isinstance(normalized["source_id"], str) or not normalized["source_id"]:
        raise ValueError("EnrichmentEnvelope source_id must be a non-empty string")
    if expected_source_id is not None and normalized["source_id"] != expected_source_id:
        raise ValueError(
            "EnrichmentEnvelope source_id mismatch: "
            f"expected {expected_source_id!r}, got {normalized['source_id']!r}"
        )
    for key in ("pass_a", "pass_b", "pass_c", "pass_d", "verification", "materialization_hints"):
        if not isinstance(normalized[key], dict):
            raise ValueError(f"EnrichmentEnvelope {key} must be a dict")
    return normalized


@dataclass(frozen=True)
class IngestionLifecycleResult:
    """Result of executing the shared lifecycle runner."""

    source: NormalizedSource
    envelope: EnvelopeDict
    materialized: Any = None
    propagate: Any = None


@dataclass(frozen=True)
class LifecycleHandlers:
    understand: PhaseHandler | None = None
    personalize: PhaseHandler | None = None
    attribute: PhaseHandler | None = None
    distill: PhaseHandler | None = None
    materialize: MaterializeHandler | None = None
    propagate: PropagateHandler | None = None


def normalize_lifecycle_stage(stage: str) -> LifecycleStage:
    normalized = _LIFECYCLE_STAGE_ALIASES.get(stage, stage)
    if normalized not in _LIFECYCLE_STAGE_ORDER:
        raise ValueError(
            f"unsupported lifecycle stage {stage!r}; expected one of "
            f"{', '.join(_LIFECYCLE_STAGE_ORDER)}"
        )
    return normalized


def _apply_phase(
    *,
    source: NormalizedSource,
    envelope: EnvelopeDict,
    phase_key: str,
    handler: PhaseHandler | None,
) -> EnvelopeDict:
    if handler is None:
        return envelope
    handler_input = deepcopy(envelope)
    payload = dict(handler(source, handler_input) or {})
    next_envelope = dict(envelope)
    materialization_hints = payload.pop("materialization_hints", None)
    verification = payload.pop("verification", None)
    next_envelope[phase_key] = payload
    if materialization_hints is not None:
        merged_hints = dict(next_envelope.get("materialization_hints") or {})
        merged_hints.update(dict(materialization_hints))
        next_envelope["materialization_hints"] = merged_hints
    if verification is not None:
        merged_verification = dict(next_envelope.get("verification") or {})
        merged_verification.update(dict(verification))
        next_envelope["verification"] = merged_verification
    return parse_enrichment_envelope(next_envelope, expected_source_id=source.source_id)


def run_ingestion_lifecycle(
    *,
    source: NormalizedSource,
    understand: PhaseHandler | None = None,
    personalize: PhaseHandler | None = None,
    attribute: PhaseHandler | None = None,
    distill: PhaseHandler | None = None,
    materialize: MaterializeHandler | None = None,
    propagate: PropagateHandler | None = None,
) -> IngestionLifecycleResult:
    """Run the canonical top-level ingestion lifecycle in order."""

    return run_ingestion_window(
        source=source,
        handlers=LifecycleHandlers(
            understand=understand,
            personalize=personalize,
            attribute=attribute,
            distill=distill,
            materialize=materialize,
            propagate=propagate,
        ),
    )


def run_ingestion_window(
    *,
    source: NormalizedSource,
    handlers: LifecycleHandlers,
    start_stage: str = "pass_a",
    through_stage: str = "propagate",
    seed_envelope: Mapping[str, Any] | None = None,
) -> IngestionLifecycleResult:
    """Run a seeded lifecycle window from start_stage through through_stage."""

    start = normalize_lifecycle_stage(start_stage)
    through = normalize_lifecycle_stage(through_stage)
    start_index = _LIFECYCLE_STAGE_ORDER.index(start)
    through_index = _LIFECYCLE_STAGE_ORDER.index(through)
    if start_index > through_index:
        raise ValueError(f"invalid lifecycle window: start_stage={start!r} is after through_stage={through!r}")

    base_envelope = dict(seed_envelope) if seed_envelope is not None else make_enrichment_envelope(source_id=source.source_id)
    envelope = parse_enrichment_envelope(base_envelope, expected_source_id=source.source_id)

    phase_handlers: tuple[tuple[str, PhaseHandler | None], ...] = (
        ("pass_a", handlers.understand),
        ("pass_b", handlers.personalize),
        ("pass_c", handlers.attribute),
        ("pass_d", handlers.distill),
    )
    for phase_key, handler in phase_handlers:
        phase_index = _LIFECYCLE_STAGE_ORDER.index(phase_key)
        if start_index <= phase_index <= through_index:
            envelope = _apply_phase(source=source, envelope=envelope, phase_key=phase_key, handler=handler)

    materialized = handlers.materialize(source, envelope) if handlers.materialize is not None and start_index <= _LIFECYCLE_STAGE_ORDER.index("materialize") <= through_index else None
    propagate_result = None
    if handlers.propagate is not None and start_index <= _LIFECYCLE_STAGE_ORDER.index("propagate") <= through_index:
        try:
            propagate_result = handlers.propagate(source, envelope, materialized)
        except Exception as exc:
            propagate_result = {
                "fanout_outcomes": [
                    {
                        "status": "error",
                        "stage": "propagate",
                        "summary": f"{type(exc).__name__}: {exc}",
                    }
                ]
            }
    return IngestionLifecycleResult(
        source=source,
        envelope=envelope,
        materialized=materialized,
        propagate=propagate_result,
    )
