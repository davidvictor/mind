from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from scripts.atoms.pass_d import pass_d_cache_path, stage_outcomes_from_payload
from scripts.common.vault import raw_path

from mind.services.ingest_contract import IngestionLifecycleResult, NormalizedSource

QUALITY_RECEIPT_VERSION = 2


def quality_receipt_path(*, repo_root: Path, lane: str, source_id: str) -> Path:
    return raw_path(repo_root, "transcripts", lane, f"{source_id}.quality.json")


def load_quality_receipt(*, repo_root: Path, lane: str, source_id: str) -> dict[str, Any] | None:
    path = quality_receipt_path(repo_root=repo_root, lane=lane, source_id=source_id)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def write_quality_receipt(
    *,
    repo_root: Path,
    result: IngestionLifecycleResult,
    executed_at: str,
) -> Path:
    source = getattr(result, "source", None)
    envelope = getattr(result, "envelope", None)
    if not isinstance(source, NormalizedSource) or not isinstance(envelope, Mapping):
        fallback_lane = str(getattr(source, "source_kind", "quality") or "quality")
        fallback_source_id = str(getattr(source, "source_id", "compatibility-skip") or "compatibility-skip")
        return quality_receipt_path(repo_root=repo_root, lane=fallback_lane, source_id=fallback_source_id)
    receipt = build_quality_receipt(
        repo_root=repo_root,
        source=source,
        envelope=envelope,
        propagate=getattr(result, "propagate", None),
        materialized=getattr(result, "materialized", None),
        executed_at=executed_at,
    )
    path = quality_receipt_path(
        repo_root=repo_root,
        lane=source.source_kind,
        source_id=source.source_id,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(receipt, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def build_quality_receipt(
    *,
    repo_root: Path,
    source: NormalizedSource,
    envelope: Mapping[str, Any],
    propagate: Any,
    materialized: Any,
    executed_at: str,
) -> dict[str, Any]:
    pass_a = dict(envelope.get("pass_a") or {})
    pass_d = dict(envelope.get("pass_d") or {})
    summary = dict(pass_a.get("summary") or {})
    propagate_payload = dict(propagate or {}) if isinstance(propagate, Mapping) else {}
    pass_d_outcomes = propagate_payload.get("pass_d")
    if not isinstance(pass_d_outcomes, list):
        pass_d_outcomes = stage_outcomes_from_payload(pass_d)

    pass_d_errors = [item for item in pass_d_outcomes if str(item.get("status") or "") == "error"]
    pass_d_warnings = [item for item in pass_d_outcomes if str(item.get("status") or "") == "warning"]
    pass_d_warning_count = sum(len(item.get("warnings") or []) for item in pass_d_warnings)

    fanout_outcomes_raw = propagate_payload.get("fanout_outcomes") or []
    fanout_outcomes = [item for item in fanout_outcomes_raw if isinstance(item, Mapping)]
    fanout_errors = [item for item in fanout_outcomes if _is_error_outcome(item)]
    fanout_warnings = [item for item in fanout_outcomes if str(item.get("status") or "").strip().lower() == "warning"]
    propagate_status = "error" if fanout_errors else ("warning" if fanout_warnings else "ok")
    propagate_detail = (
        str((fanout_errors or fanout_warnings)[0].get("summary") or "").strip()
        if (fanout_errors or fanout_warnings)
        else ""
    )

    quote_claim_count = 0
    quote_unverified_count = 0
    for claim in summary.get("key_claims") or []:
        if not isinstance(claim, Mapping):
            continue
        if claim.get("claim") or claim.get("evidence_quote"):
            quote_claim_count += 1
        if bool(claim.get("quote_unverified")):
            quote_unverified_count += 1

    fanout_discovered_count = _coerce_int(
        propagate_payload.get("fanout_discovered_count", propagate_payload.get("propagate_discovered_count"))
    )
    fanout_queued_count = _coerce_int(
        propagate_payload.get("fanout_queued_count", propagate_payload.get("propagate_queued_count"))
    )
    logged_entity_count = _coerce_int(propagate_payload.get("logged_entity_count"))
    if logged_entity_count is None:
        logged_entities = propagate_payload.get("logged_entities") or []
        if isinstance(logged_entities, list):
            logged_entity_count = len(logged_entities)

    route_identity = _read_pass_d_identity(
        repo_root=repo_root,
        lane=source.source_kind,
        source_id=source.source_id,
    )
    source_grounded = _source_grounded(source=source, envelope=envelope, propagate_payload=propagate_payload)

    parity_features = {
        "quote_verification_supported": source.source_kind in {"youtube", "article", "substack"} or source_grounded is True,
        "pass_d_outcomes_exposed": bool(pass_d_outcomes) or bool(pass_d),
        "entity_logging_supported": "logged_entities" in propagate_payload or "logged_entity_count" in propagate_payload,
        "fanout_count_supported": fanout_discovered_count is not None and fanout_queued_count is not None,
        "context_reuse_supported": any(
            key in pass_a
            for key in ("prior_context", "stance_context", "creator_context", "channel_context")
        ),
    }

    return {
        "receipt_version": QUALITY_RECEIPT_VERSION,
        "source_id": source.source_id,
        "source_kind": source.source_kind,
        "source_date": source.published_at or source.discovered_at or executed_at[:10],
        "executed_at": executed_at,
        "route_identity": route_identity,
        "source_grounded": source_grounded,
        "pass_d_status": "error" if pass_d_errors else ("warning" if pass_d_warnings else "ok"),
        "pass_d_warning_count": pass_d_warning_count,
        "propagate_status": propagate_status,
        "propagate_detail": propagate_detail,
        "pass_d_dropped_items": _coerce_int(pass_d.get("dropped_q1_matches"), default=0)
        + _coerce_int(pass_d.get("dropped_q2_candidates"), default=0),
        "quote_claim_count": quote_claim_count,
        "quote_unverified_count": quote_unverified_count,
        "entity_logged_count": logged_entity_count or 0,
        "fanout_discovered_count": fanout_discovered_count,
        "fanout_queued_count": fanout_queued_count,
        "parity_features": parity_features,
        "materialized_paths": _materialized_paths(materialized),
    }


def _read_pass_d_identity(*, repo_root: Path, lane: str, source_id: str) -> dict[str, Any] | None:
    path = pass_d_cache_path(repo_root=repo_root, source_kind=lane, source_id=source_id)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    identity = payload.get("_llm")
    return identity if isinstance(identity, dict) else None


def _source_grounded(
    *,
    source: NormalizedSource,
    envelope: Mapping[str, Any],
    propagate_payload: Mapping[str, Any],
) -> bool | None:
    if source.source_kind == "youtube":
        verification = dict(envelope.get("verification") or {})
        return bool(str(verification.get("transcription_path") or propagate_payload.get("transcription_path") or "").strip())
    if source.source_kind == "book":
        return str(propagate_payload.get("source_kind") or source.provenance.get("source_kind") or "").strip() in {"document", "audio"}
    return None


def _materialized_paths(materialized: Any) -> list[str]:
    if not isinstance(materialized, Mapping):
        return []
    paths: list[str] = []
    for value in materialized.values():
        text = str(value or "").strip()
        if text:
            paths.append(text)
    return paths


def _coerce_int(value: object, default: int | None = None) -> int | None:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _is_error_outcome(outcome: Mapping[str, Any]) -> bool:
    status = str(outcome.get("status") or "").strip().lower()
    if status == "error":
        return True
    if status == "warning":
        return False
    stage = str(outcome.get("stage") or "").strip().lower()
    summary = str(outcome.get("summary") or "").strip()
    if stage == "propagate":
        return True
    return any(token in summary for token in ("Error:", "Exception:", "RuntimeError", "TypeError", "ValueError", "NameError"))
