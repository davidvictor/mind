from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from mind.services.llm_service import get_llm_service
from mind.services.llm_cache import identity_matches
from mind.services.quality_receipts import QUALITY_RECEIPT_VERSION, load_quality_receipt, quality_receipt_path
from scripts.atoms.pass_d import pass_d_cache_identities, pass_d_cache_path, stage_outcomes_from_payload
from scripts.atoms.prompts import PASS_D_PROMPT_VERSION
from scripts.common.vault import raw_path

from .common import read_page, source_pages, vault

QUALITY_ADAPTER = "dream.quality"
QUALITY_REPORT_DIR = ("raw", "reports", "dream", "quality")
WINDOW_SIZE = 30
MINIMUM_SAMPLE_SIZE = 10
CANONICAL_LANES = ("youtube", "book", "article", "substack")
LANE_DISPLAY = {
    "youtube": "YouTube",
    "book": "Books",
    "article": "Articles",
    "substack": "Substack",
}
TRUSTED_STATES = {"trusted", "legacy"}
LEGACY_STATE = "legacy"
QUOTE_BEARING_LANES = {"youtube", "article", "substack"}
PARITY_FEATURES = {
    "youtube": {
        "quote_verification_supported": True,
        "pass_d_outcomes_exposed": True,
        "entity_logging_supported": True,
        "fanout_count_supported": True,
        "context_reuse_supported": True,
    },
    "book": {
        "quote_verification_supported": True,
        "pass_d_outcomes_exposed": True,
        "entity_logging_supported": True,
        "fanout_count_supported": False,
        "context_reuse_supported": True,
    },
    "article": {
        "quote_verification_supported": True,
        "pass_d_outcomes_exposed": True,
        "entity_logging_supported": True,
        "fanout_count_supported": True,
        "context_reuse_supported": True,
    },
    "substack": {
        "quote_verification_supported": True,
        "pass_d_outcomes_exposed": True,
        "entity_logging_supported": True,
        "fanout_count_supported": True,
        "context_reuse_supported": True,
    },
}
GROUNDING_THRESHOLDS = {
    "youtube": {"trusted": 0.80, "partial": 0.60},
    "book": {"trusted": 0.60, "partial": 0.40},
}
STATE_STAGE_BEHAVIOR = {
    "trusted": {
        "bootstrap": "full-replay",
        "light": "full-mutations",
        "deep": "promotion-eligible",
    },
    "partial-fidelity": {
        "bootstrap": "degraded-replay",
        "light": "informational-only",
        "deep": "review-only",
    },
    "bootstrap-only": {
        "bootstrap": "explicit-replay-only",
        "light": "excluded",
        "deep": "hold-only",
    },
    "blocked": {
        "bootstrap": "excluded",
        "light": "excluded",
        "deep": "excluded",
    },
    LEGACY_STATE: {
        "bootstrap": "full-replay",
        "light": "full-mutations",
        "deep": "promotion-eligible",
    },
}


@dataclass(frozen=True)
class LaneSourceRecord:
    summary_id: str
    lane: str
    source_date: str
    source_grounded: bool | None
    pass_d_success: bool
    route_policy_compliant: bool
    missing_essential_receipt: bool
    total_claims: int
    unverified_claims: int
    entity_count: int
    fanout_discovered_count: int | None
    fanout_queued_count: int | None
    parity_features: dict[str, bool]


def canonical_lane_from_source_type(value: object) -> str | None:
    normalized = str(value or "").strip().lower()
    if normalized == "video":
        return "youtube"
    if normalized in CANONICAL_LANES:
        return normalized
    return None


def lane_state_for_frontmatter(frontmatter: dict[str, Any], quality_payload: dict[str, Any] | None) -> str:
    lane = canonical_lane_from_source_type(frontmatter.get("source_type") or frontmatter.get("source_kind"))
    if lane is None:
        return LEGACY_STATE
    if not quality_payload:
        return LEGACY_STATE
    lane_payload = (quality_payload.get("lanes") or {}).get(lane) or {}
    return str(lane_payload.get("state") or LEGACY_STATE)


def lane_state_for_summary_id(summary_id: str, quality_payload: dict[str, Any] | None) -> str:
    if not quality_payload or not summary_id:
        return LEGACY_STATE
    summary_path = vault().wiki / "summaries" / f"{summary_id}.md"
    if not summary_path.exists():
        return LEGACY_STATE
    frontmatter, _body = read_page(summary_path)
    return lane_state_for_frontmatter(frontmatter, quality_payload)


def supports_full_dream_mutation(state: str) -> bool:
    return state in TRUSTED_STATES


def stage_behavior_for_lane_state(stage: str, state: str) -> str:
    stage_map = STATE_STAGE_BEHAVIOR.get(state) or STATE_STAGE_BEHAVIOR[LEGACY_STATE]
    return stage_map.get(stage, "full-mutations")


def explain_lane_state(lane: str, quality_payload: dict[str, Any] | None) -> str:
    display = LANE_DISPLAY.get(lane, lane)
    if not quality_payload:
        return f"{display}: legacy/no quality snapshot"
    lane_payload = (quality_payload.get("lanes") or {}).get(lane) or {}
    state = str(lane_payload.get("state") or LEGACY_STATE)
    reasons = ", ".join(str(item) for item in lane_payload.get("reasons") or [])
    if reasons:
        return f"{display}: {state} ({reasons})"
    return f"{display}: {state}"


def blocked_lane_summaries(quality_payload: dict[str, Any] | None) -> list[str]:
    if not quality_payload:
        return []
    blocked: list[str] = []
    for lane in CANONICAL_LANES:
        lane_payload = (quality_payload.get("lanes") or {}).get(lane) or {}
        if str(lane_payload.get("state") or "") == "blocked":
            blocked.append(explain_lane_state(lane, quality_payload))
    return blocked


def degraded_lane_summaries(quality_payload: dict[str, Any] | None) -> list[str]:
    if not quality_payload:
        return []
    degraded: list[str] = []
    for lane in CANONICAL_LANES:
        lane_payload = (quality_payload.get("lanes") or {}).get(lane) or {}
        state = str(lane_payload.get("state") or "")
        if state in {"partial-fidelity", "bootstrap-only"}:
            degraded.append(explain_lane_state(lane, quality_payload))
    return degraded


def evaluate_and_persist_quality(
    *,
    persist: bool,
    report_key: str,
) -> dict[str, Any]:
    v = vault()
    acceptable_identities = _acceptable_dream_identities()
    grouped: dict[str, list[LaneSourceRecord]] = {lane: [] for lane in CANONICAL_LANES}
    for path in source_pages(v):
        frontmatter, _body = read_page(path)
        lane = canonical_lane_from_source_type(frontmatter.get("source_type") or frontmatter.get("source_kind"))
        if lane is None:
            continue
        grouped[lane].append(
            _build_lane_record(
                v.root,
                path=path,
                frontmatter=frontmatter,
                acceptable_identities=acceptable_identities,
                persist_receipt=persist,
            )
        )

    lanes_payload: dict[str, Any] = {}
    for lane, records in grouped.items():
        ordered = sorted(records, key=lambda item: (item.source_date or "", item.summary_id), reverse=True)[:WINDOW_SIZE]
        lanes_payload[lane] = _score_lane(lane, ordered)

    payload: dict[str, Any] = {
        "evaluated_at": _utc_now(),
        "window_size": WINDOW_SIZE,
        "minimum_sample_size": MINIMUM_SAMPLE_SIZE,
        "report_path": None,
        "lanes": lanes_payload,
    }
    if persist:
        report_path = _write_quality_report(v.root, payload=payload, report_key=report_key)
        payload["report_path"] = v.logical_path(report_path)
        from .common import runtime_state

        runtime_state().upsert_adapter_state(adapter=QUALITY_ADAPTER, state=payload)
    return payload


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _acceptable_dream_identities() -> list[dict[str, Any]]:
    try:
        return [identity.to_dict() for identity in pass_d_cache_identities(get_llm_service())]
    except Exception:
        return []


def _build_lane_record(
    repo_root: Path,
    *,
    path: Path,
    frontmatter: dict[str, Any],
    acceptable_identities: list[dict[str, Any]],
    persist_receipt: bool,
) -> LaneSourceRecord:
    lane = canonical_lane_from_source_type(frontmatter.get("source_type") or frontmatter.get("source_kind"))
    assert lane is not None
    identifiers = _source_identifiers(path=path, frontmatter=frontmatter, lane=lane)
    receipt = _load_or_build_receipt(
        repo_root=repo_root,
        lane=lane,
        identifiers=identifiers,
        frontmatter=frontmatter,
        persist_receipt=persist_receipt,
    )
    identity = receipt.get("route_identity")
    pass_d_success = str(receipt.get("pass_d_status") or "") in {"ok", "warning"}
    route_policy_compliant = True
    if acceptable_identities and isinstance(identity, dict):
        route_policy_compliant = any(identity_matches(identity, candidate) for candidate in acceptable_identities)
    missing_essential = bool(receipt.get("synthesized")) and identity is None

    total_claims = int(receipt.get("quote_claim_count") or 0)
    unverified_claims = int(receipt.get("quote_unverified_count") or 0)
    entity_count = int(receipt.get("entity_logged_count") or 0)
    fanout_discovered_count = _as_optional_int(receipt.get("fanout_discovered_count"))
    fanout_queued_count = _as_optional_int(receipt.get("fanout_queued_count"))
    parity_features = dict(receipt.get("parity_features") or PARITY_FEATURES.get(lane, {}))

    return LaneSourceRecord(
        summary_id=identifiers["summary_source_id"],
        lane=lane,
        source_date=str(frontmatter.get("source_date") or frontmatter.get("last_updated") or frontmatter.get("created") or ""),
        source_grounded=receipt.get("source_grounded"),
        pass_d_success=pass_d_success,
        route_policy_compliant=route_policy_compliant,
        missing_essential_receipt=missing_essential,
        total_claims=total_claims,
        unverified_claims=unverified_claims,
        entity_count=entity_count,
        fanout_discovered_count=fanout_discovered_count,
        fanout_queued_count=fanout_queued_count,
        parity_features=parity_features,
    )


def _source_identifiers(*, path: Path, frontmatter: dict[str, Any], lane: str) -> dict[str, str]:
    summary_source_id = str(frontmatter.get("id") or path.stem)
    external_id = str(frontmatter.get("external_id") or "").strip()
    if lane == "youtube":
        source_path = str(frontmatter.get("source_path") or "")
        base = external_id.removeprefix("youtube-") or Path(source_path).stem.removeprefix("summary-") or summary_source_id.removeprefix("summary-")
        return {
            "summary_source_id": summary_source_id,
            "quote_source_id": base,
            "pass_d_source_id": f"youtube-{base}",
        }
    if lane == "substack":
        source_path = str(frontmatter.get("source_path") or "")
        base = external_id.removeprefix("substack-") or Path(source_path).stem or summary_source_id.removeprefix("summary-")
        return {
            "summary_source_id": summary_source_id,
            "quote_source_id": base,
            "pass_d_source_id": f"substack-{base}",
        }
    if lane == "article":
        source_path = str(frontmatter.get("source_path") or "")
        base = Path(source_path).stem or summary_source_id.removeprefix("summary-")
        return {
            "summary_source_id": summary_source_id,
            "quote_source_id": base,
            "pass_d_source_id": f"article-{base}",
        }
    source_path = str(frontmatter.get("source_path") or "")
    source_page_id = str(frontmatter.get("id") or "").strip()
    base = (
        source_page_id.removeprefix("summary-book-").removeprefix("summary-")
        or Path(Path(source_path).stem).stem.removeprefix("summary-book-").removeprefix("summary-")
        or summary_source_id.removeprefix("summary-book-").removeprefix("summary-")
    )
    return {
        "summary_source_id": summary_source_id,
        "quote_source_id": base,
        "pass_d_source_id": f"book-{base}",
    }


def _source_grounded(repo_root: Path, *, lane: str, frontmatter: dict[str, Any], source_id: str) -> bool | None:
    if lane == "youtube":
        payload = _cache_data(_load_json(raw_path(repo_root, "transcripts", "youtube", f"{source_id}.transcription.json"))) or {}
        transcript = str(payload.get("transcript") or "").strip()
        return bool(transcript and str(payload.get("transcription_path") or "").strip())
    if lane == "book":
        source_kind = str(frontmatter.get("source_kind") or "").strip().lower()
        return source_kind in {"document", "audio"}
    return None


def _quote_metrics(repo_root: Path, *, lane: str, source_id: str) -> tuple[int, int]:
    payload = None
    if lane == "youtube":
        payload = _cache_data(_load_json(raw_path(repo_root, "transcripts", "youtube", f"{source_id}.json")))
    elif lane == "article":
        payload = _cache_data(_load_json(raw_path(repo_root, "transcripts", "articles", f"{source_id}.json")))
    elif lane == "substack":
        payload = _cache_data(_load_json(raw_path(repo_root, "transcripts", "substack", f"{source_id}.json")))
    claims = payload.get("key_claims") if isinstance(payload, dict) else []
    if not isinstance(claims, list):
        claims = []
    total_claims = sum(1 for claim in claims if isinstance(claim, dict) and (claim.get("claim") or claim.get("evidence_quote")))
    unverified = 0
    for claim in claims:
        if isinstance(claim, dict) and bool(claim.get("quote_unverified")):
            unverified += 1
    sidecar = _load_json(raw_path(repo_root, "transcripts", lane, f"{source_id}.quote-warnings.json")) or {}
    if isinstance(sidecar, dict):
        sidecar_claims = sidecar.get("unverified_claims") or []
        if total_claims == 0 and sidecar_claims:
            total_claims = len(sidecar_claims)
        unverified = max(unverified, len(sidecar_claims) if isinstance(sidecar_claims, list) else 0)
    if total_claims == 0 and lane not in QUOTE_BEARING_LANES:
        return 0, 0
    return total_claims, unverified


def _entity_count(repo_root: Path, *, lane: str, source_id: str) -> int:
    payload = None
    if lane == "youtube":
        payload = _cache_data(_load_json(raw_path(repo_root, "transcripts", "youtube", f"{source_id.removeprefix('summary-')}.json")))
    elif lane == "article":
        payload = _cache_data(_load_json(raw_path(repo_root, "transcripts", "articles", f"{source_id.removeprefix('summary-')}.json")))
    elif lane == "substack":
        payload = _cache_data(_load_json(raw_path(repo_root, "transcripts", "substack", f"{source_id.removeprefix('summary-')}.json")))
    entities = payload.get("entities") if isinstance(payload, dict) else {}
    if not isinstance(entities, dict):
        return 0
    count = 0
    for values in entities.values():
        if isinstance(values, list):
            count += sum(1 for item in values if str(item).strip())
    return count


def _load_or_build_receipt(
    *,
    repo_root: Path,
    lane: str,
    identifiers: dict[str, str],
    frontmatter: dict[str, Any],
    persist_receipt: bool,
) -> dict[str, Any]:
    source_id = identifiers["pass_d_source_id"]
    receipt = load_quality_receipt(repo_root=repo_root, lane=lane, source_id=source_id)
    if isinstance(receipt, dict) and _receipt_is_current(receipt):
        return receipt
    synthesized = _synthesize_receipt(
        repo_root=repo_root,
        lane=lane,
        identifiers=identifiers,
        frontmatter=frontmatter,
    )
    if persist_receipt:
        path = quality_receipt_path(repo_root=repo_root, lane=lane, source_id=source_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(synthesized, indent=2, ensure_ascii=False), encoding="utf-8")
    return synthesized


def _receipt_is_current(receipt: dict[str, Any]) -> bool:
    try:
        return int(receipt.get("receipt_version") or 0) >= QUALITY_RECEIPT_VERSION
    except (TypeError, ValueError):
        return False


def _synthesize_receipt(
    *,
    repo_root: Path,
    lane: str,
    identifiers: dict[str, str],
    frontmatter: dict[str, Any],
) -> dict[str, Any]:
    pass_d_payload = _load_json(pass_d_cache_path(repo_root=repo_root, source_kind=lane, source_id=identifiers["pass_d_source_id"]))
    pass_d_data = _cache_data(pass_d_payload)
    pass_d_outcomes = stage_outcomes_from_payload(pass_d_data if isinstance(pass_d_data, dict) else {})
    pass_d_status = "error" if any(str(item.get("status") or "") == "error" for item in pass_d_outcomes) else (
        "warning" if pass_d_outcomes else ("ok" if pass_d_payload else "missing")
    )
    total_claims, unverified_claims = _quote_metrics(repo_root, lane=lane, source_id=identifiers["quote_source_id"])
    entity_count = _entity_count(repo_root, lane=lane, source_id=identifiers["summary_source_id"])
    return {
        "source_id": identifiers["pass_d_source_id"],
        "source_kind": lane,
        "source_date": str(frontmatter.get("source_date") or frontmatter.get("last_updated") or frontmatter.get("created") or ""),
        "route_identity": _cache_identity(pass_d_payload),
        "source_grounded": _source_grounded(repo_root, lane=lane, frontmatter=frontmatter, source_id=identifiers["quote_source_id"]),
        "pass_d_status": pass_d_status,
        "quote_claim_count": total_claims,
        "quote_unverified_count": unverified_claims,
        "entity_logged_count": entity_count,
        "fanout_discovered_count": None,
        "fanout_queued_count": None,
        "parity_features": PARITY_FEATURES.get(lane, {}),
        "synthesized": True,
    }


def _score_lane(lane: str, records: list[LaneSourceRecord]) -> dict[str, Any]:
    recent_sources = len(records)
    parity_features = _merge_parity_features(records, lane=lane)
    metrics = {
        "source_grounded_coverage": _coverage(
            numerator=sum(1 for item in records if item.source_grounded is True),
            denominator=sum(1 for item in records if item.source_grounded is not None),
        ),
        "pass_d_success_rate": _coverage(
            numerator=sum(1 for item in records if item.pass_d_success),
            denominator=recent_sources,
        ),
        "quote_verification_coverage": _quote_coverage(records),
        "entity_log_yield": _coverage(
            numerator=sum(1 for item in records if item.entity_count > 0),
            denominator=recent_sources,
        ),
        "fanout_yield": _fanout_yield(records),
        "route_policy_compliance": _coverage(
            numerator=sum(1 for item in records if item.route_policy_compliant),
            denominator=recent_sources,
        ),
        "required_receipt_completeness": _coverage(
            numerator=sum(1 for item in records if not item.missing_essential_receipt),
            denominator=recent_sources,
        ),
    }
    parity_status = _parity_status(parity_features)
    reasons: list[str] = []

    if recent_sources == 0:
        reasons.append("no_recent_sources")
        state = "blocked"
    elif recent_sources < MINIMUM_SAMPLE_SIZE:
        reasons.append("insufficient_sample_size")
        state = "bootstrap-only"
    elif (metrics["pass_d_success_rate"] or 0.0) < 0.80:
        reasons.append("pass_d_unstable")
        state = "blocked"
    elif (metrics["route_policy_compliance"] or 0.0) < 0.80:
        reasons.append("route_policy_stale")
        state = "blocked"
    elif (metrics["required_receipt_completeness"] or 0.0) < 0.80:
        reasons.append("missing_required_receipts")
        state = "blocked"
    else:
        trusted = True
        partial = False

        grounding = metrics["source_grounded_coverage"]
        if lane in GROUNDING_THRESHOLDS:
            thresholds = GROUNDING_THRESHOLDS[lane]
            if grounding is None or grounding < thresholds["trusted"]:
                trusted = False
                reasons.append("source_grounding_low")
                if grounding is None or grounding < thresholds["partial"]:
                    partial = True

        quote_coverage = metrics["quote_verification_coverage"]
        if lane in QUOTE_BEARING_LANES:
            if quote_coverage is None:
                trusted = False
                reasons.append("quote_coverage_missing")
            elif quote_coverage < 0.80:
                trusted = False
                reasons.append("quote_coverage_low")
                if quote_coverage < 0.50:
                    partial = True

        if (metrics["entity_log_yield"] or 0.0) < 0.25:
            trusted = False
            reasons.append("entity_yield_low")

        if parity_features.get("fanout_count_supported") and (metrics["fanout_yield"] or 0.0) < 0.20:
            trusted = False
            reasons.append("fanout_yield_low")
            if (metrics["fanout_yield"] or 0.0) < 0.10:
                partial = True

        if parity_status not in {"near_parity", "parity_ready"} and lane != "substack":
            trusted = False
            reasons.append("parity_gap")

        if (metrics["pass_d_success_rate"] or 0.0) < 0.95 and "pass_d_unstable" not in reasons:
            trusted = False
            reasons.append("pass_d_below_trusted")
        if (metrics["route_policy_compliance"] or 0.0) < 0.95 and "route_policy_stale" not in reasons:
            trusted = False
            reasons.append("route_policy_below_trusted")

        if trusted:
            state = "trusted"
        elif partial:
            state = "bootstrap-only"
        else:
            state = "partial-fidelity"

    return {
        "state": state,
        "reasons": list(dict.fromkeys(reasons)),
        "recent_sources": recent_sources,
        "metrics": metrics,
        "parity_status": parity_status,
        "parity_features": parity_features,
        "sample_source_ids": [item.summary_id for item in records[:10]],
    }


def _coverage(*, numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def _quote_coverage(records: list[LaneSourceRecord]) -> float | None:
    total_claims = sum(item.total_claims for item in records)
    if total_claims <= 0:
        return None
    verified_claims = total_claims - sum(item.unverified_claims for item in records)
    return round(verified_claims / total_claims, 4)


def _fanout_yield(records: list[LaneSourceRecord]) -> float | None:
    supported = [item for item in records if item.fanout_queued_count is not None]
    if not supported:
        return None
    return _coverage(
        numerator=sum(1 for item in supported if (item.fanout_queued_count or 0) > 0),
        denominator=len(supported),
    )


def _merge_parity_features(records: list[LaneSourceRecord], *, lane: str) -> dict[str, bool]:
    merged = dict(PARITY_FEATURES.get(lane, {}))
    for item in records:
        for key, value in item.parity_features.items():
            merged[key] = merged.get(key, False) or bool(value)
    return merged


def _parity_status(features: dict[str, bool]) -> str:
    enabled = sum(1 for value in features.values() if value)
    if enabled == 5:
        return "parity_ready"
    if features.get("quote_verification_supported") and enabled >= 4:
        return "near_parity"
    return "parity_gap"


def _write_quality_report(repo_root: Path, *, payload: dict[str, Any], report_key: str) -> Path:
    report_dir = repo_root.joinpath(*QUALITY_REPORT_DIR)
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_name = f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}-quality-report-{report_key}-{timestamp}.md"
    target = report_dir / report_name
    lines = [
        "# Dream Lane Quality Report",
        "",
        f"- Evaluated at: {payload['evaluated_at']}",
        f"- Window size: {payload['window_size']}",
        f"- Minimum sample size: {payload['minimum_sample_size']}",
        "",
    ]
    for lane in CANONICAL_LANES:
        lane_payload = (payload.get("lanes") or {}).get(lane) or {}
        lines.extend(
            [
                f"## {LANE_DISPLAY.get(lane, lane)}",
                "",
                f"- State: {lane_payload.get('state') or 'unknown'}",
                f"- Reasons: {', '.join(lane_payload.get('reasons') or ['none'])}",
                f"- Recent sources: {lane_payload.get('recent_sources') or 0}",
                f"- Parity: {lane_payload.get('parity_status') or 'unknown'}",
                f"- Bootstrap behavior: {stage_behavior_for_lane_state('bootstrap', str(lane_payload.get('state') or 'blocked'))}",
                f"- Light behavior: {stage_behavior_for_lane_state('light', str(lane_payload.get('state') or 'blocked'))}",
                f"- Deep behavior: {stage_behavior_for_lane_state('deep', str(lane_payload.get('state') or 'blocked'))}",
                "",
                "### Metrics",
                "",
            ]
        )
        for metric_name, metric_value in (lane_payload.get("metrics") or {}).items():
            rendered = "n/a" if metric_value is None else f"{float(metric_value):.2f}"
            lines.append(f"- {metric_name}: {rendered}")
        lines.extend(["", "### Sample Sources", ""])
        sample_source_ids = lane_payload.get("sample_source_ids") or []
        if sample_source_ids:
            lines.extend(f"- {source_id}" for source_id in sample_source_ids)
        else:
            lines.append("- None")
        lines.append("")
    target.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return target


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _cache_data(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    return payload


def _cache_identity(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    identity = payload.get("_llm")
    return identity if isinstance(identity, dict) else None


def _as_optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
