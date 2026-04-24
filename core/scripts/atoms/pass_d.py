"""The Pass D Gemini call.

Builds the prompt, calls Gemini, parses the JSON response, returns
structured PassDResult. Caches at raw/transcripts/<source_kind>/<source_id>.pass_d.json.

Historical design notes are kept outside the public release tree.
"""
from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import re
from typing import Any, Mapping, cast

from mind.services.llm_cache import load_llm_cache, write_llm_cache
from mind.services.llm_service import get_llm_service

from scripts.atoms.prompts import PASS_D_PROMPT_VERSION, build_pass_d_prompt
from scripts.atoms.types import Atom, PassDResult, Q1Match, Q2Candidate
from scripts.common.slugify import normalize_identifier
from scripts.common.vault import raw_path


BOOTSTRAP_CACHE_VERSION = "bootstrap-v1"
PASS_D_TASK_CLASS = "dream_pass_d"
PASS_D_LEGACY_TASK_CLASSES = ("dream",)
PASS_D_COMPAT_PROMPT_VERSIONS = (PASS_D_PROMPT_VERSION, "dream.pass-d.v2")
_SUPPORTED_ATOM_TYPES = {"concept", "playbook", "stance", "inquiry"}
_SUPPORTED_POLARITIES = {"for", "against", "neutral"}
_SUPPORTED_CONFIDENCE = {"low", "medium", "high"}
_SUPPORTED_EVIDENCE_STRENGTH = {"anecdotal", "empirical", "theoretical", "experiential"}
_SUPPORTED_RELATION_KINDS = {
    "supports",
    "contradicts",
    "example_of",
    "applies_to",
    "depends_on",
    "extends",
    "adjacent_to",
}
_FIRST_SENTENCE_RE = re.compile(r"(.+?[.!?])(?:\s|$)")


def pass_d_cache_path(*, repo_root: Path, source_kind: str, source_id: str, cache_mode: str = "default") -> Path:
    suffix = ".pass_d.json" if cache_mode == "default" else f".pass_d.{BOOTSTRAP_CACHE_VERSION}.json"
    return raw_path(repo_root, "transcripts", source_kind, f"{source_id}{suffix}")


def pass_d_cache_lookup_paths(*, repo_root: Path, source_kind: str, source_id: str, cache_mode: str = "default") -> tuple[Path, ...]:
    primary = pass_d_cache_path(
        repo_root=repo_root,
        source_kind=source_kind,
        source_id=source_id,
        cache_mode=cache_mode,
    )
    if cache_mode == "default":
        return (primary,)
    default_path = pass_d_cache_path(
        repo_root=repo_root,
        source_kind=source_kind,
        source_id=source_id,
        cache_mode="default",
    )
    if default_path == primary:
        return (primary,)
    return (primary, default_path)


def pass_d_cache_exists(*, repo_root: Path, source_kind: str, source_id: str, cache_mode: str = "default") -> bool:
    return any(
        candidate.exists()
        for candidate in pass_d_cache_lookup_paths(
            repo_root=repo_root,
            source_kind=source_kind,
            source_id=source_id,
            cache_mode=cache_mode,
        )
    )


def pass_d_cache_identities(service: Any | None = None) -> list[Any]:
    llm = service or get_llm_service()
    identities: list[Any] = []
    seen: set[str] = set()
    for prompt_version in PASS_D_COMPAT_PROMPT_VERSIONS:
        canonical_identities = list(llm.cache_identities(task_class=PASS_D_TASK_CLASS, prompt_version=prompt_version))
        for identity in canonical_identities:
            key = json.dumps(identity.to_dict(), sort_keys=True)
            if key in seen:
                continue
            identities.append(identity)
            seen.add(key)
        for task_class in PASS_D_LEGACY_TASK_CLASSES:
            for identity in llm.cache_identities(task_class=task_class, prompt_version=prompt_version):
                key = json.dumps(identity.to_dict(), sort_keys=True)
                if key in seen:
                    continue
                identities.append(identity)
                seen.add(key)
            for identity in canonical_identities:
                mirrored = replace(identity, task_class=task_class)
                key = json.dumps(mirrored.to_dict(), sort_keys=True)
                if key in seen:
                    continue
                identities.append(mirrored)
                seen.add(key)
    return identities


def run_pass_d(
    *,
    source_id: str,
    source_link: str,
    source_kind: str,
    body_or_transcript: str,
    summary: dict,
    applied: dict | None,
    pass_c_delta: str | None,
    stance_context: str,
    prior_source_context: str,
    working_set: list[Atom],
    repo_root: Path,
    today_str: str,
    cache_mode: str = "default",
    force_refresh: bool = False,
) -> PassDResult:
    """Execute Pass D for a single source.

    Builds the prompt, calls Gemini, parses the JSON response, returns
    structured PassDResult. Caches at raw/transcripts/<source_kind>/<source_id>.pass_d.json.
    Cache hits return immediately without a Gemini call.

    Raises on Gemini API errors, JSON parse errors, schema validation errors.
    Caller is responsible for the try/except → log_failure → continue pattern.
    """

    cache_path = pass_d_cache_path(
        repo_root=repo_root,
        source_kind=source_kind,
        source_id=source_id,
        cache_mode=cache_mode,
    )
    service = get_llm_service()
    if hasattr(service, "cache_identities"):
        identities = pass_d_cache_identities(service)
        identity = next(
            (candidate for candidate in identities if getattr(candidate, "task_class", "") == PASS_D_TASK_CLASS),
            identities[0],
        )
    else:  # pragma: no cover - compatibility with narrow fakes in tests
        from mind.services.llm_cache import LLMCacheIdentity

        identity = LLMCacheIdentity(
            task_class=PASS_D_TASK_CLASS,
            provider="unknown",
            model="unknown",
            transport="ai_gateway",
            api_family="responses",
            input_mode="text",
            prompt_version=PASS_D_PROMPT_VERSION,
        )
        identities = [identity]
    cached = None
    if not force_refresh:
        for candidate_path in pass_d_cache_lookup_paths(
            repo_root=repo_root,
            source_kind=source_kind,
            source_id=source_id,
            cache_mode=cache_mode,
        ):
            cached = load_llm_cache(candidate_path, expected=identities)
            if isinstance(cached, dict):
                break
    if isinstance(cached, dict):
        data = cached
    else:
        prompt = build_pass_d_prompt(
            source_id=source_id,
            source_link=source_link,
            source_kind=source_kind,
            body_or_transcript=body_or_transcript,
            summary=summary,
            applied=applied,
            pass_c_delta=pass_c_delta,
            stance_context=stance_context,
            prior_source_context=prior_source_context,
            working_set=working_set,
        )
        try:
            result = service.generate_json_prompt(
                prompt,
                with_meta=True,
                task_class=PASS_D_TASK_CLASS,
                prompt_version=PASS_D_PROMPT_VERSION,
            )
        except TypeError:  # pragma: no cover - compatibility with narrow fakes in tests
            result = service.generate_json_prompt(prompt)
        if isinstance(result, tuple):
            data, identity = result
        else:  # pragma: no cover - compatibility with narrow fakes in tests
            data = result
        write_llm_cache(cache_path, identity=identity, data=data)
    return _parse_pass_d_result(data)


def _parse_pass_d_result(data: dict) -> PassDResult:
    if not isinstance(data, dict):
        raise ValueError("Pass D response must be a JSON object")

    warnings: list[str] = []
    q1_items = _coerce_items(data, key="q1_matches", warnings=warnings)
    q2_items = _coerce_items(data, key="q2_candidates", warnings=warnings)
    q1_matches: list[Q1Match] = []
    q2_candidates: list[Q2Candidate] = []
    dropped_q1_matches = len(data.get("q1_matches") or []) - len(q1_items)
    dropped_q2_candidates = len(data.get("q2_candidates") or []) - len(q2_items)

    for index, item in q1_items:
        parsed = _parse_q1_match(item, index=index, warnings=warnings)
        if parsed is None:
            dropped_q1_matches += 1
            continue
        q1_matches.append(parsed)

    for index, item in q2_items:
        parsed = _parse_q2_candidate(item, index=index, warnings=warnings)
        if parsed is None:
            dropped_q2_candidates += 1
            continue
        q2_candidates.append(parsed)

    received_items = len(data.get("q1_matches") or []) + len(data.get("q2_candidates") or [])
    if received_items > 0 and not q1_matches and not q2_candidates:
        summary = warnings[0] if warnings else "no usable Pass D items remained after parsing"
        raise ValueError(f"Pass D payload was unusable: {summary}")

    return PassDResult(
        q1_matches=q1_matches,
        q2_candidates=q2_candidates,
        warnings=warnings,
        dropped_q1_matches=dropped_q1_matches,
        dropped_q2_candidates=dropped_q2_candidates,
    )


def stage_outcomes_from_payload(payload: Mapping[str, object]) -> list[dict[str, object]]:
    """Return operator-facing Pass D degradation summaries when needed."""

    outcomes: list[dict[str, object]] = []
    error = str(payload.get("error") or "").strip()
    warnings = [str(item).strip() for item in payload.get("warnings") or [] if str(item).strip()]
    dropped_q1_matches = int(payload.get("dropped_q1_matches") or 0)
    dropped_q2_candidates = int(payload.get("dropped_q2_candidates") or 0)
    if warnings or dropped_q1_matches > 0 or dropped_q2_candidates > 0:
        summary = (
            f"{len(warnings)} warning(s); "
            f"dropped {dropped_q1_matches} q1 match(es) and {dropped_q2_candidates} q2 candidate(s)"
        )
        if warnings:
            summary = f"{summary}; first={warnings[0]}"
        outcomes.append(
            {
                "status": "warning",
                "stage": "pass_d.parse",
                "summary": summary,
                "warnings": warnings,
                "dropped_q1_matches": dropped_q1_matches,
                "dropped_q2_candidates": dropped_q2_candidates,
            }
        )

    if error:
        outcomes.append(
            {
                "status": "error",
                "stage": str(payload.get("error_stage") or "pass_d.parse"),
                "summary": error,
            }
        )
    return outcomes


def _coerce_items(
    data: Mapping[str, object],
    *,
    key: str,
    warnings: list[str],
) -> list[tuple[int, Mapping[str, object]]]:
    raw_items = data.get(key) or []
    if not isinstance(raw_items, list):
        raise ValueError(f"Pass D field {key!r} must be a list")
    items: list[tuple[int, Mapping[str, object]]] = []
    for index, item in enumerate(raw_items):
        if not isinstance(item, dict):
            warnings.append(f"{key}[{index}]: expected object, got {type(item).__name__}")
            continue
        items.append((index, cast(Mapping[str, object], item)))
    return items


def _parse_q1_match(item: Mapping[str, object], *, index: int, warnings: list[str]) -> Q1Match | None:
    atom_type = _normalize_atom_type(
        item,
        primary_key="atom_type",
        alias_key="type",
        item_kind="q1_matches",
        index=index,
        warnings=warnings,
    )
    if atom_type is None:
        return None
    polarity = _normalize_polarity(item.get("polarity"), item_kind="q1_matches", index=index, warnings=warnings)
    return Q1Match(
        atom_id=normalize_identifier(str(item.get("atom_id") or "")),
        atom_type=atom_type,
        snippet=str(item.get("snippet") or ""),
        polarity=polarity,
        confidence=_normalize_confidence(item.get("confidence"), item_kind="q1_matches", index=index, warnings=warnings),
        evidence_strength=_normalize_evidence_strength(item.get("evidence_strength"), item_kind="q1_matches", index=index, warnings=warnings),
        relation_kind=_normalize_relation_kind(item.get("relation_kind"), polarity=polarity, item_kind="q1_matches", index=index, warnings=warnings),
    )


def _parse_q2_candidate(item: Mapping[str, object], *, index: int, warnings: list[str]) -> Q2Candidate | None:
    candidate_type = _normalize_atom_type(
        item,
        primary_key="type",
        alias_key="atom_type",
        item_kind="q2_candidates",
        index=index,
        warnings=warnings,
    )
    if candidate_type is None:
        return None
    proposed_id = normalize_identifier(str(item.get("proposed_id") or ""))
    title = str(item.get("title") or "").strip()
    description = str(item.get("description") or "").strip()
    tldr = str(item.get("tldr") or "").strip() or _first_sentence(description)
    if not proposed_id:
        warnings.append(f"q2_candidates[{index}]: missing proposed_id")
        return None
    if not title:
        warnings.append(f"q2_candidates[{index}]: missing title")
        return None
    if not description:
        warnings.append(f"q2_candidates[{index}]: missing description")
        return None
    if not tldr:
        warnings.append(f"q2_candidates[{index}]: missing tldr")
        return None
    return Q2Candidate(
        type=candidate_type,
        proposed_id=proposed_id,
        title=title,
        description=description,
        tldr=tldr,
        snippet=str(item.get("snippet") or ""),
        polarity=_normalize_polarity(item.get("polarity"), item_kind="q2_candidates", index=index, warnings=warnings),
        rationale=str(item.get("rationale") or ""),
        domains=_coerce_str_list(item.get("domains")),
        in_conversation_with=[
            normalized
            for normalized in (normalize_identifier(value) for value in _coerce_str_list(item.get("in_conversation_with")))
            if normalized
        ],
        steps=_coerce_str_list(item.get("steps")),
        position=str(item.get("position") or "").strip() or description if candidate_type == "stance" else "",
        question=str(item.get("question") or "").strip() or title if candidate_type == "inquiry" else "",
    )


def _normalize_atom_type(
    item: Mapping[str, object],
    *,
    primary_key: str,
    alias_key: str,
    item_kind: str,
    index: int,
    warnings: list[str],
) -> str | None:
    primary_value = str(item.get(primary_key) or "").strip()
    alias_value = str(item.get(alias_key) or "").strip()
    recovered_value = primary_value or alias_value
    if not recovered_value:
        warnings.append(f"{item_kind}[{index}]: missing {primary_key}")
        return None
    if not primary_value and alias_value:
        warnings.append(f"{item_kind}[{index}]: recovered {primary_key} from {alias_key}")
    if recovered_value not in _SUPPORTED_ATOM_TYPES:
        warnings.append(f"{item_kind}[{index}]: unsupported {primary_key} {recovered_value!r}")
        return None
    return cast(str, recovered_value)


def _normalize_polarity(value: object, *, item_kind: str, index: int, warnings: list[str]) -> str:
    raw = str(value or "").strip()
    if not raw:
        warnings.append(f"{item_kind}[{index}]: defaulted empty polarity to 'neutral'")
        return "neutral"
    if raw not in _SUPPORTED_POLARITIES:
        warnings.append(f"{item_kind}[{index}]: unsupported polarity {raw!r}; defaulted to 'neutral'")
        return "neutral"
    return raw


def _normalize_confidence(value: object, *, item_kind: str, index: int, warnings: list[str]) -> str:
    raw = str(value or "").strip()
    if not raw:
        warnings.append(f"{item_kind}[{index}]: defaulted empty confidence to 'low'")
        return "low"
    if raw not in _SUPPORTED_CONFIDENCE:
        warnings.append(f"{item_kind}[{index}]: unsupported confidence {raw!r}; defaulted to 'low'")
        return "low"
    return raw


def _normalize_evidence_strength(value: object, *, item_kind: str, index: int, warnings: list[str]) -> str:
    raw = str(value or "").strip()
    if not raw:
        warnings.append(f"{item_kind}[{index}]: defaulted empty evidence_strength to 'anecdotal'")
        return "anecdotal"
    if raw not in _SUPPORTED_EVIDENCE_STRENGTH:
        warnings.append(f"{item_kind}[{index}]: unsupported evidence_strength {raw!r}; defaulted to 'anecdotal'")
        return "anecdotal"
    return raw


def _normalize_relation_kind(
    value: object,
    *,
    polarity: str,
    item_kind: str,
    index: int,
    warnings: list[str],
) -> str:
    raw = str(value or "").strip()
    if not raw:
        default = "contradicts" if polarity == "against" else "supports" if polarity == "for" else "adjacent_to"
        warnings.append(f"{item_kind}[{index}]: defaulted empty relation_kind to {default!r}")
        return default
    if raw not in _SUPPORTED_RELATION_KINDS:
        default = "contradicts" if polarity == "against" else "supports" if polarity == "for" else "adjacent_to"
        warnings.append(f"{item_kind}[{index}]: unsupported relation_kind {raw!r}; defaulted to {default!r}")
        return default
    return raw


def _coerce_str_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        cleaned = value.strip()
        return [cleaned] if cleaned else []
    return []


def _first_sentence(text: str) -> str:
    cleaned = " ".join(part.strip() for part in str(text or "").splitlines() if part.strip()).strip()
    if not cleaned:
        return ""
    match = _FIRST_SENTENCE_RE.match(cleaned)
    return (match.group(1) if match else cleaned).strip()
