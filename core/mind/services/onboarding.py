from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import mimetypes
from pathlib import Path
import shutil
from typing import Any
import yaml

from mind.services.onboarding_synthesis import (
    PatchReviewRequiredError,
    MergeArtifact,
    apply_materialization_plan,
    artifact_paths as onboarding_artifact_paths,
    load_pipeline_artifacts,
    synthesize_bundle,
    verify_bundle,
)
from mind.services.onboarding_state import summarize_chunk_phase
from scripts.common.default_tags import default_tags
from scripts.common.slugify import slugify
from scripts.common.vault import Vault
from scripts.common.wiki_writer import write_page


CORE_SUMMARY_KINDS = ("overview", "profile", "values", "positioning", "open-inquiries")
OPTIONAL_GROUPS = ("projects", "people", "concepts", "playbooks", "stances", "inquiries")
FALLBACK_TAGS = {
    "profile": ["domain/identity", "function/identity", "signal/canon"],
    "project": ["domain/work", "function/note", "signal/working"],
}


@dataclass(frozen=True)
class OnboardingPaths:
    bundle_id: str
    root: Path
    bundle_dir: Path
    raw_input_path: Path
    uploads_dir: Path
    evidence_bundle_path: Path
    state_path: Path
    decisions_path: Path
    validation_path: Path
    materialization_path: Path
    current_path: Path
    interview_path: Path


@dataclass(frozen=True)
class OnboardingStatus:
    bundle_id: str
    status: str
    ready_for_materialization: bool
    raw_input_path: str
    uploads: list[dict[str, Any]]
    next_questions: list[dict[str, Any]]
    errors: list[str]
    warnings: list[str]
    materialized_pages: list[str]
    summary_pages: list[str]
    decision_page: str | None
    synthesis_status: str
    verifier_verdict: str
    blocking_reasons: list[str]
    materialization_plan_path: str | None
    replay_provenance: str | None
    graph_chunks_summary: str | None
    merge_chunks_summary: str | None
    merge_relationships_summary: str | None
    readiness: dict[str, Any]
    updated_at: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_timestamp() -> str:
    return _utc_now().strftime("%Y%m%dT%H%M%SZ")


def _utc_timestamp_readable() -> str:
    return _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _today() -> str:
    return _utc_now().date().isoformat()


def _json_dump(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _json_load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, list):
        items: list[str] = []
        for item in value:
            if isinstance(item, str):
                text = item.strip()
            elif isinstance(item, dict):
                text = _safe_text(item.get("text") or item.get("title") or item.get("name") or item.get("question"))
            else:
                text = _safe_text(item)
            if text:
                items.append(text)
        return items
    return [_safe_text(value)] if _safe_text(value) else []


def _candidate_list(value: Any, *, title_key: str, summary_key: str, extra_keys: tuple[str, ...] = ()) -> list[dict[str, Any]]:
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    candidates: list[dict[str, Any]] = []
    for index, item in enumerate(items):
        if isinstance(item, str):
            title = item.strip()
            summary = ""
            record: dict[str, Any] = {}
        elif isinstance(item, dict):
            title = _safe_text(item.get(title_key) or item.get("title") or item.get("name") or item.get("question"))
            summary = _safe_text(item.get(summary_key) or item.get("summary") or item.get("description") or item.get("position"))
            record = dict(item)
        else:
            title = _safe_text(item)
            summary = ""
            record = {}
        if not title:
            continue
        candidate = {
            "slug": slugify(title),
            "title": title,
            "summary": summary,
            "evidence_refs": [f"input:{title_key}:{index}"],
        }
        for key in extra_keys:
            raw = record.get(key)
            candidate[key] = _string_list(raw) if isinstance(raw, list) else _safe_text(raw)
        if "confidence" in record:
            candidate["confidence"] = _safe_text(record.get("confidence"))
        if "question" in record:
            candidate["question"] = _safe_text(record.get("question"))
        if "position" in record:
            candidate["position"] = _safe_text(record.get("position"))
        candidates.append(candidate)
    return candidates


def _paths(vault: Vault, *, bundle_id: str) -> OnboardingPaths:
    root = vault.onboarding_root
    bundle_dir = vault.onboarding_bundles_root / bundle_id
    return OnboardingPaths(
        bundle_id=bundle_id,
        root=root,
        bundle_dir=bundle_dir,
        raw_input_path=bundle_dir / "raw-input.json",
        uploads_dir=bundle_dir / "uploads",
        evidence_bundle_path=bundle_dir / "normalized-evidence.json",
        state_path=bundle_dir / "state.json",
        decisions_path=bundle_dir / "decisions.json",
        validation_path=bundle_dir / "validation.json",
        materialization_path=bundle_dir / "materialization.json",
        current_path=vault.onboarding_current_path,
        interview_path=bundle_dir / "interview.jsonl",
    )


def _clear_synthesis_artifacts(paths: OnboardingPaths) -> None:
    for artifact in onboarding_artifact_paths(paths.bundle_dir).values():
        if artifact.exists():
            artifact.unlink()


def _reset_synthesis_state(state: dict[str, Any]) -> None:
    state["synthesis_status"] = "not-synthesized"
    state["verifier_verdict"] = "not-run"
    state["blocking_reasons"] = []
    state["materialization_plan_path"] = None
    state["replay_provenance"] = None
    state["artifact_paths"] = {}


def _copy_uploads(
    paths: list[str],
    target_dir: Path,
    *,
    existing_uploads: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    uploads: list[dict[str, Any]] = []
    target_dir.mkdir(parents=True, exist_ok=True)
    next_index = len(existing_uploads or [])
    for raw_path in paths:
        source = Path(raw_path).expanduser().resolve()
        digest = _sha256_path(source)
        next_index += 1
        destination = target_dir / f"{next_index:04d}-{digest[:12]}-{source.name}"
        shutil.copy2(source, destination)
        uploads.append(
            {
                "id": f"upload-{next_index:04d}",
                "file_name": source.name,
                "path": destination.as_posix(),
                "media_type": _guess_media_type(source.name),
                "size_bytes": destination.stat().st_size,
                "evidence_refs": [f"upload:{next_index:04d}:{source.name}"],
            }
        )
    return uploads


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


_TEXT_EXTENSION_MEDIA_TYPES: dict[str, str] = {
    ".md": "text/markdown",
    ".markdown": "text/markdown",
    ".mdx": "text/markdown",
    ".txt": "text/plain",
    ".rst": "text/x-rst",
    ".csv": "text/csv",
    ".tsv": "text/tab-separated-values",
    ".json": "application/json",
    ".yaml": "application/yaml",
    ".yml": "application/yaml",
    ".toml": "application/toml",
}


def _guess_media_type(file_name: str) -> str:
    guessed, _ = mimetypes.guess_type(file_name)
    if guessed:
        return guessed
    suffix = Path(file_name).suffix.lower()
    if suffix in _TEXT_EXTENSION_MEDIA_TYPES:
        return _TEXT_EXTENSION_MEDIA_TYPES[suffix]
    return "application/octet-stream"


def _write_current_pointer(paths: OnboardingPaths, *, state: dict[str, Any]) -> None:
    _json_dump(
        paths.current_path,
        {
            "bundle_id": paths.bundle_id,
            "state_path": paths.state_path.as_posix(),
            "bundle_path": paths.evidence_bundle_path.as_posix(),
            "bundle_sha256": state.get("bundle_sha256"),
            "status": state.get("status"),
            "updated_at": state.get("updated_at"),
        },
    )


def _append_transcript_entries(path: Path, entries: list[dict[str, Any]]) -> None:
    if not entries:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for entry in entries:
            handle.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _paths_as_posix(paths: list[Path]) -> list[str]:
    return [path.as_posix() for path in paths]


def _projected_page_paths(state: dict[str, Any]) -> list[Path]:
    raw_paths = [*list(state.get("summary_pages") or []), *list(state.get("materialized_pages") or [])]
    return [Path(raw_path) for raw_path in raw_paths]


def _normalize_payload(payload: dict[str, Any], *, bundle_id: str, raw_input_path: Path, uploads: list[dict[str, Any]]) -> dict[str, Any]:
    identity = {
        "name": _safe_text(payload.get("name") or payload.get("identity", {}).get("name")),
        "role": _safe_text(payload.get("role") or payload.get("identity", {}).get("role")),
        "location": _safe_text(payload.get("location") or payload.get("identity", {}).get("location")),
        "summary": _safe_text(payload.get("summary") or payload.get("identity", {}).get("summary")),
    }
    positioning = payload.get("positioning")
    positioning_record = positioning if isinstance(positioning, dict) else {}
    open_inquiries_raw = payload.get("open_inquiries")
    if open_inquiries_raw is None:
        open_inquiries_raw = payload.get("open_threads")
    bundle = {
        "schema_version": 1,
        "bundle_id": bundle_id,
        "created_at": _utc_timestamp_readable(),
        "source_input": {
            "path": raw_input_path.as_posix(),
            "evidence_ref": "input:root",
        },
        "uploads": uploads,
        "identity": identity,
        "values": [
            {
                "text": text,
                "evidence_refs": [f"input:values:{index}"],
            }
            for index, text in enumerate(_string_list(payload.get("values")))
        ],
        "positioning": {
            "summary": _safe_text(
                positioning_record.get("summary")
                or positioning_record.get("narrative")
                or (positioning if isinstance(positioning, str) else payload.get("positioning_summary"))
            ),
            "work_priorities": _string_list(positioning_record.get("work_priorities") or payload.get("work_priorities")),
            "life_priorities": _string_list(positioning_record.get("life_priorities") or payload.get("life_priorities")),
            "constraints": _string_list(positioning_record.get("constraints") or payload.get("constraints")),
            "evidence_refs": ["input:positioning"],
        },
        "open_inquiries": [
            {
                "slug": slugify(question),
                "question": question,
                "evidence_refs": [f"input:open-inquiries:{index}"],
            }
            for index, question in enumerate(_string_list(open_inquiries_raw))
        ],
        "projects": _candidate_list(payload.get("projects"), title_key="project", summary_key="summary", extra_keys=("priorities", "constraints")),
        "people": _candidate_list(payload.get("people"), title_key="person", summary_key="summary"),
        "concepts": _candidate_list(payload.get("concepts"), title_key="concept", summary_key="summary"),
        "playbooks": _candidate_list(payload.get("playbooks"), title_key="playbook", summary_key="summary"),
        "stances": _candidate_list(payload.get("stances"), title_key="stance", summary_key="summary"),
        "inquiries": _candidate_list(payload.get("inquiries"), title_key="inquiry", summary_key="summary"),
    }
    return bundle


def _split_answer(answer: str) -> list[str]:
    normalized = answer.replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.strip("- ").strip() for line in normalized.replace(",", "\n").splitlines()]
    return [line for line in lines if line]


def _apply_answer(bundle: dict[str, Any], *, question_id: str, answer: str) -> None:
    values = _split_answer(answer)
    if question_id == "identity-name":
        bundle["identity"]["name"] = answer.strip()
    elif question_id == "identity-summary":
        bundle["identity"]["summary"] = answer.strip()
    elif question_id == "values":
        bundle["values"] = [
            {"text": value, "evidence_refs": [f"interview:values:{index}"]}
            for index, value in enumerate(values)
        ]
    elif question_id == "positioning-summary":
        bundle["positioning"]["summary"] = answer.strip()
    elif question_id == "positioning-work-priorities":
        bundle["positioning"]["work_priorities"] = values
    elif question_id == "positioning-constraints":
        bundle["positioning"]["constraints"] = values
    elif question_id == "open-inquiries":
        bundle["open_inquiries"] = [
            {"slug": slugify(value), "question": value, "evidence_refs": [f"interview:open-inquiries:{index}"]}
            for index, value in enumerate(values)
        ]


def build_adaptive_questions(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    questions: list[dict[str, Any]] = []
    identity = bundle.get("identity") or {}
    positioning = bundle.get("positioning") or {}
    if not _safe_text(identity.get("name")):
        questions.append(
            {
                "id": "identity-name",
                "prompt": "What name should Brain use for the owner profile?",
                "reason": "Profile materialization requires a canonical name.",
            }
        )
    if not _safe_text(identity.get("summary")):
        questions.append(
            {
                "id": "identity-summary",
                "prompt": "What short, factual profile summary best describes the owner right now?",
                "reason": "Profile.md should stay encyclopedia-like and factual.",
            }
        )
    if not bundle.get("values"):
        questions.append(
            {
                "id": "values",
                "prompt": "Which values or operating principles should always shape decisions here?",
                "reason": "Values.md should start with explicit principles.",
            }
        )
    if not _safe_text(positioning.get("summary")):
        questions.append(
            {
                "id": "positioning-summary",
                "prompt": "How should Brain frame the owner's current positioning in work and life?",
                "reason": "Positioning.md needs a narrative, not just metadata.",
            }
        )
    if not positioning.get("work_priorities"):
        questions.append(
            {
                "id": "positioning-work-priorities",
                "prompt": "Which work priorities are live right now?",
                "reason": "Positioning.md should carry current work priorities.",
                "choices": ["craft quality", "leverage", "distribution", "autonomy"],
            }
        )
    if not positioning.get("constraints"):
        questions.append(
            {
                "id": "positioning-constraints",
                "prompt": "Which constraints or tradeoffs should Brain treat as real?",
                "reason": "Positioning.md should capture current constraints.",
            }
        )
    if not bundle.get("open_inquiries"):
        questions.append(
            {
                "id": "open-inquiries",
                "prompt": "What questions are genuinely unresolved and still worth tracking?",
                "reason": "Open inquiries drive Dream and future synthesis.",
            }
        )
    if not bundle.get("uploads"):
        questions.append(
            {
                "id": "upload-documents",
                "prompt": "Are there source documents worth uploading before materialization?",
                "reason": "Raw onboarding uploads stay isolated but improve provenance.",
                "choices": ["yes, I have documents", "no, continue without uploads", "later"],
            }
        )
    if not bundle.get("projects") and not bundle.get("people"):
        questions.append(
            {
                "id": "optional-entities",
                "prompt": "Should onboarding seed any projects or people now, or defer them?",
                "reason": "Optional entity creation is governed by the onboarding decisions artifact.",
                "choices": ["create now", "defer for later", "need more evidence first"],
            }
        )
    return questions


def validate_evidence_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    identity = bundle.get("identity") or {}
    positioning = bundle.get("positioning") or {}
    errors: list[str] = []
    warnings: list[str] = []
    if not _safe_text(identity.get("name")):
        errors.append("missing identity name")
    if not _safe_text(identity.get("summary")):
        errors.append("missing profile summary")
    if not bundle.get("values"):
        errors.append("missing values")
    if not _safe_text(positioning.get("summary")):
        errors.append("missing positioning narrative")
    if not bundle.get("open_inquiries"):
        errors.append("missing open inquiries")
    if not positioning.get("work_priorities"):
        warnings.append("work priorities were not provided")
    if not positioning.get("constraints"):
        warnings.append("constraints were not provided")
    if not bundle.get("uploads"):
        warnings.append("no onboarding uploads were collected")
    return {
        "ready_for_materialization": not errors,
        "errors": errors,
        "warnings": warnings,
    }


def _decision_confidence(candidate: dict[str, Any]) -> str:
    if _safe_text(candidate.get("summary")) and (
        candidate.get("priorities") or candidate.get("constraints") or _safe_text(candidate.get("question")) or _safe_text(candidate.get("position"))
    ):
        return "high"
    if _safe_text(candidate.get("summary")):
        return "medium"
    return "low"


def build_decisions(bundle: dict[str, Any]) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    owner_slug = slugify(_safe_text((bundle.get("identity") or {}).get("name")))
    for family in OPTIONAL_GROUPS:
        candidates = list(bundle.get(family) or [])
        if not candidates:
            entries.append(
                {
                    "family": family,
                    "target": family,
                    "action": "not-create",
                    "rationale": f"No {family} candidates were provided in onboarding evidence.",
                    "evidence_refs": [],
                    "confidence": "medium",
                }
            )
            continue
        for candidate in candidates:
            is_owner_node = family == "people" and owner_slug and _safe_text(candidate.get("slug")) == owner_slug
            rich_enough = bool(_safe_text(candidate.get("summary")) or _safe_text(candidate.get("question")) or _safe_text(candidate.get("position")))
            action = "create" if rich_enough else "not-create"
            if is_owner_node:
                action = "not-create"
                rationale = "Owner identity is materialized as the canonical self person node from the onboarding profile."
            elif action == "create":
                rationale = f"Structured {family[:-1] if family.endswith('s') else family} candidate provided with enough detail to materialize safely."
            else:
                rationale = f"{family[:-1] if family.endswith('s') else family} candidate lacks enough detail for safe materialization."
            entries.append(
                {
                    "family": family,
                    "target": _safe_text(candidate.get("slug") or candidate.get("title")),
                    "action": action,
                    "rationale": rationale,
                    "evidence_refs": list(candidate.get("evidence_refs") or []),
                    "confidence": "high" if is_owner_node else _decision_confidence(candidate),
                }
            )
    return {
        "bundle_id": bundle["bundle_id"],
        "created_at": _utc_timestamp_readable(),
        "entries": entries,
    }


def import_onboarding_bundle(
    repo_root: Path,
    *,
    from_json: str,
    upload_paths: list[str] | None = None,
    bundle_id: str | None = None,
) -> OnboardingStatus:
    vault = Vault.load(repo_root)
    input_path = Path(from_json).expanduser().resolve()
    payload = _json_load(input_path)
    resolved_bundle_id = bundle_id or _utc_timestamp().lower()
    paths = _paths(vault, bundle_id=resolved_bundle_id)
    paths.bundle_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(input_path, paths.raw_input_path)
    uploads = _copy_uploads(upload_paths or [], paths.uploads_dir) if upload_paths else []
    bundle = _normalize_payload(payload, bundle_id=resolved_bundle_id, raw_input_path=paths.raw_input_path, uploads=uploads)
    decisions = build_decisions(bundle)
    validation = validate_evidence_bundle(bundle)
    next_questions = build_adaptive_questions(bundle)
    state = {
        "bundle_id": resolved_bundle_id,
        "status": "imported",
        "updated_at": _utc_timestamp_readable(),
        "raw_input_path": paths.raw_input_path.as_posix(),
        "uploads": uploads,
        "next_questions": next_questions,
        "validation": validation,
        "materialized_pages": [],
        "summary_pages": [],
        "decision_page": None,
        "readiness": {"ready": False, "checks": [], "errors": []},
    }
    _reset_synthesis_state(state)
    _json_dump(paths.evidence_bundle_path, bundle)
    _json_dump(paths.decisions_path, decisions)
    bundle_sha = _sha256_path(paths.evidence_bundle_path)
    _json_dump(paths.validation_path, validation)
    _json_dump(paths.state_path, state)
    persisted_state = _json_load(paths.state_path)
    persisted_state["bundle_sha256"] = bundle_sha
    _json_dump(paths.state_path, persisted_state)
    _write_current_pointer(paths, state=persisted_state)
    _append_transcript_entries(
        paths.interview_path,
        [
            {
                "timestamp": _utc_timestamp_readable(),
                "role": "system",
                "kind": "import",
                "bundle_id": resolved_bundle_id,
                "source": paths.raw_input_path.as_posix(),
            },
            *[
                {
                    "timestamp": _utc_timestamp_readable(),
                    "role": "assistant",
                    "kind": "question",
                    "question_id": item["id"],
                    "prompt": item["prompt"],
                }
                for item in next_questions
            ],
        ],
    )
    return read_onboarding_status(repo_root, bundle_id=resolved_bundle_id)


def normalize_onboarding_bundle(
    repo_root: Path,
    *,
    bundle_id: str,
    responses: list[dict[str, str]] | None = None,
    answers: list[str] | None = None,
    upload_paths: list[str] | None = None,
) -> OnboardingStatus:
    paths, bundle, state, _decisions = load_bundle(repo_root, bundle_id=bundle_id)
    _clear_synthesis_artifacts(paths)
    transcript_entries: list[dict[str, Any]] = []
    if upload_paths:
        existing_uploads = list(state.get("uploads") or [])
        new_uploads = _copy_uploads(upload_paths, paths.uploads_dir, existing_uploads=existing_uploads)
        bundle["uploads"] = list(bundle.get("uploads") or []) + new_uploads
        state["uploads"] = existing_uploads + new_uploads
        transcript_entries.extend(
            {
                "timestamp": _utc_timestamp_readable(),
                "role": "user",
                "kind": "upload",
                "file_name": upload["file_name"],
                "path": upload["path"],
            }
            for upload in new_uploads
        )
    outstanding = build_adaptive_questions(bundle)
    response_items: list[dict[str, str]] = []
    if responses:
        response_items.extend(responses)
    elif answers:
        response_items.extend(
            {"question_id": question["id"], "answer": answer}
            for question, answer in zip(outstanding, answers)
        )

    prompt_by_id = {question["id"]: question for question in outstanding}
    for response in response_items:
        question_id = _safe_text(response.get("question_id"))
        answer = _safe_text(response.get("answer"))
        if not question_id or not answer:
            continue
        question = prompt_by_id.get(question_id) or {"id": question_id, "prompt": question_id}
        transcript_entries.append(
            {
                "timestamp": _utc_timestamp_readable(),
                "role": "assistant",
                "kind": "question",
                "question_id": question["id"],
                "prompt": question["prompt"],
            }
        )
        transcript_entries.append(
            {
                "timestamp": _utc_timestamp_readable(),
                "role": "user",
                "kind": "answer",
                "question_id": question["id"],
                "answer": answer,
            }
        )
        _apply_answer(bundle, question_id=question["id"], answer=answer)
    decisions = build_decisions(bundle)
    validation = validate_evidence_bundle(bundle)
    next_questions = build_adaptive_questions(bundle)
    state.update(
        {
            "status": "normalized",
            "updated_at": _utc_timestamp_readable(),
            "next_questions": next_questions,
            "validation": validation,
            "bundle_sha256": _sha256_path(paths.evidence_bundle_path) if paths.evidence_bundle_path.exists() else None,
            "materialized_pages": [],
            "summary_pages": [],
            "decision_page": None,
            "readiness": {"ready": False, "checks": [], "errors": []},
        }
    )
    _reset_synthesis_state(state)
    _json_dump(paths.evidence_bundle_path, bundle)
    state["bundle_sha256"] = _sha256_path(paths.evidence_bundle_path)
    _json_dump(paths.decisions_path, decisions)
    _json_dump(paths.validation_path, validation)
    _json_dump(paths.state_path, state)
    _write_current_pointer(paths, state=state)
    _append_transcript_entries(paths.interview_path, transcript_entries)
    return read_onboarding_status(repo_root, bundle_id=bundle_id)


def continue_onboarding_interview(
    repo_root: Path,
    *,
    bundle_id: str,
    answers: list[str] | None = None,
    upload_paths: list[str] | None = None,
) -> OnboardingStatus:
    return normalize_onboarding_bundle(
        repo_root,
        bundle_id=bundle_id,
        answers=answers,
        upload_paths=upload_paths,
    )


def _current_bundle_id(vault: Vault) -> str | None:
    current_path = vault.onboarding_current_path
    if not current_path.exists():
        return None
    current = _json_load(current_path)
    bundle_id = _safe_text(current.get("bundle_id"))
    return bundle_id or None


def load_bundle(repo_root: Path, *, bundle_id: str | None = None) -> tuple[OnboardingPaths, dict[str, Any], dict[str, Any], dict[str, Any]]:
    vault = Vault.load(repo_root)
    resolved_bundle_id = bundle_id or _current_bundle_id(vault)
    if not resolved_bundle_id:
        raise FileNotFoundError("no onboarding bundle found")
    paths = _paths(vault, bundle_id=resolved_bundle_id)
    if not paths.evidence_bundle_path.exists():
        raise FileNotFoundError(f"missing onboarding evidence bundle for {resolved_bundle_id}")
    state = _json_load(paths.state_path) if paths.state_path.exists() else {}
    bundle = _json_load(paths.evidence_bundle_path)
    decisions = _json_load(paths.decisions_path) if paths.decisions_path.exists() else build_decisions(bundle)
    return paths, bundle, state, decisions


def synthesize_onboarding_bundle(repo_root: Path, *, bundle_id: str | None = None) -> OnboardingStatus:
    paths, bundle, state, _decisions = load_bundle(repo_root, bundle_id=bundle_id)
    validation = validate_evidence_bundle(bundle)
    _json_dump(paths.validation_path, validation)
    state.update(
        {
            "status": "synthesizing",
            "updated_at": _utc_timestamp_readable(),
            "validation": validation,
            "blocking_reasons": [],
            "synthesis_status": "in-progress",
            "verifier_verdict": "not-run",
            "materialization_plan_path": None,
        }
    )
    _json_dump(paths.state_path, state)
    _write_current_pointer(paths, state=state)
    try:
        if not validation["ready_for_materialization"]:
            raise RuntimeError("onboarding bundle is not structurally ready for synthesis")
        artifacts = synthesize_bundle(
            repo_root,
            bundle_dir=paths.bundle_dir,
            bundle=bundle,
            transcript_path=paths.interview_path,
        )
    except Exception as exc:
        state.update(
            {
                "status": "blocked",
                "updated_at": _utc_timestamp_readable(),
                "validation": validation,
                "blocking_reasons": [str(exc)],
                "synthesis_status": "blocked",
                "verifier_verdict": "not-run",
                "materialization_plan_path": None,
                "artifact_paths": {
                    key: value.as_posix()
                    for key, value in onboarding_artifact_paths(paths.bundle_dir).items()
                    if value.exists()
                },
            }
        )
        _json_dump(paths.state_path, state)
        _write_current_pointer(paths, state=state)
        return read_onboarding_status(repo_root, bundle_id=paths.bundle_id)

    state.update(
        {
            "status": "synthesized",
            "updated_at": _utc_timestamp_readable(),
            "validation": validation,
            "synthesis_status": "synthesized",
            "verifier_verdict": "not-run",
            "blocking_reasons": [],
            "materialization_plan_path": None,
            "artifact_paths": artifacts.artifact_paths or {},
        }
    )
    _json_dump(paths.state_path, state)
    _write_current_pointer(paths, state=state)
    return read_onboarding_status(repo_root, bundle_id=paths.bundle_id)


def verify_onboarding_bundle(repo_root: Path, *, bundle_id: str | None = None) -> OnboardingStatus:
    paths, bundle, state, _decisions = load_bundle(repo_root, bundle_id=bundle_id)
    validation = validate_evidence_bundle(bundle)
    _json_dump(paths.validation_path, validation)
    if not validation["ready_for_materialization"]:
        state.update(
            {
                "status": "blocked",
                "updated_at": _utc_timestamp_readable(),
                "validation": validation,
                "blocking_reasons": list(validation["errors"]),
                "synthesis_status": state.get("synthesis_status") or "not-synthesized",
                "verifier_verdict": "not-run",
                "materialization_plan_path": None,
            }
        )
        _json_dump(paths.state_path, state)
        _write_current_pointer(paths, state=state)
        return read_onboarding_status(repo_root, bundle_id=paths.bundle_id)

    if not (paths.bundle_dir / "synthesis-semantic.json").exists():
        synthesize_onboarding_bundle(repo_root, bundle_id=paths.bundle_id)
        _paths_state = _json_load(paths.state_path)
        state = _paths_state

    try:
        artifacts = load_pipeline_artifacts(paths.bundle_dir)
        verified = verify_bundle(
            bundle_dir=paths.bundle_dir,
            bundle=bundle,
            semantic=artifacts.semantic,
            graph=artifacts.graph,
            merge=artifacts.merge,
        )
    except Exception as exc:
        state.update(
            {
                "status": "blocked",
                "updated_at": _utc_timestamp_readable(),
                "validation": validation,
                "blocking_reasons": [str(exc)],
                "artifact_paths": {
                    key: value.as_posix()
                    for key, value in onboarding_artifact_paths(paths.bundle_dir).items()
                    if value.exists()
                },
            }
        )
        _json_dump(paths.state_path, state)
        _write_current_pointer(paths, state=state)
        return read_onboarding_status(repo_root, bundle_id=paths.bundle_id)

    verify_payload = verified.verify or {"approved": False, "blocking_issues": ["missing verifier output"]}
    approved = bool(verify_payload.get("approved"))
    state.update(
        {
            "status": "verified" if approved else "blocked",
            "updated_at": _utc_timestamp_readable(),
            "validation": validation,
            "synthesis_status": "verified" if approved else "synthesized",
            "verifier_verdict": "approved" if approved else "rejected",
            "blocking_reasons": list(verify_payload.get("blocking_issues") or []),
            "materialization_plan_path": (verified.artifact_paths or {}).get("materialization_plan") if approved else None,
            "artifact_paths": verified.artifact_paths or {},
        }
    )
    _json_dump(paths.state_path, state)
    _write_current_pointer(paths, state=state)
    return read_onboarding_status(repo_root, bundle_id=paths.bundle_id)


def _body_without_frontmatter(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        return text.strip()
    marker = text.find("\n---\n", 4)
    if marker == -1:
        return text.strip()
    return text[marker + 5 :].strip()


def _parse_markdown_page(path: Path) -> tuple[dict[str, Any], str]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        return {}, text
    marker = text.find("\n---\n", 4)
    if marker == -1:
        return {}, text
    return yaml.safe_load(text[4:marker]) or {}, text[marker + 5 :]


def _validate_core_page(path: Path, *, expected_id: str, expected_type: str) -> tuple[bool, str]:
    if not path.exists():
        return False, "missing file"
    frontmatter, body = _parse_markdown_page(path)
    if str(frontmatter.get("id") or "").strip() != expected_id:
        return False, f"expected id={expected_id}"
    if str(frontmatter.get("type") or "").strip() != expected_type:
        return False, f"expected type={expected_type}"
    if not _safe_text(frontmatter.get("title")):
        return False, "missing title"
    lower = body.lower()
    if expected_id == "profile":
        valid_body = len(body) >= 24
    elif expected_id == "values":
        valid_body = ("- " in body) or ("values" in lower) or len(body) >= 24
    elif expected_id == "positioning":
        valid_body = any(token in lower for token in ("build", "focus", "position", "priority", "constraint", "help", "work"))
    else:
        valid_body = ("[[" in body) or ("?" in body) or ("- " in body)
    if not valid_body:
        return False, "body is semantically incomplete"
    return True, "ok"


def validate_onboarding_readiness(vault: Vault) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    errors: list[str] = []
    targets = {
        "profile.md": (vault.owner_profile, "profile", "profile"),
        "values.md": (vault.values_path, "values", "note"),
        "positioning.md": (vault.positioning_path, "positioning", "note"),
        "open-inquiries.md": (vault.open_inquiries_path, "open-inquiries", "note"),
    }
    for name, (path, expected_id, expected_type) in targets.items():
        valid, reason = _validate_core_page(path, expected_id=expected_id, expected_type=expected_type)
        checks.append({"page": name, "path": path.as_posix(), "valid": valid, "reason": reason})
        if not valid:
            errors.append(f"{name} is invalid: {reason}")
    return {"ready": not errors, "checks": checks, "errors": errors}


def validate_onboarding_session_ready(vault: Vault, *, bundle_id: str | None = None) -> dict[str, Any]:
    current: dict[str, Any] = {}
    if bundle_id is None:
        current_path = vault.onboarding_current_path
        if not current_path.exists():
            return {"ready": False, "errors": ["missing current onboarding session"], "checks": []}
        current = _json_load(current_path)
        resolved_bundle_id = _safe_text(current.get("bundle_id"))
        if not resolved_bundle_id:
            return {"ready": False, "errors": ["missing current onboarding session"], "checks": []}
        paths = _paths(vault, bundle_id=resolved_bundle_id)
    else:
        resolved_bundle_id = bundle_id
        paths = _paths(vault, bundle_id=resolved_bundle_id)
    state_path = paths.state_path
    bundle_path = paths.evidence_bundle_path
    if not state_path.exists() or not bundle_path.exists():
        return {"ready": False, "errors": ["missing onboarding state or bundle"], "checks": []}
    state = _json_load(state_path)
    manifest = _json_load(Path(state["materialization_manifest"])) if state.get("materialization_manifest") else {}
    checks: list[dict[str, Any]] = []
    errors: list[str] = []
    live_bundle_sha = _sha256_path(bundle_path)

    status_ok = state.get("status") == "materialized"
    checks.append({"check": "state-materialized", "valid": status_ok})
    if not status_ok:
        errors.append("onboarding state is not materialized")

    validation_ok = bool((state.get("validation") or {}).get("ready_for_materialization"))
    checks.append({"check": "validation-ready", "valid": validation_ok})
    if not validation_ok:
        errors.append("onboarding validation is not ready")

    state_hash_ok = state.get("bundle_sha256") == live_bundle_sha
    checks.append({"check": "bundle-live-sha", "valid": state_hash_ok})
    if not state_hash_ok:
        errors.append("onboarding state hash does not match live bundle")

    if bundle_id is None:
        current_hash_ok = current.get("bundle_sha256") == live_bundle_sha
        checks.append({"check": "current-pointer-sha", "valid": current_hash_ok})
        if not current_hash_ok:
            errors.append("current onboarding pointer does not match bundle hash")

    manifest_hash_ok = bool(manifest) and manifest.get("bundle_sha256") == live_bundle_sha
    checks.append({"check": "materialization-manifest-hash", "valid": manifest_hash_ok})
    if not manifest_hash_ok:
        errors.append("materialization manifest hash does not match bundle hash")

    manifest_paths = [Path(raw_path) for raw_path in list(manifest.get("materialized_pages") or []) + list(manifest.get("summary_pages") or [])]
    decision_page = _safe_text(manifest.get("decision_page"))
    if decision_page:
        manifest_paths.append(Path(decision_page))
    else:
        checks.append({"check": "decision-page", "valid": False})
        errors.append("materialization manifest is missing the decision page")
    for path in manifest_paths:
        exists = path.exists()
        checks.append({"check": f"exists:{path.name}", "valid": exists})
        if not exists:
            errors.append(f"missing projected file {path.as_posix()}")

    readiness = validate_onboarding_readiness(vault)
    checks.extend(readiness["checks"])
    errors.extend(readiness["errors"])
    return {"ready": not errors, "checks": checks, "errors": errors}


def _write_markdown_page(
    target: Path,
    *,
    page_type: str,
    title: str,
    body: str,
    domains: list[str],
    relates_to: list[str] | None = None,
    sources: list[str] | None = None,
    extra_frontmatter: dict[str, Any] | None = None,
    force: bool = False,
) -> None:
    tags = FALLBACK_TAGS[page_type] if page_type in FALLBACK_TAGS else default_tags(page_type)
    frontmatter = {
        "id": target.stem,
        "type": page_type,
        "title": title,
        "status": "active",
        "created": _today(),
        "last_updated": _today(),
        "aliases": [],
        "tags": tags,
        "domains": domains,
        "relates_to": relates_to or [],
        "sources": sources or [],
    }
    if extra_frontmatter:
        frontmatter.update(extra_frontmatter)
    write_page(target, frontmatter=frontmatter, body=body, force=force)


def _summary_id(bundle_id: str, kind: str) -> str:
    return f"summary-onboarding-{bundle_id}-{kind}"


def _summary_path(vault: Vault, bundle_id: str, kind: str) -> Path:
    return vault.wiki / "summaries" / f"{_summary_id(bundle_id, kind)}.md"


def _summary_source_links(bundle_id: str, kind: str) -> list[str]:
    overview = f"[[{_summary_id(bundle_id, 'overview')}]]"
    specific = f"[[{_summary_id(bundle_id, kind)}]]"
    return [overview] if kind == "overview" else [overview, specific]


def _write_summary_page(vault: Vault, bundle_id: str, kind: str, title: str, body: str, *, force: bool) -> Path:
    target = _summary_path(vault, bundle_id, kind)
    _write_markdown_page(
        target,
        page_type="summary",
        title=title,
        body=body,
        domains=["meta"],
        relates_to=["[[profile]]"],
        sources=[],
        extra_frontmatter={
            "source_type": "onboarding",
            "source_date": _today(),
            "ingested": _today(),
            "external_id": bundle_id,
            "source_path": f"raw/onboarding/bundles/{bundle_id}/normalized-evidence.json",
        },
        force=force,
    )
    return target


def _bullet_block(items: list[str], *, empty_line: str) -> str:
    if not items:
        return f"- {empty_line}"
    return "\n".join(f"- {item}" for item in items)


def _materialize_core_pages(vault: Vault, bundle: dict[str, Any], *, force: bool) -> list[Path]:
    bundle_id = str(bundle["bundle_id"])
    identity = bundle["identity"]
    values = bundle["values"]
    positioning = bundle["positioning"]
    inquiries = bundle["open_inquiries"]
    owner_slug = slugify(identity["name"]) if _safe_text(identity.get("name")) else ""
    owner_link = f"[[{owner_slug}]]" if owner_slug else ""

    overview_summary = _summary_id(bundle_id, "overview")
    profile_summary = _summary_id(bundle_id, "profile")
    values_summary = _summary_id(bundle_id, "values")
    positioning_summary = _summary_id(bundle_id, "positioning")
    inquiries_summary = _summary_id(bundle_id, "open-inquiries")

    profile_body_lines = [
        f"# {identity['name']}",
        "",
        identity["summary"],
        "",
        "## Snapshot",
        "",
    ]
    if _safe_text(identity.get("role")):
        profile_body_lines.append(f"- Role: {identity['role']}")
    if _safe_text(identity.get("location")):
        profile_body_lines.append(f"- Location: {identity['location']}")
    profile_body = "\n".join(profile_body_lines).rstrip() + "\n"
    _write_markdown_page(
        vault.owner_profile,
        page_type="profile",
        title=identity["name"],
        body=profile_body,
        domains=["identity", "work"],
        relates_to=["[[values]]", "[[positioning]]", *([owner_link] if owner_link else [])],
        sources=[f"[[{overview_summary}]]", f"[[{profile_summary}]]"],
        extra_frontmatter={
            "role": identity.get("role", ""),
            "location": identity.get("location", ""),
        },
        force=force,
    )

    owner_path: Path | None = None
    if owner_slug:
        owner_path = vault.wiki / "people" / f"{owner_slug}.md"
        _write_markdown_page(
            owner_path,
            page_type="person",
            title=identity["name"],
            body=f"# {identity['name']}\n\n{identity.get('summary') or ''}\n",
            domains=["identity", "relationships"],
            relates_to=["[[profile]]"],
            sources=[f"[[{overview_summary}]]", f"[[{profile_summary}]]"],
            force=force,
        )

    values_body = (
        "# Values\n\n"
        "## Operating Principles\n\n"
        f"{_bullet_block([item['text'] for item in values], empty_line='No explicit values were captured in this bundle.')}\n"
    )
    _write_markdown_page(
        vault.values_path,
        page_type="note",
        title="Values",
        body=values_body,
        domains=["identity", "craft"],
        relates_to=["[[profile]]"],
        sources=[f"[[{overview_summary}]]", f"[[{values_summary}]]"],
        force=force,
    )

    positioning_body = "\n".join(
        [
            "# Positioning",
            "",
            "## Positioning Narrative",
            "",
            positioning["summary"],
            "",
            "## Work Priorities",
            "",
            _bullet_block(list(positioning.get("work_priorities") or []), empty_line="No explicit work priorities were captured in this bundle."),
            "",
            "## Life Priorities",
            "",
            _bullet_block(list(positioning.get("life_priorities") or []), empty_line="No explicit life priorities were captured in this bundle."),
            "",
            "## Constraints",
            "",
            _bullet_block(list(positioning.get("constraints") or []), empty_line="No explicit constraints were captured in this bundle."),
        ]
    )
    _write_markdown_page(
        vault.positioning_path,
        page_type="note",
        title="Positioning",
        body=positioning_body,
        domains=["work", "identity"],
        relates_to=["[[profile]]"],
        sources=[f"[[{overview_summary}]]", f"[[{positioning_summary}]]"],
        force=force,
    )

    inquiry_lines = "\n".join(f"- {item['question']}" for item in inquiries) if inquiries else "- No open inquiries were captured."
    inquiries_body = "# Open Inquiries\n\n## Active Inquiries\n\n" + inquiry_lines + "\n"
    _write_markdown_page(
        vault.wiki / "me" / "open-inquiries.md",
        page_type="note",
        title="Open Inquiries",
        body=inquiries_body,
        domains=["meta"],
        relates_to=[],
        sources=[f"[[{overview_summary}]]", f"[[{inquiries_summary}]]"],
        force=force,
    )
    paths = [vault.owner_profile, vault.values_path, vault.positioning_path, vault.wiki / "me" / "open-inquiries.md"]
    if owner_path is not None:
        paths.insert(1, owner_path)
    return paths


def _materialize_optional_pages(vault: Vault, bundle: dict[str, Any], decisions: dict[str, Any], *, force: bool) -> list[Path]:
    created: list[Path] = []
    decision_map = {(entry["family"], entry["target"]): entry for entry in decisions.get("entries") or []}
    bundle_id = str(bundle["bundle_id"])
    owner_slug = slugify(_safe_text((bundle.get("identity") or {}).get("name")))

    for project in bundle.get("projects") or []:
        if decision_map.get(("projects", project["slug"]), {}).get("action") != "create":
            continue
        path = vault.wiki / "projects" / f"{project['slug']}.md"
        body = "\n".join(
            [
                f"# {project['title']}",
                "",
                project.get("summary") or "",
                "",
                "## Project Priorities",
                "",
                _bullet_block(list(project.get("priorities") or []), empty_line="No explicit project priorities were captured in onboarding."),
                "",
                "## Constraints",
                "",
                _bullet_block(list(project.get("constraints") or []), empty_line="No explicit project constraints were captured in onboarding."),
            ]
        )
        _write_markdown_page(
            path,
            page_type="project",
            title=project["title"],
            body=body,
            domains=["work"],
            relates_to=["[[profile]]"],
            sources=_summary_source_links(bundle_id, "overview"),
            force=force,
        )
        created.append(path)

    for person in bundle.get("people") or []:
        if owner_slug and person.get("slug") == owner_slug:
            continue
        if decision_map.get(("people", person["slug"]), {}).get("action") != "create":
            continue
        path = vault.wiki / "people" / f"{person['slug']}.md"
        body = f"# {person['title']}\n\n{person.get('summary') or ''}\n"
        _write_markdown_page(
            path,
            page_type="person",
            title=person["title"],
            body=body,
            domains=["relationships"],
            relates_to=[],
            sources=_summary_source_links(bundle_id, "overview"),
            force=force,
        )
        created.append(path)

    for family, page_type, directory in (
        ("concepts", "concept", "concepts"),
        ("playbooks", "playbook", "playbooks"),
        ("stances", "stance", "stances"),
        ("inquiries", "inquiry", "inquiries"),
    ):
        for candidate in bundle.get(family) or []:
            if decision_map.get((family, candidate["slug"]), {}).get("action") != "create":
                continue
            path = vault.wiki / directory / f"{candidate['slug']}.md"
            body_lines = [f"# {candidate['title']}", "", candidate.get("summary") or ""]
            extra_frontmatter: dict[str, Any] = {}
            if page_type == "stance":
                extra_frontmatter["position"] = candidate.get("position") or candidate.get("summary") or ""
                extra_frontmatter["confidence"] = candidate.get("confidence") or "medium"
            if page_type == "inquiry":
                extra_frontmatter["question"] = candidate.get("question") or candidate["title"]
            _write_markdown_page(
                path,
                page_type=page_type,
                title=candidate["title"],
                body="\n".join(body_lines).rstrip() + "\n",
                domains=["meta"],
                relates_to=[],
                sources=_summary_source_links(bundle_id, "overview"),
                extra_frontmatter=extra_frontmatter,
                force=force,
            )
            created.append(path)
    return created


def _write_decision_page(vault: Vault, decisions: dict[str, Any], *, force: bool) -> Path:
    bundle_id = str(decisions["bundle_id"])
    path = vault.wiki / "decisions" / f"onboarding-{bundle_id}.md"
    sections = ["# Onboarding Decisions", ""]
    for entry in decisions.get("entries") or []:
        sections.extend(
            [
                f"## {entry['family']} / {entry['target']}",
                "",
                f"- action: {entry['action']}",
                f"- confidence: {entry['confidence']}",
                f"- rationale: {entry['rationale']}",
                f"- evidence_refs: {', '.join(entry.get('evidence_refs') or []) or 'none'}",
                "",
            ]
        )
    _write_markdown_page(
        path,
        page_type="decision",
        title=f"Onboarding decisions {bundle_id}",
        body="\n".join(sections).rstrip() + "\n",
        domains=["work", "meta"],
        relates_to=["[[profile]]"],
        sources=[f"[[{_summary_id(bundle_id, 'overview')}]]"],
        force=force,
    )
    return path


def _write_summary_set(vault: Vault, bundle: dict[str, Any], decisions: dict[str, Any], *, force: bool) -> list[Path]:
    bundle_id = str(bundle["bundle_id"])
    identity = bundle["identity"]
    positioning = bundle["positioning"]
    summary_paths = [
        _write_summary_page(
            vault,
            bundle_id,
            "overview",
            f"Onboarding Overview {bundle_id}",
            "\n".join(
                [
                    "# Summary — Onboarding Overview",
                    "",
                    "## Core Identity",
                    "",
                    f"- Name: {identity.get('name') or 'unknown'}",
                    f"- Role: {identity.get('role') or 'unspecified'}",
                    f"- Location: {identity.get('location') or 'unspecified'}",
                    "",
                    "## Optional Decisions",
                    "",
                    "\n".join(
                        f"- {entry['family']} / {entry['target']} -> {entry['action']} ({entry['confidence']})"
                        for entry in decisions.get("entries") or []
                    ),
                ]
            ),
            force=force,
        ),
        _write_summary_page(
            vault,
            bundle_id,
            "profile",
            f"Onboarding Profile Summary {bundle_id}",
            f"# Summary — Profile\n\n{identity.get('summary') or ''}\n",
            force=force,
        ),
        _write_summary_page(
            vault,
            bundle_id,
            "values",
            f"Onboarding Values Summary {bundle_id}",
            "# Summary — Values\n\n" + _bullet_block([item["text"] for item in bundle.get("values") or []], empty_line="No values were captured.") + "\n",
            force=force,
        ),
        _write_summary_page(
            vault,
            bundle_id,
            "positioning",
            f"Onboarding Positioning Summary {bundle_id}",
            "\n".join(
                [
                    "# Summary — Positioning",
                    "",
                    positioning.get("summary") or "",
                    "",
                    "## Work Priorities",
                    "",
                    _bullet_block(list(positioning.get("work_priorities") or []), empty_line="No explicit work priorities were captured."),
                    "",
                    "## Constraints",
                    "",
                    _bullet_block(list(positioning.get("constraints") or []), empty_line="No explicit constraints were captured."),
                ]
            ),
            force=force,
        ),
        _write_summary_page(
            vault,
            bundle_id,
            "open-inquiries",
            f"Onboarding Open Inquiries Summary {bundle_id}",
            "# Summary — Open Inquiries\n\n" + _bullet_block([item["question"] for item in bundle.get("open_inquiries") or []], empty_line="No open inquiries were captured.") + "\n",
            force=force,
        ),
    ]
    return summary_paths


def _write_index_and_changelog(vault: Vault, paths: list[Path]) -> None:
    vault.index.parent.mkdir(parents=True, exist_ok=True)
    existing = vault.index.read_text(encoding="utf-8") if vault.index.exists() else "# INDEX\n"
    lines = existing.splitlines()
    existing_entries = set(lines)
    additions = [f"- [[{path.stem}]]" for path in paths if f"- [[{path.stem}]]" not in existing_entries]
    if additions:
        if not lines:
            lines = ["# INDEX"]
        if lines[-1] != "":
            lines.append("")
        lines.extend(additions)
        vault.index.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    if not vault.changelog.exists():
        vault.changelog.write_text("# CHANGELOG\n", encoding="utf-8")
    with vault.changelog.open("a", encoding="utf-8") as handle:
        handle.write(f"\n## {_today()} — onboard\n")
        handle.write("- Materialized onboarding bundle\n")
        for path in paths:
            handle.write(f"- [[{path.stem}]]\n")


def materialize_onboarding_bundle(repo_root: Path, *, bundle_id: str | None = None, force: bool = False) -> OnboardingStatus:
    vault = Vault.load(repo_root)
    paths, bundle, state, _decisions = load_bundle(repo_root, bundle_id=bundle_id)
    validation = validate_evidence_bundle(bundle)
    if not validation["ready_for_materialization"]:
        state.update(
            {
                "status": "blocked",
                "updated_at": _utc_timestamp_readable(),
                "validation": validation,
                "blocking_reasons": list(validation["errors"]),
            }
        )
        _json_dump(paths.state_path, state)
        _json_dump(paths.validation_path, validation)
        _write_current_pointer(paths, state=state)
        return read_onboarding_status(repo_root, bundle_id=paths.bundle_id)

    if not state.get("materialization_plan_path") or state.get("verifier_verdict") != "approved":
        verify_onboarding_bundle(repo_root, bundle_id=paths.bundle_id)
        state = _json_load(paths.state_path)

    materialization_plan_path = str(state.get("materialization_plan_path") or "")
    if not materialization_plan_path:
        return read_onboarding_status(repo_root, bundle_id=paths.bundle_id)

    plan = _json_load(Path(materialization_plan_path))
    artifact_data = plan.get("data") if isinstance(plan, dict) and isinstance(plan.get("data"), dict) else plan
    try:
        applied = apply_materialization_plan(
            repo_root,
            bundle_id=paths.bundle_id,
            plan=dict(artifact_data),
            force=force,
        )
    except PatchReviewRequiredError as exc:
        review_paths: list[str] = []
        for review in exc.reviews:
            review_root = paths.bundle_dir / "patch-reviews"
            stem = slugify(str(review.get("target_path") or review.get("page_type") or "patch-review"))
            review_paths.extend(
                [
                    (review_root / f"{stem}.json").as_posix(),
                    (review_root / f"{stem}.md").as_posix(),
                ]
            )
        state.update(
            {
                "status": "blocked",
                "updated_at": _utc_timestamp_readable(),
                "validation": validation,
                "blocking_reasons": [
                    f"semantic patch review required for {review['target_path']}"
                    for review in exc.reviews
                ],
                "artifact_paths": {
                    **dict(state.get("artifact_paths") or {}),
                    "patch_reviews": review_paths,
                },
            }
        )
        _json_dump(paths.state_path, state)
        _write_current_pointer(paths, state=state)
        return read_onboarding_status(repo_root, bundle_id=paths.bundle_id)
    materialized_pages = list(applied["materialized_pages"])
    summary_page_paths = list(applied["summary_pages"])
    decision_page = applied["decision_page"]
    # Materialization consumes the frozen plan (not the bundle content), so
    # snapshot the live bundle hash here. This becomes the authoritative
    # integrity baseline for downstream readiness checks.
    live_bundle_sha = _sha256_path(paths.evidence_bundle_path)
    materialization_manifest = {
        "bundle_id": paths.bundle_id,
        "bundle_sha256": live_bundle_sha,
        "materialization_plan_path": materialization_plan_path,
        "materialization_plan_sha256": _sha256_path(Path(materialization_plan_path)),
        "materialized_pages": materialized_pages,
        "summary_pages": summary_page_paths,
        "decision_page": decision_page,
        "backup_paths": list(applied.get("backup_paths") or []),
        "materialized_at": _utc_timestamp_readable(),
    }
    _json_dump(paths.materialization_path, materialization_manifest)
    state.update(
        {
            "status": "materialized",
            "updated_at": _utc_timestamp_readable(),
            "validation": validation,
            "bundle_sha256": live_bundle_sha,
            "materialized_pages": materialized_pages,
            "summary_pages": summary_page_paths,
            "decision_page": decision_page,
            "readiness": {"ready": False, "checks": [], "errors": []},
            "materialization_manifest": paths.materialization_path.as_posix(),
            "replay_provenance": materialization_plan_path,
        }
    )
    _json_dump(paths.state_path, state)
    _json_dump(paths.validation_path, validation)
    _write_current_pointer(paths, state=state)
    readiness = validate_onboarding_session_ready(vault)
    state["readiness"] = readiness
    _json_dump(paths.state_path, state)
    _write_current_pointer(paths, state=state)
    return read_onboarding_status(repo_root, bundle_id=paths.bundle_id)


def replay_onboarding_bundle(repo_root: Path, *, bundle_id: str | None = None, force: bool = True) -> OnboardingStatus:
    paths, _bundle, state, _decisions = load_bundle(repo_root, bundle_id=bundle_id)
    if not state.get("materialization_plan_path"):
        verify_onboarding_bundle(repo_root, bundle_id=paths.bundle_id)
    return materialize_onboarding_bundle(repo_root, bundle_id=paths.bundle_id, force=force)


def validate_onboarding_bundle_state(repo_root: Path, *, bundle_id: str | None = None) -> OnboardingStatus:
    vault = Vault.load(repo_root)
    paths, bundle, state, _decisions = load_bundle(repo_root, bundle_id=bundle_id)
    validation = validate_evidence_bundle(bundle)
    was_materialized = state.get("status") == "materialized"
    readiness = (
        validate_onboarding_session_ready(vault, bundle_id=paths.bundle_id)
        if was_materialized
        else {"ready": False, "checks": [], "errors": []}
    )
    verifier_verdict = str(state.get("verifier_verdict") or "not-run")
    if was_materialized:
        status_value = "materialized"
    elif verifier_verdict == "approved":
        status_value = "verified"
    elif state.get("synthesis_status") == "in-progress":
        status_value = "synthesizing"
    elif state.get("synthesis_status") == "synthesized":
        status_value = "synthesized"
    else:
        status_value = "validated" if validation["ready_for_materialization"] else "blocked"
    state.update(
        {
            "status": status_value,
            "updated_at": _utc_timestamp_readable(),
            "validation": validation,
            "readiness": readiness,
        }
    )
    _json_dump(paths.validation_path, validation)
    _json_dump(paths.state_path, state)
    return read_onboarding_status(repo_root, bundle_id=paths.bundle_id)


def read_onboarding_status(repo_root: Path, *, bundle_id: str | None = None) -> OnboardingStatus:
    paths, _bundle, state, _decisions = load_bundle(repo_root, bundle_id=bundle_id)
    validation = state.get("validation") or {}
    readiness = state.get("readiness") or {"ready": False, "checks": [], "errors": []}
    graph_summary = summarize_chunk_phase(paths.bundle_dir, phase="graph_nodes")
    merge_summary = summarize_chunk_phase(paths.bundle_dir, phase="merge_nodes")
    merge_relationships_summary = summarize_chunk_phase(paths.bundle_dir, phase="merge_relationships")
    status_value = str(state.get("status") or "unknown")
    if str(state.get("synthesis_status") or "") == "in-progress":
        status_value = "synthesizing"
    return OnboardingStatus(
        bundle_id=paths.bundle_id,
        status=status_value,
        ready_for_materialization=bool(validation.get("ready_for_materialization")),
        raw_input_path=str(state.get("raw_input_path") or ""),
        uploads=list(state.get("uploads") or []),
        next_questions=list(state.get("next_questions") or []),
        errors=list(validation.get("errors") or []),
        warnings=list(validation.get("warnings") or []),
        materialized_pages=list(state.get("materialized_pages") or []),
        summary_pages=list(state.get("summary_pages") or []),
        decision_page=state.get("decision_page"),
        synthesis_status=str(state.get("synthesis_status") or "not-synthesized"),
        verifier_verdict=str(state.get("verifier_verdict") or "not-run"),
        blocking_reasons=list(state.get("blocking_reasons") or []),
        materialization_plan_path=state.get("materialization_plan_path"),
        replay_provenance=state.get("replay_provenance"),
        graph_chunks_summary=graph_summary.render() if graph_summary else None,
        merge_chunks_summary=merge_summary.render() if merge_summary else None,
        merge_relationships_summary=merge_relationships_summary.render() if merge_relationships_summary else None,
        readiness=readiness,
        updated_at=str(state.get("updated_at") or ""),
    )


def migrate_onboarding_merge_artifact(repo_root: Path, *, bundle_id: str | None = None) -> OnboardingStatus:
    paths, _bundle, state, _decisions = load_bundle(repo_root, bundle_id=bundle_id)
    artifacts = onboarding_artifact_paths(paths.bundle_dir)
    merge_path = artifacts["merge"]
    graph_path = artifacts["graph"]
    if not merge_path.exists() or not graph_path.exists():
        raise FileNotFoundError("missing onboarding graph or merge artifact")
    merge_payload = _json_load(merge_path)
    graph_payload = _json_load(graph_path)
    merge_data = dict(merge_payload.get("data") or merge_payload)
    graph_data = dict(graph_payload.get("data") or graph_payload)
    node_by_id = {str(node.get("proposal_id") or ""): node for node in graph_data.get("node_proposals") or []}

    migrated = []
    changed = False
    for decision in merge_data.get("decisions") or []:
        migrated_decision = _denormalize_merge_decision(decision, node_by_id)
        if migrated_decision != decision:
            changed = True
        migrated.append(migrated_decision)
    merge_data["decisions"] = migrated
    validated = MergeArtifact.model_validate(merge_data).model_dump(mode="json")
    if changed:
        merge_payload["data"] = validated
        _json_dump(merge_path, merge_payload)
        markdown_path = artifacts["merge_markdown"]
        markdown_path.write_text(
            "# Merge Decisions\n\n```json\n" + json.dumps(merge_payload, indent=2, ensure_ascii=False) + "\n```\n",
            encoding="utf-8",
        )
        state["updated_at"] = _utc_timestamp_readable()
        _json_dump(paths.state_path, state)
    return read_onboarding_status(repo_root, bundle_id=paths.bundle_id)


def render_onboarding_materialization_plan(repo_root: Path, *, bundle_id: str | None = None) -> dict[str, Any]:
    from mind.services.onboarding_plan_builder import build_materialization_plan

    paths, bundle, _state, _decisions = load_bundle(repo_root, bundle_id=bundle_id)
    artifacts = load_pipeline_artifacts(paths.bundle_dir)
    return build_materialization_plan(
        bundle_id=paths.bundle_id,
        bundle=bundle,
        semantic=artifacts.semantic,
        graph=artifacts.graph,
        merge=artifacts.merge,
        verify=artifacts.verify,
    )


def _denormalize_merge_decision(decision: dict[str, Any], node_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    if all(str(decision.get(key) or "").strip() for key in ("source_proposal_id", "title", "slug", "summary", "page_type")):
        migrated = dict(decision)
        if "relates_to" not in migrated:
            migrated["relates_to"] = []
        if "domains" not in migrated:
            migrated["domains"] = []
        return migrated
    proposal_id = str(decision.get("proposal_id") or "")
    node = node_by_id.get(proposal_id)
    if not node:
        raise RuntimeError(f"cannot denormalize merge decision {proposal_id}; source graph proposal missing")
    migrated = dict(decision)
    migrated["source_proposal_id"] = proposal_id
    migrated["title"] = str(node.get("title") or "")
    migrated["slug"] = str(node.get("slug") or "")
    migrated["summary"] = str(node.get("summary") or "")
    migrated["page_type"] = str(node.get("page_type") or "")
    migrated["domains"] = list(node.get("domains") or [])
    migrated["relates_to"] = list(node.get("relates_to_refs") or [])
    return migrated
