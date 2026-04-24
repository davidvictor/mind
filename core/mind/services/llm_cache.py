"""Shared routed LLM cache envelope helpers."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class LLMCacheIdentity:
    task_class: str
    provider: str
    model: str
    transport: str
    api_family: str
    input_mode: str
    prompt_version: str
    request_fingerprint: dict[str, Any] | None = None
    temperature: float | None = None
    max_tokens: int | None = None
    timeout_seconds: int | None = None
    reasoning_effort: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def load_llm_cache(path: Path, *, expected: LLMCacheIdentity | list[LLMCacheIdentity]) -> Any | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    identity = payload.get("_llm")
    data = payload.get("data")
    if not isinstance(identity, dict):
        return None
    acceptable = expected if isinstance(expected, list) else [expected]
    if not any(identity_matches(identity, candidate.to_dict()) for candidate in acceptable):
        return None
    return data


def write_llm_cache(path: Path, *, identity: LLMCacheIdentity, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "_llm": identity.to_dict(),
        "data": data,
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def identity_matches(actual: dict[str, Any], expected: dict[str, Any]) -> bool:
    if actual == expected:
        return True
    comparable = dict(actual)
    if expected.get("request_fingerprint") == {"kind": "text-prompt"}:
        actual_fp = comparable.get("request_fingerprint")
        if isinstance(actual_fp, dict):
            input_parts = actual_fp.get("input_parts")
            if (
                isinstance(input_parts, list)
                and len(input_parts) == 1
                and isinstance(input_parts[0], dict)
                and input_parts[0].get("kind") == "text"
            ):
                comparable["request_fingerprint"] = {"kind": "text-prompt"}
    return comparable == expected
