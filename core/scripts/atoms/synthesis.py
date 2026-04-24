from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any, Mapping

from mind.services.llm_service import get_llm_service

from .canonical import RELATION_KINDS


ACTIVE_SYNTHESIS_PROMPT_VERSION = "dream.active-synthesis.v1"


@dataclass(frozen=True)
class ActiveSynthesisResult:
    intro: str
    tldr: str
    in_conversation_with: list[str] = field(default_factory=list)
    typed_relations: dict[str, list[str]] = field(default_factory=dict)
    why_it_matters: str = ""
    mechanism: str = ""
    examples: list[str] = field(default_factory=list)
    when_to_use: str = ""
    prerequisites: list[str] = field(default_factory=list)
    steps: list[str] = field(default_factory=list)
    failure_modes: list[str] = field(default_factory=list)
    position: str = ""
    why: str = ""
    best_evidence_for: list[str] = field(default_factory=list)
    strongest_counterevidence: list[str] = field(default_factory=list)
    what_would_change_my_mind: list[str] = field(default_factory=list)
    question: str = ""
    current_hypotheses: list[str] = field(default_factory=list)
    what_would_resolve_it: list[str] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return {
            "intro": self.intro,
            "tldr": self.tldr,
            "in_conversation_with": self.in_conversation_with,
            "typed_relations": self.typed_relations,
            "why_it_matters": self.why_it_matters,
            "mechanism": self.mechanism,
            "examples": self.examples,
            "when_to_use": self.when_to_use,
            "prerequisites": self.prerequisites,
            "steps": self.steps,
            "failure_modes": self.failure_modes,
            "position": self.position,
            "why": self.why,
            "best_evidence_for": self.best_evidence_for,
            "strongest_counterevidence": self.strongest_counterevidence,
            "what_would_change_my_mind": self.what_would_change_my_mind,
            "question": self.question,
            "current_hypotheses": self.current_hypotheses,
            "what_would_resolve_it": self.what_would_resolve_it,
        }


def _json_default(value: Any) -> str:
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return str(isoformat())
        except Exception:
            pass
    return str(value)


def _json_dumps(value: Any, *, sort_keys: bool = False, indent: int | None = None) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=sort_keys,
        indent=indent,
        default=_json_default,
    )


def run_active_synthesis(
    *,
    atom_type: str,
    atom_id: str,
    title: str,
    frontmatter: Mapping[str, Any],
    body: str,
    evidence_log: list[str],
    typed_neighbors: list[dict[str, str]],
    generic_neighbors: list[dict[str, str]],
    contradiction_signals: list[str],
    cooccurrence_signals: list[str],
) -> ActiveSynthesisResult:
    prompt = build_active_synthesis_prompt(
        atom_type=atom_type,
        atom_id=atom_id,
        title=title,
        frontmatter=frontmatter,
        body=body,
        evidence_log=evidence_log,
        typed_neighbors=typed_neighbors,
        generic_neighbors=generic_neighbors,
        contradiction_signals=contradiction_signals,
        cooccurrence_signals=cooccurrence_signals,
    )
    data = get_llm_service().generate_json_prompt(
        prompt,
        task_class="dream",
        prompt_version=ACTIVE_SYNTHESIS_PROMPT_VERSION,
    )
    return parse_active_synthesis_result(data, atom_type=atom_type)


def build_active_synthesis_prompt(
    *,
    atom_type: str,
    atom_id: str,
    title: str,
    frontmatter: Mapping[str, Any],
    body: str,
    evidence_log: list[str],
    typed_neighbors: list[dict[str, str]],
    generic_neighbors: list[dict[str, str]],
    contradiction_signals: list[str],
    cooccurrence_signals: list[str],
) -> str:
    section_spec = {
        "concept": ["intro", "tldr", "why_it_matters", "mechanism", "examples"],
        "playbook": ["intro", "tldr", "when_to_use", "prerequisites", "steps", "failure_modes"],
        "stance": ["intro", "tldr", "position", "why", "best_evidence_for", "strongest_counterevidence", "what_would_change_my_mind"],
        "inquiry": ["intro", "tldr", "question", "why_it_matters", "current_hypotheses", "what_would_resolve_it"],
    }[atom_type]
    return (
        "You are rewriting a mature Brain atom into a richer canonical knowledge page.\n\n"
        "Hard rules:\n"
        "- Use only facts already present in the provided local evidence, page body, and linked local graph context.\n"
        "- Do not invent outside facts, citations, or examples.\n"
        "- Prefer concise, high-signal writing over essay length.\n"
        "- typed_relations may only use these keys: "
        + ", ".join(RELATION_KINDS)
        + ".\n"
        "- typed_relations targets must be atom ids from the provided neighbor context.\n"
        "- If support for a section is weak, write a short conservative section rather than fabricating detail.\n\n"
        f"Atom type: {atom_type}\n"
        f"Atom id: {atom_id}\n"
        f"Title: {title}\n\n"
        "Required output fields for this atom type:\n"
        + "\n".join(f"- {field}" for field in section_spec)
        + "\n- in_conversation_with\n- typed_relations\n\n"
        "Current frontmatter:\n"
        f"{_json_dumps(dict(frontmatter), sort_keys=True)}\n\n"
        "Current body:\n"
        f"{body}\n\n"
        "Evidence log entries:\n"
        f"{_json_dumps(evidence_log, indent=2)}\n\n"
        "Typed neighbors:\n"
        f"{_json_dumps(typed_neighbors, indent=2)}\n\n"
        "Generic neighbors:\n"
        f"{_json_dumps(generic_neighbors, indent=2)}\n\n"
        "Contradiction signals:\n"
        f"{_json_dumps(contradiction_signals, indent=2)}\n\n"
        "Cooccurrence signals:\n"
        f"{_json_dumps(cooccurrence_signals, indent=2)}\n\n"
        "Return only JSON. Use strings for prose sections and arrays of strings for bullet-list sections.\n"
    )


def parse_active_synthesis_result(data: Mapping[str, Any] | Any, *, atom_type: str) -> ActiveSynthesisResult:
    if not isinstance(data, Mapping):
        raise ValueError("active synthesis response must be an object")
    intro = str(data.get("intro") or "").strip()
    tldr = str(data.get("tldr") or "").strip()
    if not intro:
        raise ValueError("active synthesis response missing intro")
    if not tldr:
        raise ValueError("active synthesis response missing tldr")
    typed_relations = _normalize_typed_relations(data.get("typed_relations"))
    in_conversation_with = _coerce_str_list(data.get("in_conversation_with"))

    result = ActiveSynthesisResult(
        intro=intro,
        tldr=tldr,
        in_conversation_with=in_conversation_with,
        typed_relations=typed_relations,
        why_it_matters=str(data.get("why_it_matters") or "").strip(),
        mechanism=str(data.get("mechanism") or "").strip(),
        examples=_coerce_str_list(data.get("examples")),
        when_to_use=str(data.get("when_to_use") or "").strip(),
        prerequisites=_coerce_str_list(data.get("prerequisites")),
        steps=_coerce_str_list(data.get("steps")),
        failure_modes=_coerce_str_list(data.get("failure_modes")),
        position=str(data.get("position") or "").strip(),
        why=str(data.get("why") or "").strip(),
        best_evidence_for=_coerce_str_list(data.get("best_evidence_for")),
        strongest_counterevidence=_coerce_str_list(data.get("strongest_counterevidence")),
        what_would_change_my_mind=_coerce_str_list(data.get("what_would_change_my_mind")),
        question=str(data.get("question") or "").strip(),
        current_hypotheses=_coerce_str_list(data.get("current_hypotheses")),
        what_would_resolve_it=_coerce_str_list(data.get("what_would_resolve_it")),
    )
    _validate_result(atom_type, result)
    return result


def _validate_result(atom_type: str, result: ActiveSynthesisResult) -> None:
    if atom_type == "concept":
        if not result.why_it_matters:
            raise ValueError("concept synthesis missing why_it_matters")
        if not result.mechanism:
            raise ValueError("concept synthesis missing mechanism")
    elif atom_type == "playbook":
        if not result.when_to_use:
            raise ValueError("playbook synthesis missing when_to_use")
        if not result.steps:
            raise ValueError("playbook synthesis missing steps")
    elif atom_type == "stance":
        if not result.position:
            raise ValueError("stance synthesis missing position")
        if not result.why:
            raise ValueError("stance synthesis missing why")
    elif atom_type == "inquiry":
        if not result.question:
            raise ValueError("inquiry synthesis missing question")
        if not result.what_would_resolve_it:
            raise ValueError("inquiry synthesis missing what_would_resolve_it")


def _coerce_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        cleaned = value.strip()
        return [cleaned] if cleaned else []
    return []


def _normalize_typed_relations(value: Any) -> dict[str, list[str]]:
    if not isinstance(value, Mapping):
        return {}
    normalized: dict[str, list[str]] = {}
    for kind in RELATION_KINDS:
        items = _coerce_str_list(value.get(kind))
        if items:
            normalized[kind] = items
    return normalized
