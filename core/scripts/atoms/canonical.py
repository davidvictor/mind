from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Iterable, Literal, Mapping

from scripts.common.contract import canonicalize_page_type
from scripts.common.default_tags import default_tags
from scripts.common.slugify import normalize_identifier
from scripts.common.section_rewriter import ParsedSection, parse_markdown_body


ATOM_TYPES = {"concept", "playbook", "stance", "inquiry"}
PLAYBOOK_STEPS_PLACEHOLDER = "- To be expanded."
STANCE_CONTRADICTIONS_PLACEHOLDER = "- None observed yet."
_FIRST_SENTENCE_RE = re.compile(r"(.+?[.!?])(?:\s|$)")
RELATION_KINDS = (
    "supports",
    "contradicts",
    "example_of",
    "applies_to",
    "depends_on",
    "extends",
    "adjacent_to",
)
RenderMode = Literal["compact", "mature"]
_COMPACT_SECTION_ORDER = {
    "concept": ("## TL;DR", "## Evidence log"),
    "playbook": ("## TL;DR", "## Steps", "## Evidence log"),
    "stance": ("## TL;DR", "## Evidence log", "## Contradictions"),
    "inquiry": ("## TL;DR", "## Evidence log"),
}
_MATURE_SECTION_ORDER = {
    "concept": (
        "## TL;DR",
        "## Why It Matters",
        "## Mechanism",
        "## Examples",
        "## In Conversation With",
        "## Evidence log",
    ),
    "playbook": (
        "## TL;DR",
        "## When To Use",
        "## Prerequisites",
        "## Steps",
        "## Failure Modes",
        "## Evidence log",
    ),
    "stance": (
        "## TL;DR",
        "## Position",
        "## Why",
        "## Best Evidence For",
        "## Strongest Counterevidence",
        "## What Would Change My Mind",
        "## Evidence log",
        "## Contradictions",
    ),
    "inquiry": (
        "## TL;DR",
        "## Question",
        "## Why This Matters",
        "## Current Hypotheses",
        "## What Would Resolve It",
        "## Evidence log",
    ),
}
_KNOWN_SECTIONS = {heading for group in (*_COMPACT_SECTION_ORDER.values(), *_MATURE_SECTION_ORDER.values()) for heading in group}


@dataclass(frozen=True)
class CanonicalAtomPage:
    frontmatter: dict[str, Any]
    body: str


def candidate_payload(candidate: Mapping[str, Any] | object | None) -> dict[str, Any]:
    if candidate is None:
        return {}
    if isinstance(candidate, Mapping):
        return {str(key): value for key, value in candidate.items()}
    payload: dict[str, Any] = {}
    for key in (
        "type",
        "proposed_id",
        "title",
        "description",
        "tldr",
        "snippet",
        "polarity",
        "rationale",
        "domains",
        "in_conversation_with",
        "steps",
        "position",
        "question",
        "typed_relations",
        "why_it_matters",
        "mechanism",
        "examples",
        "when_to_use",
        "prerequisites",
        "failure_modes",
        "why",
        "best_evidence_for",
        "strongest_counterevidence",
        "what_would_change_my_mind",
        "current_hypotheses",
        "what_would_resolve_it",
    ):
        if hasattr(candidate, key):
            payload[key] = getattr(candidate, key)
    return payload


def page_payload(frontmatter: Mapping[str, Any], body: str) -> dict[str, Any]:
    atom_type = canonicalize_page_type(str(frontmatter.get("type") or ""))
    details = _body_details(body, title=str(frontmatter.get("title") or ""))
    payload: dict[str, Any] = {
        "type": atom_type,
        "proposed_id": str(frontmatter.get("id") or ""),
        "title": str(frontmatter.get("title") or ""),
        "description": details["intro"],
        "tldr": details["tldr"],
        "domains": _coerce_text_list(frontmatter.get("domains")),
        "in_conversation_with": [
            item[2:-2]
            for item in _coerce_text_list(frontmatter.get("relates_to"))
            if item.startswith("[[") and item.endswith("]]")
        ],
        "position": str(frontmatter.get("position") or details["legacy_position"] or "").strip(),
        "question": str(frontmatter.get("question") or details["legacy_question"] or "").strip(),
        "steps": _bullet_items(details["steps"]),
        "typed_relations": _normalize_typed_relations(frontmatter.get("typed_relations")),
        "why_it_matters": details["why_it_matters"],
        "mechanism": details["mechanism"],
        "examples": details["examples"],
        "when_to_use": details["when_to_use"],
        "prerequisites": details["prerequisites"],
        "failure_modes": details["failure_modes"],
        "why": details["why"],
        "best_evidence_for": details["best_evidence_for"],
        "strongest_counterevidence": details["strongest_counterevidence"],
        "what_would_change_my_mind": details["what_would_change_my_mind"],
        "current_hypotheses": details["current_hypotheses"],
        "what_would_resolve_it": details["what_would_resolve_it"],
    }
    return payload


def canonicalize_atom_page(
    *,
    frontmatter: Mapping[str, Any],
    body: str,
    candidate: Mapping[str, Any] | object | None = None,
    force_lifecycle_state: str | None = None,
    render_mode: RenderMode = "compact",
    replace_relations: bool = False,
) -> CanonicalAtomPage:
    payload = candidate_payload(candidate)
    atom_type = canonicalize_page_type(str(frontmatter.get("type") or payload.get("type") or ""))
    if atom_type not in ATOM_TYPES:
        raise KeyError(f"unsupported atom type: {atom_type}")

    title = _preferred_text(
        str(frontmatter.get("title") or ""),
        str(payload.get("title") or ""),
    ) or _slug_title(str(frontmatter.get("id") or payload.get("proposed_id") or "untitled"))
    atom_id = normalize_identifier(str(frontmatter.get("id") or payload.get("proposed_id") or title))
    if not atom_id:
        raise ValueError("atom id must be non-empty")

    details = _body_details(body, title=title)
    intro = _select_intro(
        atom_type=atom_type,
        title=title,
        existing_intro=details["intro"],
        payload=payload,
        frontmatter=frontmatter,
        legacy_position=details["legacy_position"],
        legacy_question=details["legacy_question"],
    )
    tldr = _select_tldr(
        title=title,
        existing_tldr=details["tldr"],
        intro=intro,
        payload=payload,
    )
    evidence = _normalize_bullet_block(details["evidence"])
    contradictions = _normalize_bullet_block(details["contradictions"])
    steps = _normalize_bullet_block(details["steps"])

    if atom_type == "playbook" and not _bullet_items(steps):
        candidate_steps = _normalize_bullet_block(_list_to_bullets(_coerce_text_list(payload.get("steps"))))
        steps = candidate_steps or PLAYBOOK_STEPS_PLACEHOLDER

    if atom_type == "stance" and not _bullet_items(contradictions):
        contradictions = STANCE_CONTRADICTIONS_PLACEHOLDER

    typed_relations = _merge_typed_relations(frontmatter, payload, replace=replace_relations)
    relates_to = _merge_relates_to(
        frontmatter,
        payload,
        replace=replace_relations,
        typed_relations=typed_relations,
    )
    domains = _select_domains(frontmatter, payload)
    tags = _select_tags(atom_type, frontmatter)
    sources = _coerce_text_list(frontmatter.get("sources"))
    aliases = _coerce_text_list(frontmatter.get("aliases"))

    rendered_frontmatter = _canonical_frontmatter(
        atom_type=atom_type,
        atom_id=atom_id,
        title=title,
        frontmatter=frontmatter,
        domains=domains,
        relates_to=relates_to,
        sources=sources,
        aliases=aliases,
        tags=tags,
        payload=payload,
        intro=intro,
        force_lifecycle_state=force_lifecycle_state,
        typed_relations=typed_relations,
        legacy_question=details["legacy_question"],
        legacy_position=details["legacy_position"],
    )

    rendered_body = _render_body(
        atom_type=atom_type,
        render_mode=render_mode,
        title=title,
        intro=intro,
        tldr=tldr,
        evidence=evidence,
        steps=steps,
        contradictions=contradictions,
        details=details,
        payload=payload,
        extra_sections=details["extra_sections"],
    )
    return CanonicalAtomPage(frontmatter=rendered_frontmatter, body=rendered_body)


def _canonical_frontmatter(
    *,
    atom_type: str,
    atom_id: str,
    title: str,
    frontmatter: Mapping[str, Any],
    domains: list[str],
    relates_to: list[str],
    sources: list[str],
    aliases: list[str],
    tags: list[str],
    payload: Mapping[str, Any],
    intro: str,
    force_lifecycle_state: str | None,
    typed_relations: dict[str, list[str]],
    legacy_question: str,
    legacy_position: str,
) -> dict[str, Any]:
    created = str(frontmatter.get("created") or "")
    last_updated = str(frontmatter.get("last_updated") or created or "")
    lifecycle_state = str(force_lifecycle_state or frontmatter.get("lifecycle_state") or "active")
    last_evidence_date = str(frontmatter.get("last_evidence_date") or last_updated or created or "")
    last_dream_pass = str(frontmatter.get("last_dream_pass") or last_evidence_date or last_updated or created or "")
    evidence_count = _coerce_int(frontmatter.get("evidence_count"))

    canonical: dict[str, Any] = {
        "id": atom_id,
        "type": atom_type,
        "title": title,
        "status": str(frontmatter.get("status") or "active"),
        "created": created,
        "last_updated": last_updated,
        "aliases": aliases,
        "tags": tags,
        "domains": domains,
        "relates_to": relates_to,
        "sources": sources,
        "typed_relations": typed_relations,
    }

    if atom_type == "concept":
        canonical["lifecycle_state"] = lifecycle_state
        canonical["last_evidence_date"] = last_evidence_date
        canonical["evidence_count"] = evidence_count
        canonical["category"] = _preserve_string_or_none(frontmatter.get("category"))
        canonical["first_encountered"] = str(frontmatter.get("first_encountered") or created or "")
        canonical["last_dream_pass"] = last_dream_pass
    elif atom_type == "playbook":
        canonical["derived_from"] = _coerce_text_list(frontmatter.get("derived_from"))
        canonical["applied_by_owner"] = bool(frontmatter.get("applied_by_owner"))
        canonical["lifecycle_state"] = lifecycle_state
        canonical["last_evidence_date"] = last_evidence_date
        canonical["evidence_count"] = evidence_count
        canonical["last_dream_pass"] = last_dream_pass
    elif atom_type == "stance":
        canonical["position"] = _select_position(frontmatter, payload, intro=intro, legacy_position=legacy_position)
        canonical["confidence"] = str(frontmatter.get("confidence") or "probationary")
        canonical["evidence_for_count"] = _coerce_int(frontmatter.get("evidence_for_count"))
        canonical["evidence_against_count"] = _coerce_int(frontmatter.get("evidence_against_count"))
        canonical["owner_alignment"] = str(frontmatter.get("owner_alignment") or "unknown")
        canonical["lifecycle_state"] = lifecycle_state
        canonical["last_evidence_date"] = last_evidence_date
        canonical["last_dream_pass"] = last_dream_pass
        canonical["evidence_count"] = evidence_count
    else:
        canonical["question"] = _select_question(frontmatter, payload, title=title, legacy_question=legacy_question)
        canonical["origin"] = str(frontmatter.get("origin") or "extracted")
        canonical["resolution"] = frontmatter.get("resolution")
        canonical["sources_pro"] = _coerce_text_list(frontmatter.get("sources_pro"))
        canonical["sources_con"] = _coerce_text_list(frontmatter.get("sources_con"))
        canonical["last_evidence_date"] = last_evidence_date
        canonical["last_dream_pass"] = last_dream_pass
        canonical["lifecycle_state"] = lifecycle_state
        canonical["evidence_count"] = evidence_count

    used = set(canonical)
    for key, value in frontmatter.items():
        if key in used:
            continue
        canonical[key] = value
    return canonical


def _render_body(
    *,
    atom_type: str,
    render_mode: RenderMode,
    title: str,
    intro: str,
    tldr: str,
    evidence: str,
    steps: str,
    contradictions: str,
    details: dict[str, Any],
    payload: Mapping[str, Any],
    extra_sections: list[ParsedSection],
) -> str:
    parts = [f"# {title}", ""]
    if intro:
        parts.extend([intro.strip(), ""])
    section_order = _MATURE_SECTION_ORDER[atom_type] if render_mode == "mature" else _COMPACT_SECTION_ORDER[atom_type]
    section_content = _section_content(
        atom_type=atom_type,
        render_mode=render_mode,
        tldr=tldr,
        evidence=evidence,
        steps=steps,
        contradictions=contradictions,
        details=details,
        payload=payload,
    )
    for heading in section_order:
        content = section_content.get(heading, "").strip()
        parts.extend([heading, ""])
        if content:
            parts.extend([content, ""])
        else:
            parts.append("")
    for section in _preserved_known_sections(atom_type=atom_type, render_mode=render_mode, details=details):
        parts.extend([section.heading, ""])
        if section.content.strip():
            parts.extend([section.content.strip(), ""])
    for section in extra_sections:
        content = section.content.strip()
        parts.extend([section.heading, ""])
        if content:
            parts.extend([content, ""])
    while parts and not parts[-1]:
        parts.pop()
    return "\n".join(parts).rstrip() + "\n"


def _body_details(body: str, *, title: str) -> dict[str, Any]:
    parsed = parse_markdown_body(body)
    intro = _strip_h1(parsed.intro, title=title)
    sections: dict[str, str] = {}
    extra_sections: list[ParsedSection] = []
    legacy_position = ""
    legacy_question = ""
    for section in parsed.sections:
        content = section.content.strip()
        if section.heading == "## Position":
            legacy_position = content
            continue
        if section.heading == "## The Question":
            legacy_question = content
            continue
        if section.heading in _KNOWN_SECTIONS:
            sections[section.heading] = content
            continue
        extra_sections.append(ParsedSection(heading=section.heading, content=content))
    return {
        "intro": intro,
        "tldr": sections.get("## TL;DR", ""),
        "why_it_matters": sections.get("## Why It Matters", "") or sections.get("## Why This Matters", ""),
        "mechanism": sections.get("## Mechanism", ""),
        "examples": sections.get("## Examples", ""),
        "in_conversation_with": sections.get("## In Conversation With", ""),
        "when_to_use": sections.get("## When To Use", ""),
        "prerequisites": sections.get("## Prerequisites", ""),
        "steps": sections.get("## Steps", ""),
        "failure_modes": sections.get("## Failure Modes", ""),
        "evidence": sections.get("## Evidence log", ""),
        "why": sections.get("## Why", ""),
        "best_evidence_for": sections.get("## Best Evidence For", ""),
        "strongest_counterevidence": sections.get("## Strongest Counterevidence", ""),
        "what_would_change_my_mind": sections.get("## What Would Change My Mind", ""),
        "contradictions": sections.get("## Contradictions", ""),
        "question_section": sections.get("## Question", ""),
        "current_hypotheses": sections.get("## Current Hypotheses", ""),
        "what_would_resolve_it": sections.get("## What Would Resolve It", ""),
        "legacy_position": legacy_position,
        "legacy_question": legacy_question,
        "extra_sections": extra_sections,
    }


def _strip_h1(text: str, *, title: str) -> str:
    lines = text.strip().splitlines()
    if not lines:
        return ""
    if lines[0].startswith("# "):
        lines = lines[1:]
        while lines and not lines[0].strip():
            lines = lines[1:]
    joined = "\n".join(lines).strip()
    if joined == title.strip():
        return ""
    return joined


def _select_intro(
    *,
    atom_type: str,
    title: str,
    existing_intro: str,
    payload: Mapping[str, Any],
    frontmatter: Mapping[str, Any],
    legacy_position: str,
    legacy_question: str,
) -> str:
    if existing_intro.strip():
        return existing_intro.strip()
    description = str(payload.get("description") or "").strip()
    if description:
        return description
    if atom_type == "stance":
        position = _select_position(frontmatter, payload, intro="", legacy_position=legacy_position)
        if position:
            return position
    if atom_type == "inquiry":
        question = _select_question(frontmatter, payload, title=title, legacy_question=legacy_question)
        if question:
            return question
    return title


def _select_tldr(*, title: str, existing_tldr: str, intro: str, payload: Mapping[str, Any]) -> str:
    if existing_tldr.strip():
        return existing_tldr.strip()
    payload_tldr = str(payload.get("tldr") or "").strip()
    if payload_tldr:
        return payload_tldr
    description = str(payload.get("description") or "").strip()
    return _first_sentence(description or intro or title)


def _select_position(
    frontmatter: Mapping[str, Any],
    payload: Mapping[str, Any],
    *,
    intro: str,
    legacy_position: str,
) -> str:
    current = str(frontmatter.get("position") or "").strip()
    candidate = str(payload.get("position") or "").strip()
    if current and current.lower() != "null":
        return current
    if candidate:
        return candidate
    if legacy_position:
        return legacy_position
    return _first_sentence(str(payload.get("description") or "").strip() or intro)


def _select_question(
    frontmatter: Mapping[str, Any],
    payload: Mapping[str, Any],
    *,
    title: str,
    legacy_question: str,
) -> str:
    current = str(frontmatter.get("question") or "").strip()
    candidate = str(payload.get("question") or "").strip()
    if current and current != title:
        return current
    if candidate:
        return candidate
    if legacy_question:
        return legacy_question
    return title


def _select_domains(frontmatter: Mapping[str, Any], payload: Mapping[str, Any]) -> list[str]:
    current = _coerce_text_list(frontmatter.get("domains"))
    candidate = _coerce_text_list(payload.get("domains"))
    if current and current != ["meta"]:
        return current
    if candidate:
        return candidate
    return current or ["meta"]


def _merge_relates_to(
    frontmatter: Mapping[str, Any],
    payload: Mapping[str, Any],
    *,
    replace: bool,
    typed_relations: dict[str, list[str]],
) -> list[str]:
    current = [] if replace else _normalize_wikilinks(_coerce_text_list(frontmatter.get("relates_to")))
    candidate = _normalize_wikilinks(
        [f"[[{item}]]" for item in _coerce_text_list(payload.get("in_conversation_with"))]
    )
    relation_targets: list[str] = []
    for values in typed_relations.values():
        relation_targets.extend(values)
    merged: list[str] = []
    seen: set[str] = set()
    for item in [*current, *candidate, *relation_targets]:
        if item in seen:
            continue
        merged.append(item)
        seen.add(item)
    return merged


def _merge_typed_relations(
    frontmatter: Mapping[str, Any],
    payload: Mapping[str, Any],
    *,
    replace: bool,
) -> dict[str, list[str]]:
    current = {} if replace else _normalize_typed_relations(frontmatter.get("typed_relations"))
    candidate = _normalize_typed_relations(payload.get("typed_relations"))
    merged: dict[str, list[str]] = {kind: list(current.get(kind, [])) for kind in RELATION_KINDS}
    for kind in RELATION_KINDS:
        if not candidate.get(kind):
            continue
        existing = merged.setdefault(kind, [])
        for item in candidate[kind]:
            if item not in existing:
                existing.append(item)
    return {kind: values for kind, values in merged.items() if values}


def _select_tags(atom_type: str, frontmatter: Mapping[str, Any]) -> list[str]:
    current = _coerce_text_list(frontmatter.get("tags"))
    return current or default_tags(atom_type)


def _normalize_wikilinks(items: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in items:
        cleaned = str(item).strip()
        if not cleaned:
            continue
        if not cleaned.startswith("[["):
            cleaned = f"[[{cleaned}]]"
        if cleaned in seen:
            continue
        normalized.append(cleaned)
        seen.add(cleaned)
    return normalized


def _normalize_typed_relations(value: Any) -> dict[str, list[str]]:
    if not isinstance(value, Mapping):
        return {}
    normalized: dict[str, list[str]] = {}
    for kind in RELATION_KINDS:
        if kind not in value:
            continue
        links = _normalize_wikilinks(_coerce_text_list(value.get(kind)))
        if links:
            normalized[kind] = links
    return normalized


def _first_sentence(text: str) -> str:
    cleaned = " ".join(part.strip() for part in str(text or "").splitlines() if part.strip()).strip()
    if not cleaned:
        return ""
    match = _FIRST_SENTENCE_RE.match(cleaned)
    return (match.group(1) if match else cleaned).strip()


def _preferred_text(*values: str) -> str:
    for value in values:
        cleaned = str(value or "").strip()
        if cleaned:
            return cleaned
    return ""


def _coerce_text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        cleaned = value.strip()
        return [cleaned] if cleaned else []
    return []


def _list_to_bullets(items: Iterable[str]) -> str:
    cleaned = [str(item).strip() for item in items if str(item).strip()]
    return "\n".join(f"- {item}" for item in cleaned)


def _bullet_items(content: str) -> list[str]:
    return [line.rstrip() for line in content.splitlines() if line.strip().startswith("- ")]


def _normalize_bullet_block(content: str) -> str:
    lines = [line.rstrip() for line in str(content or "").splitlines() if line.strip()]
    return "\n".join(lines)


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _slug_title(text: str) -> str:
    cleaned = str(text or "").strip("- ").replace("-", " ")
    words = [word for word in cleaned.split() if word]
    return " ".join(word.capitalize() for word in words) or "Untitled"


def _preserve_string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _section_content(
    *,
    atom_type: str,
    render_mode: RenderMode,
    tldr: str,
    evidence: str,
    steps: str,
    contradictions: str,
    details: dict[str, Any],
    payload: Mapping[str, Any],
) -> dict[str, str]:
    content = {
        "## TL;DR": tldr,
        "## Evidence log": evidence,
        "## Steps": steps,
        "## Contradictions": contradictions,
    }
    if render_mode == "compact":
        return content
    if atom_type == "concept":
        content.update(
            {
                "## Why It Matters": _prefer_section(payload.get("why_it_matters"), details["why_it_matters"]),
                "## Mechanism": _prefer_section(payload.get("mechanism"), details["mechanism"]),
                "## Examples": _prefer_section(payload.get("examples"), details["examples"]),
                "## In Conversation With": _render_links_or_block(
                    payload.get("in_conversation_with"),
                    details["in_conversation_with"],
                ),
            }
        )
    elif atom_type == "playbook":
        content.update(
            {
                "## When To Use": _prefer_section(payload.get("when_to_use"), details["when_to_use"]),
                "## Prerequisites": _prefer_section(payload.get("prerequisites"), details["prerequisites"]),
                "## Failure Modes": _prefer_section(payload.get("failure_modes"), details["failure_modes"]),
            }
        )
    elif atom_type == "stance":
        content.update(
            {
                "## Position": _prefer_section(payload.get("position"), details["legacy_position"]),
                "## Why": _prefer_section(payload.get("why"), details["why"]),
                "## Best Evidence For": _prefer_section(payload.get("best_evidence_for"), details["best_evidence_for"]),
                "## Strongest Counterevidence": _prefer_section(
                    payload.get("strongest_counterevidence"),
                    details["strongest_counterevidence"],
                ),
                "## What Would Change My Mind": _prefer_section(
                    payload.get("what_would_change_my_mind"),
                    details["what_would_change_my_mind"],
                ),
            }
        )
    else:
        content.update(
            {
                "## Question": _prefer_section(payload.get("question"), details["question_section"] or details["legacy_question"]),
                "## Why This Matters": _prefer_section(payload.get("why_it_matters"), details["why_it_matters"]),
                "## Current Hypotheses": _prefer_section(payload.get("current_hypotheses"), details["current_hypotheses"]),
                "## What Would Resolve It": _prefer_section(
                    payload.get("what_would_resolve_it"),
                    details["what_would_resolve_it"],
                ),
            }
        )
    return content


def _preserved_known_sections(*, atom_type: str, render_mode: RenderMode, details: dict[str, Any]) -> list[ParsedSection]:
    if render_mode == "mature":
        return []
    preserved: list[ParsedSection] = []
    current_order = set(_COMPACT_SECTION_ORDER[atom_type])
    candidate_headings = [
        "## Position",
        "## Question",
        "## Why It Matters",
        "## Mechanism",
        "## Examples",
        "## In Conversation With",
        "## When To Use",
        "## Prerequisites",
        "## Failure Modes",
        "## Why",
        "## Best Evidence For",
        "## Strongest Counterevidence",
        "## What Would Change My Mind",
        "## Current Hypotheses",
        "## What Would Resolve It",
    ]
    for heading in candidate_headings:
        if heading in current_order:
            continue
        if heading == "## Position" and details["legacy_position"]:
            preserved.append(ParsedSection(heading=heading, content=details["legacy_position"]))
            continue
        if heading == "## Question" and (details["question_section"] or details["legacy_question"]):
            preserved.append(ParsedSection(heading=heading, content=details["question_section"] or details["legacy_question"]))
            continue
        key = _section_key_for_heading(heading)
        if key and str(details.get(key) or "").strip():
            preserved.append(ParsedSection(heading=heading, content=str(details[key]).strip()))
    return preserved


def _section_key_for_heading(heading: str) -> str | None:
    mapping = {
        "## Why It Matters": "why_it_matters",
        "## Mechanism": "mechanism",
        "## Examples": "examples",
        "## In Conversation With": "in_conversation_with",
        "## When To Use": "when_to_use",
        "## Prerequisites": "prerequisites",
        "## Failure Modes": "failure_modes",
        "## Why": "why",
        "## Best Evidence For": "best_evidence_for",
        "## Strongest Counterevidence": "strongest_counterevidence",
        "## What Would Change My Mind": "what_would_change_my_mind",
        "## Current Hypotheses": "current_hypotheses",
        "## What Would Resolve It": "what_would_resolve_it",
    }
    return mapping.get(heading)


def _prefer_section(payload_value: Any, existing: str) -> str:
    if isinstance(payload_value, list):
        rendered = _list_to_bullets(_coerce_text_list(payload_value))
        if rendered:
            return rendered
    cleaned = str(payload_value or "").strip()
    if cleaned:
        return cleaned
    return str(existing or "").strip()


def _render_links_or_block(payload_value: Any, existing: str) -> str:
    links = _normalize_wikilinks(_coerce_text_list(payload_value))
    if links:
        return "\n".join(f"- {item}" for item in links)
    return str(existing or "").strip()
