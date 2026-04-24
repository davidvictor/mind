"""Deterministic materialization-plan builder for Brain onboarding.

This module replaces the LLM-backed `plan_onboarding_materialization`
call. By the time merge decisions exist the plan is a mechanical
projection over semantic + graph + merge artifacts, so the LLM is
redundant and was observed to drop entities silently.

The builder emits a full `MaterializationPlan` dict ready to be
validated by the existing Pydantic model and consumed by
`apply_materialization_plan`.
"""
from __future__ import annotations

from typing import Any, Iterable

from scripts.common.slugify import normalize_identifier


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def build_materialization_plan(
    *,
    bundle_id: str,
    bundle: dict[str, Any],
    semantic: dict[str, Any],
    graph: dict[str, Any],
    merge: dict[str, Any],
    verify: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Produce a MaterializationPlan dict from upstream artifacts.

    Args:
        bundle_id: Bundle identifier (e.g. ``20260414t151530z``).
        bundle: Normalized evidence bundle (raw-input.json contents).
        semantic: ``synthesis-semantic.json`` data payload.
        graph: ``synthesis-graph.json`` data payload.
        merge: ``merge-decisions.json`` data payload.
        verify: ``verify-report.json`` data payload (optional; notes copied).

    Returns:
        A dict matching the ``MaterializationPlan`` Pydantic schema.
    """
    owner = semantic.get("owner") or {}
    node_proposals = {
        str(node.get("proposal_id") or ""): node
        for node in graph.get("node_proposals") or []
        if node.get("proposal_id")
    }
    semantic_entities = {
        str(entity.get("proposal_id") or ""): entity
        for entity in semantic.get("entities") or []
        if entity.get("proposal_id")
    }
    kept_relationship_edges = _kept_edges(graph, merge)
    relates_to_index = _build_relates_index(kept_relationship_edges, node_proposals)

    owner_slug = _slugify(str(owner.get("name") or "owner"))

    pages: list[dict[str, Any]] = []
    plan_counter = _PlanCounter()

    # 1-5: Fixed owner pages
    pages.append(_build_owner_profile(
        plan_counter, owner, owner_slug,
        node_proposals, merge,
    ))
    pages.append(_build_owner_values(
        plan_counter, owner, owner_slug,
    ))
    pages.append(_build_owner_positioning(
        plan_counter, owner, owner_slug,
    ))
    pages.append(_build_owner_open_inquiries(
        plan_counter, owner, owner_slug,
    ))
    pages.append(_build_owner_person(
        plan_counter, owner, owner_slug,
    ))

    # 6+: Canonical pages (one per non-skip merge decision)
    pages.extend(_build_canonical_pages(
        plan_counter,
        merge=merge,
        node_proposals=node_proposals,
        semantic_entities=semantic_entities,
        relates_to_index=relates_to_index,
    ))

    # Last: Decision page
    pages.append(_build_decision_page(
        plan_counter, bundle_id, merge, verify,
    ))

    notes = [
        f"Deterministic plan; {len([p for p in pages if p['target_kind'] == 'canonical'])} canonical pages from {len(merge.get('decisions') or [])} merge decisions.",
    ]
    if verify and verify.get("warnings"):
        notes.append(f"Verifier warnings preserved on decision page ({len(verify['warnings'])}).")

    return {
        "bundle_id": bundle_id,
        "pages": pages,
        "notes": notes,
    }


# ---------------------------------------------------------------------------
# Owner pages
# ---------------------------------------------------------------------------


def _build_owner_profile(
    counter: "_PlanCounter",
    owner: dict[str, Any],
    owner_slug: str,
    node_proposals: dict[str, dict[str, Any]],
    merge: dict[str, Any],
) -> dict[str, Any]:
    name = str(owner.get("name") or "Owner")
    role = str(owner.get("role") or "")
    location = str(owner.get("location") or "")
    summary = str(owner.get("summary") or "")

    # relates_to: the two most-central canonical nodes (by edge count to owner)
    owner_edges = _owner_edges(merge, node_proposals)
    top_related = [f"[[{slug}]]" for slug in owner_edges[:3]]

    lines = [f"# {name}", ""]
    if summary:
        lines.extend([summary, ""])
    lines.append("## Snapshot")
    lines.append("")
    if role:
        lines.append(f"- Role: {role}")
    if location:
        lines.append(f"- Location: {location}")
    body = "\n".join(lines).rstrip() + "\n"

    return {
        "plan_id": counter.next(),
        "target_kind": "owner_profile",
        "write_mode": "create",
        "page_type": "profile",
        "slug": owner_slug,
        "title": name,
        "body_markdown": body,
        "intro_mode": "preserve",
        "intro_markdown": None,
        "section_operations": [],
        "domains": _owner_domains(owner),
        "relates_to": ["[[values]]", "[[positioning]]", *top_related],
        "sources": [],
        "extra_frontmatter": {"role": role, "location": location},
        "target_path": None,
        "summary_kind": None,
    }


def _build_owner_values(
    counter: "_PlanCounter",
    owner: dict[str, Any],
    owner_slug: str,
) -> dict[str, Any]:
    values = list(owner.get("values") or [])
    bullets = _value_bullets(values)
    body = (
        "# Values\n\n"
        "## Operating Principles\n\n"
        f"{bullets}\n"
    )
    return {
        "plan_id": counter.next(),
        "target_kind": "owner_values",
        "write_mode": "create",
        "page_type": "note",
        "slug": f"{owner_slug}-values",
        "title": f"{owner.get('name') or 'Owner'} — Values",
        "body_markdown": body,
        "intro_mode": "preserve",
        "intro_markdown": None,
        "section_operations": [],
        "domains": ["identity", "craft"],
        "relates_to": ["[[profile]]"],
        "sources": [],
        "extra_frontmatter": {},
        "target_path": None,
        "summary_kind": None,
    }


def _build_owner_positioning(
    counter: "_PlanCounter",
    owner: dict[str, Any],
    owner_slug: str,
) -> dict[str, Any]:
    positioning = owner.get("positioning") or {}
    body = _positioning_body(positioning)
    return {
        "plan_id": counter.next(),
        "target_kind": "owner_positioning",
        "write_mode": "create",
        "page_type": "note",
        "slug": f"{owner_slug}-positioning",
        "title": f"{owner.get('name') or 'Owner'} — Positioning",
        "body_markdown": body,
        "intro_mode": "preserve",
        "intro_markdown": None,
        "section_operations": [],
        "domains": _owner_domains(owner),
        "relates_to": ["[[profile]]"],
        "sources": [],
        "extra_frontmatter": {},
        "target_path": None,
        "summary_kind": None,
    }


def _build_owner_open_inquiries(
    counter: "_PlanCounter",
    owner: dict[str, Any],
    owner_slug: str,
) -> dict[str, Any]:
    inquiries = list(owner.get("open_inquiries") or [])
    if inquiries:
        bullets = "\n".join(f"- {item.get('question') or item.get('title') or ''}".rstrip() for item in inquiries)
    else:
        bullets = "- No open inquiries were captured."
    body = (
        "# Open Inquiries\n\n"
        "## Active Inquiries\n\n"
        f"{bullets}\n"
    )
    return {
        "plan_id": counter.next(),
        "target_kind": "owner_open_inquiries",
        "write_mode": "create",
        "page_type": "note",
        "slug": f"{owner_slug}-open-inquiries",
        "title": f"{owner.get('name') or 'Owner'} — Open Inquiries",
        "body_markdown": body,
        "intro_mode": "preserve",
        "intro_markdown": None,
        "section_operations": [],
        "domains": ["meta"],
        "relates_to": [],
        "sources": [],
        "extra_frontmatter": {},
        "target_path": None,
        "summary_kind": None,
    }


def _build_owner_person(
    counter: "_PlanCounter",
    owner: dict[str, Any],
    owner_slug: str,
) -> dict[str, Any]:
    name = str(owner.get("name") or "Owner")
    summary = str(owner.get("summary") or "")
    role = str(owner.get("role") or "")
    location = str(owner.get("location") or "")
    lines = [f"# {name}", ""]
    if summary:
        lines.extend([summary, ""])
    lines.append("## Snapshot")
    lines.append("")
    if role:
        lines.append(f"- Role: {role}")
    if location:
        lines.append(f"- Location: {location}")
    body = "\n".join(lines).rstrip() + "\n"
    return {
        "plan_id": counter.next(),
        "target_kind": "owner_person",
        "write_mode": "create",
        "page_type": "person",
        "slug": f"{owner_slug}-person",
        "title": name,
        "body_markdown": body,
        "intro_mode": "preserve",
        "intro_markdown": None,
        "section_operations": [],
        "domains": ["identity", "relationships"],
        "relates_to": ["[[profile]]"],
        "sources": [],
        "extra_frontmatter": {},
        "target_path": None,
        "summary_kind": None,
    }


# ---------------------------------------------------------------------------
# Canonical pages
# ---------------------------------------------------------------------------


_PAGE_TYPE_TO_DIR = {
    "project": "projects",
    "concept": "concepts",
    "playbook": "playbooks",
    "stance": "stances",
    "inquiry": "inquiries",
    "person": "people",
}


def _build_canonical_pages(
    counter: "_PlanCounter",
    *,
    merge: dict[str, Any],
    node_proposals: dict[str, dict[str, Any]],
    semantic_entities: dict[str, dict[str, Any]],
    relates_to_index: dict[str, list[str]],
) -> list[dict[str, Any]]:
    pages: list[dict[str, Any]] = []
    for decision in merge.get("decisions") or []:
        action = str(decision.get("action") or "")
        if action not in {"create", "update", "merge"}:
            continue
        proposal_id = str(decision.get("proposal_id") or "")
        node = node_proposals.get(proposal_id)
        if not node and not _decision_has_denormalized_fields(decision):
            # No graph proposal to ground the page; skip rather than crash.
            continue
        page_type = str(decision.get("page_type") or node.get("page_type") or "")
        if page_type not in _PAGE_TYPE_TO_DIR:
            continue

        slug = str(decision.get("slug") or node.get("slug") or "").strip()
        title = str(decision.get("title") or node.get("title") or slug or "Untitled")
        summary = str(decision.get("summary") or node.get("summary") or "")
        domains = list(decision.get("domains") or node.get("domains") or []) or ["work"]
        evidence_refs = list(decision.get("evidence_refs") or (node.get("evidence_refs") if node else []) or [])
        relates_to = _format_relates_to(
            list(decision.get("relates_to") or []) or relates_to_index.get(proposal_id, []),
            node_proposals,
        )
        extra_frontmatter: dict[str, Any] = {}
        attributes = (node.get("attributes") if node else {}) or {}
        if page_type == "stance":
            extra_frontmatter["position"] = str(
                attributes.get("position") or summary or title
            )
            extra_frontmatter["confidence"] = str(attributes.get("confidence") or "medium")
        if page_type == "inquiry":
            semantic_entity = semantic_entities.get(proposal_id) or {}
            question = (
                attributes.get("question")
                or semantic_entity.get("attributes", {}).get("question")
                or title
            )
            extra_frontmatter["question"] = str(question)

        if action == "create":
            body = _canonical_body(page_type, title, summary, node)
            page: dict[str, Any] = {
                "plan_id": counter.next(),
                "target_kind": "canonical",
                "write_mode": "create",
                "page_type": page_type,
                "slug": slug,
                "title": title,
                "body_markdown": body,
                "intro_mode": "preserve",
                "intro_markdown": None,
                "section_operations": [],
                "domains": domains,
                "relates_to": relates_to,
                "sources": evidence_refs,
                "extra_frontmatter": extra_frontmatter,
                "target_path": None,
                "summary_kind": None,
            }
        else:
            # update or merge — patch existing page with a summary section
            target_path = str(decision.get("target_path") or "")
            page = {
                "plan_id": counter.next(),
                "target_kind": "canonical",
                "write_mode": "update",
                "page_type": page_type,
                "slug": slug,
                "title": title,
                "body_markdown": None,
                "intro_mode": "preserve",
                "intro_markdown": None,
                "section_operations": _update_section_operations(page_type, summary),
                "domains": domains,
                "relates_to": relates_to,
                "sources": evidence_refs,
                "extra_frontmatter": extra_frontmatter,
                "target_path": target_path,
                "summary_kind": None,
            }
        pages.append(page)
    return pages


def _decision_has_denormalized_fields(decision: dict[str, Any]) -> bool:
    required = ("title", "slug", "summary", "page_type")
    return all(str(decision.get(key) or "").strip() for key in required)


def _canonical_body(page_type: str, title: str, summary: str, node: dict[str, Any]) -> str:
    """Assemble a reasonable canonical page body from graph node data."""
    lines = [f"# {title}", ""]
    if summary:
        lines.extend([summary, ""])
    if page_type == "project":
        lines.append("## Project Priorities")
        lines.append("")
        priorities = list((node.get("attributes") or {}).get("priorities") or [])
        if priorities:
            lines.extend(f"- {item}" for item in priorities)
        else:
            lines.append("- No explicit project priorities were captured in onboarding.")
        lines.append("")
        lines.append("## Constraints")
        lines.append("")
        constraints = list((node.get("attributes") or {}).get("constraints") or [])
        if constraints:
            lines.extend(f"- {item}" for item in constraints)
        else:
            lines.append("- No explicit project constraints were captured in onboarding.")
        lines.append("")
        lines.append("## Notes")
        lines.append("")
    elif page_type in {"concept", "playbook", "stance", "inquiry"}:
        lines.append("## TL;DR")
        lines.append("")
        lines.append(summary or title)
        lines.append("")
        if page_type == "playbook":
            lines.append("## Steps")
            lines.append("")
            lines.append("- To be expanded.")
            lines.append("")
        lines.append("## Evidence log")
        lines.append("")
        for ref in list(node.get("evidence_refs") or [])[:5]:
            lines.append(f"- {ref}")
        if page_type == "stance":
            lines.append("")
            lines.append("## Contradictions")
            lines.append("")
            lines.append("- None observed yet.")
    elif page_type == "person":
        lines.append("## Snapshot")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _update_section_operations(page_type: str, summary: str) -> list[dict[str, Any]]:
    if not summary:
        return []
    if page_type == "project":
        return [
            {
                "heading": "## Notes",
                "mode": "append",
                "content": f"\n{summary}\n",
            }
        ]
    if page_type in {"concept", "playbook", "stance", "inquiry"}:
        return [
            {
                "heading": "## Evidence log",
                "mode": "append",
                "content": f"\n- {summary}\n",
            }
        ]
    return []


# ---------------------------------------------------------------------------
# Decision page
# ---------------------------------------------------------------------------


def _build_decision_page(
    counter: "_PlanCounter",
    bundle_id: str,
    merge: dict[str, Any],
    verify: dict[str, Any] | None,
) -> dict[str, Any]:
    lines = [
        "# Onboarding Decisions",
        "",
        f"Bundle: `{bundle_id}`",
        "",
    ]
    decisions = list(merge.get("decisions") or [])
    relationships = list(merge.get("relationship_decisions") or [])
    lines.append(f"Node decisions: {len(decisions)}; kept edges: {sum(1 for r in relationships if r.get('action') == 'keep')} of {len(relationships)}.")
    lines.append("")
    lines.append("## Node Decisions")
    lines.append("")
    for d in decisions:
        action = d.get("action")
        pid = d.get("proposal_id")
        rationale = (d.get("rationale") or "").split("\n")[0][:160]
        lines.append(f"- `{pid}` — **{action}** — {rationale}")
    lines.append("")
    if verify and (verify.get("warnings") or verify.get("notes")):
        lines.append("## Verifier Warnings")
        lines.append("")
        for warn in verify.get("warnings") or []:
            lines.append(f"- {str(warn)[:300]}")
        if verify.get("notes"):
            lines.append("")
            lines.append("## Verifier Notes")
            lines.append("")
            for note in verify.get("notes") or []:
                lines.append(f"- {str(note)[:300]}")
    body = "\n".join(lines).rstrip() + "\n"
    return {
        "plan_id": counter.next(),
        "target_kind": "decision",
        "write_mode": "create",
        "page_type": "decision",
        "slug": f"onboarding-{bundle_id}",
        "title": f"Onboarding Decisions — {bundle_id}",
        "body_markdown": body,
        "intro_mode": "preserve",
        "intro_markdown": None,
        "section_operations": [],
        "domains": ["meta"],
        "relates_to": [],
        "sources": [],
        "extra_frontmatter": {
            "source_type": "onboarding",
            "external_id": bundle_id,
        },
        "target_path": None,
        "summary_kind": None,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _PlanCounter:
    def __init__(self) -> None:
        self._n = 0

    def next(self) -> str:
        self._n += 1
        return f"page-{self._n:03d}"


def _slugify(text: str) -> str:
    return normalize_identifier(text) or "owner"


def _owner_domains(owner: dict[str, Any]) -> list[str]:
    # Safe defaults that match the existing bundle's plan style.
    role = str(owner.get("role") or "").lower()
    if "design" in role or "engineer" in role or "builder" in role or "founder" in role:
        return ["work", "identity"]
    return ["identity"]


def _value_bullets(values: Iterable[dict[str, Any] | str]) -> str:
    items = list(values)
    if not items:
        return "- No explicit values were captured in this bundle."
    lines: list[str] = []
    for idx, item in enumerate(items, start=1):
        if isinstance(item, dict):
            text = str(item.get("text") or "").strip()
        else:
            text = str(item).strip()
        if not text:
            continue
        lines.append(f"{idx}. {text}")
    return "\n".join(lines) if lines else "- No explicit values were captured in this bundle."


def _positioning_body(positioning: dict[str, Any]) -> str:
    summary = str(positioning.get("summary") or "").strip()
    work_priorities = list(positioning.get("work_priorities") or [])
    life_priorities = list(positioning.get("life_priorities") or [])
    constraints = list(positioning.get("constraints") or [])
    parts = [
        "# Positioning",
        "",
        "## Positioning Narrative",
        "",
        summary or "No positioning narrative was captured in this bundle.",
        "",
        "## Work Priorities",
        "",
        _bullet_block(work_priorities, "No explicit work priorities were captured in this bundle."),
        "",
        "## Life Priorities",
        "",
        _bullet_block(life_priorities, "No explicit life priorities were captured in this bundle."),
        "",
        "## Constraints",
        "",
        _bullet_block(constraints, "No explicit constraints were captured in this bundle."),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _bullet_block(items: Iterable[str], empty_line: str) -> str:
    items = [str(i).strip() for i in items if str(i).strip()]
    if not items:
        return f"- {empty_line}"
    return "\n".join(f"- {item}" for item in items)


def _kept_edges(graph: dict[str, Any], merge: dict[str, Any]) -> list[dict[str, Any]]:
    """Return edge_proposals that weren't dropped by merge's relationship_decisions."""
    # Relationship decisions are keyed by (source_ref, target_ref). Any edge
    # whose pair appears with action="drop" is excluded.
    drop_pairs: set[tuple[str, str]] = set()
    for rd in merge.get("relationship_decisions") or []:
        if str(rd.get("action")) == "drop":
            drop_pairs.add((str(rd.get("source_ref") or ""), str(rd.get("target_ref") or "")))
    edges: list[dict[str, Any]] = []
    for edge in graph.get("edge_proposals") or []:
        pair = (str(edge.get("source_ref") or ""), str(edge.get("target_ref") or ""))
        if pair in drop_pairs:
            continue
        edges.append(edge)
    return edges


def _build_relates_index(
    edges: list[dict[str, Any]],
    node_proposals: dict[str, dict[str, Any]],
) -> dict[str, list[str]]:
    """Build {proposal_id: [other_proposal_id, ...]} from kept edges."""
    index: dict[str, list[str]] = {pid: [] for pid in node_proposals}
    for edge in edges:
        src = str(edge.get("source_ref") or "")
        tgt = str(edge.get("target_ref") or "")
        if src in index and tgt and tgt != "owner":
            if tgt not in index[src]:
                index[src].append(tgt)
        if tgt in index and src and src != "owner":
            if src not in index[tgt]:
                index[tgt].append(src)
    return index


def _owner_edges(merge: dict[str, Any], node_proposals: dict[str, dict[str, Any]]) -> list[str]:
    """Return canonical node slugs most directly connected to 'owner' (kept)."""
    kept: list[str] = []
    for rd in merge.get("relationship_decisions") or []:
        if rd.get("action") != "keep":
            continue
        src = str(rd.get("source_ref") or "")
        tgt = str(rd.get("target_ref") or "")
        other = tgt if src == "owner" else (src if tgt == "owner" else "")
        if not other:
            continue
        node = node_proposals.get(other)
        if node and node.get("slug"):
            slug = str(node["slug"])
            if slug not in kept:
                kept.append(slug)
    return kept


def _format_relates_to(
    related_ids: list[str],
    node_proposals: dict[str, dict[str, Any]],
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for pid in related_ids:
        if pid == "owner":
            if "[[profile]]" not in seen:
                seen.add("[[profile]]")
                out.append("[[profile]]")
            continue
        node = node_proposals.get(pid)
        if not node:
            continue
        slug = str(node.get("slug") or "").strip()
        if not slug or slug in seen:
            continue
        seen.add(slug)
        out.append(f"[[{slug}]]")
    return out
