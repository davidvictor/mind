from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any

from mind.dream.common import read_page, section_body, write_page_force
from mind.services.embedding_service import get_embedding_service
from mind.services.graph_registry import GraphNode, GraphRegistry, ResolutionCandidate
from mind.services.llm_service import get_llm_service
from mind.services.llm_routing import resolve_route
from mind.services.vector_index import select_vector_backend
from scripts.common.section_rewriter import replace_or_insert_section
from scripts.common.slugify import slugify
from scripts.common.vault import Vault, raw_path


TITLE_NOISE_RE = re.compile(
    r"\b(case study|portfolio website|website narrative|website|narrative|long form|long-form|longform|short form|short-form|canonical bio|linkedin profile rewrite)\b",
    re.IGNORECASE,
)
CAPS_PHRASE_RE = re.compile(r"\b(?:[A-Z][A-Za-z0-9]+|AI|HR|SQL)(?:[ /&-]+(?:[A-Z][A-Za-z0-9]+|AI|HR|SQL))+\b")
GENERIC_MENTION_PHRASES = {
    "about section",
    "case study",
    "complete",
    "data science",
    "development",
    "engineering",
    "full stack",
    "headline",
    "industry",
    "medium form",
    "next",
    "option",
    "portfolio website version",
    "present",
    "product design",
    "role",
    "scope",
    "slug",
    "stack",
    "status",
    "the challenge",
    "the solution",
    "timeline",
    "version",
    "web design",
    "year",
    "zero one",
    "conversation architecture",
}
GRAPH_RESOLUTION_PICK_PROMPT_VERSION = "graph-resolution.pick-candidate.v1"


@dataclass(frozen=True)
class ResolutionDecision:
    mention_text: str
    resolved_node_id: str | None
    resolved_registry_node_id: str | None
    resolution_kind: str
    confidence: float
    rationale: str
    candidates: list[ResolutionCandidate]
    shadow_vector_candidates: list[ResolutionCandidate]


@dataclass(frozen=True)
class ResolvedGraphDocument:
    doc_id: str
    artifact_id: str
    title: str
    body: str
    source_kind: str
    mentions: list[str]
    primary_decision: ResolutionDecision
    related_decisions: list[ResolutionDecision]
    review_required: bool
    review_payload: dict[str, Any]
    derived_aliases: list[str]


def _utc_now_string() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _read_full_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _extract_title(path: Path, text: str) -> str:
    title = path.stem.replace("-", " ").title()
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return stripped[2:].strip()
    return title


def _strip_title_noise(value: str) -> str:
    normalized = TITLE_NOISE_RE.sub("", value.replace("—", " ").replace("–", " "))
    normalized = re.sub(r"\s+", " ", normalized).strip(" -")
    return normalized.strip()


def _first_paragraph(text: str) -> str:
    paragraphs = [block.strip() for block in text.split("\n\n") if block.strip()]
    for block in paragraphs:
        if block.startswith("#") or block == "---" or block.startswith("```"):
            continue
        if block.startswith("**") and ":" in block:
            continue
        if block.lower().startswith(("slug:", "year:", "industry:", "scope:", "north star:", "ai native:")):
            continue
        return block
    return ""


def _extract_mentions(path: Path, title: str, body: str) -> list[str]:
    mentions: list[str] = []

    def _add(candidate: str) -> None:
        cleaned = candidate.strip()
        if not cleaned or len(cleaned) < 3:
            return
        lowered = cleaned.lower()
        if lowered in {"context", "problem", "approach", "constraints", "overview"}:
            return
        if lowered in GENERIC_MENTION_PHRASES:
            return
        if cleaned not in mentions:
            mentions.append(cleaned)

    _add(title)
    _add(_strip_title_noise(title))
    _add(_strip_title_noise(path.stem.replace("-", " ").title()))
    for label in ("Slug", "Project", "Company", "Client", "Product"):
        match = re.search(rf"\*\*{re.escape(label)}:\*\*\s*(.+)", body)
        if not match:
            continue
        value = match.group(1).strip().splitlines()[0].strip()
        _add(_strip_title_noise(value.replace("·", " ").replace("/", " ").title()))
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            _add(_strip_title_noise(stripped[2:]))
    for match in CAPS_PHRASE_RE.findall(body[:4000]):
        _add(_strip_title_noise(match))
    return mentions[:12]


def _llm_pick_candidate(*, title: str, body: str, mention: str, candidates: list[ResolutionCandidate]) -> tuple[str | None, float, str]:
    prompt = (
        "Choose the best existing canonical node for a document mention.\n\n"
        f"Mention: {mention}\n"
        f"Document title: {title}\n"
        f"Document excerpt: {_first_paragraph(body)[:500]}\n\n"
        "Candidates:\n"
        + "\n".join(
            f"- registry_id={candidate.registry_node_id}; page_id={candidate.page_id}; type={candidate.primary_type}; title={candidate.title}; "
            f"match_kind={candidate.match_kind}; score={candidate.score:.2f}; aliases={', '.join(candidate.aliases[:5])}"
            for candidate in candidates[:5]
        )
        + "\n\nReturn ONLY JSON with keys selected_registry_node_id, confidence, rationale. "
          "selected_registry_node_id must be one of the candidate registry ids or null. confidence must be low, medium, or high."
    )
    try:
        response = get_llm_service().generate_json_prompt(
            prompt,
            task_class="classification",
            prompt_version=GRAPH_RESOLUTION_PICK_PROMPT_VERSION,
        )
    except Exception as exc:
        return None, 0.0, f"llm-unavailable: {type(exc).__name__}: {exc}"
    selected = response.get("selected_registry_node_id")
    confidence = str(response.get("confidence") or "low").strip().lower()
    confidence_score = {"low": 0.45, "medium": 0.72, "high": 0.9}.get(confidence, 0.0)
    rationale = str(response.get("rationale") or "llm selection")
    if selected not in {candidate.registry_node_id for candidate in candidates}:
        return None, 0.0, "llm rejected candidates"
    return str(selected), confidence_score, rationale


def _choose_best_decision(mention: str, candidates: list[ResolutionCandidate], *, title: str, body: str) -> ResolutionDecision:
    if not candidates:
        return ResolutionDecision(
            mention_text=mention,
            resolved_node_id=None,
            resolved_registry_node_id=None,
            resolution_kind="no_match",
            confidence=0.0,
            rationale="no graph candidates",
            candidates=[],
            shadow_vector_candidates=[],
        )
    top = candidates[0]
    if top.match_kind == "exact":
        return ResolutionDecision(
            mention_text=mention,
            resolved_node_id=top.page_id,
            resolved_registry_node_id=top.registry_node_id,
            resolution_kind="auto_exact",
            confidence=1.0,
            rationale=f"deterministic exact match via {top.match_kind}",
            candidates=candidates,
            shadow_vector_candidates=[],
        )
    second_score = candidates[1].score if len(candidates) > 1 else 0.0
    if top.match_kind == "fts_title_alias" and top.score >= 0.8 and (top.score - second_score) >= 0.12:
        return ResolutionDecision(
            mention_text=mention,
            resolved_node_id=top.page_id,
            resolved_registry_node_id=top.registry_node_id,
            resolution_kind="auto_fts",
            confidence=top.score,
            rationale="strong unique FTS title/alias candidate",
            candidates=candidates,
            shadow_vector_candidates=[],
        )
    selected_registry_id, confidence, rationale = _llm_pick_candidate(
        title=title,
        body=body,
        mention=mention,
        candidates=candidates,
    )
    if selected_registry_id and confidence >= 0.7:
        selected = next(candidate for candidate in candidates if candidate.registry_node_id == selected_registry_id)
        return ResolutionDecision(
            mention_text=mention,
            resolved_node_id=selected.page_id,
            resolved_registry_node_id=selected.registry_node_id,
            resolution_kind="llm_selected",
            confidence=confidence,
            rationale=rationale,
            candidates=candidates,
            shadow_vector_candidates=[],
        )
    return ResolutionDecision(
        mention_text=mention,
        resolved_node_id=None,
        resolved_registry_node_id=None,
        resolution_kind="review_ambiguous",
        confidence=top.score,
        rationale="multiple plausible graph candidates",
        candidates=candidates,
        shadow_vector_candidates=[],
    )


def resolve_graph_document(*, path: Path, registry: GraphRegistry) -> ResolvedGraphDocument:
    text = _read_full_text(path)
    title = _extract_title(path, text)
    mentions = _extract_mentions(path, title, text)
    shadow_vectors_by_mention: dict[str, list[ResolutionCandidate]] = {}
    try:
        route = resolve_route("embedding")
        backend = select_vector_backend(Vault.load(registry.repo_root).vector_db)
        for mention in mentions:
            shadow_vectors_by_mention[mention] = registry.resolve_vector_candidates(
                mention,
                embedding_service=get_embedding_service(),
                vector_backend=backend,
                model=route.model,
            )
    except Exception:
        shadow_vectors_by_mention = {}
    decisions = [
        ResolutionDecision(
            mention_text=decision.mention_text,
            resolved_node_id=decision.resolved_node_id,
            resolved_registry_node_id=decision.resolved_registry_node_id,
            resolution_kind=decision.resolution_kind,
            confidence=decision.confidence,
            rationale=decision.rationale,
            candidates=decision.candidates,
            shadow_vector_candidates=shadow_vectors_by_mention.get(mention, []),
        )
        for mention in mentions
        for decision in [
            _choose_best_decision(
                mention,
                registry.resolve_candidates(mention),
                title=title,
                body=text,
            )
        ]
    ]
    if decisions:
        resolved_decisions = [item for item in decisions if item.resolved_node_id]
        if resolved_decisions:
            primary = sorted(
                resolved_decisions,
                key=lambda item: (
                    item.resolution_kind not in {"auto_exact", "auto_fts", "llm_selected"},
                    -item.confidence,
                ),
            )[0]
        else:
            primary = decisions[0]
    else:
        primary = ResolutionDecision("", None, None, "no_match", 0.0, "no mentions", [], [])
    seen_related: set[str] = set()
    related: list[ResolutionDecision] = []
    for decision in decisions:
        if decision is primary or not decision.resolved_node_id or decision.resolved_node_id == primary.resolved_node_id:
            continue
        if decision.resolved_node_id in seen_related:
            continue
        seen_related.add(decision.resolved_node_id)
        related.append(decision)
    review_required = primary.resolved_node_id is None or primary.resolution_kind == "review_ambiguous"
    doc_hash = _hash_text(text)
    derived_aliases = sorted(
        {
            decision.mention_text
            for decision in decisions
            if decision.resolved_node_id == primary.resolved_node_id and decision.mention_text
        }
    )
    return ResolvedGraphDocument(
        doc_id=f"doc-{doc_hash[:12]}",
        artifact_id=f"file-artifact-{doc_hash[:12]}",
        title=title,
        body=text,
        source_kind=path.suffix.lower().lstrip(".") or "file",
        mentions=mentions,
        primary_decision=primary,
        related_decisions=related,
        review_required=review_required,
        review_payload={
            "path": str(path),
            "title": title,
            "mentions": mentions,
            "primary_decision": _decision_payload(primary),
            "related_decisions": [_decision_payload(item) for item in related],
        },
        derived_aliases=derived_aliases,
    )


def _decision_payload(decision: ResolutionDecision) -> dict[str, Any]:
    return {
        "mention_text": decision.mention_text,
        "resolved_node_id": decision.resolved_node_id,
        "resolved_registry_node_id": decision.resolved_registry_node_id,
        "resolution_kind": decision.resolution_kind,
        "confidence": decision.confidence,
        "rationale": decision.rationale,
        "candidates": [
            {
                "candidate_node_id": candidate.page_id,
                "candidate_registry_node_id": candidate.registry_node_id,
                "score": candidate.score,
                "match_kind": candidate.match_kind,
                "debug_payload": {
                    "title": candidate.title,
                    "primary_type": candidate.primary_type,
                    "path": candidate.path,
                    "aliases": candidate.aliases,
                },
            }
            for candidate in decision.candidates
        ],
        "shadow_vector_candidates": [
            {
                "candidate_node_id": candidate.page_id,
                "candidate_registry_node_id": candidate.registry_node_id,
                "score": candidate.score,
                "match_kind": candidate.match_kind,
                "debug_payload": {
                    "title": candidate.title,
                    "primary_type": candidate.primary_type,
                    "path": candidate.path,
                    "aliases": candidate.aliases,
                },
            }
            for candidate in decision.shadow_vector_candidates
        ],
    }


def _extract_stack_capabilities(text: str) -> list[str]:
    match = re.search(r"\*\*Stack:\*\*\s*(.+)", text)
    if not match:
        return []
    return [item.strip() for item in match.group(1).split(",") if item.strip()]


def _extract_constraints(text: str) -> list[str]:
    block = section_body(text, "Constraints")
    if not block:
        return []
    constraints: list[str] = []
    for line in block.splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            constraints.append(stripped[2:].strip())
        elif stripped and not stripped.startswith("#"):
            constraints.append(stripped)
    return constraints


def _extract_roles(text: str) -> list[str]:
    roles: list[str] = []
    match = re.search(r"\*\*Role:\*\*\s*(.+)", text)
    if match:
        roles.extend([item.strip() for item in re.split(r"[—–;/]|,\s*", match.group(1)) if item.strip()])
    return roles


def _extract_field_values(text: str, label: str, *, split_pattern: str = r"[,·]") -> list[str]:
    match = re.search(rf"\*\*{re.escape(label)}:\*\*\s*(.+)", text)
    if not match:
        return []
    return [item.strip() for item in re.split(split_pattern, match.group(1)) if item.strip()]


def _extract_timeline(text: str) -> list[str]:
    return _extract_field_values(text, "Timeline", split_pattern=r"\n")


def _extract_statuses(text: str) -> list[str]:
    return _extract_field_values(text, "Status")


def _extract_scope(text: str) -> list[str]:
    return _extract_field_values(text, "Scope")


def _extract_key_paragraphs(text: str, *, limit: int = 4) -> list[str]:
    sections = ("Introduction", "Context", "Challenge", "Problem", "Solution", "Outcome", "Final Thoughts")
    paragraphs: list[str] = []
    for heading in sections:
        block = section_body(text, heading)
        if not block:
            continue
        for paragraph in [item.strip() for item in block.split("\n\n") if item.strip()]:
            if paragraph.startswith("#") or paragraph.startswith("- ") or paragraph[0:2].isdigit():
                continue
            if paragraph not in paragraphs:
                paragraphs.append(paragraph)
            if len(paragraphs) >= limit:
                return paragraphs
    first = _first_paragraph(text)
    if first and first not in paragraphs:
        paragraphs.append(first)
    return paragraphs[:limit]


def _merge_bullets(path: Path, *, heading: str, bullets: list[str], insert_after: str | None = None) -> None:
    if not bullets:
        return
    frontmatter, body = read_page(path)
    existing = [line[2:].strip() for line in section_body(body, heading).splitlines() if line.strip().startswith("- ")]
    merged: list[str] = []
    for bullet in [*existing, *bullets]:
        if bullet and bullet not in merged:
            merged.append(bullet)
    replace_or_insert_section(
        file_path=path,
        section_heading=f"## {heading}",
        new_content="\n".join(f"- {bullet}" for bullet in merged),
        insert_after=f"## {insert_after}" if insert_after else None,
    )


def _merge_recent_evidence(path: Path, *, bullet: str, insert_after: str | None = None) -> None:
    frontmatter, body = read_page(path)
    existing = [line[2:].strip() for line in section_body(body, "Recent Evidence").splitlines() if line.strip().startswith("- ")]
    by_key: dict[str, str] = {}
    for item in existing:
        by_key[_recent_evidence_key(item)] = item
    by_key[_recent_evidence_key(bullet)] = bullet
    merged = list(by_key.values())
    replace_or_insert_section(
        file_path=path,
        section_heading="## Recent Evidence",
        new_content="\n".join(f"- {item}" for item in merged),
        insert_after=f"## {insert_after}" if insert_after else None,
    )


def _recent_evidence_key(bullet: str) -> str:
    parts = [part.strip() for part in bullet.split("—")]
    if len(parts) >= 2:
        return parts[1]
    return bullet


def _merge_paragraphs(path: Path, *, heading: str, paragraphs: list[str], insert_after: str | None = None) -> None:
    if not paragraphs:
        return
    frontmatter, body = read_page(path)
    existing = [block.strip() for block in section_body(body, heading).split("\n\n") if block.strip()]
    merged: list[str] = []
    for paragraph in [*existing, *paragraphs]:
        if paragraph and paragraph not in merged:
            merged.append(paragraph)
    replace_or_insert_section(
        file_path=path,
        section_heading=f"## {heading}",
        new_content="\n\n".join(merged),
        insert_after=f"## {insert_after}" if insert_after else None,
    )


def patch_canonical_node(
    *,
    repo_root: Path,
    registry: GraphRegistry,
    resolved: ResolvedGraphDocument,
    source_ref: str,
) -> GraphNode | None:
    node_id = resolved.primary_decision.resolved_registry_node_id
    if not node_id:
        return None
    node = registry.get_node(node_id)
    if node is None:
        return None
    path = registry.resolve_path(node.path)
    frontmatter, body = read_page(path)

    aliases = list(frontmatter.get("aliases") or [])
    for alias in resolved.derived_aliases:
        if alias and alias not in aliases and alias != frontmatter.get("title"):
            aliases.append(alias)
    frontmatter["aliases"] = aliases
    frontmatter.setdefault("entity_kind", "project-company" if node.page_id == "the-pick-ai" else node.primary_type)

    source_link = source_ref
    sources = list(frontmatter.get("sources") or [])
    if source_link not in sources:
        sources.append(source_link)
    frontmatter["sources"] = sources

    relates_to = list(frontmatter.get("relates_to") or [])
    for decision in resolved.related_decisions:
        if not decision.resolved_node_id:
            continue
        link = f"[[{decision.resolved_node_id}]]"
        if link not in relates_to and decision.resolved_node_id != resolved.primary_decision.resolved_node_id:
            relates_to.append(link)
    frontmatter["relates_to"] = relates_to
    frontmatter["last_updated"] = date.today().isoformat()
    write_page_force(path, frontmatter, body)

    constraints = _extract_constraints(resolved.body)
    capabilities = _extract_stack_capabilities(resolved.body)
    roles = _extract_roles(resolved.body)
    timeline = _extract_timeline(resolved.body)
    statuses = _extract_statuses(resolved.body)
    scope = _extract_scope(resolved.body)
    note_paragraphs = _extract_key_paragraphs(resolved.body)
    recent_evidence = f"{date.today().isoformat()} — {source_link} — {_first_paragraph(resolved.body)[:220]}"
    related_links = [f"[[{decision.resolved_node_id}]]" for decision in resolved.related_decisions if decision.resolved_node_id]

    if node.primary_type == "project":
        _merge_bullets(path, heading="Roles", bullets=roles, insert_after="Project Priorities")
        _merge_bullets(path, heading="Timeline", bullets=timeline, insert_after="Roles")
        _merge_bullets(path, heading="Status", bullets=statuses, insert_after="Timeline")
        _merge_bullets(path, heading="Capabilities", bullets=capabilities, insert_after="Status")
        _merge_bullets(path, heading="Constraints", bullets=constraints, insert_after="Project Priorities")
        _merge_bullets(path, heading="Scope", bullets=scope, insert_after="Constraints")
        _merge_paragraphs(path, heading="Notes", paragraphs=note_paragraphs, insert_after="Scope")
        _merge_recent_evidence(path, bullet=recent_evidence, insert_after="Notes")
        _merge_bullets(path, heading="Links To Existing Nodes", bullets=related_links, insert_after="Recent Evidence")
    elif node.primary_type == "company":
        _merge_recent_evidence(path, bullet=recent_evidence, insert_after="Constraints")
        _merge_bullets(path, heading="Capabilities", bullets=capabilities, insert_after="Overview")
        _merge_bullets(path, heading="Constraints", bullets=constraints, insert_after="Overview")
        _merge_bullets(path, heading="Links To Existing Nodes", bullets=related_links, insert_after="Recent Evidence")
    elif node.primary_type == "person":
        _merge_recent_evidence(path, bullet=recent_evidence, insert_after="Overview")
        _merge_bullets(path, heading="Roles", bullets=roles, insert_after="Overview")
        _merge_bullets(path, heading="Relationships", bullets=related_links, insert_after="Recent Evidence")
    else:
        _merge_recent_evidence(path, bullet=recent_evidence, insert_after="Overview")
        _merge_bullets(path, heading="Links To Existing Nodes", bullets=related_links, insert_after="Recent Evidence")
    return node


def write_ingest_review_artifact(*, repo_root: Path, resolved: ResolvedGraphDocument) -> tuple[Path, Path]:
    root = raw_path(repo_root, "reports", "ingest-review")
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / f"{resolved.doc_id}.json"
    md_path = root / f"{resolved.doc_id}.md"
    payload = {
        "generated_at": _utc_now_string(),
        **resolved.review_payload,
    }
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    lines = [
        "# Ingest Review",
        "",
        f"- Path: `{resolved.review_payload['path']}`",
        f"- Title: {resolved.title}",
        f"- Primary decision: {resolved.primary_decision.resolution_kind}",
        f"- Rationale: {resolved.primary_decision.rationale}",
        "",
        "## Mentions",
        "",
    ]
    for decision in [resolved.primary_decision, *resolved.related_decisions]:
        lines.append(
            f"- `{decision.mention_text}` -> `{decision.resolution_kind}`"
            + (f" (`{decision.resolved_node_id}`)" if decision.resolved_node_id else "")
            + f" — {decision.rationale}"
        )
        if decision.shadow_vector_candidates:
            lines.append(
                "  shadow-vector: "
                + ", ".join(
                    f"{candidate.page_id} ({candidate.score:.2f})"
                    for candidate in decision.shadow_vector_candidates[:3]
                )
            )
    md_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return json_path, md_path
