from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
import re
import shutil
from typing import Any

from scripts.common.section_rewriter import ParsedMarkdownBody, ParsedSection, parse_markdown_body, render_markdown_body

from .common import (
    DreamExecutionContext,
    DreamResult,
    campaign_setting,
    dream_month,
    dream_today,
    dream_run,
    ensure_dream_enabled,
    ensure_onboarded,
    extract_wikilinks,
    maybe_locked,
    read_page,
    runtime_state,
    section_body,
    vault,
    write_note_page,
    write_page_force,
)
from .quality import QUALITY_ADAPTER, evaluate_and_persist_quality, lane_state_for_summary_id, supports_full_dream_mutation
from .substrate_queries import active_atoms, atom_path, touched_active_atoms

_EVIDENCE_ENTRY_RE = re.compile(r"^- (\d{4}-\d{2}-\d{2}) — \[\[([^\]]+)\]\]", re.MULTILINE)
_GENERIC_MERGE_TOKENS = {
    "what",
    "is",
    "the",
    "a",
    "an",
    "for",
    "of",
    "and",
    "to",
    "in",
    "on",
    "with",
    "current",
    "how",
    "does",
    "do",
    "should",
    "can",
    "be",
    "or",
}
REM_ADAPTER = "dream.rem"


def _atom_ref(atom_id: str, *, inactive_ids: set[str]) -> str:
    return f"`{atom_id}`" if atom_id in inactive_ids else f"[[{atom_id}]]"


@dataclass(frozen=True)
class RemAtomScore:
    atom_id: str
    atom_type: str
    path: Path
    title: str
    frontmatter: dict[str, Any]
    body: str
    relation_ids: tuple[str, ...]
    distinct_sources: int
    trusted_sources: int
    degraded_sources: int
    blocked_sources: int
    new_evidence_count: int
    contradiction_count: int
    life_mentions: int
    hot_score: int
    prune_score: int


@dataclass(frozen=True)
class RemCluster:
    member_ids: tuple[str, ...]
    score: int
    relation_edges: int
    life_mentions: int


def _relation_ids(frontmatter: dict[str, Any]) -> tuple[str, ...]:
    related: list[str] = []
    for item in frontmatter.get("relates_to") or []:
        related.extend(extract_wikilinks(str(item)))
    relations = frontmatter.get("typed_relations")
    if isinstance(relations, dict):
        for values in relations.values():
            for item in values or []:
                related.extend(extract_wikilinks(str(item)))
    deduped: list[str] = []
    seen: set[str] = set()
    for item in related:
        cleaned = str(item).strip()
        if not cleaned or cleaned in seen:
            continue
        deduped.append(cleaned)
        seen.add(cleaned)
    return tuple(deduped)


def _evidence_entries(body: str) -> list[tuple[str, str]]:
    entries: list[tuple[str, str]] = []
    evidence = section_body(body, "Evidence log")
    if not evidence:
        return entries
    for line in evidence.splitlines():
        match = _EVIDENCE_ENTRY_RE.match(line.strip())
        if not match:
            continue
        entries.append((match.group(1), match.group(2)))
    return entries


def _new_evidence_count(body: str, *, last_seen: str | None) -> int:
    entries = _evidence_entries(body)
    if last_seen is None:
        return len(entries)
    cutoff = last_seen[:10]
    return sum(1 for entry_date, _source_id in entries if entry_date >= cutoff)


def _source_counts(body: str, *, quality: dict[str, Any] | None) -> tuple[int, int, int, int]:
    sources = {source_id for _entry_date, source_id in _evidence_entries(body)}
    trusted = 0
    degraded = 0
    blocked = 0
    for source_id in sources:
        lane_state = lane_state_for_summary_id(source_id, quality)
        if supports_full_dream_mutation(lane_state):
            trusted += 1
        elif lane_state == "blocked":
            blocked += 1
        else:
            degraded += 1
    return len(sources), trusted, degraded, blocked


def _contradiction_count(body: str) -> int:
    contradictions = section_body(body, "Contradictions")
    if not contradictions:
        return 0
    return sum(1 for line in contradictions.splitlines() if line.strip().startswith("- "))


def _life_signal_counter(v) -> Counter[str]:
    counter: Counter[str] = Counter()
    me_paths = [
        v.wiki / "me" / "profile.md",
        v.wiki / "me" / "positioning.md",
        v.wiki / "me" / "values.md",
        v.wiki / "me" / "open-inquiries.md",
    ]
    for path in me_paths:
        if not path.exists():
            continue
        counter.update(extract_wikilinks(path.read_text(encoding="utf-8")))

    digest_dir = v.wiki / "me" / "digests"
    if digest_dir.exists():
        for path in sorted(digest_dir.glob("*.md"), reverse=True)[:4]:
            counter.update(extract_wikilinks(path.read_text(encoding="utf-8")))
    return counter


def _life_context_edges(v) -> set[frozenset[str]]:
    edges: set[frozenset[str]] = set()
    doc_paths = [
        v.wiki / "me" / "profile.md",
        v.wiki / "me" / "positioning.md",
        v.wiki / "me" / "values.md",
        v.wiki / "me" / "open-inquiries.md",
    ]
    digest_dir = v.wiki / "me" / "digests"
    if digest_dir.exists():
        doc_paths.extend(sorted(digest_dir.glob("*.md"), reverse=True)[:4])
    for path in doc_paths:
        if not path.exists():
            continue
        links = sorted({item for item in extract_wikilinks(path.read_text(encoding="utf-8")) if item})
        for index, left_id in enumerate(links):
            for right_id in links[index + 1 :]:
                if left_id != right_id:
                    edges.add(frozenset({left_id, right_id}))
    return edges


def _promotion_threshold(atom_type: str, config) -> int:
    if atom_type == "concept":
        return int(config.atom_promotion.concept.min_distinct_sources)
    if atom_type == "playbook":
        return int(config.atom_promotion.playbook.min_distinct_sources)
    if atom_type == "stance":
        return int(config.atom_promotion.stance.min_distinct_sources)
    return int(config.atom_promotion.inquiry.min_distinct_sources)


def _hot_score(
    *,
    atom_type: str,
    relation_degree: int,
    distinct_sources: int,
    trusted_sources: int,
    degraded_sources: int,
    blocked_sources: int,
    new_evidence_count: int,
    contradiction_count: int,
    life_mentions: int,
    lifecycle_state: str,
) -> int:
    score = 0
    score += new_evidence_count * 4
    score += min(distinct_sources, 6) * 2
    score += relation_degree * 3
    score += trusted_sources * 2
    score += life_mentions * 5
    score -= degraded_sources
    score -= blocked_sources * 2
    score -= contradiction_count * 2
    if atom_type == "inquiry":
        score += 2
    if atom_type == "stance":
        score += 1
    if lifecycle_state == "declining":
        score -= 3
    return score


def _prune_score(
    *,
    evidence_count: int,
    relation_degree: int,
    distinct_sources: int,
    trusted_sources: int,
    new_evidence_count: int,
    life_mentions: int,
    contradiction_count: int,
    hot_score: int,
) -> int:
    score = hot_score
    score += min(evidence_count, 6)
    score += relation_degree
    score += trusted_sources
    if new_evidence_count == 0:
        score -= 2
    if life_mentions == 0:
        score -= 2
    if contradiction_count > 0:
        score -= contradiction_count
    if distinct_sources <= 1:
        score -= 2
    return score


def _score_atoms(
    v,
    *,
    quality: dict[str, Any],
    last_seen: str | None,
    life_signals: Counter[str],
    candidate_ids: set[str],
) -> dict[str, RemAtomScore]:
    scores: dict[str, RemAtomScore] = {}
    for atom in active_atoms(v):
        if atom.id not in candidate_ids:
            continue
        path = atom_path(v, atom)
        frontmatter, body = read_page(path)
        relation_ids = _relation_ids(frontmatter)
        distinct_sources, trusted_sources, degraded_sources, blocked_sources = _source_counts(body, quality=quality)
        new_evidence_count = _new_evidence_count(body, last_seen=last_seen)
        contradiction_count = _contradiction_count(body)
        life_mentions = int(life_signals.get(atom.id, 0))
        hot_score = _hot_score(
            atom_type=atom.type,
            relation_degree=len(relation_ids),
            distinct_sources=distinct_sources,
            trusted_sources=trusted_sources,
            degraded_sources=degraded_sources,
            blocked_sources=blocked_sources,
            new_evidence_count=new_evidence_count,
            contradiction_count=contradiction_count,
            life_mentions=life_mentions,
            lifecycle_state=str(frontmatter.get("lifecycle_state") or "active"),
        )
        prune_score = _prune_score(
            evidence_count=int(frontmatter.get("evidence_count") or atom.evidence_count or 0),
            relation_degree=len(relation_ids),
            distinct_sources=distinct_sources,
            trusted_sources=trusted_sources,
            new_evidence_count=new_evidence_count,
            life_mentions=life_mentions,
            contradiction_count=contradiction_count,
            hot_score=hot_score,
        )
        scores[atom.id] = RemAtomScore(
            atom_id=atom.id,
            atom_type=atom.type,
            path=path,
            title=str(frontmatter.get("title") or atom.id),
            frontmatter=frontmatter,
            body=body,
            relation_ids=relation_ids,
            distinct_sources=distinct_sources,
            trusted_sources=trusted_sources,
            degraded_sources=degraded_sources,
            blocked_sources=blocked_sources,
            new_evidence_count=new_evidence_count,
            contradiction_count=contradiction_count,
            life_mentions=life_mentions,
            hot_score=hot_score,
            prune_score=prune_score,
        )
    return scores


def _relation_rich_ids(atoms: list, config) -> set[str]:
    ids: set[str] = set()
    for atom in atoms:
        threshold = _promotion_threshold(atom.type, config)
        evidence_count = int(atom.evidence_count or 0)
        if evidence_count >= threshold:
            ids.add(atom.id)
    return ids


def _candidate_ids(
    v,
    *,
    last_seen: str | None,
    life_signals: Counter[str],
    rem_hotset_cap: int,
    rem_cluster_limit: int,
    candidate_multiplier: int = 3,
) -> set[str]:
    atoms = active_atoms(v)
    touched_ids = {atom.id for atom in touched_active_atoms(v, last_seen=last_seen)}
    inquiry_ids = {atom.id for atom in atoms if atom.type == "inquiry"}
    stance_ids = {atom.id for atom in atoms if atom.type == "stance"}
    relation_rich = _relation_rich_ids(atoms, v.config)
    declining_ids = {atom.id for atom in atoms if atom.lifecycle_state == "declining"}
    life_ids = {atom_id for atom_id in life_signals}
    seeded_ids = touched_ids | inquiry_ids | stance_ids | relation_rich | declining_ids | life_ids
    candidate_cap = max(int(rem_hotset_cap) * int(candidate_multiplier), int(rem_cluster_limit) * int(candidate_multiplier) * 4)
    ordered = sorted(
        (atom for atom in atoms if atom.id in seeded_ids),
        key=lambda atom: (
            (8 if atom.id in life_ids else 0)
            + (6 if atom.id in touched_ids else 0)
            + (5 if atom.lifecycle_state == "declining" else 0)
            + (4 if atom.type == "inquiry" else 0)
            + (3 if atom.type == "stance" else 0)
            + (2 if atom.id in relation_rich else 0)
            + min(int(atom.evidence_count or 0), 10),
            atom.last_evidence_date or "",
            atom.id,
        ),
        reverse=True,
    )
    return {atom.id for atom in ordered[:candidate_cap]}


def _hotset(scores: dict[str, RemAtomScore], *, hotset_cap: int) -> list[RemAtomScore]:
    ordered = sorted(
        scores.values(),
        key=lambda item: (item.hot_score, item.title.lower(), item.atom_id),
        reverse=True,
    )
    return ordered[: int(hotset_cap)]


def _cooccurrence_edges(v) -> set[frozenset[str]]:
    edges: set[frozenset[str]] = set()
    nudge_dir = v.wiki / "inbox" / "nudges"
    if not nudge_dir.exists():
        return edges
    for path in sorted(nudge_dir.rglob("*-cooccurrence-*.md")):
        try:
            frontmatter, _body = read_page(path)
        except Exception:
            continue
        left_id = str(frontmatter.get("left_atom") or "").strip()
        right_id = str(frontmatter.get("right_atom") or "").strip()
        if left_id and right_id and left_id != right_id:
            edges.add(frozenset({left_id, right_id}))
    return edges


def _cluster_hotset(
    hotset: list[RemAtomScore],
    *,
    v,
    cluster_limit: int,
    extra_edges: set[frozenset[str]] | None = None,
) -> list[RemCluster]:
    hotset_ids = {item.atom_id for item in hotset}
    neighbors: dict[str, set[str]] = {item.atom_id: set() for item in hotset}
    cooccurrence_edges = _cooccurrence_edges(v)
    for item in hotset:
        for relation_id in item.relation_ids:
            if relation_id in hotset_ids and relation_id != item.atom_id:
                neighbors[item.atom_id].add(relation_id)
                neighbors[relation_id].add(item.atom_id)
    for edge in cooccurrence_edges:
        left_id, right_id = tuple(edge)
        if left_id in hotset_ids and right_id in hotset_ids:
            neighbors[left_id].add(right_id)
            neighbors[right_id].add(left_id)
    for edge in extra_edges or set():
        left_id, right_id = tuple(edge)
        if left_id in hotset_ids and right_id in hotset_ids:
            neighbors[left_id].add(right_id)
            neighbors[right_id].add(left_id)

    lookup = {item.atom_id: item for item in hotset}
    clusters: list[RemCluster] = []
    seen: set[str] = set()
    for item in hotset:
        if item.atom_id in seen:
            continue
        queue = [item.atom_id]
        members: list[str] = []
        while queue:
            current = queue.pop()
            if current in seen:
                continue
            seen.add(current)
            members.append(current)
            queue.extend(sorted(neighbors[current] - seen))
        member_scores = [lookup[member_id] for member_id in members]
        relation_edges = sum(
            1
            for member_id in members
            for target in neighbors[member_id]
            if member_id < target
        )
        clusters.append(
            RemCluster(
                member_ids=tuple(sorted(members)),
                score=sum(score.hot_score for score in member_scores) + relation_edges * 2,
                relation_edges=relation_edges,
                life_mentions=sum(score.life_mentions for score in member_scores),
            )
        )
    clusters.sort(key=lambda cluster: (cluster.score, cluster.life_mentions, cluster.member_ids), reverse=True)
    return clusters[: int(cluster_limit)]


def _cluster_heading(cluster: RemCluster, scores: dict[str, RemAtomScore]) -> str:
    members = sorted(
        (scores[member_id] for member_id in cluster.member_ids),
        key=lambda item: (item.hot_score, item.title.lower(), item.atom_id),
        reverse=True,
    )
    return " / ".join(item.title for item in members[:3])


def _merge_fingerprint(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def _merge_tokens(text: str) -> set[str]:
    return {
        token
        for token in _merge_fingerprint(text).split()
        if token and token not in _GENERIC_MERGE_TOKENS
    }


def _is_obvious_duplicate(left: RemAtomScore, right: RemAtomScore) -> bool:
    left_fp = _merge_fingerprint(left.title)
    right_fp = _merge_fingerprint(right.title)
    if left_fp == right_fp or left_fp in right_fp or right_fp in left_fp:
        return True
    left_tokens = _merge_tokens(left.title)
    right_tokens = _merge_tokens(right.title)
    if not left_tokens or not right_tokens:
        return False
    intersection = len(left_tokens & right_tokens)
    union = len(left_tokens | right_tokens)
    if union == 0:
        return False
    token_overlap = intersection / union
    neighbor_overlap = len(set(left.relation_ids) & set(right.relation_ids))
    return token_overlap >= 0.6 and neighbor_overlap >= 1


def _merge_candidates(clusters: list[RemCluster], scores: dict[str, RemAtomScore]) -> list[tuple[RemAtomScore, RemAtomScore]]:
    candidates: list[tuple[RemAtomScore, RemAtomScore]] = []
    seen: set[tuple[str, str]] = set()
    for cluster in clusters:
        members = [scores[member_id] for member_id in cluster.member_ids]
        for index, left in enumerate(members):
            for right in members[index + 1 :]:
                if left.atom_type != right.atom_type:
                    continue
                if not _is_obvious_duplicate(left, right):
                    continue
                winner, loser = (
                    (left, right)
                    if (left.hot_score, left.distinct_sources, left.atom_id) >= (right.hot_score, right.distinct_sources, right.atom_id)
                    else (right, left)
                )
                key = (winner.atom_id, loser.atom_id)
                if key in seen:
                    continue
                seen.add(key)
                candidates.append((winner, loser))
    return candidates


def _is_weak(score: RemAtomScore) -> bool:
    if score.life_mentions > 0 or score.new_evidence_count > 0:
        return False
    if score.trusted_sources > 0 and score.distinct_sources >= 2:
        return False
    if len(score.relation_ids) > 1 and score.distinct_sources >= 3:
        return False
    return score.prune_score < 8


def _prune_candidates(scores: dict[str, RemAtomScore]) -> list[tuple[str, RemAtomScore]]:
    candidates: list[tuple[str, RemAtomScore]] = []
    for score in scores.values():
        lifecycle_state = str(score.frontmatter.get("lifecycle_state") or "active")
        if not _is_weak(score):
            continue
        action = "archive" if lifecycle_state == "declining" else "decline"
        candidates.append((action, score))
    candidates.sort(key=lambda item: (item[1].prune_score, item[1].title.lower(), item[1].atom_id))
    return candidates


def _campaign_prune_action(
    score: RemAtomScore,
    *,
    decline_after_weak_months: int,
    archive_after_weak_months: int,
) -> str | None:
    if not _is_weak(score):
        return None
    projected_weak_months = int(score.frontmatter.get("rem_weak_months") or 0) + 1
    if projected_weak_months >= archive_after_weak_months:
        return "archive"
    if projected_weak_months >= decline_after_weak_months:
        return "decline"
    return "watch"


def _campaign_prune_candidates(
    scores: dict[str, RemAtomScore],
    *,
    decline_after_weak_months: int,
    archive_after_weak_months: int,
) -> list[tuple[str, RemAtomScore]]:
    candidates: list[tuple[str, RemAtomScore]] = []
    for score in scores.values():
        action = _campaign_prune_action(
            score,
            decline_after_weak_months=decline_after_weak_months,
            archive_after_weak_months=archive_after_weak_months,
        )
        if action is not None:
            candidates.append((action, score))
    candidates.sort(key=lambda item: (item[1].prune_score, item[1].title.lower(), item[1].atom_id))
    return candidates


def _life_pressure_lines(hotset: list[RemAtomScore], *, inactive_ids: set[str]) -> list[str]:
    focused = [item for item in hotset if item.life_mentions > 0]
    focused.sort(key=lambda item: (item.life_mentions, item.hot_score, item.atom_id), reverse=True)
    return [
        f"- {_atom_ref(item.atom_id, inactive_ids=inactive_ids)} appears {item.life_mentions} time(s) across `memory/me/*` and recent digests."
        for item in focused[:8]
    ] or ["- No strong current life-context pressure was detected."]


def _core_cluster_lines(
    clusters: list[RemCluster],
    scores: dict[str, RemAtomScore],
    *,
    inactive_ids: set[str],
) -> list[str]:
    if not clusters:
        return ["- No cluster cleared the monthly REM threshold."]
    lines: list[str] = []
    for index, cluster in enumerate(clusters, start=1):
        members = sorted(
            (scores[member_id] for member_id in cluster.member_ids),
            key=lambda item: (item.hot_score, item.title.lower(), item.atom_id),
            reverse=True,
        )
        member_links = ", ".join(_atom_ref(item.atom_id, inactive_ids=inactive_ids) for item in members[:5])
        lines.append(
            f"### Cluster {index}: {_cluster_heading(cluster, scores)}\n\n"
            f"- Members: {member_links}\n"
            f"- Score: {cluster.score}\n"
            f"- Relation edges: {cluster.relation_edges}\n"
            f"- Life-context mentions: {cluster.life_mentions}"
        )
    return lines


def _strength_lines(hotset: list[RemAtomScore], *, inactive_ids: set[str]) -> list[str]:
    strongest = sorted(hotset, key=lambda item: (item.hot_score, item.new_evidence_count, item.atom_id), reverse=True)[:8]
    return [
        f"- {_atom_ref(item.atom_id, inactive_ids=inactive_ids)} strengthened via {item.new_evidence_count} new evidence entries, "
        f"{item.trusted_sources} trusted sources, and {len(item.relation_ids)} durable relations."
        for item in strongest
    ] or ["- No atoms strengthened meaningfully this month."]


def _weak_lines(candidates: list[tuple[str, RemAtomScore]]) -> list[str]:
    if not candidates:
        return ["- No atoms fell below the monthly weakness threshold."]
    return [
        f"- `{score.atom_id}` is weakening: no current-life pressure, "
        f"{score.new_evidence_count} new evidence entries, {len(score.relation_ids)} relations, "
        f"candidate action `{action}`."
        for action, score in candidates[:8]
    ]


def _insight_lines(
    clusters: list[RemCluster],
    scores: dict[str, RemAtomScore],
    *,
    inactive_ids: set[str],
) -> list[str]:
    if not clusters:
        return ["- REM did not find a high-confidence cross-brain insight cluster this month."]
    insights: list[str] = []
    for cluster in clusters[:5]:
        members = sorted(
            (scores[member_id] for member_id in cluster.member_ids),
            key=lambda item: (item.hot_score, item.atom_id),
            reverse=True,
        )
        dominant = members[:3]
        insights.append(
            f"- {_cluster_heading(cluster, scores)} is cohering around "
            f"{', '.join(_atom_ref(item.atom_id, inactive_ids=inactive_ids) for item in dominant)} "
            f"with {sum(item.new_evidence_count for item in members)} recent evidence entries."
        )
    return insights


def _tension_lines(hotset: list[RemAtomScore], *, inactive_ids: set[str]) -> list[str]:
    tensions = [
        item
        for item in hotset
        if item.contradiction_count > 0 or item.blocked_sources > 0 or item.degraded_sources > item.trusted_sources
    ]
    tensions.sort(
        key=lambda item: (item.contradiction_count + item.blocked_sources + item.degraded_sources, item.atom_id),
        reverse=True,
    )
    return [
        f"- {_atom_ref(item.atom_id, inactive_ids=inactive_ids)} remains tense: contradictions={item.contradiction_count}, "
        f"trusted={item.trusted_sources}, degraded={item.degraded_sources}, blocked={item.blocked_sources}."
        for item in tensions[:8]
    ] or ["- No high-signal tensions surfaced this month."]


def _pruning_lines(candidates: list[tuple[str, RemAtomScore]], *, blocked: bool) -> list[str]:
    if not candidates:
        return ["- No pruning action is recommended this month."]
    prefix = "[report-only] " if blocked else ""
    return [
        f"- {prefix}`{action}` for [[{score.atom_id}]] (score={score.prune_score}, relations={len(score.relation_ids)}, "
        f"trusted_sources={score.trusted_sources}, life_mentions={score.life_mentions})."
        for action, score in candidates[:12]
    ]


def _merge_lines(candidates: list[tuple[RemAtomScore, RemAtomScore]], *, blocked: bool) -> list[str]:
    if not candidates:
        return ["- No merge action is recommended this month."]
    prefix = "[report-only] " if blocked else ""
    return [
        f"- {prefix}merge `{loser.atom_id}` into [[{winner.atom_id}]] "
        f"(winner score={winner.hot_score}, loser score={loser.hot_score})."
        for winner, loser in candidates[:8]
    ]


def _render_rem_body(
    *,
    month: str,
    clusters: list[RemCluster],
    hotset: list[RemAtomScore],
    prune_candidates: list[tuple[str, RemAtomScore]],
    merge_candidates: list[tuple[RemAtomScore, RemAtomScore]],
    blocked: bool,
    scores: dict[str, RemAtomScore],
    inactive_ids: set[str],
) -> str:
    sections = [
        ("## Core clusters", _core_cluster_lines(clusters, scores, inactive_ids=inactive_ids)),
        ("## What strengthened", _strength_lines(hotset, inactive_ids=inactive_ids)),
        ("## What weakened", _weak_lines(prune_candidates)),
        ("## Core insights", _insight_lines(clusters, scores, inactive_ids=inactive_ids)),
        ("## Open tensions", _tension_lines(hotset, inactive_ids=inactive_ids)),
        ("## Pruning decisions", _pruning_lines(prune_candidates, blocked=blocked)),
        ("## Merge decisions", _merge_lines(merge_candidates, blocked=blocked)),
        ("## Life-context pressure", _life_pressure_lines(hotset, inactive_ids=inactive_ids)),
    ]
    lines = [f"# REM {month}", ""]
    for heading, items in sections:
        lines.append(heading)
        lines.append("")
        lines.extend(items)
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _rewrite_links(v, *, winner_id: str, loser_id: str) -> int:
    rewrites = 0
    loser_token = f"[[{loser_id}]]"
    winner_token = f"[[{winner_id}]]"
    for path in sorted(v.wiki.rglob("*.md")):
        rel = path.relative_to(v.wiki)
        if rel.parts and rel.parts[0] == ".archive":
            continue
        if path.name == f"{loser_id}.md":
            continue
        text = path.read_text(encoding="utf-8")
        if loser_token not in text:
            continue
        path.write_text(text.replace(loser_token, winner_token), encoding="utf-8")
        rewrites += 1
    return rewrites


def split_frontmatter_body(text: str) -> tuple[str, str]:
    if not text.startswith("---\n"):
        return "", text
    lines = text.splitlines(keepends=True)
    for index in range(1, len(lines)):
        if lines[index].rstrip("\r\n") == "---":
            return "".join(lines[: index + 1]), "".join(lines[index + 1 :])
    return "", text


def _merge_active_atoms(v, *, winner: RemAtomScore, loser: RemAtomScore, today: str) -> str:
    if not winner.path.exists() or not loser.path.exists():
        return f"skipped merge `{loser.atom_id}` -> [[{winner.atom_id}]] because one side disappeared"
    winner_frontmatter, winner_body = read_page(winner.path)
    loser_frontmatter, loser_body = read_page(loser.path)
    winner_frontmatter["aliases"] = sorted({
        *[str(item) for item in winner_frontmatter.get("aliases") or []],
        *[str(item) for item in loser_frontmatter.get("aliases") or []],
        str(loser_frontmatter.get("title") or loser.atom_id),
    })
    related = {
        *[str(item) for item in winner_frontmatter.get("relates_to") or []],
        *[str(item) for item in loser_frontmatter.get("relates_to") or []],
    }
    related.discard(f"[[{loser.atom_id}]]")
    related.discard(f"[[{winner.atom_id}]]")
    winner_frontmatter["relates_to"] = sorted(related)
    merged_typed = {}
    for kind in set((winner_frontmatter.get("typed_relations") or {}).keys()) | set((loser_frontmatter.get("typed_relations") or {}).keys()):
        values = {
            *[str(item) for item in (winner_frontmatter.get("typed_relations") or {}).get(kind) or []],
            *[str(item) for item in (loser_frontmatter.get("typed_relations") or {}).get(kind) or []],
        }
        values.discard(f"[[{loser.atom_id}]]")
        values.discard(f"[[{winner.atom_id}]]")
        if values:
            merged_typed[kind] = sorted(values)
    winner_frontmatter["typed_relations"] = merged_typed
    winner_frontmatter["sources"] = sorted({
        *[str(item) for item in winner_frontmatter.get("sources") or []],
        *[str(item) for item in loser_frontmatter.get("sources") or []],
    })
    winner_frontmatter["evidence_count"] = int(winner_frontmatter.get("evidence_count") or 0) + int(loser_frontmatter.get("evidence_count") or 0)
    winner_frontmatter["last_updated"] = today
    winner_frontmatter["last_dream_pass"] = today

    winner_evidence = section_body(winner_body, "Evidence log")
    loser_evidence = section_body(loser_body, "Evidence log")
    merged_evidence_lines: list[str] = []
    seen_lines: set[str] = set()
    for block in (winner_evidence, loser_evidence):
        for line in block.splitlines():
            cleaned = line.rstrip()
            if not cleaned or cleaned in seen_lines:
                continue
            merged_evidence_lines.append(cleaned)
            seen_lines.add(cleaned)
    parsed = parse_markdown_body(winner.path.read_text(encoding="utf-8"))
    sections = [section for section in parsed.sections if section.heading != "## Evidence log"]
    sections.append(ParsedSection("## Evidence log", "\n".join(merged_evidence_lines) + "\n"))
    remade = render_markdown_body(
        ParsedMarkdownBody(
            frontmatter_block=parsed.frontmatter_block,
            intro=parsed.intro,
            sections=tuple(sections),
        )
    )
    write_page_force(winner.path, winner_frontmatter, split_frontmatter_body(remade)[1])

    loser_frontmatter["lifecycle_state"] = "dormant"
    loser_frontmatter["last_updated"] = today
    loser_frontmatter["last_dream_pass"] = today
    write_page_force(loser.path, loser_frontmatter, loser_body)
    archive_dir = v.wiki / ".archive" / loser.path.parent.relative_to(v.wiki)
    archive_dir.mkdir(parents=True, exist_ok=True)
    loser.path.replace(archive_dir / loser.path.name)
    rewrites = _rewrite_links(v, winner_id=winner.atom_id, loser_id=loser.atom_id)
    return f"merged `{loser.atom_id}` into [[{winner.atom_id}]] and rewrote {rewrites} link(s)"


def _apply_prune(v, *, action: str, score: RemAtomScore, today: str) -> str:
    frontmatter, body = read_page(score.path)
    frontmatter["last_updated"] = today
    frontmatter["last_dream_pass"] = today
    if action == "decline":
        frontmatter["lifecycle_state"] = "declining"
        write_page_force(score.path, frontmatter, body)
        return f"declined [[{score.atom_id}]]"
    frontmatter["lifecycle_state"] = "dormant"
    write_page_force(score.path, frontmatter, body)
    archive_dir = v.wiki / ".archive" / score.path.parent.relative_to(v.wiki)
    archive_dir.mkdir(parents=True, exist_ok=True)
    score.path.replace(archive_dir / score.path.name)
    return f"archived `{score.atom_id}`"


def _update_campaign_review_state(
    *,
    score: RemAtomScore,
    today: str,
    decline_after_weak_months: int,
    apply_declining_state: bool,
) -> None:
    frontmatter, body = read_page(score.path)
    weak = _is_weak(score)
    next_weak_months = int(frontmatter.get("rem_weak_months") or 0) + 1 if weak else 0
    frontmatter["last_rem_reviewed_at"] = today
    frontmatter["rem_weak_months"] = next_weak_months
    if apply_declining_state and weak and next_weak_months >= decline_after_weak_months:
        frontmatter["lifecycle_state"] = "declining"
    frontmatter["last_updated"] = today
    frontmatter["last_dream_pass"] = today
    write_page_force(score.path, frontmatter, body)


def _reactivate_declining(v, *, score: RemAtomScore, today: str) -> str | None:
    lifecycle_state = str(score.frontmatter.get("lifecycle_state") or "active")
    if lifecycle_state != "declining":
        return None
    if _is_weak(score):
        return None
    frontmatter, body = read_page(score.path)
    frontmatter["lifecycle_state"] = "active"
    frontmatter["last_updated"] = today
    frontmatter["last_dream_pass"] = today
    write_page_force(score.path, frontmatter, body)
    return f"reactivated [[{score.atom_id}]]"


def _should_reactivate(score: RemAtomScore) -> bool:
    lifecycle_state = str(score.frontmatter.get("lifecycle_state") or "active")
    return lifecycle_state == "declining" and not _is_weak(score)


def _remove_sections(path: Path, headings: set[str]) -> bool:
    if not path.exists():
        return False
    text = path.read_text(encoding="utf-8")
    parsed = parse_markdown_body(text)
    sections = tuple(section for section in parsed.sections if section.heading not in headings)
    if sections == parsed.sections:
        return False
    updated = render_markdown_body(
        ParsedMarkdownBody(
            frontmatter_block=parsed.frontmatter_block,
            intro=parsed.intro,
            sections=sections,
        )
    )
    path.write_text(updated, encoding="utf-8")
    return True


def _move_legacy_tree(source: Path, target: Path) -> int:
    if not source.exists():
        return 0
    moved = 0
    if source.is_file():
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            target.unlink()
        source.replace(target)
        return 1
    for path in sorted(source.rglob("*")):
        if not path.is_file():
            continue
        destination = target / path.relative_to(source)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            destination.unlink()
        path.replace(destination)
        moved += 1
    shutil.rmtree(source)
    return moved


def _migrate_legacy_rem_surfaces(v, *, dry_run: bool) -> list[str]:
    mutations: list[str] = []
    removals = {
        v.wiki / "me" / "profile.md": {"## Evidence"},
        v.wiki / "me" / "positioning.md": {"## Evidence"},
        v.wiki / "me" / "values.md": {"## Evidence"},
        v.wiki / "me" / "open-inquiries.md": {"## Monthly pressure"},
    }
    for path, headings in removals.items():
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        parsed = parse_markdown_body(text)
        if not any(section.heading in headings for section in parsed.sections):
            continue
        if dry_run:
            mutations.append(f"would remove legacy REM sections from {path.relative_to(v.wiki)}")
        elif _remove_sections(path, headings):
            mutations.append(f"removed legacy REM sections from {path.relative_to(v.wiki)}")

    reflections = v.wiki / "me" / "reflections"
    legacy_root = v.wiki / ".archive" / "rem-legacy"
    if reflections.exists():
        if dry_run:
            mutations.append(f"would archive {reflections.relative_to(v.wiki)}")
        else:
            moved = _move_legacy_tree(reflections, legacy_root / "reflections")
            if moved:
                mutations.append(f"archived legacy REM reflections ({moved} file(s))")

    timeline = v.wiki / "me" / "timeline.md"
    if timeline.exists() and "Dream runtime highlighted movement across" in timeline.read_text(encoding="utf-8"):
        if dry_run:
            mutations.append(f"would archive {timeline.relative_to(v.wiki)}")
        else:
            moved = _move_legacy_tree(timeline, legacy_root / "timeline.md")
            if moved:
                mutations.append("archived legacy REM timeline")
    return mutations


def _load_quality_snapshot(state, *, dry_run: bool) -> tuple[dict[str, Any], bool]:
    cached = state.get_adapter_state(QUALITY_ADAPTER) or {}
    if cached.get("lanes"):
        return cached, True
    return evaluate_and_persist_quality(persist=not dry_run, report_key="rem"), False


def _rem_carryover_state(*, today: str, month: str, hotset: list[RemAtomScore], clusters: list[RemCluster]) -> dict[str, Any]:
    return {
        "last_run_at": today,
        "month": month,
        "hotset": [
            {
                "atom_id": item.atom_id,
                "hot_score": item.hot_score,
                "life_mentions": item.life_mentions,
                "new_evidence_count": item.new_evidence_count,
            }
            for item in hotset
        ],
        "clusters": [list(cluster.member_ids) for cluster in clusters],
    }


def run_rem(
    *,
    dry_run: bool,
    acquire_lock: bool = True,
    context: DreamExecutionContext | None = None,
    write_rem_page: bool | None = None,
) -> DreamResult:
    ensure_dream_enabled()
    ensure_onboarded()
    v = vault()
    state = runtime_state()
    dream_state = state.get_dream_state()
    quality, used_cached_quality = _load_quality_snapshot(state, dry_run=dry_run)
    today = dream_today(context)
    month = dream_month(context)
    rem_hotset_cap = int(
        campaign_setting(context, "rem_hotset_cap", v.config.dream.rem_hotset_cap)
    )
    rem_cluster_limit = int(
        campaign_setting(context, "rem_cluster_limit", v.config.dream.rem_cluster_limit)
    )
    rem_candidate_multiplier = int(
        campaign_setting(context, "rem_candidate_multiplier", 3)
    )
    effective_write_rem_page = context.write_rem_page if context is not None else True
    if write_rem_page is not None:
        effective_write_rem_page = write_rem_page
    life_signals = _life_signal_counter(v)
    candidate_ids = _candidate_ids(
        v,
        last_seen=dream_state.last_rem,
        life_signals=life_signals,
        rem_hotset_cap=rem_hotset_cap,
        rem_cluster_limit=rem_cluster_limit,
        candidate_multiplier=rem_candidate_multiplier,
    )
    scores = _score_atoms(
        v,
        quality=quality,
        last_seen=dream_state.last_rem,
        life_signals=life_signals,
        candidate_ids=candidate_ids,
    )
    hotset = _hotset(scores, hotset_cap=rem_hotset_cap)
    campaign_extra_edges = _life_context_edges(v) if context and context.mode == "campaign" else None
    clusters = _cluster_hotset(
        hotset,
        v=v,
        cluster_limit=rem_cluster_limit,
        extra_edges=campaign_extra_edges,
    )
    if context and context.mode == "campaign":
        prune_candidates = _campaign_prune_candidates(
            scores,
            decline_after_weak_months=int(
                campaign_setting(context, "rem_decline_after_weak_months", 2)
            ),
            archive_after_weak_months=int(
                campaign_setting(context, "rem_archive_after_weak_months", 3)
            ),
        )
    else:
        prune_candidates = _prune_candidates(scores)
    actionable_prune_candidates = [
        (action, score)
        for action, score in prune_candidates
        if action in {"decline", "archive"}
    ]
    merge_candidates = _merge_candidates(clusters, scores)
    active_count = max(1, len(scores))
    prune_brake = bool(actionable_prune_candidates) and (
        len(actionable_prune_candidates) * 100 / active_count > v.config.dream.rem_prune_brake_pct
    )
    mutations: list[str] = []
    warnings: list[str] = []
    if prune_brake:
        warnings.append("REM prune brake triggered; report-only output for graph pruning this month")
    if used_cached_quality:
        warnings.append("using cached Dream quality snapshot")

    with dream_run("rem", dry_run=dry_run, context=context) as (runtime, run_id):
        runtime.add_run_event(
            run_id,
            stage="rem",
            event_type="selected",
            message=f"hotset={len(hotset)} clusters={len(clusters)} prune_candidates={len(prune_candidates)}",
        )
        with maybe_locked("rem", dry_run=dry_run, acquire_lock=acquire_lock):
            migrations = _migrate_legacy_rem_surfaces(v, dry_run=dry_run)
            mutations.extend(migrations)
            inactive_ids: set[str] = set()

            if dry_run:
                if prune_brake:
                    mutations.append("would keep REM graph edits in report-only mode because the prune brake is active")
                else:
                    for winner, loser in merge_candidates[:8]:
                        mutations.append(f"would merge `{loser.atom_id}` into [[{winner.atom_id}]]")
                    for action, score in prune_candidates[:12]:
                        mutations.append(f"would {action} [[{score.atom_id}]]")
                    for score in hotset:
                        if _should_reactivate(score):
                            mutations.append(f"would reactivate [[{score.atom_id}]]")
            else:
                if context and context.mode == "campaign":
                    for score in scores.values():
                        if score.path.exists():
                            _update_campaign_review_state(
                                score=score,
                                today=today,
                                decline_after_weak_months=int(v.config.dream.campaign.rem_decline_after_weak_months),
                                apply_declining_state=True,
                            )
                    for score in hotset:
                        if score.path.exists():
                            reactivation = _reactivate_declining(v, score=score, today=today)
                            if reactivation is not None:
                                mutations.append(reactivation)
                if not prune_brake:
                    merged_losers: set[str] = set()
                    for winner, loser in merge_candidates:
                        if winner.atom_id in merged_losers or loser.atom_id in merged_losers:
                            continue
                        mutations.append(_merge_active_atoms(v, winner=winner, loser=loser, today=today))
                        merged_losers.add(loser.atom_id)
                        inactive_ids.add(loser.atom_id)
                    for action, score in actionable_prune_candidates:
                        if score.atom_id in merged_losers or not score.path.exists():
                            continue
                        mutations.append(_apply_prune(v, action=action, score=score, today=today))
                        if action == "archive":
                            inactive_ids.add(score.atom_id)
                    if not (context and context.mode == "campaign"):
                        for score in hotset:
                            if score.atom_id in merged_losers or not score.path.exists():
                                continue
                            reactivation = _reactivate_declining(v, score=score, today=today)
                            if reactivation is not None:
                                mutations.append(reactivation)

            body = _render_rem_body(
                month=month,
                clusters=clusters,
                hotset=hotset,
                prune_candidates=prune_candidates,
                merge_candidates=merge_candidates,
                blocked=prune_brake,
                scores=scores,
                inactive_ids=inactive_ids if not dry_run else set(),
            )
            relates_to = [
                f"[[{item.atom_id}]]"
                for item in hotset
                if item.atom_id not in (inactive_ids if not dry_run else set())
            ][:8] or ["[[open-inquiries]]"]
            target = v.wiki / "dreams" / "rem" / f"{month}.md"
            if dry_run:
                if effective_write_rem_page:
                    mutations.append(f"would write monthly REM page {target.relative_to(v.wiki)}")
            else:
                if effective_write_rem_page:
                    write_note_page(
                        target,
                        page_type="note",
                        title=f"REM {month}",
                        body=body,
                        domains=["meta", "dream"],
                        extra_frontmatter={
                            "relates_to": relates_to,
                            "origin": "dream.rem",
                            "kind": "graph-pruning-pass",
                        },
                        force=True,
                        context=context,
                    )
                    mutations.append(f"wrote monthly REM page {target.relative_to(v.wiki)}")
                runtime.upsert_adapter_state(
                    adapter=REM_ADAPTER,
                    state=_rem_carryover_state(today=today, month=month, hotset=hotset, clusters=clusters),
                )
                runtime.update_dream_state(
                    last_rem=today,
                    deep_passes_since_rem=0,
                    last_skip_reason=None,
                )

    summary = (
        f"REM Dream processed {len(hotset)} hotset atoms, {len(clusters)} clusters, "
        f"{len(prune_candidates)} prune candidates, and {len(merge_candidates)} merge candidates."
    )
    return DreamResult(stage="rem", dry_run=dry_run, summary=summary, mutations=mutations, warnings=warnings)
