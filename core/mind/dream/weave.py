from __future__ import annotations

from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Iterable

from mind.services.graph_registry import GraphRegistry

from .common import (
    DreamExecutionContext,
    DreamPreconditionError,
    DreamResult,
    dream_run,
    dream_today,
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
from .substrate_queries import active_atoms, atom_path
from .rem import REM_ADAPTER

_EVIDENCE_ENTRY_RE = re.compile(r"^- (\d{4}-\d{2}-\d{2}) — \[\[([^\]]+)\]\]", re.MULTILINE)


@dataclass(frozen=True)
class WeaveAtomProfile:
    atom_id: str
    atom_type: str
    path: Path
    title: str
    frontmatter: dict[str, Any]
    body: str
    generic_relation_ids: frozenset[str]
    typed_relation_ids: frozenset[str]
    evidence_sources: frozenset[str]
    recent_evidence_count: int
    evidence_count: int
    life_mentions: int
    rem_carryover_bonus: int
    hot_score: int

    @property
    def relation_ids(self) -> frozenset[str]:
        return self.generic_relation_ids | self.typed_relation_ids

    @property
    def relation_degree(self) -> int:
        return len(self.relation_ids)


@dataclass(frozen=True)
class WeaveCluster:
    member_ids: tuple[str, ...]
    hub_atom_id: str
    bridge_atom_ids: tuple[str, ...]
    bridge_links: tuple["WeaveBridgeLink", ...]
    total_weight: int
    strongest_pairs: tuple[tuple[str, str, int], ...]
    merge_candidates: tuple[tuple[str, str, int, int, int], ...]
    split_candidates: tuple[tuple[str, tuple[int, ...]], ...]


@dataclass(frozen=True)
class WeaveBridgeLink:
    source_atom_id: str
    target_cluster_hub_id: str
    target_atom_id: str
    weight: int


def _coerce_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _generic_relation_ids(frontmatter: dict[str, Any]) -> frozenset[str]:
    return frozenset(
        target
        for item in _coerce_list(frontmatter.get("relates_to"))
        for target in extract_wikilinks(str(item))
        if target
    )


def _typed_relation_ids(frontmatter: dict[str, Any]) -> frozenset[str]:
    targets: set[str] = set()
    relations = frontmatter.get("typed_relations")
    if isinstance(relations, dict):
        for values in relations.values():
            for item in _coerce_list(values):
                targets.update(extract_wikilinks(str(item)))
    return frozenset(item for item in targets if item)


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


def _evidence_sources(body: str) -> frozenset[str]:
    return frozenset(source_id for _date, source_id in _evidence_entries(body))


def _recent_evidence_count(body: str, *, last_seen: str | None) -> int:
    entries = _evidence_entries(body)
    if last_seen is None:
        return len(entries)
    cutoff = last_seen[:10]
    return sum(1 for entry_date, _source_id in entries if entry_date >= cutoff)


def _life_signal_counter(v) -> Counter[str]:
    counter: Counter[str] = Counter()
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


def _hot_score(
    *,
    relation_degree: int,
    recent_evidence_count: int,
    evidence_count: int,
    life_mentions: int,
    rem_carryover_bonus: int,
) -> int:
    return (
        (relation_degree * 3)
        + (recent_evidence_count * 4)
        + min(evidence_count, 6)
        + (life_mentions * 5)
        + (rem_carryover_bonus * 4)
    )


def _tldr(body: str, *, fallback: str) -> str:
    section = section_body(body, "TL;DR")
    if section:
        line = next((item.strip() for item in section.splitlines() if item.strip()), "")
        if line:
            return line[:160]
    return fallback[:160]


def _profile_sort_key(profile: WeaveAtomProfile) -> tuple[int, int, int, int, str]:
    return (
        profile.relation_degree,
        profile.recent_evidence_count,
        profile.hot_score,
        profile.evidence_count,
        _reverse_sort_text(profile.atom_id),
    )


def _reverse_sort_text(value: str) -> str:
    return "".join(chr(255 - ord(char)) for char in value)


def _pair_key(left_id: str, right_id: str) -> tuple[str, str]:
    return (left_id, right_id) if left_id <= right_id else (right_id, left_id)


def _candidate_profiles(
    v,
    *,
    last_weave: str | None,
    candidate_cap: int,
    rem_carryover: dict[str, int],
) -> list[WeaveAtomProfile]:
    life_signals = _life_signal_counter(v)
    profiles: list[WeaveAtomProfile] = []
    for atom in active_atoms(v):
        path = atom_path(v, atom)
        if not path.exists():
            continue
        frontmatter, body = read_page(path)
        profile = WeaveAtomProfile(
            atom_id=atom.id,
            atom_type=atom.type,
            path=path,
            title=str(frontmatter.get("title") or atom.id),
            frontmatter=frontmatter,
            body=body,
            generic_relation_ids=_generic_relation_ids(frontmatter),
            typed_relation_ids=_typed_relation_ids(frontmatter),
            evidence_sources=_evidence_sources(body),
            recent_evidence_count=_recent_evidence_count(body, last_seen=last_weave),
            evidence_count=int(frontmatter.get("evidence_count") or atom.evidence_count or 0),
            life_mentions=int(life_signals.get(atom.id) or 0),
            rem_carryover_bonus=int(rem_carryover.get(atom.id) or 0),
            hot_score=0,
        )
        profiles.append(
            WeaveAtomProfile(
                **{
                    **profile.__dict__,
                    "hot_score": _hot_score(
                        relation_degree=profile.relation_degree,
                        recent_evidence_count=profile.recent_evidence_count,
                        evidence_count=profile.evidence_count,
                        life_mentions=profile.life_mentions,
                        rem_carryover_bonus=profile.rem_carryover_bonus,
                    ),
                }
            )
        )
    profiles.sort(key=_profile_sort_key, reverse=True)
    return profiles[: max(0, int(candidate_cap))]


def _raw_pair_weights(
    profiles: Iterable[WeaveAtomProfile],
    *,
    life_context_edges: set[frozenset[str]],
) -> dict[tuple[str, str], int]:
    items = list(profiles)
    weights: dict[tuple[str, str], int] = {}
    for index, left in enumerate(items):
        for right in items[index + 1 :]:
            typed = int(right.atom_id in left.typed_relation_ids or left.atom_id in right.typed_relation_ids)
            generic = int(right.atom_id in left.generic_relation_ids or left.atom_id in right.generic_relation_ids)
            shared_sources = min(len(left.evidence_sources & right.evidence_sources), 3)
            shared_life_pressure = int(frozenset({left.atom_id, right.atom_id}) in life_context_edges)
            weight = (typed * 4) + (generic * 3) + shared_sources + shared_life_pressure
            if weight > 0:
                weights[_pair_key(left.atom_id, right.atom_id)] = weight
    return weights


def _symmetric_graph(
    profiles: Iterable[WeaveAtomProfile],
    *,
    raw_weights: dict[tuple[str, str], int],
    top_neighbors_per_atom: int,
    min_edge_weight: int,
) -> dict[str, dict[str, int]]:
    neighbor_candidates: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for (left_id, right_id), weight in raw_weights.items():
        neighbor_candidates[left_id].append((right_id, weight))
        neighbor_candidates[right_id].append((left_id, weight))

    selected_pairs: set[tuple[str, str]] = set()
    for profile in profiles:
        for neighbor_id, _weight in sorted(
            neighbor_candidates.get(profile.atom_id, []),
            key=lambda item: (-item[1], item[0]),
        )[: max(0, int(top_neighbors_per_atom))]:
            selected_pairs.add(_pair_key(profile.atom_id, neighbor_id))

    adjacency: dict[str, dict[str, int]] = {profile.atom_id: {} for profile in profiles}
    for pair in selected_pairs:
        weight = raw_weights.get(pair, 0)
        if weight < int(min_edge_weight):
            continue
        left_id, right_id = pair
        adjacency[left_id][right_id] = weight
        adjacency[right_id][left_id] = weight
    return {atom_id: neighbors for atom_id, neighbors in adjacency.items() if neighbors}


def _component_total_weight(member_ids: Iterable[str], adjacency: dict[str, dict[str, int]]) -> int:
    members = set(member_ids)
    total = 0
    for left_id in sorted(members):
        for right_id, weight in adjacency.get(left_id, {}).items():
            if right_id in members and left_id < right_id:
                total += weight
    return total


def _connected_components(
    adjacency: dict[str, dict[str, int]],
    *,
    min_cluster_size: int,
    cluster_limit: int,
) -> list[tuple[str, ...]]:
    seen: set[str] = set()
    components: list[tuple[str, ...]] = []
    for atom_id in sorted(adjacency):
        if atom_id in seen:
            continue
        queue: deque[str] = deque([atom_id])
        component: list[str] = []
        seen.add(atom_id)
        while queue:
            current = queue.popleft()
            component.append(current)
            for neighbor_id in sorted(adjacency.get(current, {})):
                if neighbor_id in seen:
                    continue
                seen.add(neighbor_id)
                queue.append(neighbor_id)
        if len(component) >= int(min_cluster_size):
            components.append(tuple(sorted(component)))
    components.sort(
        key=lambda member_ids: (
            _component_total_weight(member_ids, adjacency),
            len(member_ids),
            _reverse_sort_text(member_ids[0]),
        ),
        reverse=True,
    )
    return components[: max(0, int(cluster_limit))]


def _weighted_degree(atom_id: str, member_ids: Iterable[str], adjacency: dict[str, dict[str, int]]) -> int:
    members = set(member_ids)
    return sum(weight for neighbor_id, weight in adjacency.get(atom_id, {}).items() if neighbor_id in members)


def _choose_hub(
    member_ids: tuple[str, ...],
    *,
    profiles: dict[str, WeaveAtomProfile],
    adjacency: dict[str, dict[str, int]],
) -> str:
    return sorted(
        member_ids,
        key=lambda atom_id: (
            -_weighted_degree(atom_id, member_ids, adjacency),
            -profiles[atom_id].hot_score,
            -profiles[atom_id].evidence_count,
            atom_id,
        ),
    )[0]


def _betweenness_scores(member_ids: tuple[str, ...], adjacency: dict[str, dict[str, int]]) -> dict[str, float]:
    nodes = set(member_ids)
    scores = {node_id: 0.0 for node_id in member_ids}
    for source_id in member_ids:
        stack: list[str] = []
        predecessors = {node_id: [] for node_id in member_ids}
        sigma = {node_id: 0.0 for node_id in member_ids}
        distance = {node_id: -1 for node_id in member_ids}
        sigma[source_id] = 1.0
        distance[source_id] = 0
        queue: deque[str] = deque([source_id])
        while queue:
            current = queue.popleft()
            stack.append(current)
            for neighbor_id in sorted(adjacency.get(current, {})):
                if neighbor_id not in nodes:
                    continue
                if distance[neighbor_id] < 0:
                    queue.append(neighbor_id)
                    distance[neighbor_id] = distance[current] + 1
                if distance[neighbor_id] == distance[current] + 1:
                    sigma[neighbor_id] += sigma[current]
                    predecessors[neighbor_id].append(current)
        delta = {node_id: 0.0 for node_id in member_ids}
        while stack:
            current = stack.pop()
            if sigma[current] == 0:
                continue
            for predecessor_id in predecessors[current]:
                delta[predecessor_id] += (sigma[predecessor_id] / sigma[current]) * (1.0 + delta[current])
            if current != source_id:
                scores[current] += delta[current]
    return scores


def _articulation_points(member_ids: tuple[str, ...], adjacency: dict[str, dict[str, int]]) -> set[str]:
    nodes = set(member_ids)
    discovery: dict[str, int] = {}
    low: dict[str, int] = {}
    parent: dict[str, str | None] = {}
    articulation: set[str] = set()
    time = 0

    def visit(node_id: str) -> None:
        nonlocal time
        time += 1
        discovery[node_id] = time
        low[node_id] = time
        children = 0
        for neighbor_id in sorted(adjacency.get(node_id, {})):
            if neighbor_id not in nodes:
                continue
            if neighbor_id not in discovery:
                parent[neighbor_id] = node_id
                children += 1
                visit(neighbor_id)
                low[node_id] = min(low[node_id], low[neighbor_id])
                if parent.get(node_id) is None and children > 1:
                    articulation.add(node_id)
                if parent.get(node_id) is not None and low[neighbor_id] >= discovery[node_id]:
                    articulation.add(node_id)
            elif neighbor_id != parent.get(node_id):
                low[node_id] = min(low[node_id], discovery[neighbor_id])

    for node_id in member_ids:
        if node_id in discovery:
            continue
        parent[node_id] = None
        visit(node_id)
    return articulation


def _removal_component_sizes(
    member_ids: tuple[str, ...],
    *,
    remove_id: str,
    adjacency: dict[str, dict[str, int]],
) -> tuple[int, ...]:
    remaining = [node_id for node_id in member_ids if node_id != remove_id]
    if not remaining:
        return ()
    remaining_set = set(remaining)
    seen: set[str] = set()
    sizes: list[int] = []
    for node_id in remaining:
        if node_id in seen:
            continue
        queue: deque[str] = deque([node_id])
        seen.add(node_id)
        size = 0
        while queue:
            current = queue.popleft()
            size += 1
            for neighbor_id in adjacency.get(current, {}):
                if neighbor_id not in remaining_set or neighbor_id in seen:
                    continue
                seen.add(neighbor_id)
                queue.append(neighbor_id)
        sizes.append(size)
    return tuple(sorted(sizes, reverse=True))


def _strongest_pairs(member_ids: tuple[str, ...], adjacency: dict[str, dict[str, int]], *, limit: int = 6) -> tuple[tuple[str, str, int], ...]:
    pairs: list[tuple[str, str, int]] = []
    members = set(member_ids)
    for left_id in member_ids:
        for right_id, weight in adjacency.get(left_id, {}).items():
            if right_id not in members or left_id >= right_id:
                continue
            pairs.append((left_id, right_id, weight))
    pairs.sort(key=lambda item: (-item[2], item[0], item[1]))
    return tuple(pairs[:limit])


def _merge_candidates(
    member_ids: tuple[str, ...],
    *,
    profiles: dict[str, WeaveAtomProfile],
    adjacency: dict[str, dict[str, int]],
    report_limit: int,
    min_edge_weight: int,
) -> tuple[tuple[str, str, int, int, int], ...]:
    candidates: list[tuple[str, str, int, int, int]] = []
    for index, left_id in enumerate(member_ids):
        left = profiles[left_id]
        for right_id in member_ids[index + 1 :]:
            weight = int(adjacency.get(left_id, {}).get(right_id) or 0)
            if weight < max(int(min_edge_weight) + 2, 5):
                continue
            right = profiles[right_id]
            shared_relations = len(left.relation_ids & right.relation_ids)
            shared_sources = len(left.evidence_sources & right.evidence_sources)
            if (shared_relations + shared_sources) < 2:
                continue
            candidates.append((left_id, right_id, weight, shared_relations, shared_sources))
    candidates.sort(key=lambda item: (-item[2], -(item[3] + item[4]), item[0], item[1]))
    return tuple(candidates[: max(0, int(report_limit))])


def _split_candidates(
    member_ids: tuple[str, ...],
    *,
    adjacency: dict[str, dict[str, int]],
    articulation_points: set[str],
    report_limit: int,
) -> tuple[tuple[str, tuple[int, ...]], ...]:
    candidates = [
        (atom_id, _removal_component_sizes(member_ids, remove_id=atom_id, adjacency=adjacency))
        for atom_id in articulation_points
    ]
    candidates = [item for item in candidates if len(item[1]) > 1]
    candidates.sort(key=lambda item: (-len(item[1]), -sum(item[1]), item[0]))
    return tuple(candidates[: max(0, int(report_limit))])


def _build_clusters(
    member_groups: list[tuple[str, ...]],
    *,
    profiles: dict[str, WeaveAtomProfile],
    adjacency: dict[str, dict[str, int]],
    report_bridge_limit: int,
    report_merge_limit: int,
    min_edge_weight: int,
) -> list[WeaveCluster]:
    clusters: list[WeaveCluster] = []
    for member_ids in member_groups:
        hub_atom_id = _choose_hub(member_ids, profiles=profiles, adjacency=adjacency)
        betweenness = _betweenness_scores(member_ids, adjacency)
        articulation = _articulation_points(member_ids, adjacency)
        bridge_ids = [
            atom_id
            for atom_id in sorted(
                member_ids,
                key=lambda item: (
                    -int(item in articulation),
                    -betweenness.get(item, 0.0),
                    -_weighted_degree(item, member_ids, adjacency),
                    item,
                ),
            )
            if atom_id != hub_atom_id and (atom_id in articulation or betweenness.get(atom_id, 0.0) > 0)
        ][: max(0, int(report_bridge_limit))]
        strongest_pairs = _strongest_pairs(member_ids, adjacency)
        merge_candidates = _merge_candidates(
            member_ids,
            profiles=profiles,
            adjacency=adjacency,
            report_limit=report_merge_limit,
            min_edge_weight=min_edge_weight,
        )
        split_candidates = _split_candidates(
            member_ids,
            adjacency=adjacency,
            articulation_points=articulation,
            report_limit=report_bridge_limit,
        )
        clusters.append(
            WeaveCluster(
                member_ids=member_ids,
                hub_atom_id=hub_atom_id,
                bridge_atom_ids=tuple(bridge_ids),
                bridge_links=(),
                total_weight=_component_total_weight(member_ids, adjacency),
                strongest_pairs=strongest_pairs,
                merge_candidates=merge_candidates,
                split_candidates=split_candidates,
            )
        )
    return clusters


def _attach_cross_cluster_bridges(
    clusters: list[WeaveCluster],
    *,
    raw_weights: dict[tuple[str, str], int],
    report_bridge_limit: int,
) -> list[WeaveCluster]:
    cluster_by_atom: dict[str, WeaveCluster] = {
        atom_id: cluster
        for cluster in clusters
        for atom_id in cluster.member_ids
    }
    updated: list[WeaveCluster] = []
    for cluster in clusters:
        bridge_candidates: list[WeaveBridgeLink] = []
        for source_atom_id in cluster.member_ids:
            best_by_cluster: dict[str, WeaveBridgeLink] = {}
            for (left_id, right_id), weight in raw_weights.items():
                if source_atom_id not in {left_id, right_id}:
                    continue
                other_atom_id = right_id if left_id == source_atom_id else left_id
                other_cluster = cluster_by_atom.get(other_atom_id)
                if other_cluster is None or other_cluster.hub_atom_id == cluster.hub_atom_id:
                    continue
                current = best_by_cluster.get(other_cluster.hub_atom_id)
                candidate = WeaveBridgeLink(
                    source_atom_id=source_atom_id,
                    target_cluster_hub_id=other_cluster.hub_atom_id,
                    target_atom_id=other_atom_id,
                    weight=weight,
                )
                if current is None or (candidate.weight, candidate.target_atom_id) > (current.weight, current.target_atom_id):
                    best_by_cluster[other_cluster.hub_atom_id] = candidate
            bridge_candidates.extend(best_by_cluster.values())
        bridge_candidates.sort(
            key=lambda item: (-item.weight, item.source_atom_id, item.target_cluster_hub_id, item.target_atom_id)
        )
        bridge_links = tuple(bridge_candidates[: max(0, int(report_bridge_limit))])
        bridge_atom_ids = tuple(dict.fromkeys(item.source_atom_id for item in bridge_links))
        updated.append(
            WeaveCluster(
                member_ids=cluster.member_ids,
                hub_atom_id=cluster.hub_atom_id,
                bridge_atom_ids=bridge_atom_ids,
                bridge_links=bridge_links,
                total_weight=cluster.total_weight,
                strongest_pairs=cluster.strongest_pairs,
                merge_candidates=cluster.merge_candidates,
                split_candidates=cluster.split_candidates,
            )
        )
    return updated


def _cluster_ref(cluster: WeaveCluster) -> str:
    return f"[[weave-{cluster.hub_atom_id}]]"


def _shared_evidence_sources(cluster: WeaveCluster, *, profiles: dict[str, WeaveAtomProfile], limit: int = 6) -> list[str]:
    member_sources = [profiles[atom_id].evidence_sources for atom_id in cluster.member_ids]
    if not member_sources:
        return []
    common = set(member_sources[0])
    for source_ids in member_sources[1:]:
        common &= set(source_ids)
    return sorted(common)[:limit]


def _cluster_page_body(
    cluster: WeaveCluster,
    *,
    profiles: dict[str, WeaveAtomProfile],
) -> str:
    hub = profiles[cluster.hub_atom_id]
    members = [profiles[atom_id] for atom_id in cluster.member_ids]
    strongest_pair_lines = [
        f"- [[{left_id}]] <-> [[{right_id}]] (weight={weight})"
        for left_id, right_id, weight in cluster.strongest_pairs
    ] or ["- No strong internal links rose above the current threshold."]
    bridge_lines = [
        f"- [[{item.source_atom_id}]] -> [[weave-{item.target_cluster_hub_id}]] via [[{item.target_atom_id}]] (weight={item.weight})"
        for item in cluster.bridge_links
    ] or ["- No weak cross-cluster bridge opportunities surfaced in this pass."]
    recommendation_lines: list[str] = []
    if cluster.merge_candidates:
        recommendation_lines.extend(
            f"- Merge candidate (report only): [[{left_id}]] + [[{right_id}]] "
            f"(weight={weight}, shared_relations={shared_relations}, shared_sources={shared_sources})"
            for left_id, right_id, weight, shared_relations, shared_sources in cluster.merge_candidates
        )
    if cluster.split_candidates:
        recommendation_lines.extend(
            f"- Split candidate (report only): around [[{atom_id}]] with subgroups sized {', '.join(str(size) for size in sizes)}"
            for atom_id, sizes in cluster.split_candidates
        )
    if not recommendation_lines:
        recommendation_lines.append("- No report-only merge or split recommendations surfaced this pass.")
    shared_sources = _shared_evidence_sources(cluster, profiles=profiles)
    evidence_lines = [
        f"- Shared source: [[{source_id}]]"
        for source_id in shared_sources
    ] or [
        f"- [[{member.atom_id}]] — {_tldr(member.body, fallback=member.title)}"
        for member in members[:6]
    ]
    member_lines = [
        f"- [[{member.atom_id}]] — {_tldr(member.body, fallback=member.title)}"
        for member in members
    ]
    return "\n".join(
        [
            f"# Weave Cluster: {hub.title}",
            "",
            "## Thesis",
            "",
            (
                f"This cluster organizes mature atoms around [[{hub.atom_id}]] because relation density, "
                "shared evidence, and current life-pressure signals keep them moving together."
            ),
            "",
            "## Hub",
            "",
            f"- [[{hub.atom_id}]] — {_tldr(hub.body, fallback=hub.title)}",
            "",
            "## Members",
            "",
            *member_lines,
            "",
            "## Internal structure",
            "",
            *strongest_pair_lines,
            "",
            "## Bridge candidates",
            "",
            *bridge_lines,
            "",
            "## Merge and split recommendations",
            "",
            *recommendation_lines,
            "",
            "## Evidence anchors",
            "",
            *evidence_lines,
            "",
        ]
    ).rstrip() + "\n"


def _weave_report_path(v, *, run_id: int) -> Path:
    return v.raw / "reports" / "dream" / "weave" / f"{run_id}.md"


def _render_report(
    *,
    run_id: int,
    execution_date: str,
    clusters: list[WeaveCluster],
    profiles: dict[str, WeaveAtomProfile],
    candidate_count: int,
    context: DreamExecutionContext | None,
    updated_atoms: int,
    inserted_hub_links: int,
) -> str:
    lines = [
        "# Dream Weave Report",
        "",
        f"- Run id: `{run_id}`",
        f"- Effective date: {execution_date}",
        f"- Candidate atoms: {candidate_count}",
        f"- Structural clusters: {len(clusters)}",
        f"- Atom pages touched: {updated_atoms}",
        f"- Hub-member links inserted: {inserted_hub_links}",
    ]
    if context and context.mode == "campaign":
        lines.append(f"- Campaign run id: `{context.campaign_run_id or ''}`")
    lines.extend(["", "## Clusters", ""])
    if not clusters:
        lines.append("- No structural clusters met the current thresholds.")
        return "\n".join(lines).rstrip() + "\n"
    for cluster in clusters:
        lines.extend(
            [
                f"### weave-{cluster.hub_atom_id}",
                "",
                f"- Hub: [[{cluster.hub_atom_id}]]",
                f"- Members: {', '.join(f'[[{atom_id}]]' for atom_id in cluster.member_ids)}",
                f"- Total internal weight: {cluster.total_weight}",
                (
                    "- Bridge candidates: "
                    + ", ".join(
                        f"[[{item.source_atom_id}]] -> [[weave-{item.target_cluster_hub_id}]] (via [[{item.target_atom_id}]], weight={item.weight})"
                        for item in cluster.bridge_links
                    )
                    if cluster.bridge_links
                    else "- Bridge candidates: none"
                ),
            ]
        )
        if cluster.merge_candidates:
            lines.extend(
                [
                    "- Merge candidates:",
                    *[
                        f"  - [[{left_id}]] + [[{right_id}]] "
                        f"(weight={weight}, shared_relations={shared_relations}, shared_sources={shared_sources})"
                        for left_id, right_id, weight, shared_relations, shared_sources in cluster.merge_candidates
                    ],
                ]
            )
        if cluster.split_candidates:
            lines.extend(
                [
                    "- Split candidates:",
                    *[
                        f"  - around [[{atom_id}]] with subgroup sizes {', '.join(str(size) for size in sizes)}"
                        for atom_id, sizes in cluster.split_candidates
                    ],
                ]
            )
        shared_sources = _shared_evidence_sources(cluster, profiles=profiles)
        if shared_sources:
            lines.extend(["- Shared evidence anchors:", *[f"  - [[{source_id}]]" for source_id in shared_sources]])
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _strip_weave_cluster_refs(relates_to: list[str]) -> list[str]:
    cleaned: list[str] = []
    for item in relates_to:
        targets = extract_wikilinks(str(item))
        if any(target.startswith("weave-") for target in targets):
            continue
        cleaned.append(str(item))
    return cleaned


def _update_atom_page(
    path: Path,
    *,
    cluster_ref: str,
    today: str,
    extra_relates_to: list[str],
) -> bool:
    frontmatter, body = read_page(path)
    existing_relates = _strip_weave_cluster_refs(_coerce_list(frontmatter.get("relates_to")))
    deduped_relates: list[str] = []
    seen_relates: set[str] = set()
    for item in [*existing_relates, cluster_ref, *extra_relates_to]:
        if item in seen_relates:
            continue
        seen_relates.add(item)
        deduped_relates.append(item)
    changed = False
    if deduped_relates != _coerce_list(frontmatter.get("relates_to")):
        frontmatter["relates_to"] = deduped_relates
        changed = True
    if frontmatter.get("weave_cluster_refs") != [cluster_ref]:
        frontmatter["weave_cluster_refs"] = [cluster_ref]
        changed = True
    if frontmatter.get("last_weaved_at") != today:
        frontmatter["last_weaved_at"] = today
        changed = True
    if frontmatter.get("last_updated") != today:
        frontmatter["last_updated"] = today
        changed = True
    if frontmatter.get("last_dream_pass") != today:
        frontmatter["last_dream_pass"] = today
        changed = True
    if changed:
        write_page_force(path, frontmatter, body)
    return changed


def _rem_carryover(state, *, last_rem: str | None) -> dict[str, int]:
    payload = state.get_adapter_state(REM_ADAPTER) or {}
    if not isinstance(payload, dict):
        return {}
    carried_at = str(payload.get("last_run_at") or "")
    if last_rem and carried_at[:10] != last_rem[:10]:
        return {}
    hotset = payload.get("hotset")
    if not isinstance(hotset, list):
        return {}
    total = len(hotset)
    bonuses: dict[str, int] = {}
    for index, item in enumerate(hotset):
        if not isinstance(item, dict):
            continue
        atom_id = str(item.get("atom_id") or "").strip()
        if not atom_id:
            continue
        bonuses[atom_id] = max(total - index, 1)
    return bonuses


def run_weave(
    *,
    dry_run: bool,
    acquire_lock: bool = True,
    context: DreamExecutionContext | None = None,
) -> DreamResult:
    ensure_dream_enabled()
    ensure_onboarded()
    v = vault()
    cfg = v.config.dream.weave
    if not cfg.enabled:
        raise DreamPreconditionError("dream weave is disabled in config")
    if str(cfg.auto_apply_mode or "safe") != "safe":
        raise DreamPreconditionError(f"unsupported dream.weave.auto_apply_mode={cfg.auto_apply_mode!r}")

    state = runtime_state()
    dream_state = state.get_dream_state()
    today = dream_today(context)
    rem_carryover = _rem_carryover(state, last_rem=dream_state.last_rem)
    candidate_profiles = _candidate_profiles(
        v,
        last_weave=dream_state.last_weave,
        candidate_cap=int(cfg.candidate_cap),
        rem_carryover=rem_carryover,
    )
    profiles = {profile.atom_id: profile for profile in candidate_profiles}
    life_context_edges = _life_context_edges(v)
    raw_weights = _raw_pair_weights(candidate_profiles, life_context_edges=life_context_edges)
    adjacency = _symmetric_graph(
        candidate_profiles,
        raw_weights=raw_weights,
        top_neighbors_per_atom=int(cfg.top_neighbors_per_atom),
        min_edge_weight=int(cfg.min_edge_weight),
    )
    clusters = _build_clusters(
        _connected_components(
            adjacency,
            min_cluster_size=int(cfg.min_cluster_size),
            cluster_limit=int(cfg.cluster_limit),
        ),
        profiles=profiles,
        adjacency=adjacency,
        report_bridge_limit=int(cfg.report_bridge_limit),
        report_merge_limit=int(cfg.report_merge_limit),
        min_edge_weight=int(cfg.min_edge_weight),
    )
    clusters = _attach_cross_cluster_bridges(
        clusters,
        raw_weights=raw_weights,
        report_bridge_limit=int(cfg.report_bridge_limit),
    )

    mutations: list[str] = []
    warnings: list[str] = []
    updated_atoms = 0
    inserted_hub_links = 0

    with dream_run("weave", dry_run=dry_run, context=context) as (runtime, run_id):
        runtime.add_run_event(
            run_id,
            stage="weave",
            event_type="selected",
            message=f"candidates={len(candidate_profiles)} clusters={len(clusters)}",
        )
        with maybe_locked("weave", dry_run=dry_run, acquire_lock=acquire_lock):
            for cluster in clusters:
                cluster_ref = _cluster_ref(cluster)
                hub_neighbors = [
                    (member_id, int(adjacency.get(cluster.hub_atom_id, {}).get(member_id) or 0))
                    for member_id in cluster.member_ids
                    if member_id != cluster.hub_atom_id
                ]
                strongest_hub_members = [
                    f"[[{member_id}]]"
                    for member_id, weight in sorted(hub_neighbors, key=lambda item: (-item[1], item[0]))
                    if weight >= int(cfg.hub_link_min_weight)
                ][: max(0, int(cfg.hub_link_member_limit))]
                for member_id in cluster.member_ids:
                    extra_relates = strongest_hub_members if member_id == cluster.hub_atom_id else []
                    if dry_run:
                        mutations.append(f"would update [[{member_id}]] with {cluster_ref}")
                        if member_id == cluster.hub_atom_id:
                            existing_hub_relates = set(_coerce_list(profiles[member_id].frontmatter.get("relates_to")))
                            inserted_hub_links += sum(1 for item in strongest_hub_members if item not in existing_hub_relates)
                    else:
                        existing_hub_relates = set(_coerce_list(profiles[member_id].frontmatter.get("relates_to")))
                        changed = _update_atom_page(
                            profiles[member_id].path,
                            cluster_ref=cluster_ref,
                            today=today,
                            extra_relates_to=extra_relates,
                        )
                        updated_atoms += int(changed)
                        if member_id == cluster.hub_atom_id and strongest_hub_members:
                            inserted_hub_links += sum(1 for item in strongest_hub_members if item not in existing_hub_relates)
                target = v.wiki / "dreams" / "weave" / f"weave-{cluster.hub_atom_id}.md"
                body = _cluster_page_body(cluster, profiles=profiles)
                if dry_run:
                    mutations.append(f"would write structural cluster page {target.relative_to(v.wiki)}")
                else:
                    write_note_page(
                        target,
                        page_type="note",
                        title=f"Weave Cluster: {profiles[cluster.hub_atom_id].title}",
                        body=body,
                        domains=["meta", "dream"],
                        extra_frontmatter={
                            "origin": "dream.weave",
                            "kind": "structural-cluster",
                            "hub_atom": cluster.hub_atom_id,
                            "member_atom_ids": list(cluster.member_ids),
                            "bridge_atom_ids": list(cluster.bridge_atom_ids),
                            "last_weaved_at": today,
                            "relates_to": [f"[[{atom_id}]]" for atom_id in cluster.member_ids],
                        },
                        force=True,
                        context=context,
                    )
                    mutations.append(f"wrote structural cluster page {target.relative_to(v.wiki)}")

            if dry_run:
                mutations.append("would write Dream Weave report")
                mutations.append("would rebuild graph registry")
            else:
                report_path = _weave_report_path(v, run_id=run_id)
                report_path.parent.mkdir(parents=True, exist_ok=True)
                report_path.write_text(
                    _render_report(
                        run_id=run_id,
                        execution_date=today,
                        clusters=clusters,
                        profiles=profiles,
                        candidate_count=len(candidate_profiles),
                        context=context,
                        updated_atoms=updated_atoms,
                        inserted_hub_links=inserted_hub_links,
                    ),
                    encoding="utf-8",
                )
                mutations.append(f"wrote Dream Weave report {v.logical_path(report_path)}")
                GraphRegistry.for_repo_root(v.root).rebuild()
                mutations.append("rebuilt graph registry")
                runtime.update_dream_state(last_weave=today, last_skip_reason=None)

    report_only_recommendations = sum(len(cluster.merge_candidates) + len(cluster.split_candidates) for cluster in clusters)
    summary = (
        f"Weave Dream organized {len(candidate_profiles)} mature atoms into {len(clusters)} structural clusters, "
        f"with {sum(len(cluster.bridge_atom_ids) for cluster in clusters)} bridge candidates and "
        f"{report_only_recommendations} report-only merge/split recommendations."
    )
    return DreamResult(stage="weave", dry_run=dry_run, summary=summary, mutations=mutations, warnings=warnings)
