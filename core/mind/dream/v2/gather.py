from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import re

from scripts.common.slugify import slugify

from mind.dream.common import extract_wikilinks, read_page, section_body
from mind.dream.substrate_queries import active_atoms, atom_path
from mind.runtime_state import RuntimeState
from scripts.common.vault import Vault

from .contracts import AtomSnapshot, CandidateSet, HotnessFeatures, NeighborhoodWindow, SourceEvidenceRef

REM_ADAPTER = "dream.rem"
_EVIDENCE_ENTRY_RE = re.compile(r"^- (\d{4}-\d{2}-\d{2}) — \[\[([^\]]+)\]\]")


def gather_weave_candidate_set(
    *,
    vault: Vault,
    runtime: RuntimeState,
    run_id: str,
    mode: str,
    candidate_cap: int,
    window_size: int,
) -> CandidateSet:
    dream_state = runtime.get_dream_state()
    rem_carryover = _load_rem_carryover(runtime)
    life_signals = _life_signal_counter(vault)
    snapshots: list[AtomSnapshot] = []
    last_weave = dream_state.last_weave
    for atom in active_atoms(vault):
        path = atom_path(vault, atom)
        if not path.exists():
            continue
        frontmatter, body = read_page(path)
        evidence_refs = _evidence_refs(body)
        generic_relation_ids = _generic_relation_ids(frontmatter)
        typed_relation_ids = _typed_relation_ids(frontmatter)
        relation_degree = len({*generic_relation_ids, *typed_relation_ids})
        recent_evidence_count = _recent_evidence_count(evidence_refs, last_seen=last_weave)
        evidence_count = int(frontmatter.get("evidence_count") or atom.evidence_count or len(evidence_refs))
        life_mentions = int(life_signals.get(atom.id) or 0)
        rem_bonus = int(rem_carryover.get(atom.id) or 0)
        changed_since_last_weave = _changed_since_last_weave(
            frontmatter=frontmatter,
            evidence_refs=evidence_refs,
            last_weave=last_weave,
        )
        snapshot = AtomSnapshot(
            atom_id=atom.id,
            atom_type=atom.type,
            path=path.as_posix(),
            title=str(frontmatter.get("title") or atom.id),
            frontmatter=frontmatter,
            tldr=_tldr(body, fallback=str(frontmatter.get("title") or atom.id)),
            evidence_refs=evidence_refs,
            generic_relation_ids=generic_relation_ids,
            typed_relation_ids=typed_relation_ids,
            lifecycle_state=str(frontmatter.get("lifecycle_state") or "active"),
            last_updated=str(frontmatter.get("last_updated") or ""),
            last_dream_pass=str(frontmatter.get("last_dream_pass") or "") or None,
            life_mentions=life_mentions,
            prior_cluster_refs=_prior_cluster_refs(frontmatter),
            hotness_features=HotnessFeatures(
                relation_degree=relation_degree,
                recent_evidence_count=recent_evidence_count,
                evidence_count=evidence_count,
                life_mentions=life_mentions,
                rem_carryover_bonus=rem_bonus,
                hot_score=_hot_score(
                    relation_degree=relation_degree,
                    recent_evidence_count=recent_evidence_count,
                    evidence_count=evidence_count,
                    life_mentions=life_mentions,
                    rem_carryover_bonus=rem_bonus,
                ),
            ),
            changed_since_last_weave=changed_since_last_weave,
        )
        snapshots.append(snapshot)
    snapshots.sort(
        key=lambda snapshot: (
            -snapshot.hotness_features.hot_score,
            -snapshot.hotness_features.relation_degree,
            -snapshot.hotness_features.evidence_count,
            snapshot.atom_id,
        )
    )
    all_count = len(snapshots)
    changed = [snapshot for snapshot in snapshots if snapshot.changed_since_last_weave]
    unchanged = [snapshot for snapshot in snapshots if not snapshot.changed_since_last_weave]
    selected = list(changed)
    if len(selected) < candidate_cap:
        selected.extend(unchanged[: max(0, candidate_cap - len(selected))])
    snapshots = selected[:candidate_cap]
    windows = build_neighborhood_windows(snapshots=snapshots, max_window_size=window_size)
    notes: list[str] = []
    if all_count > candidate_cap:
        notes.append(f"candidate cap truncated {all_count} active atoms to {candidate_cap}")
    if changed:
        notes.append(f"changed-since-last-weave atoms included before cap: {min(len(changed), candidate_cap)}")
    return CandidateSet(
        run_id=run_id,
        stage="weave",
        generated_at=_utc_now_string(),
        mode="shadow" if mode == "shadow" else "write",
        atom_snapshots=snapshots,
        windows=windows,
        notes=notes,
    )


def build_neighborhood_windows(
    *,
    snapshots: list[AtomSnapshot],
    max_window_size: int,
) -> list[NeighborhoodWindow]:
    snapshots_by_id = {snapshot.atom_id: snapshot for snapshot in snapshots}
    remaining = [snapshot.atom_id for snapshot in snapshots]
    windows: list[NeighborhoodWindow] = []
    while remaining:
        seed_id = remaining[0]
        candidates = []
        seed = snapshots_by_id[seed_id]
        for atom_id in remaining[1:]:
            other = snapshots_by_id[atom_id]
            weight = _window_edge_weight(seed, other)
            candidates.append((atom_id, weight, other.hotness_features.hot_score))
        candidates.sort(key=lambda item: (-item[1], -item[2], item[0]))
        selected = [seed_id]
        for atom_id, weight, _score in candidates:
            if weight <= 0:
                break
            if len(selected) >= max_window_size:
                break
            selected.append(atom_id)
        selected_set = set(selected)
        remaining = [atom_id for atom_id in remaining if atom_id not in selected_set]
        windows.append(
            NeighborhoodWindow(
                window_id=f"window-{len(windows) + 1:03d}-{slugify(seed_id)}",
                seed_atom_id=seed_id,
                atom_ids=selected,
                ranked_atom_ids=selected,
                rationale=_window_rationale(seed, selected[1:], snapshots_by_id=snapshots_by_id),
            )
        )
    return windows


def _window_rationale(seed: AtomSnapshot, member_ids: list[str], *, snapshots_by_id: dict[str, AtomSnapshot]) -> list[str]:
    if not member_ids:
        return [f"isolated seed {seed.atom_id} kept as a singleton window"]
    reasons = [
        f"seed {seed.atom_id} carries hot score {seed.hotness_features.hot_score}",
    ]
    direct_neighbors = [
        atom_id
        for atom_id in member_ids
        if atom_id in seed.relation_ids or seed.atom_id in set(snapshots_by_id[atom_id].relation_ids)
    ]
    if direct_neighbors:
        reasons.append("direct relation ties: " + ", ".join(direct_neighbors[:6]))
    return reasons


def _window_edge_weight(left: AtomSnapshot, right: AtomSnapshot) -> int:
    left_relations = set(left.relation_ids)
    right_relations = set(right.relation_ids)
    weight = 0
    if right.atom_id in left_relations or left.atom_id in right_relations:
        weight += 5
    weight += 2 * len(left_relations & right_relations)
    weight += len({ref.source_id for ref in left.evidence_refs} & {ref.source_id for ref in right.evidence_refs})
    weight += 2 * len(set(left.prior_cluster_refs) & set(right.prior_cluster_refs))
    if left.life_mentions and right.life_mentions:
        weight += 1
    return weight


def _load_rem_carryover(runtime: RuntimeState) -> dict[str, int]:
    state = runtime.get_adapter_state(REM_ADAPTER) or {}
    hotset = state.get("hotset") or []
    carryover: dict[str, int] = {}
    for item in hotset:
        if not isinstance(item, dict):
            continue
        atom_id = str(item.get("atom_id") or "").strip()
        if not atom_id:
            continue
        carryover[atom_id] = int(item.get("hot_score") or 0)
    return carryover


def _life_signal_counter(vault: Vault) -> Counter[str]:
    counter: Counter[str] = Counter()
    doc_paths = [
        vault.wiki / "me" / "profile.md",
        vault.wiki / "me" / "positioning.md",
        vault.wiki / "me" / "values.md",
        vault.wiki / "me" / "open-inquiries.md",
    ]
    digest_dir = vault.wiki / "me" / "digests"
    if digest_dir.exists():
        doc_paths.extend(sorted(digest_dir.glob("*.md"), reverse=True)[:4])
    for path in doc_paths:
        if not path.exists():
            continue
        counter.update(extract_wikilinks(path.read_text(encoding="utf-8")))
    return counter


def _generic_relation_ids(frontmatter: dict[str, object]) -> list[str]:
    targets: list[str] = []
    for item in _coerce_list(frontmatter.get("relates_to")):
        targets.extend(extract_wikilinks(item))
    return list(dict.fromkeys(item for item in targets if item))


def _typed_relation_ids(frontmatter: dict[str, object]) -> list[str]:
    targets: list[str] = []
    relations = frontmatter.get("typed_relations")
    if isinstance(relations, dict):
        for values in relations.values():
            for item in _coerce_list(values):
                targets.extend(extract_wikilinks(item))
    return list(dict.fromkeys(item for item in targets if item))


def _prior_cluster_refs(frontmatter: dict[str, object]) -> list[str]:
    refs: list[str] = []
    for item in _coerce_list(frontmatter.get("weave_cluster_refs")):
        refs.extend(extract_wikilinks(item))
    return list(dict.fromkeys(item for item in refs if item))


def _coerce_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        cleaned = value.strip()
        return [cleaned] if cleaned else []
    return []


def _evidence_refs(body: str) -> list[SourceEvidenceRef]:
    evidence = section_body(body, "Evidence log")
    if not evidence:
        return []
    refs: list[SourceEvidenceRef] = []
    for line in evidence.splitlines():
        match = _EVIDENCE_ENTRY_RE.match(line.strip())
        if not match:
            continue
        refs.append(
            SourceEvidenceRef(
                observed_at=match.group(1),
                source_id=match.group(2),
                snippet=line.strip(),
            )
        )
    return refs


def _recent_evidence_count(evidence_refs: list[SourceEvidenceRef], *, last_seen: str | None) -> int:
    if not last_seen:
        return len(evidence_refs)
    cutoff = last_seen[:10]
    return sum(1 for ref in evidence_refs if ref.observed_at >= cutoff)


def _changed_since_last_weave(
    *,
    frontmatter: dict[str, object],
    evidence_refs: list[SourceEvidenceRef],
    last_weave: str | None,
) -> bool:
    if not last_weave:
        return False
    cutoff = last_weave[:10]
    last_updated = str(frontmatter.get("last_updated") or "")
    last_dream_pass = str(frontmatter.get("last_dream_pass") or "")
    if last_updated and last_updated >= cutoff:
        return True
    if last_dream_pass and last_dream_pass >= cutoff:
        return True
    return any(ref.observed_at >= cutoff for ref in evidence_refs)


def _tldr(body: str, *, fallback: str) -> str:
    tldr = section_body(body, "TL;DR")
    if tldr:
        for line in tldr.splitlines():
            cleaned = line.strip()
            if cleaned:
                return cleaned[:160]
    return fallback[:160]


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


def _utc_now_string() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
