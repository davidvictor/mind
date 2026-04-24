"""Working set selectors for Pass D and Light Dream.

Source-aware selection powers Pass D during ingest. Source-inverse selection
supports Dream-side resurfacing. Both selectors consume the shared atom cache
instead of rereading markdown files directly.
"""
from __future__ import annotations

from pathlib import Path

from scripts.atoms import cache
from scripts.atoms.types import Atom
from scripts.common.vault import Vault


def _load_atoms(repo_root: Path) -> list[Atom]:
    if not cache.is_fresh(repo_root):
        cache.rebuild(repo_root)
    try:
        return cache.load(repo_root)
    except FileNotFoundError:
        cache.rebuild(repo_root)
        return cache.load(repo_root)


def _existing_atoms(atoms: list[Atom], repo_root: Path) -> list[Atom]:
    vault = Vault.load(repo_root)
    return [atom for atom in atoms if vault.resolve_logical_path(atom.path).exists()]


def _overlap_count(left: list[str], right: set[str]) -> int:
    return len(set(left) & right)


def _atom_key(atom: Atom) -> tuple[str, str]:
    return (atom.id, atom.path.as_posix())


def _sort_recency_desc(atoms: list[Atom]) -> list[Atom]:
    ordered = sorted(atoms, key=lambda atom: (atom.id, atom.path.as_posix()))
    ordered.sort(key=lambda atom: atom.evidence_count, reverse=True)
    ordered.sort(key=lambda atom: atom.last_evidence_date or "", reverse=True)
    return ordered


def _sort_recency_asc(atoms: list[Atom]) -> list[Atom]:
    ordered = sorted(atoms, key=lambda atom: (atom.id, atom.path.as_posix()))
    ordered.sort(key=lambda atom: atom.evidence_count)
    ordered.sort(key=lambda atom: atom.last_evidence_date or "")
    return ordered


def _sort_source_aware_bucket(
    atoms: list[Atom],
    *,
    source_domains: set[str],
    source_topics: set[str],
) -> list[Atom]:
    ordered = sorted(atoms, key=lambda atom: (atom.id, atom.path.as_posix()))
    ordered.sort(key=lambda atom: atom.evidence_count, reverse=True)
    ordered.sort(key=lambda atom: atom.last_evidence_date or "", reverse=True)
    ordered.sort(key=lambda atom: _overlap_count(atom.topics, source_topics), reverse=True)
    ordered.sort(key=lambda atom: _overlap_count(atom.domains, source_domains), reverse=True)
    return ordered


def _collect_buckets(buckets: list[list[Atom]], *, cap: int) -> list[Atom]:
    out: list[Atom] = []
    seen: set[tuple[str, str]] = set()
    for bucket in buckets:
        for atom in bucket:
            key = (atom.id, atom.path.as_posix())
            if key in seen:
                continue
            out.append(atom)
            seen.add(key)
            if len(out) >= cap:
                return out
    return out


def load_for_source(
    *,
    source_topics: list[str],
    source_domains: list[str],
    cap: int,
    repo_root: Path,
) -> list[Atom]:
    """Return the source-aware Pass D working set."""
    atoms = _existing_atoms(_load_atoms(repo_root), repo_root)
    source_topics_set = {item for item in source_topics if item}
    source_domains_set = {item for item in source_domains if item}

    probationary = _sort_recency_desc(
        [atom for atom in atoms if atom.lifecycle_state == "probationary"]
    )
    remaining = [atom for atom in atoms if atom.lifecycle_state != "probationary"]
    domain_overlap = [
        atom
        for atom in remaining
        if _overlap_count(atom.domains, source_domains_set) > 0
    ]
    domain_overlap_keys = {_atom_key(atom) for atom in domain_overlap}
    topic_overlap = [
        atom
        for atom in remaining
        if _atom_key(atom) not in domain_overlap_keys and _overlap_count(atom.topics, source_topics_set) > 0
    ]
    topic_overlap_keys = {_atom_key(atom) for atom in topic_overlap}
    rest = [
        atom
        for atom in remaining
        if _atom_key(atom) not in domain_overlap_keys and _atom_key(atom) not in topic_overlap_keys
    ]

    return _collect_buckets(
        [
            probationary,
            _sort_source_aware_bucket(
                domain_overlap,
                source_domains=source_domains_set,
                source_topics=source_topics_set,
            ),
            _sort_source_aware_bucket(
                topic_overlap,
                source_domains=source_domains_set,
                source_topics=source_topics_set,
            ),
            _sort_recency_desc(rest),
        ],
        cap=cap,
    )


def load_inverse_for_source(
    *,
    source_topics: list[str],
    source_domains: list[str],
    cap: int,
    repo_root: Path,
) -> list[Atom]:
    """Return the source-inverse Dream resurfacing working set."""
    atoms = _existing_atoms(_load_atoms(repo_root), repo_root)
    return load_inverse_for_source_from_atoms(
        source_topics=source_topics,
        source_domains=source_domains,
        cap=cap,
        atoms=atoms,
    )


def load_inverse_for_source_from_atoms(
    *,
    source_topics: list[str],
    source_domains: list[str],
    cap: int,
    atoms: list[Atom],
) -> list[Atom]:
    """Return the source-inverse Dream resurfacing working set from a caller-owned snapshot."""
    source_topics_set = {item for item in source_topics if item}
    source_domains_set = {item for item in source_domains if item}

    eligible = [atom for atom in atoms if atom.lifecycle_state != "probationary"]
    dormant = _sort_recency_asc(
        [atom for atom in eligible if atom.lifecycle_state == "dormant"]
    )
    remaining = [atom for atom in eligible if atom.lifecycle_state != "dormant"]
    domain_inverse = [
        atom
        for atom in remaining
        if _overlap_count(atom.domains, source_domains_set) == 0
    ]
    domain_inverse_keys = {_atom_key(atom) for atom in domain_inverse}
    topic_inverse = [
        atom
        for atom in remaining
        if _atom_key(atom) not in domain_inverse_keys and _overlap_count(atom.topics, source_topics_set) == 0
    ]
    topic_inverse_keys = {_atom_key(atom) for atom in topic_inverse}
    rest = [
        atom
        for atom in remaining
        if _atom_key(atom) not in domain_inverse_keys and _atom_key(atom) not in topic_inverse_keys
    ]

    return _collect_buckets(
        [
            dormant,
            _sort_recency_asc(domain_inverse),
            _sort_recency_asc(topic_inverse),
            _sort_recency_asc(rest),
        ],
        cap=cap,
    )
