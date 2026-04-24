from __future__ import annotations

from pathlib import Path

from scripts.atoms import cache, working_set
from scripts.atoms.types import Atom
from scripts.common.contract import atom_collection_dirs
from scripts.common.vault import Vault

from .common import read_page


def _load_atoms(repo_root: Path) -> list[Atom]:
    if not cache.is_fresh(repo_root):
        cache.rebuild(repo_root)
    try:
        return cache.load(repo_root)
    except FileNotFoundError:
        cache.rebuild(repo_root)
        return cache.load(repo_root)


def _existing(atom: Atom, *, v: Vault) -> bool:
    return v.resolve_logical_path(atom.path).exists()


def active_atoms(v: Vault) -> list[Atom]:
    return [
        atom
        for atom in _load_atoms(v.root)
        if atom.lifecycle_state != "probationary" and _existing(atom, v=v)
    ]


def probationary_atoms(v: Vault) -> list[Atom]:
    return [
        atom
        for atom in _load_atoms(v.root)
        if atom.lifecycle_state == "probationary" and _existing(atom, v=v)
    ]


def atom_path(v: Vault, atom: Atom) -> Path:
    return v.resolve_logical_path(atom.path)


def atom_frontmatter(v: Vault, atom: Atom) -> dict:
    frontmatter, _body = read_page(atom_path(v, atom))
    return frontmatter


def inverse_tail_candidates(
    v: Vault,
    *,
    source_topics: list[str],
    source_domains: list[str],
    cap: int,
) -> list[Atom]:
    return working_set.load_inverse_for_source(
        source_topics=source_topics,
        source_domains=source_domains,
        cap=cap,
        repo_root=v.root,
    )


def inverse_tail_candidates_from_atoms(
    *,
    atoms: list[Atom],
    source_topics: list[str],
    source_domains: list[str],
    cap: int,
) -> list[Atom]:
    return working_set.load_inverse_for_source_from_atoms(
        source_topics=source_topics,
        source_domains=source_domains,
        cap=cap,
        atoms=atoms,
    )


def inverse_tail_overflow(
    v: Vault,
    *,
    source_topics: list[str],
    source_domains: list[str],
    cap: int,
) -> bool:
    atoms = working_set.load_inverse_for_source(
        source_topics=source_topics,
        source_domains=source_domains,
        cap=cap + 1,
        repo_root=v.root,
    )
    return len(atoms) > cap


def touched_active_atoms(v: Vault, *, last_seen: str | None) -> list[Atom]:
    touched: list[Atom] = []
    for atom in active_atoms(v):
        touched_date = _latest_atom_touch_date(atom)
        if last_seen is None or not touched_date or touched_date >= last_seen[:10]:
            touched.append(atom)
    return touched


def _latest_atom_touch_date(atom: Atom) -> str:
    dates = [
        str(value).strip()[:10]
        for value in (getattr(atom, "last_dream_pass", ""), atom.last_evidence_date)
        if str(value).strip()
    ]
    return max(dates) if dates else ""


def active_atom_lookup(v: Vault) -> dict[str, Atom]:
    return {atom.id: atom for atom in active_atoms(v)}


def probationary_atom_lookup(v: Vault) -> dict[str, Atom]:
    return {atom.id: atom for atom in probationary_atoms(v)}


def known_atom_ids(v: Vault) -> set[str]:
    return {atom.id for atom in _load_atoms(v.root) if _existing(atom, v=v)}


def atom_dirs() -> tuple[str, ...]:
    return tuple(atom_collection_dirs().values())
