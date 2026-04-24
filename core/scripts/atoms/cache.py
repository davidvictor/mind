"""Configured memory atom-cache rebuilder and loader.

Owns the `atoms` block of the configured memory `.brain-state.json`. Three operations:
  - `rebuild()`: walk the canonical atom directories, parse frontmatter, write the index
  - `load()`: read the serialized index as `list[Atom]`
  - `is_fresh()`: check whether the cache is newer than the current substrate tree
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
from typing import Any, Iterable

import yaml

from scripts.atoms.types import Atom
from scripts.common.contract import atom_collection_dir, atom_collection_dirs, canonicalize_page_type
from scripts.common.frontmatter import split_frontmatter as _split_frontmatter
from scripts.common.slugify import normalize_identifier
from scripts.common.vault import Vault


def _state_path(repo_root: Path) -> Path:
    return Vault.load(repo_root).brain_state


def _coerce_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value]
    return []


def _extract_topics(frontmatter: dict[str, Any]) -> list[str]:
    explicit = _coerce_list(frontmatter.get("topics"))
    if explicit:
        return explicit
    return [
        tag
        for tag in _coerce_list(frontmatter.get("tags"))
        if not tag.startswith(("domain/", "function/", "signal/"))
    ]


def _extract_tldr(body: str) -> str:
    marker = "## TL;DR"
    if marker in body:
        tail = body.split(marker, 1)[1]
        lines = [
            line.strip()
            for line in tail.splitlines()
            if line.strip() and not line.startswith("##")
        ]
        if lines:
            return " ".join(lines[:2])[:160]
    lines = [
        line.strip()
        for line in body.splitlines()
        if line.strip() and not line.startswith("#")
    ]
    return " ".join(lines[:2])[:160]


def _coerce_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _iter_atom_paths(vault: Vault) -> Iterable[tuple[Path, str]]:
    for atom_type, dirname in atom_collection_dirs().items():
        active_dir = vault.wiki / dirname
        if active_dir.exists():
            for path in sorted(active_dir.rglob("*.md")):
                yield path, "active"
        probationary_dir = vault.wiki / "inbox" / "probationary" / atom_collection_dir(atom_type)
        if probationary_dir.exists():
            for path in sorted(probationary_dir.rglob("*.md")):
                yield path, "probationary"


def _index_entry(vault: Vault, path: Path, *, lifecycle_default: str) -> dict[str, Any] | None:
    try:
        text = path.read_text(encoding="utf-8")
    except (FileNotFoundError, UnicodeDecodeError):
        return None

    frontmatter, body = _split_frontmatter(text)
    atom_id = normalize_identifier(str(frontmatter.get("id") or path.stem))
    if not atom_id:
        return None

    atom_type = canonicalize_page_type(str(frontmatter.get("type") or ""))
    if atom_type not in atom_collection_dirs():
        return None

    lifecycle_state = str(frontmatter.get("lifecycle_state") or lifecycle_default)
    if not lifecycle_state:
        lifecycle_state = lifecycle_default

    last_evidence_date = str(
        frontmatter.get("last_evidence_date")
        or frontmatter.get("last_updated")
        or ""
    )
    last_dream_pass = str(frontmatter.get("last_dream_pass") or "")
    title = str(frontmatter.get("title") or atom_id)
    tldr = str(frontmatter.get("tldr") or _extract_tldr(body) or title)[:160]

    return {
        "id": atom_id,
        "type": atom_type,
        "path": vault.logical_path(path),
        "lifecycle_state": lifecycle_state,
        "domains": _coerce_list(frontmatter.get("domains")),
        "topics": _extract_topics(frontmatter),
        "last_evidence_date": last_evidence_date,
        "evidence_count": _coerce_int(frontmatter.get("evidence_count"), default=0),
        "tldr": tldr,
        "last_dream_pass": last_dream_pass,
    }


def _read_state(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {"schema_version": "2.3"}
    try:
        loaded = json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"schema_version": "2.3"}
    if isinstance(loaded, dict):
        return loaded
    return {"schema_version": "2.3"}


def _type_order(atom_type: str) -> int:
    order = {name: idx for idx, name in enumerate(atom_collection_dirs())}
    return order.get(atom_type, len(order))


def _newest_substrate_mtime(vault: Vault) -> float:
    roots = [vault.wiki / dirname for dirname in atom_collection_dirs().values()]
    roots.extend(
        vault.wiki / "inbox" / "probationary" / dirname
        for dirname in atom_collection_dirs().values()
    )

    latest = 0.0
    for root in roots:
        if not root.exists():
            continue
        try:
            latest = max(latest, root.stat().st_mtime)
        except FileNotFoundError:
            continue
        for child in root.rglob("*"):
            try:
                latest = max(latest, child.stat().st_mtime)
            except FileNotFoundError:
                continue
    return latest


def rebuild(repo_root: Path) -> dict[str, Any]:
    """Rebuild the shared atom cache from the configured wiki tree."""
    state_path = _state_path(repo_root)
    state_path.parent.mkdir(parents=True, exist_ok=True)

    state = _read_state(state_path)
    by_type = {atom_type: 0 for atom_type in atom_collection_dirs()}
    index: list[dict[str, Any]] = []

    vault = Vault.load(repo_root)
    for path, lifecycle_default in _iter_atom_paths(vault):
        entry = _index_entry(vault, path, lifecycle_default=lifecycle_default)
        if entry is None:
            continue
        index.append(entry)
        by_type[entry["type"]] += 1

    index.sort(
        key=lambda entry: (
            _type_order(str(entry["type"])),
            str(entry["id"]),
            str(entry["path"]),
        )
    )

    state["atoms"] = {
        "last_built_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(index),
        "by_type": by_type,
        "index": index,
    }

    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=state_path.parent,
        prefix=f"{state_path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        tmp_path = Path(handle.name)
        json.dump(state, handle, indent=2, ensure_ascii=False)
        handle.write("\n")

    tmp_path.replace(state_path)
    return state


def load(repo_root: Path) -> list[Atom]:
    """Load cached atoms from `.brain-state.json`."""
    state_path = _state_path(repo_root)
    if not state_path.exists():
        raise FileNotFoundError(f"{state_path} does not exist; call rebuild first")

    state = _read_state(state_path)
    atoms_block = state.get("atoms")
    if not isinstance(atoms_block, dict):
        raise FileNotFoundError(f"{state_path} has no atoms block; call rebuild first")

    index = atoms_block.get("index")
    if not isinstance(index, list):
        raise FileNotFoundError(f"{state_path} has no atoms index; call rebuild first")

    atoms: list[Atom] = []
    for entry in index:
        if not isinstance(entry, dict):
            continue
        atom_type = canonicalize_page_type(str(entry.get("type") or ""))
        if atom_type not in atom_collection_dirs():
            continue
        atoms.append(
            Atom(
                id=str(entry.get("id") or ""),
                type=atom_type,  # type: ignore[arg-type]
                path=Path(str(entry.get("path") or "")),
                lifecycle_state=str(entry.get("lifecycle_state") or "active"),  # type: ignore[arg-type]
                domains=_coerce_list(entry.get("domains")),
                topics=_coerce_list(entry.get("topics")),
                last_evidence_date=str(entry.get("last_evidence_date") or ""),
                evidence_count=_coerce_int(entry.get("evidence_count"), default=0),
                tldr=str(entry.get("tldr") or ""),
                last_dream_pass=str(entry.get("last_dream_pass") or ""),
            )
        )
    return atoms


def is_fresh(repo_root: Path) -> bool:
    """Return True when the cache is newer than the current substrate tree."""
    state_path = _state_path(repo_root)
    if not state_path.exists():
        return False

    state = _read_state(state_path)
    atoms_block = state.get("atoms")
    if not isinstance(atoms_block, dict) or "index" not in atoms_block:
        return False

    try:
        cache_mtime = state_path.stat().st_mtime
    except FileNotFoundError:
        return False
    substrate_mtime = _newest_substrate_mtime(Vault.load(repo_root))
    return cache_mtime >= substrate_mtime
