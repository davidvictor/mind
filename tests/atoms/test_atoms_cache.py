from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

import pytest

from scripts.atoms import cache, working_set
from scripts.atoms.types import Atom
from tests.paths import EXAMPLES_ROOT


def _copy_harness(tmp_path: Path) -> Path:
    target = tmp_path / "thin-harness"
    shutil.copytree(EXAMPLES_ROOT, target)
    return target


def _read_state(root: Path) -> dict:
    return json.loads((root / "memory" / ".brain-state.json").read_text(encoding="utf-8"))


def _write_state(root: Path, state: dict) -> None:
    (root / "memory" / ".brain-state.json").write_text(
        json.dumps(state, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_atom(
    path: Path,
    *,
    atom_id: str,
    atom_type: str,
    lifecycle_state: str = "active",
    domains: list[str] | None = None,
    topics: list[str] | None = None,
    last_evidence_date: str = "2026-04-09",
    evidence_count: int = 0,
    tldr: str = "Synthetic atom",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    domains_yaml = "\n".join(f"  - {item}" for item in (domains or ["meta"]))
    topics = topics or []
    topics_block = (
        "topics: []\n"
        if not topics
        else "topics:\n" + "\n".join(f"  - {item}" for item in topics) + "\n"
    )
    path.write_text(
        "---\n"
        f"id: {atom_id}\n"
        f"type: {atom_type}\n"
        f"title: {atom_id.replace('-', ' ').title()}\n"
        "status: active\n"
        "created: 2026-04-08\n"
        f"last_updated: {last_evidence_date}\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - signal/working\n"
        f"domains:\n{domains_yaml}\n"
        f"{topics_block}"
        "sources: []\n"
        f"lifecycle_state: {lifecycle_state}\n"
        f"last_evidence_date: {last_evidence_date}\n"
        f"evidence_count: {evidence_count}\n"
        "---\n\n"
        f"# {atom_id}\n\n"
        "## TL;DR\n\n"
        f"{tldr}\n\n"
        "## Evidence log\n\n",
        encoding="utf-8",
    )


def test_rebuild_writes_atoms_block_for_copied_harness(tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    state = _read_state(root)
    state.pop("atoms")
    _write_state(root, state)

    rebuilt = cache.rebuild(root)

    assert rebuilt["atoms"]["count"] == 4
    assert rebuilt["atoms"]["by_type"] == {
        "concept": 1,
        "playbook": 1,
        "stance": 1,
        "inquiry": 1,
    }
    assert all(entry["path"].startswith("memory/") for entry in rebuilt["atoms"]["index"])

    atoms = cache.load(root)
    assert len(atoms) == 4
    assert all(isinstance(atom, Atom) for atom in atoms)
    assert {atom.id for atom in atoms} == {
        "local-first-systems",
        "weekly-review-loop",
        "user-owned-ai",
        "how-to-balance-depth-and-speed",
    }
    assert Path("memory/concepts/local-first-systems.md") in {atom.path for atom in atoms}


def test_rebuild_uses_logical_paths_for_external_memory_root(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    memory = tmp_path / "private" / "memory"
    raw = tmp_path / "private" / "raw"
    state = tmp_path / "private" / "state"
    repo.mkdir()
    memory.mkdir(parents=True)
    raw.mkdir()
    state.mkdir()
    (repo / "config.yaml").write_text(
        "paths:\n"
        f"  memory_root: {memory.as_posix()}\n"
        f"  raw_root: {raw.as_posix()}\n"
        f"  state_root: {state.as_posix()}\n"
        "vault:\n"
        "  owner_profile: me/profile.md\n"
        "llm:\n"
        "  model: google/gemini-2.5-pro\n",
        encoding="utf-8",
    )
    _write_atom(
        memory / "concepts" / "local-first-systems.md",
        atom_id="local-first-systems",
        atom_type="concept",
    )

    rebuilt = cache.rebuild(repo)
    atoms = cache.load(repo)

    assert rebuilt["atoms"]["index"][0]["path"] == "memory/concepts/local-first-systems.md"
    assert atoms[0].path == Path("memory/concepts/local-first-systems.md")
    selected = working_set.load_for_source(
        source_topics=[],
        source_domains=[],
        cap=5,
        repo_root=repo,
    )
    assert [atom.id for atom in selected] == ["local-first-systems"]


def test_rebuild_includes_probationary_atoms_and_preserves_top_level_fields(tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    state = _read_state(root)
    state["last_light_dream_at"] = "2026-04-01T00:00:00Z"
    _write_state(root, state)
    _write_atom(
        root / "memory" / "inbox" / "probationary" / "concepts" / "2026-04-10-fresh-idea.md",
        atom_id="fresh-idea",
        atom_type="concept",
        lifecycle_state="probationary",
        topics=["systems"],
        tldr="Fresh probationary idea",
    )

    rebuilt = cache.rebuild(root)
    fresh = next(entry for entry in rebuilt["atoms"]["index"] if entry["id"] == "fresh-idea")

    assert rebuilt["last_light_dream_at"] == "2026-04-01T00:00:00Z"
    assert rebuilt["atoms"]["count"] == 5
    assert rebuilt["atoms"]["by_type"]["concept"] == 2
    assert fresh["lifecycle_state"] == "probationary"
    assert fresh["path"] == "memory/inbox/probationary/concepts/2026-04-10-fresh-idea.md"


def test_is_fresh_tracks_modify_create_delete_and_rebuild(tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    cache.rebuild(root)
    assert cache.is_fresh(root) is True

    concept = root / "memory" / "concepts" / "local-first-systems.md"
    time.sleep(0.02)
    concept.write_text(concept.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    assert cache.is_fresh(root) is False

    cache.rebuild(root)
    assert cache.is_fresh(root) is True

    time.sleep(0.02)
    new_probationary = (
        root / "memory" / "inbox" / "probationary" / "concepts" / "2026-04-10-new-idea.md"
    )
    _write_atom(
        new_probationary,
        atom_id="new-idea",
        atom_type="concept",
        lifecycle_state="probationary",
        tldr="New probationary idea",
    )
    assert cache.is_fresh(root) is False

    cache.rebuild(root)
    assert cache.is_fresh(root) is True
    assert any(atom.id == "new-idea" for atom in cache.load(root))

    time.sleep(0.02)
    victim = root / "memory" / "playbooks" / "weekly-review-loop.md"
    victim.unlink()
    assert cache.is_fresh(root) is False

    cache.rebuild(root)
    assert cache.is_fresh(root) is True
    assert all(atom.id != "weekly-review-loop" for atom in cache.load(root))


def test_load_raises_when_atoms_block_missing(tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    state = _read_state(root)
    state.pop("atoms")
    _write_state(root, state)

    with pytest.raises(FileNotFoundError):
        cache.load(root)


def test_rebuild_leaves_no_tempfiles_behind(tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    cache.rebuild(root)

    leftovers = list((root / "memory").glob(".brain-state.json*.tmp"))
    assert leftovers == []
