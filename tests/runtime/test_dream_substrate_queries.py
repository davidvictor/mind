from __future__ import annotations

import shutil
from pathlib import Path

from mind.dream.substrate_queries import active_atoms, atom_path, probationary_atoms, touched_active_atoms
from scripts.atoms import cache
from scripts.atoms.probationary import create_or_extend
from scripts.common.vault import Vault
from tests.paths import EXAMPLES_ROOT


def _copy_harness(tmp_path: Path) -> Path:
    target = tmp_path / "thin-harness"
    shutil.copytree(EXAMPLES_ROOT, target)
    cfg = target / "config.yaml"
    cfg.write_text(cfg.read_text(encoding="utf-8").replace("enabled: false", "enabled: true", 1), encoding="utf-8")
    return target


def _replace_frontmatter_field(path: Path, key: str, value: str) -> None:
    text = path.read_text(encoding="utf-8")
    updated = []
    replaced = False
    for line in text.splitlines():
        if line.startswith(f"{key}:"):
            updated.append(f"{key}: {value}")
            replaced = True
        else:
            updated.append(line)
    assert replaced, key
    path.write_text("\n".join(updated) + "\n", encoding="utf-8")


def test_substrate_queries_split_active_and_probationary_atoms(tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    create_or_extend(
        type="concept",
        proposed_id="probationary-signal",
        title="Probationary signal",
        description="Probationary signal",
        snippet="Probationary signal",
        polarity="neutral",
        rationale="test",
        date="2026-04-10",
        source_link="[[summary-example-seed]]",
        repo_root=root,
    )

    cache.rebuild(root)
    vault = Vault.load(root)
    active = active_atoms(vault)
    probationary = probationary_atoms(vault)

    assert "local-first-systems" in {atom.id for atom in active}
    assert "probationary-signal" in {atom.id for atom in probationary}
    assert atom_path(vault, probationary[0]).exists()


def test_touched_active_atoms_honor_last_seen(tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    stance = root / "memory" / "stances" / "user-owned-ai.md"
    concept = root / "memory" / "concepts" / "local-first-systems.md"
    _replace_frontmatter_field(stance, "last_dream_pass", "2026-04-10")
    _replace_frontmatter_field(concept, "last_dream_pass", "2026-04-08")

    cache.rebuild(root)
    touched = touched_active_atoms(Vault.load(root), last_seen="2026-04-09")

    assert [atom.id for atom in touched] == ["user-owned-ai"]
