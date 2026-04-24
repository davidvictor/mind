from __future__ import annotations

from pathlib import Path

from mind.dream.v2.gather import gather_weave_candidate_set
from mind.runtime_state import RuntimeState
from scripts.common.vault import Vault
from tests.support import write_repo_config


def _write_active_concept(
    root: Path,
    *,
    atom_id: str,
    title: str,
    relates_to: list[str] | None = None,
    evidence_sources: list[str] | None = None,
) -> None:
    target = root / "memory" / "concepts" / f"{atom_id}.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    relates_yaml = "\n".join(f'  - "[[{item}]]"' for item in (relates_to or []))
    evidence_sources = evidence_sources or [f"summary-{atom_id}"]
    evidence_lines = "\n".join(
        f"- 2026-04-20 — [[{source_id}]] — evidence for {atom_id}"
        for source_id in evidence_sources
    )
    target.write_text(
        "---\n"
        f"id: {atom_id}\n"
        "type: concept\n"
        f'title: "{title}"\n'
        "status: active\n"
        "created: 2026-04-01\n"
        "last_updated: 2026-04-20\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/concept\n  - signal/working\n"
        "domains:\n  - meta\n"
        f"relates_to:\n{relates_yaml if relates_yaml else '  []'}\n"
        "typed_relations: {}\n"
        "lifecycle_state: active\n"
        f"evidence_count: {len(evidence_sources)}\n"
        "---\n\n"
        f"# {title}\n\n"
        "## TL;DR\n\n"
        f"{title}\n\n"
        "## Evidence log\n\n"
        f"{evidence_lines}\n",
        encoding="utf-8",
    )


def test_gather_weave_candidate_set_uses_rem_carryover_and_relation_windows(tmp_path: Path) -> None:
    write_repo_config(tmp_path, dream_enabled=True, create_me=True, create_digests=True)
    (tmp_path / "memory" / "me" / "profile.md").write_text(
        "# Profile\n\n[[alpha]]\n[[beta]]\n",
        encoding="utf-8",
    )
    _write_active_concept(tmp_path, atom_id="alpha", title="Alpha", relates_to=["beta"], evidence_sources=["shared-a", "shared-b"])
    _write_active_concept(tmp_path, atom_id="beta", title="Beta", relates_to=["alpha"], evidence_sources=["shared-a"])
    _write_active_concept(tmp_path, atom_id="zeta", title="Zeta", evidence_sources=["solo-zeta"])

    runtime = RuntimeState.for_repo_root(tmp_path)
    runtime.upsert_adapter_state(
        adapter="dream.rem",
        state={
            "hotset": [
                {"atom_id": "zeta", "hot_score": 30},
            ]
        },
    )

    candidate_set = gather_weave_candidate_set(
        vault=Vault.load(tmp_path),
        runtime=runtime,
        run_id="run-1",
        mode="shadow",
        candidate_cap=10,
        window_size=4,
    )

    assert [snapshot.atom_id for snapshot in candidate_set.atom_snapshots[:2]] == ["zeta", "alpha"]
    assert candidate_set.windows[0].atom_ids == ["zeta"]
    assert candidate_set.windows[1].atom_ids == ["alpha", "beta"]


def test_gather_weave_candidate_set_keeps_changed_atoms_before_hotness_cap(tmp_path: Path) -> None:
    write_repo_config(tmp_path, dream_enabled=True, create_me=True)
    _write_active_concept(tmp_path, atom_id="alpha", title="Alpha", evidence_sources=["source-a"])
    _write_active_concept(tmp_path, atom_id="beta", title="Beta", evidence_sources=["source-b"])
    _write_active_concept(tmp_path, atom_id="gamma", title="Gamma", evidence_sources=["source-c"])

    alpha_path = tmp_path / "memory" / "concepts" / "alpha.md"
    beta_path = tmp_path / "memory" / "concepts" / "beta.md"
    gamma_path = tmp_path / "memory" / "concepts" / "gamma.md"
    alpha_path.write_text(alpha_path.read_text(encoding="utf-8").replace("last_updated: 2026-04-20", "last_updated: 2026-04-05"), encoding="utf-8")
    beta_path.write_text(beta_path.read_text(encoding="utf-8").replace("last_updated: 2026-04-20", "last_updated: 2026-04-22"), encoding="utf-8")
    gamma_path.write_text(gamma_path.read_text(encoding="utf-8").replace("last_updated: 2026-04-20", "last_updated: 2026-04-04"), encoding="utf-8")

    runtime = RuntimeState.for_repo_root(tmp_path)
    runtime.update_dream_state(last_weave="2026-04-21")

    candidate_set = gather_weave_candidate_set(
        vault=Vault.load(tmp_path),
        runtime=runtime,
        run_id="run-2",
        mode="shadow",
        candidate_cap=1,
        window_size=4,
    )

    assert [snapshot.atom_id for snapshot in candidate_set.atom_snapshots] == ["beta"]
    assert candidate_set.atom_snapshots[0].changed_since_last_weave is True


def test_dense_corpus_windowing_does_not_force_unrelated_atoms_together(tmp_path: Path) -> None:
    write_repo_config(tmp_path, dream_enabled=True, create_me=True)
    for atom_id in ("alpha", "beta", "gamma", "delta", "epsilon", "zeta"):
        _write_active_concept(tmp_path, atom_id=atom_id, title=atom_id.title(), evidence_sources=[f"source-{atom_id}"])
    alpha_path = tmp_path / "memory" / "concepts" / "alpha.md"
    beta_path = tmp_path / "memory" / "concepts" / "beta.md"
    alpha_path.write_text(alpha_path.read_text(encoding="utf-8").replace("relates_to:\n  []", "relates_to:\n  - \"[[beta]]\""), encoding="utf-8")
    beta_path.write_text(beta_path.read_text(encoding="utf-8").replace("relates_to:\n  []", "relates_to:\n  - \"[[alpha]]\""), encoding="utf-8")

    candidate_set = gather_weave_candidate_set(
        vault=Vault.load(tmp_path),
        runtime=RuntimeState.for_repo_root(tmp_path),
        run_id="run-3",
        mode="shadow",
        candidate_cap=6,
        window_size=4,
    )

    assert candidate_set.windows[0].atom_ids == ["alpha", "beta"]
    assert all(len(window.atom_ids) == 1 for window in candidate_set.windows[1:])
