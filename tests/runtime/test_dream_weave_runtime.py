from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

from mind.cli import main
from mind.commands.common import score_pages, vault as command_vault
from mind.dream.rem import REM_ADAPTER
from mind.dream.weave import _candidate_profiles, run_weave as run_legacy_weave
from mind.runtime_state import RuntimeState
from mind.services.graph_registry import GraphRegistry
from tests.paths import EXAMPLES_ROOT
from tests.support import write_repo_config


def _copy_harness(tmp_path: Path) -> Path:
    target = tmp_path / "thin-harness"
    shutil.copytree(EXAMPLES_ROOT, target)
    cfg = target / "config.yaml"
    cfg.write_text(cfg.read_text(encoding="utf-8").replace("enabled: false", "enabled: true", 1), encoding="utf-8")
    return target


def _patch_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)
    monkeypatch.setattr("mind.dream.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.dream.run_weave", run_legacy_weave)


def _write_active_concept(
    root: Path,
    *,
    atom_id: str,
    title: str,
    relates_to: list[str] | None = None,
    typed_relations: dict[str, list[str]] | None = None,
    evidence_sources: list[str] | None = None,
    evidence_count: int | None = None,
) -> Path:
    target = root / "memory" / "concepts" / f"{atom_id}.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    relates_yaml = "\n".join(f'  - "[[{item}]]"' for item in (relates_to or []))
    typed_relations = typed_relations or {}
    typed_block = "typed_relations: {}\n"
    if typed_relations:
        typed_blocks: list[str] = []
        for kind, targets in typed_relations.items():
            entries = "\n".join(f'    - "[[{item}]]"' for item in targets)
            typed_blocks.append(f"  {kind}:\n{entries}")
        typed_block = "typed_relations:\n" + "\n".join(typed_blocks) + "\n"
    evidence_sources = evidence_sources or ["summary-example-seed"]
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
        "sources: []\n"
        f"{typed_block}"
        "lifecycle_state: active\n"
        "last_evidence_date: 2026-04-20\n"
        f"evidence_count: {evidence_count or len(evidence_sources)}\n"
        "---\n\n"
        f"# {title}\n\n"
        "## TL;DR\n\n"
        f"{title}\n\n"
        "## Evidence log\n\n"
        f"{evidence_lines}\n",
        encoding="utf-8",
    )
    return target


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_weave_dry_run_surfaces_cluster_without_mutating_files(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    _write_active_concept(
        root,
        atom_id="alpha",
        title="Alpha",
        evidence_sources=["shared-a", "shared-b", "shared-c"],
        evidence_count=6,
    )
    _write_active_concept(
        root,
        atom_id="beta",
        title="Beta",
        typed_relations={"extends": ["alpha"]},
        evidence_sources=["shared-a", "shared-b"],
    )
    _write_active_concept(
        root,
        atom_id="gamma",
        title="Gamma",
        typed_relations={"example_of": ["alpha"]},
        evidence_sources=["shared-a", "shared-c"],
    )

    assert main(["dream", "weave", "--dry-run"]) == 0
    out = capsys.readouterr().out

    assert "Dream stage: weave" in out
    assert "would write structural cluster page dreams/weave/weave-alpha.md" in out
    assert not (root / "memory" / "dreams" / "weave").exists()
    assert not (root / "raw" / "reports" / "dream" / "weave").exists()
    assert RuntimeState.for_repo_root(root).get_dream_state().last_weave is None


def test_weave_candidate_selection_uses_rem_carryover(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    write_repo_config(root, dream_enabled=True)
    _patch_roots(monkeypatch, root)
    for atom_id in ("alpha", "beta", "gamma", "omega", "zeta"):
        _write_active_concept(
            root,
            atom_id=atom_id,
            title=atom_id.title(),
            evidence_sources=[f"source-{atom_id}"],
            evidence_count=1,
        )

    state = RuntimeState.for_repo_root(root)
    state.upsert_adapter_state(
        adapter=REM_ADAPTER,
        state={
            "last_run_at": "2026-04-20",
            "month": "2026-04",
            "hotset": [
                {"atom_id": "zeta", "hot_score": 25},
                {"atom_id": "omega", "hot_score": 20},
            ],
        },
    )
    state.update_dream_state(last_rem="2026-04-20")

    profiles = _candidate_profiles(
        command_vault(),
        last_weave=None,
        candidate_cap=2,
        rem_carryover={"zeta": 20, "omega": 19},
    )

    assert [profile.atom_id for profile in profiles] == ["zeta", "omega"]


def test_weave_live_materializes_cluster_pages_updates_atoms_and_keeps_id_stable(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    _write_active_concept(
        root,
        atom_id="alpha",
        title="Alpha",
        evidence_sources=["shared-a", "shared-b", "shared-c"],
        evidence_count=6,
    )
    _write_active_concept(
        root,
        atom_id="beta",
        title="Beta",
        typed_relations={"extends": ["alpha"]},
        evidence_sources=["shared-a", "shared-b"],
    )
    _write_active_concept(
        root,
        atom_id="gamma",
        title="Gamma",
        typed_relations={"example_of": ["alpha"]},
        evidence_sources=["shared-a", "shared-c"],
    )

    assert main(["dream", "weave"]) == 0
    first_out = capsys.readouterr().out
    assert "Weave Dream organized" in first_out

    cluster_page = root / "memory" / "dreams" / "weave" / "weave-alpha.md"
    assert cluster_page.exists()
    cluster_text = _read_text(cluster_page)
    assert "origin: dream.weave" in cluster_text
    assert "kind: structural-cluster" in cluster_text
    assert "hub_atom: alpha" in cluster_text
    assert "## Bridge candidates" in cluster_text

    today = date.today().isoformat()
    for atom_id in ("alpha", "beta", "gamma"):
        text = _read_text(root / "memory" / "concepts" / f"{atom_id}.md")
        assert 'weave_cluster_refs:\n  - "[[weave-alpha]]"' in text
        assert f"last_weaved_at: {today}" in text
        assert '"[[weave-alpha]]"' in text

    alpha_text = _read_text(root / "memory" / "concepts" / "alpha.md")
    assert '"[[beta]]"' in alpha_text
    assert '"[[gamma]]"' in alpha_text

    reports = sorted((root / "raw" / "reports" / "dream" / "weave").glob("*.md"))
    assert len(reports) == 1
    assert "Dream Weave Report" in _read_text(reports[0])
    assert RuntimeState.for_repo_root(root).get_dream_state().last_weave == today

    matches = score_pages("shared evidence and current life-pressure signals", command_vault(), limit=10)
    assert any(match.path == cluster_page for match in matches)
    assert main(["query", "shared evidence and current life-pressure signals"]) == 0
    assert "[[weave-alpha]]" in capsys.readouterr().out

    assert main(["dream", "weave"]) == 0
    _ = capsys.readouterr().out
    assert cluster_page.exists()
    assert "weave-alpha.md" in sorted(path.name for path in (root / "memory" / "dreams" / "weave").glob("weave-*.md"))


def test_weave_keeps_merge_and_split_recommendations_report_only(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    _write_active_concept(
        root,
        atom_id="alpha",
        title="Alpha",
        evidence_sources=["shared-a", "shared-b", "shared-c"],
        evidence_count=6,
    )
    _write_active_concept(
        root,
        atom_id="beta",
        title="Beta",
        typed_relations={"extends": ["alpha"], "adjacent_to": ["gamma"]},
        evidence_sources=["shared-a", "shared-b", "shared-c"],
    )
    _write_active_concept(
        root,
        atom_id="gamma",
        title="Gamma",
        typed_relations={"example_of": ["beta"], "adjacent_to": ["delta"]},
        evidence_sources=["shared-c", "shared-d"],
    )
    _write_active_concept(
        root,
        atom_id="delta",
        title="Delta",
        typed_relations={"example_of": ["gamma"]},
        evidence_sources=["shared-d"],
    )

    assert main(["dream", "weave"]) == 0
    _ = capsys.readouterr().out

    cluster_page = root / "memory" / "dreams" / "weave" / "weave-beta.md"
    cluster_text = _read_text(cluster_page)
    assert "Merge candidate (report only)" in cluster_text
    assert "Split candidate (report only)" in cluster_text
    for atom_id in ("alpha", "beta", "gamma", "delta"):
        assert (root / "memory" / "concepts" / f"{atom_id}.md").exists()


def test_weave_reports_cross_cluster_bridge_candidates(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    _write_active_concept(
        root,
        atom_id="alpha",
        title="Alpha",
        evidence_sources=["cluster-a-1", "cluster-a-2"],
        evidence_count=4,
    )
    _write_active_concept(
        root,
        atom_id="beta",
        title="Beta",
        typed_relations={"extends": ["alpha"]},
        evidence_sources=["cluster-a-1", "cluster-a-2"],
    )
    _write_active_concept(
        root,
        atom_id="gamma",
        title="Gamma",
        typed_relations={"example_of": ["alpha"]},
        evidence_sources=["bridge-source"],
    )
    _write_active_concept(
        root,
        atom_id="delta",
        title="Delta",
        evidence_sources=["cluster-b-1", "cluster-b-2"],
        evidence_count=4,
    )
    _write_active_concept(
        root,
        atom_id="epsilon",
        title="Epsilon",
        typed_relations={"extends": ["delta"]},
        evidence_sources=["cluster-b-1", "cluster-b-2"],
    )
    _write_active_concept(
        root,
        atom_id="zeta",
        title="Zeta",
        typed_relations={"example_of": ["delta"]},
        evidence_sources=["bridge-source"],
    )

    assert main(["dream", "weave"]) == 0
    _ = capsys.readouterr().out

    cluster_text = _read_text(root / "memory" / "dreams" / "weave" / "weave-alpha.md")
    assert "[[weave-delta]]" in cluster_text
    assert "via [[zeta]]" in cluster_text or "via [[delta]]" in cluster_text


def test_weave_rebuild_indexes_cluster_pages_as_documents_not_nodes(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    _write_active_concept(
        root,
        atom_id="alpha",
        title="Alpha",
        evidence_sources=["shared-a", "shared-b", "shared-c"],
        evidence_count=6,
    )
    _write_active_concept(
        root,
        atom_id="beta",
        title="Beta",
        typed_relations={"extends": ["alpha"]},
        evidence_sources=["shared-a", "shared-b"],
    )
    _write_active_concept(
        root,
        atom_id="gamma",
        title="Gamma",
        typed_relations={"example_of": ["alpha"]},
        evidence_sources=["shared-a", "shared-c"],
    )

    assert main(["dream", "weave"]) == 0
    _ = capsys.readouterr().out

    registry = GraphRegistry.for_repo_root(root)
    with registry.connect() as conn:
        node_count = int(
            conn.execute("SELECT COUNT(*) AS count FROM nodes WHERE page_id = ?", ("weave-alpha",)).fetchone()["count"]
        )
        doc_count = int(
            conn.execute(
                "SELECT COUNT(*) AS count FROM documents WHERE path = ?",
                ("memory/dreams/weave/weave-alpha.md",),
            ).fetchone()["count"]
        )
    assert node_count == 0
    assert doc_count == 1

    matches = registry.query_pages("shared evidence and current life-pressure signals", limit=10)
    assert any(match.path == "memory/dreams/weave/weave-alpha.md" for match in matches)
