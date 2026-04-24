from __future__ import annotations

import shutil
import time
from pathlib import Path

from scripts.atoms import cache, working_set
from tests.paths import EXAMPLES_ROOT


def _copy_harness(tmp_path: Path) -> Path:
    target = tmp_path / "thin-harness"
    shutil.copytree(EXAMPLES_ROOT, target)
    return target


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
        f"{atom_id}\n\n"
        "## Evidence log\n\n",
        encoding="utf-8",
    )


def test_load_for_source_uses_source_aware_priority_order(tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    _write_atom(
        root / "memory" / "inbox" / "probationary" / "concepts" / "2026-04-10-fresh-probationary.md",
        atom_id="fresh-probationary",
        atom_type="concept",
        lifecycle_state="probationary",
        domains=["archive"],
        topics=["misc"],
        last_evidence_date="2026-04-10",
        evidence_count=0,
    )
    _write_atom(
        root / "memory" / "concepts" / "domain-match.md",
        atom_id="domain-match",
        atom_type="concept",
        domains=["strategy"],
        topics=["misc"],
        last_evidence_date="2026-04-10",
        evidence_count=3,
    )
    _write_atom(
        root / "memory" / "playbooks" / "topic-match.md",
        atom_id="topic-match",
        atom_type="playbook",
        domains=["archive"],
        topics=["systems"],
        last_evidence_date="2026-04-10",
        evidence_count=2,
    )
    _write_atom(
        root / "memory" / "stances" / "rest-alpha.md",
        atom_id="rest-alpha",
        atom_type="stance",
        domains=["archive"],
        topics=["misc"],
        last_evidence_date="2026-04-10",
        evidence_count=1,
    )
    _write_atom(
        root / "memory" / "stances" / "rest-beta.md",
        atom_id="rest-beta",
        atom_type="stance",
        domains=["archive"],
        topics=["misc"],
        last_evidence_date="2026-04-10",
        evidence_count=1,
    )

    cache.rebuild(root)
    atoms = working_set.load_for_source(
        source_topics=["systems"],
        source_domains=["strategy"],
        cap=5,
        repo_root=root,
    )

    assert [atom.id for atom in atoms] == [
        "fresh-probationary",
        "domain-match",
        "topic-match",
        "rest-alpha",
        "rest-beta",
    ]


def test_load_inverse_for_source_prioritizes_dormant_then_inverse_buckets(tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    _write_atom(
        root / "memory" / "concepts" / "dormant-old.md",
        atom_id="dormant-old",
        atom_type="concept",
        lifecycle_state="dormant",
        domains=["work"],
        topics=["systems"],
        last_evidence_date="2026-01-01",
    )
    _write_atom(
        root / "memory" / "concepts" / "dormant-new.md",
        atom_id="dormant-new",
        atom_type="concept",
        lifecycle_state="dormant",
        domains=["work"],
        topics=["systems"],
        last_evidence_date="2026-03-01",
    )
    _write_atom(
        root / "memory" / "playbooks" / "domain-away.md",
        atom_id="domain-away",
        atom_type="playbook",
        domains=["archive"],
        topics=["systems"],
        last_evidence_date="2026-02-01",
    )
    _write_atom(
        root / "memory" / "stances" / "topic-away.md",
        atom_id="topic-away",
        atom_type="stance",
        domains=["work"],
        topics=["archive"],
        last_evidence_date="2026-02-02",
    )

    cache.rebuild(root)
    atoms = working_set.load_inverse_for_source(
        source_topics=["systems"],
        source_domains=["work"],
        cap=4,
        repo_root=root,
    )

    assert [atom.id for atom in atoms] == [
        "dormant-old",
        "dormant-new",
        "domain-away",
        "topic-away",
    ]


def test_load_for_source_rebuilds_on_stale_delete_and_filters_removed_atom(tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    cache.rebuild(root)

    victim = root / "memory" / "concepts" / "local-first-systems.md"
    time.sleep(0.02)
    victim.unlink()

    atoms = working_set.load_for_source(
        source_topics=["systems"],
        source_domains=["work"],
        cap=10,
        repo_root=root,
    )

    assert cache.is_fresh(root) is True
    assert "local-first-systems" not in {atom.id for atom in atoms}
