from __future__ import annotations

import shutil
from pathlib import Path

from mind.cli import main
from mind.dream.common import DreamExecutionContext
from mind.dream.light import run_light
from mind.runtime_state import RuntimeState
from scripts.atoms.evidence_writer import append_evidence
from scripts.atoms.probationary import create_or_extend
from tests.paths import EXAMPLES_ROOT


def _copy_harness(tmp_path: Path) -> Path:
    target = tmp_path / "thin-harness"
    shutil.copytree(EXAMPLES_ROOT, target)
    cfg = target / "config.yaml"
    text = cfg.read_text(encoding="utf-8").replace("enabled: false", "enabled: true", 1)
    cfg.write_text(text, encoding="utf-8")
    return target


def _patch_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)
    monkeypatch.setattr("mind.dream.common.project_root", lambda: root)


def _write_summary(root: Path, name: str, body: str, *, concepts: list[str] | None = None) -> None:
    concepts_yaml = "\n".join(f'  - "{item}"' for item in (concepts or []))
    default_concepts = '  - "[[local-first-systems]]"'
    (root / "memory" / "summaries" / f"{name}.md").write_text(
        "---\n"
        f"id: {name}\n"
        "type: summary\n"
        'title: "Summary"\n'
        "status: active\n"
        "created: 2026-04-09\n"
        "last_updated: 2026-04-10\n"
        "aliases: []\n"
        "tags:\n  - domain/learning\n  - function/summary\n  - signal/canon\n"
        "domains:\n  - learning\n"
        "source_path: raw/drops/example.md\n"
        "source_type: document\n"
        "source_date: 2026-04-10\n"
        "ingested: 2026-04-10\n"
        "entities: []\n"
        f"concepts:\n{concepts_yaml if concepts_yaml else default_concepts}\n"
        "---\n\n"
        f"# Summary\n\n{body}\n",
        encoding="utf-8",
    )


def _write_active_atom(
    root: Path,
    *,
    dirname: str,
    atom_id: str,
    atom_type: str,
    last_evidence_date: str,
) -> None:
    (root / "memory" / dirname).mkdir(parents=True, exist_ok=True)
    (root / "memory" / dirname / f"{atom_id}.md").write_text(
        "---\n"
        f"id: {atom_id}\n"
        f"type: {atom_type}\n"
        f"title: {atom_id.replace('-', ' ').title()}\n"
        "status: active\n"
        "created: 2026-04-08\n"
        f"last_updated: {last_evidence_date}\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/note\n  - signal/working\n"
        "domains:\n  - archive\n"
        "relates_to: []\n"
        "sources: []\n"
        "lifecycle_state: active\n"
        f"last_evidence_date: {last_evidence_date}\n"
        "evidence_count: 0\n"
        "---\n\n"
        f"# {atom_id}\n\n"
        "## TL;DR\n\n"
        f"{atom_id}\n\n"
        "## Evidence log\n\n",
        encoding="utf-8",
    )


def test_light_does_not_create_probationary_inquiries_from_summary_questions(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    _write_summary(
        root,
        "summary-phase5-boundary",
        "How should the system evolve? This source revisits [[user-owned-ai]].",
    )

    assert main(["dream", "light"]) == 0
    out = capsys.readouterr().out
    assert "Light Dream processed" in out
    probationary_dir = root / "memory" / "inbox" / "probationary" / "inquiries"
    assert not probationary_dir.exists()
    stance = (root / "memory" / "stances" / "user-owned-ai.md").read_text(encoding="utf-8")
    assert "[[summary-phase5-boundary]]" in stance


def test_light_dry_run_reports_bounded_audits_and_deferred_weave(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    cfg = root / "config.yaml"
    cfg.write_text(cfg.read_text(encoding="utf-8").replace("cooccurrence_threshold: 5", "cooccurrence_threshold: 3"), encoding="utf-8")
    _write_summary(root, "summary-phase5-a", "Source A mentions [[user-owned-ai]] and [[local-first-systems]].")
    _write_summary(root, "summary-phase5-b", "Source B mentions [[user-owned-ai]] and [[local-first-systems]].")
    _write_summary(
        root,
        "summary-phase5-audits",
        "This source links [[user-owned-ai]] and however challenges the stance.",
    )
    for source_id in ("summary-phase5-a", "summary-phase5-b"):
        append_evidence(
            atom_id="user-owned-ai",
            atom_type="stance",
            date="2026-04-10",
            source_link=f"[[{source_id}]]",
            snippet="shared evidence",
            polarity="for",
            repo_root=root,
        )
        append_evidence(
            atom_id="local-first-systems",
            atom_type="concept",
            date="2026-04-10",
            source_link=f"[[{source_id}]]",
            snippet="shared evidence",
            polarity="for",
            repo_root=root,
        )

    assert main(["dream", "light", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "would tail-rescan append evidence" in out
    assert "would write co-occurrence nudge for local-first-systems and user-owned-ai (count=3)" in out
    assert "would write polarity-audit nudge" in out
    # weave warning was removed during naming cleanup


def test_light_detects_recent_probationary_duplicates(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    create_or_extend(
        type="concept",
        proposed_id="shared-pattern",
        title="Shared pattern",
        description="Shared pattern",
        snippet="Shared pattern",
        polarity="neutral",
        rationale="test",
        date="2026-04-10",
        source_link="[[summary-example-seed]]",
        repo_root=root,
    )
    create_or_extend(
        type="concept",
        proposed_id="shared-pattern-alt",
        title="Shared pattern",
        description="Shared pattern",
        snippet="Shared pattern",
        polarity="neutral",
        rationale="test",
        date="2026-04-10",
        source_link="[[summary-phase5-other]]",
        repo_root=root,
    )

    assert main(["dream", "light", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "would write merge-detection nudge for shared-pattern and shared-pattern-alt" in out


def test_light_does_not_recreate_archived_cooccurrence_nudges(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    cfg = root / "config.yaml"
    cfg.write_text(cfg.read_text(encoding="utf-8").replace("cooccurrence_threshold: 5", "cooccurrence_threshold: 3"), encoding="utf-8")
    _write_summary(root, "summary-phase5-repeat-a", "Source A mentions [[user-owned-ai]] and [[local-first-systems]].")
    _write_summary(root, "summary-phase5-repeat-b", "Source B mentions [[user-owned-ai]] and [[local-first-systems]].")
    _write_summary(root, "summary-phase5-repeat-c", "Source C mentions [[user-owned-ai]] and [[local-first-systems]].")
    for source_id in ("summary-phase5-repeat-a", "summary-phase5-repeat-b", "summary-phase5-repeat-c"):
        append_evidence(
            atom_id="user-owned-ai",
            atom_type="stance",
            date="2026-04-10",
            source_link=f"[[{source_id}]]",
            snippet="shared evidence",
            polarity="for",
            repo_root=root,
        )
        append_evidence(
            atom_id="local-first-systems",
            atom_type="concept",
            date="2026-04-10",
            source_link=f"[[{source_id}]]",
            snippet="shared evidence",
            polarity="for",
            repo_root=root,
        )

    assert main(["dream", "light"]) == 0
    _ = capsys.readouterr().out

    nudge_dir = root / "memory" / "inbox" / "nudges"
    live_matches = sorted(nudge_dir.glob("*-cooccurrence-local-first-systems-user-owned-ai.md"))
    assert live_matches
    live_nudge = live_matches[0]
    nudge_name = live_nudge.name
    archived_nudge = nudge_dir / ".processed" / nudge_name
    archived_nudge.parent.mkdir(parents=True, exist_ok=True)
    live_nudge.replace(archived_nudge)

    RuntimeState.for_repo_root(root).update_dream_state(last_light=None)

    assert main(["dream", "light"]) == 0
    out = capsys.readouterr().out

    assert "skipped existing co-occurrence nudge" in out
    assert not live_nudge.exists()
    assert archived_nudge.exists()


def test_light_demotes_repeated_cap_misses_to_dormant(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    cfg = root / "config.yaml"
    cfg.write_text(cfg.read_text(encoding="utf-8").replace("working_set_cap: 300", "working_set_cap: 1"), encoding="utf-8")
    _write_active_atom(
        root,
        dirname="concepts",
        atom_id="cap-overflow",
        atom_type="concept",
        last_evidence_date="2026-02-01",
    )
    _write_summary(
        root,
        "summary-phase5-cap",
        "This source revisits [[user-owned-ai]].",
    )

    state = RuntimeState.for_repo_root(root)
    for _ in range(3):
        assert main(["dream", "light"]) == 0
        _ = capsys.readouterr().out
        state.update_dream_state(last_light=None)

    nudge_dir = root / "memory" / "inbox" / "nudges"
    assert any("working-set-cap-audit" in path.name for path in nudge_dir.glob("*.md"))
    atom_text = (root / "memory" / "concepts" / "cap-overflow.md").read_text(encoding="utf-8")
    assert "lifecycle_state: dormant" in atom_text


def test_light_campaign_relation_only_bootstrap_lane_writes_relation_nudges_without_evidence(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    cfg = root / "config.yaml"
    cfg.write_text(cfg.read_text(encoding="utf-8").replace("cooccurrence_threshold: 5", "cooccurrence_threshold: 1"), encoding="utf-8")
    _write_summary(
        root,
        "summary-campaign-bootstrap",
        "This video mentions [[user-owned-ai]] and [[local-first-systems]] and however challenges the stance.",
    )
    summary_path = root / "memory" / "summaries" / "summary-campaign-bootstrap.md"
    summary_path.write_text(
        summary_path.read_text(encoding="utf-8").replace("source_type: document\n", "source_type: video\n"),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "mind.dream.light.evaluate_and_persist_quality",
        lambda persist, report_key: {"lanes": {"youtube": {"state": "bootstrap-only"}}},
    )

    context = DreamExecutionContext(
        effective_date="2026-04-21",
        mode="campaign",
        lane_relaxation_mode="relation_only",
        campaign_run_id="campaign-test",
    )
    result = run_light(dry_run=False, context=context)

    assert "bootstrap-only=0" in result.summary
    stance = (root / "memory" / "stances" / "user-owned-ai.md").read_text(encoding="utf-8")
    assert "[[summary-campaign-bootstrap]]" not in stance
    nudge_dir = root / "memory" / "inbox" / "nudges"
    assert (nudge_dir / "2026-04-21-polarity-audit-summary-campaign-bootstrap.md").exists()
    assert (nudge_dir / "2026-04-21-cooccurrence-local-first-systems-user-owned-ai.md").exists()


def test_light_campaign_rescans_full_corpus_without_duplicate_evidence(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    _write_summary(
        root,
        "summary-campaign-repeat",
        "This source revisits [[user-owned-ai]] with the same supporting signal.",
    )

    first_context = DreamExecutionContext(
        effective_date="2026-04-21",
        mode="campaign",
        lane_relaxation_mode="relation_only",
        campaign_run_id="campaign-test",
    )
    second_context = DreamExecutionContext(
        effective_date="2026-04-22",
        mode="campaign",
        lane_relaxation_mode="relation_only",
        campaign_run_id="campaign-test",
    )

    first = run_light(dry_run=False, context=first_context)
    second = run_light(dry_run=False, context=second_context)

    stance = (root / "memory" / "stances" / "user-owned-ai.md").read_text(encoding="utf-8")
    assert "processed 2 source pages" in first.summary
    assert "processed 2 source pages" in second.summary
    assert "0 evidence appends" not in first.summary
    assert "0 evidence appends" in second.summary
    assert stance.count("[[summary-campaign-repeat]]") == 1
