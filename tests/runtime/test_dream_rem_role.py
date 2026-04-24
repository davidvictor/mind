from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

from mind.cli import main
from mind.dream.common import DreamExecutionContext
from mind.dream.rem import run_rem
from mind.runtime_state import RuntimeState
from tests.paths import EXAMPLES_ROOT


def _copy_harness(tmp_path: Path) -> Path:
    target = tmp_path / "thin-harness"
    shutil.copytree(EXAMPLES_ROOT, target)
    cfg = target / "config.yaml"
    text = cfg.read_text(encoding="utf-8").replace("enabled: false", "enabled: true", 1)
    text = text.replace("run_after_rem: true", "run_after_rem: false", 1)
    cfg.write_text(text, encoding="utf-8")
    return target


def _patch_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)
    monkeypatch.setattr("mind.dream.common.project_root", lambda: root)


def _append_section(path: Path, heading: str, content: str) -> None:
    text = path.read_text(encoding="utf-8").rstrip() + f"\n\n{heading}\n\n{content.strip()}\n"
    path.write_text(text, encoding="utf-8")


def _write_active_concept(
    root: Path,
    *,
    atom_id: str,
    title: str,
    relates_to: list[str] | None = None,
    lifecycle_state: str = "active",
    evidence_count: int = 1,
    last_evidence_date: str = "2026-04-10",
) -> Path:
    target = root / "memory" / "concepts" / f"{atom_id}.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    relates_yaml = "\n".join(f'  - "[[{item}]]"' for item in (relates_to or []))
    target.write_text(
        "---\n"
        f"id: {atom_id}\n"
        "type: concept\n"
        f"title: {title}\n"
        "status: active\n"
        "created: 2026-04-01\n"
        "last_updated: 2026-04-10\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/concept\n  - signal/working\n"
        "domains:\n  - meta\n"
        f"relates_to:\n{relates_yaml if relates_yaml else '  []'}\n"
        "sources: []\n"
        f"lifecycle_state: {lifecycle_state}\n"
        f"last_evidence_date: {last_evidence_date}\n"
        f"evidence_count: {evidence_count}\n"
        "---\n\n"
        f"# {title}\n\n"
        "## TL;DR\n\n"
        f"{title}\n\n"
        "## Evidence log\n\n"
        f"- {last_evidence_date} — [[summary-example-seed]] — seeded evidence\n",
        encoding="utf-8",
    )
    return target


def test_rem_writes_monthly_graph_page_without_touching_me_inputs(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    before = {
        name: (root / "memory" / "me" / name).read_text(encoding="utf-8")
        for name in ("profile.md", "positioning.md", "values.md", "open-inquiries.md")
    }

    assert main(["dream", "rem", "--dry-run"]) == 0
    dry_out = capsys.readouterr().out
    assert "would write monthly REM page" in dry_out

    assert main(["dream", "rem"]) == 0
    out = capsys.readouterr().out
    assert "REM Dream processed" in out
    assert list((root / "memory" / "dreams" / "rem").glob("*.md"))
    reflection_pages = sorted((root / "memory" / "me" / "reflections").glob("*.md"))
    assert reflection_pages
    reflection_text = reflection_pages[0].read_text(encoding="utf-8")
    assert "REM Reflection" in reflection_text
    assert not (root / "memory" / "me" / "timeline.md").exists()
    for name, text in before.items():
        assert (root / "memory" / "me" / name).read_text(encoding="utf-8") == text
    state = RuntimeState.for_repo_root(root)
    assert state.get_dream_state().last_rem == date.today().isoformat()


def test_rem_migrates_legacy_me_surfaces_and_archives_old_outputs(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    _append_section(root / "memory" / "me" / "profile.md", "## Evidence", "- [[local-first-systems]]")
    _append_section(root / "memory" / "me" / "positioning.md", "## Evidence", "- [[weekly-review-loop]]")
    _append_section(root / "memory" / "me" / "values.md", "## Evidence", "- [[user-owned-ai]]")
    _append_section(root / "memory" / "me" / "open-inquiries.md", "## Monthly pressure", "- [[how-to-balance-depth-and-speed]]")
    legacy_reflection = root / "memory" / "me" / "reflections" / "2026-04.md"
    legacy_reflection.parent.mkdir(parents=True, exist_ok=True)
    legacy_reflection.write_text("# Reflection\n", encoding="utf-8")
    (root / "memory" / "me" / "timeline.md").write_text(
        "---\n"
        "id: timeline\n"
        "type: note\n"
        "title: Timeline\n"
        "status: active\n"
        "created: 2026-04-21\n"
        "last_updated: 2026-04-21\n"
        "aliases: []\n"
        "tags: []\n"
        "domains: []\n"
        "relates_to: []\n"
        "sources: []\n"
        "---\n\n"
        "# Timeline\n\n"
        "### 2026-04\n\n"
        "Dream runtime highlighted movement across 903 atoms.\n",
        encoding="utf-8",
    )

    assert main(["dream", "rem"]) == 0
    _ = capsys.readouterr().out

    assert "## Evidence" not in (root / "memory" / "me" / "profile.md").read_text(encoding="utf-8")
    assert "## Evidence" not in (root / "memory" / "me" / "positioning.md").read_text(encoding="utf-8")
    assert "## Evidence" not in (root / "memory" / "me" / "values.md").read_text(encoding="utf-8")
    assert "## Monthly pressure" not in (root / "memory" / "me" / "open-inquiries.md").read_text(encoding="utf-8")
    assert (root / "memory" / ".archive" / "rem-legacy" / "reflections" / "2026-04.md").exists()
    assert (root / "memory" / ".archive" / "rem-legacy" / "timeline.md").exists()
    assert sorted((root / "memory" / "me" / "reflections").glob("*.md"))
    assert not (root / "memory" / "me" / "timeline.md").exists()


def test_rem_prune_brake_falls_back_to_report_only(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    cfg = root / "config.yaml"
    cfg.write_text(cfg.read_text(encoding="utf-8").replace("rem_prune_brake_pct: 15", "rem_prune_brake_pct: 0"), encoding="utf-8")
    stale = _write_active_concept(
        root,
        atom_id="stale-idea",
        title="Stale Idea",
        lifecycle_state="declining",
        evidence_count=0,
        last_evidence_date="2025-01-01",
    )
    stale.write_text(stale.read_text(encoding="utf-8").replace("- 2025-01-01 — [[summary-example-seed]] — seeded evidence\n", ""), encoding="utf-8")

    assert main(["dream", "rem"]) == 0
    out = capsys.readouterr().out

    assert "report-only output for graph pruning this month" in out
    assert list((root / "memory" / "dreams" / "rem").glob("*.md"))
    assert (root / "memory" / "concepts" / "local-first-systems.md").exists()
    assert not (root / "memory" / ".archive" / "concepts" / "local-first-systems.md").exists()


def test_rem_applies_cluster_merge_and_archive_on_repeated_weakness(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    cfg = root / "config.yaml"
    cfg.write_text(cfg.read_text(encoding="utf-8").replace("rem_prune_brake_pct: 15", "rem_prune_brake_pct: 100"), encoding="utf-8")
    duplicate = _write_active_concept(
        root,
        atom_id="local-first-systems-pattern",
        title="Local First Systems Pattern",
        relates_to=["local-first-systems"],
        evidence_count=1,
        last_evidence_date="2026-04-10",
    )
    stale = _write_active_concept(
        root,
        atom_id="stale-idea",
        title="Stale Idea",
        lifecycle_state="declining",
        evidence_count=0,
        last_evidence_date="2025-01-01",
    )
    stale.write_text(stale.read_text(encoding="utf-8").replace("- 2025-01-01 — [[summary-example-seed]] — seeded evidence\n", ""), encoding="utf-8")

    assert main(["dream", "rem"]) == 0
    out = capsys.readouterr().out

    assert "REM Dream processed" in out
    assert not duplicate.exists()
    assert (root / "memory" / ".archive" / "concepts" / "local-first-systems-pattern.md").exists()
    assert not stale.exists()
    assert (root / "memory" / ".archive" / "concepts" / "stale-idea.md").exists()
    winner_text = (root / "memory" / "concepts" / "local-first-systems.md").read_text(encoding="utf-8")
    assert "Local First Systems Pattern" in winner_text


def test_rem_campaign_tracks_repeated_weak_months_until_archive(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    cfg = root / "config.yaml"
    cfg.write_text(cfg.read_text(encoding="utf-8").replace("rem_prune_brake_pct: 15", "rem_prune_brake_pct: 100"), encoding="utf-8")
    stale = _write_active_concept(
        root,
        atom_id="campaign-stale",
        title="Campaign Stale",
        evidence_count=3,
        last_evidence_date="2025-01-01",
    )
    stale.write_text(stale.read_text(encoding="utf-8").replace("- 2025-01-01 — [[summary-example-seed]] — seeded evidence\n", ""), encoding="utf-8")

    context_one = DreamExecutionContext(
        effective_date="2026-04-30",
        mode="campaign",
        lane_relaxation_mode="relation_only",
        campaign_run_id="campaign-test",
    )
    run_rem(dry_run=False, context=context_one)
    first_text = stale.read_text(encoding="utf-8")
    assert "last_rem_reviewed_at: 2026-04-30" in first_text
    assert "rem_weak_months: 1" in first_text
    assert "lifecycle_state: active" in first_text

    context_two = DreamExecutionContext(
        effective_date="2026-05-30",
        mode="campaign",
        lane_relaxation_mode="relation_only",
        campaign_run_id="campaign-test",
    )
    run_rem(dry_run=False, context=context_two)
    second_text = stale.read_text(encoding="utf-8")
    assert "last_rem_reviewed_at: 2026-05-30" in second_text
    assert "rem_weak_months: 2" in second_text
    assert "lifecycle_state: declining" in second_text

    context_three = DreamExecutionContext(
        effective_date="2026-06-30",
        mode="campaign",
        lane_relaxation_mode="relation_only",
        campaign_run_id="campaign-test",
    )
    run_rem(dry_run=False, context=context_three)
    assert not stale.exists()
    archived = root / "memory" / ".archive" / "concepts" / "campaign-stale.md"
    assert archived.exists()
    assert "rem_weak_months: 3" in archived.read_text(encoding="utf-8")


def test_rem_campaign_resets_weak_counter_when_life_pressure_returns(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    stale = _write_active_concept(
        root,
        atom_id="campaign-reset",
        title="Campaign Reset",
        evidence_count=3,
        last_evidence_date="2025-01-01",
    )
    stale.write_text(stale.read_text(encoding="utf-8").replace("- 2025-01-01 — [[summary-example-seed]] — seeded evidence\n", ""), encoding="utf-8")

    first = DreamExecutionContext(
        effective_date="2026-04-30",
        mode="campaign",
        lane_relaxation_mode="relation_only",
        campaign_run_id="campaign-test",
    )
    run_rem(dry_run=False, context=first)
    assert "rem_weak_months: 1" in stale.read_text(encoding="utf-8")

    _append_section(root / "memory" / "me" / "open-inquiries.md", "## Campaign pressure", "- [[campaign-reset]]")
    second = DreamExecutionContext(
        effective_date="2026-05-30",
        mode="campaign",
        lane_relaxation_mode="relation_only",
        campaign_run_id="campaign-test",
    )
    run_rem(dry_run=False, context=second)
    second_text = stale.read_text(encoding="utf-8")
    assert "rem_weak_months: 0" in second_text
    assert "last_rem_reviewed_at: 2026-05-30" in second_text


def test_rem_campaign_prune_brake_keeps_decline_report_only_but_updates_review_state(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    cfg = root / "config.yaml"
    cfg.write_text(cfg.read_text(encoding="utf-8").replace("rem_prune_brake_pct: 15", "rem_prune_brake_pct: 0"), encoding="utf-8")
    stale = _write_active_concept(
        root,
        atom_id="campaign-brake",
        title="Campaign Brake",
        evidence_count=3,
        last_evidence_date="2025-01-01",
    )
    stale.write_text(stale.read_text(encoding="utf-8").replace("- 2025-01-01 — [[summary-example-seed]] — seeded evidence\n", ""), encoding="utf-8")

    context = DreamExecutionContext(
        effective_date="2026-04-30",
        mode="campaign",
        lane_relaxation_mode="relation_only",
        campaign_run_id="campaign-test",
    )
    run_rem(dry_run=False, context=context)
    second_context = DreamExecutionContext(
        effective_date="2026-05-30",
        mode="campaign",
        lane_relaxation_mode="relation_only",
        campaign_run_id="campaign-test",
    )
    result = run_rem(dry_run=False, context=second_context)
    text = stale.read_text(encoding="utf-8")

    assert "report-only output for graph pruning this month" in "\n".join(result.warnings)
    assert "rem_weak_months: 2" in text
    assert "lifecycle_state: declining" in text
    assert stale.exists()
