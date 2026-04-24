from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from mind.cli import main
from mind.dream.bootstrap import BootstrapSource, run_bootstrap_checkpoint
from mind.dream.common import DreamResult
from mind.services.llm_cache import LLMCacheIdentity, write_llm_cache
from mind.runtime_state import RuntimeState
from scripts.atoms.prompts import PASS_D_PROMPT_VERSION
from tests.paths import EXAMPLES_ROOT


THIN_HARNESS = EXAMPLES_ROOT


def _patch_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)
    monkeypatch.setattr("mind.dream.common.project_root", lambda: root)


def _enable_dream(root: Path) -> None:
    cfg = root / "config.yaml"
    text = cfg.read_text(encoding="utf-8")
    text = text.replace("enabled: false", "enabled: true", 1)
    cfg.write_text(text, encoding="utf-8")


def _copy_harness(tmp_path: Path) -> Path:
    target = tmp_path / "thin-harness"
    shutil.copytree(THIN_HARNESS, target)
    _enable_dream(target)
    return target


def _write_summary(root: Path, name: str, *, source_date: str) -> Path:
    path = root / "memory" / "summaries" / f"{name}.md"
    path.write_text(
        "---\n"
        f"id: {name}\n"
        "type: summary\n"
        'title: "Summary"\n'
        "status: active\n"
        f"created: {source_date}\n"
        f"last_updated: {source_date}\n"
        "aliases: []\n"
        "tags:\n  - domain/learning\n  - function/summary\n  - signal/canon\n"
        "domains:\n  - learning\n"
        f"source_date: {source_date}\n"
        "source_type: article\n"
        "relates_to: []\n"
        "sources: []\n"
        "---\n\n"
        "# Summary\n\n"
        "## TL;DR\n\n"
        "Bootstrap summary body.\n",
        encoding="utf-8",
    )
    return path


def test_bootstrap_dry_run_reports_work_without_mutation(tmp_path: Path, monkeypatch, capsys):
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    summary_paths = [
        _write_summary(root, "summary-bootstrap-a", source_date="2026-04-01"),
        _write_summary(root, "summary-bootstrap-b", source_date="2026-04-02"),
    ]
    sources = [
        BootstrapSource(
            summary_id=path.stem,
            source_kind="article",
            source_date=f"2026-04-0{index + 1}",
            summary_path=path,
        )
        for index, path in enumerate(summary_paths)
    ]
    monkeypatch.setattr("mind.dream.bootstrap.enumerate_bootstrap_sources", lambda _vault: sources)

    before_text = (root / "memory" / "stances" / "user-owned-ai.md").read_text(encoding="utf-8")
    before_dream = RuntimeState.for_repo_root(root).get_dream_state()

    assert main(["dream", "bootstrap", "--dry-run", "--checkpoint-every", "1"]) == 0
    out = capsys.readouterr().out

    after_text = (root / "memory" / "stances" / "user-owned-ai.md").read_text(encoding="utf-8")
    after_dream = RuntimeState.for_repo_root(root).get_dream_state()
    assert "Dream stage: bootstrap" in out
    assert "Bootstrap rehearsal planned for 2 sources" in out
    assert before_text == after_text
    assert before_dream.last_light == after_dream.last_light
    assert before_dream.last_deep == after_dream.last_deep
    assert before_dream.last_rem == after_dream.last_rem
    assert RuntimeState.for_repo_root(root).get_adapter_state("dream.bootstrap") is None


def test_bootstrap_dry_run_counts_default_pass_d_cache_reuse(tmp_path: Path, monkeypatch, capsys):
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    summary_path = root / "memory" / "summaries" / "summary-yt-video-1.md"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        "---\n"
        "id: summary-yt-video-1\n"
        "type: summary\n"
        'title: "Summary"\n'
        "status: active\n"
        "created: 2026-04-01\n"
        "last_updated: 2026-04-01\n"
        "aliases: []\n"
        "tags:\n  - domain/learning\n  - function/summary\n  - signal/canon\n"
        "domains:\n  - learning\n"
        "source_date: 2026-04-01\n"
        "source_type: video\n"
        "external_id: youtube-video-1\n"
        "relates_to: []\n"
        "sources: []\n"
        "---\n\n"
        "# Summary\n\n"
        "## TL;DR\n\n"
        "Bootstrap summary body.\n",
        encoding="utf-8",
    )
    source = BootstrapSource(
        summary_id="summary-yt-video-1",
        source_kind="youtube",
        source_date="2026-04-01",
        summary_path=summary_path,
    )
    monkeypatch.setattr("mind.dream.bootstrap.enumerate_bootstrap_sources", lambda _vault: [source])
    monkeypatch.setattr("mind.dream.bootstrap.evaluate_and_persist_quality", lambda persist, report_key: {"lanes": {"youtube": {"state": "trusted"}}})
    identity = LLMCacheIdentity(
        task_class="dream",
        provider="anthropic",
        model="anthropic/claude-sonnet-4.6",
        transport="ai_gateway",
        api_family="responses",
        input_mode="text",
        prompt_version=PASS_D_PROMPT_VERSION,
        request_fingerprint={"kind": "text-prompt"},
    )
    write_llm_cache(
        root / "raw" / "transcripts" / "youtube" / "youtube-video-1.pass_d.json",
        identity=identity,
        data={"q1_matches": [], "q2_candidates": []},
    )

    assert main(["dream", "bootstrap", "--dry-run", "--limit", "1"]) == 0
    out = capsys.readouterr().out

    assert "would reuse up to 1 existing Pass D caches" in out


def test_bootstrap_resume_uses_checkpoint_and_skips_completed_sources(tmp_path: Path, monkeypatch):
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    summary_paths = [
        _write_summary(root, "summary-bootstrap-1", source_date="2026-04-01"),
        _write_summary(root, "summary-bootstrap-2", source_date="2026-04-02"),
        _write_summary(root, "summary-bootstrap-3", source_date="2026-04-03"),
    ]
    sources = [
        BootstrapSource(
            summary_id=path.stem,
            source_kind="article",
            source_date=f"2026-04-0{index + 1}",
            summary_path=path,
        )
        for index, path in enumerate(summary_paths)
    ]
    monkeypatch.setattr("mind.dream.bootstrap.enumerate_bootstrap_sources", lambda _vault: sources)

    replayed: list[str] = []
    checkpoint_calls: list[str] = []
    fail_on_third = {"enabled": True}

    def fake_replay(source: BootstrapSource, *, repo_root: Path, force_pass_d: bool, execution_date: str) -> dict[str, int | bool]:
        replayed.append(source.summary_id)
        if fail_on_third["enabled"] and source.summary_id == "summary-bootstrap-3":
            raise RuntimeError("forced interruption")
        return {
            "cache_reused": False,
            "evidence_updates": 1,
            "probationary_updates": 0,
            "missing_atoms": 0,
        }

    def fake_checkpoint(*, dry_run: bool) -> DreamResult:
        checkpoint_calls.append(f"checkpoint-{len(checkpoint_calls) + 1}")
        return DreamResult(stage="deep", dry_run=dry_run, summary=checkpoint_calls[-1])

    monkeypatch.setattr("mind.dream.bootstrap.replay_bootstrap_source", fake_replay)
    monkeypatch.setattr("mind.dream.bootstrap.run_bootstrap_checkpoint", fake_checkpoint)

    with pytest.raises(RuntimeError, match="forced interruption"):
        main(["dream", "bootstrap", "--checkpoint-every", "1"])

    state = RuntimeState.for_repo_root(root).get_adapter_state("dream.bootstrap")
    assert replayed == ["summary-bootstrap-1", "summary-bootstrap-2", "summary-bootstrap-3"]
    assert checkpoint_calls == ["checkpoint-1", "checkpoint-2"]
    assert state is not None
    assert state["status"] == "interrupted"
    assert state["completed_source_ids"] == ["summary-bootstrap-1", "summary-bootstrap-2"]

    fail_on_third["enabled"] = False
    replayed.clear()
    checkpoint_calls.clear()

    assert main(["dream", "bootstrap", "--resume"]) == 0

    state = RuntimeState.for_repo_root(root).get_adapter_state("dream.bootstrap")
    assert replayed == ["summary-bootstrap-3"]
    assert checkpoint_calls == []
    assert state is not None
    assert state["status"] == "completed"
    report_path = root / state["report_path"]
    assert report_path.exists()


def test_bootstrap_resume_advances_to_next_batch_after_completed_limited_run(tmp_path: Path, monkeypatch):
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    summary_paths = [
        _write_summary(root, f"summary-bootstrap-{index}", source_date=f"2026-04-0{index}")
        for index in range(1, 6)
    ]
    sources = [
        BootstrapSource(
            summary_id=path.stem,
            source_kind="article",
            source_date=f"2026-04-0{index + 1}",
            summary_path=path,
        )
        for index, path in enumerate(summary_paths)
    ]
    monkeypatch.setattr("mind.dream.bootstrap.enumerate_bootstrap_sources", lambda _vault: sources)

    replayed: list[str] = []

    def fake_replay(source: BootstrapSource, *, repo_root: Path, force_pass_d: bool, execution_date: str) -> dict[str, int | bool]:
        replayed.append(source.summary_id)
        return {
            "cache_reused": False,
            "evidence_updates": 1,
            "probationary_updates": 0,
            "missing_atoms": 0,
        }

    monkeypatch.setattr("mind.dream.bootstrap.replay_bootstrap_source", fake_replay)

    assert main(["dream", "bootstrap", "--limit", "2"]) == 0
    assert replayed == ["summary-bootstrap-1", "summary-bootstrap-2"]

    replayed.clear()
    assert main(["dream", "bootstrap", "--resume"]) == 0
    assert replayed == ["summary-bootstrap-3", "summary-bootstrap-4"]

    state = RuntimeState.for_repo_root(root).get_adapter_state("dream.bootstrap")
    assert state is not None
    assert state["status"] == "completed"
    assert state["planned_source_ids"] == ["summary-bootstrap-3", "summary-bootstrap-4"]
    assert state["completed_source_ids"] == ["summary-bootstrap-3", "summary-bootstrap-4"]

    replayed.clear()
    assert main(["dream", "bootstrap", "--resume"]) == 0
    assert replayed == ["summary-bootstrap-5"]


def test_bootstrap_resume_completed_batch_accepts_new_limit(tmp_path: Path, monkeypatch):
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    summary_paths = [
        _write_summary(root, f"summary-bootstrap-{index}", source_date=f"2026-04-0{index}")
        for index in range(1, 6)
    ]
    sources = [
        BootstrapSource(
            summary_id=path.stem,
            source_kind="article",
            source_date=f"2026-04-0{index + 1}",
            summary_path=path,
        )
        for index, path in enumerate(summary_paths)
    ]
    monkeypatch.setattr("mind.dream.bootstrap.enumerate_bootstrap_sources", lambda _vault: sources)

    replayed: list[str] = []

    def fake_replay(source: BootstrapSource, *, repo_root: Path, force_pass_d: bool, execution_date: str) -> dict[str, int | bool]:
        replayed.append(source.summary_id)
        return {
            "cache_reused": False,
            "evidence_updates": 1,
            "probationary_updates": 0,
            "missing_atoms": 0,
        }

    monkeypatch.setattr("mind.dream.bootstrap.replay_bootstrap_source", fake_replay)

    assert main(["dream", "bootstrap", "--limit", "2"]) == 0

    replayed.clear()
    assert main(["dream", "bootstrap", "--resume", "--limit", "3"]) == 0
    assert replayed == ["summary-bootstrap-3", "summary-bootstrap-4", "summary-bootstrap-5"]


def test_bootstrap_default_does_not_force_pass_d(tmp_path: Path, monkeypatch):
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    summary_path = _write_summary(root, "summary-bootstrap-a", source_date="2026-04-01")
    source = BootstrapSource(
        summary_id=summary_path.stem,
        source_kind="article",
        source_date="2026-04-01",
        summary_path=summary_path,
    )
    monkeypatch.setattr("mind.dream.bootstrap.enumerate_bootstrap_sources", lambda _vault: [source])

    seen: list[bool] = []

    def fake_replay(source: BootstrapSource, *, repo_root: Path, force_pass_d: bool, execution_date: str) -> dict[str, int | bool]:
        seen.append(force_pass_d)
        return {
            "cache_reused": False,
            "evidence_updates": 1,
            "probationary_updates": 0,
            "missing_atoms": 0,
        }

    monkeypatch.setattr("mind.dream.bootstrap.replay_bootstrap_source", fake_replay)

    assert main(["dream", "bootstrap", "--limit", "1"]) == 0
    assert seen == [False]

    seen.clear()
    assert main(["dream", "bootstrap", "--limit", "1", "--force-pass-d"]) == 0
    assert seen == [True]


def test_bootstrap_reports_are_unique_per_run(tmp_path: Path, monkeypatch):
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    summary_paths = [
        _write_summary(root, f"summary-bootstrap-{index}", source_date=f"2026-04-0{index}")
        for index in range(1, 3)
    ]
    sources = [
        BootstrapSource(
            summary_id=path.stem,
            source_kind="article",
            source_date=f"2026-04-0{index + 1}",
            summary_path=path,
        )
        for index, path in enumerate(summary_paths)
    ]
    monkeypatch.setattr("mind.dream.bootstrap.enumerate_bootstrap_sources", lambda _vault: sources)
    monkeypatch.setattr(
        "mind.dream.bootstrap.replay_bootstrap_source",
        lambda source, *, repo_root, force_pass_d, execution_date: {
            "cache_reused": False,
            "evidence_updates": 1,
            "probationary_updates": 0,
            "missing_atoms": 0,
        },
    )

    assert main(["dream", "bootstrap", "--limit", "1"]) == 0
    first_state = RuntimeState.for_repo_root(root).get_adapter_state("dream.bootstrap")
    assert first_state is not None
    first_report = root / first_state["report_path"]
    assert first_report.exists()

    assert main(["dream", "bootstrap", "--resume", "--limit", "1"]) == 0
    second_state = RuntimeState.for_repo_root(root).get_adapter_state("dream.bootstrap")
    assert second_state is not None
    second_report = root / second_state["report_path"]
    assert second_report.exists()

    assert first_report != second_report
    assert first_report.name != second_report.name


def test_bootstrap_continues_when_source_returns_error_payload(tmp_path: Path, monkeypatch):
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    summary_paths = [
        _write_summary(root, "summary-bootstrap-ok", source_date="2026-04-01"),
        _write_summary(root, "summary-bootstrap-error", source_date="2026-04-02"),
    ]
    sources = [
        BootstrapSource(
            summary_id=path.stem,
            source_kind="article",
            source_date=f"2026-04-0{index + 1}",
            summary_path=path,
        )
        for index, path in enumerate(summary_paths)
    ]
    monkeypatch.setattr("mind.dream.bootstrap.enumerate_bootstrap_sources", lambda _vault: sources)

    def fake_replay(source: BootstrapSource, *, repo_root: Path, force_pass_d: bool, execution_date: str) -> dict[str, int | bool | str | list[str]]:
        if source.summary_id == "summary-bootstrap-error":
            return {
                "cache_reused": False,
                "evidence_updates": 0,
                "probationary_updates": 0,
                "missing_atoms": [],
                "error": "KeyError: unsupported atom type",
                "error_stage": "pass_d.dispatch",
            }
        return {
            "cache_reused": False,
            "evidence_updates": 1,
            "probationary_updates": 0,
            "missing_atoms": [],
        }

    monkeypatch.setattr("mind.dream.bootstrap.replay_bootstrap_source", fake_replay)

    assert main(["dream", "bootstrap", "--limit", "2"]) == 0
    state = RuntimeState.for_repo_root(root).get_adapter_state("dream.bootstrap")
    assert state is not None
    assert state["status"] == "completed"
    assert state["completed_source_ids"] == ["summary-bootstrap-error", "summary-bootstrap-ok"]


def test_bootstrap_report_splits_trusted_and_degraded_updates(tmp_path: Path, monkeypatch):
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    summary_paths = [
        _write_summary(root, "summary-bootstrap-trusted", source_date="2026-04-01"),
        _write_summary(root, "summary-bootstrap-degraded", source_date="2026-04-02"),
    ]
    sources = [
        BootstrapSource(
            summary_id=path.stem,
            source_kind="article",
            source_date=f"2026-04-0{index + 1}",
            summary_path=path,
        )
        for index, path in enumerate(summary_paths)
    ]
    monkeypatch.setattr("mind.dream.bootstrap.enumerate_bootstrap_sources", lambda _vault: sources)
    monkeypatch.setattr(
        "mind.dream.bootstrap.evaluate_and_persist_quality",
        lambda persist, report_key: {"report_path": "raw/reports/dream/quality/fake.md"},
    )
    monkeypatch.setattr(
        "mind.dream.bootstrap.lane_state_for_frontmatter",
        lambda frontmatter, _quality: "trusted" if frontmatter.get("id") == "summary-bootstrap-trusted" else "partial-fidelity",
    )
    monkeypatch.setattr(
        "mind.dream.bootstrap.replay_bootstrap_source",
        lambda source, *, repo_root, force_pass_d, execution_date: {
            "cache_reused": False,
            "evidence_updates": 2 if source.summary_id == "summary-bootstrap-trusted" else 1,
            "probationary_updates": 1 if source.summary_id == "summary-bootstrap-degraded" else 0,
            "missing_atoms": 0,
        },
    )

    assert main(["dream", "bootstrap"]) == 0

    state = RuntimeState.for_repo_root(root).get_adapter_state("dream.bootstrap")
    assert state is not None
    report = (root / state["report_path"]).read_text(encoding="utf-8")
    assert "Trusted evidence updates: 2" in report
    assert "Degraded evidence updates: 1" in report
    assert "Trusted probationary updates: 0" in report
    assert "Degraded probationary updates: 1" in report


def test_bootstrap_checkpoint_does_not_advance_deep_cadence(tmp_path: Path, monkeypatch):
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    state = RuntimeState.for_repo_root(root)
    before = state.get_dream_state()

    result = run_bootstrap_checkpoint(dry_run=False)

    after = state.get_dream_state()
    assert result.stage == "deep"
    assert before.last_deep == after.last_deep
    assert before.light_passes_since_deep == after.light_passes_since_deep
    assert before.deep_passes_since_rem == after.deep_passes_since_rem
