from __future__ import annotations

import shutil
from pathlib import Path

import pytest

import mind.dream.campaign as campaign_module
from mind.dream.campaign import CAMPAIGN_ADAPTER, _build_schedule, run_campaign
from mind.dream.common import DreamPreconditionError
from mind.runtime_state import RuntimeState
from scripts.common.config import DreamCampaignConfig
from tests.paths import EXAMPLES_ROOT


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


def _daily_reports(root: Path, adapter: dict[str, object]) -> list[Path]:
    plan_path = root / str(adapter["plan_path"])
    return sorted((plan_path.parent / "daily").glob("*.md"))


def _rem_pages(root: Path) -> list[str]:
    return sorted(path.name for path in (root / "memory" / "dreams" / "rem").glob("*.md"))


def test_campaign_schedule_uses_calendar_month_rem_and_not_day_modulo() -> None:
    schedule = _build_schedule(
        start_date="2026-01-01",
        days=31,
        config=DreamCampaignConfig(),
    )

    rem_days = [day.effective_date for day in schedule if "rem" in day.stages]

    assert rem_days == ["2026-01-01"]


def test_campaign_schedule_clamps_month_end_when_stepping_rem_dates() -> None:
    schedule = _build_schedule(
        start_date="2026-01-31",
        days=60,
        config=DreamCampaignConfig(),
    )

    rem_days = [day.effective_date for day in schedule if "rem" in day.stages]

    assert rem_days == ["2026-01-31", "2026-02-28", "2026-03-28"]


def test_campaign_dry_run_renders_projected_counts(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)

    result = run_campaign(
        days=35,
        start_date="2026-01-01",
        dry_run=True,
        resume=False,
        profile="aggressive",
    )

    assert "Campaign rehearsal planned for 35 simulated days" in result.summary
    assert "light=35" in result.summary
    assert "deep=5" in result.summary
    assert "rem=2" in result.summary
    assert "kene=0" in result.summary
    assert "weave=" not in result.summary
    assert any("2026-01-01: light, deep, rem" in item for item in result.mutations)


def test_campaign_can_append_kene_after_rem_when_config_enabled(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    cfg = root / "config.yaml"
    cfg.write_text(
        cfg.read_text(encoding="utf-8").replace(
            "  probationary_stale_warn_days: 90\n",
            "  probationary_stale_warn_days: 90\n"
            "  v2:\n"
            "    kene:\n"
            "      campaign_enabled: true\n",
        ),
        encoding="utf-8",
    )
    stage_calls: list[str] = []

    def fake_run_stage(*, stage: str, context, progress_callback=None):
        stage_calls.append(stage)
        return campaign_module.DreamResult(stage=stage, dry_run=stage == "kene", summary=f"{stage} complete")

    monkeypatch.setattr("mind.dream.campaign._run_stage", fake_run_stage)

    result = run_campaign(
        days=1,
        start_date="2026-01-01",
        dry_run=False,
        resume=False,
        profile="aggressive",
    )

    assert "light=1 deep=1 rem=1 kene=1" in result.summary
    assert stage_calls == ["light", "deep", "rem", "kene"]
    adapter = RuntimeState.for_repo_root(root).get_adapter_state(CAMPAIGN_ADAPTER)
    assert adapter is not None
    assert adapter["completed_counts"] == {"light": 1, "deep": 1, "rem": 1, "kene": 1}
    report_text = _daily_reports(root, adapter)[0].read_text(encoding="utf-8")
    assert "Scheduled stages: light, deep, rem, kene" in report_text
    assert "## Ken\u00e9" in report_text


def test_campaign_dry_run_render_avoids_nested_preview_bullets_for_short_windows(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)

    result = run_campaign(
        days=8,
        start_date="2026-01-01",
        dry_run=True,
        resume=False,
        profile="aggressive",
    )

    rendered = result.render()
    assert "- Preview:" in rendered
    assert "- 2026-01-08: light, deep" in rendered
    assert "- - 2026-01-01: light, deep, rem" not in rendered
    assert "... 3 more simulated day(s)" not in rendered


def test_campaign_yearly_profile_uses_quieter_deep_cadence(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)

    result = run_campaign(
        days=35,
        start_date="2026-01-01",
        dry_run=True,
        resume=False,
        profile="yearly",
    )

    assert "light=35" in result.summary
    assert "deep=3" in result.summary
    assert "rem=2" in result.summary
    assert "kene=0" in result.summary
    assert "weave=" not in result.summary


def test_campaign_yearly_profile_suppresses_operator_nudges(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    cfg = root / "config.yaml"
    cfg.write_text(
        cfg.read_text(encoding="utf-8").replace(
            "working_set_cap: 300",
            "working_set_cap: 1",
        ),
        encoding="utf-8",
    )
    (root / "memory" / "summaries" / "summary-yearly-quiet.md").write_text(
        "---\n"
        "id: summary-yearly-quiet\n"
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
        "concepts:\n  - \"[[local-first-systems]]\"\n"
        "---\n\n"
        "# Summary\n\nThis source revisits [[user-owned-ai]] and however challenges the stance.\n",
        encoding="utf-8",
    )
    (root / "memory" / "concepts").mkdir(parents=True, exist_ok=True)
    (root / "memory" / "concepts" / "cap-overflow.md").write_text(
        "---\n"
        "id: cap-overflow\n"
        "type: concept\n"
        'title: "Cap Overflow"\n'
        "status: active\n"
        "created: 2026-04-08\n"
        "last_updated: 2026-02-01\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/note\n  - signal/working\n"
        "domains:\n  - archive\n"
        "relates_to: []\n"
        "sources: []\n"
        "lifecycle_state: active\n"
        "last_evidence_date: 2026-02-01\n"
        "evidence_count: 0\n"
        "---\n\n"
        "# Cap Overflow\n\n## TL;DR\n\ncap overflow\n\n## Evidence log\n\n",
        encoding="utf-8",
    )

    result = run_campaign(
        days=1,
        start_date="2026-01-01",
        dry_run=False,
        resume=False,
        profile="yearly",
    )

    assert "light=1 deep=1 rem=1 kene=0" in result.summary
    assert "weave=" not in result.summary
    nudge_dir = root / "memory" / "inbox" / "nudges"
    assert not any(path.name.startswith("2026-01-01-") for path in nudge_dir.glob("*.md"))
    adapter = RuntimeState.for_repo_root(root).get_adapter_state(CAMPAIGN_ADAPTER)
    assert adapter is not None
    assert adapter["status"] == "completed"
    plan_path = root / str(adapter["plan_path"])
    daily_report = plan_path.parent / "daily" / "2026-01-01.md"
    report_text = daily_report.read_text(encoding="utf-8")
    assert "cap signals" in report_text
    assert "polarity signals" in report_text
    assert "0 lifecycle updates" in report_text
    assert "## Weave" not in report_text
    assert "tail-rescan appended evidence" not in report_text


def test_campaign_fast_forward_skips_unchanged_yearly_light_inputs(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    cfg = root / "config.yaml"
    cfg.write_text(
        cfg.read_text(encoding="utf-8").replace(
            "  probationary_stale_warn_days: 90\n",
            "  probationary_stale_warn_days: 90\n"
            "  campaign:\n"
            "    yearly:\n"
            "      fast_forward_skip_unchanged_light: true\n",
        ),
        encoding="utf-8",
    )
    stage_calls: list[str] = []

    def fake_run_stage(*, stage: str, context, progress_callback=None):
        stage_calls.append(stage)
        return campaign_module.DreamResult(stage=stage, dry_run=False, summary=f"{stage} complete")

    monkeypatch.setattr("mind.dream.campaign._run_stage", fake_run_stage)
    monkeypatch.setattr(
        "mind.dream.campaign._light_fast_forward_fingerprint",
        lambda _v, *, config, refresh_atom_cache=False: "stable",
    )

    result = run_campaign(
        days=3,
        start_date="2026-01-01",
        dry_run=False,
        resume=False,
        profile="yearly",
    )

    assert "light=3 deep=1 rem=1 kene=0" in result.summary
    assert stage_calls == ["light", "deep", "rem"]
    state = RuntimeState.for_repo_root(root)
    adapter = state.get_adapter_state(CAMPAIGN_ADAPTER)
    assert adapter is not None
    assert adapter["completed_counts"] == {"light": 3, "deep": 1, "rem": 1, "kene": 0}
    reports = _daily_reports(root, adapter)
    assert "Light Dream skipped because campaign fast-forward inputs were unchanged" in reports[1].read_text(encoding="utf-8")
    with state.connect() as conn:
        skipped = conn.execute(
            "SELECT count(*) AS count FROM run_events WHERE event_type = 'stage-skipped'"
        ).fetchone()
    assert skipped["count"] == 2


def test_campaign_live_35_day_rehearsal_writes_real_reports_and_monthly_outputs(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)

    result = run_campaign(
        days=35,
        start_date="2026-01-01",
        dry_run=False,
        resume=False,
        profile="aggressive",
    )

    assert "Campaign processed 35 simulated days" in result.summary
    state = RuntimeState.for_repo_root(root)
    adapter = state.get_adapter_state(CAMPAIGN_ADAPTER)
    assert adapter is not None
    assert adapter["status"] == "completed"
    assert adapter["completed_counts"] == {"light": 35, "deep": 5, "rem": 2, "kene": 0}
    assert adapter["config_snapshot"]
    assert adapter["schedule"]
    reports = _daily_reports(root, adapter)
    assert len(reports) == 35
    assert "Light Dream processed 1 source pages" in (reports[1]).read_text(encoding="utf-8")
    assert "Light Dream processed 1 source pages" in (reports[-1]).read_text(encoding="utf-8")
    assert "## Weave" not in reports[0].read_text(encoding="utf-8")
    assert "## Weave" not in reports[1].read_text(encoding="utf-8")
    assert _rem_pages(root) == ["2026-01.md", "2026-02.md"]
    with state.connect() as conn:
        progress_messages = [
            str(row["message"])
            for row in conn.execute(
                """
                SELECT message
                FROM run_events
                WHERE stage = 'light' AND event_type = 'progress'
                ORDER BY id
                """
            ).fetchall()
        ]
    assert progress_messages
    assert progress_messages[0] == "processed 1/1 source pages"
    dream_state = state.get_dream_state()
    assert dream_state.last_light == "2026-02-04"
    assert dream_state.last_deep == "2026-01-29"
    assert dream_state.last_rem == "2026-02-01"


def test_campaign_base_exception_marks_runtime_and_adapter_interrupted(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)

    class ForcedInterrupt(BaseException):
        pass

    def interrupting_run_stage(*, stage: str, context, progress_callback=None):
        if stage == "light":
            raise ForcedInterrupt
        return campaign_module._run_stage(stage=stage, context=context, progress_callback=progress_callback)

    monkeypatch.setattr("mind.dream.campaign._run_stage", interrupting_run_stage)

    with pytest.raises(ForcedInterrupt):
        run_campaign(
            days=2,
            start_date="2026-01-01",
            dry_run=False,
            resume=False,
            profile="aggressive",
        )

    state = RuntimeState.for_repo_root(root)
    interrupted = state.get_adapter_state(CAMPAIGN_ADAPTER)
    assert interrupted is not None
    assert interrupted["status"] == "interrupted"
    assert interrupted["last_error"] == "ForcedInterrupt"
    with state.connect() as conn:
        run_statuses = {
            (str(row["kind"]), str(row["status"]), str(row["notes"] or ""))
            for row in conn.execute(
                """
                SELECT kind, status, notes
                FROM runs
                WHERE kind IN ('dream.campaign', 'dream.light')
                ORDER BY id
                """
            ).fetchall()
        }
    assert ("dream.campaign", "failed", "ForcedInterrupt") in run_statuses
    assert all(status != "running" for _kind, status, _notes in run_statuses)


def test_campaign_resume_fails_fast_on_config_drift_against_persisted_snapshot(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    real_run_stage = campaign_module._run_stage
    fault_triggered = False

    class ForcedInterrupt(RuntimeError):
        pass

    def interrupting_run_stage(*, stage: str, context, progress_callback=None):
        nonlocal fault_triggered
        if stage == "light" and context.effective_date == "2026-01-02" and not fault_triggered:
            fault_triggered = True
            raise ForcedInterrupt("stop after first campaign day")
        return real_run_stage(stage=stage, context=context, progress_callback=progress_callback)

    monkeypatch.setattr("mind.dream.campaign._run_stage", interrupting_run_stage)

    with pytest.raises(ForcedInterrupt):
        run_campaign(
            days=8,
            start_date="2026-01-01",
            dry_run=False,
            resume=False,
            profile="aggressive",
        )

    cfg = root / "config.yaml"
    cfg.write_text(
        cfg.read_text(encoding="utf-8").replace(
            "  probationary_stale_warn_days: 90\n",
            "  probationary_stale_warn_days: 90\n"
            "  campaign:\n"
            "    deep_interval_days: 3\n",
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("mind.dream.campaign._run_stage", real_run_stage)

    with pytest.raises(DreamPreconditionError, match="deep_interval_days"):
        run_campaign(
            days=8,
            start_date="2026-01-01",
            dry_run=False,
            resume=True,
            profile="aggressive",
        )


def test_campaign_live_95_day_rehearsal_resumes_against_persisted_schedule(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    real_run_stage = campaign_module._run_stage
    rem_runs = 0
    fault_triggered = False

    class ForcedInterrupt(RuntimeError):
        pass

    def interrupting_run_stage(*, stage: str, context, progress_callback=None):
        nonlocal rem_runs, fault_triggered
        if stage == "light" and rem_runs >= 2 and not fault_triggered:
            fault_triggered = True
            raise ForcedInterrupt("interrupt after the second monthly REM pass")
        result = real_run_stage(stage=stage, context=context, progress_callback=progress_callback)
        if stage == "rem":
            rem_runs += 1
        return result

    monkeypatch.setattr("mind.dream.campaign._run_stage", interrupting_run_stage)

    with pytest.raises(ForcedInterrupt):
        run_campaign(
            days=95,
            start_date="2026-01-01",
            dry_run=False,
            resume=False,
            profile="aggressive",
        )

    state = RuntimeState.for_repo_root(root)
    interrupted = state.get_adapter_state(CAMPAIGN_ADAPTER)
    assert interrupted is not None
    assert interrupted["status"] == "interrupted"
    assert interrupted["completed_counts"] == {"light": 32, "deep": 5, "rem": 2, "kene": 0}
    persisted_schedule = interrupted["schedule"]

    monkeypatch.setattr("mind.dream.campaign._run_stage", real_run_stage)

    result = run_campaign(
        days=95,
        start_date="2026-01-01",
        dry_run=False,
        resume=True,
        profile="aggressive",
    )

    assert "Campaign processed 95 simulated days" in result.summary
    completed = state.get_adapter_state(CAMPAIGN_ADAPTER)
    assert completed is not None
    assert completed["status"] == "completed"
    assert completed["completed_counts"] == {"light": 95, "deep": 14, "rem": 4, "kene": 0}
    assert completed["schedule"] == persisted_schedule
    assert len(_daily_reports(root, completed)) == 95
    assert _rem_pages(root) == ["2026-01.md", "2026-02.md", "2026-03.md", "2026-04.md"]
    dream_state = state.get_dream_state()
    assert dream_state.last_light == "2026-04-05"
    assert dream_state.last_deep == "2026-04-02"
    assert dream_state.last_rem == "2026-04-01"


def test_campaign_resume_restarts_light_from_checkpointed_source_index(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    call_log: list[tuple[str, int]] = []
    interrupted = False

    class ForcedInterrupt(RuntimeError):
        pass

    def fake_run_stage(*, stage: str, context, progress_callback=None):
        nonlocal interrupted
        if stage == "light" and not interrupted:
            interrupted = True
            assert progress_callback is not None
            progress_callback(
                {
                    "processed_sources": 2,
                    "total_sources": 5,
                    "last_source_id": "summary-checkpoint-02",
                }
            )
            raise ForcedInterrupt("interrupt mid-light")
        call_log.append((stage, context.campaign_resume_from_source_index))
        return campaign_module.DreamResult(stage=stage, dry_run=False, summary=f"{stage} complete")

    monkeypatch.setattr("mind.dream.campaign._run_stage", fake_run_stage)

    with pytest.raises(ForcedInterrupt):
        run_campaign(
            days=1,
            start_date="2026-01-01",
            dry_run=False,
            resume=False,
            profile="yearly",
        )

    state = RuntimeState.for_repo_root(root)
    adapter = state.get_adapter_state(CAMPAIGN_ADAPTER)
    assert adapter is not None
    assert adapter["status"] == "interrupted"
    assert adapter["inflight_stage"] == "light"
    assert adapter["stage_progress"]["processed_sources"] == 2
    assert adapter["stage_progress"]["total_sources"] == 5

    result = run_campaign(
        days=1,
        start_date="2026-01-01",
        dry_run=False,
        resume=True,
        profile="yearly",
    )

    assert "light=1 deep=1 rem=1 kene=0" in result.summary
    assert "weave=" not in result.summary
    assert call_log[0] == ("light", 2)
    assert ("deep", 0) in call_log
    assert ("rem", 0) in call_log
