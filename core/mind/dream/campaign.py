from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from scripts.common.frontmatter import today_str

from .common import (
    DreamExecutionContext,
    DreamPreconditionError,
    DreamResult,
    _exception_message,
    dream_run,
    ensure_dream_enabled,
    ensure_onboarded,
    maybe_locked,
    runtime_state,
    vault,
)
from .v2.runtime import run_dream_v2_stage
from .v2.weave_stage import run_weave_v2_shadow

CAMPAIGN_ADAPTER = "dream.campaign"
CAMPAIGN_REPORT_ROOT = ("raw", "reports", "dream", "campaign")
STAGE_ORDER = ("light", "deep", "rem", "weave")
CONFIG_SNAPSHOT_KEYS = (
    "light_interval_days",
    "deep_interval_days",
    "rem_interval_days",
    "weave_enabled",
    "weave_run_after_rem",
    "light_working_set_cap",
    "deep_probationary_cap",
    "deep_progress_every_probationaries",
    "deep_active_synthesis_max_atoms_per_run",
    "deep_active_synthesis_cooldown_days",
    "deep_external_grounding_max_atoms_per_run",
    "deep_external_grounding_cooldown_days",
    "rem_hotset_cap",
    "rem_cluster_limit",
    "rem_candidate_multiplier",
    "lane_relaxation_mode",
    "rem_decline_after_weak_months",
    "rem_archive_after_weak_months",
    "apply_cap_miss_lifecycle_changes",
    "write_audit_nudges",
    "emit_verbose_mutations",
    "checkpoint_every_sources",
)
SUPPORTED_PROFILES = ("aggressive", "yearly")


@dataclass(frozen=True)
class CampaignDayPlan:
    day_index: int
    effective_date: str
    stages: tuple[str, ...]


@dataclass(frozen=True)
class CampaignResolvedConfig:
    light_interval_days: int
    deep_interval_days: int
    rem_interval_days: int
    weave_enabled: bool
    weave_run_after_rem: bool
    light_working_set_cap: int
    deep_probationary_cap: int
    deep_progress_every_probationaries: int
    deep_active_synthesis_max_atoms_per_run: int
    deep_active_synthesis_cooldown_days: int
    deep_external_grounding_max_atoms_per_run: int
    deep_external_grounding_cooldown_days: int
    rem_hotset_cap: int
    rem_cluster_limit: int
    rem_candidate_multiplier: int
    lane_relaxation_mode: str
    rem_decline_after_weak_months: int
    rem_archive_after_weak_months: int
    apply_cap_miss_lifecycle_changes: bool
    write_audit_nudges: bool
    emit_verbose_mutations: bool
    checkpoint_every_sources: int

    def snapshot(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in CONFIG_SNAPSHOT_KEYS}


def _resolve_campaign_config(*, raw_config, dream_config, profile: str) -> CampaignResolvedConfig:
    if profile not in SUPPORTED_PROFILES:
        raise DreamPreconditionError(
            "mind dream campaign: unsupported profile "
            f"{profile!r}; expected one of {', '.join(SUPPORTED_PROFILES)}"
        )

    resolved = CampaignResolvedConfig(
        light_interval_days=int(raw_config.light_interval_days),
        deep_interval_days=int(raw_config.deep_interval_days),
        rem_interval_days=int(raw_config.rem_interval_days),
        weave_enabled=bool(dream_config.weave.enabled and profile == "yearly"),
        weave_run_after_rem=bool(dream_config.weave.run_after_rem and profile == "yearly"),
        light_working_set_cap=int(raw_config.light_working_set_cap),
        deep_probationary_cap=int(raw_config.deep_probationary_cap),
        deep_progress_every_probationaries=int(raw_config.deep_progress_every_probationaries),
        deep_active_synthesis_max_atoms_per_run=int(raw_config.deep_active_synthesis_max_atoms_per_run),
        deep_active_synthesis_cooldown_days=int(raw_config.deep_active_synthesis_cooldown_days),
        deep_external_grounding_max_atoms_per_run=int(raw_config.deep_external_grounding_max_atoms_per_run),
        deep_external_grounding_cooldown_days=int(raw_config.deep_external_grounding_cooldown_days),
        rem_hotset_cap=int(raw_config.rem_hotset_cap),
        rem_cluster_limit=int(raw_config.rem_cluster_limit),
        rem_candidate_multiplier=int(raw_config.rem_candidate_multiplier),
        lane_relaxation_mode=str(raw_config.lane_relaxation_mode),
        rem_decline_after_weak_months=int(raw_config.rem_decline_after_weak_months),
        rem_archive_after_weak_months=int(raw_config.rem_archive_after_weak_months),
        apply_cap_miss_lifecycle_changes=bool(raw_config.apply_cap_miss_lifecycle_changes),
        write_audit_nudges=bool(raw_config.write_audit_nudges),
        emit_verbose_mutations=bool(raw_config.emit_verbose_mutations),
        checkpoint_every_sources=int(raw_config.checkpoint_every_sources),
    )
    if profile != "yearly":
        return resolved

    yearly = raw_config.yearly

    def override(name: str, current: Any) -> Any:
        value = getattr(yearly, name)
        if value is None:
            return current
        return value

    return CampaignResolvedConfig(
        light_interval_days=int(override("light_interval_days", resolved.light_interval_days)),
        deep_interval_days=int(override("deep_interval_days", resolved.deep_interval_days)),
        rem_interval_days=int(override("rem_interval_days", resolved.rem_interval_days)),
        weave_enabled=resolved.weave_enabled,
        weave_run_after_rem=resolved.weave_run_after_rem,
        light_working_set_cap=int(override("light_working_set_cap", resolved.light_working_set_cap)),
        deep_probationary_cap=int(override("deep_probationary_cap", resolved.deep_probationary_cap)),
        deep_progress_every_probationaries=int(
            override("deep_progress_every_probationaries", resolved.deep_progress_every_probationaries)
        ),
        deep_active_synthesis_max_atoms_per_run=int(
            override("deep_active_synthesis_max_atoms_per_run", dream_config.active_synthesis.max_atoms_per_run)
        ),
        deep_active_synthesis_cooldown_days=int(
            override("deep_active_synthesis_cooldown_days", dream_config.active_synthesis.cooldown_days)
        ),
        deep_external_grounding_max_atoms_per_run=int(
            override("deep_external_grounding_max_atoms_per_run", dream_config.external_grounding.max_atoms_per_run)
        ),
        deep_external_grounding_cooldown_days=int(
            override("deep_external_grounding_cooldown_days", dream_config.external_grounding.cooldown_days)
        ),
        rem_hotset_cap=int(override("rem_hotset_cap", dream_config.rem_hotset_cap)),
        rem_cluster_limit=int(override("rem_cluster_limit", dream_config.rem_cluster_limit)),
        rem_candidate_multiplier=int(override("rem_candidate_multiplier", 3)),
        lane_relaxation_mode=str(override("lane_relaxation_mode", "strict")),
        rem_decline_after_weak_months=int(
            override("rem_decline_after_weak_months", resolved.rem_decline_after_weak_months)
        ),
        rem_archive_after_weak_months=int(
            override("rem_archive_after_weak_months", resolved.rem_archive_after_weak_months)
        ),
        apply_cap_miss_lifecycle_changes=bool(
            override("apply_cap_miss_lifecycle_changes", resolved.apply_cap_miss_lifecycle_changes)
        ),
        write_audit_nudges=bool(override("write_audit_nudges", False)),
        emit_verbose_mutations=bool(override("emit_verbose_mutations", False)),
        checkpoint_every_sources=max(1, int(override("checkpoint_every_sources", resolved.checkpoint_every_sources))),
    )


def _parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise DreamPreconditionError(f"mind dream campaign: invalid date {value!r}; expected YYYY-MM-DD") from exc


def _next_date(value: str | None) -> str | None:
    if value is None:
        return None
    return (_parse_iso_date(value) + timedelta(days=1)).isoformat()


def _campaign_run_id(*, start_date: str, profile: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{start_date}-{profile}-{stamp}"


def _stage_due(day_index: int, interval_days: int) -> bool:
    if interval_days <= 0:
        raise DreamPreconditionError("mind dream campaign: interval days must be positive")
    return day_index % interval_days == 0


def _add_month_clamped(value: date) -> date:
    next_month = value.month + 1
    next_year = value.year
    if next_month > 12:
        next_month = 1
        next_year += 1
    next_day = min(value.day, monthrange(next_year, next_month)[1])
    return date(next_year, next_month, next_day)


def _monthly_rem_dates(*, start: date, end: date) -> set[str]:
    dates = {start.isoformat()}
    current = start
    while True:
        current = _add_month_clamped(current)
        if current > end:
            break
        dates.add(current.isoformat())
    return dates


def _build_schedule(*, start_date: str, days: int, config) -> list[CampaignDayPlan]:
    if days <= 0:
        raise DreamPreconditionError("mind dream campaign: --days must be greater than 0")
    start = _parse_iso_date(start_date)
    end = start + timedelta(days=days - 1)
    rem_dates = _monthly_rem_dates(start=start, end=end)
    weave_enabled = bool(getattr(config, "weave_enabled", False))
    weave_run_after_rem = bool(getattr(config, "weave_run_after_rem", False))
    schedule: list[CampaignDayPlan] = []
    for day_index in range(days):
        effective_date = (start + timedelta(days=day_index)).isoformat()
        stages: list[str] = []
        if _stage_due(day_index, int(config.light_interval_days)):
            stages.append("light")
        if _stage_due(day_index, int(config.deep_interval_days)):
            stages.append("deep")
        if effective_date in rem_dates:
            stages.append("rem")
            if weave_enabled and weave_run_after_rem:
                stages.append("weave")
        schedule.append(CampaignDayPlan(day_index=day_index, effective_date=effective_date, stages=tuple(stages)))
    return schedule


def _projected_counts(schedule: Iterable[CampaignDayPlan]) -> dict[str, int]:
    counts = {stage: 0 for stage in STAGE_ORDER}
    for day in schedule:
        for stage in day.stages:
            counts[stage] += 1
    return counts


def _campaign_config_snapshot(config: CampaignResolvedConfig) -> dict[str, Any]:
    return config.snapshot()


def _snapshot_mismatch_keys(*, current: dict[str, Any], expected: dict[str, Any]) -> list[str]:
    mismatches: list[str] = []
    for key in CONFIG_SNAPSHOT_KEYS:
        if current.get(key) != expected.get(key):
            mismatches.append(key)
    return mismatches


def _serialize_schedule(schedule: Iterable[CampaignDayPlan]) -> list[dict[str, Any]]:
    return [
        {
            "day_index": day.day_index,
            "effective_date": day.effective_date,
            "stages": list(day.stages),
        }
        for day in schedule
    ]


def _deserialize_schedule(payload: object) -> list[CampaignDayPlan]:
    if not isinstance(payload, list) or not payload:
        raise DreamPreconditionError("mind dream campaign --resume: resumable state is missing the original schedule")
    schedule: list[CampaignDayPlan] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise DreamPreconditionError(
                f"mind dream campaign --resume: malformed schedule entry at index {index}"
            )
        schedule.append(
            CampaignDayPlan(
                day_index=int(item.get("day_index", index)),
                effective_date=str(item.get("effective_date") or ""),
                stages=tuple(str(stage) for stage in (item.get("stages") or [])),
            )
        )
    return schedule


def _report_dir(v, *, run_id: str) -> Path:
    return v.root.joinpath(*CAMPAIGN_REPORT_ROOT, run_id)


def _write_plan_report(
    v,
    *,
    run_id: str,
    profile: str,
    start_date: str,
    end_date: str,
    schedule: list[CampaignDayPlan],
    projected_counts: dict[str, int],
) -> Path:
    report_dir = _report_dir(v, run_id=run_id)
    report_dir.mkdir(parents=True, exist_ok=True)
    target = report_dir / "plan.md"
    lines = [
        "# Dream Campaign Plan",
        "",
        f"- Run id: `{run_id}`",
        f"- Profile: `{profile}`",
        f"- Start date: {start_date}",
        f"- End date: {end_date}",
        f"- Simulated days: {len(schedule)}",
        f"- Projected Light runs: {projected_counts['light']}",
        f"- Projected Deep runs: {projected_counts['deep']}",
        f"- Projected REM runs: {projected_counts['rem']}",
        f"- Projected Weave runs: {projected_counts['weave']}",
        "",
        "## Schedule",
        "",
    ]
    for day in schedule:
        labels = ", ".join(day.stages) if day.stages else "idle"
        lines.append(f"- {day.effective_date}: {labels}")
    target.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return target


def _write_daily_report(
    v,
    *,
    run_id: str,
    day: CampaignDayPlan,
    completed_counts: dict[str, int],
    stage_results: list[DreamResult],
    resumed_from_stage: str | None = None,
) -> Path:
    daily_dir = _report_dir(v, run_id=run_id) / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)
    target = daily_dir / f"{day.effective_date}.md"
    lines = [
        "# Dream Campaign Daily Report",
        "",
        f"- Run id: `{run_id}`",
        f"- Effective date: {day.effective_date}",
        f"- Scheduled stages: {', '.join(day.stages) if day.stages else 'idle'}",
        f"- Completed Light runs: {completed_counts['light']}",
        f"- Completed Deep runs: {completed_counts['deep']}",
        f"- Completed REM runs: {completed_counts['rem']}",
        f"- Completed Weave runs: {completed_counts['weave']}",
        "",
    ]
    if resumed_from_stage and not stage_results:
        lines.extend(
            [
                "## Resume state",
                "",
                f"- Prior attempt already completed through `{resumed_from_stage}` for this simulated date.",
                "",
            ]
        )
    if not day.stages:
        lines.extend(["## Activity", "", "- No Dream stages were scheduled for this simulated date.", ""])
    for result in stage_results:
        lines.extend([f"## {result.stage.title()}", "", result.render(), ""])
    target.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return target


def _campaign_context(
    *,
    effective_date: str,
    run_id: str,
    profile: str,
    config: CampaignResolvedConfig,
    resume_from_source_index: int = 0,
) -> DreamExecutionContext:
    return DreamExecutionContext(
        effective_date=effective_date,
        mode="campaign",
        lane_relaxation_mode=str(config.lane_relaxation_mode),
        campaign_run_id=run_id,
        campaign_profile=profile,
        campaign_settings=config.snapshot(),
        campaign_resume_from_source_index=max(0, int(resume_from_source_index)),
        write_digest=True,
        write_rem_page=True,
    )


def _run_stage(
    *,
    stage: str,
    context: DreamExecutionContext,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> DreamResult:
    return run_dream_v2_stage(
        stage=stage,
        dry_run=False,
        acquire_lock=False,
        context=context,
        progress_callback=progress_callback,
    )


def _progress_payload(
    *,
    run_id: str,
    profile: str,
    start_date: str,
    end_date: str,
    total_days: int,
    config_snapshot: dict[str, Any],
    schedule: list[CampaignDayPlan],
    projected_counts: dict[str, int],
) -> dict[str, object]:
    return {
        "run_id": run_id,
        "status": "running",
        "start_date": start_date,
        "current_effective_date": start_date,
        "end_date": end_date,
        "profile": profile,
        "total_days": total_days,
        "config_snapshot": dict(config_snapshot),
        "schedule": _serialize_schedule(schedule),
        "days_completed": 0,
        "last_completed_stage": None,
        "projected_counts": dict(projected_counts),
        "completed_counts": {stage: 0 for stage in STAGE_ORDER},
        "plan_path": None,
        "inflight_stage": None,
        "stage_progress": None,
    }


def _render_preview(schedule: list[CampaignDayPlan], *, projected_counts: dict[str, int]) -> list[str]:
    lines = [
        f"Projected Light runs: {projected_counts['light']}",
        f"Projected Deep runs: {projected_counts['deep']}",
        f"Projected REM runs: {projected_counts['rem']}",
        f"Projected Weave runs: {projected_counts['weave']}",
        "Preview:",
    ]
    preview_days = schedule if len(schedule) <= 8 else schedule[:5]
    trailing_days = schedule[-3:] if len(schedule) > 8 else []
    rendered = set()
    for day in [*preview_days, *trailing_days]:
        if day.day_index in rendered:
            continue
        rendered.add(day.day_index)
        labels = ", ".join(day.stages) if day.stages else "idle"
        lines.append(f"{day.effective_date}: {labels}")
    if len(schedule) > len(rendered):
        lines.append(f"... {len(schedule) - len(rendered)} more simulated day(s)")
    return lines


def run_campaign(
    *,
    days: int,
    start_date: str | None,
    dry_run: bool,
    resume: bool,
    profile: str,
) -> DreamResult:
    ensure_dream_enabled()
    ensure_onboarded()

    v = vault()
    state = runtime_state()
    progress = state.get_adapter_state(CAMPAIGN_ADAPTER)
    raw_campaign_cfg = v.config.dream.campaign

    if resume:
        if not progress:
            raise DreamPreconditionError("mind dream campaign --resume: no resumable campaign state found")
        if str(progress.get("status") or "") == "completed":
            raise DreamPreconditionError("mind dream campaign --resume: campaign already completed")
        start_date = str(progress.get("start_date") or "")
        days = int(progress.get("total_days") or days)
        profile = str(progress.get("profile") or profile)
        run_id = str(progress.get("run_id") or "")
        if not run_id:
            raise DreamPreconditionError("mind dream campaign --resume: resumable state is missing a run id")
        persisted_config_snapshot = progress.get("config_snapshot")
        if not isinstance(persisted_config_snapshot, dict):
            raise DreamPreconditionError("mind dream campaign --resume: resumable state is missing the original config snapshot")
        campaign_cfg = _resolve_campaign_config(
            raw_config=raw_campaign_cfg,
            dream_config=v.config.dream,
            profile=profile,
        )
        current_config_snapshot = _campaign_config_snapshot(campaign_cfg)
        mismatch_keys = _snapshot_mismatch_keys(
            current=current_config_snapshot,
            expected={str(key): value for key, value in persisted_config_snapshot.items()},
        )
        if mismatch_keys:
            raise DreamPreconditionError(
                "mind dream campaign --resume: current campaign config no longer matches the resumable run "
                f"({', '.join(sorted(mismatch_keys))})"
            )
        schedule = _deserialize_schedule(progress.get("schedule"))
    else:
        campaign_cfg = _resolve_campaign_config(
            raw_config=raw_campaign_cfg,
            dream_config=v.config.dream,
            profile=profile,
        )
        current_config_snapshot = _campaign_config_snapshot(campaign_cfg)
        start_date = start_date or today_str()
        run_id = _campaign_run_id(start_date=start_date, profile=profile)
        progress = None
        schedule = _build_schedule(start_date=start_date, days=days, config=campaign_cfg)
    end_date = schedule[-1].effective_date
    projected_counts = _projected_counts(schedule)

    if dry_run:
        summary = (
            f"Campaign rehearsal planned for {len(schedule)} simulated days "
            f"from {start_date} through {end_date}: "
            f"light={projected_counts['light']} deep={projected_counts['deep']} rem={projected_counts['rem']} weave={projected_counts['weave']}."
        )
        return DreamResult(
            stage="campaign",
            dry_run=True,
            summary=summary,
            mutations=_render_preview(schedule, projected_counts=projected_counts),
        )

    if not resume:
        progress = _progress_payload(
            run_id=run_id,
            profile=profile,
            start_date=start_date,
            end_date=end_date,
            total_days=len(schedule),
            config_snapshot=current_config_snapshot,
            schedule=schedule,
            projected_counts=projected_counts,
        )
    else:
        assert progress is not None
        progress["status"] = "running"
        progress["projected_counts"] = dict(projected_counts)
        progress["schedule"] = _serialize_schedule(schedule)

    completed_counts = {
        stage: int(((progress or {}).get("completed_counts") or {}).get(stage) or 0)
        for stage in STAGE_ORDER
    }
    days_completed = int((progress or {}).get("days_completed") or 0)
    resume_effective_date = str((progress or {}).get("current_effective_date") or start_date)
    resume_stage = str((progress or {}).get("last_completed_stage") or "")
    inflight_stage = str((progress or {}).get("inflight_stage") or "")
    stage_progress = (progress or {}).get("stage_progress")
    if not isinstance(stage_progress, dict):
        stage_progress = {}
    mutations: list[str] = []
    warnings: list[str] = []

    with dream_run("campaign", dry_run=False) as (runtime, run_record_id):
        runtime.add_run_event(
            run_record_id,
            stage="campaign",
            event_type="selected",
            message=f"{len(schedule)} simulated days planned",
            payload={"projected_counts": projected_counts, "run_id": run_id},
        )
        with maybe_locked("campaign", dry_run=False):
            try:
                if not progress.get("plan_path"):
                    plan_path = _write_plan_report(
                        v,
                        run_id=run_id,
                        profile=profile,
                        start_date=start_date,
                        end_date=end_date,
                        schedule=schedule,
                        projected_counts=projected_counts,
                    )
                    progress["plan_path"] = v.logical_path(plan_path)
                    mutations.append(f"wrote {v.logical_path(plan_path)}")
                state.upsert_adapter_state(adapter=CAMPAIGN_ADAPTER, state=progress)

                for day in schedule[days_completed:]:
                    progress["current_effective_date"] = day.effective_date
                    progress["completed_counts"] = dict(completed_counts)
                    state.upsert_adapter_state(adapter=CAMPAIGN_ADAPTER, state=progress)

                    remaining_stages = list(day.stages)
                    if resume and day.day_index == days_completed and resume_effective_date == day.effective_date and resume_stage in day.stages:
                        remaining_stages = list(day.stages[day.stages.index(resume_stage) + 1 :])
                    elif (
                        resume
                        and day.day_index == days_completed
                        and resume_effective_date == day.effective_date
                        and inflight_stage in day.stages
                    ):
                        remaining_stages = list(day.stages[day.stages.index(inflight_stage) :])

                    if not remaining_stages and resume and day.day_index == days_completed and resume_stage == (day.stages[-1] if day.stages else ""):
                        daily_path = _write_daily_report(
                            v,
                            run_id=run_id,
                            day=day,
                            completed_counts=completed_counts,
                            stage_results=[],
                            resumed_from_stage=resume_stage,
                        )
                        progress["days_completed"] = day.day_index + 1
                        progress["current_effective_date"] = _next_date(day.effective_date) or day.effective_date
                        progress["last_completed_stage"] = resume_stage or "idle"
                        state.upsert_adapter_state(adapter=CAMPAIGN_ADAPTER, state=progress)
                        mutations.append(f"wrote {v.logical_path(daily_path)}")
                        continue

                    stage_results: list[DreamResult] = []
                    last_scheduled_stage = "idle"
                    for stage in remaining_stages:
                        resume_from_source_index = 0
                        if (
                            resume
                            and stage == "light"
                            and day.day_index == days_completed
                            and resume_effective_date == day.effective_date
                            and inflight_stage == "light"
                        ):
                            resume_from_source_index = max(0, int(stage_progress.get("processed_sources") or 0))
                        context = _campaign_context(
                            effective_date=day.effective_date,
                            run_id=run_id,
                            profile=profile,
                            config=campaign_cfg,
                            resume_from_source_index=resume_from_source_index,
                        )
                        progress["inflight_stage"] = stage
                        progress["stage_progress"] = {
                            "processed_sources": resume_from_source_index,
                            "total_sources": int(stage_progress.get("total_sources") or 0) if stage == "light" else 0,
                        }
                        state.upsert_adapter_state(adapter=CAMPAIGN_ADAPTER, state=progress)

                        progress_callback = None
                        if stage == "light":
                            def progress_callback(payload: dict[str, Any]) -> None:
                                progress["inflight_stage"] = "light"
                                progress["stage_progress"] = dict(payload)
                                state.upsert_adapter_state(adapter=CAMPAIGN_ADAPTER, state=progress)

                        result = _run_stage(stage=stage, context=context, progress_callback=progress_callback)
                        stage_results.append(result)
                        last_scheduled_stage = stage
                        completed_counts[stage] += 1
                        progress["completed_counts"] = dict(completed_counts)
                        progress["last_completed_stage"] = stage
                        progress["inflight_stage"] = None
                        progress["stage_progress"] = None
                        progress["current_effective_date"] = day.effective_date
                        runtime.add_run_event(
                            run_record_id,
                            stage="campaign",
                            event_type="stage-completed",
                            message=f"{day.effective_date}:{stage}",
                            payload={"campaign_run_id": run_id, "effective_date": day.effective_date},
                        )
                        state.upsert_adapter_state(adapter=CAMPAIGN_ADAPTER, state=progress)
                    daily_path = _write_daily_report(
                        v,
                        run_id=run_id,
                        day=day,
                        completed_counts=completed_counts,
                        stage_results=stage_results,
                    )
                    progress["days_completed"] = day.day_index + 1
                    progress["current_effective_date"] = _next_date(day.effective_date) or day.effective_date
                    progress["last_completed_stage"] = last_scheduled_stage
                    progress["inflight_stage"] = None
                    progress["stage_progress"] = None
                    state.upsert_adapter_state(adapter=CAMPAIGN_ADAPTER, state=progress)
                    mutations.append(f"wrote {v.logical_path(daily_path)}")
                    resume = False
                    resume_stage = ""
                    inflight_stage = ""
                    stage_progress = {}

                progress["status"] = "completed"
                progress["current_effective_date"] = end_date
                progress["completed_counts"] = dict(completed_counts)
                progress["inflight_stage"] = None
                progress["stage_progress"] = None
                state.upsert_adapter_state(adapter=CAMPAIGN_ADAPTER, state=progress)
            except BaseException as exc:
                progress["status"] = "interrupted"
                progress["last_error"] = _exception_message(exc)
                progress["completed_counts"] = dict(completed_counts)
                state.upsert_adapter_state(adapter=CAMPAIGN_ADAPTER, state=progress)
                raise

    summary = (
        f"Campaign processed {len(schedule)} simulated days "
        f"from {start_date} through {end_date}: "
        f"light={completed_counts['light']} deep={completed_counts['deep']} rem={completed_counts['rem']} weave={completed_counts['weave']}."
    )
    return DreamResult(stage="campaign", dry_run=False, summary=summary, mutations=mutations, warnings=warnings)
