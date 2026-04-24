from __future__ import annotations

from typing import Any, Callable

from mind.dream.common import DreamExecutionContext, DreamResult

from .deep_stage import run_deep as run_deep_v2
from .kene_stage import run_kene as run_kene_v2
from .light_stage import run_light as run_light_v2
from .rem_stage import run_rem as run_rem_v2


def run_dream_v2_stage(
    *,
    stage: str,
    dry_run: bool,
    acquire_lock: bool = True,
    context: DreamExecutionContext | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> DreamResult:
    if stage == "light":
        return run_light_v2(
            dry_run=dry_run,
            acquire_lock=acquire_lock,
            context=context,
            progress_callback=progress_callback,
        )
    if stage == "deep":
        return run_deep_v2(
            dry_run=dry_run,
            acquire_lock=acquire_lock,
            context=context,
        )
    if stage == "rem":
        return run_rem_v2(
            dry_run=dry_run,
            acquire_lock=acquire_lock,
            context=context,
        )
    if stage == "kene":
        return run_kene_v2(
            dry_run=dry_run,
            acquire_lock=acquire_lock,
            context=context,
            progress_callback=progress_callback,
        )
    raise KeyError(stage)
