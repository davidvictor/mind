from __future__ import annotations

import argparse

from mind.services.cli_progress import progress_for_args
from mind.services.orchestrator import run_daily_orchestrator
from .common import project_root


def cmd_orchestrate_daily(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="running daily orchestrator", default=True) as progress:
        progress.phase("running daily orchestrator")
        result = run_daily_orchestrator(project_root(), phase_callback=progress.phase)
        print(result.render())
        return result.exit_code
