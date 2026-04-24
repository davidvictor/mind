from __future__ import annotations

import argparse

from .common import project_root
from mind.services.cli_progress import progress_for_args
from mind.services.queue_worker import drain_until_empty, process_one_queued_run


def cmd_worker_run_once(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="processing one queued run", default=True) as progress:
        progress.phase("processing one queued run")
        rc, message = process_one_queued_run(project_root(), phase_callback=progress.phase)
        print(message)
        return rc


def cmd_worker_drain_until_empty(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="draining queued work", default=True) as progress:
        progress.phase("draining queued work")
        result = drain_until_empty(project_root(), phase_callback=progress.phase)
        print(
            "worker: drain-until-empty -> "
            f"processed={result.processed} failed={result.failures}"
        )
        return result.exit_code
