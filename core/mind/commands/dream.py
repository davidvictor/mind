from __future__ import annotations

import argparse

from mind.dream.common import DreamPreconditionError, vault as dream_vault
from mind.dream.bootstrap import run_bootstrap
from mind.dream.campaign import run_campaign
from mind.dream.simulation import run_simulate_year
from mind.dream.v2.runtime import run_dream_v2_stage
from mind.dream.v2.weave_stage import run_weave_v2_shadow
from mind.services.cli_progress import progress_for_args

from .common import project_root


def run_light(*, dry_run: bool):
    return run_dream_v2_stage(stage="light", dry_run=dry_run)


def run_deep(*, dry_run: bool):
    return run_dream_v2_stage(stage="deep", dry_run=dry_run)


def run_rem(*, dry_run: bool):
    return run_dream_v2_stage(stage="rem", dry_run=dry_run)


def run_weave(*, dry_run: bool):
    return run_dream_v2_stage(stage="weave", dry_run=dry_run)


def _run(stage: str, *, dry_run: bool) -> int:
    try:
        if stage == "light":
            result = run_light(dry_run=dry_run)
        elif stage == "deep":
            result = run_deep(dry_run=dry_run)
        elif stage == "weave":
            result = run_weave(dry_run=dry_run)
        else:
            result = run_rem(dry_run=dry_run)
    except DreamPreconditionError as exc:
        print(f"mind dream {stage}: {exc}")
        return 1
    print(result.render())
    return 0


def cmd_dream_light(args: argparse.Namespace) -> int:
    return _run("light", dry_run=bool(args.dry_run))


def cmd_dream_deep(args: argparse.Namespace) -> int:
    return _run("deep", dry_run=bool(args.dry_run))


def cmd_dream_rem(args: argparse.Namespace) -> int:
    dry_run = bool(args.dry_run)
    try:
        rem = run_rem(dry_run=dry_run)
    except DreamPreconditionError as exc:
        print(f"mind dream rem: {exc}")
        return 1
    print(rem.render())
    weave_cfg = dream_vault().config.dream.weave
    if dry_run or not weave_cfg.enabled or not weave_cfg.run_after_rem:
        return 0
    try:
        weave = run_weave(dry_run=False)
    except DreamPreconditionError as exc:
        print(f"\nmind dream weave: {exc}")
        return 1
    print()
    print(weave.render())
    return 0


def cmd_dream_weave(args: argparse.Namespace) -> int:
    dry_run = bool(args.dry_run)
    if bool(getattr(args, "shadow_v2", False)):
        try:
            shadow = run_weave_v2_shadow(dry_run=dry_run)
        except DreamPreconditionError as exc:
            print(f"mind dream weave --shadow-v2: {exc}")
            return 1
        print(shadow.render())
        return 0
    return _run("weave", dry_run=dry_run)


def cmd_dream_bootstrap(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="running dream bootstrap", default=True) as progress:
        progress.phase("loading bootstrap sources")
        try:
            progress.phase("replaying historical sources")
            result = run_bootstrap(
                dry_run=bool(args.dry_run),
                force_pass_d=bool(args.force_pass_d),
                checkpoint_every=args.checkpoint_every,
                resume=bool(args.resume),
                limit=args.limit,
            )
        except DreamPreconditionError as exc:
            print(f"mind dream bootstrap: {exc}")
            return 1
        print(result.render())
        return 0


def cmd_dream_campaign(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="running dream campaign", default=True) as progress:
        progress.phase("building campaign schedule")
        try:
            result = run_campaign(
                days=int(args.days),
                start_date=args.start_date,
                dry_run=bool(args.dry_run),
                resume=bool(args.resume),
                profile=str(args.profile or "aggressive"),
            )
        except DreamPreconditionError as exc:
            print(f"mind dream campaign: {exc}")
            return 1
        print(result.render())
        return 0


def cmd_dream_simulate_year(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="running isolated dream simulation", default=True) as progress:
        progress.phase("creating isolated simulation roots")
        try:
            result = run_simulate_year(
                repo_root=project_root(),
                start_date=args.start_date,
                run_id=args.run_id,
                days=int(args.days),
                dry_run=bool(args.dry_run),
            )
        except DreamPreconditionError as exc:
            print(f"mind dream simulate-year: {exc}")
            return 1
        print(result.render())
        return 0
