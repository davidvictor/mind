from __future__ import annotations

import argparse
from pathlib import Path

from mind.services.atom_page_repair import run_atom_page_repair
from mind.services.content_policy import run_content_policy_migration, run_content_policy_repair
from mind.services.identifier_repair import run_identifier_repair
from mind.services.repair_graph import run_graph_repair
from mind.services.reingest import run_personalization_link_repair
from mind.services.vault_housekeeping import run_vault_housekeeping
from mind.services.weave_cleanup import run_weave_cleanup

from .common import project_root


def cmd_repair_graph(args: argparse.Namespace) -> int:
    scopes = [item.strip() for item in (args.scope or "").split(",") if item.strip()]
    report = run_graph_repair(
        project_root(),
        apply=bool(args.apply),
        scopes=scopes or None,
    )
    print(report.render())
    return 0


def cmd_repair_content_policy(args: argparse.Namespace) -> int:
    report = run_content_policy_repair(
        project_root(),
        apply=bool(args.apply),
    )
    print(report.render())
    return 0


def cmd_repair_content_policy_migrate(args: argparse.Namespace) -> int:
    report = run_content_policy_migration(
        project_root(),
        lane=args.lane,
        apply=bool(args.apply),
    )
    print(report.render())
    return 0


def cmd_repair_vault_housekeeping(args: argparse.Namespace) -> int:
    report = run_vault_housekeeping(
        project_root(),
        apply=bool(args.apply),
    )
    print(report.render())
    return 0


def cmd_repair_atom_pages(args: argparse.Namespace) -> int:
    report = run_atom_page_repair(
        project_root(),
        apply=bool(args.apply),
    )
    print(report.render())
    return 0


def cmd_repair_identifiers(args: argparse.Namespace) -> int:
    report = run_identifier_repair(
        project_root(),
        apply=bool(args.apply),
    )
    print(report.render())
    return 0


def cmd_repair_personalization_links(args: argparse.Namespace) -> int:
    report = run_personalization_link_repair(
        repo_root=project_root(),
        lane=args.lane,
        path=(Path(args.path).expanduser() if args.path else None),
        today=args.today,
        limit=args.limit,
        source_ids=tuple(args.source_ids or ()),
        apply=bool(args.apply),
    )
    print(report.render())
    return 0


def cmd_repair_weave_cleanup(args: argparse.Namespace) -> int:
    report = run_weave_cleanup(
        project_root(),
        apply=bool(args.apply),
    )
    print(report.render())
    return 0
