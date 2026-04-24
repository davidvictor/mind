from __future__ import annotations

import argparse

from mind.services.obsidian_theme import apply_obsidian_theme

from . import common as command_common


def cmd_obsidian_theme_apply(args: argparse.Namespace) -> int:
    try:
        result = apply_obsidian_theme(
            command_common.project_root(),
            dark=str(getattr(args, "dark", "dragon") or "dragon"),
            light=str(getattr(args, "light", "lotus") or "lotus"),
            force=bool(getattr(args, "force", False)),
        )
    except ValueError as exc:
        print(str(exc))
        return 1
    print(result.render())
    return 0
