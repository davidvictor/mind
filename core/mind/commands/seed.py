from __future__ import annotations

import argparse

from mind.services.seed_service import DEFAULT_PRESET, PRESET_CHOICES, seed_brain

from . import common as command_common


def cmd_seed(args: argparse.Namespace) -> int:
    preset = getattr(args, "preset", DEFAULT_PRESET) or DEFAULT_PRESET
    result = seed_brain(command_common.project_root(), preset=preset)
    print(result.render())
    return 0


__all__ = ["DEFAULT_PRESET", "PRESET_CHOICES", "cmd_seed"]
