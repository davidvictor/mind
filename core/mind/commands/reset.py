from __future__ import annotations

import argparse

from mind.services.reset_service import reset_brain

from . import common as command_common


def cmd_reset(args: argparse.Namespace) -> int:
    result = reset_brain(command_common.project_root(), apply=bool(args.apply))
    print(result.render())
    return 0
