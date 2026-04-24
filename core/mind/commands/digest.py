from __future__ import annotations

import argparse

from mind.services.digest_service import write_digest_snapshot
from .common import project_root


def cmd_digest(args: argparse.Namespace) -> int:
    path = write_digest_snapshot(project_root(), today=args.today)
    print(path)
    return 0
