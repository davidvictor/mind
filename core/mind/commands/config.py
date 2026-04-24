from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.common.config import BrainConfig
from mind.services.llm_validation import validate_routed_llm
from scripts.common import env

from .common import project_root


def config_path() -> str:
    root = project_root()
    return BrainConfig.describe_active_config(root)


def cmd_config_show(_args: argparse.Namespace) -> int:
    cfg = BrainConfig.load(project_root())
    runtime_cfg = env.load()
    validation = validate_routed_llm(runtime_cfg).to_public_dict()
    payload = cfg.model_dump(mode="json")
    payload["_validation"] = validation
    print(
        json.dumps(
            payload,
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


def cmd_config_path(_args: argparse.Namespace) -> int:
    print(config_path())
    return 0
