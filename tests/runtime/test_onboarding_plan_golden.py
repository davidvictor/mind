from __future__ import annotations

import json
import shutil
from pathlib import Path

from mind.services.onboarding import render_onboarding_materialization_plan
from tests.paths import FIXTURES_ROOT, REPO_ROOT
from tests.support import write_repo_config


FIXTURE_BUNDLE = "20260414t151530z"


def test_onboarding_plan_matches_golden_projection(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    fixture_dir = FIXTURES_ROOT / "synthetic" / "onboarding" / FIXTURE_BUNDLE
    bundle_dir = tmp_path / "raw" / "onboarding" / "bundles" / FIXTURE_BUNDLE
    shutil.copytree(fixture_dir, bundle_dir, dirs_exist_ok=True)

    plan = render_onboarding_materialization_plan(tmp_path, bundle_id=FIXTURE_BUNDLE)
    golden = json.loads((REPO_ROOT / "tests" / "golden" / "synthetic" / "onboarding" / f"{FIXTURE_BUNDLE}_plan.json").read_text(encoding="utf-8"))

    assert _project_plan(plan) == _project_plan(golden)


def _project_plan(plan: dict) -> dict:
    return {
        "bundle_id": plan.get("bundle_id"),
        "page_count": len(plan.get("pages") or []),
        "pages": [
            {
                "target_kind": page.get("target_kind"),
                "slug": page.get("slug"),
                "page_type": page.get("page_type"),
                "write_mode": page.get("write_mode"),
                "summary_kind": page.get("summary_kind"),
            }
            for page in plan.get("pages") or []
        ],
    }
