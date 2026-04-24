from __future__ import annotations

import json
import shutil
from pathlib import Path

from mind.cli import main
from mind.services.llm_telemetry import read_events
from tests.paths import FIXTURES_ROOT
from tests.support import patch_onboarding_llm, write_repo_config


def _patch_project_root(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.llm.project_root", lambda: root)


def test_onboarding_full_pipeline_materializes_expected_page_count(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)

    fixture_dir = FIXTURES_ROOT / "synthetic" / "onboarding" / "20260414t151530z"
    raw_input = tmp_path / "onboarding.json"
    shutil.copy2(fixture_dir / "raw-input.json", raw_input)
    upload = fixture_dir / "uploads" / "0001-5b8b09403607-example-owner-canonical-bio.md"

    assert main(["onboard", "import", "--from-json", str(raw_input), "--upload", str(upload), "--bundle", "full-pipeline"]) == 0
    assert main(["onboard", "verify", "--bundle", "full-pipeline"]) == 0
    assert main(["onboard", "materialize", "--bundle", "full-pipeline"]) == 0

    state = json.loads((tmp_path / "raw" / "onboarding" / "bundles" / "full-pipeline" / "state.json").read_text(encoding="utf-8"))
    page_count = len(state.get("materialized_pages") or []) + (1 if state.get("decision_page") else 0)
    plan = json.loads((tmp_path / "raw" / "onboarding" / "bundles" / "full-pipeline" / "materialization-plan.json").read_text(encoding="utf-8"))
    assert page_count == len(plan["data"]["pages"])
    assert page_count == 33

    events = read_events(tmp_path, bundle_id="full-pipeline")
    assert all(event.get("status") != "error" for event in events)
