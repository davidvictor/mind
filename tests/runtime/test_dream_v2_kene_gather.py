from __future__ import annotations

import json
import shutil
from pathlib import Path

from mind.cli import main
from tests.paths import EXAMPLES_ROOT


def _copy_harness(tmp_path: Path) -> Path:
    target = tmp_path / "thin-harness"
    shutil.copytree(EXAMPLES_ROOT, target)
    cfg = target / "config.yaml"
    cfg.write_text(cfg.read_text(encoding="utf-8").replace("enabled: false", "enabled: true", 1), encoding="utf-8")
    return target


def _patch_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)
    monkeypatch.setattr("mind.dream.common.project_root", lambda: root)


def test_kene_dry_run_emits_shadow_artifacts_without_memory_write(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)

    assert main(["dream", "kene", "--dry-run"]) == 0
    out = capsys.readouterr().out

    assert "Dream stage: kene" in out
    assert "blocked 1 canonical writes" in out
    assert not (root / "memory" / "dreams" / "kene").exists()

    stage_roots = sorted((root / "raw" / "reports" / "dream" / "v2" / "runs").glob("run-*/stage-kene"))
    assert len(stage_roots) == 1
    stage_root = stage_roots[0]
    expected = {
        "input-bundle.json",
        "prior-output-map.json",
        "arrangement-plan.json",
        "relation-diff.json",
        "critique.json",
        "render-package.json",
        "apply-plan.json",
        "apply-manifest.json",
        "compare.json",
    }
    assert {path.name for path in stage_root.glob("*.json")} == expected
    bundle = json.loads((stage_root / "input-bundle.json").read_text(encoding="utf-8"))
    assert bundle["stage"] == "kene"
    assert bundle["prior_stages"] == ["light", "deep", "rem"]
    assert bundle["atoms"]
    apply_manifest = json.loads((stage_root / "apply-manifest.json").read_text(encoding="utf-8"))
    assert apply_manifest["write_count"] == 0
    assert apply_manifest["entries"][0]["status"] == "blocked"
