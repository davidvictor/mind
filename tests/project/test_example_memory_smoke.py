from __future__ import annotations

import json
import shutil
from pathlib import Path

from scripts import lint
from scripts.common.vault import Vault
from tests.paths import EXAMPLES_ROOT


THIN_HARNESS = EXAMPLES_ROOT


def _copy_harness(tmp_path: Path) -> Path:
    target = tmp_path / "thin-harness"
    shutil.copytree(THIN_HARNESS, target)
    return target


def _assert_clean_lint_report(report) -> None:
    assert report.failing_pages == 0
    assert report.schema_violations == 0
    assert report.broken_links == 0
    assert report.orphans == 0
    assert report.exit_code == 0


def _set_dream_enabled(root: Path, enabled: bool) -> None:
    cfg = root / "config.yaml"
    text = cfg.read_text(encoding="utf-8")
    text = text.replace("enabled: false", f"enabled: {'true' if enabled else 'false'}", 1)
    cfg.write_text(text, encoding="utf-8")


def test_thin_harness_loads_with_vault(tmp_path: Path):
    root = _copy_harness(tmp_path)
    v = Vault.load(root)
    assert v.root == root
    assert v.wiki == root / "memory"
    assert v.raw == root / "raw"
    assert v.memory_root == root
    assert v.runtime_db == root / ".brain-runtime.sqlite3"
    assert v.brain_state.exists()
    assert v.open_inquiries_path.name == "open-inquiries.md"


def test_thin_harness_matches_minimum_seed_shape(tmp_path: Path):
    root = _copy_harness(tmp_path)
    brain_state = json.loads((root / "memory" / ".brain-state.json").read_text(encoding="utf-8"))
    required_files = [
        root / "config.yaml",
        root / "memory" / ".brain-state.json",
        root / "memory" / "INDEX.md",
        root / "memory" / "CHANGELOG.md",
        root / "memory" / "me" / "profile.md",
        root / "memory" / "me" / "values.md",
        root / "memory" / "me" / "positioning.md",
        root / "memory" / "me" / "open-inquiries.md",
        root / "memory" / "concepts" / "local-first-systems.md",
        root / "memory" / "playbooks" / "weekly-review-loop.md",
        root / "memory" / "stances" / "user-owned-ai.md",
        root / "memory" / "inquiries" / "how-to-balance-depth-and-speed.md",
        root / "memory" / "summaries" / "summary-example-seed.md",
        root / "raw" / "drops" / "example-seed.md",
    ]

    for path in required_files:
        assert path.exists(), path

    assert brain_state["atoms"]["count"] == 4
    assert brain_state["atoms"]["by_type"] == {
        "concept": 1,
        "playbook": 1,
        "stance": 1,
        "inquiry": 1,
    }

    assert not (root / "memory" / "inbox").exists()
    assert not (root / "memory" / "me" / "digests").exists()
    assert not (root / "memory" / "me" / "reflections").exists()
    assert not (root / "memory" / "me" / "timeline.md").exists()
    assert not (root / "skills").exists()


def test_thin_harness_lints_with_dream_disabled(tmp_path: Path):
    root = _copy_harness(tmp_path)
    _set_dream_enabled(root, False)
    report = lint.run(Vault.load(root))
    _assert_clean_lint_report(report)


def test_thin_harness_lints_with_dream_enabled(tmp_path: Path):
    root = _copy_harness(tmp_path)
    _set_dream_enabled(root, True)
    report = lint.run(Vault.load(root))
    _assert_clean_lint_report(report)
