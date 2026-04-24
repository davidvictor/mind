from __future__ import annotations

import importlib.util
from pathlib import Path

from tests.paths import REPO_ROOT


def _load_guard_check():
    path = REPO_ROOT / "core" / "tools" / "check_no_private_data.py"
    spec = importlib.util.spec_from_file_location("check_no_private_data", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module.check


def test_private_data_guard_rejects_owner_identity_markers(tmp_path: Path, monkeypatch) -> None:
    candidate = tmp_path / "fixture.json"
    candidate.write_text(
        "\n".join(
            [
                "hello@private-" + "owner.invalid",
                "github.com/" + "private-owner",
                "linkedin.com/in/" + "private-owner",
                "twitter.com/" + "private-owner",
                "(555) " + "000-0000",
                "/Users/" + "private-owner/project",
                "-Users-" + "private-owner",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    check = _load_guard_check()
    failures = check([candidate.name], scan_content=True)

    assert failures
    assert all(candidate.name in failure for failure in failures)


def test_private_data_guard_rejects_private_artifact_paths() -> None:
    check = _load_guard_check()

    failures = check(
        [
            "memory/INDEX.md",
            "raw/cache/item.json",
            "local_data/config.yaml",
            "dropbox/inbox.md",
            ".obsidian/graph.json",
            ".omx/context/plan.md",
            "contracts/.omc/state/mission-state.json",
            ".logs/llm/attempts.jsonl",
            ".claude/settings.local.json",
            "2026-04-14.md",
            ".brain-runtime.sqlite3",
        ],
        scan_content=False,
    )

    assert len(failures) == 11
