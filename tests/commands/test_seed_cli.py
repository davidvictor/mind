from __future__ import annotations

from pathlib import Path

from mind.cli import build_parser, main
from tests.support import option_strings, parser_for_command, write_repo_config


def _patch_project_root(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)


def test_mind_seed_parser_exposes_presets() -> None:
    parser = build_parser()
    seed_parser = parser_for_command(parser, "seed")
    assert "--preset" in option_strings(parser, "seed")
    preset_action = next(action for action in seed_parser._actions if "--preset" in action.option_strings)
    assert set(preset_action.choices) >= {"core", "skeleton", "framework"}


def test_mind_seed_defaults_to_skeleton(tmp_path: Path, monkeypatch, capsys) -> None:
    write_repo_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)

    assert main(["seed"]) == 0
    out = capsys.readouterr().out
    assert "preset=skeleton" in out
    assert (tmp_path / "memory" / "projects" / "brain.md").exists()
    assert (tmp_path / "memory" / "concepts" / "starter-graph.md").exists()
