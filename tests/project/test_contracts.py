from __future__ import annotations

from tests.paths import REPO_ROOT


WRAPPER_COMMANDS = {
    "config.md",
    "digest.md",
    "doctor.md",
    "dream.md",
    "dropbox.md",
    "expand.md",
    "graph.md",
    "ingest.md",
    "ingest-articles.md",
    "ingest-audible.md",
    "ingest-books.md",
    "ingest-readiness.md",
    "ingest-substack.md",
    "ingest-youtube.md",
    "lint.md",
    "llm.md",
    "obsidian.md",
    "onboard.md",
    "orchestrate.md",
    "query.md",
    "readiness.md",
    "repair-articles.md",
    "seed.md",
    "state.md",
    "worker.md",
}

LEGACY_COMMANDS = {
    "triage.md",
    "weave.md",
}

LEGACY_TEXT = (
    "memory/wiki",
    "intentionally prompt-native",
    "prompt-native holdouts",
    "weave_mode",
    "mind dream weave",
    "Phase 8+",
)

MISSING_ACTIVE_DOC_LINKS = tuple(f"{name}.md" for name in ("agent", "DESIGN"))


def _command_dir():
    return REPO_ROOT / ".claude" / "commands"


def _wrapper_paths():
    return sorted(_command_dir().glob("*.md"))


def test_wrapper_command_set_matches_current_cli_surface() -> None:
    actual = {path.name for path in _wrapper_paths()}
    assert actual == WRAPPER_COMMANDS
    assert actual.isdisjoint(LEGACY_COMMANDS)


def test_wrapper_commands_reference_canonical_cli() -> None:
    for path in _wrapper_paths():
        text = path.read_text(encoding="utf-8")
        assert ".venv/bin/python -m mind" in text, path.name
        for snippet in LEGACY_TEXT:
            assert snippet not in text, (path.name, snippet)


def test_onboard_wrapper_is_no_longer_a_thin_wrapper() -> None:
    text = (REPO_ROOT / ".claude" / "commands" / "onboard.md").read_text(encoding="utf-8")
    assert "Thin wrapper" not in text
    assert "Claude is the interaction owner" in text


def test_readme_mentions_current_operator_surface() -> None:
    text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    for snippet in (
        "Turn private sources into a self-organizing local knowledge graph you can inspect, rerun, and own.",
        "Why Dreaming Matters",
        "What A Brain Is",
        "Dreaming: Light, Deep, REM",
        "Build In Public Notes",
        "python3.11 -m venv .venv",
        ".venv/bin/python -m mind onboard import --from-json",
        ".venv/bin/python -m mind orchestrate daily",
        ".venv/bin/python -m mind worker run-once",
        ".venv/bin/python -m mind worker drain-until-empty",
        ".venv/bin/python -m mind dream light",
        ".venv/bin/python -m mind dream deep",
        ".venv/bin/python -m mind dream rem",
        ".venv/bin/python -m mind dream simulate-year",
        ".venv/bin/python -m mind digest",
        ".venv/bin/python -m mind state",
        ".venv/bin/python core/tools/check_no_private_data.py --tracked",
    ):
        assert snippet in text
    assert "mind onboard interview" not in text


def test_active_docs_do_not_reference_missing_agent_or_design_files() -> None:
    for rel in (
        "README.md",
        "AGENTS.md",
        "CLAUDE.md",
        "docs/README.md",
        "docs/README.md",
    ):
        text = (REPO_ROOT / rel).read_text(encoding="utf-8")
        for missing in MISSING_ACTIVE_DOC_LINKS:
            assert missing not in text, (rel, missing)


def test_repo_pins_python_311() -> None:
    version = (REPO_ROOT / ".python-version").read_text(encoding="utf-8").strip()
    assert version.startswith("3.11")


def test_runtime_artifacts_are_gitignored_and_documented() -> None:
    gitignore = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8")
    agents = (REPO_ROOT / "AGENTS.md").read_text(encoding="utf-8")
    assert ".logs/" in gitignore
    assert ".logs/" in agents
