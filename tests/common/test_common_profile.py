"""Tests for scripts.common.profile.load_profile_context."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.common.profile as profile_module
from mind.cli import main
from scripts.common.profile import load_profile_context
from tests.support import patch_onboarding_llm, write_repo_config


@pytest.fixture(autouse=True)
def reset_cache():
    """Ensure the process-level cache is cleared before and after each test."""
    profile_module._PROFILE_CACHE = None
    yield
    profile_module._PROFILE_CACHE = None


@pytest.fixture()
def fake_env(tmp_path, monkeypatch):
    """Patch env.load() to point repo_root at tmp_path."""
    from scripts.common import env

    class FakeCfg:
        gemini_api_key = "fake"
        llm_model = "fake"
        browser_for_cookies = "chrome"
        repo_root = tmp_path
        wiki_root = tmp_path / "memory"
        raw_root = tmp_path / "raw"
        substack_session_cookie = ""

    monkeypatch.setattr(env, "load", lambda: FakeCfg())
    return tmp_path


def test_happy_path_prefers_canonical_owner_note(fake_env):
    """Canonical owner note is included alongside the identity files."""
    me_dir = fake_env / "memory" / "me"
    me_dir.mkdir(parents=True)
    files = {
        "profile.md": "I am Example Owner.",
        "positioning.md": "Example Owner's positioning.",
        "values.md": "Example Owner's values.",
        "open-inquiries.md": "Example Owner's inquiries.",
    }
    for name, content in files.items():
        (me_dir / name).write_text(content)

    result = load_profile_context()

    for name, content in files.items():
        assert f"### {name}" in result
        assert content in result


def test_legacy_open_threads_note_is_read_only_fallback(fake_env):
    legacy_owner_note = "open" + "-threads.md"
    me_dir = fake_env / "memory" / "me"
    me_dir.mkdir(parents=True)
    (me_dir / "profile.md").write_text("I am Example Owner.")
    (me_dir / legacy_owner_note).write_text("Legacy threads.")

    result = load_profile_context()

    assert f"### {legacy_owner_note}" in result
    assert "Legacy threads." in result
    assert "### open-inquiries.md" not in result


def test_missing_wiki_me_dir_returns_empty(fake_env):
    """memory/me/ directory absent → returns empty string."""
    # Do not create the memory/me dir at all
    result = load_profile_context()
    assert result == ""


def test_partial_files_only_present_ones_included(fake_env):
    """Only some of the four files exist → result contains only those."""
    me_dir = fake_env / "memory" / "me"
    me_dir.mkdir(parents=True)
    (me_dir / "profile.md").write_text("Profile content.")
    (me_dir / "values.md").write_text("Values content.")
    # positioning.md and open-inquiries.md intentionally missing

    result = load_profile_context()

    assert "### profile.md" in result
    assert "Profile content." in result
    assert "### values.md" in result
    assert "Values content." in result
    assert "### positioning.md" not in result
    assert "### open-inquiries.md" not in result


def test_cache_hit_reads_disk_only_once(fake_env, monkeypatch):
    """Second call must not hit disk — verify via a spy on Path.read_text."""
    me_dir = fake_env / "memory" / "me"
    me_dir.mkdir(parents=True)
    (me_dir / "profile.md").write_text("Cached profile.")

    read_count = {"n": 0}
    original_read_text = __import__("pathlib").Path.read_text

    def spy_read_text(self, *args, **kwargs):
        read_count["n"] += 1
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(__import__("pathlib").Path, "read_text", spy_read_text)

    first = load_profile_context()
    reads_after_first = read_count["n"]

    second = load_profile_context()
    reads_after_second = read_count["n"]

    assert first == second
    assert reads_after_second == reads_after_first  # no extra reads on second call
    assert reads_after_first >= 1  # at least one read happened on first call


def _patch_project_root(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)


def test_load_profile_context_reads_real_onboarding_outputs(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = tmp_path / "onboarding.json"
    payload.write_text(
        json.dumps(
            {
                "name": "Example Owner",
                "role": "Founder",
                "location": "Remote",
                "summary": "Example Owner builds local-first tools.",
                "values": ["clarity", "taste"],
                "positioning": {
                    "summary": "Design engineer and founder.",
                    "work_priorities": ["craft quality"],
                    "constraints": ["keep it local-first"],
                },
                "open_threads": ["How should Brain evolve?"],
            }
        ),
        encoding="utf-8",
    )

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "profile-context"]) == 0
    assert main(["onboard", "validate", "--bundle", "profile-context"]) == 0
    assert main(["onboard", "materialize", "--bundle", "profile-context"]) == 0

    profile_module._PROFILE_CACHE = None
    context = load_profile_context(repo_root=tmp_path)

    for name in ("profile.md", "positioning.md", "values.md", "open-inquiries.md"):
        assert f"### {name}" in context
    assert "Example Owner builds local-first tools." in context
    assert "Design engineer and founder." in context
    assert "clarity" in context
    assert "How should Brain evolve?" in context
