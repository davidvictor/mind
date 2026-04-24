"""Tests for scripts/lint.py."""
from __future__ import annotations

from pathlib import Path
import pytest


def _make_vault(tmp_path: Path):
    from scripts.common.vault import Vault
    (tmp_path / "memory").mkdir()
    (tmp_path / "memory" / "sources").mkdir()
    (tmp_path / "raw").mkdir()
    (tmp_path / "config.yaml").write_text(
        "vault:\n  wiki_dir: memory\n  raw_dir: raw\n  owner_profile: me/profile.md\n"
        "llm:\n  model: google/gemini-2.5-pro\n"
    )
    return Vault.load(tmp_path)


def test_lint_clean_vault_returns_zero(tmp_path: Path):
    from scripts import lint
    v = _make_vault(tmp_path)
    report = lint.run(v)
    assert report.failing_pages == 0
    assert report.broken_links == 0
    assert report.exit_code == 0


def test_lint_detects_missing_required_frontmatter_field(tmp_path: Path):
    from scripts import lint
    v = _make_vault(tmp_path)
    (v.wiki / "sources" / "test-page.md").write_text(
        "---\ntype: summary\n---\n# missing id, title, etc.\n"
    )
    report = lint.run(v)
    assert report.failing_pages >= 1
    assert report.exit_code != 0


def test_lint_detects_broken_wikilinks(tmp_path: Path):
    from scripts import lint
    v = _make_vault(tmp_path)
    (v.wiki / "sources" / "page.md").write_text(
        "---\nid: page\ntype: note\ntitle: Page\n---\nrefers to [[nonexistent]].\n"
    )
    report = lint.run(v)
    assert report.broken_links >= 1


def test_lint_counts_frontmatter_wikilinks_as_valid_references(tmp_path: Path):
    from scripts import lint

    v = _make_vault(tmp_path)
    (v.wiki / "sources" / "page-a.md").write_text(
        "---\n"
        "id: page-a\n"
        "type: summary\n"
        "title: Page A\n"
        "status: active\n"
        "created: 2026-04-09\n"
        "last_updated: 2026-04-09\n"
        "aliases: []\n"
        "tags: []\n"
        "domains: []\n"
        "relates_to:\n"
        "  - \"[[page-b]]\"\n"
        "sources: []\n"
        "source_path: raw/example.md\n"
        "source_type: document\n"
        "source_date: 2026-04-09\n"
        "ingested: 2026-04-09\n"
        "---\n"
        "# Page A\n"
    )
    (v.wiki / "sources" / "page-b.md").write_text(
        "---\n"
        "id: page-b\n"
        "type: note\n"
        "title: Page B\n"
        "status: active\n"
        "created: 2026-04-09\n"
        "last_updated: 2026-04-09\n"
        "aliases: []\n"
        "tags: []\n"
        "domains: []\n"
        "relates_to: []\n"
        "sources: []\n"
        "---\n"
        "# Page B\n"
    )

    report = lint.run(v)
    assert report.broken_links == 0
    assert report.orphans == 0


def test_lint_excludes_system_files_from_orphan_detection(tmp_path: Path):
    from scripts import lint
    v = _make_vault(tmp_path)
    (v.wiki / "CHANGELOG.md").write_text("# changelog\n")
    (v.wiki / "INDEX.md").write_text("# index\n")
    (v.wiki / ".lint-report.md").write_text("# report\n")
    (v.wiki / "inbox").mkdir()
    (v.wiki / "inbox" / "log.md").write_text("# inbox\n")
    (v.wiki / "me" / "digests").mkdir(parents=True)
    (v.wiki / "me" / "digests" / "2026-04-09.md").write_text(
        "---\nid: 2026-04-09\ntype: note\ntitle: Digest\nstatus: active\ncreated: 2026-04-09\nlast_updated: 2026-04-09\naliases: []\ntags: []\ndomains: []\nrelates_to: []\nsources: []\n---\n# Digest\n",
        encoding="utf-8",
    )
    (v.wiki / "me").mkdir(exist_ok=True)
    (v.wiki / "me" / "timeline.md").write_text(
        "---\nid: timeline\ntype: note\ntitle: Timeline\nstatus: active\ncreated: 2026-04-09\nlast_updated: 2026-04-09\naliases: []\ntags: []\ndomains: []\nrelates_to: []\nsources: []\n---\n# Timeline\n",
        encoding="utf-8",
    )
    report = lint.run(v)
    assert report.orphans == 0  # all excluded


def test_lint_warns_when_owner_profile_missing(tmp_path: Path, caplog):
    from scripts import lint
    v = _make_vault(tmp_path)
    # owner profile not created
    lint.run(v)
    assert "owner profile not found" in caplog.text.lower()


def test_lint_rejects_duplicate_tags_frontmatter_keys(tmp_path: Path):
    from scripts import lint

    v = _make_vault(tmp_path)
    (v.wiki / "sources" / "dupe.md").write_text(
        "---\n"
        "id: dupe\n"
        "type: note\n"
        "title: Duplicate Tags\n"
        "status: active\n"
        "created: 2026-04-09\n"
        "last_updated: 2026-04-09\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/note\n  - signal/working\n"
        "domains:\n  - meta\n"
        "relates_to: []\n"
        "sources: []\n"
        "tags:\n  - duplicate\n"
        "---\n"
        "# Duplicate Tags\n",
        encoding="utf-8",
    )

    report = lint.run(v)
    assert any("duplicate frontmatter keys" in detail for detail in report.details)


def test_lint_rejects_reserved_metadata_tags(tmp_path: Path):
    from scripts import lint

    v = _make_vault(tmp_path)
    (v.wiki / "sources" / "reserved.md").write_text(
        "---\n"
        "id: reserved\n"
        "type: note\n"
        "title: Reserved Tags\n"
        "status: active\n"
        "created: 2026-04-09\n"
        "last_updated: 2026-04-09\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/note\n  - signal/working\n  - youtube\n"
        "domains:\n  - meta\n"
        "relates_to: []\n"
        "sources: []\n"
        "---\n"
        "# Reserved Tags\n",
        encoding="utf-8",
    )

    report = lint.run(v)
    assert any("invalid topic tag" in detail for detail in report.details)
