from __future__ import annotations

from pathlib import Path

from mind.services.repair_graph import run_graph_repair
from tests.support import write_repo_config


def _write_page(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_repair_graph_dry_run_reports_changes(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _write_page(
        tmp_path / "memory" / "templates" / "note.md",
        "---\nid: template-note\ntype: note\ntitle: Template Note\nstatus: active\ncreated: 2026-04-09\nlast_updated: 2026-04-09\naliases: []\ntags:\n  - domain/meta\n  - function/note\n  - signal/working\ndomains:\n  - meta\nrelates_to: []\nsources: []\n---\n# Template\n",
    )
    _write_page(
        tmp_path / "memory" / "sources" / "alpha-page.md",
        "---\nid: alpha-page\ntype: note\ntitle: Alpha Page\nstatus: active\ncreated: 2026-04-09\nlast_updated: 2026-04-09\naliases: []\ntags:\n  - domain/meta\n  - function/note\n  - signal/working\ndomains:\n  - meta\nrelates_to: []\nsources: []\n---\n# Alpha Page\n",
    )
    _write_page(
        tmp_path / "memory" / "sources" / "broken.md",
        "---\nid: broken\ntype: article\ntitle: Broken\nstatus: active\ncreated: 2026-04-09\nlast_updated: 2026-04-09\naliases: []\ntags:\n  - domain/learning\n  - function/source\n  - signal/canon\n  - youtube\n  - systems-thinking\ndomains:\n  - learning\nrelates_to: []\nsources: []\n---\n# Broken\n\nSee [[[[Alpha Page]]]] and [[Unknown Thing]].\n",
    )
    _write_page(
        tmp_path / "memory" / "people" / "alice-author.md",
        "---\nid: alice-author\ntype: person\ntitle: Alice Author\nstatus: active\ncreated: 2026-04-09\nlast_updated: 2026-04-09\naliases: []\ntags:\n  - domain/relationships\n  - function/reference\n  - signal/canon\n  - writer\ndomains:\n  - learning\nrelates_to: []\nsources: []\n---\n# Alice Author\n",
    )

    report = run_graph_repair(tmp_path, apply=False)

    assert report.templates_moved == 1
    assert report.tags_rewritten >= 2
    assert report.links_rewritten >= 1
    assert report.links_downgraded >= 1
    assert report.stubs_rewritten >= 1
    assert Path(report.report_path).exists()
    assert (tmp_path / "memory" / "templates" / "note.md").exists()


def test_repair_graph_apply_rewrites_tags_links_and_templates(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _write_page(
        tmp_path / "memory" / "templates" / "note.md",
        "---\nid: template-note\ntype: note\ntitle: Template Note\nstatus: active\ncreated: 2026-04-09\nlast_updated: 2026-04-09\naliases: []\ntags:\n  - domain/meta\n  - function/note\n  - signal/working\ndomains:\n  - meta\nrelates_to: []\nsources: []\n---\n# Template\n",
    )
    _write_page(
        tmp_path / "memory" / "sources" / "alpha-page.md",
        "---\nid: alpha-page\ntype: note\ntitle: Alpha Page\nstatus: active\ncreated: 2026-04-09\nlast_updated: 2026-04-09\naliases: []\ntags:\n  - domain/meta\n  - function/note\n  - signal/working\ndomains:\n  - meta\nrelates_to: []\nsources: []\n---\n# Alpha Page\n",
    )
    _write_page(
        tmp_path / "memory" / "sources" / "broken.md",
        "---\nid: broken\ntype: article\ntitle: Broken\nstatus: active\ncreated: 2026-04-09\nlast_updated: 2026-04-09\naliases: []\ntags:\n  - domain/learning\n  - function/source\n  - signal/canon\n  - youtube\n  - systems-thinking\ndomains:\n  - learning\nrelates_to: []\nsources: []\n---\n# Broken\n\nSee [[[[Alpha Page]]]] and [[Unknown Thing]].\n",
    )
    _write_page(
        tmp_path / "memory" / "people" / "alice-author.md",
        "---\nid: alice-author\ntype: person\ntitle: Alice Author\nstatus: active\ncreated: 2026-04-09\nlast_updated: 2026-04-09\naliases: []\ntags:\n  - domain/relationships\n  - function/reference\n  - signal/canon\n  - writer\ndomains:\n  - learning\nrelates_to: []\nsources: []\n---\n# Alice Author\n",
    )

    report = run_graph_repair(tmp_path, apply=True)

    assert report.pages_rewritten >= 2
    assert not (tmp_path / "memory" / "templates").exists()
    assert (tmp_path / "templates" / "note.md").exists()

    broken = (tmp_path / "memory" / "sources" / "broken.md").read_text(encoding="utf-8")
    assert "[[alpha-page]]" in broken
    assert "Unknown Thing" in broken
    assert "[[Unknown Thing]]" not in broken
    assert "  - systems-thinking" in broken
    assert "  - youtube" not in broken

    person = (tmp_path / "memory" / "people" / "alice-author.md").read_text(encoding="utf-8")
    assert "domains:\n  - relationships" in person
    assert "  - writer" not in person
