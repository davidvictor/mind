from __future__ import annotations

from pathlib import Path

from mind.services.vault_housekeeping import run_vault_housekeeping
from tests.support import write_repo_config


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_vault_housekeeping_reports_pair_issues_and_noise(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _write(
        tmp_path / "memory" / "sources" / "books" / "business" / "alice-author-designing-data-intensive-applications.md",
        "---\nid: alice-author-designing-data-intensive-applications\ntype: book\ntitle: Designing Data-Intensive Applications\naliases: []\ndomains:\n  - business\nsources:\n  - summary-book-alice-author-designing-data-intensive-applications\nauthor:\n  - alice-author\nfinished: 2026-03-15\n---\nBody\n",
    )
    _write(
        tmp_path / "memory" / "sources" / "books" / "business" / "martin-kleppmann-designing-data-intensive-applications.md",
        "---\nid: martin-kleppmann-designing-data-intensive-applications\ntype: book\ntitle: Designing Data-Intensive Applications\naliases: []\ndomains:\n  - business\nsources:\n  - summary-book-martin-kleppmann-designing-data-intensive-applications\nauthor:\n  - martin-kleppmann\nfinished: 2026-03-15\n---\nBody\n",
    )
    _write(
        tmp_path / "memory" / "sources" / "substack" / "thegeneralist" / "2026-03-15-on-trust.md",
        "---\nid: 2026-03-15-on-trust\ntype: article\ntitle: On Trust\nexternal_id: substack-190000001\naliases: []\ndomains:\n  - business\n---\nBody\n",
    )
    _write(
        tmp_path / "memory" / "summaries" / "summary-substack-190000001.md",
        "---\nid: summary-substack-190000001\ntype: summary\ntitle: Summary On Trust\nsource_type: substack\nexternal_id: substack-190000001\naliases: []\ndomains:\n  - business\n---\nBody\n",
    )

    report = run_vault_housekeeping(tmp_path, apply=False)

    assert any(issue.lane == "substack" and issue.issue == "legacy summary slug drift" for issue in report.pair_issues)
    assert any(issue.lane == "books" and issue.title == "Designing Data-Intensive Applications" for issue in report.duplicate_issues)
    assert report.filename_noise["leading_external_id"] >= 0
    assert any(plan.new_path.endswith("summary-2026-03-15-on-trust.md") for plan in report.rename_plans)


def test_vault_housekeeping_apply_renames_substack_summary_and_rewrites_index(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _write(
        tmp_path / "memory" / "sources" / "substack" / "thegeneralist" / "2026-03-15-on-trust.md",
        "---\nid: 2026-03-15-on-trust\ntype: article\ntitle: On Trust\nexternal_id: substack-190000001\naliases: []\ndomains:\n  - business\nsources:\n  - \"[[summary-substack-190000001]]\"\n---\nBody\n",
    )
    _write(
        tmp_path / "memory" / "summaries" / "summary-substack-190000001.md",
        "---\nid: summary-substack-190000001\ntype: summary\ntitle: Summary On Trust\nsource_type: substack\nexternal_id: substack-190000001\naliases: []\ndomains:\n  - business\n---\nBody\n",
    )
    _write(
        tmp_path / "memory" / "INDEX.md",
        "# INDEX\n\n- [[summary-substack-190000001]]\n",
    )

    report = run_vault_housekeeping(tmp_path, apply=True)

    assert (tmp_path / "memory" / "summaries" / "summary-2026-03-15-on-trust.md").exists()
    assert not (tmp_path / "memory" / "summaries" / "summary-substack-190000001.md").exists()
    summary_text = (tmp_path / "memory" / "summaries" / "summary-2026-03-15-on-trust.md").read_text(encoding="utf-8")
    assert "id: summary-2026-03-15-on-trust" in summary_text
    assert 'aliases:\n  - summary-substack-190000001' in summary_text
    index_text = (tmp_path / "memory" / "INDEX.md").read_text(encoding="utf-8")
    assert "[[summary-2026-03-15-on-trust]]" in index_text
    assert any(path.endswith("summary-substack-190000001.md") for path in report.deleted_paths)
