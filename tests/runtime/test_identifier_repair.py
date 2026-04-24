from __future__ import annotations

from pathlib import Path

from mind.services.graph_registry import GraphRegistry
from mind.services.identifier_repair import run_identifier_repair
from tests.support import write_repo_config


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_identifier_repair_renames_non_ascii_page_and_rewrites_links(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _write(
        tmp_path / "memory" / "concepts" / "lütke-eval-methodology-as-gap-collapse-discipline.md",
        "---\n"
        "id: lütke-eval-methodology-as-gap-collapse-discipline\n"
        "type: concept\n"
        "title: Lütke Eval Methodology as Gap-Collapse Discipline\n"
        "status: active\n"
        "created: 2026-04-18\n"
        "last_updated: 2026-04-22\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/concept\n  - signal/working\n"
        "domains:\n  - meta\n"
        "relates_to: []\n"
        "sources: []\n"
        "lifecycle_state: active\n"
        "evidence_count: 1\n"
        "---\n\n"
        "# Lütke Eval Methodology as Gap-Collapse Discipline\n\n"
        "## TL;DR\n\n"
        "Lütke evals.\n\n"
        "## Evidence log\n\n"
        "- 2026-04-18 — [[summary-a]] — evidence\n",
    )
    _write(
        tmp_path / "memory" / "INDEX.md",
        "# INDEX\n\n- [[lütke-eval-methodology-as-gap-collapse-discipline]]\n",
    )

    report = run_identifier_repair(tmp_path, apply=True)

    assert report.renamed_pages >= 1
    target = tmp_path / "memory" / "concepts" / "lutke-eval-methodology-as-gap-collapse-discipline.md"
    assert target.exists()
    assert not (tmp_path / "memory" / "concepts" / "lütke-eval-methodology-as-gap-collapse-discipline.md").exists()
    text = target.read_text(encoding="utf-8")
    assert "id: lutke-eval-methodology-as-gap-collapse-discipline" in text
    index_text = (tmp_path / "memory" / "INDEX.md").read_text(encoding="utf-8")
    assert "[[lutke-eval-methodology-as-gap-collapse-discipline]]" in index_text
    registry = GraphRegistry.for_repo_root(tmp_path)
    node = registry.get_node("lutke-eval-methodology-as-gap-collapse-discipline")
    assert node is not None
    assert node.page_id.isascii()


def test_identifier_repair_merges_ascii_and_non_ascii_duplicates(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _write(
        tmp_path / "memory" / "concepts" / "lutke-eval-methodology-as-gap-collapse-discipline.md",
        "---\n"
        "id: lutke-eval-methodology-as-gap-collapse-discipline\n"
        "type: concept\n"
        "title: Lutke Eval Methodology as Gap-Collapse Discipline\n"
        "status: active\n"
        "created: 2026-04-18\n"
        "last_updated: 2026-04-21\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/concept\n  - signal/working\n"
        "domains:\n  - meta\n"
        "relates_to: []\n"
        "sources:\n  - \"[[summary-a]]\"\n"
        "lifecycle_state: active\n"
        "evidence_count: 1\n"
        "---\n\n"
        "# Lutke Eval Methodology as Gap-Collapse Discipline\n\n"
        "## TL;DR\n\n"
        "ASCII page.\n\n"
        "## Evidence log\n\n"
        "- 2026-04-18 — [[summary-a]] — evidence\n",
    )
    _write(
        tmp_path / "memory" / "concepts" / "lütke-eval-methodology-as-gap-collapse-discipline.md",
        "---\n"
        "id: lütke-eval-methodology-as-gap-collapse-discipline\n"
        "type: concept\n"
        "title: Lütke Eval Methodology as Gap-Collapse Discipline\n"
        "status: active\n"
        "created: 2026-04-19\n"
        "last_updated: 2026-04-22\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/concept\n  - signal/working\n"
        "domains:\n  - craft\n"
        "relates_to:\n  - \"[[alpha]]\"\n"
        "sources:\n  - \"[[summary-b]]\"\n"
        "lifecycle_state: active\n"
        "evidence_count: 1\n"
        "---\n\n"
        "# Lütke Eval Methodology as Gap-Collapse Discipline\n\n"
        "## TL;DR\n\n"
        "Unicode page.\n\n"
        "## Evidence log\n\n"
        "- 2026-04-19 — [[summary-b]] — evidence\n",
    )

    report = run_identifier_repair(tmp_path, apply=True)

    assert report.merged_pages >= 1
    target = tmp_path / "memory" / "concepts" / "lutke-eval-methodology-as-gap-collapse-discipline.md"
    assert target.exists()
    assert not (tmp_path / "memory" / "concepts" / "lütke-eval-methodology-as-gap-collapse-discipline.md").exists()
    text = target.read_text(encoding="utf-8")
    assert "lütke-eval-methodology-as-gap-collapse-discipline" not in text
    assert 'domains:\n  - meta\n  - craft' in text
    assert 'sources:\n  - "[[summary-a]]"\n  - "[[summary-b]]"' in text
    assert "- 2026-04-18 — [[summary-a]] — evidence" in text
    assert "- 2026-04-19 — [[summary-b]] — evidence" in text
