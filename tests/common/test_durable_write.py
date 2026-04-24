from __future__ import annotations

from pathlib import Path

import pytest

from mind.services.durable_write import (
    DurableLinkTarget,
    build_frontmatter,
    ensure_tag_order,
    render_link_target,
    write_contract_page,
)


def test_ensure_tag_order_keeps_default_axes_first() -> None:
    tags = ensure_tag_order("summary", ["article", "custom-topic", "domain/learning"])
    assert tags[:3] == ["domain/learning", "function/summary", "signal/canon"]
    assert tags[3:] == ["custom-topic"]


def test_render_link_target_rejects_invalid_type() -> None:
    with pytest.raises(ValueError):
        render_link_target(DurableLinkTarget(page_type="note", page_id="x"))  # type: ignore[arg-type]


def test_build_frontmatter_rejects_missing_required_fields() -> None:
    with pytest.raises(ValueError):
        build_frontmatter(
            page_type="summary",
            page_id="summary-1",
            title="Summary 1",
            status="active",
            created="2026-04-09",
            last_updated="2026-04-09",
        )


def test_build_frontmatter_rejects_protected_overrides() -> None:
    with pytest.raises(ValueError):
        build_frontmatter(
            page_type="summary",
            page_id="summary-1",
            title="Summary 1",
            status="active",
            created="2026-04-09",
            last_updated="2026-04-09",
            extra_frontmatter={
                "type": "article",
                "source_path": "raw/example.md",
                "source_type": "md",
                "source_date": "2026-04-09",
                "ingested": "2026-04-09",
            },
        )


def test_write_contract_page_writes_summary_with_contract_fields(tmp_path: Path) -> None:
    target = tmp_path / "wiki" / "summaries" / "summary-1.md"
    write_contract_page(
        target,
        page_type="summary",
        title="Summary 1",
        body="# Summary 1\n",
        created="2026-04-09",
        last_updated="2026-04-09",
        domains=["learning"],
        tags=["article"],
        extra_frontmatter={
            "source_path": "raw/example.md",
            "source_type": "md",
            "source_date": "2026-04-09",
            "ingested": "2026-04-09",
        },
    )
    text = target.read_text(encoding="utf-8")
    assert "type: summary" in text
    assert "domain/learning" in text
    assert "function/summary" in text
    assert "signal/canon" in text
    assert "source_type: md" in text
