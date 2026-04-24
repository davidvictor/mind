from __future__ import annotations

import shutil
from pathlib import Path

from mind.dream.common import DreamExecutionContext
from mind.dream.external_grounding import run_external_grounding_pass
from tests.paths import EXAMPLES_ROOT


def _copy_harness(tmp_path: Path) -> Path:
    target = tmp_path / "thin-harness"
    shutil.copytree(EXAMPLES_ROOT, target)
    cfg = target / "config.yaml"
    cfg.write_text(cfg.read_text(encoding="utf-8").replace("enabled: false", "enabled: true", 1), encoding="utf-8")
    return target


def _replace_frontmatter_field(path: Path, key: str, value: str) -> None:
    lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{key}:"):
            lines.append(f"{key}: {value}")
        else:
            lines.append(line)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_external_grounding_dry_run_respects_maturity_and_cap(tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    target = root / "memory" / "concepts" / "local-first-systems.md"
    _replace_frontmatter_field(target, "evidence_count", "8")
    _replace_frontmatter_field(target, "last_evidence_date", "2026-04-16")
    text = target.read_text(encoding="utf-8")
    text = text.replace("seed_managed: true\n", "")
    target.write_text(text, encoding="utf-8")

    from scripts.common.vault import Vault

    summary = run_external_grounding_pass(v=Vault.load(root), today="2026-04-16", dry_run=True)

    assert summary.eligible_count == 1
    assert any("would ground mature concept local-first-systems" in item for item in summary.mutations)


def test_external_grounding_live_records_source_refs_and_cooldown(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    target = root / "memory" / "concepts" / "local-first-systems.md"
    _replace_frontmatter_field(target, "evidence_count", "8")
    _replace_frontmatter_field(target, "last_evidence_date", "2026-04-16")
    text = target.read_text(encoding="utf-8")
    text = text.replace("seed_managed: true\n", "")
    target.write_text(text, encoding="utf-8")

    monkeypatch.setattr(
        "mind.dream.external_grounding.ingest_web_articles",
        lambda **kwargs: [
            __import__("mind.services.web_research", fromlist=["GroundedArticleResult"]).GroundedArticleResult(
                query=kwargs["queries"][0],
                url="https://example.com/article",
                article_page_id="summary-grounded-article",
            )
        ],
    )

    from scripts.common.vault import Vault

    summary = run_external_grounding_pass(v=Vault.load(root), today="2026-04-16", dry_run=False)
    updated = target.read_text(encoding="utf-8")

    assert summary.grounded_count == 1
    assert "last_grounded_at: 2026-04-16" in updated
    assert 'grounding_source_refs:\n  - "[[summary-grounded-article]]"\n' in updated

    second = run_external_grounding_pass(v=Vault.load(root), today="2026-04-16", dry_run=True)
    assert second.eligible_count == 0


def test_external_grounding_campaign_context_overrides_cooldown_and_dates(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    target = root / "memory" / "concepts" / "local-first-systems.md"
    _replace_frontmatter_field(target, "evidence_count", "8")
    _replace_frontmatter_field(target, "last_evidence_date", "2026-04-17")
    text = target.read_text(encoding="utf-8")
    text = text.replace("seed_managed: true\n", "")
    text = text.replace("last_updated: 2026-04-08\n", "last_updated: 2026-04-17\n")
    text = text.replace("evidence_count: 8\n", "evidence_count: 8\nlast_grounded_at: 2026-04-05\n")
    target.write_text(text, encoding="utf-8")

    monkeypatch.setattr(
        "mind.dream.external_grounding.ingest_web_articles",
        lambda **kwargs: [
            __import__("mind.services.web_research", fromlist=["GroundedArticleResult"]).GroundedArticleResult(
                query=kwargs["queries"][0],
                url="https://example.com/article",
                article_page_id="summary-grounded-article",
            )
        ],
    )

    from scripts.common.vault import Vault

    context = DreamExecutionContext(
        effective_date="2026-04-19",
        mode="campaign",
        lane_relaxation_mode="relation_only",
        campaign_run_id="campaign-test",
    )
    summary = run_external_grounding_pass(v=Vault.load(root), dry_run=False, context=context)
    updated = target.read_text(encoding="utf-8")

    assert summary.eligible_count == 1
    assert summary.grounded_count == 1
    assert "last_grounded_at: 2026-04-19" in updated
