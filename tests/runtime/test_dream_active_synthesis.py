from __future__ import annotations

import shutil
from pathlib import Path

from mind.dream.common import DreamExecutionContext
from mind.dream.active_synthesis import run_active_synthesis_pass
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


def test_active_synthesis_dry_run_selects_mature_non_seed_atoms(tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    target = root / "memory" / "concepts" / "local-first-systems.md"
    _replace_frontmatter_field(target, "evidence_count", "5")
    text = target.read_text(encoding="utf-8")
    text = text.replace("seed_managed: true\n", "")
    target.write_text(text, encoding="utf-8")

    from scripts.common.vault import Vault

    summary = run_active_synthesis_pass(v=Vault.load(root), today="2026-04-16", dry_run=True)

    assert summary.eligible_count == 1
    assert any("would synthesize mature concept local-first-systems" in item for item in summary.mutations)


def test_active_synthesis_live_writes_mature_sections(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    target = root / "memory" / "concepts" / "local-first-systems.md"
    _replace_frontmatter_field(target, "evidence_count", "5")
    text = target.read_text(encoding="utf-8")
    text = text.replace("seed_managed: true\n", "")
    text = text.replace("relates_to: []\n", 'relates_to:\n  - "[[weekly-review-loop]]"\n')
    text = text.replace(
        "sources: []\n",
        "sources: []\n"
        "typed_relations:\n"
        '  supports:\n    - "[[weekly-review-loop]]"\n',
    )
    target.write_text(text, encoding="utf-8")

    monkeypatch.setattr(
        "mind.dream.active_synthesis.run_active_synthesis",
        lambda **kwargs: __import__("scripts.atoms.synthesis", fromlist=["ActiveSynthesisResult"]).ActiveSynthesisResult(
            intro="A richer local-first concept.",
            tldr="Local-first systems are durable because the record stays inspectable.",
            why_it_matters="It preserves trust and inspectability.",
            mechanism="The durable record stays visible and user-owned.",
            examples=["Obsidian vaults", "Brain repo"],
            in_conversation_with=["user-owned-ai"],
            typed_relations={"supports": ["user-owned-ai"]},
        ),
    )

    from scripts.common.vault import Vault

    summary = run_active_synthesis_pass(v=Vault.load(root), today="2026-04-16", dry_run=False)
    updated = target.read_text(encoding="utf-8")

    assert summary.synthesized_count == 1
    assert "## Why It Matters" in updated
    assert "## Mechanism" in updated
    assert "typed_relations:" in updated
    assert "last_synthesized_at: 2026-04-16" in updated
    assert '"[[user-owned-ai]]"' in updated
    assert '"[[weekly-review-loop]]"' not in updated


def test_active_synthesis_campaign_context_overrides_cooldown_and_dates(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    target = root / "memory" / "concepts" / "local-first-systems.md"
    _replace_frontmatter_field(target, "evidence_count", "5")
    text = target.read_text(encoding="utf-8")
    text = text.replace("seed_managed: true\n", "")
    text = text.replace("last_evidence_date: 2026-04-08\n", "last_evidence_date: 2026-04-17\n")
    text = text.replace("last_updated: 2026-04-08\n", "last_updated: 2026-04-17\n")
    text = text.replace("last_dream_pass: 2026-04-08\n", "last_dream_pass: 2026-04-17\n")
    text = text.replace("evidence_count: 5\n", "evidence_count: 5\nlast_synthesized_at: 2026-04-10\n")
    target.write_text(text, encoding="utf-8")

    monkeypatch.setattr(
        "mind.dream.active_synthesis.run_active_synthesis",
        lambda **kwargs: __import__("scripts.atoms.synthesis", fromlist=["ActiveSynthesisResult"]).ActiveSynthesisResult(
            intro="A richer local-first concept.",
            tldr="Local-first systems are durable because the record stays inspectable.",
            why_it_matters="It preserves trust and inspectability.",
            mechanism="The durable record stays visible and user-owned.",
            examples=["Obsidian vaults", "Brain repo"],
            in_conversation_with=["user-owned-ai"],
            typed_relations={"supports": ["user-owned-ai"]},
        ),
    )

    from scripts.common.vault import Vault

    context = DreamExecutionContext(
        effective_date="2026-04-17",
        mode="campaign",
        lane_relaxation_mode="relation_only",
        campaign_run_id="campaign-test",
    )
    summary = run_active_synthesis_pass(v=Vault.load(root), dry_run=False, context=context)
    updated = target.read_text(encoding="utf-8")

    assert summary.eligible_count == 1
    assert summary.synthesized_count == 1
    assert "last_synthesized_at: 2026-04-17" in updated
