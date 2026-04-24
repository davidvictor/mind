from __future__ import annotations

import json
import shutil
from pathlib import Path

from mind.services.atom_page_repair import run_atom_page_repair
from tests.paths import EXAMPLES_ROOT


def _copy_harness(tmp_path: Path) -> Path:
    target = tmp_path / "thin-harness"
    shutil.copytree(EXAMPLES_ROOT, target)
    return target


def test_atom_page_repair_dry_run_reports_rewrites(tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    target = root / "memory" / "inbox" / "probationary" / "concepts" / "2026-04-16-agent-layer.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        "---\n"
        "id: agent-layer\n"
        "type: concept\n"
        "title: Agent Layer\n"
        "status: active\n"
        "created: 2026-04-16\n"
        "last_updated: 2026-04-16\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/concept\n  - signal/working\n"
        "domains:\n  - meta\n"
        "relates_to: []\n"
        "sources:\n  - \"[[summary-agent-source]]\"\n"
        "lifecycle_state: probationary\n"
        "last_evidence_date: 2026-04-16\n"
        "evidence_count: 1\n"
        "category: null\n"
        "first_encountered: 2026-04-16\n"
        "last_dream_pass: 2026-04-16\n"
        "---\n\n"
        "# A thin stub\n\n"
        "## Evidence log\n\n"
        "- 2026-04-16 — [[summary-agent-source]] — first signal\n",
        encoding="utf-8",
    )
    cache_path = root / "raw" / "transcripts" / "article" / "article-agent-source.pass_d.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(
            {
                "_llm": {"prompt_version": "dream.pass-d.v3"},
                "data": {
                    "q1_matches": [],
                    "q2_candidates": [
                        {
                            "type": "concept",
                            "proposed_id": "agent-layer",
                            "title": "Agent Layer",
                            "description": "A distinct layer where agents orchestrate work across tools.",
                            "tldr": "Agents orchestrate work across tools.",
                            "snippet": "agent layer",
                            "polarity": "neutral",
                            "rationale": "distinct architectural layer",
                            "domains": ["work", "craft"],
                            "in_conversation_with": ["source-to-atom-promotion"],
                        }
                    ],
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    report = run_atom_page_repair(root, apply=False)

    assert report.rewritten_pages >= 1
    assert "memory/inbox/probationary/concepts/2026-04-16-agent-layer.md" in report.details


def test_atom_page_repair_apply_rewrites_from_cache_and_rebuilds_atom_cache(tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    target = root / "memory" / "inbox" / "probationary" / "stances" / "2026-04-16-durable-data-moat.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        "---\n"
        "id: durable-data-moat\n"
        "type: stance\n"
        "title: Durable Data Moat\n"
        "status: active\n"
        "created: 2026-04-16\n"
        "last_updated: 2026-04-16\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/stance\n  - signal/working\n"
        "domains:\n  - meta\n"
        "relates_to: []\n"
        "sources:\n  - \"[[summary-agent-source]]\"\n"
        "position: null\n"
        "confidence: probationary\n"
        "evidence_for_count: 0\n"
        "evidence_against_count: 0\n"
        "owner_alignment: unknown\n"
        "lifecycle_state: probationary\n"
        "last_evidence_date: 2026-04-16\n"
        "last_dream_pass: 2026-04-16\n"
        "evidence_count: 1\n"
        "---\n\n"
        "# A thin stub\n\n"
        "## Evidence log\n\n"
        "- 2026-04-16 — [[summary-agent-source]] — first signal\n",
        encoding="utf-8",
    )
    cache_path = root / "raw" / "transcripts" / "article" / "article-agent-source.pass_d.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(
            {
                "_llm": {"prompt_version": "dream.pass-d.v3"},
                "data": {
                    "q1_matches": [],
                    "q2_candidates": [
                        {
                            "type": "stance",
                            "proposed_id": "durable-data-moat",
                            "title": "Durable Data Moat",
                            "description": "Expert-curated domain data is a durable moat.",
                            "tldr": "Expert-curated domain data is a durable moat.",
                            "snippet": "domain data moat",
                            "polarity": "for",
                            "rationale": "recurs across sources",
                            "domains": ["work"],
                            "in_conversation_with": ["three-window-temporal-validation"],
                            "position": "Expert-curated domain data is a durable moat.",
                        }
                    ],
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    report = run_atom_page_repair(root, apply=True)
    text = target.read_text(encoding="utf-8")

    assert report.rewritten_pages >= 1
    assert report.rebuilt_atom_cache is True
    assert "domains:\n  - work\n" in text
    assert 'relates_to:\n  - "[[three-window-temporal-validation]]"\n' in text
    assert "position: Expert-curated domain data is a durable moat.\n" in text
    assert "# Durable Data Moat\n\nExpert-curated domain data is a durable moat.\n" in text
    assert "\n\n\n" not in text[text.index("---\n\n") + 5 : text.index("## Evidence log")]
