from __future__ import annotations

import json
from pathlib import Path

from mind.services.content_policy import normalize_book_classification, normalize_youtube_classification, run_content_policy_migration, run_content_policy_repair
from tests.support import write_repo_config


def _write_page(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_content_policy_repair_reports_and_removes_ignored_youtube_residue(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_indexes=True)

    source = tmp_path / "memory" / "sources" / "youtube" / "ignore" / "ngLQfhJZ7Rs-here-s-why-the-laferrari-is-the-3-5-million-ultimate-ferrari.md"
    summary = tmp_path / "memory" / "summaries" / "summary-yt-ngLQfhJZ7Rs.md"
    inbox = tmp_path / "memory" / "inbox" / "probationary" / "concepts" / "2026-04-13-concept-car-mode.md"
    active = tmp_path / "memory" / "playbooks" / "keep-manual-review.md"

    _write_page(source, "---\ntitle: Ignore source\n---\n")
    _write_page(summary, "---\ntitle: Ignore summary\n---\n[[summary-yt-ngLQfhJZ7Rs]]\n")
    _write_page(inbox, "---\ntitle: Inbox artifact\n---\nDerived from [[summary-yt-ngLQfhJZ7Rs]]\n")
    _write_page(active, "---\ntitle: Active artifact\n---\nManual review for [[summary-yt-ngLQfhJZ7Rs]]\n")

    dry = run_content_policy_repair(tmp_path, apply=False)

    assert dry.youtube_counts["ignore"] == 1
    assert "memory/sources/youtube/ignore/ngLQfhJZ7Rs-here-s-why-the-laferrari-is-the-3-5-million-ultimate-ferrari.md" in dry.ignore_source_paths
    assert "memory/summaries/summary-yt-ngLQfhJZ7Rs.md" in dry.ignore_summary_paths
    assert "memory/inbox/probationary/concepts/2026-04-13-concept-car-mode.md" in dry.downstream_inbox_paths
    assert "memory/playbooks/keep-manual-review.md" in dry.downstream_noninbox_paths
    assert source.exists()
    assert summary.exists()
    assert inbox.exists()
    assert active.exists()

    applied = run_content_policy_repair(tmp_path, apply=True)

    assert not source.exists()
    assert not summary.exists()
    assert not inbox.exists()
    assert active.exists()
    assert "memory/playbooks/keep-manual-review.md" in applied.downstream_noninbox_paths
    assert applied.report_path == "raw/reports/repair-content-policy-report.json"


def test_content_policy_repair_respects_excluded_classification_cache(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_indexes=True)

    source = tmp_path / "memory" / "sources" / "youtube" / "personal" / "0GGxyIA6Yr0-test-video.md"
    summary = tmp_path / "memory" / "summaries" / "summary-yt-0GGxyIA6Yr0.md"
    inbox = tmp_path / "memory" / "inbox" / "probationary" / "concepts" / "2026-04-13-test-video.md"
    cache = tmp_path / "raw" / "transcripts" / "youtube" / "0GGxyIA6Yr0.classification.json"

    _write_page(source, "---\ntitle: Source\ncategory: personal\nretention: keep\ndomains:\n  - personal\nsynthesis_mode: light\n---\n")
    _write_page(summary, "---\ntitle: Summary\n---\n[[summary-yt-0GGxyIA6Yr0]]\n")
    _write_page(inbox, "---\ntitle: Inbox artifact\n---\nDerived from [[summary-yt-0GGxyIA6Yr0]]\n")
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(
        json.dumps(
            {
                "_llm": {"prompt_version": "youtube.classification.v3"},
                "data": {
                    "retention": "exclude",
                    "domains": ["personal"],
                    "synthesis_mode": "none",
                    "category": "ignore",
                    "confidence": "high",
                    "reasoning": "reviewed exclusion",
                },
            }
        ),
        encoding="utf-8",
    )

    dry = run_content_policy_repair(tmp_path, apply=False)
    assert "memory/sources/youtube/personal/0GGxyIA6Yr0-test-video.md" in dry.ignore_source_paths

    applied = run_content_policy_repair(tmp_path, apply=True)
    assert not source.exists()
    assert not summary.exists()
    assert not inbox.exists()
    assert "memory/sources/youtube/personal/0GGxyIA6Yr0-test-video.md" in applied.deleted_paths


def test_normalize_youtube_classification_converts_legacy_category() -> None:
    payload = normalize_youtube_classification({"category": "business", "confidence": "high", "reasoning": "x"})
    assert payload["retention"] == "keep"
    assert payload["domains"] == ["business"]
    assert payload["synthesis_mode"] == "deep"
    assert payload["category"] == "business"


def test_normalize_book_classification_converts_legacy_fiction() -> None:
    payload = normalize_book_classification({"category": "fiction", "confidence": "medium", "reasoning": "x"})
    assert payload["retention"] == "keep"
    assert payload["domains"] == ["personal"]
    assert payload["synthesis_mode"] == "light"
    assert payload["category"] == "fiction"


def test_content_policy_migration_reports_and_applies_youtube_metadata(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    source = tmp_path / "memory" / "sources" / "youtube" / "personal" / "abc123xyz00-test-video.md"
    summary = tmp_path / "memory" / "summaries" / "summary-yt-abc123xyz00.md"
    _write_page(source, "---\nid: abc123xyz00-test-video\ntitle: History of Science\ntype: video\ncategory: personal\ndomains:\n  - learning\n---\n# Body\n")
    _write_page(summary, "---\nid: summary-yt-abc123xyz00\ntitle: Summary\ntype: summary\ndomains:\n  - learning\n---\n# Summary\n")

    dry = run_content_policy_migration(tmp_path, lane="youtube", apply=False)
    assert dry.sources_scanned == 1
    assert dry.summaries_scanned == 1
    assert dry.projected_policy_buckets["keep:personal:light"] == 1
    assert dry.review_candidates == ["memory/sources/youtube/personal/abc123xyz00-test-video.md"]

    applied = run_content_policy_migration(tmp_path, lane="youtube", apply=True)
    assert "memory/sources/youtube/personal/abc123xyz00-test-video.md" in applied.updated_paths
    source_text = source.read_text(encoding="utf-8")
    summary_text = summary.read_text(encoding="utf-8")
    assert "retention: keep" in source_text
    assert "domains:\n  - personal" in source_text
    assert "synthesis_mode: light" in source_text
    assert "retention: keep" in summary_text
