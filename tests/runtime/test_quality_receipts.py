from __future__ import annotations

from pathlib import Path

from mind.services.ingest_contract import NormalizedSource
from mind.services.quality_receipts import build_quality_receipt


def test_build_quality_receipt_captures_propagate_error_and_queue_counts(tmp_path: Path) -> None:
    source = NormalizedSource(
        source_id="youtube-abc123xyz00",
        source_kind="youtube",
        external_id="youtube-abc123xyz00",
        canonical_url="https://www.youtube.com/watch?v=abc123xyz00",
        title="Test Video",
        transcript_text="hello world",
    )
    envelope = {
        "pass_a": {
            "summary": {
                "key_claims": [],
            },
        },
        "pass_d": {},
        "verification": {
            "transcription_path": "transcript-api",
        },
    }

    receipt = build_quality_receipt(
        repo_root=tmp_path,
        source=source,
        envelope=envelope,
        propagate={
            "propagate_discovered_count": 3,
            "propagate_queued_count": 2,
            "fanout_outcomes": [
                {
                    "stage": "propagate",
                    "summary": "RuntimeError: boom",
                }
            ],
        },
        materialized={"video": str(tmp_path / "memory" / "sources" / "youtube" / "video.md")},
        executed_at="2026-04-17",
    )

    assert receipt["propagate_status"] == "error"
    assert receipt["propagate_detail"] == "RuntimeError: boom"
    assert receipt["fanout_discovered_count"] == 3
    assert receipt["fanout_queued_count"] == 2
