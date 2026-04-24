from __future__ import annotations

import json
import shutil
from pathlib import Path

from mind.cli import main
from mind.dream.quality import QUALITY_ADAPTER, evaluate_and_persist_quality
from mind.runtime_state import RuntimeState
from mind.services.llm_cache import LLMCacheIdentity, write_llm_cache
from mind.services.quality_receipts import QUALITY_RECEIPT_VERSION, quality_receipt_path
from scripts.atoms.pass_d import PASS_D_TASK_CLASS, PASS_D_PROMPT_VERSION
from tests.paths import EXAMPLES_ROOT


def _copy_harness(tmp_path: Path) -> Path:
    target = tmp_path / "thin-harness"
    shutil.copytree(EXAMPLES_ROOT, target)
    cfg = target / "config.yaml"
    cfg.write_text(cfg.read_text(encoding="utf-8").replace("enabled: false", "enabled: true", 1), encoding="utf-8")
    return target


def _patch_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)
    monkeypatch.setattr("mind.dream.common.project_root", lambda: root)
    monkeypatch.setattr("mind.cli._project_root", lambda: root)


def _write_youtube_summary(root: Path, video_id: str, *, source_date: str = "2026-04-10") -> None:
    (root / "memory" / "summaries").mkdir(parents=True, exist_ok=True)
    (root / "memory" / "summaries" / f"summary-yt-{video_id}.md").write_text(
        "---\n"
        f"id: summary-yt-{video_id}\n"
        "type: summary\n"
        'title: "Summary"\n'
        "status: active\n"
        f"created: {source_date}\n"
        f"last_updated: {source_date}\n"
        "aliases: []\n"
        "tags:\n  - domain/learning\n  - function/summary\n  - signal/canon\n"
        "domains:\n  - learning\n"
        "source_path: raw/transcripts/youtube/fake.json\n"
        "source_type: video\n"
        f"source_date: {source_date}\n"
        f"ingested: {source_date}\n"
        f"external_id: youtube-{video_id}\n"
        "entities: []\n"
        "concepts: []\n"
        "---\n\n"
        "# Summary\n\nA canonical YouTube summary.\n",
        encoding="utf-8",
    )


def _write_youtube_quality_artifacts(
    root: Path,
    video_id: str,
    *,
    pass_d_identity: LLMCacheIdentity,
    include_quote_sidecar: bool = False,
) -> None:
    transcript_dir = root / "raw" / "transcripts" / "youtube"
    transcript_dir.mkdir(parents=True, exist_ok=True)
    (transcript_dir / f"{video_id}.json").write_text(
        json.dumps(
            {
                "key_claims": [
                    {
                        "claim": "Trust compounds.",
                        "evidence_quote": "trust compounds",
                        "quote_unverified": False,
                    }
                ],
                "entities": {"people": ["Example Owner"]},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (transcript_dir / f"{video_id}.transcription.json").write_text(
        json.dumps(
            {
                "transcript": "Trust compounds when systems stay local-first.",
                "transcription_path": "transcript-api",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    if include_quote_sidecar:
        (transcript_dir / f"{video_id}.quote-warnings.json").write_text(
            json.dumps({"unverified_claims": [{"claim": "Trust compounds."}]}, indent=2),
            encoding="utf-8",
        )
    write_llm_cache(
        transcript_dir / f"youtube-{video_id}.pass_d.json",
        identity=pass_d_identity,
        data={"q1_matches": [], "q2_candidates": []},
    )


def _write_quality_receipt(root: Path, *, lane: str, source_id: str, payload: dict[str, object]) -> None:
    path = quality_receipt_path(repo_root=root, lane=lane, source_id=source_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    materialized = {"receipt_version": QUALITY_RECEIPT_VERSION, **payload}
    path.write_text(json.dumps(materialized, indent=2), encoding="utf-8")


def _mark_probationary_ready(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    text = text.replace("created: 2026-04-10", "created: 2026-04-01")
    text = text.replace("evidence_count: 0", "evidence_count: 3")
    if "[[summary-yt-phase5-gated]]" not in text:
        text = text.rstrip() + "\n- 2026-04-10 [[summary-yt-phase5-gated]]\n"
    path.write_text(text, encoding="utf-8")


def test_evaluate_and_persist_quality_blocks_stale_youtube_lane(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    good_identity = LLMCacheIdentity(
        task_class=PASS_D_TASK_CLASS,
        provider="gemini",
        model="google/gemini-current",
        transport="ai_gateway",
        api_family="responses",
        input_mode="text",
        prompt_version="pass-d.v1",
    )
    bad_identity = LLMCacheIdentity(
        task_class=PASS_D_TASK_CLASS,
        provider="gemini",
        model="google/gemini-stale",
        transport="ai_gateway",
        api_family="responses",
        input_mode="text",
        prompt_version="pass-d.v1",
    )
    monkeypatch.setattr(
        "mind.dream.quality._acceptable_dream_identities",
        lambda: [good_identity.to_dict()],
    )
    for index in range(10):
        video_id = f"phase5-{index:02d}"
        _write_youtube_summary(root, video_id)
        _write_youtube_quality_artifacts(root, video_id, pass_d_identity=bad_identity)

    snapshot = evaluate_and_persist_quality(persist=True, report_key="pytest")

    youtube = snapshot["lanes"]["youtube"]
    assert youtube["state"] == "blocked"
    assert "route_policy_stale" in youtube["reasons"]
    assert youtube["recent_sources"] == 10
    persisted = RuntimeState.for_repo_root(root).get_adapter_state(QUALITY_ADAPTER)
    assert persisted is not None
    assert str(persisted.get("report_path") or "").endswith(".md")


def test_acceptable_dream_identities_include_legacy_pass_d_route(monkeypatch) -> None:
    current_identity = LLMCacheIdentity(
        task_class=PASS_D_TASK_CLASS,
        provider="gemini",
        model="google/gemini-3.1-flash-lite-preview",
        transport="ai_gateway",
        api_family="responses",
        input_mode="text",
        prompt_version=PASS_D_PROMPT_VERSION,
    )
    legacy_identity = LLMCacheIdentity(
        task_class="dream",
        provider="anthropic",
        model="anthropic/claude-sonnet-4.6",
        transport="ai_gateway",
        api_family="responses",
        input_mode="text",
        prompt_version=PASS_D_PROMPT_VERSION,
    )

    class FakeService:
        def cache_identities(self, *, task_class: str, prompt_version: str):
            if task_class == PASS_D_TASK_CLASS:
                return [current_identity]
            if task_class == "dream":
                return [legacy_identity]
            return []

    monkeypatch.setattr("mind.dream.quality.get_llm_service", lambda: FakeService())

    identities = __import__("mind.dream.quality", fromlist=["_acceptable_dream_identities"])._acceptable_dream_identities()

    assert current_identity.to_dict() in identities
    assert legacy_identity.to_dict() in identities


def test_evaluate_and_persist_quality_accepts_text_prompt_fingerprint_compatibility(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    compatible_identity = LLMCacheIdentity(
        task_class="dream",
        provider="anthropic",
        model="anthropic/claude-sonnet-4.6",
        transport="ai_gateway",
        api_family="responses",
        input_mode="text",
        prompt_version=PASS_D_PROMPT_VERSION,
        request_fingerprint={"kind": "text-prompt"},
    )
    cached_identity = LLMCacheIdentity(
        task_class="dream",
        provider="anthropic",
        model="anthropic/claude-sonnet-4.6",
        transport="ai_gateway",
        api_family="responses",
        input_mode="text",
        prompt_version=PASS_D_PROMPT_VERSION,
        request_fingerprint={
            "instructions_sha256": "",
            "input_parts": [
                {
                    "kind": "text",
                    "text_sha256": "abc",
                }
            ],
            "request_metadata": {},
            "has_tools": False,
            "has_response_schema": False,
        },
    )
    monkeypatch.setattr(
        "mind.dream.quality._acceptable_dream_identities",
        lambda: [compatible_identity.to_dict()],
    )
    for index in range(10):
        video_id = f"compat-{index:02d}"
        _write_youtube_summary(root, video_id)
        _write_youtube_quality_artifacts(root, video_id, pass_d_identity=cached_identity)

    snapshot = evaluate_and_persist_quality(persist=False, report_key="compat")

    youtube = snapshot["lanes"]["youtube"]
    assert youtube["metrics"]["route_policy_compliance"] == 1.0
    assert "route_policy_stale" not in youtube["reasons"]


def test_source_grounded_reads_wrapped_youtube_transcription_cache(tmp_path: Path) -> None:
    from mind.services.llm_cache import LLMCacheIdentity, write_llm_cache

    identity = LLMCacheIdentity(
        task_class="transcription",
        provider="gemini",
        model="google/gemini-3.1-pro-preview",
        transport="ai_gateway",
        api_family="responses",
        input_mode="media",
        prompt_version="youtube.transcription.v1",
    )
    payload = {
        "transcript": "Wrapped transcript text.",
        "transcription_path": "transcript-api",
    }
    write_llm_cache(
        tmp_path / "raw" / "transcripts" / "youtube" / "video-1.transcription.json",
        identity=identity,
        data=payload,
    )

    quality = __import__("mind.dream.quality", fromlist=["_source_grounded"])
    grounded = quality._source_grounded(
        tmp_path,
        lane="youtube",
        frontmatter={},
        source_id="video-1",
    )

    assert grounded is True


def test_source_identifiers_strip_book_summary_suffix_from_source_path(tmp_path: Path) -> None:
    quality = __import__("mind.dream.quality", fromlist=["_source_identifiers"])
    identifiers = quality._source_identifiers(
        path=tmp_path / "memory" / "sources" / "books" / "business" / "andrew-ross-sorkin-1929.md",
        frontmatter={
            "id": "andrew-ross-sorkin-1929",
            "source_path": "raw/research/books/andrew-ross-sorkin-1929.summary.json",
        },
        lane="book",
    )

    assert identifiers["pass_d_source_id"] == "book-andrew-ross-sorkin-1929"


def test_evaluate_and_persist_quality_prefers_receipt_entity_and_fanout_metrics(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    identity = LLMCacheIdentity(
        task_class=PASS_D_TASK_CLASS,
        provider="anthropic",
        model="anthropic/claude-sonnet-4.6",
        transport="ai_gateway",
        api_family="responses",
        input_mode="text",
        prompt_version= "pass-d.v1",
    )
    monkeypatch.setattr(
        "mind.dream.quality._acceptable_dream_identities",
        lambda: [identity.to_dict()],
    )
    for index in range(10):
        video_id = f"receipt-{index:02d}"
        _write_youtube_summary(root, video_id)
        _write_youtube_quality_artifacts(root, video_id, pass_d_identity=identity)
        _write_quality_receipt(
            root,
            lane="youtube",
            source_id=f"youtube-{video_id}",
            payload={
                "source_id": f"youtube-{video_id}",
                "source_kind": "youtube",
                "source_date": "2026-04-10",
                "route_identity": identity.to_dict(),
                "source_grounded": True,
                "pass_d_status": "ok",
                "quote_claim_count": 1,
                "quote_unverified_count": 0,
                "entity_logged_count": 0,
                "fanout_discovered_count": 3,
                "fanout_queued_count": 0,
                "parity_features": {
                    "quote_verification_supported": True,
                    "pass_d_outcomes_exposed": True,
                    "entity_logging_supported": True,
                    "fanout_count_supported": True,
                    "context_reuse_supported": True,
                },
            },
        )

    snapshot = evaluate_and_persist_quality(persist=False, report_key="receipt-metrics")
    youtube = snapshot["lanes"]["youtube"]

    assert youtube["metrics"]["entity_log_yield"] == 0.0
    assert youtube["metrics"]["fanout_yield"] == 0.0
    assert "entity_yield_low" in youtube["reasons"]
    assert "fanout_yield_low" in youtube["reasons"]


def test_evaluate_and_persist_quality_marks_partial_when_grounding_misses_trusted_threshold(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    identity = LLMCacheIdentity(
        task_class=PASS_D_TASK_CLASS,
        provider="anthropic",
        model="anthropic/claude-sonnet-4.6",
        transport="ai_gateway",
        api_family="responses",
        input_mode="text",
        prompt_version="pass-d.v1",
    )
    monkeypatch.setattr(
        "mind.dream.quality._acceptable_dream_identities",
        lambda: [identity.to_dict()],
    )
    for index in range(10):
        summary_id = f"summary-book-grounding-{index:02d}"
        (root / "memory" / "summaries" / f"{summary_id}.md").write_text(
            "---\n"
            f"id: {summary_id}\n"
            "type: summary\n"
            'title: "Summary"\n'
            "status: active\n"
            "created: 2026-04-10\n"
            "last_updated: 2026-04-10\n"
            "aliases: []\n"
            "tags: []\n"
            "domains:\n  - learning\n"
            "source_type: book\n"
            "source_kind: research\n"
            "source_date: 2026-04-10\n"
            "---\n\n# Summary\n",
            encoding="utf-8",
        )
        grounded = index < 5
        _write_quality_receipt(
            root,
            lane="book",
            source_id=f"book-grounding-{index:02d}",
            payload={
                "source_id": f"book-grounding-{index:02d}",
                "source_kind": "book",
                "source_date": "2026-04-10",
                "route_identity": identity.to_dict(),
                "source_grounded": grounded,
                "pass_d_status": "ok",
                "quote_claim_count": 0,
                "quote_unverified_count": 0,
                "entity_logged_count": 1,
                "fanout_discovered_count": 0,
                "fanout_queued_count": 0,
                "parity_features": {
                    "quote_verification_supported": True,
                    "pass_d_outcomes_exposed": True,
                    "entity_logging_supported": True,
                    "fanout_count_supported": False,
                    "context_reuse_supported": True,
                },
            },
        )

    snapshot = evaluate_and_persist_quality(persist=False, report_key="grounding")
    book = snapshot["lanes"]["book"]
    assert book["metrics"]["source_grounded_coverage"] == 0.5
    assert book["state"] == "partial-fidelity"
    assert "source_grounding_low" in book["reasons"]


def test_light_blocks_when_only_canonical_lane_is_blocked(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    for existing in (root / "memory" / "summaries").glob("*.md"):
        existing.unlink()
    _write_youtube_summary(root, "phase5-only")
    monkeypatch.setattr(
        "mind.dream.light.evaluate_and_persist_quality",
        lambda persist, report_key: {
            "lanes": {
                "youtube": {"state": "blocked", "reasons": ["pass_d_unstable"]},
                "book": {"state": "blocked", "reasons": ["no_recent_sources"]},
                "article": {"state": "blocked", "reasons": ["no_recent_sources"]},
                "substack": {"state": "blocked", "reasons": ["no_recent_sources"]},
            }
        },
    )

    assert main(["dream", "light"]) == 1
    out = capsys.readouterr().out
    assert "light dream blocked by lane quality" in out


def test_deep_holds_probationary_atom_when_only_partial_lane_evidence_exists(tmp_path: Path, monkeypatch, capsys) -> None:
    from scripts.atoms.probationary import create_or_extend

    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    _write_youtube_summary(root, "phase5-gated")
    create_or_extend(
        type="inquiry",
        proposed_id="phase5-lane-trust",
        title="Phase5 lane trust",
        description="Phase5 lane trust?",
        snippet="Phase5 lane trust?",
        polarity="neutral",
        rationale="test",
        date="2026-04-10",
        source_link="[[summary-yt-phase5-gated]]",
        repo_root=root,
    )
    probationary = root / "memory" / "inbox" / "probationary" / "inquiries" / "2026-04-10-phase5-lane-trust.md"
    _mark_probationary_ready(probationary)
    monkeypatch.setattr(
        "mind.dream.deep.evaluate_and_persist_quality",
        lambda persist, report_key: {
            "lanes": {
                "youtube": {"state": "partial-fidelity", "reasons": ["parity_gap"]},
                "book": {"state": "blocked", "reasons": ["no_recent_sources"]},
                "article": {"state": "blocked", "reasons": ["no_recent_sources"]},
                "substack": {"state": "blocked", "reasons": ["no_recent_sources"]},
            }
        },
    )

    assert main(["dream", "deep"]) == 0
    out = capsys.readouterr().out
    assert "trusted_sources=0 degraded_sources=1 blocked_sources=0" in out
    assert probationary.exists()
    assert not (root / "memory" / "inquiries" / "phase5-lane-trust.md").exists()


def test_state_health_surfaces_lane_trust_snapshot(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    RuntimeState.for_repo_root(root).upsert_adapter_state(
        adapter=QUALITY_ADAPTER,
        state={
            "evaluated_at": "2026-04-12T20:00:00Z",
            "report_path": "raw/reports/dream/quality/2026-04-12-quality-report-light.md",
            "lanes": {
                "youtube": {
                    "state": "partial-fidelity",
                    "recent_sources": 12,
                    "reasons": ["quote_coverage_low"],
                    "metrics": {"pass_d_success_rate": 0.95, "route_policy_compliance": 1.0},
                },
                "book": {"state": "bootstrap-only", "recent_sources": 4, "reasons": ["insufficient_sample_size"], "metrics": {}},
                "article": {"state": "trusted", "recent_sources": 18, "reasons": [], "metrics": {}},
                "substack": {"state": "trusted", "recent_sources": 24, "reasons": [], "metrics": {}},
            },
        },
    )

    assert main(["state", "health"]) == 0
    out = capsys.readouterr().out
    assert "Dream lane trust:" in out
    assert "YouTube\tstate=partial-fidelity" in out
    assert "quote_coverage_low" in out
    assert "fanout=" in out
    assert "parity=" in out
