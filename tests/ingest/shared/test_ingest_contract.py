from __future__ import annotations

import pytest

from mind.services.ingest_contract import (
    LifecycleHandlers,
    NormalizedSource,
    make_enrichment_envelope,
    parse_enrichment_envelope,
    run_ingestion_lifecycle,
    run_ingestion_window,
)


def _source(**overrides) -> NormalizedSource:
    data = {
        "source_id": "source-1",
        "source_kind": "file",
        "external_id": "",
        "canonical_url": "/tmp/example.md",
        "title": "Example",
        "creator_candidates": [],
        "published_at": "2026-04-09",
        "discovered_at": "2026-04-09",
        "source_metadata": {},
        "discovered_links": [],
        "provenance": {"adapter": "file"},
        "raw_text": "hello",
    }
    data.update(overrides)
    return NormalizedSource(**data)


def test_normalized_source_requires_exactly_one_primary_content_field() -> None:
    with pytest.raises(ValueError):
        _source(raw_text="", body_markdown="", transcript_text="")
    with pytest.raises(ValueError):
        _source(raw_text="a", body_markdown="b")


def test_normalized_source_primary_content_returns_populated_field() -> None:
    source = _source(raw_text="", body_markdown="body")
    assert source.primary_content == "body"


def test_parse_enrichment_envelope_validates_required_keys() -> None:
    with pytest.raises(ValueError):
        parse_enrichment_envelope({"schema_version": 1, "source_id": "x"})


def test_parse_enrichment_envelope_enforces_source_id_match() -> None:
    with pytest.raises(ValueError):
        parse_enrichment_envelope(make_enrichment_envelope(source_id="x"), expected_source_id="y")


def test_run_ingestion_lifecycle_executes_phases_in_order() -> None:
    calls: list[str] = []

    def understand(source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        calls.append(f"understand:{source.source_id}")
        assert envelope["pass_b"] == {}
        return {"summary": source.primary_content}

    def personalize(source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        calls.append(f"personalize:{source.source_id}")
        assert envelope["pass_a"] == {"summary": "hello"}
        return {"relevance": "low"}

    def materialize(source: NormalizedSource, envelope: dict[str, object]) -> str:
        calls.append(f"materialize:{source.source_id}")
        assert envelope["pass_b"] == {"relevance": "low"}
        return "page-id"

    def fanout(source: NormalizedSource, envelope: dict[str, object], materialized: str) -> dict[str, object]:
        calls.append(f"fanout:{source.source_id}")
        assert materialized == "page-id"
        return {"queued": False}

    result = run_ingestion_lifecycle(
        source=_source(),
        understand=understand,
        personalize=personalize,
        materialize=materialize,
        propagate=fanout,
    )

    assert calls == [
        "understand:source-1",
        "personalize:source-1",
        "materialize:source-1",
        "fanout:source-1",
    ]
    assert result.materialized == "page-id"
    assert result.propagate == {"queued": False}


def test_run_ingestion_lifecycle_isolates_phase_input_from_mutation() -> None:
    def understand(_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        envelope["pass_b"] = {"corrupted": True}
        return {"summary": "safe"}

    result = run_ingestion_lifecycle(source=_source(), understand=understand)

    assert result.envelope["pass_a"] == {"summary": "safe"}
    assert result.envelope["pass_b"] == {}


def test_run_ingestion_lifecycle_promotes_materialization_hints_to_top_level() -> None:
    def understand(_source: NormalizedSource, _envelope: dict[str, object]) -> dict[str, object]:
        return {
            "summary": "safe",
            "materialization_hints": {"additional_author_hints": ["Bob Writer"]},
        }

    result = run_ingestion_lifecycle(source=_source(), understand=understand)

    assert result.envelope["pass_a"] == {"summary": "safe"}
    assert result.envelope["materialization_hints"] == {"additional_author_hints": ["Bob Writer"]}


def test_run_ingestion_lifecycle_makes_fanout_non_fatal() -> None:
    def materialize(_source: NormalizedSource, _envelope: dict[str, object]) -> str:
        return "page-id"

    def fanout(_source: NormalizedSource, _envelope: dict[str, object], _materialized: str) -> dict[str, object]:
        raise RuntimeError("fanout boom")

    result = run_ingestion_lifecycle(
        source=_source(),
        materialize=materialize,
        propagate=fanout,
    )

    assert result.materialized == "page-id"
    assert result.propagate == {
        "fanout_outcomes": [
            {
                "status": "error",
                "stage": "propagate",
                "summary": "RuntimeError: fanout boom",
            }
        ]
    }


def test_run_ingestion_window_supports_seeded_replay_ranges() -> None:
    calls: list[str] = []
    seed = make_enrichment_envelope(source_id="source-1")
    seed["pass_a"] = {"summary": "cached"}
    seed["pass_b"] = {"relevance": "cached"}

    def creator_memory(_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        calls.append("pass_c")
        assert envelope["pass_a"] == {"summary": "cached"}
        assert envelope["pass_b"] == {"relevance": "cached"}
        return {"stance_change_note": "delta"}

    def substrate(_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        calls.append("pass_d")
        assert envelope["pass_c"] == {"stance_change_note": "delta"}
        return {"warnings": []}

    result = run_ingestion_window(
        source=_source(),
        handlers=LifecycleHandlers(
            attribute=creator_memory,
            distill=substrate,
        ),
        start_stage="pass_c",
        through_stage="pass_d",
        seed_envelope=seed,
    )

    assert calls == ["pass_c", "pass_d"]
    assert result.envelope["pass_a"] == {"summary": "cached"}
    assert result.envelope["pass_b"] == {"relevance": "cached"}
    assert result.envelope["pass_c"] == {"stance_change_note": "delta"}
    assert result.envelope["pass_d"] == {"warnings": []}
