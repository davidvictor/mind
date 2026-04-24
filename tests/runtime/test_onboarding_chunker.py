from __future__ import annotations

from pathlib import Path

import mind.services.onboarding_state as onboarding_state
from mind.services.onboarding_chunker import (
    assemble_graph_chunks,
    assemble_merge_chunks,
    chunk_graph_entities,
    chunk_merge_nodes,
)
from mind.services.onboarding_state import (
    ChunkState,
    acquire_chunk_lease,
    ensure_chunk_state,
    mark_chunk_done,
    mark_chunk_failed,
    next_retry_not_before,
    prune_chunk_phase,
    summarize_chunk_phase,
    write_chunk_state,
)


def test_graph_chunking_is_deterministic() -> None:
    semantic = {
        "entities": [
            {"proposal_id": f"projects:item-{index}", "family": "projects", "slug": f"item-{index}", "title": f"Item {index}", "summary": ""}
            for index in range(10)
        ]
    }
    first = chunk_graph_entities(semantic)
    second = chunk_graph_entities(semantic)
    assert [chunk["chunk_id"] for chunk in first] == [chunk["chunk_id"] for chunk in second]
    assert [len(chunk["entities"]) for chunk in first] == [8, 2]


def test_merge_chunking_groups_by_page_type() -> None:
    graph = {
        "node_proposals": [
            {"proposal_id": "a", "page_type": "project"},
            {"proposal_id": "b", "page_type": "project"},
            {"proposal_id": "c", "page_type": "person"},
        ]
    }
    candidate_context = {"candidates": [{"proposal_id": "a"}, {"proposal_id": "b"}, {"proposal_id": "c"}]}
    chunks = chunk_merge_nodes(graph, candidate_context)
    assert [chunk["page_type"] for chunk in chunks] == ["person", "project"]
    assert chunks[1]["proposal_ids"] == ["a", "b"]


def test_graph_assembly_dedupes_edges() -> None:
    payload = assemble_graph_chunks(
        "bundle-1",
        [
            {
                "node_proposals": [{"proposal_id": "a"}],
                "edge_proposals": [{"source_ref": "a", "target_ref": "b", "relation_type": "rel", "rationale": "", "evidence_refs": []}],
                "notes": ["one"],
            },
            {
                "node_proposals": [{"proposal_id": "b"}],
                "edge_proposals": [{"source_ref": "a", "target_ref": "b", "relation_type": "rel", "rationale": "", "evidence_refs": []}],
                "notes": ["two"],
            },
        ],
    )
    assert len(payload["edge_proposals"]) == 1
    assert payload["notes"] == ["one", "two"]


def test_graph_assembly_dedupes_duplicate_proposal_ids_and_keeps_richer_node() -> None:
    payload = assemble_graph_chunks(
        "bundle-1",
        [
            {
                "node_proposals": [
                    {
                        "proposal_id": "a",
                        "title": "Alpha",
                        "slug": "alpha",
                        "page_type": "concept",
                        "summary": "Short summary.",
                        "domains": ["work"],
                        "aliases": [],
                        "evidence_refs": ["input:concept:0"],
                        "attributes": {},
                        "relates_to_refs": [],
                    }
                ],
                "edge_proposals": [],
                "notes": ["one"],
            },
            {
                "node_proposals": [
                    {
                        "proposal_id": "a",
                        "title": "Alpha",
                        "slug": "alpha",
                        "page_type": "concept",
                        "summary": "Richer summary with more context.",
                        "domains": ["work", "craft"],
                        "aliases": ["A"],
                        "evidence_refs": ["input:concept:0", "upload:bio"],
                        "attributes": {"detail": "present"},
                        "relates_to_refs": ["owner"],
                    }
                ],
                "edge_proposals": [],
                "notes": ["two"],
            },
        ],
    )
    assert len(payload["node_proposals"]) == 1
    assert payload["node_proposals"][0]["summary"] == "Richer summary with more context."
    assert any("duplicate graph proposal for a" in note for note in payload["notes"])


def test_merge_assembly_dedupes_duplicate_proposal_ids() -> None:
    payload = assemble_merge_chunks(
        "bundle-1",
        [
            {"decisions": [{"proposal_id": "a", "action": "create", "title": "A", "slug": "a", "page_type": "project"}], "notes": ["one"]},
            {"decisions": [{"proposal_id": "a", "action": "merge", "title": "A", "slug": "a", "page_type": "project"}], "notes": ["two"]},
        ],
        [],
        graph_artifact={
            "node_proposals": [
                {"proposal_id": "a", "title": "A", "slug": "a", "page_type": "project"},
            ]
        },
    )
    assert payload["decisions"] == [
        {"proposal_id": "a", "action": "merge", "title": "A", "slug": "a", "page_type": "project"}
    ]
    assert any("duplicate merge decision" in note for note in payload["notes"])


def test_chunk_state_reclaims_stale_inflight(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundle"
    ensure_chunk_state(bundle_dir, bundle_id="bundle-1", phase="graph_nodes", chunk_id="chunk-1")
    state = ChunkState(
        bundle_id="bundle-1",
        phase="graph_nodes",
        chunk_id="chunk-1",
        status="in_flight",
        attempts=1,
        last_attempt_started_at="2000-01-01T00:00:00Z",
    )
    write_chunk_state(bundle_dir, state=state)
    acquired, updated = acquire_chunk_lease(bundle_dir, bundle_id="bundle-1", phase="graph_nodes", chunk_id="chunk-1")
    assert acquired is True
    assert updated.status == "in_flight"
    assert updated.attempts == 2


def test_chunk_state_reclaims_dead_owner_inflight(tmp_path: Path, monkeypatch) -> None:
    bundle_dir = tmp_path / "bundle"
    ensure_chunk_state(bundle_dir, bundle_id="bundle-1", phase="graph_nodes", chunk_id="chunk-1")
    state = ChunkState(
        bundle_id="bundle-1",
        phase="graph_nodes",
        chunk_id="chunk-1",
        status="in_flight",
        owner_pid=424242,
        attempts=1,
        last_attempt_started_at="2999-01-01T00:00:00Z",
    )
    write_chunk_state(bundle_dir, state=state)
    monkeypatch.setattr(onboarding_state, "_process_exists", lambda pid: False)
    acquired, updated = acquire_chunk_lease(bundle_dir, bundle_id="bundle-1", phase="graph_nodes", chunk_id="chunk-1")
    assert acquired is True
    assert updated.status == "in_flight"
    assert updated.owner_pid is not None
    assert updated.attempts == 2


def test_chunk_summary_counts_done_chunks(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundle"
    state = ensure_chunk_state(bundle_dir, bundle_id="bundle-1", phase="merge_nodes", chunk_id="chunk-1")
    mark_chunk_done(bundle_dir, state=state, payload={"data": {"ok": True}}, generation_id="gen_1")
    ensure_chunk_state(bundle_dir, bundle_id="bundle-1", phase="merge_nodes", chunk_id="chunk-2")
    summary = summarize_chunk_phase(bundle_dir, phase="merge_nodes")
    assert summary is not None
    assert summary.done == 1
    assert summary.pending == 1


def test_chunk_summary_treats_dead_owner_inflight_as_pending(tmp_path: Path, monkeypatch) -> None:
    bundle_dir = tmp_path / "bundle"
    state = ChunkState(
        bundle_id="bundle-1",
        phase="merge_nodes",
        chunk_id="chunk-1",
        status="in_flight",
        owner_pid=424242,
        attempts=1,
        last_attempt_started_at="2999-01-01T00:00:00Z",
    )
    write_chunk_state(bundle_dir, state=state)
    monkeypatch.setattr(onboarding_state, "_process_exists", lambda pid: False)
    summary = summarize_chunk_phase(bundle_dir, phase="merge_nodes")
    assert summary is not None
    assert summary.in_flight == 0
    assert summary.pending == 1


def test_prune_chunk_phase_removes_obsolete_state_and_result_files(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundle"
    ensure_chunk_state(bundle_dir, bundle_id="bundle-1", phase="graph_nodes", chunk_id="keep")
    stale = ensure_chunk_state(bundle_dir, bundle_id="bundle-1", phase="graph_nodes", chunk_id="stale")
    mark_chunk_done(bundle_dir, state=stale, payload={"data": {"ok": True}}, generation_id="gen_1")

    prune_chunk_phase(bundle_dir, phase="graph_nodes", keep_chunk_ids={"keep"})

    assert summarize_chunk_phase(bundle_dir, phase="graph_nodes") is not None
    assert (bundle_dir / "chunks" / "graph_nodes" / "keep.state.json").exists()
    assert not (bundle_dir / "chunks" / "graph_nodes" / "stale.state.json").exists()
    assert not (bundle_dir / "chunks" / "graph_nodes" / "stale.result.json").exists()


def test_failed_chunk_sets_persisted_retry_window(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundle"
    state = ensure_chunk_state(bundle_dir, bundle_id="bundle-1", phase="merge_nodes", chunk_id="chunk-1")
    acquired, leased = acquire_chunk_lease(bundle_dir, bundle_id="bundle-1", phase="merge_nodes", chunk_id="chunk-1")
    assert acquired is True
    failed = mark_chunk_failed(bundle_dir, state=leased, error_message="boom", retry_after_seconds=None)
    assert failed.retry_after_seconds == 30
    assert failed.next_retry_not_before_at is not None
    assert next_retry_not_before(bundle_dir, phase="merge_nodes") == failed.next_retry_not_before_at


def test_chunk_summary_reports_cooling_down_state(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundle"
    state = ChunkState(
        bundle_id="bundle-1",
        phase="merge_nodes",
        chunk_id="chunk-1",
        status="failed",
        attempts=1,
        retry_after_seconds=30,
        next_retry_not_before_at="2999-01-01T00:00:00Z",
    )
    write_chunk_state(bundle_dir, state=state)
    summary = summarize_chunk_phase(bundle_dir, phase="merge_nodes")
    assert summary is not None
    assert summary.cooling_down == 1
    assert summary.next_retry_not_before_at == "2999-01-01T00:00:00Z"
    assert "cooling down until 2999-01-01T00:00:00Z" in summary.render()
