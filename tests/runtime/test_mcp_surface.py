from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from mind.mcp.models import EnqueueLinksRequest, GenerateSkillRequest, ReadSkillRequest, SearchMemoryRequest
from mind.mcp.models import ClearStaleLockRequest
from mind.mcp.models import RetryQueueItemRequest, SetSkillStatusRequest
from mind.mcp.models import (
    GraphHealthRequest,
    IngestReadinessRequest,
    StartArticleRepairRequest,
    StartDreamBootstrapRequest,
    StartDreamRequest,
    StartIngestRequest,
    StartReingestRequest,
)
from mind.mcp.server import BrainMCPServer, MCPAuthError, MCPUnsupportedOperationError
from mind.mcp.stdio import TOOL_SPECS
from mind.cli import main
from tests.support import write_repo_config


def _write_config(root: Path) -> None:
    write_repo_config(root, create_me=True, create_indexes=True)
    (root / "memory" / "me" / "profile.md").write_text(
        "---\n"
        "id: profile\n"
        "type: profile\n"
        "title: Example Owner\n"
        "status: active\n"
        "created: 2026-04-08\n"
        "last_updated: 2026-04-08\n"
        "aliases: []\n"
        "tags:\n  - domain/identity\n  - function/identity\n  - signal/canon\n"
        "domains:\n  - identity\n"
        "relates_to: []\n"
        "sources: []\n"
        "role: Founder\n"
        "location: Remote\n"
        "---\n\n"
        "# Example Owner\n\nBuilds tools for thought.\n",
        encoding="utf-8",
    )
    skill_dir = root / "skills" / "skill-creator"
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text("---\nname: skill-creator\ndescription: test\nid: skill-creator\n---\n", encoding="utf-8")


def test_mcp_read_surface_uses_runtime_and_memory_search(tmp_path: Path):
    _write_config(tmp_path)
    server = BrainMCPServer(root=tmp_path)
    status = server.get_runtime_status()
    assert status.schema_version == "1"
    matches = server.search_memory(SearchMemoryRequest(query="tools for thought"))
    assert matches
    assert matches[0].page_id == "profile"
    skills = server.list_skills()
    assert skills
    read_skill = server.read_skill(ReadSkillRequest(skill_id="skill-creator"))
    assert "skill-creator" in read_skill.content


def test_mcp_write_surface_is_enqueue_only_and_returns_durable_ids(tmp_path: Path):
    _write_config(tmp_path)
    server = BrainMCPServer(root=tmp_path)
    resp = server.enqueue_links(
        EnqueueLinksRequest(
            links=[{"url": "https://example.com", "title": "Example"}],
            today="2026-04-08",
        )
    )
    assert resp.status == "queued"
    assert resp.run_id > 0
    queue = server.list_queue()
    assert any(item.name == "links" and item.pending_count >= 1 for item in queue)
    run = server.get_run(resp.run_id)
    assert run is not None
    assert run.run.status == "queued"
    assert run.run.queue_name == "links"
    assert run.run.item_ref == str(tmp_path / "raw" / "drops" / "articles-from-mcp-2026-04-08.jsonl")
    drop = tmp_path / "raw" / "drops" / "articles-from-mcp-2026-04-08.jsonl"
    assert not drop.exists()


def test_worker_run_once_consumes_enqueued_mcp_links(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    server = BrainMCPServer(root=tmp_path)
    _ = server.enqueue_links(
        EnqueueLinksRequest(
            links=[{"url": "https://example.com", "title": "Example"}],
            today="2026-04-08",
        )
    )
    monkeypatch.setattr("mind.commands.common.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.worker.project_root", lambda: tmp_path)
    assert main(["worker", "run-once"]) == 0
    out = capsys.readouterr().out
    assert "worker: processed run" in out
    drop = tmp_path / "raw" / "drops" / "articles-from-mcp-2026-04-08.jsonl"
    payload = [json.loads(line) for line in drop.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert payload[0]["url"] == "https://example.com"
    runs = server.state.list_runs(limit=5)
    assert any(run.status == "completed" for run in runs)


def test_worker_run_once_applies_skill_status_change(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    server = BrainMCPServer(root=tmp_path)
    queued = server.set_skill_status(SetSkillStatusRequest(skill_id="skill-creator", status="archived"))
    monkeypatch.setattr("mind.commands.common.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.worker.project_root", lambda: tmp_path)
    assert main(["worker", "run-once"]) == 0
    content = (tmp_path / "skills" / "skill-creator" / "SKILL.md").read_text(encoding="utf-8")
    assert "status: archived" in content
    run = server.get_run(queued.run_id)
    assert run is not None
    assert run.run.status == "completed"


def test_worker_run_once_retry_replays_failed_link_append(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    server = BrainMCPServer(root=tmp_path)
    queued = server.enqueue_links(
        EnqueueLinksRequest(
            links=[{"url": "https://example.com/a", "title": "A"}],
            today="2026-04-08",
        )
    )
    monkeypatch.setattr("mind.commands.common.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.worker.project_root", lambda: tmp_path)
    calls = {"count": 0}

    def flaky_append(*, path, links):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("disk-full")
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            for link in links:
                handle.write(json.dumps(link) + "\n")
        return len(links)

    monkeypatch.setattr("mind.services.queue_worker._append_enqueued_links", flaky_append)
    assert main(["worker", "run-once"]) == 1
    retry = server.retry_queue_item(RetryQueueItemRequest(run_id=queued.run_id))
    assert main(["worker", "run-once"]) == 0
    assert main(["worker", "run-once"]) == 0
    runs = server.state.list_runs(limit=10)
    assert len([run for run in runs if run.kind == "mcp.enqueue_links"]) >= 2
    drop = tmp_path / "raw" / "drops" / "articles-from-mcp-2026-04-08.jsonl"
    payload = [json.loads(line) for line in drop.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert payload == [{"url": "https://example.com/a", "title": "A"}]
    retry_details = server.get_run(retry.run_id)
    assert retry_details is not None
    assert retry_details.run.status == "completed"


def test_mcp_auth_token_is_enforced(tmp_path: Path):
    _write_config(tmp_path)
    server = BrainMCPServer(root=tmp_path, auth_token="secret")
    with pytest.raises(MCPAuthError):
        server.generate_skill(GenerateSkillRequest(prompt="x"))
    ok = server.generate_skill(GenerateSkillRequest(prompt="x", auth_token="secret"))
    assert ok.status == "queued"


def test_start_ingest_rejects_unknown_kind(tmp_path: Path):
    _write_config(tmp_path)
    with pytest.raises(ValidationError):
        StartIngestRequest(kind="bogus")


def test_start_ingest_requires_path_for_path_backed_kinds(tmp_path: Path):
    _write_config(tmp_path)
    with pytest.raises(ValidationError):
        StartIngestRequest(kind="file")


def test_mcp_start_ingest_normalizes_repo_relative_paths_and_rejects_external_paths(tmp_path: Path):
    _write_config(tmp_path)
    server = BrainMCPServer(root=tmp_path)
    source = tmp_path / "raw" / "web" / "note.md"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("# Note\n", encoding="utf-8")

    queued = server.start_ingest(StartIngestRequest(kind="file", path="raw/web/note.md"))
    run = server.get_run(queued.run_id)
    assert run is not None
    assert run.run.item_ref == source.as_posix()

    with pytest.raises(ValueError):
        server.start_ingest(StartIngestRequest(kind="file", path="../outside.md"))


def test_start_reingest_rejects_invalid_stage_window(tmp_path: Path):
    _write_config(tmp_path)
    with pytest.raises(ValidationError):
        StartReingestRequest(lane="articles", stage="bogus")
    with pytest.raises(ValidationError):
        StartReingestRequest(lane="articles", stage="fanout", through="pass_a")


def test_mcp_start_reingest_enqueues_dedicated_queue(tmp_path: Path):
    _write_config(tmp_path)
    server = BrainMCPServer(root=tmp_path)

    queued = server.start_reingest(
        StartReingestRequest(
            lane="articles",
            today="2026-04-09",
            stage="pass_d",
            through="materialize",
            dry_run=True,
        )
    )

    assert queued.queue_name == "ingest:reingest"
    run = server.get_run(queued.run_id)
    assert run is not None
    assert run.run.kind == "mcp.start_reingest"


def test_mcp_graph_health_and_readiness_surfaces_return_structured_results(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    server = BrainMCPServer(root=tmp_path)

    monkeypatch.setattr(
        "mind.mcp.server.build_graph_health",
        lambda repo_root, include_promotion_gate=True: __import__("types").SimpleNamespace(
            graph_built=True,
            node_count=5,
            edge_count=2,
            document_count=8,
            embedding_model="openai/text-embedding-3-small",
            embedding_count=8,
            embedding_backend="sqlite",
            embedding_backend_count=8,
            shadow_mode="advisory-only",
            promotion_gate_passed=True,
            promotion_gate_artifact_json=None,
            promotion_gate_artifact_markdown=None,
            issues=(),
        ),
    )
    monkeypatch.setattr(
        "mind.mcp.server.run_ingest_readiness",
        lambda repo_root, dropbox_limit=None, lane_limit=None, include_promotion_gate=False: __import__("types").SimpleNamespace(
            passed=False,
            issues=("dropbox dry-run predicts 1 review-required files",),
            report_json_path=tmp_path / "raw" / "reports" / "ingest-review" / "readiness.json",
            report_markdown_path=tmp_path / "raw" / "reports" / "ingest-review" / "readiness.md",
            dropbox=__import__("types").SimpleNamespace(metadata={"would_review_count": 1}),
            graph=__import__("types").SimpleNamespace(
                graph_built=True,
                node_count=5,
                edge_count=2,
                document_count=8,
                embedding_model="openai/text-embedding-3-small",
                embedding_count=8,
                embedding_backend="sqlite",
                embedding_backend_count=8,
                shadow_mode="advisory-only",
                promotion_gate_passed=True,
                issues=(),
            ),
            lanes=(),
            article_repair=__import__("types").SimpleNamespace(
                plan=__import__("types").SimpleNamespace(ready_count=0, reacquire_count=1, recompute_count=0, blocked_count=0)
            ),
        ),
    )

    graph = server.get_graph_health(GraphHealthRequest(skip_promotion_gate=True))
    readiness = server.run_ingest_readiness(IngestReadinessRequest(dropbox_limit=3))

    assert graph["shadow_mode"] == "advisory-only"
    assert readiness["passed"] is False
    assert readiness["dropbox"]["would_review_count"] == 1


def test_mcp_start_article_repair_enqueues_dedicated_queue(tmp_path: Path):
    _write_config(tmp_path)
    server = BrainMCPServer(root=tmp_path)

    queued = server.start_article_repair(
        StartArticleRepairRequest(
            today="2026-04-09",
            apply=False,
        )
    )

    assert queued.queue_name == "ingest:repair-articles"
    run = server.get_run(queued.run_id)
    assert run is not None
    assert run.run.kind == "mcp.start_article_repair"


def test_mcp_stdio_exposes_reingest_readiness_and_article_repair_tools():
    for name in ("start_reingest", "get_graph_health", "run_ingest_readiness", "start_article_repair"):
        assert name in TOOL_SPECS


def test_mcp_write_does_not_clear_active_lock(tmp_path: Path):
    _write_config(tmp_path)
    server = BrainMCPServer(root=tmp_path)
    server.state.acquire_lock(holder="dream-light")
    queued = server.clear_stale_lock(ClearStaleLockRequest())
    assert queued.status == "queued"
    monkeypatch = __import__("pytest").MonkeyPatch()
    monkeypatch.setattr("mind.commands.common.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.worker.project_root", lambda: tmp_path)
    assert main(["worker", "run-once"]) == 1
    monkeypatch.undo()
    assert server.state.read_lock() is not None
    details = server.get_run(queued.run_id)
    assert details is not None
    assert details.run.status == "blocked"


def test_mcp_start_dream_entrypoints_are_rejected(tmp_path: Path):
    _write_config(tmp_path)
    server = BrainMCPServer(root=tmp_path)

    with pytest.raises(MCPUnsupportedOperationError):
        server.start_dream_light(StartDreamRequest(dry_run=True))
    with pytest.raises(MCPUnsupportedOperationError):
        server.start_dream_deep(StartDreamRequest(dry_run=False))
    with pytest.raises(MCPUnsupportedOperationError):
        server.start_dream_rem(StartDreamRequest(dry_run=True))
    with pytest.raises(MCPUnsupportedOperationError):
        server.start_dream_bootstrap(
            StartDreamBootstrapRequest(dry_run=True, checkpoint_every=5, resume=True, limit=10)
        )
