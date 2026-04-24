from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import sys
from types import SimpleNamespace

from mind.cli import build_parser, main
from tests.support import write_repo_config


def _write_config(root: Path) -> None:
    write_repo_config(root, create_indexes=True, create_exports=True, create_me=True)


def _patch_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.digest.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.worker.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.orchestrate.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.dropbox.command_common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.graph.command_common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.onboard.command_common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.llm.project_root", lambda: root)


@contextmanager
def _fake_progress(*_args, **_kwargs):
    class _Progress:
        def phase(self, message: str) -> None:
            print(f"[progress] {message}", file=sys.stderr)

        def update(self, message: str) -> None:
            print(f"[progress] {message}", file=sys.stderr)

        def clear(self, *, newline: bool = False) -> None:
            if newline:
                print("", file=sys.stderr)

    yield _Progress()


def test_progress_enabled_is_set_for_covered_long_running_commands():
    parser = build_parser()
    samples = [
        ["dropbox", "sweep", "--dry-run"],
        ["links", "ingest", "links.json"],
        ["ingest", "youtube", "export.json"],
        ["ingest", "articles"],
        ["ingest", "books", "export.json"],
        ["ingest", "substack", "export.json"],
        ["ingest", "audible"],
        ["ingest", "reingest", "--lane", "books", "--dry-run"],
        ["ingest", "registry", "rebuild"],
        ["ingest", "inventory", "--lane", "books", "--json"],
        ["ingest", "plan", "--lane", "books", "--json"],
        ["ingest", "reconcile", "--lane", "books", "--json"],
        ["ingest", "repair-articles", "--dry-run"],
        ["expand", "question"],
        ["llm", "audit", "--refresh-gateway"],
        ["graph", "rebuild"],
        ["graph", "embed", "rebuild"],
        ["graph", "embed", "evaluate"],
        ["dream", "bootstrap", "--dry-run"],
        ["dream", "campaign", "--days", "7", "--dry-run"],
        ["dream", "simulate-year", "--days", "7", "--dry-run"],
        ["orchestrate", "daily"],
        ["worker", "run-once"],
        ["worker", "drain-until-empty"],
        ["onboard", "--from-json", "payload.json"],
        ["onboard", "import", "--from-json", "payload.json"],
        ["onboard", "normalize", "--bundle", "bundle-a"],
        ["onboard", "synthesize", "--bundle", "bundle-a"],
        ["onboard", "verify", "--bundle", "bundle-a"],
        ["onboard", "materialize", "--bundle", "bundle-a"],
        ["onboard", "replay", "--bundle", "bundle-a"],
        ["onboard", "status", "--bundle", "bundle-a"],
        ["onboard", "validate", "--bundle", "bundle-a"],
    ]
    for argv in samples:
        args = parser.parse_args(argv)
        assert bool(getattr(args, "progress_enabled", False)), f"expected progress_enabled for {argv}"


def test_expand_progress_goes_to_stderr(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    monkeypatch.setattr("mind.commands.expand.progress_for_args", _fake_progress)
    monkeypatch.setattr("mind.commands.expand._search_web", lambda question, limit=3: [("Example result", "https://example.com/result")])
    monkeypatch.setattr(
        "mind.commands.expand.ingest_web_articles",
        lambda **kwargs: [
            __import__("mind.services.web_research", fromlist=["GroundedArticleResult"]).GroundedArticleResult(
                query=kwargs["queries"][0],
                url="https://example.com/result",
                article_page_id="example-article",
            )
        ],
    )
    monkeypatch.setattr("mind.commands.expand.cmd_query", lambda args: print("query answer") or 0)

    assert main(["expand", "what is new here"]) == 0
    captured = capsys.readouterr()
    assert "Saved web sources:" in captured.out
    assert "[[example-article]]" in captured.out
    assert "query answer" in captured.out
    assert "[progress] searching the web" in captured.err
    assert "[progress] fetching web results" in captured.err
    assert "[progress] saving raw sources" in captured.err
    assert "[progress] querying local graph" in captured.err


def test_ingest_youtube_progress_goes_to_stderr(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    export = tmp_path / "raw" / "exports" / "youtube-recent-2026-04-09.json"
    export.parent.mkdir(parents=True, exist_ok=True)
    export.write_text("[]", encoding="utf-8")
    monkeypatch.setattr("mind.commands.ingest.progress_for_args", _fake_progress)

    def _fake_ingest(path, *, default_duration_minutes=30.0, phase_callback=None, **_kwargs):
        if phase_callback is not None:
            phase_callback("processing selected videos")
        return SimpleNamespace(
            pages_written=2,
            selected_count=2,
            skipped_materialized=0,
            resumable=2,
            blocked=0,
            stale=0,
            executed=2,
            failed=0,
            blocked_samples=[],
            failed_items=[],
        )

    monkeypatch.setattr("mind.commands.ingest.ingest_youtube_export", _fake_ingest)

    assert main(["ingest", "youtube", str(export)]) == 0
    captured = capsys.readouterr()
    assert "ingest-youtube:" in captured.out
    assert "pages_written=2" in captured.out
    assert "[progress] loading export" in captured.err
    assert "[progress] processing selected videos" in captured.err


def test_ingest_youtube_cli_returns_nonzero_when_failures_remain(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    export = tmp_path / "raw" / "exports" / "youtube-recent-2026-04-09.json"
    export.parent.mkdir(parents=True, exist_ok=True)
    export.write_text("[]", encoding="utf-8")
    monkeypatch.setattr("mind.commands.ingest.progress_for_args", _fake_progress)
    monkeypatch.setattr(
        "mind.commands.ingest.ingest_youtube_export",
        lambda path, **kwargs: SimpleNamespace(
            pages_written=1,
            selected_count=1,
            skipped_materialized=0,
            resumable=1,
            blocked=0,
            stale=0,
            executed=1,
            failed=1,
            blocked_samples=[],
            failed_items=["youtube-abc123xyz00"],
        ),
    )

    assert main(["ingest", "youtube", str(export)]) == 1


def test_ingest_reingest_apply_emits_item_progress_and_writes_report(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    monkeypatch.setattr("mind.commands.ingest.progress_for_args", _fake_progress)

    def _fake_run_reingest(request, *, repo_root=None, item_callback=None):
        plan_item = SimpleNamespace(
            source_id="youtube-abc123xyz00",
            label="Test Video",
            blocked_reasons=(),
            excluded_reason="",
        )
        result_item = SimpleNamespace(
            source_id="youtube-abc123xyz00",
            status="completed",
            detail="ok",
            materialized_paths={"video": str(tmp_path / "memory" / "sources" / "youtube" / "business" / "test-video.md")},
        )
        if item_callback is not None:
            item_callback(plan_item, result_item, 1, 1)
        return SimpleNamespace(
            plan=SimpleNamespace(lane="youtube", stage="acquire", through="propagate", source_label="youtube-recent.json"),
            applied=True,
            results=(result_item,),
            exit_code=0,
        )

    monkeypatch.setattr("mind.commands.ingest.run_reingest", _fake_run_reingest)
    monkeypatch.setattr("mind.commands.ingest.render_reingest_report", lambda result: "reingest[youtube]: ok")

    export = tmp_path / "raw" / "exports" / "youtube-recent-2026-04-17.json"
    export.parent.mkdir(parents=True, exist_ok=True)
    export.write_text("[]", encoding="utf-8")

    assert main(["ingest", "reingest", "--lane", "youtube", "--path", str(export), "--source-id", "youtube-abc123xyz00", "--apply"]) == 0
    captured = capsys.readouterr()
    assert "reingest-start: lane=youtube report=" in captured.out
    assert "reingest[youtube]: ok" in captured.out
    assert "reingest-report:" in captured.out
    assert "[progress] replaying youtube" in captured.err
    assert "[progress] youtube report: reingest-youtube-" in captured.err
    assert "[progress] youtube 1/1: youtube-abc123xyz00 completed" in captured.err

    reports = sorted((tmp_path / "raw" / "reports" / "ingest-review").glob("reingest-youtube-*.jsonl"))
    assert len(reports) == 1
    lines = [json.loads(line) for line in reports[0].read_text(encoding="utf-8").splitlines()]
    assert [line["event"] for line in lines] == ["start", "item", "complete"]
    assert lines[1]["source_id"] == "youtube-abc123xyz00"
    assert lines[1]["status"] == "completed"


def test_ingest_articles_progress_goes_to_stderr(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    monkeypatch.setattr("mind.commands.ingest.progress_for_args", _fake_progress)
    monkeypatch.setattr(
        "mind.commands.ingest.ingest_articles_queue",
        lambda **kwargs: SimpleNamespace(drop_files_processed=1, fetched_summarized=2, failed=0),
    )

    assert main(["ingest", "articles"]) == 0
    captured = capsys.readouterr()
    assert "ingest-articles: 1 drop files -> 2 fetched, 0 failed" in captured.out
    assert "[progress] draining article queue" in captured.err
    assert "[progress] summarizing fetched articles" in captured.err


def test_dropbox_sweep_progress_goes_to_stderr(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    note = tmp_path / "dropbox" / "note.md"
    note.parent.mkdir(parents=True, exist_ok=True)
    note.write_text("# Note\n\nhello\n", encoding="utf-8")
    monkeypatch.setattr("mind.commands.dropbox.progress_for_args", _fake_progress)
    monkeypatch.setattr("mind.services.dropbox.GraphRegistry.rebuild", lambda self: None)
    monkeypatch.setattr(
        "mind.services.dropbox.ingest_file_with_details",
        lambda path, **kwargs: (tmp_path / "memory" / "summaries" / f"{path.stem}.md", {}),
    )

    assert main(["dropbox", "sweep"]) == 0
    captured = capsys.readouterr()
    assert "dropbox-sweep:" in captured.out
    assert "[progress] scanning dropbox" in captured.err
    assert "[progress] routing files to ingest lanes" in captured.err


def test_graph_rebuild_progress_goes_to_stderr(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    monkeypatch.setattr("mind.commands.graph.progress_for_args", _fake_progress)

    class _Registry:
        def rebuild(self, *, phase_callback=None):
            if phase_callback is not None:
                phase_callback("scanning canonical pages")
                phase_callback("writing graph registry")
            return SimpleNamespace(render=lambda: "graph-rebuild:\n- nodes=1")

    monkeypatch.setattr("mind.commands.graph.GraphRegistry.for_repo_root", lambda root: _Registry())

    assert main(["graph", "rebuild"]) == 0
    captured = capsys.readouterr()
    assert "graph-rebuild:" in captured.out
    assert "[progress] scanning canonical pages" in captured.err
    assert "[progress] writing graph registry" in captured.err


def test_graph_embed_rebuild_progress_goes_to_stderr(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    monkeypatch.setattr("mind.commands.graph.progress_for_args", _fake_progress)

    class _Registry:
        def ensure_built(self):
            return None

        def list_embedding_targets(self):
            return []

        def list_embedding_metadata(self, model):
            return {}

        def upsert_embeddings(self, model, records):
            return None

        def prune_embeddings(self, model, valid_target_ids):
            return None

        def embedding_status(self, model):
            return {"count": 0}

    class _Backend:
        def upsert(self, model, vectors):
            return None

        def prune(self, model, valid_ids):
            return None

        def status(self, model):
            return {"backend": "sqlite"}

    monkeypatch.setattr("mind.commands.graph.GraphRegistry.for_repo_root", lambda root: _Registry())
    monkeypatch.setattr("mind.commands.graph.resolve_route", lambda key: SimpleNamespace(model="test-model"))
    monkeypatch.setattr("mind.commands.graph.select_vector_backend", lambda path: _Backend())
    monkeypatch.setattr("mind.commands.graph.get_embedding_service", lambda: SimpleNamespace(embed_requests=lambda requests: SimpleNamespace(records=[])))

    assert main(["graph", "embed", "rebuild"]) == 0
    captured = capsys.readouterr()
    assert "graph-embed-rebuild:" in captured.out
    assert "[progress] loading embedding targets" in captured.err
    assert "[progress] embedding changed targets" in captured.err
    assert "[progress] writing vector index" in captured.err


def test_graph_embed_evaluate_progress_goes_to_stderr(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    monkeypatch.setattr("mind.commands.graph.progress_for_args", _fake_progress)
    monkeypatch.setattr("mind.commands.graph.GraphRegistry.for_repo_root", lambda root: object())
    monkeypatch.setattr("mind.commands.graph.resolve_route", lambda key: SimpleNamespace(model="test-model"))
    monkeypatch.setattr("mind.commands.graph.select_vector_backend", lambda path: object())
    monkeypatch.setattr(
        "mind.commands.graph.evaluate_promotion_gate",
        lambda **kwargs: SimpleNamespace(
            passed=True,
            phase1_regressions=0,
            vector_false_negatives=0,
            rows=[],
            artifact_json_path=tmp_path / "gate.json",
            artifact_markdown_path=tmp_path / "gate.md",
        ),
    )

    assert main(["graph", "embed", "evaluate"]) == 0
    captured = capsys.readouterr()
    assert "graph-embed-evaluate:" in captured.out
    assert "[progress] loading evaluation set" in captured.err
    assert "[progress] querying shadow vectors" in captured.err
    assert "[progress] writing evaluation artifacts" in captured.err


def test_llm_audit_refresh_progress_goes_to_stderr(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    monkeypatch.setattr("mind.commands.llm.progress_for_args", _fake_progress)
    monkeypatch.setattr("mind.commands.llm.read_events", lambda *args, **kwargs: [{"generation_id": "g"}])
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: SimpleNamespace(
            ai_gateway_api_key="gateway-key",
            repo_root=tmp_path,
            llm_model="google/gemini-2.5-pro",
            llm_transport_mode="ai_gateway",
            llm_routes={},
            llm_backup=None,
            browser_for_cookies="chrome",
            app_root=tmp_path,
            wiki_root=tmp_path / "memory",
            raw_root=tmp_path / "raw",
            substack_session_cookie="",
        ),
    )
    monkeypatch.setattr("mind.commands.llm.enrich_events_with_gateway", lambda events, api_key: (events, []))
    monkeypatch.setattr(
        "mind.commands.llm.summarize_events",
        lambda events: {
            "total_attempts": 1,
            "success_count": 1,
            "failure_count": 0,
            "missing_generation_ids": 0,
            "per_task": {"summary": 1},
            "per_model": {"test": 1},
            "slowest": [],
        },
    )
    monkeypatch.setattr(
        "mind.commands.llm.summarize_gateway_costs",
        lambda events: {"total_cost": 0.0, "per_task_cost": {}, "per_model_cost": {}},
    )

    assert main(["llm", "audit", "--refresh-gateway"]) == 0
    captured = capsys.readouterr()
    assert "llm-audit: attempts=1 success=1 failed=0" in captured.out
    assert "[progress] reading local telemetry" in captured.err
    assert "[progress] refreshing gateway metadata" in captured.err
    assert "[progress] summarizing costs" in captured.err


def test_dream_bootstrap_progress_goes_to_stderr(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    monkeypatch.setattr("mind.commands.dream.progress_for_args", _fake_progress)
    monkeypatch.setattr("mind.commands.dream.run_bootstrap", lambda **kwargs: SimpleNamespace(render=lambda: "Dream stage: bootstrap"))

    assert main(["dream", "bootstrap", "--dry-run"]) == 0
    captured = capsys.readouterr()
    assert "Dream stage: bootstrap" in captured.out
    assert "[progress] loading bootstrap sources" in captured.err
    assert "[progress] replaying historical sources" in captured.err


def test_onboard_import_materialize_status_progress_and_print_json_suppression(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    monkeypatch.setattr("mind.commands.onboard.progress_for_args", _fake_progress)
    status = SimpleNamespace(
        bundle_id="bundle-a",
        status="imported",
        raw_input_path="raw/onboarding/bundles/bundle-a/raw-input.json",
        ready_for_materialization=True,
        synthesis_status="not-synthesized",
        verifier_verdict="not-run",
        graph_chunks_summary=None,
        merge_chunks_summary=None,
        merge_relationships_summary=None,
        materialization_plan_path=None,
        replay_provenance=None,
        uploads=[],
        next_questions=[],
        errors=[],
        warnings=[],
        blocking_reasons=[],
        materialized_pages=[],
        summary_pages=[],
        decision_page=None,
        readiness={"ready": True},
    )
    monkeypatch.setattr("mind.commands.onboard.import_onboarding_bundle", lambda *args, **kwargs: status)
    monkeypatch.setattr("mind.commands.onboard.materialize_onboarding_bundle", lambda *args, **kwargs: status)
    monkeypatch.setattr("mind.commands.onboard.read_onboarding_status", lambda *args, **kwargs: status)
    monkeypatch.setattr("mind.commands.onboard.validate_onboarding_bundle_state", lambda *args, **kwargs: status)
    monkeypatch.setattr("mind.commands.onboard.render_onboarding_materialization_plan", lambda *args, **kwargs: {"bundle_id": "bundle-a", "pages": []})

    payload = tmp_path / "payload.json"
    payload.write_text("{}", encoding="utf-8")

    assert main(["onboard", "import", "--from-json", str(payload)]) == 0
    captured = capsys.readouterr()
    assert "onboard-import:" in captured.out
    assert "[progress] importing onboarding input" in captured.err

    assert main(["onboard", "materialize", "--bundle", "bundle-a"]) == 0
    captured = capsys.readouterr()
    assert "onboard-materialize:" in captured.out
    assert "[progress] materializing pages" in captured.err

    assert main(["onboard", "status", "--bundle", "bundle-a"]) == 0
    captured = capsys.readouterr()
    assert "onboard-status:" in captured.out
    assert "[progress] reading onboarding status" in captured.err

    assert main(["onboard", "plan", "--bundle", "bundle-a", "--print-json"]) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out)["bundle_id"] == "bundle-a"
    assert captured.err == ""
