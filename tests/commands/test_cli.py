from __future__ import annotations

from contextlib import contextmanager
import argparse
from pathlib import Path
import json
import sys
import types
from unittest.mock import Mock, patch

from mind.cli import main
from mind.commands.readiness import cmd_readiness
from mind.services.ingest_readiness import GraphHealthSnapshot
from mind.runtime_state import RuntimeState
from tests.paths import FIXTURES_ROOT
from tests.support import fake_env_config, write_repo_config


def _write_config(root: Path) -> None:
    write_repo_config(root, create_me=True)


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


def test_mind_lint_invokes_scripts_lint_main():
    with patch("scripts.lint.main", return_value=0) as mock_main:
        rc = main(["lint"])
    assert rc == 0
    mock_main.assert_called_once_with([])


def test_mind_check_env_requires_substack_cookie_when_requested(monkeypatch, capsys):
    class _Cfg:
        substack_session_cookie = ""

    monkeypatch.setattr("scripts.common.env.load", lambda: _Cfg())
    rc = main(["check", "env", "--substack-cookie"])
    assert rc == 1
    assert "SUBSTACK_SESSION_COOKIE" in capsys.readouterr().out


def test_mind_repair_content_policy_dispatches_service(tmp_path, capsys):
    _write_config(tmp_path)
    fake_report = types.SimpleNamespace(render=lambda: "content policy report")

    with patch("mind.commands.repair.project_root", return_value=tmp_path), \
         patch("mind.commands.repair.run_content_policy_repair", return_value=fake_report) as mock_run:
        rc = main(["repair", "content-policy", "--apply"])

    assert rc == 0
    assert "content policy report" in capsys.readouterr().out
    mock_run.assert_called_once_with(tmp_path, apply=True)


def test_mind_repair_content_policy_migrate_dispatches_service(tmp_path, capsys):
    _write_config(tmp_path)
    fake_report = types.SimpleNamespace(render=lambda: "content policy migrate report")

    with patch("mind.commands.repair.project_root", return_value=tmp_path), \
         patch("mind.commands.repair.run_content_policy_migration", return_value=fake_report) as mock_run:
        rc = main(["repair", "content-policy-migrate", "--lane", "youtube", "--apply"])

    assert rc == 0
    assert "content policy migrate report" in capsys.readouterr().out
    mock_run.assert_called_once_with(tmp_path, lane="youtube", apply=True)


def test_mind_repair_atom_pages_dispatches_service(tmp_path, capsys):
    _write_config(tmp_path)
    fake_report = types.SimpleNamespace(render=lambda: "atom page repair report")

    with patch("mind.commands.repair.project_root", return_value=tmp_path), \
         patch("mind.commands.repair.run_atom_page_repair", return_value=fake_report) as mock_run:
        rc = main(["repair", "atom-pages", "--apply"])

    assert rc == 0
    assert "atom page repair report" in capsys.readouterr().out
    mock_run.assert_called_once_with(tmp_path, apply=True)


def test_mind_repair_personalization_links_dispatches_service(tmp_path, capsys):
    _write_config(tmp_path)
    fake_report = types.SimpleNamespace(render=lambda: "personalization links repair report")

    with patch("mind.commands.repair.project_root", return_value=tmp_path), \
         patch("mind.commands.repair.run_personalization_link_repair", return_value=fake_report) as mock_run:
        rc = main(["repair", "personalization-links", "--lane", "books", "--limit", "5", "--source-id", "book-a", "--apply"])

    assert rc == 0
    assert "personalization links repair report" in capsys.readouterr().out
    mock_run.assert_called_once_with(
        repo_root=tmp_path,
        lane="books",
        path=None,
        today=None,
        limit=5,
        source_ids=("book-a",),
        apply=True,
    )


def test_mind_repair_vault_housekeeping_dispatches_service(tmp_path, capsys):
    _write_config(tmp_path)
    fake_report = types.SimpleNamespace(render=lambda: "vault housekeeping report")

    with patch("mind.commands.repair.project_root", return_value=tmp_path), \
         patch("mind.commands.repair.run_vault_housekeeping", return_value=fake_report) as mock_run:
        rc = main(["repair", "vault-housekeeping", "--apply"])

    assert rc == 0
    assert "vault housekeeping report" in capsys.readouterr().out
    mock_run.assert_called_once_with(tmp_path, apply=True)


def test_mind_repair_identifiers_dispatches_service(tmp_path, capsys):
    _write_config(tmp_path)
    fake_report = types.SimpleNamespace(render=lambda: "identifier repair report")

    with patch("mind.commands.repair.project_root", return_value=tmp_path), \
         patch("mind.commands.repair.run_identifier_repair", return_value=fake_report) as mock_run:
        rc = main(["repair", "identifiers", "--apply"])

    assert rc == 0
    assert "identifier repair report" in capsys.readouterr().out
    mock_run.assert_called_once_with(tmp_path, apply=True)


def test_mind_llm_audit_summarizes_local_logs(tmp_path, monkeypatch, capsys):
    _write_config(tmp_path)
    (tmp_path / ".logs" / "llm").mkdir(parents=True, exist_ok=True)
    (tmp_path / ".logs" / "llm" / "attempts-2026-04-14.jsonl").write_text(
        json.dumps(
            {
                "timestamp": "2026-04-14T10:00:00.000Z",
                "task_class": "summary",
                "prompt_version": "summary.test.v1",
                "provider": "openai",
                "model": "openai/gpt-test",
                "bundle_id": "bundle-1",
                "attempt_role": "primary",
                "attempt_index": 1,
                "status": "success",
                "latency_ms": 120,
                "response_id": "resp_1",
                "generation_id": "gen_1",
                "tokens_in": 10,
                "tokens_out": 4,
                "tokens_total": 14,
                "error_class": None,
                "request_metadata": {"bundle_id": "bundle-1"},
            }
    )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("mind.commands.common.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.llm.project_root", lambda: tmp_path)

    assert main(["llm", "audit", "--date", "2026-04-14", "--bundle", "bundle-1"]) == 0
    out = capsys.readouterr().out
    assert "llm-audit: attempts=1 success=1 failed=0" in out
    assert "missing generation ids: 0" in out
    assert "openai/gpt-test" in out


def test_mind_llm_audit_refreshes_gateway_when_requested(tmp_path, monkeypatch, capsys):
    _write_config(tmp_path)
    (tmp_path / ".logs" / "llm").mkdir(parents=True, exist_ok=True)
    (tmp_path / ".logs" / "llm" / "attempts-2026-04-14.jsonl").write_text(
        json.dumps(
            {
                "timestamp": "2026-04-14T10:00:00.000Z",
                "task_class": "summary",
                "prompt_version": "summary.test.v1",
                "provider": "openai",
                "model": "openai/gpt-test",
                "bundle_id": "bundle-1",
                "attempt_role": "primary",
                "attempt_index": 1,
                "status": "success",
                "latency_ms": 120,
                "response_id": "resp_1",
                "generation_id": "gen_1",
                "tokens_in": 10,
                "tokens_out": 4,
                "tokens_total": 14,
                "error_class": None,
                "request_metadata": {"bundle_id": "bundle-1"},
            }
    )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("mind.commands.common.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.llm.project_root", lambda: tmp_path)
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: types.SimpleNamespace(
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
    monkeypatch.setattr(
        "mind.services.llm_telemetry.fetch_generation_details",
        lambda generation_id, api_key, timeout_seconds=15: {
            "id": generation_id,
            "total_cost": 0.125,
            "provider_name": "openai",
            "tokens_prompt": 10,
            "tokens_completion": 4,
        },
    )

    assert main(["llm", "audit", "--date", "2026-04-14", "--refresh-gateway"]) == 0
    out = capsys.readouterr().out
    assert "Gateway total cost: $0.125000" in out
    assert "Gateway cost by task:" in out


def test_onboard_status_prints_chunk_summaries(tmp_path, monkeypatch, capsys):
    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.common.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: tmp_path)
    from tests.support import patch_onboarding_llm

    patch_onboarding_llm(monkeypatch)
    payload = tmp_path / "onboarding.json"
    payload.write_text(
        json.dumps(
            {
                "name": "Example Owner",
                "role": "Founder",
                "location": "Remote",
                "summary": "Example Owner builds local-first tools.",
                "values": ["clarity", "taste"],
                "positioning": {"summary": "Design engineer and founder.", "work_priorities": ["craft quality"], "constraints": ["keep it local-first"]},
                "open_threads": ["How should Brain evolve?"],
                "people": [{"name": "Jordan Lee", "summary": "Collaborator"}],
                "projects": [{"title": "Brain", "summary": "Personal wiki"}],
            }
        ),
        encoding="utf-8",
    )

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "status-pass"]) == 0
    assert main(["onboard", "synthesize", "--bundle", "status-pass"]) == 0
    assert main(["onboard", "status", "--bundle", "status-pass"]) == 0
    out = capsys.readouterr().out
    assert "graph_chunks:" in out
    assert "merge_chunks:" in out
    assert "merge_relationships:" in out


def test_onboard_plan_print_json_uses_deterministic_builder(tmp_path, monkeypatch, capsys):
    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.common.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: tmp_path)
    fixture = FIXTURES_ROOT / "synthetic" / "onboarding" / "20260414t151530z"
    bundle_dir = tmp_path / "raw" / "onboarding" / "bundles" / "20260414t151530z"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    for name in ("normalized-evidence.json", "synthesis-semantic.json", "synthesis-graph.json", "merge-decisions.json", "verify-report.json", "state.json"):
        (bundle_dir / name).write_text((fixture / name).read_text(encoding="utf-8"), encoding="utf-8")

    assert main(["onboard", "plan", "--bundle", "20260414t151530z", "--print-json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["bundle_id"] == "20260414t151530z"
    assert payload["pages"]


def test_mind_youtube_pull_records_provider_run(tmp_path):
    _write_config(tmp_path)
    with patch("mind.cli._project_root", return_value=tmp_path), \
         patch(
             "mind.cli.run_youtube_pull",
             return_value=types.SimpleNamespace(exit_code=0, detail="found 12 watch items"),
         ) as mock_pull:
        rc = main(["youtube", "pull", "--dry-run", "--limit", "12"])
    assert rc == 0
    mock_pull.assert_called_once_with(tmp_path, dry_run=True, limit=12)
    assert RuntimeState.for_repo_root(tmp_path).list_runs(limit=1)[0].kind == "youtube.pull"


def test_mind_audible_pull_delegates_to_module_main(tmp_path):
    _write_config(tmp_path)
    fake_result = types.SimpleNamespace(exit_code=0, detail="dry-run: audible auth file present and loadable")

    with patch("mind.cli._project_root", return_value=tmp_path), \
         patch("mind.cli.run_audible_pull", return_value=fake_result) as mock_pull:
        rc = main(["audible", "pull", "--dry-run", "--library-only", "--sleep", "2.5"])
    assert rc == 0
    mock_pull.assert_called_once_with(
        tmp_path,
        dry_run=True,
        library_only=True,
        sleep=2.5,
    )
    assert RuntimeState.for_repo_root(tmp_path).list_runs(limit=1)[0].kind == "audible.pull"


def test_mind_audible_pull_progress_goes_to_stderr_not_stdout(tmp_path, monkeypatch, capsys):
    _write_config(tmp_path)
    monkeypatch.setattr("mind.cli._project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.cli.progress_for_args", _fake_progress)
    monkeypatch.setattr("mind.cli.run_audible_pull", lambda *args, **kwargs: types.SimpleNamespace(exit_code=0, detail="ok"))

    assert main(["audible", "pull", "--dry-run"]) == 0
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "[progress] pulling Audible library export" in captured.err


def test_mind_substack_pull_prints_export_path(tmp_path, capsys):
    _write_config(tmp_path)
    target = tmp_path / "raw" / "exports" / "substack-saved-2026-04-08.json"
    with patch("mind.cli._project_root", return_value=tmp_path), \
         patch("scripts.substack.auth.build_client", return_value=object()), \
         patch("scripts.substack.pull.pull_saved", return_value=target) as mock_pull:
        rc = main(["substack", "pull", "--today", "2026-04-08"])
    assert rc == 0
    assert str(target) in capsys.readouterr().out
    mock_pull.assert_called_once()
    assert RuntimeState.for_repo_root(tmp_path).list_runs(limit=1)[0].kind == "substack.pull"


def test_mind_articles_drain_reports_summary(tmp_path, capsys):
    _write_config(tmp_path)
    fake_result = types.SimpleNamespace(
        drop_files_processed=1,
        urls_in_queue=2,
        skipped_existing=1,
        fetched_summarized=1,
        paywalled=0,
        failed=0,
    )

    with patch("mind.cli._project_root", return_value=tmp_path), \
         patch("mind.cli._repo_root", return_value=tmp_path), \
         patch("mind.cli.ingest_articles_queue", return_value=fake_result):
        rc = main(["articles", "drain", "--today", "2026-04-08"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "1 drop files" in out
    assert "1 fetched" in out
    state = RuntimeState.for_repo_root(tmp_path)
    assert state.list_runs(limit=1)[0].kind == "articles.drain"
    assert state.list_queue()[0].name == "articles"


def test_mind_repo_root_resolves_split_layout(tmp_path):
    (tmp_path / "config.yaml").write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n"
        "llm:\n"
        "  model: google/gemini-2.5-pro\n"
    )
    with patch("mind.cli._project_root", return_value=tmp_path):
        from mind.cli import _repo_root

        assert _repo_root() == tmp_path


def test_mind_reset_dry_run_reports_without_mutation(tmp_path, monkeypatch, capsys):
    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.common.project_root", lambda: tmp_path)

    note = tmp_path / "memory" / "people" / "example-owner.md"
    note.parent.mkdir(parents=True, exist_ok=True)
    note.write_text("# Example Owner\n", encoding="utf-8")
    onboarding_state = tmp_path / "raw" / "onboarding" / "current.json"
    onboarding_state.parent.mkdir(parents=True, exist_ok=True)
    onboarding_state.write_text('{"bundle_id":"private"}\n', encoding="utf-8")
    inbox_file = tmp_path / "dropbox" / "private.md"
    inbox_file.parent.mkdir(parents=True, exist_ok=True)
    inbox_file.write_text("# Private\n", encoding="utf-8")
    (tmp_path / ".brain-runtime.sqlite3").write_text("runtime", encoding="utf-8")
    (tmp_path / ".brain-graph.sqlite3").write_text("graph", encoding="utf-8")

    assert main(["reset"]) == 0
    out = capsys.readouterr().out
    assert "mode=dry-run" in out
    assert note.exists()
    assert onboarding_state.exists()
    assert inbox_file.exists()
    assert (tmp_path / ".brain-runtime.sqlite3").exists()
    assert (tmp_path / ".brain-graph.sqlite3").exists()


def test_mind_reset_apply_wipes_data_and_rebuilds_scaffold(tmp_path, monkeypatch, capsys):
    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.common.project_root", lambda: tmp_path)

    note = tmp_path / "memory" / "people" / "example-owner.md"
    note.parent.mkdir(parents=True, exist_ok=True)
    note.write_text("# Example Owner\n", encoding="utf-8")
    (tmp_path / "memory" / ".obsidian" / "workspace.json").parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / "memory" / ".obsidian" / "workspace.json").write_text("{}", encoding="utf-8")
    onboarding_state = tmp_path / "raw" / "onboarding" / "current.json"
    onboarding_state.parent.mkdir(parents=True, exist_ok=True)
    onboarding_state.write_text('{"bundle_id":"private"}\n', encoding="utf-8")
    exports_file = tmp_path / "raw" / "exports" / "saved.json"
    exports_file.parent.mkdir(parents=True, exist_ok=True)
    exports_file.write_text("{}", encoding="utf-8")
    inbox_file = tmp_path / "dropbox" / "private.md"
    inbox_file.parent.mkdir(parents=True, exist_ok=True)
    inbox_file.write_text("# Private\n", encoding="utf-8")
    (tmp_path / ".brain-runtime.sqlite3").write_text("runtime", encoding="utf-8")
    (tmp_path / ".brain-graph.sqlite3").write_text("graph", encoding="utf-8")

    assert main(["reset", "--apply"]) == 0
    out = capsys.readouterr().out
    assert "mode=apply" in out
    assert not note.exists()
    assert not onboarding_state.exists()
    assert not exports_file.exists()
    assert not inbox_file.exists()
    assert not (tmp_path / ".brain-runtime.sqlite3").exists()
    assert not (tmp_path / ".brain-graph.sqlite3").exists()

    assert (tmp_path / "memory" / "INDEX.md").read_text(encoding="utf-8") == "# INDEX\n"
    assert (tmp_path / "memory" / "CHANGELOG.md").read_text(encoding="utf-8") == "# CHANGELOG\n"
    assert (tmp_path / "raw" / "onboarding" / "bundles").exists()
    assert not (tmp_path / "raw" / "onboarding" / "current.json").exists()
    assert (tmp_path / "dropbox" / ".processed" / ".gitkeep").exists()
    assert (tmp_path / "dropbox" / ".review" / ".gitkeep").exists()
    brain_state = json.loads((tmp_path / "memory" / ".brain-state.json").read_text(encoding="utf-8"))
    assert brain_state["atoms"]["count"] == 0
    assert all(value == 0 for value in brain_state["atoms"]["by_type"].values())


def test_mind_links_import_writes_drop_file(tmp_path, capsys):
    _write_config(tmp_path)
    payload = tmp_path / "links.json"
    payload.write_text(json.dumps([{"url": "https://example.com/x", "title": "Example"}]), encoding="utf-8")

    with patch("mind.cli._project_root", return_value=tmp_path), \
         patch("mind.cli._repo_root", return_value=tmp_path):
        rc = main(["links", "import", str(payload), "--today", "2026-04-08"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "links-import: 1 links" in out
    assert (tmp_path / "raw" / "drops" / "articles-from-links-2026-04-08.jsonl").exists()
    queue = RuntimeState.for_repo_root(tmp_path).list_queue()
    assert queue[0].pending_count == 1


def test_mind_links_ingest_imports_and_drains(tmp_path, capsys):
    _write_config(tmp_path)
    payload = tmp_path / "links.json"
    payload.write_text(json.dumps([{"url": "https://example.com/x", "title": "Example"}]), encoding="utf-8")

    fake_result = types.SimpleNamespace(
        drop_files_processed=1,
        urls_in_queue=1,
        skipped_existing=0,
        fetched_summarized=1,
        paywalled=0,
        failed=0,
    )

    with patch("mind.cli._project_root", return_value=tmp_path), \
         patch("mind.cli._repo_root", return_value=tmp_path), \
         patch("mind.cli.ingest_articles_queue", return_value=fake_result):
        rc = main(["links", "ingest", str(payload), "--today", "2026-04-08"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "links-ingest: 1 links imported" in out
    assert "1 fetched" in out
    state = RuntimeState.for_repo_root(tmp_path)
    assert state.list_runs(limit=1)[0].kind == "links.ingest"
    assert state.list_queue()[0].pending_count == 0


def test_mind_readiness_new_user_fails_until_onboarding_and_substack_are_ready(tmp_path, monkeypatch, capsys):
    write_repo_config(
        tmp_path,
        create_indexes=True,
        ingestors_enabled=["youtube", "audible", "substack", "books", "articles"],
    )
    monkeypatch.setattr("mind.commands.common.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.readiness.command_common.project_root", lambda: tmp_path)
    monkeypatch.setattr("scripts.common.env.load", lambda: fake_env_config(tmp_path))
    monkeypatch.setattr("mind.commands.readiness._check_env", lambda: (True, "model=google/gemini-2.5-pro transport=ai_gateway"))
    monkeypatch.setattr("mind.commands.readiness._check_doctor", lambda: (True, "runtime/config diagnostics passed"))
    monkeypatch.setattr("mind.commands.readiness._check_state_health", lambda root: (True, "runs=0 queue=0 locks=0 db=.brain-runtime.sqlite3"))
    monkeypatch.setattr("scripts.audible.auth.load_authenticator", lambda: object())
    monkeypatch.setattr(
        "mind.commands.readiness.run_youtube_pull",
        lambda root, dry_run=True, limit=1: types.SimpleNamespace(exit_code=0, detail="found 1 watch item"),
    )
    monkeypatch.setattr(
        "mind.commands.readiness.run_audible_pull",
        lambda root, dry_run=True, library_only=True: types.SimpleNamespace(exit_code=0, detail="auth file present"),
    )
    monkeypatch.setattr("mind.commands.readiness._check_substack_auth", lambda: (False, "403 Forbidden"))
    monkeypatch.setattr(
        "mind.commands.readiness.read_onboarding_status",
        lambda root: (_ for _ in ()).throw(FileNotFoundError("no onboarding bundle found")),
    )
    monkeypatch.setattr(
        "mind.commands.readiness.run_ingest_readiness",
        lambda **kwargs: types.SimpleNamespace(
            passed=False,
            graph=GraphHealthSnapshot(
                graph_built=True,
                node_count=16,
                edge_count=24,
                document_count=16,
                embedding_model="google/text-embedding",
                embedding_count=16,
                embedding_backend="sqlite",
                embedding_backend_count=16,
                shadow_mode="advisory-only",
                promotion_gate_passed=False,
                promotion_gate_artifact_json=str(tmp_path / "raw" / "reports" / "graph-embed" / "gate.json"),
                promotion_gate_artifact_markdown=str(tmp_path / "raw" / "reports" / "graph-embed" / "gate.md"),
                issues=("shadow vector promotion gate is failing",),
            ),
            lanes=(
                types.SimpleNamespace(lane="youtube", selected_count=1, blocked_count=0, ready=True),
                types.SimpleNamespace(lane="books", selected_count=0, blocked_count=0, ready=True),
            ),
            issues=("shadow vector promotion gate is failing",),
            report_json_path=tmp_path / "raw" / "reports" / "ingest-review" / "readiness.json",
        ),
    )

    rc = cmd_readiness(
        argparse.Namespace(
            scope="new-user",
            dropbox_limit=None,
            lane_limit=None,
            include_promotion_gate=True,
            skip_source_checks=False,
        )
    )

    assert rc == 1
    out = capsys.readouterr().out
    assert "readiness-scope: new-user" in out
    assert "- onboarding session: fail (no onboarding bundle found)" in out
    assert "- graph health: pass (nodes=16 docs=16 embeddings=16/16)" in out
    assert "- promotion gate: fail" in out
    assert "- youtube dry-run: pass (found 1 watch item)" in out
    assert "- substack auth: fail (403 Forbidden)" in out
    assert "readiness: fail" in out


def test_mind_chrome_scan_reports_summary(tmp_path, capsys):
    _write_config(tmp_path)
    fake_result = types.SimpleNamespace(events_scanned=5, event_files=[tmp_path / "raw" / "chrome" / "events" / "x.jsonl"])

    with patch("mind.cli._project_root", return_value=tmp_path), \
         patch("mind.cli._repo_root", return_value=tmp_path), \
         patch("mind.cli.scan_chrome", return_value=fake_result):
        rc = main(["chrome", "scan", "--today", "2026-04-09"])

    assert rc == 0
    assert "chrome-scan: 5 events" in capsys.readouterr().out
    assert RuntimeState.for_repo_root(tmp_path).list_runs(limit=1)[0].kind == "chrome.scan"


def test_mind_chrome_ingest_updates_queue_state(tmp_path, capsys):
    _write_config(tmp_path)
    fake_result = types.SimpleNamespace(
        raw_events_seen=7,
        candidates_written=2,
        search_signals_written=1,
        candidate_drop_path=tmp_path / "raw" / "drops" / "web-discovery-candidates-from-chrome-2026-04-09.jsonl",
        search_signal_drop_path=tmp_path / "raw" / "drops" / "search-signals-from-chrome-2026-04-09.jsonl",
    )

    with patch("mind.cli._project_root", return_value=tmp_path), \
         patch("mind.cli._repo_root", return_value=tmp_path), \
         patch("mind.cli.ingest_chrome", return_value=fake_result):
        rc = main(["chrome", "ingest", "--today", "2026-04-09"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "2 candidates" in out
    queue = {item.name: item for item in RuntimeState.for_repo_root(tmp_path).list_queue()}
    assert queue["web-discovery"].pending_count == 2
    assert queue["search-signals"].pending_count == 1


def test_mind_search_signals_ingest_reports_pages(tmp_path, capsys):
    _write_config(tmp_path)
    fake_result = types.SimpleNamespace(drop_files_processed=1, signals_materialized=3, pages_written=1)

    with patch("mind.cli._project_root", return_value=tmp_path), \
         patch("mind.cli._repo_root", return_value=tmp_path), \
         patch("mind.cli.ingest_search_signals", return_value=fake_result):
        rc = main(["search-signals", "ingest", "--today", "2026-04-09"])

    assert rc == 0
    assert "3 signals" in capsys.readouterr().out
    assert RuntimeState.for_repo_root(tmp_path).list_runs(limit=1)[0].kind == "search-signals.ingest"


def test_mind_web_discovery_drain_reports_summary(tmp_path, capsys):
    _write_config(tmp_path)
    fake_result = types.SimpleNamespace(
        drop_files_processed=1,
        candidates_processed=2,
        pages_written=2,
        crawled=0,
        failed=0,
    )

    with patch("mind.cli._project_root", return_value=tmp_path), \
         patch("mind.cli._repo_root", return_value=tmp_path), \
         patch("mind.cli.drain_web_discovery", return_value=fake_result):
        rc = main(["web-discovery", "drain", "--today", "2026-04-09"])

    assert rc == 0
    assert "2 pages" in capsys.readouterr().out
    assert RuntimeState.for_repo_root(tmp_path).list_runs(limit=1)[0].kind == "web-discovery.drain"


def test_mind_dream_campaign_dispatches_runtime(tmp_path, monkeypatch, capsys):
    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.dream.progress_for_args", _fake_progress)
    seen: dict[str, object] = {}

    def fake_run_campaign(*, days, start_date, dry_run, resume, profile):
        seen.update(
            {
                "days": days,
                "start_date": start_date,
                "dry_run": dry_run,
                "resume": resume,
                "profile": profile,
            }
        )
        return types.SimpleNamespace(render=lambda: "campaign-ok")

    with patch("mind.commands.dream.run_campaign", fake_run_campaign):
        rc = main(["dream", "campaign", "--days", "7", "--start-date", "2026-04-21", "--dry-run"])

    assert rc == 0
    assert seen == {
        "days": 7,
        "start_date": "2026-04-21",
        "dry_run": True,
        "resume": False,
        "profile": "aggressive",
    }
    assert "campaign-ok" in capsys.readouterr().out


def test_mind_dream_simulate_year_dispatches_runtime(tmp_path, monkeypatch, capsys):
    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.dream.project_root", lambda: tmp_path)
    monkeypatch.setattr("mind.commands.dream.progress_for_args", _fake_progress)
    seen: dict[str, object] = {}

    def fake_run_simulate_year(*, repo_root, start_date, run_id, days, dry_run):
        seen.update(
            {
                "repo_root": repo_root,
                "start_date": start_date,
                "run_id": run_id,
                "days": days,
                "dry_run": dry_run,
            }
        )
        return types.SimpleNamespace(render=lambda: "simulate-year-ok")

    with patch("mind.commands.dream.run_simulate_year", fake_run_simulate_year):
        rc = main(
            [
                "dream",
                "simulate-year",
                "--days",
                "3",
                "--start-date",
                "2026-04-21",
                "--run-id",
                "sim-test",
                "--dry-run",
            ]
        )

    assert rc == 0
    assert seen == {
        "repo_root": tmp_path,
        "start_date": "2026-04-21",
        "run_id": "sim-test",
        "days": 3,
        "dry_run": True,
    }
    assert "simulate-year-ok" in capsys.readouterr().out


def test_mind_dream_weave_shadow_v2_dispatches_runtime(tmp_path, capsys):
    _write_config(tmp_path)
    seen: list[tuple[str, bool]] = []

    def fake_run_weave(*, dry_run):
        seen.append(("v1", dry_run))
        return types.SimpleNamespace(render=lambda: "weave-v1-ok")

    def fake_run_weave_v2_shadow(*, dry_run):
        seen.append(("v2-shadow", dry_run))
        return types.SimpleNamespace(render=lambda: "weave-v2-shadow-ok")

    with patch("mind.commands.dream.run_weave", fake_run_weave), \
         patch("mind.commands.dream.run_weave_v2_shadow", fake_run_weave_v2_shadow):
        rc = main(["dream", "weave", "--dry-run", "--shadow-v2"])

    assert rc == 0
    assert seen == [("v2-shadow", True)]
    out = capsys.readouterr().out
    assert "weave-v2-shadow-ok" in out


def test_mind_dream_rem_runs_weave_handoff_when_enabled(tmp_path, capsys):
    _write_config(tmp_path)
    seen: list[tuple[str, bool]] = []

    def fake_run_rem(*, dry_run):
        seen.append(("rem", dry_run))
        return types.SimpleNamespace(render=lambda: "rem-ok")

    def fake_run_weave(*, dry_run):
        seen.append(("weave", dry_run))
        return types.SimpleNamespace(render=lambda: "weave-ok")

    with patch("mind.commands.dream.run_rem", fake_run_rem), \
         patch("mind.commands.dream.run_weave", fake_run_weave):
        rc = main(["dream", "rem"])

    assert rc == 0
    assert seen == [("rem", False), ("weave", False)]
    out = capsys.readouterr().out
    assert "rem-ok" in out
    assert "weave-ok" in out
