from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from mind.cli import build_parser, main
from mind.runtime_state import RuntimeState
from tests.support import subcommand_names, write_repo_config


def _write_config(root: Path) -> None:
    write_repo_config(root, create_indexes=True)


def _patch_project_root(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)


def test_top_level_parser_includes_dropbox():
    assert "dropbox" in subcommand_names(build_parser())


def test_dropbox_parser_exposes_expected_subcommands():
    assert subcommand_names(build_parser(), "dropbox") >= {"sweep", "status", "migrate-legacy"}


def test_dropbox_sweep_updates_runtime_queue(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    note = tmp_path / "dropbox" / "note.md"
    note.parent.mkdir(parents=True, exist_ok=True)
    note.write_text("# Note\n\nhello\n", encoding="utf-8")

    monkeypatch.setattr(
        "mind.services.dropbox.GraphRegistry.rebuild",
        lambda self: None,
    )
    monkeypatch.setattr(
        "mind.services.dropbox.ingest_file_with_details",
        lambda path, **kwargs: (tmp_path / "memory" / "summaries" / f"{path.stem}.md", {}),
    )

    assert main(["dropbox", "sweep"]) == 0
    out = capsys.readouterr().out
    assert "dropbox-sweep:" in out
    queue = {item.name: item for item in RuntimeState.for_repo_root(tmp_path).list_queue()}
    assert queue["dropbox"].status == "ready"
    assert queue["dropbox"].pending_count == 0
    assert (tmp_path / "dropbox" / ".processed" / "note.md").exists()


def test_dropbox_status_reads_pending_files(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    note = tmp_path / "dropbox" / "note.md"
    note.parent.mkdir(parents=True, exist_ok=True)
    note.write_text("# Note\n\nhello\n", encoding="utf-8")
    state = RuntimeState.for_repo_root(tmp_path)
    state.upsert_queue_state(
        name="dropbox",
        status="queued",
        pending_count=1,
        metadata={
            "processed_count": 0,
            "failed_count": 0,
            "unsupported_count": 0,
            "pending_count_after": 1,
            "failed_items": [],
            "last_sweep_at": "2026-04-12T18:00:00Z",
        },
    )

    assert main(["dropbox", "status"]) == 0
    out = capsys.readouterr().out
    assert "dropbox-status: pending=1" in out
    assert "file=1" in out


def test_dropbox_migrate_legacy_moves_user_files(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    legacy = tmp_path / "raw" / "drops" / "note.md"
    legacy.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_text("# Note\n\nhello\n", encoding="utf-8")

    assert main(["dropbox", "migrate-legacy"]) == 0
    out = capsys.readouterr().out
    assert "dropbox-migrate-legacy:" in out
    assert not legacy.exists()
    assert (tmp_path / "dropbox" / "note.md").exists()


def test_dropbox_sweep_returns_nonzero_for_review_queue(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    note = tmp_path / "dropbox" / "note.md"
    note.parent.mkdir(parents=True, exist_ok=True)
    note.write_text("# Note\n\nhello\n", encoding="utf-8")

    monkeypatch.setattr("mind.services.dropbox.GraphRegistry.rebuild", lambda self: None)
    monkeypatch.setattr(
        "mind.services.dropbox.ingest_file_with_details",
        lambda path, **kwargs: (
            tmp_path / "memory" / "summaries" / f"{path.stem}.md",
            {"review_required": True, "review_artifacts": ["review.json", "review.md"]},
        ),
    )

    assert main(["dropbox", "sweep"]) == 1
    out = capsys.readouterr().out
    assert "- review=1" in out


def test_dropbox_sweep_rejects_path_outside_dropbox(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    external = tmp_path / "raw" / "drops" / "note.md"
    external.parent.mkdir(parents=True, exist_ok=True)
    external.write_text("# Note\n\nhello\n", encoding="utf-8")

    assert main(["dropbox", "sweep", "--path", str(external)]) == 1
    out = capsys.readouterr().out
    assert "must stay within dropbox/" in out


def test_dropbox_sweep_dry_run_reports_graph_preflight_counts(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    note = tmp_path / "dropbox" / "note.md"
    note.parent.mkdir(parents=True, exist_ok=True)
    note.write_text("# Note\n\nhello\n", encoding="utf-8")

    monkeypatch.setattr("mind.services.dropbox.GraphRegistry.rebuild", lambda self: None)
    monkeypatch.setattr(
        "mind.services.dropbox.preflight_file_ingest",
        lambda path, **kwargs: SimpleNamespace(
            details={
                "review_required": True,
                "review_reasons": ["Example Product: multiple plausible graph candidates"],
                "candidate_summaries": ["Example Product: the-pick-ai (fts_title_alias, 0.84)"],
                "would_patch_existing_node": True,
                "would_create_canonical_page": True,
                "canonical_page_target": str(tmp_path / "memory" / "summaries" / "note.md"),
            }
        ),
    )

    assert main(["dropbox", "sweep", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "- would_review=1" in out
    assert "- would_patch_existing_node=1" in out
    assert "- would_create_canonical_page=1" in out
