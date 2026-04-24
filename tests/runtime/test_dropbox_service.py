from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from mind.services.dropbox import migrate_legacy_dropbox_files, scan_dropbox_pending, sweep_dropbox
from mind.services.graph_registry import GraphRegistry
from tests.support import write_repo_config


def _write_config(root: Path) -> None:
    write_repo_config(root, create_indexes=True)


def test_scan_dropbox_pending_classifies_supported_and_machine_queue(tmp_path: Path):
    _write_config(tmp_path)
    dropbox = tmp_path / "dropbox"
    (dropbox / ".processed").mkdir(parents=True, exist_ok=True)
    (dropbox / ".failed").mkdir(parents=True, exist_ok=True)
    (dropbox / ".reports").mkdir(parents=True, exist_ok=True)
    (dropbox / "note.md").write_text("# Note\n\nhello\n", encoding="utf-8")
    (dropbox / "books-reading.csv").write_text("title,author\nBook,Author\n", encoding="utf-8")
    (dropbox / "articles-from-substack-2026-04-11.jsonl").write_text("{}", encoding="utf-8")
    (dropbox / ".processed" / "ignored.md").write_text("ignored", encoding="utf-8")

    items = scan_dropbox_pending(tmp_path)

    by_name = {item.path.name: item for item in items}
    assert by_name["note.md"].route == "file"
    assert by_name["books-reading.csv"].route == "books"
    assert by_name["articles-from-substack-2026-04-11.jsonl"].classification == "machine-queue"
    assert "ignored.md" not in by_name


def test_sweep_dropbox_processes_supported_files_and_archives_failures(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    dropbox = tmp_path / "dropbox"
    dropbox.mkdir(parents=True, exist_ok=True)
    note = dropbox / "note.md"
    books = dropbox / "books-reading.csv"
    machine_queue = dropbox / "articles-from-substack-2026-04-11.jsonl"
    note.write_text("# Note\n\nhello\n", encoding="utf-8")
    books.write_text("title,author\nBook,Author\n", encoding="utf-8")
    machine_queue.write_text('{"url":"https://example.com"}\n', encoding="utf-8")

    monkeypatch.setattr(GraphRegistry, "rebuild", lambda self: None)
    monkeypatch.setattr(
        "mind.services.dropbox.ingest_file_with_details",
        lambda path, **kwargs: (tmp_path / "memory" / "summaries" / f"{path.stem}.md", {}),
    )
    monkeypatch.setattr(
        "mind.services.dropbox.ingest_books_export",
        lambda path: SimpleNamespace(page_ids=["book-page", "summary-book-page"]),
    )

    result = sweep_dropbox(tmp_path)

    assert result.processed_count == 2
    assert result.unsupported_count == 1
    assert result.failed_count == 0
    assert not note.exists()
    assert not books.exists()
    assert not machine_queue.exists()
    assert (dropbox / ".processed" / "note.md").exists()
    assert (dropbox / ".processed" / "books-reading.csv").exists()
    assert (dropbox / ".failed" / "articles-from-substack-2026-04-11.jsonl").exists()
    assert result.report_json_path.exists()
    assert result.report_markdown_path.exists()
    assert result.mirror_json_path.exists()
    assert result.mirror_markdown_path.exists()


def test_sweep_dropbox_routes_youtube_and_substack_exports_through_planner_ingest(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    dropbox = tmp_path / "dropbox"
    dropbox.mkdir(parents=True, exist_ok=True)
    youtube_export = dropbox / "youtube-recent-2026-04-11.json"
    substack_export = dropbox / "substack-saved-2026-04-11.json"
    youtube_export.write_text("[]", encoding="utf-8")
    substack_export.write_text('{"posts":[]}', encoding="utf-8")
    seen: dict[str, object] = {}

    monkeypatch.setattr(
        "mind.services.dropbox.ingest_youtube_export",
        lambda path: seen.__setitem__("youtube_path", path)
        or SimpleNamespace(selected_count=2, executed=1, failed=0, pages_written=1),
    )
    monkeypatch.setattr(
        "mind.services.dropbox.ingest_substack_export",
        lambda **kwargs: seen.__setitem__("substack_kwargs", kwargs)
        or SimpleNamespace(selected_count=2, executed=1, failed=0, paywalled=1, posts_written=1),
    )

    result = sweep_dropbox(tmp_path)

    assert result.processed_count == 2
    assert seen["youtube_path"] == youtube_export
    assert seen["substack_kwargs"] == {"export_path": substack_export, "drain_articles": False}
    assert (dropbox / ".processed" / youtube_export.name).exists()
    assert (dropbox / ".processed" / substack_export.name).exists()


def test_sweep_dropbox_dry_run_does_not_mutate_files(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    dropbox = tmp_path / "dropbox"
    dropbox.mkdir(parents=True, exist_ok=True)
    note = dropbox / "note.md"
    note.write_text("# Note\n\nhello\n", encoding="utf-8")

    monkeypatch.setattr(
        "mind.services.dropbox.preflight_file_ingest",
        lambda path, **kwargs: SimpleNamespace(details={"review_required": False, "would_create_canonical_page": True}),
    )

    result = sweep_dropbox(tmp_path, dry_run=True)

    assert result.scanned_count == 1
    assert result.processed_count == 0
    assert result.predicted_process_count == 1
    assert note.exists()
    assert not (dropbox / ".processed" / "note.md").exists()


def test_sweep_dropbox_dry_run_preflights_graph_review(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    dropbox = tmp_path / "dropbox"
    note = dropbox / "note.md"
    note.parent.mkdir(parents=True, exist_ok=True)
    note.write_text("# Note\n\nhello\n", encoding="utf-8")

    monkeypatch.setattr(GraphRegistry, "rebuild", lambda self: None)
    monkeypatch.setattr(
        "mind.services.dropbox.preflight_file_ingest",
        lambda path, **kwargs: SimpleNamespace(
            details={
                "review_required": True,
                "review_reasons": ["Example Product: multiple plausible graph candidates"],
                "candidate_summaries": ["Example Product: the-pick-ai (fts_title_alias, 0.84)"],
                "would_patch_existing_node": False,
                "would_create_canonical_page": True,
                "canonical_page_target": str(tmp_path / "memory" / "summaries" / "note.md"),
            }
        ),
    )

    result = sweep_dropbox(tmp_path, dry_run=True)

    assert result.predicted_review_count == 1
    assert result.predicted_fail_count == 0
    assert result.predicted_create_canonical_count == 1
    assert result.outcomes[0].disposition == "would_review"
    assert "multiple plausible graph candidates" in result.outcomes[0].detail
    assert note.exists()
    assert not (dropbox / ".review" / "note.md").exists()


def test_sweep_dropbox_marks_review_required_files(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    dropbox = tmp_path / "dropbox"
    note = dropbox / "note.md"
    note.parent.mkdir(parents=True, exist_ok=True)
    note.write_text("# Note\n\nhello\n", encoding="utf-8")

    monkeypatch.setattr(GraphRegistry, "rebuild", lambda self: None)
    monkeypatch.setattr(
        "mind.services.dropbox.ingest_file_with_details",
        lambda path, **kwargs: (
            tmp_path / "memory" / "summaries" / f"{path.stem}.md",
            {"review_required": True, "review_artifacts": ["review.json", "review.md"]},
        ),
    )

    result = sweep_dropbox(tmp_path)

    assert result.review_count == 1
    assert result.processed_count == 0
    assert result.outcomes[0].disposition == "review"
    assert (dropbox / ".review" / "note.md").exists()


def test_sweep_dropbox_rejects_target_outside_dropbox(tmp_path: Path):
    _write_config(tmp_path)
    external = tmp_path / "raw" / "drops" / "note.md"
    external.parent.mkdir(parents=True, exist_ok=True)
    external.write_text("# Note\n\nhello\n", encoding="utf-8")

    try:
        sweep_dropbox(tmp_path, target_path=external)
    except ValueError as exc:
        assert "must stay within dropbox/" in str(exc)
    else:  # pragma: no cover - defensive assertion
        raise AssertionError("expected sweep_dropbox() to reject targets outside dropbox/")


def test_sweep_dropbox_directory_target_skips_reserved_archive_dirs(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    dropbox = tmp_path / "dropbox"
    note = dropbox / "note.md"
    archived = dropbox / ".failed" / "archived.md"
    archived.parent.mkdir(parents=True, exist_ok=True)
    note.parent.mkdir(parents=True, exist_ok=True)
    note.write_text("# Note\n\nhello\n", encoding="utf-8")
    archived.write_text("# Archived\n\nignore me\n", encoding="utf-8")

    monkeypatch.setattr(
        "mind.services.dropbox.ingest_file_with_details",
        lambda path, **kwargs: (tmp_path / "memory" / "summaries" / f"{path.stem}.md", {}),
    )

    result = sweep_dropbox(tmp_path, dry_run=True, target_path=dropbox)

    assert result.scanned_count == 1
    assert result.outcomes[0].source_path.endswith("note.md")


def test_migrate_legacy_dropbox_files_moves_user_files_and_keeps_machine_queue(tmp_path: Path):
    _write_config(tmp_path)
    legacy_root = tmp_path / "raw" / "drops"
    legacy_root.mkdir(parents=True, exist_ok=True)
    legacy_note = legacy_root / "note.md"
    queue_file = legacy_root / "articles-from-substack-2026-04-11.jsonl"
    legacy_note.write_text("# Note\n\nhello\n", encoding="utf-8")
    queue_file.write_text('{"url":"https://example.com"}\n', encoding="utf-8")

    result = migrate_legacy_dropbox_files(tmp_path)

    assert result.moved_count == 1
    assert result.kept_count == 1
    assert not legacy_note.exists()
    assert (tmp_path / "dropbox" / "note.md").exists()
    assert queue_file.exists()
    assert result.report_json_path.exists()
    assert result.report_markdown_path.exists()
