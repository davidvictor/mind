from __future__ import annotations

from datetime import date
import json
from pathlib import Path
from types import SimpleNamespace

from mind.commands.ingest import SubstackIngestResult, _ingest_books_export_direct, ingest_books_export, ingest_substack_export, ingest_youtube_export
from mind.services.provider_ops import run_youtube_pull
from mind.services.queue_worker import dispatch_run, process_one_queued_run
from tests.support import write_repo_config


def _write_config(root: Path) -> None:
    write_repo_config(root, create_me=True, create_exports=True)


def test_run_youtube_pull_does_not_reuse_stale_export_on_failure(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    stale = tmp_path / "raw" / "exports" / "youtube-recent-2026-04-08.json"
    stale.write_text("[]", encoding="utf-8")
    monkeypatch.setattr("mind.services.provider_ops.env.load", lambda: SimpleNamespace(browser_for_cookies="chrome"))
    monkeypatch.setattr(
        "scripts.youtube.pull.run",
        lambda **kwargs: SimpleNamespace(exit_code=1, detail="HTTP Error 403", records=[], export_path=None),
    )

    result = run_youtube_pull(tmp_path)

    assert result.exit_code == 1
    assert result.export_path is None
    assert result.detail == "HTTP Error 403"


def test_ingest_substack_export_can_skip_article_drain(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    export_path = tmp_path / "raw" / "exports" / "substack-saved-2026-04-09.json"
    export_path.write_text(json.dumps({"posts": []}), encoding="utf-8")
    seen = {"drained": False}

    monkeypatch.setattr("mind.commands.ingest.substack_auth.build_client", lambda: object())
    monkeypatch.setattr("mind.commands.ingest.substack_parse.parse_export", lambda data: [])
    monkeypatch.setattr(
        "mind.commands.ingest.ingest_articles_queue",
        lambda **kwargs: seen.__setitem__("drained", True),
    )

    result = ingest_substack_export(
        export_path=export_path,
        today="2026-04-09",
        drain_articles=False,
    )

    assert isinstance(result, SubstackIngestResult)
    assert result.linked_articles_fetched == 0
    assert seen["drained"] is False


def test_ingest_substack_export_drains_article_queue_by_default(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    export_path = tmp_path / "raw" / "exports" / "substack-saved-2026-04-09.json"
    export_path.write_text(json.dumps({"posts": []}), encoding="utf-8")
    seen = {"drained": False}

    monkeypatch.setattr("mind.commands.ingest.substack_auth.build_client", lambda: object())
    monkeypatch.setattr("mind.commands.ingest.substack_parse.parse_export", lambda data: [])
    monkeypatch.setattr(
        "mind.commands.ingest.ingest_articles_queue",
        lambda **kwargs: seen.__setitem__("drained", True)
        or SimpleNamespace(drop_files_processed=1, fetched_summarized=2, failed=0),
    )

    result = ingest_substack_export(
        export_path=export_path,
        today="2026-04-09",
    )

    assert result.linked_articles_fetched == 2
    assert seen["drained"] is True


def test_ingest_substack_export_fails_when_article_queue_drain_fails(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    export_path = tmp_path / "raw" / "exports" / "substack-saved-2026-04-09.json"
    export_path.write_text(json.dumps({"posts": []}), encoding="utf-8")

    monkeypatch.setattr("mind.commands.ingest.substack_auth.build_client", lambda: object())
    monkeypatch.setattr("mind.commands.ingest.substack_parse.parse_export", lambda data: [])
    monkeypatch.setattr(
        "mind.commands.ingest.ingest_articles_queue",
        lambda **kwargs: SimpleNamespace(drop_files_processed=1, fetched_summarized=2, failed=3),
    )

    result = ingest_substack_export(
        export_path=export_path,
        today="2026-04-09",
    )

    assert result.linked_articles_fetched == 2
    assert result.failed == 3
    assert "linked article queue: failed=3" in result.failed_items


def test_ingest_substack_export_logs_pass_d_error_without_failing_record(tmp_path: Path, monkeypatch):
    from scripts.common.vault import Vault

    _write_config(tmp_path)
    export_path = tmp_path / "raw" / "exports" / "substack-saved-2026-04-09.json"
    export_path.write_text(json.dumps({"posts": [{"id": "1"}]}), encoding="utf-8")

    record = type("FakeRecord", (), {
        "id": "1",
        "title": "On Trust",
        "url": "https://example.com/p/on-trust",
        "is_paywalled": False,
        "body_html": None,
    })()

    lifecycle = type("Lifecycle", (), {
        "envelope": {
            "schema_version": 1,
            "source_id": "substack-1",
            "pass_a": {},
            "pass_b": {},
            "pass_c": {},
            "pass_d": {},
            "verification": {},
            "materialization_hints": {},
        },
        "propagate": {
            "pass_d": [{"stage": "pass_d.dispatch", "summary": "RuntimeError: boom"}],
            "unsaved_refs": 0,
        },
        "materialized": {
            "article": str(tmp_path / "memory" / "sources" / "substack" / "example" / "on-trust.md"),
        },
    })()

    monkeypatch.setattr("mind.commands.ingest.substack_auth.build_client", lambda: object())
    monkeypatch.setattr("mind.commands.ingest.substack_parse.parse_export", lambda data: [record])
    monkeypatch.setattr("mind.commands.ingest.substack_enrich.run_substack_record_lifecycle", lambda *args, **kwargs: lifecycle)
    monkeypatch.setattr("mind.commands.ingest.vault", lambda: Vault.load(tmp_path))

    result = ingest_substack_export(
        export_path=export_path,
        today="2026-04-09",
        drain_articles=False,
    )

    assert result.posts_written == 1
    failure_log = tmp_path / "memory" / "inbox" / "substack-failures-2026-04-09.md"
    assert failure_log.exists()
    log_text = failure_log.read_text(encoding="utf-8")
    assert "stage=pass_d.dispatch" in log_text
    assert log_text.count("stage=pass_d.dispatch") == 1


def test_ingest_substack_export_logs_acquire_failure_into_failure_log(tmp_path: Path, monkeypatch):
    from scripts.common.vault import Vault

    _write_config(tmp_path)
    export_path = tmp_path / "raw" / "exports" / "substack-saved-2026-04-09.json"
    export_path.write_text(json.dumps({"posts": [{"id": "1"}]}), encoding="utf-8")

    record = type("FakeRecord", (), {
        "id": "1",
        "title": "On Trust",
        "url": "https://example.com/p/on-trust",
        "is_paywalled": False,
        "body_html": None,
    })()

    monkeypatch.setattr("mind.commands.ingest.substack_auth.build_client", lambda: object())
    monkeypatch.setattr("mind.commands.ingest.substack_parse.parse_export", lambda data: [record])
    monkeypatch.setattr(
        "mind.commands.ingest.substack_enrich.run_substack_record_lifecycle",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr("mind.commands.ingest.vault", lambda: Vault.load(tmp_path))

    result = ingest_substack_export(
        export_path=export_path,
        today="2026-04-09",
        drain_articles=False,
    )

    assert result.failed == 1
    failure_log = tmp_path / "memory" / "inbox" / "substack-failures-2026-04-09.md"
    assert failure_log.exists()
    assert "stage=acquire" in failure_log.read_text(encoding="utf-8")


def test_ingest_substack_export_ignores_preexisting_export_marker_for_planner_execution(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    export_path = tmp_path / "raw" / "exports" / "substack-saved-2026-04-09.json"
    export_path.write_text(json.dumps({"posts": []}), encoding="utf-8")
    marker = tmp_path / "memory" / "sources" / "substack" / f".ingested-{export_path.name}"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("", encoding="utf-8")
    seen = {"executed": False}

    monkeypatch.setattr("mind.commands.ingest.substack_auth.build_client", lambda: object())
    monkeypatch.setattr("mind.commands.ingest.build_inventory", lambda *args, **kwargs: SimpleNamespace(items=tuple()))
    monkeypatch.setattr("mind.commands.ingest.refresh_registry_for_inventory", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "mind.commands.ingest.build_plan",
        lambda *args, **kwargs: SimpleNamespace(
            selected_count=1,
            skipped_materialized_count=0,
            resumable_count=1,
            blocked_count=0,
            stale_count=0,
            items=(),
        ),
    )
    monkeypatch.setattr(
        "mind.commands.ingest.execute_substack_plan",
        lambda *args, **kwargs: seen.__setitem__("executed", True)
        or (
            SimpleNamespace(
                executed_count=1,
                failed_count=0,
                blocked_samples=(),
                failed_items=(),
                completed_items=(),
            ),
            (),
        ),
    )

    result = ingest_substack_export(export_path=export_path, today="2026-04-09", drain_articles=False)

    assert seen["executed"] is True
    assert result.selected_count == 1


def test_articles_lifecycle_logs_pass_d_error_without_failing_entry(tmp_path: Path, monkeypatch):
    from scripts.articles.fetch import ArticleFetchResult
    from scripts.articles.parse import ArticleDropEntry
    from scripts.common.vault import Vault

    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.ingest.vault", lambda: Vault.load(tmp_path))

    entry = ArticleDropEntry(
        url="https://example.com/article",
        source_post_id="1",
        source_post_url="https://example.com/source",
        anchor_text="article",
        context_snippet="ctx",
        category="business",
        discovered_at="2026-04-09T00:00:00Z",
        source_type="substack-link",
    )
    fr = ArticleFetchResult(
        body_text="Body text",
        title="Title",
        author="Author",
        sitename="Outlet",
        published="2024-05-15",
        raw_html_path=tmp_path / "raw" / "transcripts" / "articles" / "fake.html",
    )
    lifecycle = type("Lifecycle", (), {
        "materialized": {
                "article": str(tmp_path / "memory" / "sources" / "articles" / "x.md"),
                "summary": str(tmp_path / "memory" / "summaries" / "summary-article-x.md"),
        },
        "propagate": {"pass_d": [{"stage": "pass_d.dispatch", "summary": "RuntimeError: boom"}]},
    })()

    drop_dir = tmp_path / "raw" / "drops"
    drop_dir.mkdir(parents=True, exist_ok=True)
    (drop_dir / "articles-from-substack-2026-04-09.jsonl").write_text(
        '{"url":"https://example.com/article","source_post_id":"1","source_post_url":"https://example.com/source","anchor_text":"article","context_snippet":"ctx","category":"business","discovered_at":"2026-04-09T00:00:00Z","source_type":"substack-link"}\n',
        encoding="utf-8",
    )

    monkeypatch.setattr("scripts.articles.pipeline.fetch_article", lambda entry, repo_root: fr)
    monkeypatch.setattr(
        "scripts.articles.pipeline.run_article_entry_lifecycle",
        lambda entry, fetch_result, repo_root, today, summarize_override=None: lifecycle,
    )
    (Path(lifecycle.materialized["article"])).parent.mkdir(parents=True, exist_ok=True)
    (Path(lifecycle.materialized["article"])).write_text("x", encoding="utf-8")

    from scripts.articles import pipeline

    result = pipeline.drain_drop_queue(today_str="2026-04-09", repo_root=tmp_path)

    assert result.fetched_summarized == 1
    failure_log = tmp_path / "memory" / "inbox" / "articles-failures-2026-04-09.md"
    assert failure_log.exists()
    log_text = failure_log.read_text(encoding="utf-8")
    assert "stage=pass_d.dispatch" in log_text
    assert log_text.count("stage=pass_d.dispatch") == 1


def test_ingest_youtube_export_logs_pass_d_error_without_failing_video(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.ingest.vault", lambda: __import__("scripts.common.vault", fromlist=["Vault"]).Vault.load(tmp_path))
    export_path = tmp_path / "raw" / "exports" / "youtube-recent-2026-04-09.json"
    export_path.write_text(json.dumps([{"video_id": "abc123xyz00", "title": "Video", "channel": "Channel", "watched_at": "2026-04-01T10:00:00Z"}]), encoding="utf-8")
    lifecycle = type("Lifecycle", (), {
        "propagate": {"pass_d": [{"stage": "pass_d.dispatch", "summary": "RuntimeError: boom"}]},
        "materialized": {
            "video": str(tmp_path / "memory" / "sources" / "youtube" / "business" / "video.md"),
        },
    })()
    monkeypatch.setattr("mind.commands.ingest.youtube_enrich.run_youtube_record_lifecycle", lambda *args, **kwargs: lifecycle)

    from mind.commands.ingest import ingest_youtube_export

    result = ingest_youtube_export(export_path)

    assert result.pages_written == 1
    assert result.failed == 0
    failure_log = tmp_path / "memory" / "inbox" / f"youtube-failures-{date.today().isoformat()}.md"
    assert failure_log.exists()
    log_text = failure_log.read_text(encoding="utf-8")
    assert "stage=pass_d.dispatch" in log_text
    assert log_text.count("stage=pass_d.dispatch") == 1


def test_ingest_youtube_export_updates_index_and_changelog(tmp_path: Path, monkeypatch):
    from scripts.common.vault import Vault

    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.ingest.vault", lambda: Vault.load(tmp_path))
    export_path = tmp_path / "raw" / "exports" / "youtube-recent-2026-04-09.json"
    export_path.write_text("[]", encoding="utf-8")
    completed = SimpleNamespace(
        title="Video",
        source_id="youtube-abc123xyz00",
        materialized_paths={
            "video": str(tmp_path / "memory" / "sources" / "youtube" / "business" / "video.md"),
        },
        propagate={},
    )
    monkeypatch.setattr("mind.commands.ingest.build_inventory", lambda *args, **kwargs: SimpleNamespace(items=tuple()))
    monkeypatch.setattr("mind.commands.ingest.refresh_registry_for_inventory", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "mind.commands.ingest.build_plan",
        lambda *args, **kwargs: SimpleNamespace(
            selected_count=1,
            skipped_materialized_count=0,
            resumable_count=0,
            blocked_count=0,
            stale_count=0,
            items=(),
        ),
    )
    monkeypatch.setattr(
        "mind.commands.ingest.execute_youtube_plan",
        lambda *args, **kwargs: SimpleNamespace(
            executed_count=1,
            failed_count=0,
            blocked_samples=(),
            failed_items=(),
            completed_items=(completed,),
        ),
    )

    result = ingest_youtube_export(export_path)

    assert result.pages_written == 1
    assert "[[video]]" in (tmp_path / "memory" / "INDEX.md").read_text(encoding="utf-8")
    assert "[[video]]" in (tmp_path / "memory" / "CHANGELOG.md").read_text(encoding="utf-8")


def test_ingest_youtube_export_does_not_write_export_marker(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.ingest.vault", lambda: __import__("scripts.common.vault", fromlist=["Vault"]).Vault.load(tmp_path))
    export_path = tmp_path / "raw" / "exports" / "youtube-recent-2026-04-09.json"
    export_path.write_text("[]", encoding="utf-8")
    monkeypatch.setattr("mind.commands.ingest.build_inventory", lambda *args, **kwargs: SimpleNamespace(items=tuple()))
    monkeypatch.setattr("mind.commands.ingest.refresh_registry_for_inventory", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "mind.commands.ingest.build_plan",
        lambda *args, **kwargs: SimpleNamespace(
            selected_count=0,
            skipped_materialized_count=0,
            resumable_count=0,
            blocked_count=0,
            stale_count=0,
            items=(),
        ),
    )
    monkeypatch.setattr(
        "mind.commands.ingest.execute_youtube_plan",
        lambda *args, **kwargs: SimpleNamespace(
            executed_count=0,
            failed_count=0,
            blocked_samples=(),
            failed_items=(),
            completed_items=(),
        ),
    )

    ingest_youtube_export(export_path)

    marker = tmp_path / "memory" / "sources" / "youtube" / f".ingested-{export_path.name}"
    assert not marker.exists()


def test_ingest_youtube_export_logs_acquire_failure_into_failure_log(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.ingest.vault", lambda: __import__("scripts.common.vault", fromlist=["Vault"]).Vault.load(tmp_path))
    export_path = tmp_path / "raw" / "exports" / "youtube-recent-2026-04-09.json"
    export_path.write_text(json.dumps([{"video_id": "abc123xyz00", "title": "Video", "channel": "Channel", "watched_at": "2026-04-01T10:00:00Z"}]), encoding="utf-8")
    monkeypatch.setattr(
        "mind.commands.ingest.youtube_enrich.run_youtube_record_lifecycle",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    from mind.commands.ingest import ingest_youtube_export

    result = ingest_youtube_export(export_path)

    assert result.failed == 1
    failure_log = tmp_path / "memory" / "inbox" / f"youtube-failures-{date.today().isoformat()}.md"
    assert failure_log.exists()
    assert "stage=acquire" in failure_log.read_text(encoding="utf-8")


def test_ingest_youtube_export_logs_fanout_error_without_failing_video(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.ingest.vault", lambda: __import__("scripts.common.vault", fromlist=["Vault"]).Vault.load(tmp_path))
    export_path = tmp_path / "raw" / "exports" / "youtube-recent-2026-04-09.json"
    export_path.write_text(json.dumps([{"video_id": "abc123xyz00", "title": "Video", "channel": "Channel", "watched_at": "2026-04-01T10:00:00Z"}]), encoding="utf-8")
    lifecycle = type("Lifecycle", (), {
        "propagate": {"fanout_outcomes": [{"stage": "propagate", "summary": "RuntimeError: boom"}]},
        "materialized": {
            "video": str(tmp_path / "memory" / "sources" / "youtube" / "business" / "video.md"),
        },
    })()
    monkeypatch.setattr("mind.commands.ingest.youtube_enrich.run_youtube_record_lifecycle", lambda *args, **kwargs: lifecycle)

    from mind.commands.ingest import ingest_youtube_export

    result = ingest_youtube_export(export_path)

    assert result.pages_written == 1
    assert result.failed == 0
    failure_log = tmp_path / "memory" / "inbox" / f"youtube-failures-{date.today().isoformat()}.md"
    assert failure_log.exists()
    assert "stage=propagate" in failure_log.read_text(encoding="utf-8")


def test_ingest_youtube_export_ignores_warning_only_outcomes_in_failure_log(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.ingest.vault", lambda: __import__("scripts.common.vault", fromlist=["Vault"]).Vault.load(tmp_path))
    export_path = tmp_path / "raw" / "exports" / "youtube-recent-2026-04-09.json"
    export_path.write_text(json.dumps([{"video_id": "abc123xyz00", "title": "Video", "channel": "Channel", "watched_at": "2026-04-01T10:00:00Z"}]), encoding="utf-8")
    lifecycle = type("Lifecycle", (), {
        "propagate": {
            "pass_d": [{"status": "warning", "stage": "pass_d.parse", "summary": "warning only"}],
            "fanout_outcomes": [{"status": "warning", "stage": "propagate", "summary": "warning only"}],
        },
        "materialized": {
            "video": str(tmp_path / "memory" / "sources" / "youtube" / "business" / "video.md"),
        },
    })()
    monkeypatch.setattr("mind.commands.ingest.youtube_enrich.run_youtube_record_lifecycle", lambda *args, **kwargs: lifecycle)

    from mind.commands.ingest import ingest_youtube_export

    result = ingest_youtube_export(export_path)

    assert result.pages_written == 1
    assert result.failed == 0
    failure_log = tmp_path / "memory" / "inbox" / f"youtube-failures-{date.today().isoformat()}.md"
    assert not failure_log.exists()


def test_ingest_books_export_logs_pass_d_error_without_failing_book(tmp_path: Path, monkeypatch):
    from scripts.common.vault import Vault

    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.ingest.vault", lambda: Vault.load(tmp_path))
    monkeypatch.setattr("mind.commands.ingest.ensure_index_entries", lambda *args, **kwargs: None)
    monkeypatch.setattr("mind.commands.ingest.append_changelog", lambda *args, **kwargs: None)
    export_path = tmp_path / "raw" / "exports" / "books-export.md"
    export_path.write_text("", encoding="utf-8")
    completed = SimpleNamespace(
        title="Designing Data-Intensive Applications",
        materialized_paths={
            "book": str(tmp_path / "memory" / "sources" / "books" / "business" / "x.md"),
            "summary": str(tmp_path / "memory" / "summaries" / "summary-book-x.md"),
        },
        propagate={"pass_d": [{"stage": "pass_d.dispatch", "summary": "RuntimeError: boom"}]},
    )
    monkeypatch.setattr("mind.commands.ingest.build_inventory", lambda *args, **kwargs: SimpleNamespace(items=tuple()))
    monkeypatch.setattr("mind.commands.ingest.refresh_registry_for_inventory", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "mind.commands.ingest.build_plan",
        lambda *args, **kwargs: SimpleNamespace(
            selected_count=1,
            skipped_materialized_count=0,
            resumable_count=1,
            blocked_count=0,
            stale_count=0,
        ),
    )
    monkeypatch.setattr(
        "mind.commands.ingest.execute_books_plan",
        lambda *args, **kwargs: SimpleNamespace(
            page_ids=("x",),
            executed_count=1,
            failed_count=0,
            blocked_samples=(),
            completed_items=(completed,),
        ),
    )

    result = ingest_books_export(export_path)

    assert result.pages_written == 1
    failure_log = tmp_path / "memory" / "inbox" / f"books-failures-{date.today().isoformat()}.md"
    assert failure_log.exists()
    log_text = failure_log.read_text(encoding="utf-8")
    assert "stage=pass_d.dispatch" in log_text
    assert log_text.count("stage=pass_d.dispatch") == 1


def test_ingest_articles_queue_updates_index_and_changelog(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    page = tmp_path / "memory" / "sources" / "articles" / "linked-article.md"
    monkeypatch.setattr(
        "mind.commands.ingest.drain_drop_queue",
        lambda **kwargs: SimpleNamespace(
            drop_files_processed=1,
            urls_in_queue=1,
            skipped_existing=0,
            fetched_summarized=1,
            paywalled=0,
            failed=0,
            new_pages_written=1,
            new_page_paths=[page],
        ),
    )

    result = __import__("mind.commands.ingest", fromlist=["ingest_articles_queue"]).ingest_articles_queue(
        today="2026-04-09",
        repo_root=tmp_path,
    )

    assert result.new_pages_written == 1
    assert "[[linked-article]]" in (tmp_path / "memory" / "INDEX.md").read_text(encoding="utf-8")
    assert "[[linked-article]]" in (tmp_path / "memory" / "CHANGELOG.md").read_text(encoding="utf-8")


def test_ingest_books_export_logs_recoverable_pass_d_parse_warning_without_failing_book(tmp_path: Path, monkeypatch):
    from scripts.common.vault import Vault

    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.ingest.vault", lambda: Vault.load(tmp_path))
    monkeypatch.setattr("mind.commands.ingest.ensure_index_entries", lambda *args, **kwargs: None)
    monkeypatch.setattr("mind.commands.ingest.append_changelog", lambda *args, **kwargs: None)
    export_path = tmp_path / "raw" / "exports" / "books-export.md"
    export_path.write_text("", encoding="utf-8")
    completed = SimpleNamespace(
        title="Designing Data-Intensive Applications",
        materialized_paths={
            "book": str(tmp_path / "memory" / "sources" / "books" / "business" / "x.md"),
            "summary": str(tmp_path / "memory" / "summaries" / "summary-book-x.md"),
        },
        propagate={
            "pass_d": [
                {
                    "stage": "pass_d.parse",
                    "summary": "1 warning(s); dropped 0 q1 match(es) and 1 q2 candidate(s); first=q2_candidates[0]: unsupported type 'note'",
                }
            ]
        },
    )
    monkeypatch.setattr("mind.commands.ingest.build_inventory", lambda *args, **kwargs: SimpleNamespace(items=tuple()))
    monkeypatch.setattr("mind.commands.ingest.refresh_registry_for_inventory", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "mind.commands.ingest.build_plan",
        lambda *args, **kwargs: SimpleNamespace(
            selected_count=1,
            skipped_materialized_count=0,
            resumable_count=1,
            blocked_count=0,
            stale_count=0,
        ),
    )
    monkeypatch.setattr(
        "mind.commands.ingest.execute_books_plan",
        lambda *args, **kwargs: SimpleNamespace(
            page_ids=("x",),
            executed_count=1,
            failed_count=0,
            blocked_samples=(),
            completed_items=(completed,),
        ),
    )

    result = ingest_books_export(export_path)

    assert result.pages_written == 1
    failure_log = tmp_path / "memory" / "inbox" / f"books-failures-{date.today().isoformat()}.md"
    assert failure_log.exists()
    log_text = failure_log.read_text(encoding="utf-8")
    assert "stage=pass_d.parse" in log_text
    assert "unsupported type 'note'" in log_text


def test_ingest_books_export_ignores_audible_clip_cache(tmp_path: Path, monkeypatch):
    from scripts.books.parse import BookRecord
    from scripts.common.vault import Vault

    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.ingest.vault", lambda: Vault.load(tmp_path))
    monkeypatch.setattr("mind.commands.ingest.ensure_index_entries", lambda *args, **kwargs: None)
    monkeypatch.setattr("mind.commands.ingest.append_changelog", lambda *args, **kwargs: None)

    book = BookRecord(
        title="Designing Data-Intensive Applications",
        author=["Martin Kleppmann"],
        status="finished",
        finished_date="2026-03-15",
        format="audiobook",
        asin="B00BOOK123",
    )
    export_path = tmp_path / "raw" / "exports" / "audible-library-2026-04-14.json"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    export_path.write_text("[]", encoding="utf-8")
    clip_cache = tmp_path / "raw" / "audible" / "clips" / "B00BOOK123.json"
    clip_cache.parent.mkdir(parents=True, exist_ok=True)
    clip_cache.write_text('{"annotations":[{"note":"stale clip cache"}]}', encoding="utf-8")

    captured: dict[str, object] = {}
    lifecycle = type("Lifecycle", (), {
        "materialized": {
            "book": str(tmp_path / "memory" / "sources" / "books" / "business" / "x.md"),
        },
        "propagate": {},
    })()

    def fake_lifecycle(book_record, *args, **kwargs):
        captured["clips"] = list(book_record.clips)
        return lifecycle

    monkeypatch.setattr("mind.commands.ingest._iter_books_from_path", lambda path: [book])
    monkeypatch.setattr("mind.commands.ingest.books_enrich.run_book_record_lifecycle", fake_lifecycle)

    result = _ingest_books_export_direct(export_path)

    assert result.pages_written == 1
    assert captured["clips"] == []


def test_ingest_books_export_logs_fanout_error_without_failing_book(tmp_path: Path, monkeypatch):
    from scripts.common.vault import Vault

    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.ingest.vault", lambda: Vault.load(tmp_path))
    monkeypatch.setattr("mind.commands.ingest.ensure_index_entries", lambda *args, **kwargs: None)
    monkeypatch.setattr("mind.commands.ingest.append_changelog", lambda *args, **kwargs: None)
    export_path = tmp_path / "raw" / "exports" / "books-export.md"
    export_path.write_text("", encoding="utf-8")
    completed = SimpleNamespace(
        title="Designing Data-Intensive Applications",
        materialized_paths={
            "book": str(tmp_path / "memory" / "sources" / "books" / "business" / "x.md"),
            "summary": str(tmp_path / "memory" / "summaries" / "summary-book-x.md"),
        },
        propagate={"fanout_outcomes": [{"stage": "propagate", "summary": "RuntimeError: boom"}]},
    )
    monkeypatch.setattr("mind.commands.ingest.build_inventory", lambda *args, **kwargs: SimpleNamespace(items=tuple()))
    monkeypatch.setattr("mind.commands.ingest.refresh_registry_for_inventory", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "mind.commands.ingest.build_plan",
        lambda *args, **kwargs: SimpleNamespace(
            selected_count=1,
            skipped_materialized_count=0,
            resumable_count=1,
            blocked_count=0,
            stale_count=0,
        ),
    )
    monkeypatch.setattr(
        "mind.commands.ingest.execute_books_plan",
        lambda *args, **kwargs: SimpleNamespace(
            page_ids=("x",),
            executed_count=1,
            failed_count=0,
            blocked_samples=(),
            completed_items=(completed,),
        ),
    )

    result = ingest_books_export(export_path)

    assert result.pages_written == 1
    failure_log = tmp_path / "memory" / "inbox" / f"books-failures-{date.today().isoformat()}.md"
    assert failure_log.exists()
    assert "stage=propagate" in failure_log.read_text(encoding="utf-8")


def test_ingest_substack_export_updates_index_and_changelog(tmp_path: Path, monkeypatch):
    from scripts.common.vault import Vault

    _write_config(tmp_path)
    monkeypatch.setattr("mind.commands.ingest.vault", lambda: Vault.load(tmp_path))
    export_path = tmp_path / "raw" / "exports" / "substack-saved-2026-04-09.json"
    export_path.write_text(json.dumps({"posts": []}), encoding="utf-8")
    completed = SimpleNamespace(
        title="On Trust",
        source_id="substack-1",
        materialized_paths={
            "article": str(tmp_path / "memory" / "sources" / "substack" / "example" / "on-trust.md"),
        },
        propagate={},
    )
    monkeypatch.setattr("mind.commands.ingest.substack_auth.build_client", lambda: object())
    monkeypatch.setattr("mind.commands.ingest.build_inventory", lambda *args, **kwargs: SimpleNamespace(items=tuple()))
    monkeypatch.setattr("mind.commands.ingest.refresh_registry_for_inventory", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "mind.commands.ingest.build_plan",
        lambda *args, **kwargs: SimpleNamespace(
            selected_count=1,
            skipped_materialized_count=0,
            resumable_count=0,
            blocked_count=0,
            stale_count=0,
            items=(),
        ),
    )
    monkeypatch.setattr(
        "mind.commands.ingest.execute_substack_plan",
        lambda *args, **kwargs: (
            SimpleNamespace(
                executed_count=1,
                failed_count=0,
                blocked_samples=(),
                failed_items=(),
                completed_items=(completed,),
            ),
            [],
        ),
    )

    result = ingest_substack_export(
        export_path=export_path,
        today="2026-04-09",
        drain_articles=False,
    )

    assert result.posts_written == 1
    assert "[[on-trust]]" in (tmp_path / "memory" / "INDEX.md").read_text(encoding="utf-8")
    assert "[[on-trust]]" in (tmp_path / "memory" / "CHANGELOG.md").read_text(encoding="utf-8")


def test_filtered_queue_processing_ignores_dream_runs(tmp_path: Path):
    from mind.runtime_state import RuntimeState

    _write_config(tmp_path)
    state = RuntimeState.for_repo_root(tmp_path)
    links_run = state.enqueue_run(
        queue_name="links",
        kind="mcp.enqueue_links",
        metadata={"count": 1, "path": str(tmp_path / "raw" / "drops" / "articles-from-mcp-2026-04-08.jsonl"), "links": []},
        last_item_ref="links-item",
    )
    dream_run = state.enqueue_run(
        queue_name="dream:light",
        kind="mcp.start_dream.light",
        metadata={"dry_run": False},
        last_item_ref="dream-item",
    )

    rc, message = process_one_queued_run(
        tmp_path,
        allowed_queue_prefixes=("links", "ingest:file", "ingest:links", "ingest:articles"),
    )

    assert rc == 0
    assert "mcp.enqueue_links" in message
    links_details = state.get_run(links_run)
    dream_details = state.get_run(dream_run)
    assert links_details is not None and links_details.run.status == "completed"
    assert dream_details is not None and dream_details.run.status == "queued"


def test_worker_retries_lock_conflicts_without_losing_queue_item(tmp_path: Path, monkeypatch):
    from mind.runtime_state import RuntimeState

    _write_config(tmp_path)
    state = RuntimeState.for_repo_root(tmp_path)
    run_id = state.enqueue_run(
        queue_name="skills",
        kind="mcp.set_skill_status",
        metadata={"skill_id": "missing-skill", "status": "archived"},
        last_item_ref="missing-skill",
    )
    state.acquire_lock(holder="external-holder")

    rc, message = process_one_queued_run(tmp_path)

    assert rc == 0
    assert "retry scheduled" in message
    details = state.get_run(run_id)
    assert details is not None
    assert details.run.status == "retry_scheduled"
    queue = state.list_queue()
    assert queue[0].pending_count == 1
    assert queue[0].status == "queued"
    state.release_lock(holder="external-holder")


def test_dispatch_run_routes_reingest_metadata(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    captured: dict[str, object] = {}

    def fake_cmd(args):
        captured.update(
            {
                "lane": args.lane,
                "path": args.path,
                "today": args.today,
                "stage": args.stage,
                "through": args.through,
                "limit": args.limit,
                "source_ids": list(args.source_ids),
                "dry_run": args.dry_run,
            }
        )
        return 0

    monkeypatch.setattr("mind.services.queue_worker.cmd_ingest_reingest", fake_cmd)

    rc = dispatch_run(
        tmp_path,
        "mcp.start_reingest",
        {
            "lane": "articles",
            "path": "raw/drops/articles-from-substack-2026-04-09.jsonl",
            "today": "2026-04-09",
            "stage": "pass_d",
            "through": "materialize",
            "limit": 5,
            "source_ids": ["article-2026-04-09-example"],
            "dry_run": True,
        },
    )

    assert rc == 0
    assert captured == {
        "lane": "articles",
        "path": "raw/drops/articles-from-substack-2026-04-09.jsonl",
        "today": "2026-04-09",
        "stage": "pass_d",
        "through": "materialize",
        "limit": 5,
        "source_ids": ["article-2026-04-09-example"],
        "dry_run": True,
    }


def test_dispatch_run_routes_youtube_ingest_options(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    captured: dict[str, object] = {}

    def fake_ingest(path, **kwargs):
        captured["path"] = str(path)
        captured.update(kwargs)
        return SimpleNamespace(failed=0)

    monkeypatch.setattr("mind.services.queue_worker.ingest_youtube_export", fake_ingest)

    rc = dispatch_run(
        tmp_path,
        "mcp.start_ingest.youtube",
        {
            "path": "raw/exports/youtube-recent-2026-04-09.json",
            "options": {
                "default_duration_minutes": 42.0,
                "resume": False,
                "skip_materialized": False,
                "refresh_stale": True,
                "recompute_missing": True,
                "from_stage": "pass_a",
                "through": "materialize",
                "source_ids": ["youtube-abc123xyz00"],
                "external_ids": ["youtube-abc123xyz00"],
                "selection": ["incomplete"],
            },
        },
    )

    assert rc == 0
    assert captured == {
        "path": "raw/exports/youtube-recent-2026-04-09.json",
        "default_duration_minutes": 42.0,
        "resume": False,
        "skip_materialized": False,
        "refresh_stale": True,
        "recompute_missing": True,
        "from_stage": "pass_a",
        "through": "materialize",
        "source_ids": ("youtube-abc123xyz00",),
        "external_ids": ("youtube-abc123xyz00",),
        "selection": ("incomplete",),
    }


def test_dispatch_run_routes_substack_ingest_options(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    captured: dict[str, object] = {}

    def fake_ingest(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(failures=0)

    monkeypatch.setattr("mind.services.queue_worker.ingest_substack_export", fake_ingest)

    rc = dispatch_run(
        tmp_path,
        "mcp.start_ingest.substack",
        {
            "path": "raw/exports/substack-saved-2026-04-09.json",
            "today": "2026-04-09",
            "options": {
                "drain_articles": False,
                "resume": False,
                "skip_materialized": False,
                "refresh_stale": True,
                "recompute_missing": True,
                "from_stage": "pass_b",
                "through": "materialize",
                "source_ids": ["substack-1"],
                "external_ids": ["substack-1"],
                "selection": ["incomplete"],
            },
        },
    )

    assert rc == 0
    assert captured == {
        "export_path": Path("raw/exports/substack-saved-2026-04-09.json"),
        "today": "2026-04-09",
        "drain_articles": False,
        "resume": False,
        "skip_materialized": False,
        "refresh_stale": True,
        "recompute_missing": True,
        "from_stage": "pass_b",
        "through": "materialize",
        "source_ids": ("substack-1",),
        "external_ids": ("substack-1",),
        "selection": ("incomplete",),
    }
