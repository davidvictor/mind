from __future__ import annotations

import json
from types import SimpleNamespace
from pathlib import Path

from mind.commands.ingest import ingest_books_export
from mind.services.llm_cache import LLMCacheIdentity, write_llm_cache
from mind.services.source_models import InventoryItem, InventoryResult, PlanItem, PlanResult, SourceKey, StageProbeState
from mind.services.source_planner import (
    InventoryRequest,
    PlanRequest,
    build_inventory,
    build_plan,
    execute_books_plan,
    execute_youtube_plan,
    rebuild_source_registry,
    reconcile_source_registry,
)
from mind.services.source_registry import SourceRegistry
from scripts.books import enrich as books_enrich
from scripts.books.parse import BookRecord
from scripts.common.vault import raw_path
from tests.support import write_repo_config


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


class _FakeCacheService:
    def cache_identities(self, *, task_class: str, prompt_version: str):
        return [
            LLMCacheIdentity(
                task_class=task_class,
                provider="test",
                model="test/model",
                transport="ai_gateway",
                api_family="responses",
                input_mode="text",
                prompt_version=prompt_version,
                request_fingerprint={"kind": "test-cache"},
            )
        ]


def _patch_cache_service(monkeypatch):
    service = _FakeCacheService()
    monkeypatch.setattr("mind.services.reingest.get_llm_service", lambda: service)
    return service


def _audible_entry(*, asin: str, title: str, authors: str) -> dict[str, object]:
    return {
        "asin": asin,
        "title": title,
        "authors": authors,
        "runtime_length_min": 600,
        "is_finished": True,
        "purchase_date": "2026-04-01T00:00:00Z",
        "rating": "4.7",
    }


def _book_slug(book: BookRecord) -> str:
    author_slug = books_enrich.slugify(book.author[0]) if book.author else "unknown"
    title_slug = books_enrich.slugify(book.title)
    return f"{author_slug}-{title_slug}"


def _write_book_page(root: Path, book: BookRecord, *, category: str = "business") -> None:
    slug = _book_slug(book)
    external_id = f"audible-{book.asin}" if book.asin else ""
    _write(
        root / "memory" / "sources" / "books" / category / f"{slug}.md",
        (
            "---\n"
            f"id: {slug}\n"
            "type: book\n"
            f"title: {book.title}\n"
            f"external_id: {external_id}\n"
            "source_date: 2026-04-01\n"
            "---\n"
        ),
    )


def _write_partial_book_caches(root: Path, service: _FakeCacheService, book: BookRecord) -> None:
    write_llm_cache(
        books_enrich.classification_path(root, book),
        identity=service.cache_identities(task_class="classification", prompt_version=books_enrich.CLASSIFY_BOOK_PROMPT_VERSION)[0],
        data={"category": "business"},
    )
    write_llm_cache(
        books_enrich.deep_research_path(root, book),
        identity=service.cache_identities(task_class="research", prompt_version=books_enrich.RESEARCH_BOOK_DEEP_PROMPT_VERSION)[0],
        data={"core_argument": "Deep research cache"},
    )
    write_llm_cache(
        books_enrich.summary_path(root, book),
        identity=service.cache_identities(task_class="summary", prompt_version=books_enrich.SUMMARIZE_BOOK_RESEARCH_PROMPT_VERSION)[0],
        data={"tldr": "Summary cache"},
    )
    write_llm_cache(
        books_enrich.applied_path(root, book),
        identity=service.cache_identities(task_class="personalization", prompt_version=books_enrich.APPLIED_TO_YOU_PROMPT_VERSION)[0],
        data={"applied_paragraph": "Applied", "applied_bullets": [], "thread_links": []},
    )


def test_rebuild_source_registry_collects_books_from_pages_and_exports(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_exports=True)
    service = _patch_cache_service(monkeypatch)

    materialized = BookRecord(title="Done Book", author=["Done Author"], format="audiobook", status="finished", asin="B00DONE")
    partial = BookRecord(title="Partial Book", author=["Partial Author"], format="audiobook", status="finished", asin="B00PARTIAL")
    unseen = BookRecord(title="Unseen Book", author=["Unseen Author"], format="audiobook", status="finished", asin="B00UNSEEN")

    _write_book_page(tmp_path, materialized)
    _write_partial_book_caches(tmp_path, service, partial)

    export_path = tmp_path / "raw" / "exports" / "audible-library-2026-04-15.json"
    export_path.write_text(
        json.dumps(
            [
                _audible_entry(asin="B00DONE", title="Done Book", authors="Done Author"),
                _audible_entry(asin="B00PARTIAL", title="Partial Book", authors="Partial Author"),
                _audible_entry(asin="B00UNSEEN", title="Unseen Book", authors="Unseen Author"),
            ]
        ),
        encoding="utf-8",
    )

    registry, count = rebuild_source_registry(repo_root=tmp_path)
    status = registry.status()
    details = registry.get("audible-B00DONE")

    assert count == 3
    assert status.source_count == 3
    assert status.lane_counts["books"] == 3
    assert details is not None
    assert details.source.source_key == "book:audible:B00DONE"
    assert {alias.alias for alias in details.aliases} >= {"audible-B00DONE", "book-done-author-done-book", "done-author-done-book"}


def test_rebuild_source_registry_recovers_cache_only_incomplete_book(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_exports=True)
    service = _patch_cache_service(monkeypatch)

    cache_only = BookRecord(title="Cache Only Book", author=["Cache Author"], format="audiobook", status="finished")
    _write_partial_book_caches(tmp_path, service, cache_only)

    registry, count = rebuild_source_registry(repo_root=tmp_path)
    details = registry.get("book-cache-author-cache-only-book")

    assert count == 1
    assert details is not None
    assert details.source.source_key == "book:slug:cache-author-cache-only-book"
    assert details.source.status == "incomplete"


def test_build_inventory_slice_scoped_for_export_and_lane_wide_without_slice(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_exports=True)
    service = _patch_cache_service(monkeypatch)

    materialized = BookRecord(title="Done Book", author=["Done Author"], format="audiobook", status="finished", asin="B00DONE")
    partial = BookRecord(title="Partial Book", author=["Partial Author"], format="audiobook", status="finished")
    page_only = BookRecord(title="Page Only Book", author=["Page Author"], format="audiobook", status="finished", asin="B00PAGE")

    _write_book_page(tmp_path, materialized)
    _write_book_page(tmp_path, page_only)
    _write_partial_book_caches(tmp_path, service, partial)

    export_path = tmp_path / "raw" / "exports" / "audible-library-2026-04-15.json"
    export_path.write_text(
        json.dumps([_audible_entry(asin="B00DONE", title="Done Book", authors="Done Author")]),
        encoding="utf-8",
    )

    scoped = build_inventory(
        InventoryRequest(lane="books", path=export_path),
        repo_root=tmp_path,
        use_registry=False,
    )
    lane_wide = build_inventory(
        InventoryRequest(lane="books"),
        repo_root=tmp_path,
        use_registry=False,
    )

    assert {item.title for item in scoped.items} == {"Done Book"}
    assert {item.title for item in lane_wide.items} == {"Done Book", "Partial Author Partial Book", "Page Only Book"}


def test_build_books_plan_reports_materialized_resumable_and_blocked(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_exports=True)
    service = _patch_cache_service(monkeypatch)

    materialized = BookRecord(title="Done Book", author=["Done Author"], format="audiobook", status="finished", asin="B00DONE")
    partial = BookRecord(title="Partial Book", author=["Partial Author"], format="audiobook", status="finished", asin="B00PARTIAL")
    unseen = BookRecord(title="Unseen Book", author=["Unseen Author"], format="audiobook", status="finished", asin="B00UNSEEN")

    _write_book_page(tmp_path, materialized)
    _write_partial_book_caches(tmp_path, service, partial)

    export_path = tmp_path / "raw" / "exports" / "audible-library-2026-04-15.json"
    export_path.write_text(
        json.dumps(
            [
                _audible_entry(asin="B00DONE", title="Done Book", authors="Done Author"),
                _audible_entry(asin="B00PARTIAL", title="Partial Book", authors="Partial Author"),
                _audible_entry(asin="B00UNSEEN", title="Unseen Book", authors="Unseen Author"),
            ]
        ),
        encoding="utf-8",
    )

    plan = build_plan(
        PlanRequest(
            lane="books",
            path=export_path,
        ),
        repo_root=tmp_path,
        use_registry=False,
    )
    actions = {item.source_key: item.action for item in plan.items}

    assert actions["book:audible:B00DONE"] == "skip_materialized"
    assert actions["book:audible:B00PARTIAL"] == "resume_from_pass_c"
    assert actions["book:audible:B00UNSEEN"] == "blocked_missing_artifacts"


def test_build_plan_treats_unseen_youtube_and_substack_as_acquire_work(tmp_path: Path, monkeypatch) -> None:
    items_by_lane = {
        "youtube": InventoryItem(
            source_key=SourceKey("youtube:video:abc123xyz00"),
            lane="youtube",
            adapter="youtube",
            title="Test Video",
            source_date="2026-04-01",
            status="unseen",
            aliases=("youtube:video:abc123xyz00", "youtube-abc123xyz00"),
            canonical_page_path=None,
            stage_states=(),
            artifacts=(),
            source_id="youtube-abc123xyz00",
            external_id="youtube-abc123xyz00",
        ),
        "substack": InventoryItem(
            source_key=SourceKey("substack:post:1"),
            lane="substack",
            adapter="substack",
            title="Test Post",
            source_date="2026-04-01",
            status="unseen",
            aliases=("substack:post:1", "substack-1"),
            canonical_page_path=None,
            stage_states=(),
            artifacts=(),
            source_id="substack-1",
            external_id="substack-1",
        ),
    }

    def _fake_build_inventory(request, *, repo_root, use_registry=True, phase_callback=None):
        return InventoryResult(request=request, items=(items_by_lane[request.lane],))

    monkeypatch.setattr("mind.services.source_planner.build_inventory", _fake_build_inventory)

    youtube_plan = build_plan(PlanRequest(lane="youtube"), repo_root=tmp_path, use_registry=False)
    substack_plan = build_plan(PlanRequest(lane="substack"), repo_root=tmp_path, use_registry=False)

    assert youtube_plan.items[0].action == "resume_from_acquire"
    assert substack_plan.items[0].action == "resume_from_acquire"


def test_ingest_books_export_executes_only_resumable_items(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_exports=True)
    service = _patch_cache_service(monkeypatch)
    from scripts.common.vault import Vault

    monkeypatch.setattr("mind.commands.ingest.vault", lambda: Vault.load(tmp_path))

    materialized = BookRecord(title="Done Book", author=["Done Author"], format="audiobook", status="finished", asin="B00DONE")
    partial = BookRecord(title="Partial Book", author=["Partial Author"], format="audiobook", status="finished", asin="B00PARTIAL")
    unseen = BookRecord(title="Unseen Book", author=["Unseen Author"], format="audiobook", status="finished", asin="B00UNSEEN")

    _write_book_page(tmp_path, materialized)
    _write_partial_book_caches(tmp_path, service, partial)

    export_path = tmp_path / "raw" / "exports" / "audible-library-2026-04-15.json"
    export_path.write_text(
        json.dumps(
            [
                _audible_entry(asin="B00DONE", title="Done Book", authors="Done Author"),
                _audible_entry(asin="B00PARTIAL", title="Partial Book", authors="Partial Author"),
                _audible_entry(asin="B00UNSEEN", title="Unseen Book", authors="Unseen Author"),
            ]
        ),
        encoding="utf-8",
    )

    seen: list[tuple[str, tuple[str, ...]]] = []

    def fake_run_reingest(request, *, repo_root):
        seen.append((request.stage, request.source_ids))
        return SimpleNamespace(exit_code=0, results=())

    monkeypatch.setattr("mind.services.source_planner.reingest_service.run_reingest", fake_run_reingest)

    result = ingest_books_export(export_path)

    assert result.selected_count == 3
    assert result.skipped_materialized == 1
    assert result.resumable == 1
    assert result.blocked == 1
    assert result.executed == 1
    assert result.failed == 0
    assert seen == [("pass_c", ("book-partial-author-partial-book",))]


def test_execute_youtube_plan_passes_duration_override_into_nested_reingest(tmp_path: Path, monkeypatch) -> None:
    inventory_item = InventoryItem(
        source_key=SourceKey("youtube:video:abc123xyz00"),
        lane="youtube",
        adapter="youtube",
        title="Test Video",
        source_date="2026-04-01",
        status="incomplete",
        aliases=("youtube:video:abc123xyz00", "youtube-abc123xyz00"),
        canonical_page_path=None,
        stage_states=(StageProbeState(stage="pass_a", status="missing", freshness="missing"),),
        artifacts=(),
        source_id="youtube-abc123xyz00",
        external_id="youtube-abc123xyz00",
        payload=SimpleNamespace(
            video_id="abc123xyz00",
            title="Test Video",
            channel="Channel",
            watched_at="2026-04-01T10:00:00Z",
            description="",
            tags=(),
        ),
    )
    export_path = tmp_path / "raw" / "exports" / "youtube-recent-2026-04-15.json"
    plan = PlanResult(
        request=PlanRequest(lane="youtube", path=export_path),
        inventory=InventoryResult(
            request=InventoryRequest(lane="youtube", path=export_path),
            items=(inventory_item,),
        ),
        items=(
            PlanItem(
                source_key=inventory_item.source_key,
                lane="youtube",
                title="Test Video",
                status="incomplete",
                action="resume_from_pass_a",
                start_stage="pass_a",
                through_stage="propagate",
                source_id="youtube-abc123xyz00",
            ),
        ),
    )
    seen: dict[str, object] = {}

    def fake_run_reingest(request, repo_root=None):
        seen["duration"] = request.youtube_default_duration_minutes
        return SimpleNamespace(
            exit_code=0,
            results=(
                SimpleNamespace(
                    status="completed",
                    materialized_paths={"video": str(tmp_path / "memory" / "sources" / "youtube" / "business" / "x.md")},
                    propagate={},
                ),
            ),
        )

    monkeypatch.setattr("mind.services.source_planner.reingest_service.run_reingest", fake_run_reingest)
    monkeypatch.setattr(
        "mind.services.source_planner.build_inventory",
        lambda *args, **kwargs: InventoryResult(
            request=InventoryRequest(lane="youtube", path=export_path),
            items=(inventory_item,),
        ),
    )
    monkeypatch.setattr("mind.services.source_planner.refresh_registry_for_inventory", lambda *args, **kwargs: None)

    result = execute_youtube_plan(plan, repo_root=tmp_path, default_duration_minutes=42.0)

    assert seen["duration"] == 42.0
    assert result.executed_count == 1
    assert result.failed_count == 0


def test_build_inventory_excludes_non_article_youtube_description_links(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_exports=True)
    _patch_cache_service(monkeypatch)

    drop = tmp_path / "raw" / "drops" / "articles-from-youtube-description-2026-04-14.jsonl"
    drop.parent.mkdir(parents=True, exist_ok=True)
    drop.write_text(
        json.dumps(
            {
                "url": "https://www.youtube.com/@science.revolution",
                "source_post_id": "youtube-source",
                "source_post_url": "https://www.youtube.com/watch?v=abc123xyz00",
                "anchor_text": "Science Revolution",
                "context_snippet": "channel link",
                "category": "business",
                "discovered_at": "2026-04-14",
                "source_type": "youtube-description",
                "source_label": "youtube-description",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    inventory = build_inventory(
        InventoryRequest(lane="articles"),
        repo_root=tmp_path,
        use_registry=False,
    )

    assert len(inventory.items) == 1
    assert inventory.items[0].status == "excluded"
    assert inventory.items[0].excluded_reason == "excluded non-article URL from YouTube description fanout"


def test_build_inventory_marks_youtube_page_incomplete_when_receipt_records_propagate_error(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_exports=True)
    _patch_cache_service(monkeypatch)

    page = tmp_path / "memory" / "sources" / "youtube" / "business" / "video.md"
    _write(
        page,
        (
            "---\n"
            "id: video\n"
            "type: video\n"
            "title: Test Video\n"
            "external_id: youtube-abc123xyz00\n"
            "youtube_id: abc123xyz00\n"
            "source_date: 2026-04-14\n"
            "---\n"
        ),
    )
    receipt_path = raw_path(tmp_path, "transcripts", "youtube", "youtube-abc123xyz00.quality.json")
    _write(
        receipt_path,
        json.dumps(
            {
                "source_id": "youtube-abc123xyz00",
                "source_kind": "youtube",
                "source_date": "2026-04-14",
                "executed_at": "2026-04-14",
                "pass_d_status": "ok",
                "propagate_status": "error",
                "propagate_detail": "NameError: write_pages is not defined",
            }
        ),
    )

    inventory = build_inventory(
        InventoryRequest(lane="youtube"),
        repo_root=tmp_path,
        use_registry=False,
    )

    assert len(inventory.items) == 1
    assert inventory.items[0].status == "incomplete"
    assert inventory.items[0].blocked_reason == "NameError: write_pages is not defined"


def test_execute_books_plan_runs_acquire_stage_books_lifecycle(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_exports=True)

    book = BookRecord(title="Fresh Book", author=["Fresh Author"], format="audiobook", status="finished", asin="B00FRESH")
    inventory_item = InventoryItem(
        source_key=SourceKey("book:audible:B00FRESH"),
        lane="books",
        adapter="books",
        title="Fresh Book",
        source_date="2026-04-16",
        status="unseen",
        aliases=("book:audible:B00FRESH", "book-fresh-author-fresh-book", "audible-B00FRESH", "B00FRESH"),
        canonical_page_path=None,
        stage_states=(StageProbeState(stage="acquire", status="missing", freshness="missing"),),
        artifacts=(),
        source_id="book-fresh-author-fresh-book",
        external_id="audible-B00FRESH",
        payload=book,
    )
    export_path = tmp_path / "raw" / "exports" / "audible-library-2026-04-16.json"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    export_path.write_text("[]", encoding="utf-8")
    plan = PlanResult(
        request=PlanRequest(lane="books", path=export_path, recompute_missing=True),
        inventory=InventoryResult(
            request=InventoryRequest(lane="books", path=export_path),
            items=(inventory_item,),
        ),
        items=(
            PlanItem(
                source_key=SourceKey("book:audible:B00FRESH"),
                lane="books",
                title="Fresh Book",
                status="unseen",
                action="resume_from_acquire",
                start_stage="acquire",
                through_stage="propagate",
                source_id="book-fresh-author-fresh-book",
                external_id="audible-B00FRESH",
            ),
        ),
    )

    lifecycle = SimpleNamespace(
        materialized={"book": str(tmp_path / "memory" / "sources" / "books" / "business" / "fresh-author-fresh-book.md")},
        propagate={},
    )
    seen: dict[str, object] = {}

    def fake_lifecycle(book_record, *, repo_root, today, force_deep):
        seen["title"] = book_record.title
        seen["repo_root"] = repo_root
        seen["today"] = today
        seen["force_deep"] = force_deep
        return lifecycle

    monkeypatch.setattr("mind.services.source_planner.books_enrich.run_book_record_lifecycle", fake_lifecycle)
    monkeypatch.setattr(
        "mind.services.source_planner.build_inventory",
        lambda *args, **kwargs: InventoryResult(
            request=InventoryRequest(lane="books", path=export_path),
            items=(inventory_item,),
        ),
    )
    monkeypatch.setattr("mind.services.source_planner.refresh_registry_for_inventory", lambda *args, **kwargs: None)

    result = execute_books_plan(plan, repo_root=tmp_path)

    assert result.executed_count == 1
    assert result.failed_count == 0
    assert result.page_ids == ("fresh-author-fresh-book",)
    assert seen["title"] == "Fresh Book"


def test_execute_books_plan_passes_force_deep_into_nested_reingest(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_exports=True)

    book = BookRecord(title="Resume Book", author=["Resume Author"], format="audiobook", status="finished", asin="B00RESUME")
    inventory_item = InventoryItem(
        source_key=SourceKey("book:audible:B00RESUME"),
        lane="books",
        adapter="books",
        title="Resume Book",
        source_date="2026-04-16",
        status="incomplete",
        aliases=("book:audible:B00RESUME", "book-resume-author-resume-book", "audible-B00RESUME", "B00RESUME"),
        canonical_page_path=None,
        stage_states=(StageProbeState(stage="pass_a", status="stale", freshness="stale"),),
        artifacts=(),
        source_id="resume-author-resume-book",
        external_id="audible-B00RESUME",
        payload=book,
    )
    export_path = tmp_path / "raw" / "exports" / "audible-library-2026-04-16.json"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    export_path.write_text("[]", encoding="utf-8")
    plan = PlanResult(
        request=PlanRequest(lane="books", path=export_path, recompute_missing=True),
        inventory=InventoryResult(
            request=InventoryRequest(lane="books", path=export_path),
            items=(inventory_item,),
        ),
        items=(
            PlanItem(
                source_key=SourceKey("book:audible:B00RESUME"),
                lane="books",
                title="Resume Book",
                status="incomplete",
                action="resume_from_pass_a",
                start_stage="pass_a",
                through_stage="propagate",
                source_id="resume-author-resume-book",
                external_id="audible-B00RESUME",
            ),
        ),
    )

    seen: dict[str, object] = {}

    def fake_run_reingest(request, repo_root=None):
        seen["force_deep"] = request.force_deep
        seen["repo_root"] = repo_root
        return SimpleNamespace(
            exit_code=0,
            results=(
                SimpleNamespace(
                    status="completed",
                    materialized_paths={"book": str(tmp_path / "memory" / "sources" / "books" / "personal" / "resume-author-resume-book.md")},
                    propagate={},
                ),
            ),
        )

    monkeypatch.setattr("mind.services.source_planner.reingest_service.run_reingest", fake_run_reingest)
    monkeypatch.setattr(
        "mind.services.source_planner.build_inventory",
        lambda *args, **kwargs: InventoryResult(
            request=InventoryRequest(lane="books", path=export_path),
            items=(inventory_item,),
        ),
    )
    monkeypatch.setattr("mind.services.source_planner.refresh_registry_for_inventory", lambda *args, **kwargs: None)

    result = execute_books_plan(plan, repo_root=tmp_path, force_deep=True)

    assert result.executed_count == 1
    assert result.failed_count == 0
    assert result.page_ids == ("resume-author-resume-book",)
    assert seen["force_deep"] is True
    assert seen["repo_root"] == tmp_path


def test_reconcile_books_reports_registry_page_and_cache_only_drift(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_exports=True)
    service = _patch_cache_service(monkeypatch)

    materialized = BookRecord(title="Done Book", author=["Done Author"], format="audiobook", status="finished", asin="B00DONE")
    cache_only = BookRecord(title="Cache Only Book", author=["Cache Author"], format="audiobook", status="finished")
    page_only = BookRecord(title="Page Only Book", author=["Page Author"], format="audiobook", status="finished", asin="B00PAGE")

    _write_book_page(tmp_path, materialized)
    _write_partial_book_caches(tmp_path, service, cache_only)

    rebuild_source_registry(repo_root=tmp_path)
    _write_book_page(tmp_path, page_only)

    export_path = tmp_path / "raw" / "exports" / "audible-library-2026-04-15.json"
    export_path.write_text(
        json.dumps([_audible_entry(asin="B00DONE", title="Done Book", authors="Done Author")]),
        encoding="utf-8",
    )

    result = reconcile_source_registry(
        InventoryRequest(lane="books", path=export_path),
        repo_root=tmp_path,
    )

    assert result.upstream_selected_count == 1
    assert result.registry_matched_count == 1
    assert result.page_matched_count == 1
    assert result.registry_only_count >= 1
    assert result.page_only_count == 1
    assert result.cache_only_count >= 1
    assert "Cache Author Cache Only Book" in result.registry_only_samples
    assert "Page Only Book" in result.page_only_samples
