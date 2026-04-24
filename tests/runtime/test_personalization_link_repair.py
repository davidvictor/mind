from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path

from mind.services import reingest as reingest_service
from scripts.books.parse import BookRecord
from scripts.youtube.parse import YouTubeRecord
from tests.support import write_repo_config


def test_probe_book_pass_c_uses_empty_fallback_without_stance_doc(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    book = BookRecord(
        title="Crossing the Chasm",
        author=["Geoffrey A. Moore"],
        status="finished",
        finished_date="2026-04-15",
        format="ebook",
    )

    probe = reingest_service._probe_book_pass_c(tmp_path, book)

    assert probe.reusable is True
    assert probe.stale is False


def test_execute_personalization_link_repair_item_runs_pass_b_then_materialize(
    monkeypatch,
    tmp_path: Path,
) -> None:
    item = reingest_service._LaneItem(
        lane="youtube",
        source_id="youtube-abc123xyz00",
        label="Test Video",
        payload=YouTubeRecord(
            video_id="abc123xyz00",
            title="Test Video",
            channel="Test Channel",
            watched_at="2026-04-01T10:00:00Z",
        ),
        source_label="youtube-export.json",
    )
    seen: list[tuple[str, str, tuple[str, ...]]] = []

    def fake_execute(request, repo_root, received_item):
        assert repo_root == tmp_path
        assert received_item == item
        seen.append((request.stage, request.through, request.source_ids))
        return SimpleNamespace(materialized={"video": "memory/sources/youtube/business/test-video.md"}, propagate={})

    monkeypatch.setattr(reingest_service, "_execute_youtube_item", fake_execute)

    result = reingest_service._execute_personalization_link_repair_item(
        lane="youtube",
        repo_root=tmp_path,
        item=item,
        today="2026-04-18",
    )

    assert seen == [
        ("pass_b", "pass_b", ("youtube-abc123xyz00",)),
        ("materialize", "materialize", ("youtube-abc123xyz00",)),
    ]
    assert result.materialized == {"video": "memory/sources/youtube/business/test-video.md"}


def test_run_personalization_link_repair_reports_ready_and_blocked_items(
    monkeypatch,
    tmp_path: Path,
) -> None:
    ready_item = reingest_service._LaneItem(
        lane="books",
        source_id="book-ready",
        label="Ready Book",
        payload=BookRecord(
            title="Ready Book",
            author=["Author One"],
            status="finished",
            finished_date="2026-04-15",
            format="ebook",
        ),
        source_label="books-export.json",
    )
    blocked_item = reingest_service._LaneItem(
        lane="books",
        source_id="book-blocked",
        label="Blocked Book",
        payload=BookRecord(
            title="Blocked Book",
            author=["Author Two"],
            status="finished",
            finished_date="2026-04-15",
            format="ebook",
        ),
        source_label="books-export.json",
    )

    monkeypatch.setattr(reingest_service, "_inventory_items", lambda request, repo_root: [ready_item, blocked_item])

    plans = {
        "book-ready": reingest_service.PersonalizationLinkRepairItem(
            source_id="book-ready",
            label="Ready Book",
            reusable_stages=("acquire", "pass_a", "pass_c"),
            blocked_reasons=(),
            projected_rewrites=3,
        ),
        "book-blocked": reingest_service.PersonalizationLinkRepairItem(
            source_id="book-blocked",
            label="Blocked Book",
            reusable_stages=("acquire",),
            blocked_reasons=("missing or stale pass_a cache",),
            projected_rewrites=0,
        ),
    }
    monkeypatch.setattr(
        reingest_service,
        "_build_personalization_link_repair_item",
        lambda repo_root, lane, item: plans[item.source_id],
    )

    report = reingest_service.run_personalization_link_repair(
        repo_root=tmp_path,
        lane="books",
        apply=False,
    )

    assert report.lane == "books"
    assert report.selected_count == 2
    assert report.ready_count == 1
    assert report.blocked_count == 1
    assert report.projected_rewrites == 3
    rendered = report.render()
    assert "repair personalization-links[books]" in rendered
    assert "book-ready" in rendered
    assert "book-blocked" in rendered
