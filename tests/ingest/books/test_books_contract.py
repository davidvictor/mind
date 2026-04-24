from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from scripts.books import enrich, write_pages
from scripts.common.vault import Vault
from scripts.books.parse import BookRecord
from tests.support import fake_env_config, write_repo_config


def _book(**overrides) -> BookRecord:
    data = {
        "title": "Designing Data-Intensive Applications",
        "author": ["Martin Kleppmann"],
        "status": "finished",
        "finished_date": "2026-03-15",
        "format": "ebook",
    }
    data.update(overrides)
    return BookRecord(**data)


def test_normalize_book_source_uses_person_as_primary_creator() -> None:
    source = enrich.normalize_book_source(
        _book(),
        classification={"category": "business"},
        research={"tldr": "x", "topics": []},
    )
    assert source.source_kind == "book"
    assert source.creator_candidates[0]["page_type"] == "person"
    assert source.creator_candidates[0]["role"] == "creator"


def test_normalize_book_source_emits_publisher_target_when_known() -> None:
    source = enrich.normalize_book_source(
        _book(publisher="Addison-Wesley"),
        classification={"category": "business"},
        research={"tldr": "x", "topics": []},
    )
    roles = {(candidate["role"], candidate["page_type"]) for candidate in source.creator_candidates}
    assert ("publisher", "company") in roles


def test_normalize_book_source_uses_transcript_field_for_audio_grounding() -> None:
    source = enrich.normalize_book_source(
        _book(format="audiobook"),
        classification={"category": "business"},
        research={"tldr": "x", "topics": []},
        source_kind="audio",
        source_text="chapter one transcript",
        source_asset_path="/tmp/audio.m4a",
    )

    assert source.primary_content == "chapter one transcript"
    assert source.provenance["source_kind"] == "audio"
    assert source.provenance["source_asset_path"] == "/tmp/audio.m4a"


def test_run_book_record_lifecycle_uses_source_grounded_payload_when_assets_exist(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(enrich, "classify", lambda book: {"category": "business"})
    monkeypatch.setattr(
        enrich,
        "enrich_from_source",
        lambda book: {
            "source_kind": "document",
            "source_asset_path": "/tmp/book.pdf",
            "source_text": "real source text",
            "segment_count": 1,
            "segmentation_strategy": "fixed-window",
            "summary": {"tldr": "x", "topics": ["systems"]},
        },
    )

    def fake_run_ingestion_lifecycle(**kwargs):
        captured.update(kwargs)

        class Result:
            envelope = {
                "schema_version": 1,
                "source_id": "book-martin-kleppmann-designing-data-intensive-applications",
                "pass_a": {},
                "pass_b": {},
                "pass_c": {},
                "pass_d": {},
                "verification": {},
                "materialization_hints": {},
            }
            materialized = {"book": "a", "summary": "b"}
            propagate = {}

        return Result()

    monkeypatch.setattr(enrich, "run_ingestion_lifecycle", fake_run_ingestion_lifecycle)

    out = enrich.run_book_record_lifecycle(_book(), repo_root=tmp_path, today="2026-04-09")

    assert out is not None
    source = captured["source"]
    assert source.primary_content == "real source text"  # type: ignore[attr-defined]
    assert source.provenance["source_kind"] == "document"  # type: ignore[index]


def test_write_book_summary_page_records_source_grounding_metadata(tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    source = enrich.normalize_book_source(
        _book(),
        classification={"category": "business"},
        research={"tldr": "x", "topics": []},
        source_kind="document",
        source_text="body",
        source_asset_path="/tmp/book.pdf",
    )
    targets = enrich._materialization_targets_from_source(source)
    with patch("scripts.common.env.load", return_value=fake_env_config(tmp_path)):
        path = write_pages.write_summary_page(
            _book(),
            {"tldr": "x", "key_ideas": [], "frameworks_introduced": [], "in_conversation_with": [], "topics": []},
            category="business",
            creator_target=targets.creator_target,
            source_kind="document",
            source_asset_path="/tmp/book.pdf",
            force=True,
        )
    text = path.read_text(encoding="utf-8")
    assert "Source-grounded from local document" in text
    assert "source_kind: document" in text


def test_enrich_from_source_segments_documents_before_summary(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "book.pdf"
    pdf_path.write_text("fake", encoding="utf-8")

    class _Identity:
        def to_dict(self):
            return {"provider": "test"}

    seen_segments: list[str] = []
    monkeypatch.setattr(enrich, "extract_document_text", lambda path: "A" * 13000 + "B" * 13000)
    monkeypatch.setattr(
        "scripts.books.enrich.get_llm_service",
        lambda: type(
            "FakeService",
            (),
            {
                "cache_identities": staticmethod(lambda **kwargs: [_Identity()]),
                "summarize_book_source_text": staticmethod(
                    lambda **kwargs: seen_segments.append(kwargs["segment_label"]) or {
                        "tldr": kwargs["segment_label"],
                        "key_ideas": [],
                        "frameworks_introduced": [],
                        "in_conversation_with": [],
                        "notable_quotes": [],
                        "topics": [],
                    }
                ),
            },
        )(),
    )
    monkeypatch.setattr("scripts.common.env.load", lambda: fake_env_config(tmp_path))

    result = enrich.enrich_from_source(_book(document_path=str(pdf_path)))

    assert result is not None
    assert result["segment_count"] == 3
    assert seen_segments == ["segment-1", "segment-2", "segment-3"]


def test_enrich_from_source_segments_audio_before_summary(tmp_path: Path, monkeypatch) -> None:
    audio_path = tmp_path / "book.m4a"
    audio_path.write_text("fake", encoding="utf-8")

    class _Identity:
        def to_dict(self):
            return {"provider": "test"}

    calls: list[str] = []
    monkeypatch.setattr(
        enrich,
        "_segment_audio_asset",
        lambda path, clip_windows: (
            [
                {"label": "clip-1", "bytes": b"one", "mime_type": "audio/wav", "file_name": "clip-1.wav"},
                {"label": "clip-2", "bytes": b"two", "mime_type": "audio/wav", "file_name": "clip-2.wav"},
            ],
            "audible-clips",
        ),
    )
    monkeypatch.setattr(
        "scripts.books.enrich.get_llm_service",
        lambda: type(
            "FakeService",
            (),
            {
                "cache_identities": staticmethod(lambda **kwargs: [_Identity()]),
                "summarize_book_source": staticmethod(
                    lambda **kwargs: calls.append(kwargs["input_parts"][0].metadata["segment_label"]) or {
                        "transcript": kwargs["input_parts"][0].metadata["segment_label"],
                        "tldr": kwargs["input_parts"][0].metadata["segment_label"],
                        "key_ideas": [],
                        "frameworks_introduced": [],
                        "in_conversation_with": [],
                        "notable_quotes": [],
                        "topics": [],
                    }
                ),
            },
        )(),
    )
    monkeypatch.setattr("scripts.common.env.load", lambda: fake_env_config(tmp_path))

    result = enrich.enrich_from_source(_book(format="audiobook", audio_path=str(audio_path)))

    assert result is not None
    assert result["segment_count"] == 2
    assert result["segmentation_strategy"] == "audible-clips"
    assert calls == ["clip-1", "clip-2"]


def test_run_book_record_lifecycle_allows_in_progress_books(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(enrich, "classify", lambda book: {"category": "business"})
    monkeypatch.setattr(enrich, "enrich_deep", lambda book: {"tldr": "x", "topics": []})
    monkeypatch.setattr(enrich, "summarize_research", lambda book, research: {"tldr": "x", "topics": []})

    def fake_run_ingestion_lifecycle(**kwargs):
        captured.update(kwargs)

        class Result:
            envelope = {
                "schema_version": 1,
                "source_id": "book-martin-kleppmann-designing-data-intensive-applications",
                "pass_a": {},
                "pass_b": {},
                "pass_c": {},
                "pass_d": {},
                "verification": {},
                "materialization_hints": {},
            }
            materialized = {"book": "a", "summary": "b"}
            propagate = {}

        return Result()

    monkeypatch.setattr(enrich, "run_ingestion_lifecycle", fake_run_ingestion_lifecycle)

    out = enrich.run_book_record_lifecycle(
        _book(status="in-progress", finished_date=""),
        repo_root=tmp_path,
        today="2026-04-09",
    )

    assert out is not None
    assert captured["source"].source_kind == "book"  # type: ignore[attr-defined]


def test_run_book_record_lifecycle_skips_to_read_books(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(enrich, "classify", lambda book: {"category": "business"})

    out = enrich.run_book_record_lifecycle(
        _book(status="to-read", finished_date=""),
        repo_root=tmp_path,
        today="2026-04-09",
    )

    assert out is None


def test_run_book_record_lifecycle_force_deep_overrides_ignore_classification(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(enrich, "classify", lambda book: {"category": "ignore"})
    monkeypatch.setattr(enrich, "enrich_deep", lambda book: {"tldr": "x", "topics": []})
    monkeypatch.setattr(enrich, "summarize_research", lambda book, research: {"tldr": "x", "topics": []})

    def fake_run_ingestion_lifecycle(**kwargs):
        captured.update(kwargs)

        class Result:
            envelope = {
                "schema_version": 1,
                "source_id": "book-martin-kleppmann-designing-data-intensive-applications",
                "pass_a": {},
                "pass_b": {},
                "pass_c": {},
                "pass_d": {},
                "verification": {},
                "materialization_hints": {},
            }
            materialized = {"book": "a", "summary": "b"}
            propagate = {}

        return Result()

    monkeypatch.setattr(enrich, "run_ingestion_lifecycle", fake_run_ingestion_lifecycle)

    out = enrich.run_book_record_lifecycle(
        _book(),
        repo_root=tmp_path,
        today="2026-04-09",
        force_deep=True,
    )

    assert out is not None
    understand = captured["understand"]
    source = captured["source"]
    pass_a = understand(source, {})
    assert pass_a["classification"]["retention"] == "keep"
    assert pass_a["classification"]["synthesis_mode"] == "deep"


def test_run_book_record_lifecycle_audiobooks_default_to_deep_synthesis(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        enrich,
        "classify",
        lambda book: {"retention": "keep", "domains": ["personal"], "synthesis_mode": "light", "category": "personal"},
    )
    monkeypatch.setattr(enrich, "enrich_deep", lambda book: {"tldr": "x", "topics": []})
    monkeypatch.setattr(enrich, "summarize_research", lambda book, research: {"tldr": "x", "topics": []})

    def fake_run_ingestion_lifecycle(**kwargs):
        captured.update(kwargs)

        class Result:
            envelope = {
                "schema_version": 1,
                "source_id": "book-martin-kleppmann-designing-data-intensive-applications",
                "pass_a": {},
                "pass_b": {},
                "pass_c": {},
                "pass_d": {},
                "verification": {},
                "materialization_hints": {},
            }
            materialized = {"book": "a", "summary": "b"}
            propagate = {}

        return Result()

    monkeypatch.setattr(enrich, "run_ingestion_lifecycle", fake_run_ingestion_lifecycle)

    out = enrich.run_book_record_lifecycle(
        _book(format="audiobook", asin="B00AUDIO123"),
        repo_root=tmp_path,
        today="2026-04-09",
    )

    assert out is not None
    understand = captured["understand"]
    source = captured["source"]
    pass_a = understand(source, {})
    assert pass_a["classification"]["retention"] == "keep"
    assert pass_a["classification"]["synthesis_mode"] == "deep"


def test_write_book_pages_use_author_target_when_present(tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    source = enrich.normalize_book_source(
        _book(),
        classification={"category": "business"},
        research={"tldr": "x", "topics": []},
    )
    targets = enrich._materialization_targets_from_source(source)
    enriched = {
        "tldr": "x",
        "key_ideas": [],
        "frameworks_introduced": [],
        "in_conversation_with": [],
        "notable_quotes": [],
        "takeaways": [],
        "topics": [],
    }
    with patch("scripts.common.env.load", return_value=fake_env_config(tmp_path)):
        path = write_pages.write_book_page(
            _book(),
            enriched,
            category="business",
            creator_target=targets.creator_target,
            force=True,
        )
    assert path.is_relative_to(Vault.load(tmp_path).wiki)
    text = path.read_text(encoding="utf-8")
    assert 'author:' in text
    assert '[[martin-kleppmann]]' in text


def test_write_book_page_preserves_coauthors_on_force_rewrite(tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    book = _book(author=["Alice Author", "Bob Writer"])
    source = enrich.normalize_book_source(
        book,
        classification={"category": "business"},
        research={"tldr": "x", "topics": []},
    )
    targets = enrich._materialization_targets_from_source(source)
    enriched = {
        "tldr": "x",
        "key_ideas": [],
        "frameworks_introduced": [],
        "in_conversation_with": [],
        "notable_quotes": [],
        "takeaways": [],
        "topics": [],
    }
    with patch("scripts.common.env.load", return_value=fake_env_config(tmp_path)):
        path = write_pages.write_book_page(
            book,
            enriched,
            category="business",
            creator_target=targets.creator_target,
            force=True,
        )
    assert path.is_relative_to(Vault.load(tmp_path).wiki)
    text = path.read_text(encoding="utf-8")
    assert '[[alice-author]]' in text
    assert 'Bob Writer' in text
