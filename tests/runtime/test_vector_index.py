from __future__ import annotations

from pathlib import Path

from mind.services.vector_index import FileVectorIndexBackend, SQLiteVectorIndexBackend, select_vector_backend


def test_file_vector_index_upsert_query_and_prune(tmp_path: Path):
    backend = FileVectorIndexBackend(tmp_path)
    backend.upsert(
        model="openai/text-embedding-3-small",
        vectors={
            "a": [1.0, 0.0],
            "b": [0.0, 1.0],
        },
    )

    matches = backend.query(
        model="openai/text-embedding-3-small",
        query_vector=[0.9, 0.1],
        limit=2,
    )
    assert matches[0].target_id == "a"
    assert backend.status(model="openai/text-embedding-3-small")["count"] == 2

    backend.prune(model="openai/text-embedding-3-small", valid_ids={"a"})
    assert backend.status(model="openai/text-embedding-3-small")["count"] == 1


def test_sqlite_vector_index_upsert_query_and_prune(tmp_path: Path):
    backend = SQLiteVectorIndexBackend(tmp_path / "vectors.sqlite3")
    backend.upsert(
        model="openai/text-embedding-3-small",
        vectors={
            "a": [1.0, 0.0],
            "b": [0.0, 1.0],
        },
    )

    matches = backend.query(
        model="openai/text-embedding-3-small",
        query_vector=[0.9, 0.1],
        limit=2,
    )
    assert matches[0].target_id == "a"
    assert backend.status(model="openai/text-embedding-3-small")["backend"] == "sqlite"

    backend.prune(model="openai/text-embedding-3-small", valid_ids={"a"})
    assert backend.status(model="openai/text-embedding-3-small")["count"] == 1


def test_select_vector_backend_prefers_sqlite(tmp_path: Path):
    backend = select_vector_backend(tmp_path)
    assert backend.__class__.__name__ == "SQLiteVectorIndexBackend"
