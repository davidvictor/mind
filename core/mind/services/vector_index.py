from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path
import sqlite3
from typing import Protocol


@dataclass(frozen=True)
class VectorQueryMatch:
    target_id: str
    score: float


class VectorIndexBackend(Protocol):
    def upsert(self, *, model: str, vectors: dict[str, list[float]]) -> None: ...

    def prune(self, *, model: str, valid_ids: set[str]) -> None: ...

    def query(self, *, model: str, query_vector: list[float], limit: int) -> list[VectorQueryMatch]: ...

    def status(self, *, model: str) -> dict[str, object]: ...


class SQLiteVectorIndexBackend:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._bootstrap()

    @staticmethod
    def is_available() -> bool:
        try:
            sqlite3.connect(":memory:").close()
            return True
        except Exception:
            return False

    def _bootstrap(self) -> None:
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS vectors (
                    model TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    vector_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(model, target_id)
                )
                """
            )

    def upsert(self, *, model: str, vectors: dict[str, list[float]]) -> None:
        with sqlite3.connect(self.path) as conn:
            for target_id, vector in vectors.items():
                conn.execute(
                    """
                    INSERT INTO vectors(model, target_id, vector_json)
                    VALUES (?, ?, ?)
                    ON CONFLICT(model, target_id) DO UPDATE SET
                        vector_json = excluded.vector_json,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (model, target_id, json.dumps(vector)),
                )

    def prune(self, *, model: str, valid_ids: set[str]) -> None:
        with sqlite3.connect(self.path) as conn:
            rows = conn.execute(
                "SELECT target_id FROM vectors WHERE model = ?",
                (model,),
            ).fetchall()
            stale = [str(row[0]) for row in rows if str(row[0]) not in valid_ids]
            for target_id in stale:
                conn.execute(
                    "DELETE FROM vectors WHERE model = ? AND target_id = ?",
                    (model, target_id),
                )

    def query(self, *, model: str, query_vector: list[float], limit: int) -> list[VectorQueryMatch]:
        with sqlite3.connect(self.path) as conn:
            rows = conn.execute(
                "SELECT target_id, vector_json FROM vectors WHERE model = ?",
                (model,),
            ).fetchall()
        matches = [
            VectorQueryMatch(target_id=str(target_id), score=_cosine_similarity(query_vector, list(json.loads(vector_json))))
            for target_id, vector_json in rows
        ]
        return sorted(matches, key=lambda item: (-item.score, item.target_id))[:limit]

    def status(self, *, model: str) -> dict[str, object]:
        with sqlite3.connect(self.path) as conn:
            rows = conn.execute(
                "SELECT vector_json FROM vectors WHERE model = ?",
                (model,),
            ).fetchall()
        dim = len(json.loads(rows[0][0])) if rows else 0
        return {
            "backend": "sqlite",
            "count": len(rows),
            "dimension": dim,
            "path": str(self.path),
        }


class FileVectorIndexBackend:
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, *, model: str) -> Path:
        safe = model.replace("/", "__")
        return self.root / f"{safe}.json"

    def _load(self, *, model: str) -> dict[str, list[float]]:
        path = self._path(model=model)
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _write(self, *, model: str, payload: dict[str, list[float]]) -> None:
        path = self._path(model=model)
        path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")

    def upsert(self, *, model: str, vectors: dict[str, list[float]]) -> None:
        payload = self._load(model=model)
        payload.update(vectors)
        self._write(model=model, payload=payload)

    def prune(self, *, model: str, valid_ids: set[str]) -> None:
        payload = self._load(model=model)
        pruned = {key: value for key, value in payload.items() if key in valid_ids}
        self._write(model=model, payload=pruned)

    def query(self, *, model: str, query_vector: list[float], limit: int) -> list[VectorQueryMatch]:
        payload = self._load(model=model)
        matches = [
            VectorQueryMatch(target_id=target_id, score=_cosine_similarity(query_vector, vector))
            for target_id, vector in payload.items()
        ]
        return sorted(matches, key=lambda item: (-item.score, item.target_id))[:limit]

    def status(self, *, model: str) -> dict[str, object]:
        payload = self._load(model=model)
        dim = len(next(iter(payload.values()))) if payload else 0
        return {
            "backend": "file",
            "count": len(payload),
            "dimension": dim,
            "path": str(self._path(model=model)),
        }


def select_vector_backend(path: Path) -> VectorIndexBackend:
    if path.suffix in {".sqlite3", ".db"}:
        return SQLiteVectorIndexBackend(path)
    if SQLiteVectorIndexBackend.is_available():
        return SQLiteVectorIndexBackend(path / "graph-vectors.sqlite3")
    return FileVectorIndexBackend(path)


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    mag_a = math.sqrt(sum(x * x for x in a))
    mag_b = math.sqrt(sum(y * y for y in b))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)
