from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import re
from typing import Any, Callable, Iterator

import yaml

from scripts.atoms.canonical import RELATION_KINDS
from scripts.common.slugify import normalize_identifier
from scripts.common.vault import SYSTEM_SKIP_NAMES, Vault
from .embedding_service import EmbeddingService
from .vector_index import VectorIndexBackend, VectorQueryMatch


GRAPH_DB_NAME = ".brain-graph.sqlite3"
CANONICAL_NODE_DIRS = (
    "projects",
    "companies",
    "people",
    "concepts",
    "playbooks",
    "stances",
    "inquiries",
)
SYSTEM_SKIP_DIRS = {"templates", ".archive"}
TOKEN_RE = re.compile(r"[a-z0-9]+")
FRONTMATTER_WIKILINK_EDGE_FIELDS = (
    "author",
    "publisher",
    "channel",
    "outlet",
)


@dataclass(frozen=True)
class GraphNode:
    node_id: str
    page_id: str
    primary_type: str
    title: str
    path: str
    status: str
    normalized_title: str
    canonical_slug: str
    domains: list[str]
    facets: list[str]
    aliases: list[str]


@dataclass(frozen=True)
class GraphStatus:
    db_path: Path
    node_count: int
    alias_count: int
    edge_count: int
    document_count: int
    chunk_count: int
    last_built_at: str | None

    def render(self) -> str:
        return "\n".join(
            [
                f"graph-db: {self.db_path}",
                f"nodes: {self.node_count}",
                f"aliases: {self.alias_count}",
                f"edges: {self.edge_count}",
                f"documents: {self.document_count}",
                f"chunks: {self.chunk_count}",
                f"last_built_at: {self.last_built_at or '-'}",
            ]
        )


@dataclass(frozen=True)
class GraphRebuildResult:
    node_count: int
    alias_count: int
    edge_count: int
    document_count: int
    chunk_count: int
    built_at: str

    def render(self) -> str:
        return "\n".join(
            [
                "graph-rebuild:",
                f"- nodes={self.node_count}",
                f"- aliases={self.alias_count}",
                f"- edges={self.edge_count}",
                f"- documents={self.document_count}",
                f"- chunks={self.chunk_count}",
                f"- built_at={self.built_at}",
            ]
        )


@dataclass(frozen=True)
class ResolutionCandidate:
    registry_node_id: str
    page_id: str
    title: str
    primary_type: str
    path: str
    score: float
    match_kind: str
    aliases: list[str]


@dataclass(frozen=True)
class EmbeddingTarget:
    target_id: str
    target_type: str
    page_id: str
    content: str
    content_sha256: str


@dataclass(frozen=True)
class QueryPageMatch:
    page_id: str
    title: str
    path: str
    score: float
    snippet: str
    annotations: list[str]


@dataclass(frozen=True)
class _DocumentQueryCandidate:
    doc_id: str
    page_id: str
    title: str
    path: str
    score: float
    annotations: list[str]


def _utc_now_string() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _frontmatter_block(text: str) -> tuple[str | None, str]:
    if not text.startswith("---\n"):
        return None, text
    end = text.find("\n---\n", 4)
    if end == -1:
        return None, text
    return text[4:end], text[end + 5 :]


def _parse_page(path: Path) -> tuple[dict[str, Any], str]:
    text = path.read_text(encoding="utf-8")
    block, body = _frontmatter_block(text)
    if block is None:
        return {}, text
    try:
        frontmatter = yaml.safe_load(block) or {}
    except yaml.YAMLError:
        frontmatter = {}
    if not isinstance(frontmatter, dict):
        frontmatter = {}
    return frontmatter, body


def _coerce_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _extract_wikilinks(text: str) -> list[str]:
    import re

    return [match.strip() for match in re.findall(r"\[\[([^\]|#]+)", text) if match.strip()]


def _normalize_lookup_key(value: str) -> str:
    return normalize_identifier(value, max_len=120)


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _split_chunks(text: str, *, max_chars: int = 800) -> list[str]:
    chunks: list[str] = []
    for block in [item.strip() for item in text.split("\n\n") if item.strip()]:
        if len(block) <= max_chars:
            chunks.append(block)
            continue
        lines = [line.strip() for line in block.splitlines() if line.strip()]
        current = ""
        for line in lines:
            tentative = f"{current}\n{line}".strip() if current else line
            if len(tentative) > max_chars and current:
                chunks.append(current)
                current = line
            else:
                current = tentative
        if current:
            chunks.append(current)
    return chunks[:24]


def _extract_query_snippet(body: str, *, max_chars: int = 280) -> str:
    paragraphs = [line.strip() for line in body.splitlines() if line.strip() and not line.strip().startswith("#")]
    snippet = " ".join(paragraphs[:3]).strip()
    return snippet[:max_chars]


def _entity_facets(frontmatter: dict[str, Any], page_type: str) -> list[str]:
    explicit = _coerce_list(frontmatter.get("entity_facets") or frontmatter.get("facets"))
    if explicit:
        return sorted(set(explicit))
    entity_kind = str(frontmatter.get("entity_kind") or "").strip()
    if entity_kind:
        return sorted(set(part.strip() for part in entity_kind.split("-") if part.strip()))
    return [page_type]


def _typed_relation_targets(frontmatter: dict[str, Any]) -> dict[str, list[str]]:
    raw = frontmatter.get("typed_relations")
    if not isinstance(raw, dict):
        return {}
    normalized: dict[str, list[str]] = {}
    for kind in RELATION_KINDS:
        values = [
            str(item).strip().replace("[[", "").replace("]]", "")
            for item in _coerce_list(raw.get(kind))
            if str(item).strip()
        ]
        if values:
            normalized[kind] = values
    return normalized


def _frontmatter_wikilink_targets(frontmatter: dict[str, Any]) -> list[str]:
    targets: list[str] = []
    for key in FRONTMATTER_WIKILINK_EDGE_FIELDS:
        value = frontmatter.get(key)
        if value is None:
            continue
        for item in value if isinstance(value, list) else [value]:
            targets.extend(_extract_wikilinks(str(item)))
    return targets


class GraphRegistry:
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        self.vault = Vault.load(repo_root)
        self.db_path = self.vault.graph_db

    @classmethod
    def for_repo_root(cls, repo_root: Path) -> "GraphRegistry":
        registry = cls(repo_root)
        registry.bootstrap()
        return registry

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def bootstrap(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS graph_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS nodes (
                    id TEXT PRIMARY KEY,
                    page_id TEXT NOT NULL,
                    primary_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    path TEXT NOT NULL,
                    status TEXT NOT NULL,
                    normalized_title TEXT NOT NULL,
                    canonical_slug TEXT NOT NULL,
                    domains_json TEXT NOT NULL,
                    facets_json TEXT NOT NULL,
                    created_at TEXT,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS aliases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
                    alias TEXT NOT NULL,
                    normalized_alias TEXT NOT NULL,
                    alias_kind TEXT NOT NULL,
                    source TEXT NOT NULL,
                    UNIQUE(node_id, normalized_alias)
                );

                CREATE TABLE IF NOT EXISTS edges (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    src_id TEXT NOT NULL,
                    rel_type TEXT NOT NULL,
                    dst_id TEXT NOT NULL,
                    source_page_id TEXT NOT NULL,
                    confidence REAL NOT NULL DEFAULT 1.0,
                    updated_at TEXT NOT NULL,
                    UNIQUE(src_id, rel_type, dst_id, source_page_id)
                );

                CREATE TABLE IF NOT EXISTS documents (
                    doc_id TEXT PRIMARY KEY,
                    path TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    title TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    ingest_lane TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS document_chunks (
                    chunk_id TEXT PRIMARY KEY,
                    doc_id TEXT NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
                    ordinal INTEGER NOT NULL,
                    text TEXT NOT NULL,
                    sha256 TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS document_targets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    doc_id TEXT NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
                    node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
                    relation_kind TEXT NOT NULL,
                    UNIQUE(doc_id, node_id, relation_kind)
                );

                CREATE TABLE IF NOT EXISTS ingest_resolutions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    doc_id TEXT NOT NULL,
                    mention_text TEXT NOT NULL,
                    resolved_node_id TEXT,
                    resolution_kind TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    rationale TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ingest_candidates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    doc_id TEXT NOT NULL,
                    mention_text TEXT NOT NULL,
                    candidate_node_id TEXT NOT NULL,
                    score REAL NOT NULL,
                    match_kind TEXT NOT NULL,
                    debug_payload_json TEXT
                );

                CREATE TABLE IF NOT EXISTS embeddings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_id TEXT NOT NULL,
                    target_type TEXT NOT NULL,
                    page_id TEXT NOT NULL,
                    model TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    vector_dim INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(target_id, model)
                );

                CREATE TABLE IF NOT EXISTS query_embeddings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    model TEXT NOT NULL,
                    query_text TEXT NOT NULL,
                    normalized_query TEXT NOT NULL,
                    vector_json TEXT NOT NULL,
                    vector_dim INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(model, normalized_query)
                );

                CREATE VIRTUAL TABLE IF NOT EXISTS nodes_fts USING fts5(
                    node_id UNINDEXED,
                    title,
                    aliases,
                    path
                );

                CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
                    doc_id UNINDEXED,
                    title,
                    body
                );

                CREATE VIRTUAL TABLE IF NOT EXISTS document_chunks_fts USING fts5(
                    chunk_id UNINDEXED,
                    text
                );
                """
            )
            existing_columns = {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(nodes)").fetchall()
            }
            if "page_id" not in existing_columns:
                conn.execute("ALTER TABLE nodes ADD COLUMN page_id TEXT NOT NULL DEFAULT ''")

    def _iter_canonical_paths(self) -> list[Path]:
        paths: list[Path] = []
        for dirname in CANONICAL_NODE_DIRS:
            root = self.vault.wiki / dirname
            if not root.exists():
                continue
            for path in sorted(root.rglob("*.md")):
                if path.name in SYSTEM_SKIP_NAMES:
                    continue
                paths.append(path)
        return paths

    def _iter_document_paths(self) -> list[Path]:
        paths: list[Path] = []
        for path in sorted(self.vault.wiki.rglob("*.md")):
            rel = path.relative_to(self.vault.wiki)
            if path.name in SYSTEM_SKIP_NAMES:
                continue
            if rel.parts and rel.parts[0] in SYSTEM_SKIP_DIRS:
                continue
            paths.append(path)
        return paths

    def _logical_path(self, path: Path) -> str:
        return self.vault.logical_path(path)

    def _resolve_logical_path(self, path_text: str) -> Path:
        return self.vault.resolve_logical_path(path_text)

    def resolve_path(self, path_text: str) -> Path:
        return self._resolve_logical_path(path_text)

    def _canonical_embedding_text(self, node: GraphNode) -> str:
        path = self._resolve_logical_path(node.path)
        _frontmatter, body = _parse_page(path)
        lines = [f"Title: {node.title}", f"Type: {node.primary_type}"]
        if node.aliases:
            lines.append("Aliases: " + ", ".join(node.aliases))
        if node.facets:
            lines.append("Facets: " + ", ".join(node.facets))
        paragraphs = [block.strip() for block in body.split("\n\n") if block.strip()]
        intro = ""
        for block in paragraphs:
            if not block.startswith("#"):
                intro = block
                break
        if intro:
            lines.append(f"Overview: {intro[:400]}")
        heading_map = {
            "project": ("Project Priorities", "Constraints", "Capabilities", "Recent Evidence"),
            "company": ("Overview", "Constraints", "Capabilities", "Recent Evidence"),
            "person": ("Roles", "Relationships", "Recent Evidence"),
            "concept": ("TL;DR", "Evidence log"),
            "playbook": ("TL;DR", "Evidence log"),
            "stance": ("TL;DR", "Evidence log"),
            "inquiry": ("TL;DR", "Evidence log"),
        }
        headings = heading_map.get(node.primary_type, ("Recent Evidence",))
        for heading in headings:
            if f"## {heading}" not in body:
                continue
            import re

            match = re.search(rf"^## {re.escape(heading)}\s*$", body, re.MULTILINE)
            if not match:
                continue
            rest = body[match.end():].lstrip("\n")
            next_heading = re.search(r"^## ", rest, re.MULTILINE)
            content = rest[: next_heading.start()].strip() if next_heading else rest.strip()
            if content:
                lines.append(f"{heading}: {content[:400]}")
        return "\n".join(lines)

    def rebuild(self, *, phase_callback: Callable[[str], None] | None = None) -> GraphRebuildResult:
        built_at = _utc_now_string()
        if phase_callback is not None:
            phase_callback("scanning canonical pages")
        canonical_pages = self._iter_canonical_paths()
        page_rows: list[dict[str, Any]] = []
        page_id_counts: dict[str, int] = {}
        for path in canonical_pages:
            frontmatter, body = _parse_page(path)
            page_id = normalize_identifier(str(frontmatter.get("id") or path.stem))
            if not page_id:
                continue
            page_type = str(frontmatter.get("type") or path.parent.name.rstrip("s")).strip() or "note"
            title = str(frontmatter.get("title") or path.stem.replace("-", " ").title()).strip()
            page_rows.append(
                {
                    "path": path,
                    "frontmatter": frontmatter,
                    "body": body,
                    "page_id": page_id,
                    "page_type": page_type,
                    "title": title,
                }
            )
            page_id_counts[page_id] = page_id_counts.get(page_id, 0) + 1

        raw_nodes: list[GraphNode] = []
        edges_to_insert: list[tuple[str, str, str, str]] = []
        docs_to_insert: list[tuple[str, str, str, str, str, str, str, list[str], list[tuple[str, str]]]] = []
        raw_to_registry_ids: dict[str, list[str]] = {}

        for row in page_rows:
            path = row["path"]
            frontmatter = row["frontmatter"]
            body = row["body"]
            page_id = row["page_id"]
            page_type = row["page_type"]
            title = row["title"]
            node_id = page_id if page_id_counts.get(page_id, 0) == 1 else f"{page_type}:{page_id}"
            raw_to_registry_ids.setdefault(page_id, []).append(node_id)
            alias_map: dict[str, str] = {}
            for item in [title, path.stem, *_coerce_list(frontmatter.get("aliases"))]:
                text = str(item).strip()
                if not text:
                    continue
                alias_map.setdefault(_normalize_lookup_key(text), text)
            aliases = sorted(alias_map.values())
            raw_nodes.append(
                GraphNode(
                    node_id=node_id,
                    page_id=page_id,
                    primary_type=page_type,
                    title=title,
                    path=self._logical_path(path),
                    status=str(frontmatter.get("status") or "active"),
                    normalized_title=_normalize_lookup_key(title),
                    canonical_slug=_normalize_lookup_key(page_id),
                    domains=_coerce_list(frontmatter.get("domains")),
                    facets=_entity_facets(frontmatter, page_type),
                    aliases=aliases,
                )
            )
            for kind, targets in _typed_relation_targets(frontmatter).items():
                for target in targets:
                    edges_to_insert.append((node_id, kind, target, node_id))
            for target in [
                *_coerce_list(frontmatter.get("relates_to")),
                *_coerce_list(frontmatter.get("sources")),
                *_frontmatter_wikilink_targets(frontmatter),
                *_extract_wikilinks(body),
            ]:
                normalized_target = normalize_identifier(target.replace("[[", "").replace("]]", "").strip())
                if normalized_target:
                    edges_to_insert.append((node_id, "wikilink", normalized_target, node_id))
        for path in self._iter_document_paths():
            frontmatter, body = _parse_page(path)
            page_id = normalize_identifier(str(frontmatter.get("id") or path.stem)) or normalize_identifier(path.stem)
            page_type = str(frontmatter.get("type") or path.parent.name.rstrip("s")).strip() or "note"
            title = str(frontmatter.get("title") or path.stem.replace("-", " ").title()).strip()
            rel_path = self._logical_path(path)
            rel = path.relative_to(self.vault.wiki)
            is_canonical_doc = bool(rel.parts) and rel.parts[0] in CANONICAL_NODE_DIRS
            if is_canonical_doc and page_id in raw_to_registry_ids and len(raw_to_registry_ids[page_id]) == 1:
                doc_id = raw_to_registry_ids[page_id][0]
            else:
                doc_id = f"doc:{rel_path}"
            targets: list[tuple[str, str]] = []
            if page_id in raw_to_registry_ids and len(raw_to_registry_ids[page_id]) == 1:
                targets.append((raw_to_registry_ids[page_id][0], "self"))
            for target_page_id in {
                *_coerce_list(frontmatter.get("entities")),
                *_coerce_list(frontmatter.get("relates_to")),
                *_coerce_list(frontmatter.get("sources")),
                *_frontmatter_wikilink_targets(frontmatter),
                *[item for values in _typed_relation_targets(frontmatter).values() for item in values],
                *_extract_wikilinks(body),
            }:
                normalized_target = normalize_identifier(target_page_id.replace("[[", "").replace("]]", "").strip())
                if normalized_target in raw_to_registry_ids and len(raw_to_registry_ids[normalized_target]) == 1:
                    targets.append((raw_to_registry_ids[normalized_target][0], "linked"))
            deduped_targets = sorted({item for item in targets})
            docs_to_insert.append(
                (
                    doc_id,
                    rel_path,
                    _hash_text(body),
                    title,
                    page_type,
                    "graph-rebuild",
                    body,
                    _split_chunks(body),
                    deduped_targets,
                )
            )

        filtered_edges = sorted(
            {
                (src_id, rel_type, raw_to_registry_ids[dst_id][0], source_page_id)
                for src_id, rel_type, dst_id, source_page_id in edges_to_insert
                if dst_id in raw_to_registry_ids and len(raw_to_registry_ids[dst_id]) == 1 and raw_to_registry_ids[dst_id][0] != src_id
            }
        )

        if phase_callback is not None:
            phase_callback("writing graph registry")
        with self.connect() as conn:
            for table in ("aliases", "edges", "document_targets", "document_chunks", "documents", "nodes_fts", "documents_fts", "document_chunks_fts", "nodes"):
                conn.execute(f"DELETE FROM {table}")

            for node in raw_nodes:
                conn.execute(
                    """
                    INSERT INTO nodes(
                        id, page_id, primary_type, title, path, status, normalized_title, canonical_slug,
                        domains_json, facets_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        node.node_id,
                        node.page_id,
                        node.primary_type,
                        node.title,
                        node.path,
                        node.status,
                        node.normalized_title,
                        node.canonical_slug,
                        json.dumps(node.domains, ensure_ascii=False),
                        json.dumps(node.facets, ensure_ascii=False),
                        built_at,
                        built_at,
                    ),
                )
                for alias in node.aliases:
                    conn.execute(
                        """
                        INSERT INTO aliases(node_id, alias, normalized_alias, alias_kind, source)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            node.node_id,
                            alias,
                            _normalize_lookup_key(alias),
                            "canonical",
                            node.path,
                        ),
                    )
                conn.execute(
                    """
                    INSERT INTO nodes_fts(node_id, title, aliases, path)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        node.node_id,
                        node.title,
                        " ".join(node.aliases),
                        node.path,
                    ),
                )

            for src_id, rel_type, dst_id, source_page_id in filtered_edges:
                conn.execute(
                    """
                    INSERT INTO edges(src_id, rel_type, dst_id, source_page_id, confidence, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (src_id, rel_type, dst_id, source_page_id, 1.0, built_at),
                )

            chunk_count = 0
            for doc_id, path_text, sha256, title, source_kind, ingest_lane, body, chunks, targets in docs_to_insert:
                conn.execute(
                    """
                    INSERT INTO documents(doc_id, path, sha256, title, source_kind, ingest_lane, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (doc_id, path_text, sha256, title, source_kind, ingest_lane, built_at),
                )
                conn.execute(
                    """
                    INSERT INTO documents_fts(doc_id, title, body)
                    VALUES (?, ?, ?)
                    """,
                    (doc_id, title, body),
                )
                for ordinal, chunk in enumerate(chunks):
                    chunk_id = f"{doc_id}:{ordinal}"
                    conn.execute(
                        """
                        INSERT INTO document_chunks(chunk_id, doc_id, ordinal, text, sha256)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (chunk_id, doc_id, ordinal, chunk, _hash_text(chunk)),
                    )
                    conn.execute(
                        """
                        INSERT INTO document_chunks_fts(chunk_id, text)
                        VALUES (?, ?)
                        """,
                        (chunk_id, chunk),
                    )
                    chunk_count += 1
                for node_id, relation_kind in targets:
                    conn.execute(
                        """
                        INSERT INTO document_targets(doc_id, node_id, relation_kind)
                        VALUES (?, ?, ?)
                        """,
                        (doc_id, node_id, relation_kind),
                    )

            conn.execute(
                """
                INSERT INTO graph_meta(key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                ("last_built_at", built_at, built_at),
            )

        return GraphRebuildResult(
            node_count=len(raw_nodes),
            alias_count=sum(len(node.aliases) for node in raw_nodes),
            edge_count=len(filtered_edges),
            document_count=len(docs_to_insert),
            chunk_count=chunk_count,
            built_at=built_at,
        )

    def status(self) -> GraphStatus:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM nodes) AS node_count,
                    (SELECT COUNT(*) FROM aliases) AS alias_count,
                    (SELECT COUNT(*) FROM edges) AS edge_count,
                    (SELECT COUNT(*) FROM documents) AS document_count,
                    (SELECT COUNT(*) FROM document_chunks) AS chunk_count
                """
            ).fetchone()
            built = conn.execute(
                "SELECT value FROM graph_meta WHERE key = 'last_built_at'"
            ).fetchone()
        return GraphStatus(
            db_path=self.db_path,
            node_count=int(row["node_count"] or 0),
            alias_count=int(row["alias_count"] or 0),
            edge_count=int(row["edge_count"] or 0),
            document_count=int(row["document_count"] or 0),
            chunk_count=int(row["chunk_count"] or 0),
            last_built_at=str(built["value"]) if built else None,
        )

    def ensure_built(self) -> GraphStatus:
        status = self.status()
        if status.node_count == 0 or status.document_count == 0:
            self.rebuild()
            status = self.status()
        return status

    def is_stale(self) -> bool:
        status = self.status()
        built_at = _parse_timestamp(status.last_built_at)
        if built_at is None:
            return True
        latest_mtime = 0.0
        for path in self._iter_document_paths():
            try:
                latest_mtime = max(latest_mtime, path.stat().st_mtime)
            except FileNotFoundError:
                continue
        return latest_mtime > built_at.timestamp()

    def ensure_fresh(self) -> GraphStatus:
        status = self.ensure_built()
        if self.is_stale():
            self.rebuild()
            status = self.status()
        return status

    def get_node(self, node_id: str) -> GraphNode | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT n.*, GROUP_CONCAT(a.alias, '\u001f') AS aliases_text
                FROM nodes n
                LEFT JOIN aliases a ON a.node_id = n.id
                WHERE n.id = ?
                GROUP BY n.id
                """,
                (node_id,),
            ).fetchone()
        if row is None:
            return None
        aliases = [item for item in str(row["aliases_text"] or "").split("\u001f") if item]
        node_id = str(row["id"])
        primary_type = str(row["primary_type"])
        return GraphNode(
            node_id=node_id,
            page_id=str(row["page_id"] or (node_id.split(":", 1)[1] if node_id.startswith(f"{primary_type}:") else node_id)),
            primary_type=primary_type,
            title=str(row["title"]),
            path=str(row["path"]),
            status=str(row["status"]),
            normalized_title=str(row["normalized_title"]),
            canonical_slug=str(row["canonical_slug"]),
            domains=json.loads(str(row["domains_json"] or "[]")),
            facets=json.loads(str(row["facets_json"] or "[]")),
            aliases=aliases,
        )

    def resolve_candidates(self, mention_text: str, *, limit: int = 5) -> list[ResolutionCandidate]:
        self.ensure_fresh()
        mention = mention_text.strip()
        if not mention:
            return []
        normalized = _normalize_lookup_key(mention)
        sanitized = " ".join(part for part in normalized.replace("-", " ").split() if part)
        candidates: dict[str, ResolutionCandidate] = {}

        def _store(rows: list[sqlite3.Row], *, score: float, match_kind: str) -> None:
            for row in rows:
                node_id = str(row["id"])
                if node_id in candidates and candidates[node_id].score >= score:
                    continue
                alias_text = str(row["aliases_text"] or "")
                aliases = [item for item in alias_text.split("\u001f") if item]
                candidates[node_id] = ResolutionCandidate(
                    registry_node_id=node_id,
                    page_id=str(row["page_id"] or node_id),
                    title=str(row["title"]),
                    primary_type=str(row["primary_type"]),
                    path=str(row["path"]),
                    score=score,
                    match_kind=match_kind,
                    aliases=aliases,
                )

        with self.connect() as conn:
            exact_rows = conn.execute(
                """
                SELECT n.*, GROUP_CONCAT(a.alias, '\u001f') AS aliases_text
                FROM nodes n
                LEFT JOIN aliases a ON a.node_id = n.id
                WHERE n.id = ? OR n.page_id = ? OR n.normalized_title = ?
                   OR EXISTS (
                        SELECT 1 FROM aliases ax
                        WHERE ax.node_id = n.id AND ax.normalized_alias = ?
                   )
                GROUP BY n.id
                """,
                (mention, mention, normalized, normalized),
            ).fetchall()
            _store(list(exact_rows), score=1.0, match_kind="exact")
            if sanitized:
                fts_rows = conn.execute(
                    """
                    SELECT n.*, GROUP_CONCAT(a.alias, '\u001f') AS aliases_text
                    FROM nodes_fts f
                    JOIN nodes n ON n.id = f.node_id
                    LEFT JOIN aliases a ON a.node_id = n.id
                    WHERE nodes_fts MATCH ?
                    GROUP BY n.id
                    ORDER BY n.id
                    LIMIT ?
                    """,
                    (sanitized, limit),
                ).fetchall()
                _store(list(fts_rows), score=0.82, match_kind="fts_title_alias")
                body_rows = conn.execute(
                    """
                    SELECT n.*, GROUP_CONCAT(a.alias, '\u001f') AS aliases_text
                    FROM documents_fts d
                    JOIN document_targets dt ON dt.doc_id = d.doc_id
                    JOIN nodes n ON n.id = dt.node_id
                    LEFT JOIN aliases a ON a.node_id = n.id
                    WHERE documents_fts MATCH ?
                    GROUP BY n.id
                    ORDER BY n.id
                    LIMIT ?
                    """,
                    (sanitized, limit),
                ).fetchall()
                _store(list(body_rows), score=0.68, match_kind="fts_body")
        ordered = sorted(candidates.values(), key=lambda item: (-item.score, item.registry_node_id))
        return ordered[:limit]

    def query_pages(self, question: str, *, limit: int = 8) -> list[QueryPageMatch]:
        self.ensure_fresh()
        candidates = self.resolve_candidates(question, limit=max(limit * 6, 24))
        matched: dict[str, dict[str, Any]] = {}
        for candidate in candidates:
            matched[candidate.registry_node_id] = {
                "candidate": candidate,
                "score": float(candidate.score) * 10.0,
                "annotations": [],
            }
        document_matches: dict[str, _DocumentQueryCandidate] = {}
        normalized = _normalize_lookup_key(question)
        sanitized = " ".join(part for part in normalized.replace("-", " ").split() if part)
        with self.connect() as conn:
            if sanitized:
                chunk_rows = conn.execute(
                    """
                    SELECT n.id AS node_id
                    FROM document_chunks_fts c
                    JOIN document_chunks dc ON dc.chunk_id = c.chunk_id
                    JOIN document_targets dt ON dt.doc_id = dc.doc_id
                    JOIN nodes n ON n.id = dt.node_id
                    WHERE document_chunks_fts MATCH ?
                    GROUP BY n.id
                    LIMIT ?
                    """,
                    (sanitized, max(limit * 6, 24)),
                ).fetchall()
                for row in chunk_rows:
                    node_id = str(row["node_id"])
                    if node_id in matched:
                        matched[node_id]["score"] += 3.0
                document_rows = conn.execute(
                    """
                    SELECT
                        d.doc_id,
                        d.path,
                        d.title,
                        EXISTS(
                            SELECT 1
                            FROM document_targets dt
                            WHERE dt.doc_id = d.doc_id AND dt.relation_kind = 'self'
                        ) AS has_self_target
                    FROM documents_fts f
                    JOIN documents d ON d.doc_id = f.doc_id
                    WHERE documents_fts MATCH ?
                    LIMIT ?
                    """,
                    (sanitized, max(limit * 8, 32)),
                ).fetchall()
                matched_paths = {
                    str(item["candidate"].path)
                    for item in matched.values()
                }
                for row in document_rows:
                    path_text = str(row["path"])
                    if int(row["has_self_target"] or 0) == 1 or path_text in matched_paths:
                        continue
                    page_path = self._resolve_logical_path(path_text)
                    frontmatter, body = _parse_page(page_path)
                    page_id = normalize_identifier(str(frontmatter.get("id") or Path(path_text).stem)) or normalize_identifier(Path(path_text).stem)
                    title = str(frontmatter.get("title") or row["title"] or Path(path_text).stem.replace("-", " ").title()).strip()
                    current = document_matches.get(path_text)
                    score = float(current.score) if current else 6.0
                    annotations = list(current.annotations) if current else ["document-only"]
                    linked_rows = conn.execute(
                        """
                        SELECT DISTINCT n.page_id
                        FROM document_targets dt
                        JOIN nodes n ON n.id = dt.node_id
                        WHERE dt.doc_id = ? AND dt.relation_kind != 'self'
                        ORDER BY n.page_id
                        LIMIT 3
                        """,
                        (str(row["doc_id"]),),
                    ).fetchall()
                    linked_page_ids = [str(item["page_id"]) for item in linked_rows]
                    if linked_page_ids:
                        annotation = "links " + ", ".join(f"[[{item}]]" for item in linked_page_ids)
                        if annotation not in annotations:
                            annotations.append(annotation)
                    document_matches[path_text] = _DocumentQueryCandidate(
                        doc_id=str(row["doc_id"]),
                        page_id=page_id,
                        title=title,
                        path=path_text,
                        score=score,
                        annotations=annotations,
                    )
                document_chunk_rows = conn.execute(
                    """
                    SELECT d.doc_id, d.path
                    FROM document_chunks_fts c
                    JOIN document_chunks dc ON dc.chunk_id = c.chunk_id
                    JOIN documents d ON d.doc_id = dc.doc_id
                    WHERE document_chunks_fts MATCH ?
                    GROUP BY d.doc_id
                    LIMIT ?
                    """,
                    (sanitized, max(limit * 8, 32)),
                ).fetchall()
                for row in document_chunk_rows:
                    path_text = str(row["path"])
                    current = document_matches.get(path_text)
                    if current is None:
                        continue
                    document_matches[path_text] = _DocumentQueryCandidate(
                        doc_id=current.doc_id,
                        page_id=current.page_id,
                        title=current.title,
                        path=current.path,
                        score=current.score + 2.5,
                        annotations=current.annotations,
                    )
            for node_id in list(matched):
                rows = conn.execute(
                    """
                    SELECT rel_type, dst_id AS other_id
                    FROM edges
                    WHERE src_id = ? AND rel_type != 'wikilink'
                    UNION ALL
                    SELECT rel_type, src_id AS other_id
                    FROM edges
                    WHERE dst_id = ? AND rel_type != 'wikilink'
                    """,
                    (node_id, node_id),
                ).fetchall()
                for row in rows:
                    other_id = str(row["other_id"])
                    rel_type = str(row["rel_type"])
                    if other_id not in matched:
                        continue
                    matched[node_id]["score"] += 2.0
                    if rel_type == "contradicts":
                        other = matched[other_id]["candidate"]
                        note = f"tension with [[{other.page_id}]]"
                        if note not in matched[node_id]["annotations"]:
                            matched[node_id]["annotations"].append(note)
        combined: list[tuple[float, str, Any, str]] = []
        for item in matched.values():
            combined.append((float(item["score"]), item["candidate"].registry_node_id, item, "node"))
        for item in document_matches.values():
            combined.append((float(item.score), item.path, item, "document"))
        if not combined:
            return []
        ordered = sorted(
            combined,
            key=lambda item: (-item[0], item[1]),
        )[:limit]
        results: list[QueryPageMatch] = []
        for score, _sort_key, payload, kind in ordered:
            if kind == "node":
                candidate = payload["candidate"]
                path = self._resolve_logical_path(candidate.path)
                snippet = ""
                try:
                    _frontmatter, body = _parse_page(path)
                    snippet = _extract_query_snippet(body)
                except Exception:
                    snippet = ""
                results.append(
                    QueryPageMatch(
                        page_id=candidate.page_id,
                        title=candidate.title,
                        path=candidate.path,
                        score=score,
                        snippet=snippet,
                        annotations=list(payload["annotations"]),
                    )
                )
                continue
            candidate = payload
            snippet = ""
            try:
                _frontmatter, body = _parse_page(self._resolve_logical_path(candidate.path))
                snippet = _extract_query_snippet(body)
            except Exception:
                snippet = ""
            results.append(
                QueryPageMatch(
                    page_id=candidate.page_id,
                    title=candidate.title,
                    path=candidate.path,
                    score=score,
                    snippet=snippet,
                    annotations=list(candidate.annotations),
                )
            )
        return results

    def resolve_vector_candidates(
        self,
        mention_text: str,
        *,
        embedding_service: EmbeddingService,
        vector_backend: VectorIndexBackend,
        model: str,
        limit: int = 5,
    ) -> list[ResolutionCandidate]:
        mention = mention_text.strip()
        if not mention:
            return []
        if int(self.embedding_status(model=model).get("count") or 0) == 0:
            return []
        normalized = _normalize_lookup_key(mention)
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT vector_json
                FROM query_embeddings
                WHERE model = ? AND normalized_query = ?
                """,
                (model, normalized),
            ).fetchone()
        if row is not None:
            query_vector = list(json.loads(str(row["vector_json"] or "[]")))
        else:
            execution = embedding_service.embed_query(mention)
            if not execution.vectors:
                return []
            query_vector = execution.vectors[0]
            with self.connect() as conn:
                conn.execute(
                    """
                    INSERT INTO query_embeddings(model, query_text, normalized_query, vector_json, vector_dim, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(model, normalized_query) DO UPDATE SET
                        query_text = excluded.query_text,
                        vector_json = excluded.vector_json,
                        vector_dim = excluded.vector_dim,
                        updated_at = excluded.updated_at
                    """,
                    (
                        model,
                        mention,
                        normalized,
                        json.dumps(query_vector, ensure_ascii=False),
                        len(query_vector),
                        _utc_now_string(),
                    ),
                )
        matches: list[VectorQueryMatch] = vector_backend.query(
            model=model,
            query_vector=query_vector,
            limit=limit,
        )
        candidates: list[ResolutionCandidate] = []
        for match in matches:
            node = self.get_node(match.target_id)
            if node is None:
                continue
            candidates.append(
                ResolutionCandidate(
                    registry_node_id=node.node_id,
                    page_id=node.page_id,
                    title=node.title,
                    primary_type=node.primary_type,
                    path=node.path,
                    score=match.score,
                    match_kind="vector",
                    aliases=node.aliases,
                )
            )
        return candidates

    def query_embedding_status(self, *, model: str) -> dict[str, object]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS count, MAX(updated_at) AS last_updated, MAX(vector_dim) AS vector_dim
                FROM query_embeddings
                WHERE model = ?
                """,
                (model,),
            ).fetchone()
        return {
            "model": model,
            "count": int(row["count"] or 0),
            "last_updated": str(row["last_updated"]) if row and row["last_updated"] else None,
            "vector_dim": int(row["vector_dim"] or 0),
        }

    def list_embedding_targets(self) -> list[EmbeddingTarget]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT n.*, GROUP_CONCAT(a.alias, '\u001f') AS aliases_text
                FROM nodes n
                LEFT JOIN aliases a ON a.node_id = n.id
                GROUP BY n.id
                ORDER BY n.id
                """
            ).fetchall()
        targets: list[EmbeddingTarget] = []
        for row in rows:
            aliases = [item for item in str(row["aliases_text"] or "").split("\u001f") if item]
            node = GraphNode(
                node_id=str(row["id"]),
                page_id=str(row["page_id"]),
                primary_type=str(row["primary_type"]),
                title=str(row["title"]),
                path=str(row["path"]),
                status=str(row["status"]),
                normalized_title=str(row["normalized_title"]),
                canonical_slug=str(row["canonical_slug"]),
                domains=json.loads(str(row["domains_json"] or "[]")),
                facets=json.loads(str(row["facets_json"] or "[]")),
                aliases=aliases,
            )
            content = self._canonical_embedding_text(node)
            targets.append(
                EmbeddingTarget(
                    target_id=node.node_id,
                    target_type="node",
                    page_id=node.page_id,
                    content=content,
                    content_sha256=_hash_text(content),
                )
            )
        return targets

    def list_embedding_metadata(self, *, model: str) -> dict[str, dict[str, object]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT target_id, target_type, page_id, content_sha256, vector_dim, updated_at
                FROM embeddings
                WHERE model = ?
                """,
                (model,),
            ).fetchall()
        return {
            str(row["target_id"]): {
                "target_type": str(row["target_type"]),
                "page_id": str(row["page_id"]),
                "content_sha256": str(row["content_sha256"]),
                "vector_dim": int(row["vector_dim"]),
                "updated_at": str(row["updated_at"]),
            }
            for row in rows
        }

    def upsert_embeddings(
        self,
        *,
        model: str,
        records: list[dict[str, object]],
    ) -> None:
        with self.connect() as conn:
            for record in records:
                conn.execute(
                    """
                    INSERT INTO embeddings(target_id, target_type, page_id, model, content_sha256, vector_dim, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(target_id, model) DO UPDATE SET
                        target_type = excluded.target_type,
                        page_id = excluded.page_id,
                        content_sha256 = excluded.content_sha256,
                        vector_dim = excluded.vector_dim,
                        updated_at = excluded.updated_at
                    """,
                    (
                        str(record["target_id"]),
                        str(record["target_type"]),
                        str(record["page_id"]),
                        model,
                        str(record["content_sha256"]),
                        int(record["vector_dim"]),
                        _utc_now_string(),
                    ),
                )

    def prune_embeddings(self, *, model: str, valid_target_ids: set[str]) -> None:
        with self.connect() as conn:
            rows = conn.execute("SELECT target_id FROM embeddings WHERE model = ?", (model,)).fetchall()
            stale_ids = [str(row["target_id"]) for row in rows if str(row["target_id"]) not in valid_target_ids]
            for target_id in stale_ids:
                conn.execute("DELETE FROM embeddings WHERE target_id = ? AND model = ?", (target_id, model))

    def embedding_status(self, *, model: str) -> dict[str, object]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS count, MAX(updated_at) AS last_updated, MAX(vector_dim) AS vector_dim
                FROM embeddings
                WHERE model = ?
                """,
                (model,),
            ).fetchone()
        return {
            "model": model,
            "count": int(row["count"] or 0),
            "last_updated": str(row["last_updated"]) if row and row["last_updated"] else None,
            "vector_dim": int(row["vector_dim"] or 0),
        }

    def resolve_vector_candidates(
        self,
        mention_text: str,
        *,
        embedding_service: EmbeddingService,
        vector_backend: VectorIndexBackend,
        model: str,
        limit: int = 5,
    ) -> list[ResolutionCandidate]:
        mention = mention_text.strip()
        if not mention:
            return []
        if int(self.embedding_status(model=model).get("count") or 0) == 0:
            return []
        execution = embedding_service.embed_query(mention)
        if not execution.vectors:
            return []
        matches: list[VectorQueryMatch] = vector_backend.query(
            model=model,
            query_vector=execution.vectors[0],
            limit=limit,
        )
        candidates: list[ResolutionCandidate] = []
        for match in matches:
            node = self.get_node(match.target_id)
            if node is None:
                continue
            candidates.append(
                ResolutionCandidate(
                    registry_node_id=node.node_id,
                    page_id=node.page_id,
                    title=node.title,
                    primary_type=node.primary_type,
                    path=node.path,
                    score=match.score,
                    match_kind="vector",
                    aliases=node.aliases,
                )
            )
        return candidates

    def record_document(
        self,
        *,
        doc_id: str,
        path: Path,
        title: str,
        source_kind: str,
        ingest_lane: str,
        body: str,
        resolutions: list[dict[str, Any]],
        candidates: list[dict[str, Any]],
        document_targets: list[str] | None = None,
    ) -> None:
        updated_at = _utc_now_string()
        sha256 = _hash_text(body)
        chunks = _split_chunks(body)
        with self.connect() as conn:
            conn.execute("DELETE FROM ingest_resolutions WHERE doc_id = ?", (doc_id,))
            conn.execute("DELETE FROM ingest_candidates WHERE doc_id = ?", (doc_id,))
            conn.execute("DELETE FROM document_targets WHERE doc_id = ?", (doc_id,))
            conn.execute("DELETE FROM document_chunks_fts WHERE chunk_id IN (SELECT chunk_id FROM document_chunks WHERE doc_id = ?)", (doc_id,))
            conn.execute("DELETE FROM document_chunks WHERE doc_id = ?", (doc_id,))
            conn.execute("DELETE FROM documents_fts WHERE doc_id = ?", (doc_id,))
            conn.execute(
                """
                INSERT INTO documents(doc_id, path, sha256, title, source_kind, ingest_lane, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(doc_id) DO UPDATE SET
                    path = excluded.path,
                    sha256 = excluded.sha256,
                    title = excluded.title,
                    source_kind = excluded.source_kind,
                    ingest_lane = excluded.ingest_lane,
                    updated_at = excluded.updated_at
                """,
                (doc_id, self._logical_path(path), sha256, title, source_kind, ingest_lane, updated_at),
            )
            conn.execute(
                """
                INSERT INTO documents_fts(doc_id, title, body)
                VALUES (?, ?, ?)
                """,
                (doc_id, title, body),
            )
            for ordinal, chunk in enumerate(chunks):
                chunk_id = f"{doc_id}:{ordinal}"
                conn.execute(
                    """
                    INSERT INTO document_chunks(chunk_id, doc_id, ordinal, text, sha256)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (chunk_id, doc_id, ordinal, chunk, _hash_text(chunk)),
                )
                conn.execute(
                    """
                    INSERT INTO document_chunks_fts(chunk_id, text)
                    VALUES (?, ?)
                    """,
                    (chunk_id, chunk),
                )
            for node_id in sorted(set(document_targets or [])):
                conn.execute(
                    """
                    INSERT INTO document_targets(doc_id, node_id, relation_kind)
                    VALUES (?, ?, ?)
                    """,
                    (doc_id, node_id, "resolved"),
                )
            for item in resolutions:
                conn.execute(
                    """
                    INSERT INTO ingest_resolutions(doc_id, mention_text, resolved_node_id, resolution_kind, confidence, rationale, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        doc_id,
                        str(item.get("mention_text") or ""),
                        item.get("resolved_node_id"),
                        str(item.get("resolution_kind") or ""),
                        float(item.get("confidence") or 0.0),
                        str(item.get("rationale") or ""),
                        updated_at,
                    ),
                )
            for item in candidates:
                conn.execute(
                    """
                    INSERT INTO ingest_candidates(doc_id, mention_text, candidate_node_id, score, match_kind, debug_payload_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        doc_id,
                        str(item.get("mention_text") or ""),
                        str(item.get("candidate_node_id") or ""),
                        float(item.get("score") or 0.0),
                        str(item.get("match_kind") or ""),
                        json.dumps(item.get("debug_payload") or {}, ensure_ascii=False),
                    ),
                )
