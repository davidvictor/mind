from __future__ import annotations

import argparse
import json
from pathlib import Path

from mind.services.cli_progress import progress_for_args
from mind.services.embedding_evaluation import evaluate_promotion_gate
from mind.services.embedding_service import EmbeddingRequest, get_embedding_service
from mind.services.graph_registry import GraphRegistry
from mind.services.ingest_readiness import build_graph_health
from mind.services.llm_routing import resolve_route
from mind.services.vector_index import select_vector_backend
from scripts.common.vault import Vault, raw_path

from . import common as command_common


def _project_root() -> Path:
    return command_common.project_root()


def _write_shadow_trace(*, prefix: str, payload: dict[str, object]) -> tuple[Path, Path]:
    root = raw_path(_project_root(), "reports", "graph-embed")
    root.mkdir(parents=True, exist_ok=True)
    timestamp = __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")
    json_path = root / f"{prefix}-{timestamp}.json"
    md_path = root / f"{prefix}-{timestamp}.md"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    lines = ["# Graph Shadow Trace", ""]
    for key, value in payload.items():
        if key in {"exact_candidates", "vector_candidates", "rows"}:
            continue
        lines.append(f"- {key}: {value}")
    if "exact_candidates" in payload:
        lines.extend(["", "## Exact Candidates", ""])
        for item in payload["exact_candidates"]:
            lines.append(f"- {item['page_id']} ({item['match_kind']}, {item['score']:.2f})")
    if "vector_candidates" in payload:
        lines.extend(["", "## Shadow Vector Candidates", ""])
        for item in payload["vector_candidates"]:
            lines.append(f"- {item['page_id']} ({item['score']:.2f})")
    if "rows" in payload:
        lines.extend(["", "## Evaluation Rows", ""])
        for item in payload["rows"]:
            lines.append(
                f"- {item['query']} — expected={item['expected_page_id'] or '-'} "
                f"phase1={item['phase1_page_id'] or '-'} vector={item['vector_page_id'] or '-'}"
            )
    md_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return json_path, md_path


def cmd_graph_rebuild(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="rebuilding graph registry", default=True) as progress:
        registry = GraphRegistry.for_repo_root(_project_root())
        result = registry.rebuild(phase_callback=progress.phase)
        print(result.render())
        return 0


def cmd_graph_status(_args: argparse.Namespace) -> int:
    registry = GraphRegistry.for_repo_root(_project_root())
    status = registry.status()
    print(status.render())
    return 0


def cmd_graph_health(args: argparse.Namespace) -> int:
    health = build_graph_health(
        repo_root=_project_root(),
        include_promotion_gate=not bool(getattr(args, "skip_promotion_gate", False)),
    )
    print(health.render())
    if health.issues:
        return 1
    return 0


def cmd_graph_resolve(args: argparse.Namespace) -> int:
    registry = GraphRegistry.for_repo_root(_project_root())
    registry.ensure_built()
    candidates = registry.resolve_candidates(args.text, limit=args.limit)
    vector_candidates = []
    try:
        route = resolve_route("embedding")
        backend = select_vector_backend(Vault.load(_project_root()).vector_db)
        vector_candidates = registry.resolve_vector_candidates(
            args.text,
            embedding_service=get_embedding_service(),
            vector_backend=backend,
            model=route.model,
            limit=args.limit,
        )
    except Exception:
        vector_candidates = []
    if not candidates:
        print(f"graph-resolve: no matches for {args.text!r}")
        if vector_candidates:
            print("shadow-vector-candidates:")
            for candidate in vector_candidates:
                print(f"- {candidate.page_id}\t{candidate.primary_type}\t{candidate.score:.2f}\tshadow-vector\t{candidate.title}")
        _write_shadow_trace(
            prefix="resolve",
            payload={
                "query": args.text,
                "exact_candidates": [],
                "vector_candidates": [
                    {
                        "page_id": candidate.page_id,
                        "score": candidate.score,
                        "match_kind": candidate.match_kind,
                    }
                    for candidate in vector_candidates
                ],
            },
        )
        return 0
    print(f"graph-resolve: {args.text}")
    for candidate in candidates:
        print(
            f"- {candidate.page_id}\t{candidate.primary_type}\t{candidate.score:.2f}\t"
            f"{candidate.match_kind}\t{candidate.title}"
        )
    if vector_candidates:
        print("shadow-vector-candidates:")
        for candidate in vector_candidates:
            print(f"- {candidate.page_id}\t{candidate.primary_type}\t{candidate.score:.2f}\tshadow-vector\t{candidate.title}")
    _write_shadow_trace(
        prefix="resolve",
        payload={
            "query": args.text,
            "exact_candidates": [
                {
                    "page_id": candidate.page_id,
                    "score": candidate.score,
                    "match_kind": candidate.match_kind,
                }
                for candidate in candidates
            ],
            "vector_candidates": [
                {
                    "page_id": candidate.page_id,
                    "score": candidate.score,
                    "match_kind": candidate.match_kind,
                }
                for candidate in vector_candidates
            ],
        },
    )
    return 0


def cmd_graph_embed_rebuild(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="rebuilding graph embeddings", default=True) as progress:
        registry = GraphRegistry.for_repo_root(_project_root())
        registry.ensure_built()
        route = resolve_route("embedding")
        backend = select_vector_backend(Vault.load(_project_root()).vector_db)
        service = get_embedding_service()
        progress.phase("loading embedding targets")
        targets = registry.list_embedding_targets()
        existing = registry.list_embedding_metadata(model=route.model)
        requests = [
            EmbeddingRequest(
                target_id=target.target_id,
                target_type=target.target_type,
                content=target.content,
                content_sha256=target.content_sha256,
            )
            for target in targets
            if existing.get(target.target_id, {}).get("content_sha256") != target.content_sha256
        ]
        progress.phase("embedding changed targets")
        result = service.embed_requests(requests)
        progress.phase("writing vector index")
        backend.upsert(
            model=route.model,
            vectors={record.target_id: record.vector for record in result.records},
        )
        registry.upsert_embeddings(
            model=route.model,
            records=[
                {
                    "target_id": record.target_id,
                    "target_type": record.target_type,
                    "page_id": next(target.page_id for target in targets if target.target_id == record.target_id),
                    "content_sha256": record.content_sha256,
                    "vector_dim": record.vector_dim,
                }
                for record in result.records
            ],
        )
        valid_target_ids = {target.target_id for target in targets}
        backend.prune(model=route.model, valid_ids=valid_target_ids)
        registry.prune_embeddings(model=route.model, valid_target_ids=valid_target_ids)
        status = registry.embedding_status(model=route.model)
        backend_status = backend.status(model=route.model)
        print("graph-embed-rebuild:")
        print(f"- model={route.model}")
        print(f"- embedded={len(result.records)}")
        print(f"- total_targets={len(targets)}")
        print(f"- backend={backend_status.get('backend')}")
        print(f"- stored={status['count']}")
        return 0


def cmd_graph_embed_status(_args: argparse.Namespace) -> int:
    registry = GraphRegistry.for_repo_root(_project_root())
    route = resolve_route("embedding")
    backend = select_vector_backend(Vault.load(_project_root()).vector_db)
    status = registry.embedding_status(model=route.model)
    backend_status = backend.status(model=route.model)
    print(f"graph-embed-model: {route.model}")
    print(f"graph-embed-count: {status['count']}")
    print(f"graph-embed-last-updated: {status['last_updated'] or '-'}")
    print(f"graph-embed-vector-dim: {status['vector_dim']}")
    query_status = registry.query_embedding_status(model=route.model)
    print(f"graph-embed-query-cache-count: {query_status['count']}")
    print(f"graph-embed-query-cache-last-updated: {query_status['last_updated'] or '-'}")
    print(f"graph-embed-backend: {backend_status.get('backend')}")
    print(f"graph-embed-backend-count: {backend_status.get('count')}")
    print(f"graph-embed-backend-path: {backend_status.get('path', '-')}")
    return 0


def cmd_graph_embed_query(args: argparse.Namespace) -> int:
    registry = GraphRegistry.for_repo_root(_project_root())
    route = resolve_route("embedding")
    backend = select_vector_backend(Vault.load(_project_root()).vector_db)
    candidates = registry.resolve_vector_candidates(
        args.text,
        embedding_service=get_embedding_service(),
        vector_backend=backend,
        model=route.model,
        limit=args.limit,
    )
    print(f"graph-embed-query: {args.text}")
    if not candidates:
        print("- no vector matches")
        return 0
    for candidate in candidates:
        print(f"- {candidate.page_id}\t{candidate.primary_type}\t{candidate.score:.2f}\tvector\t{candidate.title}")
    return 0


def cmd_graph_embed_evaluate(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="evaluating graph promotion gate", default=True) as progress:
        registry = GraphRegistry.for_repo_root(_project_root())
        route = resolve_route("embedding")
        backend = select_vector_backend(Vault.load(_project_root()).vector_db)
        progress.phase("loading evaluation set")
        progress.phase("querying shadow vectors")
        gate = evaluate_promotion_gate(
            repo_root=_project_root(),
            registry=registry,
            embedding_service=get_embedding_service(),
            vector_backend=backend,
            model=route.model,
        )
        progress.phase("writing evaluation artifacts")
        print("graph-embed-evaluate:")
        print(f"- passed={gate.passed}")
        print(f"- phase1_regressions={gate.phase1_regressions}")
        print(f"- vector_false_negatives={gate.vector_false_negatives}")
        print(f"- artifact_json={gate.artifact_json_path}")
        print(f"- artifact_md={gate.artifact_markdown_path}")
        for row in gate.rows:
            print(
                f"- {row.query}\texpected={row.expected_page_id or '-'}\t"
                f"phase1={row.phase1_page_id or '-'}\tvector={row.vector_page_id or '-'}\t"
                f"phase1_ok={row.phase1_ok}\tvector_ok={row.vector_ok}"
            )
        _write_shadow_trace(
            prefix="evaluate",
            payload={
                "passed": gate.passed,
                "phase1_regressions": gate.phase1_regressions,
                "vector_false_negatives": gate.vector_false_negatives,
                "rows": [row.__dict__ for row in gate.rows],
            },
        )
        return 0 if gate.passed else 1
