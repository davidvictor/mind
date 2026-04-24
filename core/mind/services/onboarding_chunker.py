"""Deterministic chunking and assembly helpers for onboarding synthesis."""
from __future__ import annotations

import hashlib
from typing import Any


CHUNK_SIZE = 8


def chunk_graph_entities(semantic_artifact: dict[str, Any], *, chunk_size: int = CHUNK_SIZE) -> list[dict[str, Any]]:
    entities = list(semantic_artifact.get("entities") or [])
    relationships = list(semantic_artifact.get("relationships") or [])
    grouped: dict[str, list[dict[str, Any]]] = {}
    for entity in entities:
        grouped.setdefault(str(entity.get("family") or "unknown"), []).append(entity)
    chunks: list[dict[str, Any]] = []
    for family in sorted(grouped):
        family_entities = sorted(grouped[family], key=lambda item: str(item.get("proposal_id") or ""))
        for start in range(0, len(family_entities), chunk_size):
            chunk_entities = family_entities[start:start + chunk_size]
            proposal_ids = [str(item.get("proposal_id") or "") for item in chunk_entities]
            chunk_refs = {"owner", *proposal_ids}
            chunk_relationships = [
                relationship
                for relationship in relationships
                if str(relationship.get("source_ref") or "") in chunk_refs
                or str(relationship.get("target_ref") or "") in chunk_refs
            ]
            chunks.append(
                {
                    "phase": "graph_nodes",
                    "family": family,
                    "chunk_id": _stable_chunk_id(prefix=f"graph-{family}", ids=proposal_ids),
                    "proposal_ids": proposal_ids,
                    "entities": chunk_entities,
                    "relationships": chunk_relationships,
                }
            )
    return chunks


def assemble_graph_chunks(bundle_id: str, chunk_payloads: list[dict[str, Any]]) -> dict[str, Any]:
    node_map: dict[str, dict[str, Any]] = {}
    notes: list[str] = []
    edge_map: dict[tuple[str, str, str], dict[str, Any]] = {}
    for payload in chunk_payloads:
        notes.extend(list(payload.get("notes") or []))
        for node in payload.get("node_proposals") or []:
            proposal_id = str(node.get("proposal_id") or "")
            if not proposal_id:
                continue
            existing = node_map.get(proposal_id)
            if existing is not None:
                notes.append(f"duplicate graph proposal for {proposal_id}; richer chunk won")
                node_map[proposal_id] = _prefer_richer_node(existing, node)
            else:
                node_map[proposal_id] = node
        for edge in payload.get("edge_proposals") or []:
            key = (
                str(edge.get("source_ref") or ""),
                str(edge.get("target_ref") or ""),
                str(edge.get("relation_type") or ""),
            )
            edge_map[key] = edge
    return {
        "bundle_id": bundle_id,
        "node_proposals": [node_map[key] for key in sorted(node_map)],
        "edge_proposals": [edge_map[key] for key in sorted(edge_map)],
        "notes": notes,
    }


def _prefer_richer_node(existing: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    return candidate if _node_richness(candidate) > _node_richness(existing) else existing


def _node_richness(node: dict[str, Any]) -> int:
    score = 0
    for key in ("summary", "title", "page_type", "slug"):
        if str(node.get(key) or "").strip():
            score += 1
    score += len(list(node.get("aliases") or []))
    score += len(list(node.get("domains") or []))
    score += len(list(node.get("evidence_refs") or []))
    score += len(list(node.get("relates_to_refs") or []))
    score += len(dict(node.get("attributes") or {}))
    return score


def chunk_merge_nodes(
    graph_artifact: dict[str, Any],
    candidate_context: dict[str, Any],
    *,
    chunk_size: int = CHUNK_SIZE,
) -> list[dict[str, Any]]:
    node_proposals = list(graph_artifact.get("node_proposals") or [])
    candidates = {str(item.get("proposal_id") or ""): item for item in candidate_context.get("candidates") or []}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for node in node_proposals:
        grouped.setdefault(str(node.get("page_type") or "unknown"), []).append(node)
    chunks: list[dict[str, Any]] = []
    for page_type in sorted(grouped):
        page_nodes = sorted(grouped[page_type], key=lambda item: str(item.get("proposal_id") or ""))
        for start in range(0, len(page_nodes), chunk_size):
            chunk_nodes = page_nodes[start:start + chunk_size]
            proposal_ids = [str(item.get("proposal_id") or "") for item in chunk_nodes]
            chunks.append(
                {
                    "phase": "merge_nodes",
                    "page_type": page_type,
                    "chunk_id": _stable_chunk_id(prefix=f"merge-{page_type}", ids=proposal_ids),
                    "proposal_ids": proposal_ids,
                    "node_proposals": chunk_nodes,
                    "candidates": [candidates[proposal_id] for proposal_id in proposal_ids if proposal_id in candidates],
                }
            )
    return chunks


def assemble_merge_chunks(
    bundle_id: str,
    node_payloads: list[dict[str, Any]],
    relationship_decisions: list[dict[str, Any]],
    *,
    graph_artifact: dict[str, Any],
) -> dict[str, Any]:
    decision_map: dict[str, dict[str, Any]] = {}
    notes: list[str] = []
    warnings: list[str] = []
    node_proposals = {str(node.get("proposal_id") or ""): node for node in graph_artifact.get("node_proposals") or []}
    for payload in node_payloads:
        notes.extend(list(payload.get("notes") or []))
        for decision in payload.get("decisions") or []:
            proposal_id = str(decision.get("proposal_id") or "")
            if proposal_id in decision_map:
                warnings.append(f"duplicate merge decision for {proposal_id}; last chunk won")
            _validate_denormalized_merge_decision(decision, node_proposals)
            decision_map[proposal_id] = decision
    if warnings:
        notes.extend(warnings)
    return {
        "bundle_id": bundle_id,
        "decisions": [decision_map[key] for key in sorted(decision_map)],
        "relationship_decisions": list(relationship_decisions),
        "notes": notes,
    }


def kept_nodes_for_relationships(graph_artifact: dict[str, Any], merge_decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    decisions = {str(item.get("proposal_id") or ""): item for item in merge_decisions}
    kept: list[dict[str, Any]] = []
    for node in graph_artifact.get("node_proposals") or []:
        proposal_id = str(node.get("proposal_id") or "")
        if proposal_id in decisions and str(decisions[proposal_id].get("action") or "") in {"create", "update", "merge"}:
            kept.append(node)
    return kept


def relationship_edges_for_kept_nodes(graph_artifact: dict[str, Any], merge_decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    kept_refs = {str(item.get("proposal_id") or "") for item in kept_nodes_for_relationships(graph_artifact, merge_decisions)}
    return [
        edge
        for edge in graph_artifact.get("edge_proposals") or []
        if str(edge.get("source_ref") or "") in kept_refs and str(edge.get("target_ref") or "") in kept_refs
    ]


def _stable_chunk_id(*, prefix: str, ids: list[str]) -> str:
    digest = hashlib.sha256("\n".join(sorted(ids)).encode("utf-8")).hexdigest()[:8]
    return f"{prefix}-{digest}"


def _validate_denormalized_merge_decision(decision: dict[str, Any], node_proposals: dict[str, dict[str, Any]]) -> None:
    proposal_id = str(decision.get("proposal_id") or "")
    node = node_proposals.get(proposal_id)
    if not node:
        raise RuntimeError(f"missing source graph proposal for merge decision {proposal_id}")
    expected = {
        "title": str(node.get("title") or ""),
        "slug": str(node.get("slug") or ""),
        "page_type": str(node.get("page_type") or ""),
    }
    for key, value in expected.items():
        actual = str(decision.get(key) or "")
        if actual != value:
            raise RuntimeError(f"merge decision {proposal_id} drifted from source proposal field {key}")
