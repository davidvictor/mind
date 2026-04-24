"""Idempotent appender to atom evidence logs.

Appends a single line to an atom's `## Evidence log` section in the locked
format from the dream spec:

  - YYYY-MM-DD — [[source-page]] — snippet (≤160 chars)

Idempotent on (date, source_link). Updates last_evidence_date and
last_dream_pass frontmatter fields. Writes contradiction nudges when
polarity is 'against'.

Phase A: STUB. Implementation lands in Phase C.

Historical design notes are kept outside the public release tree.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re

from mind.services.ingest_contract import EvidenceEdge
from scripts.atoms.canonical import canonicalize_atom_page
from scripts.atoms.types import AtomType, Confidence, EvidenceStrength, Polarity, RelationKind
from scripts.common.contract import atom_collection_dir, canonicalize_page_type
from scripts.common.frontmatter import split_frontmatter as _split_frontmatter
from scripts.common.slugify import normalize_identifier
from scripts.common.vault import Vault
from scripts.common.wiki_writer import write_page


def _find_atom_path(repo_root: Path, atom_type: str, atom_id: str) -> Path:
    canonical = canonicalize_page_type(atom_type)
    atom_id = normalize_identifier(atom_id)
    wiki_root = Vault.load(repo_root).wiki
    try:
        dirname = atom_collection_dir(canonical)
    except KeyError as exc:
        raise FileNotFoundError(atom_id) from exc
    active = wiki_root / dirname / f"{atom_id}.md"
    if active.exists():
        return active
    probationary_dir = wiki_root / "inbox" / "probationary" / dirname
    if probationary_dir.exists():
        matches = sorted(probationary_dir.glob(f"*-{atom_id}.md"))
        if matches:
            return matches[0]
        for candidate in sorted(probationary_dir.glob("*.md")):
            frontmatter, _body = _split_frontmatter(candidate.read_text(encoding="utf-8"))
            if normalize_identifier(str(frontmatter.get("id") or candidate.stem)) == atom_id:
                return candidate
    for candidate in sorted((wiki_root / dirname).glob("*.md")):
        frontmatter, _body = _split_frontmatter(candidate.read_text(encoding="utf-8"))
        if normalize_identifier(str(frontmatter.get("id") or candidate.stem)) == atom_id:
            return candidate
    raise FileNotFoundError(atom_id)


def _write_nudge(repo_root: Path, *, atom_id: str, date: str, source_link: str, snippet: str) -> None:
    target = Vault.load(repo_root).wiki / "inbox" / "nudges" / f"{date}-contradiction-{atom_id}.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    frontmatter = {
        "id": target.stem,
        "type": "note",
        "title": f"Contradiction nudge: {atom_id}",
        "status": "active",
        "created": date,
        "last_updated": date,
        "aliases": [],
        "tags": ["domain/meta", "function/note", "signal/working"],
        "domains": ["meta"],
        "relates_to": [source_link, f"[[{atom_id}]]"],
        "sources": [source_link],
    }
    body = (
        f"# Contradiction nudge: {atom_id}\n\n"
        f"## Contradictions detected\n\n"
        f"- [[{atom_id}]] vs {source_link} — {snippet}\n"
    )
    write_page(target, frontmatter=frontmatter, body=body, force=True)


def _evidence_entry_exists(body: str, *, date: str, source_link: str) -> bool:
    if "## Evidence log" not in body:
        return False
    evidence = body.split("## Evidence log", 1)[1]
    prefix = f"- {date} — {source_link} — "
    return any(line.strip().startswith(prefix) for line in evidence.splitlines())


def _evidence_source_exists(body: str, *, source_link: str) -> bool:
    if "## Evidence log" not in body:
        return False
    evidence = body.split("## Evidence log", 1)[1]
    marker = f"— {source_link} — "
    return any(marker in line for line in evidence.splitlines())


def _source_id_from_link(source_link: str) -> str:
    match = re.search(r"\[\[([^\]|#]+)", source_link)
    if match:
        return normalize_identifier(match.group(1))
    return normalize_identifier(source_link) or "unknown-source"


def _edge_hash(payload: dict[str, object]) -> str:
    material = json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]


def _append_evidence_edge(
    *,
    repo_root: Path,
    edge: EvidenceEdge,
) -> None:
    source_kind = normalize_identifier(edge.source_kind or "unknown")
    source_id = normalize_identifier(edge.source_id or "unknown-source")
    target = Vault.load(repo_root).raw / "evidence-edges" / source_kind / f"{source_id}.jsonl"
    payload = edge.to_dict()
    payload_without_id = {key: value for key, value in payload.items() if key != "edge_id"}
    payload["edge_id"] = edge.edge_id or _edge_hash(payload_without_id)
    if target.exists():
        marker = f'"edge_id": "{payload["edge_id"]}"'
        if marker in target.read_text(encoding="utf-8"):
            return
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, ensure_ascii=True) + "\n")


def append_evidence(
    *,
    atom_id: str,
    atom_type: AtomType,
    date: str,
    dedupe_by_source: bool = False,
    recorded_on: str | None = None,
    source_link: str,
    snippet: str,
    polarity: Polarity,
    confidence: Confidence = "medium",
    evidence_strength: EvidenceStrength = "anecdotal",
    relation_kind: RelationKind | None = None,
    source_id: str | None = None,
    source_kind: str = "unknown",
    source_date: str | None = None,
    creator_id: str = "",
    quote_verified: bool = False,
    source_section: str = "",
    source_span: str = "",
    topics: list[str] | None = None,
    entities: list[str] | None = None,
    discovered_via: str = "",
    repo_root: Path,
) -> bool:
    """Append an entry to the atom's ## Evidence log section.

    Idempotent: if an entry with the same (date, source_link) tuple already
    exists, returns False without modifying the file. When `dedupe_by_source`
    is enabled, any existing entry for the same `source_link` is treated as a
    duplicate even if the date differs. Otherwise appends and returns True.

    Also updates the atom's frontmatter:
      - last_evidence_date = date
      - last_dream_pass = today

    Resolves the atom file path by checking wiki/<type>s/<id>.md first,
    then wiki/inbox/probationary/<type>/*-<id>.md.

    Raises FileNotFoundError if the atom doesn't exist.

    If polarity == "against", also writes a contradiction nudge to
    wiki/inbox/nudges/<date>-contradiction-<atom-id>.md.

    Phase A stub.
    """
    canonical = canonicalize_page_type(atom_type)
    path = _find_atom_path(repo_root, canonical, atom_id)
    fm, body = _split_frontmatter(path.read_text(encoding="utf-8"))
    execution_date = recorded_on or date
    entry = f"- {date} — {source_link} — {snippet.strip()[:160]}"
    if (dedupe_by_source and _evidence_source_exists(body, source_link=source_link)) or entry in body:
        return False
    evidence_count = int(fm.get("evidence_count") or 0) + 1
    fm["last_evidence_date"] = date
    fm["last_dream_pass"] = execution_date
    fm["evidence_count"] = evidence_count
    if canonical == "stance":
        if polarity == "for":
            fm["evidence_for_count"] = int(fm.get("evidence_for_count") or 0) + 1
        elif polarity == "against":
            fm["evidence_against_count"] = int(fm.get("evidence_against_count") or 0) + 1
    if "## Evidence log" in body:
        body = re.sub(r"(## Evidence log\s*\n)", r"\1\n" + entry + "\n", body, count=1)
    else:
        if not body.endswith("\n"):
            body += "\n"
        body += f"\n## Evidence log\n\n{entry}\n"
    rendered = canonicalize_atom_page(frontmatter=fm, body=body)
    write_page(path, frontmatter=rendered.frontmatter, body=rendered.body, force=True)
    edge_relation_kind = relation_kind or ("contradicts" if polarity == "against" else "supports" if polarity == "for" else "adjacent_to")
    _append_evidence_edge(
        repo_root=repo_root,
        edge=EvidenceEdge(
            source_id=source_id or _source_id_from_link(source_link),
            source_kind=source_kind,
            source_date=source_date or date,
            creator_id=creator_id,
            atom_id=normalize_identifier(atom_id),
            atom_type=canonical,
            polarity=polarity,
            confidence=confidence,
            evidence_strength=evidence_strength,
            relation_kind=edge_relation_kind,
            snippet=snippet.strip()[:500],
            quote_verified=quote_verified,
            source_section=source_section,
            source_span=source_span,
            topics=list(topics or []),
            entities=list(entities or []),
            discovered_via=discovered_via,
        ),
    )
    if polarity == "against":
        _write_nudge(
            repo_root,
            atom_id=atom_id,
            date=execution_date,
            source_link=source_link,
            snippet=snippet,
        )
    return True
