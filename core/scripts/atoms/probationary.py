"""Probationary atom file creator/extender.

Creates new probationary atom files in wiki/inbox/probationary/<type>/
or appends to existing files with the same proposed_id.

Phase A: STUB. Implementation lands in Phase C.

Historical design notes are kept outside the public release tree.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from scripts.atoms.canonical import canonicalize_atom_page
from scripts.atoms.types import AtomType, Polarity
from scripts.common.contract import atom_collection_dir, canonicalize_page_type
from scripts.common.default_tags import default_tags
from scripts.common.slugify import normalize_identifier
from scripts.common.vault import Vault
from scripts.common.wiki_writer import write_page

from .evidence_writer import append_evidence


def _frontmatter_for(
    type_name: str,
    proposed_id: str,
    title: str,
    date: str,
    source_link: str,
    *,
    domains: list[str] | None = None,
    in_conversation_with: list[str] | None = None,
) -> dict[str, Any]:
    relates_to = [f"[[{item}]]" for item in (in_conversation_with or []) if str(item).strip()]
    default_domains = list(domains or ["meta"])
    if type_name == "concept":
        return {
            "id": proposed_id,
            "type": "concept",
            "title": title,
            "status": "active",
            "created": date,
            "last_updated": date,
            "aliases": [],
            "tags": default_tags("concept"),
            "domains": default_domains,
            "relates_to": relates_to,
            "sources": [source_link],
            "lifecycle_state": "probationary",
            "last_evidence_date": date,
            "evidence_count": 0,
            "category": "",
            "first_encountered": date,
            "last_dream_pass": date,
        }
    if type_name == "playbook":
        return {
            "id": proposed_id,
            "type": "playbook",
            "title": title,
            "status": "active",
            "created": date,
            "last_updated": date,
            "aliases": [],
            "tags": default_tags("playbook"),
            "domains": default_domains,
            "relates_to": relates_to,
            "sources": [source_link],
            "derived_from": [],
            "applied_by_owner": False,
            "lifecycle_state": "probationary",
            "last_evidence_date": date,
            "evidence_count": 0,
            "last_dream_pass": date,
        }
    if type_name == "stance":
        return {
            "id": proposed_id,
            "type": "stance",
            "title": title,
            "status": "active",
            "created": date,
            "last_updated": date,
            "aliases": [],
            "tags": default_tags("stance"),
            "domains": default_domains,
            "relates_to": relates_to,
            "sources": [source_link],
            "position": "",
            "confidence": "probationary",
            "evidence_for_count": 0,
            "evidence_against_count": 0,
            "owner_alignment": "unknown",
            "lifecycle_state": "probationary",
            "last_evidence_date": date,
            "last_dream_pass": date,
        }
    return {
        "id": proposed_id,
        "type": "inquiry",
        "title": title,
        "status": "active",
        "created": date,
        "last_updated": date,
        "aliases": [],
        "tags": default_tags("inquiry"),
        "domains": default_domains,
        "relates_to": relates_to,
        "sources": [source_link],
        "question": title,
        "origin": "extracted",
        "resolution": None,
        "sources_pro": [source_link],
        "sources_con": [],
        "last_evidence_date": date,
        "last_dream_pass": date,
        "lifecycle_state": "probationary",
        "evidence_count": 0,
    }


def create_or_extend(
    *,
    type: AtomType,
    proposed_id: str,
    title: str,
    description: str,
    tldr: str | None = None,
    snippet: str,
    polarity: Polarity,
    rationale: str,
    domains: list[str] | None = None,
    in_conversation_with: list[str] | None = None,
    steps: list[str] | None = None,
    position: str | None = None,
    question: str | None = None,
    date: str,
    dedupe_by_source: bool = False,
    recorded_on: str | None = None,
    source_link: str,
    repo_root: Path,
) -> Path:
    """Create or extend a probationary atom file.

    Creates wiki/inbox/probationary/<type>/<date>-<proposed_id>.md, or
    appends to an existing file with the same proposed_id.

    Idempotent: if the file already has an entry with the same
    (date, source_link), this function is a no-op.

    Returns the path to the created or extended file.

    Phase A stub.
    """
    canonical = canonicalize_page_type(type)
    proposed_id = normalize_identifier(proposed_id)
    execution_date = recorded_on or date
    target_dir = Vault.load(repo_root).wiki / "inbox" / "probationary" / atom_collection_dir(canonical)
    target_dir.mkdir(parents=True, exist_ok=True)
    existing = sorted(target_dir.glob(f"*-{proposed_id}.md"))
    if existing:
        append_evidence(
            atom_id=proposed_id,
            atom_type=canonical,  # type: ignore[arg-type]
            date=date,
            dedupe_by_source=dedupe_by_source,
            recorded_on=execution_date,
            source_link=source_link,
            snippet=snippet,
            polarity=polarity,
            repo_root=repo_root,
        )
        return existing[0]
    target = target_dir / f"{date}-{proposed_id}.md"
    frontmatter = _frontmatter_for(
        canonical,
        proposed_id,
        title,
        date,
        source_link,
        domains=domains,
        in_conversation_with=in_conversation_with,
    )
    rendered = canonicalize_atom_page(
        frontmatter=frontmatter,
        body="",
        candidate={
            "type": canonical,
            "proposed_id": proposed_id,
            "title": title,
            "description": description,
            "tldr": tldr or "",
            "domains": domains or [],
            "in_conversation_with": in_conversation_with or [],
            "steps": steps or [],
            "position": position or "",
            "question": question or "",
        },
    )
    write_page(target, frontmatter=rendered.frontmatter, body=rendered.body, force=False)
    append_evidence(
        atom_id=proposed_id,
        atom_type=canonical,  # type: ignore[arg-type]
        date=date,
        dedupe_by_source=dedupe_by_source,
        recorded_on=execution_date,
        source_link=source_link,
        snippet=snippet,
        polarity=polarity,
        repo_root=repo_root,
    )
    return target
