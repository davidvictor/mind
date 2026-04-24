from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any

from scripts.atoms import cache as atoms_cache
from scripts.atoms.canonical import ATOM_TYPES, candidate_payload, canonicalize_atom_page
from scripts.atoms.pass_d import _parse_pass_d_result
from scripts.common.frontmatter import read_page
from scripts.common.slugify import normalize_identifier
from scripts.common.vault import Vault
from scripts.common.wiki_writer import write_page


@dataclass(frozen=True)
class _CachedAtomCandidate:
    atom_type: str
    atom_id: str
    payload: dict[str, Any]
    score: int
    mtime: float
    path: Path


@dataclass
class AtomPageRepairReport:
    apply: bool
    scanned_pages: int = 0
    rewritten_pages: int = 0
    cache_backed_pages: int = 0
    cache_misses: int = 0
    rebuilt_atom_cache: bool = False
    details: list[str] = field(default_factory=list)

    def render(self) -> str:
        lines = [
            f"Atom page repair ({'apply' if self.apply else 'dry-run'})",
            f"Scanned: {self.scanned_pages}",
            f"Would rewrite: {self.rewritten_pages}" if not self.apply else f"Rewritten: {self.rewritten_pages}",
            f"Cache-backed pages: {self.cache_backed_pages}",
            f"Cache misses: {self.cache_misses}",
        ]
        if self.apply:
            lines.append(f"Atom cache rebuilt: {'yes' if self.rebuilt_atom_cache else 'no'}")
        if self.details:
            lines.append("")
            lines.append("Details:")
            lines.extend(f"- {item}" for item in self.details[:50])
        return "\n".join(lines)


def run_atom_page_repair(repo_root: Path, *, apply: bool) -> AtomPageRepairReport:
    vault = Vault.load(repo_root)
    report = AtomPageRepairReport(apply=apply)
    candidate_index = _load_cached_candidates(repo_root)
    mutated = False

    for path in _iter_atom_pages(vault):
        frontmatter, body = read_page(path)
        atom_type = str(frontmatter.get("type") or "").strip()
        atom_id = normalize_identifier(str(frontmatter.get("id") or path.stem))
        if atom_type not in ATOM_TYPES or not atom_id:
            continue
        report.scanned_pages += 1
        candidate = candidate_index.get((atom_type, atom_id))
        if candidate is None:
            report.cache_misses += 1
            rendered = canonicalize_atom_page(frontmatter=frontmatter, body=body)
        else:
            report.cache_backed_pages += 1
            rendered = canonicalize_atom_page(
                frontmatter=frontmatter,
                body=body,
                candidate=candidate.payload,
            )
        current = path.read_text(encoding="utf-8")
        rewritten = f"{_serialize_frontmatter(rendered.frontmatter)}\n\n{rendered.body.rstrip()}\n"
        if rewritten == current:
            continue
        report.rewritten_pages += 1
        report.details.append(vault.logical_path(path))
        if apply:
            write_page(path, frontmatter=rendered.frontmatter, body=rendered.body, force=True)
            mutated = True

    if apply and mutated:
        atoms_cache.rebuild(repo_root)
        report.rebuilt_atom_cache = True
    return report


def _iter_atom_pages(vault: Vault) -> list[Path]:
    pages: list[Path] = []
    for dirname in ("concepts", "playbooks", "stances", "inquiries"):
        active_dir = vault.wiki / dirname
        if active_dir.exists():
            pages.extend(path for path in sorted(active_dir.glob("*.md")) if path.is_file())
        probationary_dir = vault.wiki / "inbox" / "probationary" / dirname
        if probationary_dir.exists():
            pages.extend(path for path in sorted(probationary_dir.glob("*.md")) if path.is_file())
    return pages


def _load_cached_candidates(repo_root: Path) -> dict[tuple[str, str], _CachedAtomCandidate]:
    index: dict[tuple[str, str], _CachedAtomCandidate] = {}
    root = Vault.load(repo_root).raw / "transcripts"
    if not root.exists():
        return index

    for path in sorted(root.rglob("*.pass_d.json")):
        parsed = _parse_cache_candidates(path)
        for payload in parsed:
            atom_type = str(payload.get("type") or "").strip()
            atom_id = normalize_identifier(str(payload.get("proposed_id") or ""))
            if atom_type not in ATOM_TYPES or not atom_id:
                continue
            candidate = _CachedAtomCandidate(
                atom_type=atom_type,
                atom_id=atom_id,
                payload=payload,
                score=_candidate_score(payload),
                mtime=path.stat().st_mtime,
                path=path,
            )
            key = (atom_type, atom_id)
            current = index.get(key)
            if current is None or (candidate.score, candidate.mtime) > (current.score, current.mtime):
                index[key] = candidate
    return index


def _parse_cache_candidates(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if not isinstance(data, dict):
        data = payload
    try:
        result = _parse_pass_d_result(data)
    except Exception:
        return []
    return [candidate_payload(candidate) for candidate in result.q2_candidates]


def _candidate_score(payload: dict[str, Any]) -> int:
    score = 0
    for key in ("description", "tldr", "position", "question"):
        if str(payload.get(key) or "").strip():
            score += 2
    for key in ("domains", "in_conversation_with", "steps"):
        if payload.get(key):
            score += 1
    return score


def _serialize_frontmatter(frontmatter: dict[str, Any]) -> str:
    from scripts.common.wiki_writer import _serialize_frontmatter as serialize

    return serialize(frontmatter)
