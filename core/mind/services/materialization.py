"""Narrow primary-actor materialization helpers."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal

from scripts.common.slugify import normalize_identifier, slugify
from scripts.common.vault import Vault
from scripts.common.default_tags import default_domains

from .durable_write import DurableLinkTarget, write_contract_page

MaterializationRole = Literal["creator", "publisher", "tool"]
MaterializationPageType = Literal["person", "company", "channel", "tool"]

_PAGE_DIRS: dict[MaterializationPageType, str] = {
    "person": "people",
    "company": "companies",
    "channel": "channels",
    "tool": "tools",
}


@dataclass(frozen=True)
class MaterializationCandidate:
    """Primary-actor candidate evaluated at the write boundary."""

    page_type: MaterializationPageType
    name: str
    role: MaterializationRole
    confidence: float
    deterministic: bool
    source: str
    page_id: str = ""
    central_subject: bool = False

    def resolved_page_id(self) -> str:
        return normalize_identifier(self.page_id or self.name)


@dataclass(frozen=True)
class MaterializationTargets:
    """Separated creator and publisher targets for a source."""

    creator_target: MaterializationCandidate | None = None
    publisher_target: MaterializationCandidate | None = None
    tool_target: MaterializationCandidate | None = None


def _is_eligible(candidate: MaterializationCandidate) -> bool:
    if not candidate.deterministic or candidate.confidence < 0.9:
        return False
    if candidate.page_type == "tool" and not candidate.central_subject:
        return False
    return True


def _best_candidate(
    candidates: list[MaterializationCandidate],
    *,
    role: MaterializationRole,
) -> MaterializationCandidate | None:
    eligible = [candidate for candidate in candidates if candidate.role == role and _is_eligible(candidate)]
    if not eligible:
        return None
    return max(
        eligible,
        key=lambda candidate: (
            candidate.confidence,
            1 if candidate.deterministic else 0,
            1 if candidate.central_subject else 0,
            candidate.name,
        ),
    )


def select_primary_targets(
    candidates: list[MaterializationCandidate],
) -> MaterializationTargets:
    """Select the narrow Phase 2 primary targets from candidate list."""

    creator = _best_candidate(candidates, role="creator")
    publisher = _best_candidate(candidates, role="publisher")
    tool = _best_candidate(candidates, role="tool")
    return MaterializationTargets(
        creator_target=creator,
        publisher_target=publisher,
        tool_target=tool,
    )


def materialize_primary_target(
    candidate: MaterializationCandidate | None,
    *,
    repo_root: Path,
    source_link: DurableLinkTarget,
    today: str | None = None,
) -> Path | None:
    """Create a narrow, high-confidence primary-actor stub when allowed."""

    if candidate is None:
        return None
    if not _is_eligible(candidate):
        return None

    vault = Vault.load(repo_root)
    target_dir = vault.wiki / _PAGE_DIRS[candidate.page_type]
    target = target_dir / f"{candidate.resolved_page_id()}.md"
    if target.exists():
        return target

    current_day = today or date.today().isoformat()
    title = candidate.name
    if candidate.page_type == "channel":
        body = f"# {title}\n\nPrimary channel materialized from {source_link.page_id}.\n"
    elif candidate.page_type == "tool":
        body = f"# {title}\n\nPrimary tool materialized from {source_link.page_id}.\n"
    else:
        body = f"# {title}\n\nPrimary {candidate.page_type} materialized from {source_link.page_id}.\n"
    write_contract_page(
        target,
        page_type=candidate.page_type,
        title=title,
        body=body,
        created=current_day,
        last_updated=current_day,
        domains=default_domains(candidate.page_type),
        sources=[source_link],
    )
    return target
