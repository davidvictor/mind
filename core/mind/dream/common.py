from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import re
from typing import Any, Iterator, Literal

from mind.runtime_state import RuntimeState, RuntimeStateLockBusy
from mind.services.onboarding import validate_onboarding_readiness, validate_onboarding_session_ready
from scripts.common.contract import atom_collection_dir, atom_collection_dirs
from scripts.common.default_tags import default_tags
from scripts.common.frontmatter import read_page, split_frontmatter, today_str
from scripts.common.vault import Vault, project_root
from scripts.common.wikilinks import extract_wikilinks
from scripts.common.wiki_writer import write_page


@dataclass
class DreamResult:
    stage: str
    dry_run: bool
    summary: str
    status: str = "completed"
    mutations: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def render(self) -> str:
        lines = [f"Dream stage: {self.stage}", f"Mode: {'dry-run' if self.dry_run else 'live'}", self.summary]
        if self.mutations:
            lines.append("")
            lines.append("Mutations:")
            lines.extend(f"- {item}" for item in self.mutations)
        if self.warnings:
            lines.append("")
            lines.append("Warnings:")
            lines.extend(f"- {item}" for item in self.warnings)
        return "\n".join(lines)


def _exception_message(exc: BaseException) -> str:
    message = str(exc).strip()
    return message or exc.__class__.__name__


class DreamPreconditionError(RuntimeError):
    """Raised when a Dream stage cannot safely execute."""


class DreamBlockedError(DreamPreconditionError):
    """Raised when a Dream stage is blocked by active runtime coordination."""


@dataclass(frozen=True)
class DreamExecutionContext:
    effective_date: str
    mode: Literal["normal", "campaign"] = "normal"
    lane_relaxation_mode: Literal["strict", "relation_only"] = "strict"
    campaign_run_id: str | None = None
    campaign_profile: str | None = None
    campaign_settings: dict[str, Any] | None = None
    campaign_resume_from_source_index: int = 0
    write_digest: bool = True
    write_rem_page: bool = True


def vault() -> Vault:
    return Vault.load(project_root())


def runtime_state() -> RuntimeState:
    return RuntimeState.for_repo_root(project_root())


def month_str() -> str:
    return datetime.now().strftime("%Y-%m")


def dream_today(context: DreamExecutionContext | None = None) -> str:
    return context.effective_date if context else today_str()


def dream_month(context: DreamExecutionContext | None = None) -> str:
    return dream_today(context)[:7]


def campaign_setting(context: DreamExecutionContext | None, key: str, default: Any) -> Any:
    if context and context.mode == "campaign" and context.campaign_settings:
        return context.campaign_settings.get(key, default)
    return default


def write_page_force(path: Path, frontmatter: dict[str, Any], body: str) -> None:
    write_page(path, frontmatter=frontmatter, body=body, force=True)


def ensure_dream_enabled() -> None:
    cfg = vault().config
    if not cfg.dream.enabled:
        raise DreamPreconditionError("dream runtime is disabled in config")


def ensure_onboarded() -> None:
    v = vault()
    readiness = validate_onboarding_session_ready(v)
    if readiness["ready"]:
        return
    if "missing current onboarding session" not in readiness["errors"]:
        raise DreamPreconditionError("run mind onboard first")
    fallback = validate_onboarding_readiness(v)
    if not fallback["ready"]:
        raise DreamPreconditionError("run mind onboard first")


@contextmanager
def dream_run(
    stage: str,
    *,
    dry_run: bool,
    context: DreamExecutionContext | None = None,
) -> Iterator[tuple[RuntimeState, int]]:
    state = runtime_state()
    metadata = {"dry_run": dry_run}
    if context is not None:
        metadata.update(
            {
                "effective_date": context.effective_date,
                "mode": context.mode,
                "campaign_run_id": context.campaign_run_id,
            }
        )
    run_id = state.create_run(kind=f"dream.{stage}", holder=f"dream-{stage}", metadata=metadata)
    state.add_run_event(run_id, stage=stage, event_type="started", message="dream stage started")
    try:
        yield state, run_id
        state.add_run_event(run_id, stage=stage, event_type="completed", message="dream stage completed")
        state.finish_run(run_id, status="completed", notes="dry-run" if dry_run else "live")
    except BaseException as exc:
        message = _exception_message(exc)
        state.add_run_event(run_id, stage=stage, event_type="failed", message=message)
        state.add_error(run_id=run_id, stage=stage, error_type=type(exc).__name__, message=message)
        status = "blocked" if isinstance(exc, DreamBlockedError) else "failed"
        state.finish_run(run_id, status=status, notes=message)
        raise


@contextmanager
def maybe_locked(stage: str, *, dry_run: bool, acquire_lock: bool = True) -> Iterator[None]:
    if dry_run:
        yield
        return
    if not acquire_lock:
        yield
        return
    state = runtime_state()
    holder = f"dream-{stage}"
    try:
        state.acquire_lock(holder=holder)
    except RuntimeStateLockBusy as exc:
        raise DreamBlockedError(str(exc)) from exc
    try:
        yield
    finally:
        state.release_lock(holder=holder)


def summary_snippet(body: str, *, max_chars: int = 160) -> str:
    lines = [line.strip() for line in body.splitlines() if line.strip() and not line.startswith("#")]
    text = " ".join(lines[:4])
    return text[:max_chars]


def section_body(body: str, heading: str) -> str:
    pattern = re.compile(rf"^## {re.escape(heading)}\s*$", re.MULTILINE)
    match = pattern.search(body)
    if not match:
        return ""
    start = match.end()
    rest = body[start:].lstrip("\n")
    next_heading = re.search(r"^## ", rest, re.MULTILINE)
    if not next_heading:
        return rest.strip()
    return rest[: next_heading.start()].strip()


def _legacy_summary_targets(frontmatter: dict) -> set[str]:
    targets: set[str] = set()
    for link in frontmatter.get("relates_to") or []:
        targets.update(extract_wikilinks(str(link)))
    return targets


def source_pages(v: Vault) -> list[Path]:
    root = v.wiki / "sources"
    pages: list[Path] = []
    canonical_ids: set[str] = set()
    if root.exists():
        pages.extend(
            page
            for page in sorted(root.rglob("*.md"))
            if page.is_file()
        )
        canonical_ids = {page.stem for page in pages}

    summary_root = v.wiki / "summaries"
    if summary_root.exists():
        for page in sorted(summary_root.glob("*.md")):
            if not page.is_file():
                continue
            frontmatter, _body = read_page(page)
            source_type = str(frontmatter.get("source_type") or "").strip().lower()
            if source_type in {"", "onboarding"}:
                continue
            if canonical_ids & _legacy_summary_targets(frontmatter):
                continue
            pages.append(page)
    return pages


def atom_pages(v: Vault) -> list[Path]:
    pages: list[Path] = []
    for dirname in atom_collection_dirs().values():
        root = v.wiki / dirname
        if root.exists():
            pages.extend(sorted(root.glob("*.md")))
    return pages


def probationary_pages(v: Vault) -> list[Path]:
    pages: list[Path] = []
    for dirname in atom_collection_dirs().values():
        root = v.wiki / "inbox" / "probationary" / dirname
        if root.exists():
            pages.extend(sorted(root.rglob("*.md")))
    return pages


def regenerate_index(v: Vault) -> Path:
    entries: list[str] = []
    for path in sorted(v.wiki.rglob("*.md")):
        rel = path.relative_to(v.wiki)
        if rel.parts and rel.parts[0] in {"templates", "inbox", ".archive"}:
            continue
        if path.name in {"INDEX.md", "CHANGELOG.md", ".brain-state.json"}:
            continue
        entries.append(path.stem)
    lines = ["# INDEX", ""] + [f"- [[{entry}]]" for entry in entries]
    v.index.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return v.index


def regenerate_open_inquiries(v: Vault, *, context: DreamExecutionContext | None = None) -> Path:
    target = v.wiki / "me" / "open-inquiries.md"
    active: list[str] = []
    resolved: list[str] = []
    for path in sorted((v.wiki / atom_collection_dir("inquiry")).glob("*.md")):
        fm, _body = read_page(path)
        if str(fm.get("status") or "active") == "resolved":
            resolved.append(path.stem)
        else:
            active.append(path.stem)
    frontmatter = {
        "id": "open-inquiries",
        "type": "note",
        "title": "Open Inquiries",
        "status": "active",
        "created": dream_today(context),
        "last_updated": dream_today(context),
        "aliases": [],
        "tags": ["domain/meta", "function/note", "signal/working"],
        "domains": ["meta"],
        "relates_to": [f"[[{item}]]" for item in active[:5]],
        "sources": [],
    }
    body_lines = ["# Open Inquiries", "", "## Active inquiries", ""]
    body_lines.extend(f"- [[{item}]]" for item in active)
    body_lines.extend(["", "## Resolved inquiries", ""])
    body_lines.extend(f"- [[{item}]]" for item in resolved)
    write_page_force(target, frontmatter, "\n".join(body_lines))
    return target


def append_month_entry(
    path: Path,
    heading: str,
    content: str,
    *,
    page_title: str,
    context: DreamExecutionContext | None = None,
) -> None:
    if path.exists():
        frontmatter, body = read_page(path)
    else:
        frontmatter = {
            "id": path.stem,
            "type": "note",
            "title": page_title,
            "status": "active",
            "created": dream_today(context),
            "last_updated": dream_today(context),
            "aliases": [],
            "tags": ["domain/identity", "function/note", "signal/working"],
            "domains": ["identity"],
            "relates_to": ["[[profile]]"],
            "sources": [],
        }
        body = f"# {page_title}\n"
    if heading in body and content.strip() in body:
        return
    frontmatter["last_updated"] = dream_today(context)
    if not body.endswith("\n"):
        body += "\n"
    body += f"\n### {heading}\n\n{content.strip()}\n"
    write_page_force(path, frontmatter, body.rstrip() + "\n")


def write_note_page(
    target: Path,
    *,
    page_type: str,
    title: str,
    body: str,
    domains: list[str],
    sources: list[str] | None = None,
    extra_frontmatter: dict[str, Any] | None = None,
    force: bool = False,
    context: DreamExecutionContext | None = None,
) -> Path:
    frontmatter = {
        "id": target.stem,
        "type": page_type,
        "title": title,
        "status": "active",
        "created": dream_today(context),
        "last_updated": dream_today(context),
        "aliases": [],
        "tags": default_tags(page_type),
        "domains": domains,
        "relates_to": [],
        "sources": sources or [],
    }
    if extra_frontmatter:
        frontmatter.update(extra_frontmatter)
    write_page(target, frontmatter=frontmatter, body=body, force=force)
    return target
