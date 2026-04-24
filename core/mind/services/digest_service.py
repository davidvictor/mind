from __future__ import annotations

from pathlib import Path

from mind.dream.common import DreamExecutionContext, append_month_entry, dream_today
from scripts.common.vault import Vault


def write_digest_snapshot(
    repo_root: Path,
    *,
    promotions: int = 0,
    merges: int = 0,
    relation_updates: int = 0,
    contradictions: int = 0,
    polarity_reviews: int = 0,
    today: str | None = None,
    context: DreamExecutionContext | None = None,
) -> Path:
    v = Vault.load(repo_root)
    day = today or dream_today(context)
    digest_path = v.wiki / "me" / "digests" / f"{day}.md"
    append_month_entry(
        digest_path,
        heading=day,
        content=(
            f"Promoted: {promotions}\n\nMerged: {merges}\n\nRelation updates: {relation_updates}\n\n"
            f"Contradictions resolved: {contradictions}\n\nPolarity reviews: {polarity_reviews}"
        ),
        page_title="Weekly Digest",
        context=context,
    )
    return digest_path
