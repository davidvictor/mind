from __future__ import annotations

import json
from pathlib import Path

from mind.services.reingest import run_article_repair
from tests.support import write_repo_config


def _write_config(root: Path) -> None:
    write_repo_config(root, create_indexes=True)


def _seed_drop_file(repo_root: Path) -> Path:
    drop = repo_root / "raw" / "drops" / "articles-from-substack-2026-04-12.jsonl"
    drop.parent.mkdir(parents=True, exist_ok=True)
    drop.write_text(
        json.dumps(
            {
                "url": "https://example.com/article",
                "source_post_id": "1",
                "source_post_url": "https://example.com/post",
                "anchor_text": "Example article",
                "context_snippet": "ctx",
                "category": "business",
                "discovered_at": "2026-04-12T00:00:00Z",
                "source_type": "substack-link",
                "source_label": "substack",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return drop


def _append_drop_entry(repo_root: Path, payload: dict[str, object]) -> None:
    drop = repo_root / "raw" / "drops" / "articles-from-substack-2026-04-12.jsonl"
    drop.parent.mkdir(parents=True, exist_ok=True)
    with drop.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


def test_article_repair_plans_acquisition_refresh_when_fetch_cache_missing(tmp_path: Path):
    _write_config(tmp_path)
    _seed_drop_file(tmp_path)

    result = run_article_repair(repo_root=tmp_path, apply=False)

    assert result.plan.reacquire_count == 1
    assert result.plan.items[0].action == "refresh_acquisition"
    assert result.plan.items[0].start_stage == "acquire"


def test_article_repair_plans_downstream_recompute_when_fetch_cache_exists(tmp_path: Path):
    _write_config(tmp_path)
    _seed_drop_file(tmp_path)
    cache_root = tmp_path / "raw" / "transcripts" / "articles"
    cache_root.mkdir(parents=True, exist_ok=True)
    (cache_root / "2026-04-12-example-com-article.html").write_text("body", encoding="utf-8")
    (cache_root / "2026-04-12-example-com-article.meta.json").write_text(
        json.dumps({"title": "Example", "author": "Author", "sitename": "Site", "published": "2026-04-12"}),
        encoding="utf-8",
    )

    result = run_article_repair(repo_root=tmp_path, apply=False)

    assert result.plan.recompute_count == 1
    assert result.plan.items[0].action == "recompute_downstream"
    assert result.plan.items[0].start_stage == "pass_a"


def test_article_repair_respects_source_id_filter(tmp_path: Path):
    _write_config(tmp_path)
    _seed_drop_file(tmp_path)
    _append_drop_entry(
        tmp_path,
        {
            "url": "https://other.example.com/second-article",
            "source_post_id": "2",
            "source_post_url": "https://example.com/post-2",
            "anchor_text": "Second article",
            "context_snippet": "ctx",
            "category": "business",
            "discovered_at": "2026-04-12T00:00:00Z",
            "source_type": "substack-link",
            "source_label": "substack",
        },
    )

    result = run_article_repair(
        repo_root=tmp_path,
        apply=False,
        source_ids=("article-2026-04-12-example-com-article",),
    )

    assert len(result.plan.items) == 1
    assert result.plan.items[0].source_id == "article-2026-04-12-example-com-article"


def test_article_repair_treats_unsupported_article_links_as_ready_exclusions(tmp_path: Path):
    _write_config(tmp_path)
    _append_drop_entry(
        tmp_path,
        {
            "url": "https://agent.minimax.io/",
            "source_post_id": "source",
            "source_post_url": "https://example.com/source",
            "anchor_text": "MiniMax Agent",
            "context_snippet": "app landing page",
            "category": "business",
            "discovered_at": "2026-04-12T00:00:00Z",
            "source_type": "article-link",
            "source_label": "article-link",
        },
    )

    result = run_article_repair(repo_root=tmp_path, apply=False)

    assert result.plan.ready_count == 1
    assert result.plan.blocked_count == 0
    assert result.plan.items[0].action == "ready"
    assert result.plan.items[0].detail == "unsupported URL is intentionally excluded from article repair"
