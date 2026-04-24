from __future__ import annotations

import json
from pathlib import Path

from mind.services.reingest import ReingestRequest, run_reingest
from mind.services.rebuild_manifest import build_rebuild_manifest, load_rebuild_manifest, write_rebuild_manifest
from tests.support import write_repo_config


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_build_rebuild_manifest_collects_canonical_source_ids(tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    _write(
        tmp_path / "memory" / "sources" / "books" / "business" / "martin-kleppmann-designing-data-intensive-applications.md",
        "---\nid: martin-kleppmann-designing-data-intensive-applications\ntype: book\ntitle: DDIA\nexternal_id: audible-123\n---\n",
    )
    _write(
        tmp_path / "memory" / "sources" / "youtube" / "business" / "test-video.md",
        "---\nid: test-video\ntype: video\ntitle: Test Video\nexternal_id: youtube-abc123xyz00\nyoutube_id: abc123xyz00\n---\n",
    )
    _write(
        tmp_path / "memory" / "sources" / "substack" / "thegeneralist" / "2026-03-15-on-trust.md",
        "---\nid: 2026-03-15-on-trust\ntype: article\ntitle: On Trust\nexternal_id: substack-190000001\n---\n",
    )
    _write(
        tmp_path / "memory" / "sources" / "articles" / "2026-04-02-stratechery-com-aggregators.md",
        "---\nid: 2026-04-02-stratechery-com-aggregators\ntype: article\ntitle: Aggregators\n---\n",
    )

    manifest = build_rebuild_manifest(tmp_path)

    assert [item.source_id for item in manifest.lanes["books"]] == ["book-martin-kleppmann-designing-data-intensive-applications"]
    assert [item.source_id for item in manifest.lanes["youtube"]] == ["youtube-abc123xyz00"]
    assert [item.source_id for item in manifest.lanes["substack"]] == ["substack-190000001"]
    assert [item.source_id for item in manifest.lanes["articles"]] == ["article-2026-04-02-stratechery-com-aggregators"]


def test_write_and_load_rebuild_manifest_round_trip(tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    _write(
        tmp_path / "memory" / "sources" / "youtube" / "business" / "test-video.md",
        "---\nid: test-video\ntype: video\ntitle: Test Video\nexternal_id: youtube-abc123xyz00\nyoutube_id: abc123xyz00\n---\n",
    )
    output = tmp_path / "raw" / "reports" / "ingest-rebuild-manifest.json"

    manifest = write_rebuild_manifest(repo_root=tmp_path, output_path=output)

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["generated_at"] == manifest.generated_at
    assert load_rebuild_manifest(output, lane="youtube") == ("youtube-abc123xyz00",)


def test_reingest_manifest_selection_dedupes_duplicate_article_inventory(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_exports=True)
    _write(
        tmp_path / "memory" / "sources" / "articles" / "2026-04-02-stratechery-com-2024-aggregators.md",
        "---\nid: 2026-04-02-stratechery-com-2024-aggregators\ntype: article\ntitle: Aggregators\n---\n",
    )
    drops = tmp_path / "raw" / "drops"
    drops.mkdir(parents=True, exist_ok=True)
    duplicate_line = '{"url":"https://stratechery.com/2024/aggregators","source_post_id":"1","source_post_url":"https://example.com/source","anchor_text":"aggregators","context_snippet":"ctx","category":"business","discovered_at":"2026-04-02T00:00:00Z","source_type":"substack-link"}\n'
    (drops / "articles-from-substack-2026-04-09.jsonl").write_text(duplicate_line + duplicate_line, encoding="utf-8")
    manifest_path = tmp_path / "raw" / "reports" / "ingest-rebuild-manifest.json"
    write_rebuild_manifest(repo_root=tmp_path, output_path=manifest_path)

    result = run_reingest(
        ReingestRequest(
            lane="articles",
            source_ids=load_rebuild_manifest(manifest_path, lane="articles"),
            dry_run=True,
        ),
        repo_root=tmp_path,
    )

    assert result.plan.selected_count == 1
