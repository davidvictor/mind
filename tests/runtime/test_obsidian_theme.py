from __future__ import annotations

import json
from pathlib import Path

from mind.services.obsidian_theme import apply_obsidian_theme
from tests.support import write_repo_config


def _prepare_root(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    write_repo_config(root)
    return root


def test_apply_obsidian_theme_generates_managed_files_and_preserves_unrelated_appearance_keys(tmp_path: Path) -> None:
    root = _prepare_root(tmp_path)
    appearance = root / "memory" / ".obsidian" / "appearance.json"
    appearance.parent.mkdir(parents=True, exist_ok=True)
    appearance.write_text(
        json.dumps(
            {
                "baseFontSize": 17,
                "cssTheme": "Minimal",
                "enabledCssSnippets": ["custom-snippet"],
            }
        ),
        encoding="utf-8",
    )

    result = apply_obsidian_theme(root)

    assert result.changed_paths == [
        "memory/.obsidian/appearance.json",
        "memory/.obsidian/graph.json",
        "memory/.obsidian/snippets/brain-kanagawa.css",
    ]

    appearance_payload = json.loads(appearance.read_text(encoding="utf-8"))
    assert appearance_payload["baseFontSize"] == 17
    assert appearance_payload["cssTheme"] == ""
    assert appearance_payload["enabledCssSnippets"] == ["custom-snippet", "brain-kanagawa"]

    snippet_text = (root / "memory" / ".obsidian" / "snippets" / "brain-kanagawa.css").read_text(encoding="utf-8")
    assert ".theme-dark {" in snippet_text
    assert ".theme-light {" in snippet_text
    assert "--brain-background: #181616;" in snippet_text
    assert "--brain-background: #f2ecbc;" in snippet_text


def test_apply_obsidian_theme_rewrites_graph_with_memory_queries(tmp_path: Path) -> None:
    root = _prepare_root(tmp_path)

    apply_obsidian_theme(root)
    graph = json.loads((root / "memory" / ".obsidian" / "graph.json").read_text(encoding="utf-8"))
    base_filter = "-path:summaries"

    assert graph["search"] == base_filter
    assert graph["showTags"] is False
    queries = [group["query"] for group in graph["colorGroups"]]
    assert queries == [
        f"{base_filter} path:me",
        f"{base_filter} path:projects",
        f"{base_filter} path:people",
        f"{base_filter} path:companies",
        f"{base_filter} path:channels",
        f"{base_filter} path:concepts",
        f"{base_filter} path:playbooks",
        f"{base_filter} path:stances",
        f"{base_filter} path:inquiries",
        f"{base_filter} path:decisions",
        f"{base_filter} path:sources/books",
        f"{base_filter} path:sources/youtube",
        f"{base_filter} path:sources/substack",
        (
            f"{base_filter} path:sources"
            " -path:sources/books"
            " -path:sources/youtube"
            " -path:sources/substack"
        ),
        f"{base_filter} path:inbox",
    ]


def test_apply_obsidian_theme_preserves_existing_graph_tuning_while_refreshing_managed_color_groups(
    tmp_path: Path,
) -> None:
    root = _prepare_root(tmp_path)
    graph_path = root / "memory" / ".obsidian" / "graph.json"
    graph_path.parent.mkdir(parents=True, exist_ok=True)
    graph_path.write_text(
        json.dumps(
            {
                "search": "path:wiki",
                "scale": 0.03,
                "lineSizeMultiplier": 0.61,
                "repelStrength": 17,
                "showTags": True,
            }
        ),
        encoding="utf-8",
    )

    apply_obsidian_theme(root)
    graph = json.loads(graph_path.read_text(encoding="utf-8"))

    assert graph["search"] == "-path:summaries"
    assert graph["scale"] == 0.03
    assert graph["lineSizeMultiplier"] == 0.61
    assert graph["repelStrength"] == 17
    assert graph["showTags"] is True
    queries = [group["query"] for group in graph["colorGroups"]]
    assert f"{graph['search']} path:sources/books" in queries
    assert f"{graph['search']} path:sources/youtube" in queries
    assert f"{graph['search']} path:sources/substack" in queries


def test_apply_obsidian_theme_is_idempotent(tmp_path: Path) -> None:
    root = _prepare_root(tmp_path)

    first = apply_obsidian_theme(root)
    second = apply_obsidian_theme(root)

    assert len(first.changed_paths) == 3
    assert second.changed_paths == []
    assert set(second.unchanged_paths) == {
        "memory/.obsidian/appearance.json",
        "memory/.obsidian/graph.json",
        "memory/.obsidian/snippets/brain-kanagawa.css",
    }


def test_apply_obsidian_theme_force_rewrites_even_when_unchanged(tmp_path: Path) -> None:
    root = _prepare_root(tmp_path)
    apply_obsidian_theme(root)

    forced = apply_obsidian_theme(root, force=True)

    assert set(forced.changed_paths) == {
        "memory/.obsidian/appearance.json",
        "memory/.obsidian/graph.json",
        "memory/.obsidian/snippets/brain-kanagawa.css",
    }
