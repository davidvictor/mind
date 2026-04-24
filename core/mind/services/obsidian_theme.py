from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from scripts.common.vault import Vault, project_root

TOKENS_PATH = Path("design/obsidian-kanagawa.json")
SNIPPET_FILENAME = "brain-kanagawa.css"
SNIPPET_NAME = "brain-kanagawa"
SOURCE_REPO_ROOT = project_root()
GRAPH_BASE_FILTER = '-path:summaries'
GRAPH_COLOR_GROUPS = (
    ("me", f"{GRAPH_BASE_FILTER} path:me"),
    ("projects", f"{GRAPH_BASE_FILTER} path:projects"),
    ("people", f"{GRAPH_BASE_FILTER} path:people"),
    ("companies", f"{GRAPH_BASE_FILTER} path:companies"),
    ("channels", f"{GRAPH_BASE_FILTER} path:channels"),
    ("concepts", f"{GRAPH_BASE_FILTER} path:concepts"),
    ("playbooks", f"{GRAPH_BASE_FILTER} path:playbooks"),
    ("stances", f"{GRAPH_BASE_FILTER} path:stances"),
    ("inquiries", f"{GRAPH_BASE_FILTER} path:inquiries"),
    ("decisions", f"{GRAPH_BASE_FILTER} path:decisions"),
    ("books", f"{GRAPH_BASE_FILTER} path:sources/books"),
    ("youtube", f"{GRAPH_BASE_FILTER} path:sources/youtube"),
    ("substack", f"{GRAPH_BASE_FILTER} path:sources/substack"),
    (
        "sources",
        f"{GRAPH_BASE_FILTER} path:sources"
        " -path:sources/books"
        " -path:sources/youtube"
        " -path:sources/substack",
    ),
    ("inbox", f"{GRAPH_BASE_FILTER} path:inbox"),
)


@dataclass(frozen=True)
class ObsidianThemeApplyResult:
    dark: str
    light: str
    snippet_path: str
    graph_path: str
    appearance_path: str
    changed_paths: list[str]
    unchanged_paths: list[str]

    def render(self) -> str:
        lines = [
            "obsidian-theme:",
            f"- dark={self.dark}",
            f"- light={self.light}",
            f"- snippet={self.snippet_path}",
            f"- graph={self.graph_path}",
            f"- appearance={self.appearance_path}",
            f"- changed={len(self.changed_paths)}",
            f"- unchanged={len(self.unchanged_paths)}",
        ]
        if self.changed_paths:
            lines.append("- changed_paths:")
            lines.extend(f"  - {path}" for path in self.changed_paths)
        if self.unchanged_paths:
            lines.append("- unchanged_paths:")
            lines.extend(f"  - {path}" for path in self.unchanged_paths)
        return "\n".join(lines)


def apply_obsidian_theme(
    repo_root: Path,
    *,
    dark: str = "dragon",
    light: str = "lotus",
    force: bool = False,
) -> ObsidianThemeApplyResult:
    tokens = _load_tokens(repo_root)
    if dark not in tokens["dark_variants"]:
        raise ValueError(f"unknown dark variant {dark!r}")
    if light not in tokens["light_variants"]:
        raise ValueError(f"unknown light variant {light!r}")

    vault = Vault.load(repo_root)
    obsidian_root = vault.wiki / ".obsidian"
    snippet_path = obsidian_root / "snippets" / SNIPPET_FILENAME
    graph_path = obsidian_root / "graph.json"
    appearance_path = obsidian_root / "appearance.json"

    changed_paths: list[str] = []
    unchanged_paths: list[str] = []

    css_text = _render_css(tokens, dark=dark, light=light)
    _write_if_changed(
        snippet_path,
        css_text,
        force=force,
        changed_paths=changed_paths,
        unchanged_paths=unchanged_paths,
        vault=vault,
    )

    graph_payload = _build_graph_config(tokens, existing=_read_json_object(graph_path))
    _write_json_if_changed(
        graph_path,
        graph_payload,
        force=force,
        changed_paths=changed_paths,
        unchanged_paths=unchanged_paths,
        vault=vault,
    )

    appearance_payload = _build_appearance_config(_read_json_object(appearance_path))
    _write_json_if_changed(
        appearance_path,
        appearance_payload,
        force=force,
        changed_paths=changed_paths,
        unchanged_paths=unchanged_paths,
        vault=vault,
    )

    return ObsidianThemeApplyResult(
        dark=dark,
        light=light,
        snippet_path=vault.logical_path(snippet_path),
        graph_path=vault.logical_path(graph_path),
        appearance_path=vault.logical_path(appearance_path),
        changed_paths=sorted(changed_paths),
        unchanged_paths=sorted(unchanged_paths),
    )


def _load_tokens(repo_root: Path) -> dict[str, Any]:
    path = repo_root / TOKENS_PATH
    if not path.exists():
        path = SOURCE_REPO_ROOT / TOKENS_PATH
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"invalid token file at {path}")
    return payload


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_if_changed(
    path: Path,
    content: str,
    *,
    force: bool,
    changed_paths: list[str],
    unchanged_paths: list[str],
    vault: Vault,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not force and path.read_text(encoding="utf-8") == content:
        unchanged_paths.append(vault.logical_path(path))
        return
    path.write_text(content, encoding="utf-8")
    changed_paths.append(vault.logical_path(path))


def _write_json_if_changed(
    path: Path,
    payload: dict[str, Any],
    *,
    force: bool,
    changed_paths: list[str],
    unchanged_paths: list[str],
    vault: Vault,
) -> None:
    content = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
    _write_if_changed(
        path,
        content,
        force=force,
        changed_paths=changed_paths,
        unchanged_paths=unchanged_paths,
        vault=vault,
    )


def _build_appearance_config(existing: dict[str, Any]) -> dict[str, Any]:
    payload = dict(existing)
    raw_enabled = payload.get("enabledCssSnippets") or []
    enabled = raw_enabled if isinstance(raw_enabled, list) else [raw_enabled]
    normalized = [str(item) for item in enabled if str(item).strip()]
    if SNIPPET_NAME not in normalized:
        normalized.append(SNIPPET_NAME)
    payload["enabledCssSnippets"] = normalized
    payload["cssTheme"] = ""
    return payload


def _build_graph_config(tokens: dict[str, Any], *, existing: dict[str, Any] | None = None) -> dict[str, Any]:
    graph_colors = tokens["graph"]
    payload = {
        "collapse-filter": False,
        "search": GRAPH_BASE_FILTER,
        "showTags": False,
        "showAttachments": False,
        "hideUnresolved": True,
        "showOrphans": False,
        "collapse-color-groups": False,
        "colorGroups": [],
        "collapse-display": False,
        "showArrow": False,
        "textFadeMultiplier": 0.08,
        "nodeSizeMultiplier": 1.32,
        "lineSizeMultiplier": 1.36,
        "collapse-forces": True,
        "centerStrength": 0.5,
        "repelStrength": 11,
        "linkStrength": 1,
        "linkDistance": 190,
        "scale": 0.14,
        "close": False,
    }
    if existing:
        payload.update(existing)
    payload["search"] = GRAPH_BASE_FILTER
    payload["collapse-color-groups"] = False
    payload["colorGroups"] = [
        {
            "query": query,
            "color": {
                "a": 1,
                "rgb": _hex_to_rgb_int(graph_colors[family]),
            },
        }
        for family, query in GRAPH_COLOR_GROUPS
    ]
    return payload


def _hex_to_rgb_int(value: str) -> int:
    return int(value.lstrip("#"), 16)


def _hex_to_rgb_components(value: str) -> str:
    raw = value.lstrip("#")
    return ", ".join(str(int(raw[index:index + 2], 16)) for index in (0, 2, 4))


def _css_var_block(name: str, colors: dict[str, str]) -> str:
    lines = [f"{name} {{"]
    lines.extend(f"  --brain-{key.replace('_', '-')}: {value};" for key, value in colors.items())
    lines.extend(
        [
            "  --background-primary: var(--brain-background);",
            "  --background-secondary: var(--brain-background-elevated);",
            "  --background-secondary-alt: var(--brain-panel);",
            "  --background-modifier-border: var(--brain-border);",
            "  --background-modifier-border-hover: var(--brain-link);",
            "  --background-modifier-hover: color-mix(in srgb, var(--brain-link) 12%, transparent);",
            "  --background-modifier-active-hover: color-mix(in srgb, var(--brain-link) 18%, transparent);",
            "  --text-normal: var(--brain-text-primary);",
            "  --text-muted: var(--brain-text-muted);",
            "  --text-faint: var(--brain-text-faint);",
            "  --link-color: var(--brain-link);",
            "  --link-color-hover: var(--brain-link-hover);",
            "  --interactive-accent: var(--brain-accent-primary);",
            "  --interactive-accent-hover: var(--brain-accent-secondary);",
            "  --text-selection: var(--brain-selection);",
            "  --code-background: var(--brain-code-background);",
            "  --code-normal: var(--brain-text-primary);",
            "  --inline-code-background: color-mix(in srgb, var(--brain-inline-code-background) 82%, transparent);",
            "  --blockquote-border-color: var(--brain-blockquote-border);",
            "  --table-border-color: var(--brain-border);",
            "  --table-selection: color-mix(in srgb, var(--brain-selection) 55%, transparent);",
            "  --h1-color: var(--brain-heading-1);",
            "  --h2-color: var(--brain-heading-2);",
            "  --h3-color: var(--brain-heading-3);",
            "  --h4-color: var(--brain-heading-4);",
            "  --h5-color: var(--brain-heading-5);",
            "  --h6-color: var(--brain-heading-6);",
        ]
    )
    lines.append("}")
    return "\n".join(lines)


def _render_css(tokens: dict[str, Any], *, dark: str, light: str) -> str:
    dark_colors = tokens["dark_variants"][dark]
    light_colors = tokens["light_variants"][light]
    note_rgb = _hex_to_rgb_components(dark_colors["callout_note"])
    tip_rgb = _hex_to_rgb_components(dark_colors["callout_tip"])
    warning_rgb = _hex_to_rgb_components(dark_colors["callout_warning"])
    danger_rgb = _hex_to_rgb_components(dark_colors["callout_danger"])
    return "\n".join(
        [
            "/*",
            " * Generated by `mind obsidian theme apply`.",
            " * Canonical visual system: default Obsidian base + Brain Kanagawa snippet.",
            " */",
            "",
            _css_var_block(".theme-dark", dark_colors),
            "",
            _css_var_block(".theme-light", light_colors),
            "",
            "body {",
            "  --titlebar-background-focused: var(--background-secondary);",
            "  --titlebar-text-color-focused: var(--text-muted);",
            "}",
            "",
            ".workspace-split.mod-left-split,",
            ".workspace-split.mod-right-split,",
            ".workspace-tabs .workspace-leaf,",
            ".workspace-tab-container,",
            ".workspace-sidedock-vault-profile {",
            "  background: var(--background-secondary);",
            "}",
            "",
            ".workspace-tab-header.is-active,",
            ".workspace-tab-header.is-active:hover {",
            "  background: color-mix(in srgb, var(--interactive-accent) 14%, var(--background-secondary));",
            "  color: var(--text-normal);",
            "}",
            "",
            ".workspace-tab-header:hover,",
            ".tree-item-self:hover,",
            ".nav-file-title:hover,",
            ".nav-folder-title:hover {",
            "  background: var(--background-modifier-hover);",
            "}",
            "",
            ".tree-item-self.is-active,",
            ".nav-file.is-active .nav-file-title,",
            ".workspace-leaf-content[data-type='file-explorer'] .tree-item-self.is-active {",
            "  background: color-mix(in srgb, var(--interactive-accent) 16%, var(--background-secondary));",
            "  color: var(--text-normal);",
            "  border-left: 2px solid var(--interactive-accent);",
            "}",
            "",
            ".markdown-preview-view,",
            ".markdown-source-view.mod-cm6 .cm-scroller {",
            "  line-height: 1.72;",
            "  letter-spacing: 0.01em;",
            "}",
            "",
            ".markdown-preview-view h1,",
            ".markdown-source-view.mod-cm6 .HyperMD-header-1 {",
            "  letter-spacing: 0.01em;",
            "  font-weight: 700;",
            "}",
            "",
            ".markdown-preview-view h2,",
            ".markdown-preview-view h3,",
            ".markdown-preview-view h4,",
            ".markdown-preview-view h5,",
            ".markdown-preview-view h6 {",
            "  font-weight: 650;",
            "}",
            "",
            "a,",
            ".cm-s-obsidian span.cm-link,",
            ".cm-s-obsidian span.cm-hmd-internal-link,",
            ".markdown-source-view.mod-cm6 .cm-url,",
            ".markdown-rendered .internal-link {",
            "  color: var(--link-color);",
            "  text-decoration-thickness: 0.08em;",
            "  text-underline-offset: 0.12em;",
            "}",
            "",
            "a:hover,",
            ".markdown-rendered .internal-link:hover {",
            "  color: var(--link-color-hover);",
            "}",
            "",
            ".tag,",
            ".metadata-property-key,",
            ".metadata-property-value,",
            ".multi-select-pill {",
            "  border: 1px solid var(--background-modifier-border);",
            "  border-radius: 999px;",
            "  background: color-mix(in srgb, var(--interactive-accent) 10%, transparent);",
            "}",
            "",
            ".callout {",
            "  border: 1px solid var(--background-modifier-border);",
            "  background: color-mix(in srgb, var(--background-secondary) 88%, transparent);",
            "  border-radius: 14px;",
            "}",
            "",
            f".callout[data-callout='note'] {{ --callout-color: {note_rgb}; }}",
            f".callout[data-callout='tip'] {{ --callout-color: {tip_rgb}; }}",
            f".callout[data-callout='warning'] {{ --callout-color: {warning_rgb}; }}",
            f".callout[data-callout='danger'] {{ --callout-color: {danger_rgb}; }}",
            f".callout[data-callout='error'] {{ --callout-color: {danger_rgb}; }}",
            "",
            "blockquote {",
            "  border-left: 3px solid var(--blockquote-border-color);",
            "  color: var(--text-muted);",
            "  background: color-mix(in srgb, var(--background-secondary) 72%, transparent);",
            "  padding: 0.85rem 1rem;",
            "  border-radius: 0 12px 12px 0;",
            "}",
            "",
            "pre,",
            "code,",
            ".cm-inline-code,",
            ".HyperMD-codeblock {",
            "  border-radius: 10px;",
            "}",
            "",
            "pre,",
            ".markdown-rendered pre,",
            ".markdown-source-view.mod-cm6 .cm-line.HyperMD-codeblock {",
            "  background: var(--code-background) !important;",
            "  border: 1px solid var(--background-modifier-border);",
            "}",
            "",
            "code,",
            ".cm-inline-code {",
            "  background: var(--inline-code-background);",
            "  padding: 0.12em 0.4em;",
            "}",
            "",
            ".markdown-rendered table {",
            "  border-collapse: separate;",
            "  border-spacing: 0;",
            "  overflow: hidden;",
            "  border: 1px solid var(--table-border-color);",
            "  border-radius: 12px;",
            "}",
            "",
            ".markdown-rendered table tr:nth-child(even) td {",
            "  background: color-mix(in srgb, var(--brain-table-stripe) 52%, transparent);",
            "}",
            "",
            "::selection,",
            ".markdown-source-view.mod-cm6 .cm-selectionBackground,",
            ".markdown-source-view.mod-cm6 .cm-content .cm-highlight {",
            "  background: color-mix(in srgb, var(--text-selection) 48%, transparent) !important;",
            "}",
            "",
            ".canvas-node {",
            "  background: var(--background-primary);",
            "  border: 1px solid var(--background-modifier-border);",
            "  box-shadow: 0 14px 32px rgba(0, 0, 0, 0.14);",
            "}",
            "",
            ".canvas-node.is-focused,",
            ".canvas-node:hover {",
            "  border-color: var(--interactive-accent);",
            "}",
            "",
            ".graph-view.color-circle,",
            ".graph-view.color-fill-highlight {",
            "  filter: saturate(1.05);",
            "}",
            "",
        ]
    ).rstrip() + "\n"
