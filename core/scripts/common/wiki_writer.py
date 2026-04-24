"""Helper for writing wiki pages with schema-v2 frontmatter.

Used by both the YouTube and Books pipelines. Hand-rolls YAML serialization to control
exact formatting (canonical Brain shape: arrays as bullet lists, scalars inline,
[[wiki-links]] always quoted).
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any


def _serialize_value(value: Any, indent: int = 0) -> str:
    pad = "  " * indent
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        # Quote strings that look like wiki-links or contain YAML-special chars.
        # Apostrophes in bare scalars can break frontmatter parsing for titles like
        # "'Too expensive' in exit interviews ...", so force double-quoted scalars.
        if value.startswith("[[") or any(c in value for c in ":#\"'"):
            escaped = value.replace('"', '\\"')
            return f'"{escaped}"'
        return value
    if isinstance(value, list):
        if not value:
            return "[]"
        lines = []
        for item in value:
            lines.append(f"\n{pad}  - {_serialize_value(item, indent + 1)}")
        return "".join(lines)
    if isinstance(value, dict):
        if not value:
            return "{}"
        lines = []
        for key, item in value.items():
            serialized = _serialize_value(item, indent + 1)
            if serialized.startswith("\n"):
                lines.append(f"\n{pad}  {key}:{serialized}")
            else:
                lines.append(f"\n{pad}  {key}: {serialized}")
        return "".join(lines)
    raise TypeError(f"Unsupported frontmatter value type: {type(value).__name__}")


def _serialize_frontmatter(frontmatter: dict[str, Any]) -> str:
    lines = ["---"]
    for key, value in frontmatter.items():
        serialized = _serialize_value(value)
        if serialized.startswith("\n"):
            lines.append(f"{key}:{serialized}")
        else:
            lines.append(f"{key}: {serialized}")
    lines.append("---")
    return "\n".join(lines)


def write_page(
    target: Path,
    *,
    frontmatter: dict[str, Any],
    body: str,
    force: bool = False,
) -> None:
    """Write a wiki markdown page atomically.

    Refuses to overwrite an existing file unless force=True.
    Creates parent directories as needed.
    Always ends with a single trailing newline.
    """
    if target.exists() and not force:
        raise FileExistsError(f"Refusing to overwrite {target} (use force=True)")
    target.parent.mkdir(parents=True, exist_ok=True)
    serialized = _serialize_frontmatter(frontmatter)
    body_text = body.rstrip() + "\n"
    target.write_text(f"{serialized}\n\n{body_text}")
