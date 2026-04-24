"""Replace or insert a markdown section by heading.

Used by Phase G's --reingest mode to update existing source pages with
new sections (## Atom evidence, ## Probationary atoms surfaced) without
touching unrelated body content.

The rewriter parses by line scanning, not full markdown AST — sections are
identified by their `## ` heading line and run until the next `## ` heading
or end-of-file. Frontmatter (between leading --- fences) is preserved
verbatim.

Atomic: writes to a sibling tempfile and renames over the target so a crash
mid-write doesn't leave a half-written file.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class ParsedSection:
    heading: str
    content: str


@dataclass(frozen=True)
class ParsedMarkdownBody:
    frontmatter_block: str
    intro: str
    sections: tuple[ParsedSection, ...]


@dataclass(frozen=True)
class SectionOperation:
    heading: str
    mode: str
    content: str = ""
    insert_after: str | None = None


def _split_frontmatter(text: str) -> tuple[str, str]:
    """Return (frontmatter_block, rest). frontmatter_block includes its
    fences and trailing newline; rest is everything after."""
    if not text.startswith("---\n") and not text.startswith("---\r\n"):
        return "", text
    # Find the closing fence
    lines = text.splitlines(keepends=True)
    if not lines:
        return "", text
    close_idx = None
    for i in range(1, len(lines)):
        if lines[i].rstrip("\r\n") == "---":
            close_idx = i
            break
    if close_idx is None:
        return "", text
    fm_block = "".join(lines[: close_idx + 1])
    rest = "".join(lines[close_idx + 1 :])
    return fm_block, rest


def _find_section(body: str, heading: str) -> tuple[int, int] | None:
    """Find a section by its `## Heading` line. Returns (start, end) byte
    offsets where start is the position of the heading line and end is the
    position of the next `## ` heading or len(body).

    Returns None if the heading is not found.
    """
    target_line = heading.rstrip()
    lines = body.splitlines(keepends=True)
    start_line_idx = None
    for i, line in enumerate(lines):
        if line.rstrip() == target_line:
            start_line_idx = i
            break
    if start_line_idx is None:
        return None

    # Find the next "## " heading after this one
    end_line_idx = len(lines)
    for j in range(start_line_idx + 1, len(lines)):
        stripped = lines[j].lstrip()
        if stripped.startswith("## ") and not stripped.startswith("### "):
            end_line_idx = j
            break

    start_offset = sum(len(line) for line in lines[:start_line_idx])
    end_offset = sum(len(line) for line in lines[:end_line_idx])
    return start_offset, end_offset


def _build_section(heading: str, content: str) -> str:
    """Build a `## Heading\\n\\n<content>\\n` block, normalizing trailing
    whitespace so consecutive sections format predictably."""
    content = content.rstrip("\n") + "\n"
    return f"{heading}\n\n{content}"


def parse_markdown_body(text: str) -> ParsedMarkdownBody:
    fm_block, body = _split_frontmatter(text)
    lines = body.splitlines(keepends=True)
    intro_lines: list[str] = []
    sections: list[ParsedSection] = []
    index = 0
    while index < len(lines):
        stripped = lines[index].lstrip()
        if stripped.startswith("## ") and not stripped.startswith("### "):
            break
        intro_lines.append(lines[index])
        index += 1

    while index < len(lines):
        heading = lines[index].rstrip("\r\n")
        index += 1
        content_lines: list[str] = []
        while index < len(lines):
            stripped = lines[index].lstrip()
            if stripped.startswith("## ") and not stripped.startswith("### "):
                break
            content_lines.append(lines[index])
            index += 1
        sections.append(ParsedSection(heading=heading, content="".join(content_lines)))

    return ParsedMarkdownBody(
        frontmatter_block=fm_block,
        intro="".join(intro_lines),
        sections=tuple(sections),
    )


def render_markdown_body(parsed: ParsedMarkdownBody) -> str:
    body_parts: list[str] = [parsed.intro]
    for section in parsed.sections:
        block = _build_section(section.heading, section.content)
        if body_parts and body_parts[-1] and not body_parts[-1].endswith("\n\n"):
            if body_parts[-1].endswith("\n"):
                body_parts.append("\n")
            else:
                body_parts.append("\n\n")
        body_parts.append(block)
    return parsed.frontmatter_block + "".join(body_parts)


def _normalize_block_content(content: str) -> str:
    return content.rstrip("\n") + "\n"


def _is_bullet_list(content: str) -> bool:
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    return bool(lines) and all(line.startswith("- ") for line in lines)


def _union_bullet_lists(existing: str, new: str) -> str:
    if not _is_bullet_list(existing) or not _is_bullet_list(new):
        raise ValueError("union operations require bullet-list content")
    existing_lines = [line.rstrip() for line in existing.splitlines() if line.strip()]
    new_lines = [line.rstrip() for line in new.splitlines() if line.strip()]
    seen = set(existing_lines)
    merged = list(existing_lines)
    for line in new_lines:
        if line in seen:
            continue
        merged.append(line)
        seen.add(line)
    return "\n".join(merged) + "\n"


def apply_section_operations(
    *,
    text: str,
    intro_mode: str = "preserve",
    intro_content: str = "",
    section_operations: list[SectionOperation],
) -> str:
    parsed = parse_markdown_body(text)
    intro = parsed.intro
    if intro_mode == "replace":
        intro = _normalize_block_content(intro_content)
    elif intro_mode == "append":
        base = intro.rstrip("\n")
        addition = intro_content.strip("\n")
        if base and addition:
            intro = f"{base}\n\n{addition}\n"
        elif addition:
            intro = addition + "\n"
        else:
            intro = _normalize_block_content(intro)
    elif intro_mode == "preserve":
        intro = _normalize_block_content(intro) if intro else ""
    else:
        raise ValueError(f"unsupported intro mode: {intro_mode}")

    sections = list(parsed.sections)
    by_heading = {section.heading: index for index, section in enumerate(sections)}

    for operation in section_operations:
        if operation.mode == "preserve":
            continue
        index = by_heading.get(operation.heading)
        normalized_content = _normalize_block_content(operation.content)
        if index is None:
            if operation.mode == "replace":
                new_section = ParsedSection(heading=operation.heading, content=normalized_content)
            elif operation.mode == "append":
                new_section = ParsedSection(heading=operation.heading, content=normalized_content)
            elif operation.mode == "union":
                new_section = ParsedSection(heading=operation.heading, content=normalized_content)
            else:
                raise ValueError(f"unsupported section mode: {operation.mode}")
            if operation.insert_after and operation.insert_after in by_heading:
                anchor_index = by_heading[operation.insert_after] + 1
                sections.insert(anchor_index, new_section)
            else:
                sections.append(new_section)
            by_heading = {section.heading: idx for idx, section in enumerate(sections)}
            continue

        existing = sections[index]
        if operation.mode == "replace":
            sections[index] = ParsedSection(heading=existing.heading, content=normalized_content)
        elif operation.mode == "append":
            base = existing.content.rstrip("\n")
            addition = operation.content.strip("\n")
            if base and addition:
                merged = f"{base}\n\n{addition}\n"
            elif addition:
                merged = addition + "\n"
            else:
                merged = _normalize_block_content(existing.content)
            sections[index] = ParsedSection(heading=existing.heading, content=merged)
        elif operation.mode == "union":
            sections[index] = ParsedSection(
                heading=existing.heading,
                content=_union_bullet_lists(existing.content, normalized_content),
            )
        else:
            raise ValueError(f"unsupported section mode: {operation.mode}")

    return render_markdown_body(
        ParsedMarkdownBody(
            frontmatter_block=parsed.frontmatter_block,
            intro=intro,
            sections=tuple(sections),
        )
    )


def _atomic_write(file_path: Path, text: str) -> None:
    """Write text to a sibling tempfile and rename over file_path."""
    tmp = file_path.with_name(file_path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(file_path)


def replace_or_insert_section(
    *,
    file_path: Path,
    section_heading: str,
    new_content: str,
    insert_after: Optional[str] = None,
) -> bool:
    """Find a section by heading and replace its body. If the section
    doesn't exist, insert it after `insert_after` (or at end of file).

    Args:
        file_path: Page to modify.
        section_heading: The full heading line including `## ` prefix
                         (e.g. "## Atom evidence").
        new_content: The section body, NOT including the heading line.
                     A trailing newline is added if missing.
        insert_after: Optional anchor heading to insert after, if the
                      target section doesn't already exist. If None or
                      not found, the new section is appended at the end.

    Returns:
        True if the file was modified, False if the new content was
        identical to existing content (no-op).
    """
    text = file_path.read_text(encoding="utf-8")
    fm_block, body = _split_frontmatter(text)

    new_section = _build_section(section_heading, new_content)
    existing = _find_section(body, section_heading)

    if existing is not None:
        start, end = existing
        # Check if replacement is a no-op
        existing_section = body[start:end]
        if existing_section.rstrip() == new_section.rstrip():
            return False
        new_body = body[:start] + new_section + body[end:]
    else:
        # Insert after the anchor, or append at end
        if insert_after is not None:
            anchor = _find_section(body, insert_after)
        else:
            anchor = None
        if anchor is not None:
            _, anchor_end = anchor
            new_body = body[:anchor_end] + new_section + body[anchor_end:]
        else:
            # Append at end with a leading blank line if body doesn't end in one
            sep = "" if body.endswith("\n\n") else ("\n" if body.endswith("\n") else "\n\n")
            new_body = body + sep + new_section

    new_text = fm_block + new_body
    if new_text == text:
        return False

    _atomic_write(file_path, new_text)
    return True
