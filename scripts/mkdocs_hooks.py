"""MkDocs hooks for rendering citation references as clickable links."""

from __future__ import annotations

import html
import re
from urllib.parse import quote

CITATION_RE = re.compile(r"\[src:\s*([^\]]+)\]")
REF_RE = re.compile(r"^([^:\s]+):(?:L)?(\d+)(?:-(?:L)?(\d+))?$", re.IGNORECASE)


def _parse_ref(chunk: str) -> tuple[str, int, int] | None:
    parsed = REF_RE.match(chunk)
    if not parsed:
        return None

    rel_path = parsed.group(1)
    start = int(parsed.group(2))
    end = int(parsed.group(3) or start)
    if end < start:
        start, end = end, start
    return rel_path, start, end


def _convert_citation_block(raw: str, source_prefix: str) -> str:
    pills: list[str] = []
    parsed_rows: list[tuple[str, int, int]] = []
    chunks: list[str] = [item.strip() for item in raw.split(",") if item.strip()]

    for chunk in chunks:
        parsed = _parse_ref(chunk)
        if not parsed:
            parsed_rows.append(("", 0, 0))
        else:
            parsed_rows.append(parsed)

    unique_paths = list(dict.fromkeys(row[0] for row in parsed_rows if row[0]))
    stems = _compute_short_stems(unique_paths)

    for idx, chunk in enumerate(chunks):
        rel_path, start, end = parsed_rows[idx]
        if not rel_path:
            escaped = html.escape(chunk)
            pills.append(f'<span class="src-pill src-pill-raw">{escaped}</span>')
            continue

        spec = f"L{start}-{end}" if start != end else f"L{start}"
        hash_spec = f"L{start}-L{end}" if start != end else f"L{start}"
        target = f"{source_prefix}_source/{quote(rel_path, safe='/._-')}/#{hash_spec}"
        short_stem = stems.get(rel_path, rel_path.split("/")[-1])
        label = f"{short_stem}:{spec}"
        full_label = f"{rel_path}:{spec}"
        pills.append(
            '<a class="src-pill" '
            f'href="{html.escape(target, quote=True)}" '
            f'title="{html.escape(full_label, quote=True)}">'
            f"{html.escape(label)}</a>"
        )

    return '<span class="src-pills" data-role="citation-pills">' + "".join(pills) + "</span>"


def _source_prefix_for_page(page) -> str:
    src_uri = getattr(getattr(page, "file", None), "src_uri", "") or ""
    parts = [part for part in src_uri.replace("\\", "/").split("/") if part]
    depth = max(1, len(parts))
    return "../" * depth


def on_page_markdown(markdown: str, **kwargs) -> str:
    page = kwargs.get("page")
    source_prefix = _source_prefix_for_page(page)
    return CITATION_RE.sub(
        lambda match: _convert_citation_block(match.group(1), source_prefix),
        markdown,
    )


def _compute_short_stems(paths: list[str]) -> dict[str, str]:
    tails: dict[str, int] = {path: 1 for path in paths}
    parts_map: dict[str, list[str]] = {path: path.split("/") for path in paths}

    while True:
        grouped: dict[str, list[str]] = {}
        for path in paths:
            parts = parts_map[path]
            width = min(tails[path], len(parts))
            tail = "/".join(parts[-width:])
            grouped.setdefault(tail, []).append(path)

        conflicts = [group for group in grouped.values() if len(group) > 1]
        if not conflicts:
            break

        advanced = False
        for group in conflicts:
            for path in group:
                max_width = len(parts_map[path])
                if tails[path] < max_width:
                    tails[path] += 1
                    advanced = True
        if not advanced:
            break

    out: dict[str, str] = {}
    for path in paths:
        parts = parts_map[path]
        width = min(tails[path], len(parts))
        out[path] = "/".join(parts[-width:])
    return out
