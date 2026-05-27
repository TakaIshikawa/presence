"""Flag accessibility gaps in blog draft table blocks."""
from __future__ import annotations

from html.parser import HTMLParser
from typing import Any
import re

from ._batch_report_common import (
    clean,
    connection,
    empty_state,
    flatten_missing,
    json_dumps,
    load_table,
    now_value,
    schema,
)

ARTIFACT_TYPE = "blog_draft_table_accessibility_gaps"
DEFAULT_LIMIT = 100
MD_SEPARATOR_RE = re.compile(r"^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?\s*$")


def build_blog_draft_table_accessibility_gaps_report(
    rows: list[dict[str, Any]],
    *,
    draft_id: Any = None,
    limit: int = DEFAULT_LIMIT,
    now=None,
    missing_tables=None,
    missing_columns=None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    gen = now_value(now)
    findings: list[dict[str, Any]] = []
    scanned = 0
    for row in rows:
        did = row.get("draft_id") or row.get("id")
        if draft_id is not None and str(did) != str(draft_id):
            continue
        body = clean(row.get("body") or row.get("content") or row.get("markdown") or row.get("html"))
        tables = _extract_tables(body)
        scanned += len(tables)
        for table_index, table in enumerate(tables, 1):
            issues = _issues(table)
            if not issues:
                continue
            findings.append(
                {
                    "draft_id": did,
                    "table_index": table_index,
                    "issue_codes": issues,
                    "severity": _severity(issues),
                    "recommendation": _recommendation(issues),
                }
            )
    findings.sort(key=lambda f: (_sort_id(f["draft_id"]), f["table_index"]))
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": gen.isoformat(),
        "filters": {"draft_id": draft_id, "limit": limit},
        "summary": {
            "draft_count": len(rows),
            "table_count": scanned,
            "finding_count": len(findings),
            "shown_count": len(shown),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items()) if v},
        "empty_state": empty_state(
            findings,
            "No blog draft table accessibility gaps found.",
            schema_gap=bool(missing_tables or missing_columns),
        ),
    }


def build_blog_draft_table_accessibility_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    sch = schema(conn)
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    rows: list[dict[str, Any]] = []
    if "blog_drafts" not in sch:
        missing_tables.append("blog_drafts")
    else:
        cols = sch["blog_drafts"]
        missing = []
        if "id" not in cols and "draft_id" not in cols:
            missing.append("id|draft_id")
        if not ({"body", "content", "markdown", "html"} & cols):
            missing.append("body|content|markdown|html")
        if missing:
            missing_columns["blog_drafts"] = missing
        else:
            rows = load_table(
                conn,
                "blog_drafts",
                cols,
                {"draft_id": ("id", "draft_id"), "body": ("body", "content", "markdown", "html")},
            )
    return build_blog_draft_table_accessibility_gaps_report(
        rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_blog_draft_table_accessibility_gaps_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_blog_draft_table_accessibility_gaps_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Blog Draft Table Accessibility Gaps",
        f"Generated: {report['generated_at']}",
        f"Totals: drafts={summary['draft_count']} tables={summary['table_count']} findings={summary['finding_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("")
    lines.append("draft_id | table | severity | issues | recommendation")
    for finding in report["findings"]:
        lines.append(
            f"{finding['draft_id']} | {finding['table_index']} | {finding['severity']} | "
            f"{','.join(finding['issue_codes'])} | {finding['recommendation']}"
        )
    return "\n".join(lines)


def _extract_tables(body: str) -> list[dict[str, Any]]:
    tables: list[tuple[int, dict[str, Any]]] = []
    for match in re.finditer(r"<table\b.*?</table>", body, re.I | re.S):
        parser = _TableParser()
        parser.feed(match.group(0))
        tables.append((match.start(), parser.table()))
    for start, block in _markdown_table_blocks(body):
        tables.append((start, _markdown_table(block, body[:start], body[start + len(block) :])))
    return [table for _, table in sorted(tables, key=lambda item: item[0])]


def _markdown_table_blocks(body: str) -> list[tuple[int, str]]:
    blocks: list[tuple[int, str]] = []
    lines = body.splitlines(keepends=True)
    offset = 0
    i = 0
    while i < len(lines):
        line_start = offset
        if "|" not in lines[i] or (i + 1 < len(lines) and not MD_SEPARATOR_RE.match(lines[i + 1])):
            offset += len(lines[i])
            i += 1
            continue
        block_lines = [lines[i], lines[i + 1]]
        offset += len(lines[i]) + len(lines[i + 1])
        i += 2
        while i < len(lines) and "|" in lines[i] and lines[i].strip():
            block_lines.append(lines[i])
            offset += len(lines[i])
            i += 1
        blocks.append((line_start, "".join(block_lines)))
    return blocks


def _markdown_table(block: str, before: str, after: str) -> dict[str, Any]:
    rows = [_split_markdown_row(line) for line in block.splitlines() if "|" in line]
    data_rows = [rows[0], *rows[2:]] if len(rows) >= 2 and MD_SEPARATOR_RE.match(block.splitlines()[1]) else rows
    previous = before.rstrip().splitlines()[-1].strip() if before.rstrip().splitlines() else ""
    following = after.lstrip().splitlines()[0].strip() if after.lstrip().splitlines() else ""
    has_caption = _caption_line(previous) or _caption_line(following)
    return {"format": "markdown", "rows": data_rows, "header": rows[0] if rows else [], "has_header": len(rows) >= 2 and MD_SEPARATOR_RE.match(block.splitlines()[1]) is not None, "has_caption": has_caption}


def _split_markdown_row(line: str) -> list[str]:
    text = line.strip()
    if text.startswith("|"):
        text = text[1:]
    if text.endswith("|"):
        text = text[:-1]
    return [cell.strip() for cell in text.split("|")]


def _caption_line(line: str) -> bool:
    return bool(re.match(r"^(table|caption|summary)\s*:", line, re.I))


def _issues(table: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    rows = table["rows"]
    if not table["has_header"]:
        issues.append("missing_header_row")
    if table.get("header") and any(not clean(cell) for cell in table["header"]):
        issues.append("empty_header_cells")
    if not table["has_caption"]:
        issues.append("missing_caption_or_summary")
    counts = {len(row) for row in rows if row}
    if len(counts) > 1:
        issues.append("inconsistent_column_counts")
    return issues


def _severity(issues: list[str]) -> str:
    if "missing_header_row" in issues or "inconsistent_column_counts" in issues:
        return "high"
    if "empty_header_cells" in issues:
        return "medium"
    return "low"


def _recommendation(issues: list[str]) -> str:
    if "missing_header_row" in issues:
        return "Add a header row with non-empty column labels."
    if "inconsistent_column_counts" in issues:
        return "Make every table row use the same number of columns."
    if "empty_header_cells" in issues:
        return "Fill every header cell with descriptive text."
    return "Add a caption, summary, aria-label, or nearby Markdown caption."


def _sort_id(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, clean(value))


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self.header: list[str] = []
        self.has_caption = False
        self._in_caption = False
        self._in_cell = False
        self._cell_is_header = False
        self._current_row: list[str] | None = None
        self._current_cell: list[str] = []
        self._first_row_cells: list[tuple[bool, str]] | None = None
        self._table_attrs: dict[str, str | None] = {}
        self._row_cells: list[tuple[bool, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self._table_attrs = dict(attrs)
        elif tag == "caption":
            self.has_caption = True
            self._in_caption = True
        elif tag == "tr":
            self._current_row = []
            self._row_cells = []
        elif tag in {"th", "td"}:
            self._in_cell = True
            self._cell_is_header = tag == "th"
            self._current_cell = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "caption":
            self._in_caption = False
        elif tag in {"th", "td"} and self._in_cell:
            text = clean("".join(self._current_cell))
            if self._current_row is not None:
                self._current_row.append(text)
                self._row_cells.append((self._cell_is_header, text))
            self._in_cell = False
        elif tag == "tr" and self._current_row is not None:
            if self._first_row_cells is None:
                self._first_row_cells = list(self._row_cells)
                self.header = [text for is_header, text in self._first_row_cells if is_header]
            self.rows.append(self._current_row)
            self._current_row = None
            self._row_cells = []

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._current_cell.append(data)

    def table(self) -> dict[str, Any]:
        attrs = self._table_attrs
        has_meta = self.has_caption or any(clean(attrs.get(key)) for key in ("summary", "aria-label", "aria-describedby"))
        first = self._first_row_cells or []
        return {
            "format": "html",
            "rows": self.rows,
            "header": [text for is_header, text in first if is_header],
            "has_header": bool(first) and all(is_header for is_header, _ in first),
            "has_caption": has_meta,
        }

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def unknown_decl(self, data: str) -> None:
        return None

    def handle_entityref(self, name: str) -> None:
        if self._in_cell:
            self._current_cell.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        if self._in_cell:
            self._current_cell.append(f"&#{name};")
