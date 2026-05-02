"""Digest Claude Code file impact patterns from recent session logs."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Mapping


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 20

EVENT_TABLE_CANDIDATES = (
    "claude_session_events",
    "claude_tool_events",
    "claude_events",
)
TEXT_TABLE = "claude_messages"
SESSION_COLUMNS = ("session_id", "sessionId")
TIMESTAMP_COLUMNS = ("timestamp", "created_at", "event_time", "event_at")
TOOL_COLUMNS = ("tool_name", "tool", "toolName", "name")
STATUS_COLUMNS = ("status", "outcome")
METADATA_COLUMNS = ("metadata", "raw_metadata", "event_json", "payload")
TEXT_COLUMNS = (
    "prompt_text",
    "response_text",
    "content",
    "text",
    "message",
    "body",
    "error",
    "error_message",
    "stderr",
    "output",
    "result",
)
PATH_COLUMNS = ("file_path", "filepath", "path", "paths", "files", "file")

FAILURE_WORD_RE = re.compile(
    r"\b(error|failed|failure|exception|traceback|timeout|timed out|denied|"
    r"non[- ]?zero|exit code|no such file)\b",
    re.IGNORECASE,
)
RELATIVE_PATH_RE = re.compile(
    r"(?<![\w./-])(?:\.?/)?(?:src|scripts|tests|docs|config|migrations|schemas?|"
    r"notebooks|examples|app|apps|lib|libs|packages|tools)/"
    r"[\w@%+=:,./-]*[\w@%+=-]\.[A-Za-z0-9]{1,12}(?::\d+)?"
)
ROOT_FILE_RE = re.compile(
    r"(?<![\w./-])(?:README|Makefile|Dockerfile|pyproject|package|schema|"
    r"requirements|uv\.lock|poetry\.lock|Cargo|go\.mod|go\.sum)"
    r"(?:\.[A-Za-z0-9]{1,12})?(?::\d+)?"
)
ABSOLUTE_PATH_RE = re.compile(r"(?<![\w])/(?:[^\s`'\"<>|]+/)*[^\s`'\"<>|]+\.[A-Za-z0-9]{1,12}(?::\d+)?")


def build_claude_file_impact_digest(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    repo_root: str | Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a ranked digest of project files touched or mentioned by Claude logs."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    root = Path(repo_root or Path.cwd()).expanduser().resolve(strict=False)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": cutoff.isoformat(),
        "repo_root": str(root),
    }
    schema_gaps: dict[str, Any] = {"missing_columns": {}, "missing_tables": []}

    if _looks_like_rows(db_or_rows):
        source_table = "rows"
        rows = [_mapping(row) for row in db_or_rows]
        rows = _filter_rows(rows, cutoff=cutoff)
    else:
        conn = _connection(db_or_rows)
        schema = _schema(conn)
        source_table = _source_table(schema)
        if source_table is None:
            schema_gaps["missing_tables"] = [*EVENT_TABLE_CANDIDATES, TEXT_TABLE]
            return _report(
                generated_at=generated_at,
                filters=filters,
                source_table=None,
                rows_scanned=0,
                malformed_metadata_count=0,
                file_stats=[],
                schema_gaps=schema_gaps,
            )
        missing = _missing_optional_columns(schema[source_table], event_table=source_table != TEXT_TABLE)
        if missing:
            schema_gaps["missing_columns"] = {source_table: missing}
        rows = _load_rows(conn, source_table, schema[source_table], cutoff=cutoff)

    file_stats, malformed_metadata_count = _group_file_stats(rows, repo_root=root)
    file_stats.sort(key=_file_sort_key)
    return _report(
        generated_at=generated_at,
        filters=filters,
        source_table=source_table,
        rows_scanned=len(rows),
        malformed_metadata_count=malformed_metadata_count,
        file_stats=file_stats[:limit],
        schema_gaps=schema_gaps,
    )


def format_claude_file_impact_digest_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_claude_file_impact_digest_markdown(report: dict[str, Any]) -> str:
    """Render a concise Markdown file-impact digest."""
    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "# Claude File Impact Digest",
        "",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"days={filters['days']} limit={filters['limit']} "
            f"repo_root={filters['repo_root']}"
        ),
        (
            "Totals: "
            f"rows={totals['rows_scanned']} files={totals['file_count']} "
            f"mentions={totals['mention_count']} tools={totals['tool_count']} "
            f"errors={totals['error_count']} malformed_metadata="
            f"{totals['malformed_metadata_count']}"
        ),
    ]
    if report.get("source_table"):
        lines.append(f"Source table: {report['source_table']}")
    gaps = report.get("schema_gaps") or {}
    if gaps.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(gaps["missing_tables"]))
    if gaps.get("missing_columns"):
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(gaps["missing_columns"].items())
        ]
        lines.append("Missing optional columns: " + "; ".join(missing))
    if not report["files"]:
        lines.extend(["", "No project file activity found."])
        return "\n".join(lines)

    lines.extend(["", "## Highest Impact Files"])
    for item in report["files"]:
        lines.append(
            f"- `{item['path']}` impact={item['impact_score']} "
            f"mentions={item['mention_count']} tools={item['tool_count']} "
            f"errors={item['error_count']} latest={item['latest_seen'] or '-'}"
        )
        lines.append(f"  Recommendation: {item['recommendation']}")
    return "\n".join(lines)


def _report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    source_table: str | None,
    rows_scanned: int,
    malformed_metadata_count: int,
    file_stats: list[dict[str, Any]],
    schema_gaps: dict[str, Any],
) -> dict[str, Any]:
    return {
        "artifact_type": "claude_file_impact_digest",
        "files": file_stats,
        "filters": filters,
        "generated_at": generated_at.isoformat(),
        "schema_gaps": {
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(schema_gaps.get("missing_columns", {}).items())
            },
            "missing_tables": list(schema_gaps.get("missing_tables", [])),
        },
        "source_table": source_table,
        "totals": {
            "error_count": sum(int(item["error_count"]) for item in file_stats),
            "file_count": len(file_stats),
            "malformed_metadata_count": malformed_metadata_count,
            "mention_count": sum(int(item["mention_count"]) for item in file_stats),
            "rows_scanned": rows_scanned,
            "tool_count": sum(int(item["tool_count"]) for item in file_stats),
        },
    }


def _group_file_stats(
    rows: Iterable[dict[str, Any]],
    *,
    repo_root: Path,
) -> tuple[list[dict[str, Any]], int]:
    grouped: dict[str, dict[str, Any]] = defaultdict(_empty_file_stat)
    malformed_metadata_count = 0
    for row in rows:
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        timestamp = _first_text(row, TIMESTAMP_COLUMNS) or _first_text(metadata, TIMESTAMP_COLUMNS)
        timestamp_sort = _timestamp_sort(timestamp)
        tool_name = _tool_name(row, metadata)
        is_tool_event = tool_name != ""
        is_error_event = _is_failure(
            _first_text(row, STATUS_COLUMNS) or _first_text(metadata, STATUS_COLUMNS),
            _event_text(row, metadata),
            metadata,
        )
        paths = _extract_paths(row, metadata, repo_root=repo_root)
        for path in paths:
            stat = grouped[path]
            stat["path"] = path
            stat["mention_count"] += 1
            if is_tool_event:
                stat["tool_count"] += 1
                if tool_name:
                    stat["tool_names"].add(tool_name)
            if is_error_event:
                stat["error_count"] += 1
            stat["session_ids"].add(
                _first_text(row, SESSION_COLUMNS)
                or _first_text(metadata, SESSION_COLUMNS)
                or "unknown-session"
            )
            if timestamp_sort >= stat["latest_seen_sort"]:
                stat["latest_seen"] = timestamp
                stat["latest_seen_sort"] = timestamp_sort

    stats = []
    for stat in grouped.values():
        impact_score = stat["mention_count"] + (2 * stat["tool_count"]) + (4 * stat["error_count"])
        stats.append(
            {
                "error_count": stat["error_count"],
                "impact_score": impact_score,
                "latest_seen": stat["latest_seen"],
                "mention_count": stat["mention_count"],
                "path": stat["path"],
                "recommendation": _recommendation(stat),
                "session_count": len(stat["session_ids"]),
                "session_ids": sorted(stat["session_ids"])[:5],
                "tool_count": stat["tool_count"],
                "tool_names": sorted(stat["tool_names"])[:5],
            }
        )
    return stats, malformed_metadata_count


def _empty_file_stat() -> dict[str, Any]:
    return {
        "error_count": 0,
        "latest_seen": None,
        "latest_seen_sort": "",
        "mention_count": 0,
        "path": "",
        "session_ids": set(),
        "tool_count": 0,
        "tool_names": set(),
    }


def _extract_paths(row: Mapping[str, Any], metadata: Mapping[str, Any], *, repo_root: Path) -> list[str]:
    project_roots = [repo_root]
    for source in (row, metadata):
        project_path = _first_text(source, ("project_path", "cwd", "project", "repo_root"))
        if project_path:
            project_roots.append(Path(project_path).expanduser().resolve(strict=False))

    candidates: list[str] = []
    for source in (row, metadata):
        candidates.extend(_path_values_from_mapping(source))
        for text in _text_values(source):
            candidates.extend(_paths_from_text(text))

    normalized = []
    seen = set()
    for candidate in candidates:
        path = _normalize_path(candidate, project_roots=project_roots)
        if path and path not in seen:
            seen.add(path)
            normalized.append(path)
    return normalized


def _path_values_from_mapping(source: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for key, value in source.items():
        if key in PATH_COLUMNS:
            values.extend(_path_values(value))
        elif isinstance(value, Mapping):
            values.extend(_path_values_from_mapping(value))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, Mapping):
                    values.extend(_path_values_from_mapping(item))
    return values


def _path_values(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        values = []
        for key in PATH_COLUMNS:
            values.extend(_path_values(value.get(key)))
        return values
    if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray)):
        values = []
        for item in value:
            values.extend(_path_values(item))
        return values
    return [str(value)]


def _text_values(source: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for column in TEXT_COLUMNS:
        value = source.get(column)
        if isinstance(value, str) and value.strip():
            values.append(value)
        elif isinstance(value, Mapping):
            values.extend(_text_values(value))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, str):
                    values.append(item)
                elif isinstance(item, Mapping):
                    values.extend(_text_values(item))
    return values


def _paths_from_text(text: str) -> list[str]:
    return [
        match.group(0)
        for regex in (ABSOLUTE_PATH_RE, RELATIVE_PATH_RE, ROOT_FILE_RE)
        for match in regex.finditer(text)
    ]


def _normalize_path(value: Any, *, project_roots: list[Path]) -> str | None:
    text = str(value or "").strip().strip("`'\"()[]{}<>,")
    text = re.sub(r"^(?:file://)", "", text)
    text = re.sub(r"(?::\d+)+$", "", text)
    text = text.replace("\\", "/")
    if not text or "://" in text or text.startswith("#"):
        return None

    path = Path(text).expanduser()
    if path.is_absolute():
        resolved = path.resolve(strict=False)
        for root in project_roots:
            try:
                return resolved.relative_to(root).as_posix()
            except ValueError:
                continue
        return None

    path = Path(text.lstrip("./"))
    parts = path.parts
    if not parts or parts[0] in {"..", ".git", ".claude"} or ".." in parts:
        return None
    normalized = path.as_posix()
    if "/" not in normalized and not ROOT_FILE_RE.fullmatch(normalized):
        return None
    return normalized


def _metadata(row: Mapping[str, Any]) -> tuple[Mapping[str, Any], bool]:
    for column in METADATA_COLUMNS:
        value = row.get(column)
        if value in (None, ""):
            continue
        if isinstance(value, Mapping):
            return value, False
        if isinstance(value, str):
            try:
                decoded = json.loads(value)
            except json.JSONDecodeError:
                return {}, True
            return decoded if isinstance(decoded, Mapping) else {}, False
    return {}, False


def _event_text(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    values = [*_text_values(row), *_text_values(metadata)]
    return "\n".join(dict.fromkeys(values)) or None


def _is_failure(
    status: str | None,
    text: str | None,
    metadata: Mapping[str, Any],
) -> bool:
    status_text = (status or "").lower()
    if status_text in {"error", "failed", "failure", "exception"}:
        return True
    if bool(metadata.get("is_error") or metadata.get("failed")):
        return True
    return bool(text and FAILURE_WORD_RE.search(text))


def _tool_name(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str:
    value = (
        _first_text(row, TOOL_COLUMNS)
        or _first_text(metadata, TOOL_COLUMNS)
        or _nested_text(metadata, ("tool", "name"))
        or _nested_text(metadata, ("tool_use", "name"))
        or ""
    )
    return re.sub(r"[^a-z0-9_.-]+", "_", value.strip().lower()).strip("_")


def _recommendation(stat: Mapping[str, Any]) -> str:
    path = str(stat["path"])
    if int(stat["error_count"]) > 0:
        if path.startswith("tests/"):
            return "Stabilize this flaky or failing test path before expanding coverage."
        return "Stabilize recent failures here and add a regression test around the affected behavior."
    if int(stat["tool_count"]) >= 3:
        if path.startswith("tests/"):
            return "Review repeated test edits for missing shared fixtures or brittle assertions."
        return "Add or refresh tests for this high-touch implementation file."
    if int(stat["mention_count"]) >= 2:
        if path.startswith("docs/") or path.lower().endswith((".md", ".rst")):
            return "Refresh documentation because this file is repeatedly referenced in sessions."
        return "Document ownership or usage notes so repeated Claude work has clearer context."
    return "Keep an eye on this file during routine maintenance."


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("db_or_rows must be rows, a sqlite3.Connection, or a Database-like object")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY name"
    ).fetchall()
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in rows}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _source_table(schema: dict[str, set[str]]) -> str | None:
    for table in EVENT_TABLE_CANDIDATES:
        if table in schema:
            return table
    return TEXT_TABLE if TEXT_TABLE in schema else None


def _missing_optional_columns(columns: set[str], *, event_table: bool) -> list[str]:
    expected = {
        "timestamp": TIMESTAMP_COLUMNS,
        "metadata": METADATA_COLUMNS,
    }
    if event_table:
        expected["tool_name"] = TOOL_COLUMNS
    else:
        expected["prompt_text"] = ("prompt_text", "content", "text", "message")
    return [
        name
        for name, variants in expected.items()
        if not any(column in columns for column in variants)
    ]


def _load_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: set[str],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    timestamp_column = _first_column(columns, TIMESTAMP_COLUMNS)
    select_columns = ", ".join(sorted(columns))
    params: list[Any] = []
    where = ""
    if timestamp_column:
        where = f"WHERE {timestamp_column} >= ?"
        params.append(cutoff.isoformat())
    cursor = conn.execute(
        f"SELECT {select_columns} FROM {table} {where} ORDER BY "
        f"{timestamp_column or 'rowid'} ASC, rowid ASC",
        params,
    )
    column_names = [description[0] for description in cursor.description]
    return [
        dict(row)
        if isinstance(row, Mapping)
        else dict(zip(column_names, row, strict=False))
        for row in cursor.fetchall()
    ]


def _filter_rows(rows: list[dict[str, Any]], *, cutoff: datetime) -> list[dict[str, Any]]:
    filtered = []
    for row in rows:
        metadata, _malformed = _metadata(row)
        timestamp = _first_text(row, TIMESTAMP_COLUMNS) or _first_text(metadata, TIMESTAMP_COLUMNS)
        parsed = _parse_datetime(timestamp)
        if parsed is not None and parsed < cutoff:
            continue
        filtered.append(row)
    return filtered


def _first_column(columns: set[str], candidates: tuple[str, ...]) -> str | None:
    for column in candidates:
        if column in columns:
            return column
    return None


def _first_text(source: Mapping[str, Any], candidates: tuple[str, ...]) -> str | None:
    for key in candidates:
        value = source.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _nested_text(source: Mapping[str, Any], keys: tuple[str, ...]) -> str | None:
    current: Any = source
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    if current is None:
        return None
    text = str(current).strip()
    return text or None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    try:
        return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _timestamp_sort(value: str | None) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value or "")


def _file_sort_key(item: dict[str, Any]) -> tuple[int, str, str]:
    return (-int(item["impact_score"]), _reverse_text(item["latest_seen"]), str(item["path"]))


def _reverse_text(value: Any) -> str:
    return "".join(chr(0x10FFFF - ord(char)) for char in str(value or ""))


def _mapping(row: Any) -> dict[str, Any]:
    return dict(row) if isinstance(row, Mapping) else dict(row)


def _looks_like_rows(value: Any) -> bool:
    return isinstance(value, Iterable) and not isinstance(
        value,
        (str, bytes, sqlite3.Connection),
    ) and not hasattr(value, "conn")
