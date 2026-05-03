"""Report how broadly Claude Code sessions touched workspace files."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Mapping


DEFAULT_LIMIT = 50
BROAD_FILE_THRESHOLD = 8
BROAD_EXTENSION_THRESHOLD = 4
BROAD_DIRECTORY_THRESHOLD = 4

SOURCE_TABLE_CANDIDATES = (
    "claude_session_events",
    "claude_tool_events",
    "claude_events",
    "claude_messages",
)
SESSION_COLUMNS = ("session_id", "sessionId")
TIMESTAMP_COLUMNS = ("timestamp", "created_at", "event_time", "event_at")
PROJECT_COLUMNS = ("project_path", "cwd", "working_directory", "project")
TOOL_COLUMNS = ("tool_name", "tool", "toolName", "name")
COMMAND_COLUMNS = ("command", "cmd", "shell_command", "input")
METADATA_COLUMNS = ("metadata", "raw_metadata", "event_json", "payload")
PATH_COLUMNS = ("file_path", "filepath", "path", "paths", "files", "file")
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
ABSOLUTE_PATH_RE = re.compile(
    r"(?<![\w])/(?:[^\s`'\"<>|]+/)*[^\s`'\"<>|]+\.[A-Za-z0-9]{1,12}(?::\d+)?"
)


@dataclass(frozen=True)
class ClaudeSessionFileTouchBreadthRow:
    """File-touch breadth metrics for one Claude session."""

    session_id: str
    project_path: str | None
    first_seen_at: str | None
    last_seen_at: str | None
    unique_file_count: int
    extension_breakdown: dict[str, int]
    command_count: int
    breadth_classification: str
    top_files: tuple[str, ...]
    top_directories: dict[str, int]
    row_count: int

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["top_files"] = list(self.top_files)
        return payload


@dataclass(frozen=True)
class ClaudeSessionFileTouchBreadthReport:
    """Claude session file-touch breadth report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionFileTouchBreadthRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_file_touch_breadth",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_claude_session_file_touch_breadth_report(
    db_or_rows: Any,
    *,
    limit: int = DEFAULT_LIMIT,
    repo_root: str | Path | None = None,
    now: datetime | None = None,
) -> ClaudeSessionFileTouchBreadthReport:
    """Build a deterministic per-session file-touch breadth report."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    root = Path(repo_root or Path.cwd()).expanduser().resolve(strict=False)
    filters = {"limit": limit, "repo_root": str(root)}
    missing_columns: dict[str, tuple[str, ...]] = {}

    if _looks_like_rows(db_or_rows):
        rows = [_mapping(row) for row in db_or_rows]
        source_tables: tuple[str, ...] = ("rows",)
        missing_tables: tuple[str, ...] = ()
    else:
        conn = _connection(db_or_rows)
        schema = _schema(conn)
        source_tables = tuple(table for table in SOURCE_TABLE_CANDIDATES if table in schema)
        missing_tables = () if source_tables else SOURCE_TABLE_CANDIDATES
        missing_columns = {
            table: _missing_optional_columns(schema[table])
            for table in source_tables
            if _missing_optional_columns(schema[table])
        }
        rows = [
            row
            for table in source_tables
            for row in _load_rows(conn, table, schema[table])
        ]

    malformed_metadata_count = 0
    sessions: dict[str, dict[str, Any]] = defaultdict(_empty_session)
    for row in rows:
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        session_id = (
            _first_text(row, SESSION_COLUMNS)
            or _first_text(metadata, SESSION_COLUMNS)
            or "unknown-session"
        )
        project_path = _first_text(row, PROJECT_COLUMNS) or _first_text(metadata, PROJECT_COLUMNS)
        timestamp = _first_text(row, TIMESTAMP_COLUMNS) or _first_text(metadata, TIMESTAMP_COLUMNS)
        timestamp_sort = _timestamp_sort(timestamp)
        stat = sessions[session_id]
        stat["session_id"] = session_id
        stat["row_count"] += 1
        if project_path and not stat["project_path"]:
            stat["project_path"] = project_path
        if timestamp_sort and (not stat["first_seen_sort"] or timestamp_sort < stat["first_seen_sort"]):
            stat["first_seen_at"] = timestamp
            stat["first_seen_sort"] = timestamp_sort
        if timestamp_sort >= stat["last_seen_sort"]:
            stat["last_seen_at"] = timestamp
            stat["last_seen_sort"] = timestamp_sort
        if _command(row, metadata):
            stat["command_count"] += 1
        for path in _extract_paths(row, metadata, repo_root=root):
            stat["file_mentions"][path] += 1

    report_rows = [_session_row(stat) for stat in sessions.values()]
    report_rows.sort(key=_row_sort_key)

    return ClaudeSessionFileTouchBreadthReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "broad_session_count": sum(
                1 for row in report_rows if row.breadth_classification == "broad"
            ),
            "malformed_metadata_count": malformed_metadata_count,
            "narrow_session_count": sum(
                1 for row in report_rows if row.breadth_classification == "narrow"
            ),
            "no_file_session_count": sum(
                1 for row in report_rows if row.breadth_classification == "no_files"
            ),
            "rows_scanned": len(rows),
            "session_count": len(report_rows),
        },
        rows=tuple(report_rows[:limit]),
        source_tables=source_tables,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_claude_session_file_touch_breadth_json(
    report: ClaudeSessionFileTouchBreadthReport,
) -> str:
    """Serialize a Claude session file-touch breadth report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _empty_session() -> dict[str, Any]:
    return {
        "command_count": 0,
        "file_mentions": Counter(),
        "first_seen_at": None,
        "first_seen_sort": "",
        "last_seen_at": None,
        "last_seen_sort": "",
        "project_path": None,
        "row_count": 0,
        "session_id": "",
    }


def _session_row(stat: Mapping[str, Any]) -> ClaudeSessionFileTouchBreadthRow:
    file_mentions: Counter[str] = stat["file_mentions"]
    files = sorted(file_mentions)
    extensions = Counter(_extension(path) for path in files)
    directories = Counter(_top_directory(path) for path in files)
    return ClaudeSessionFileTouchBreadthRow(
        session_id=str(stat["session_id"]),
        project_path=stat["project_path"],
        first_seen_at=stat["first_seen_at"],
        last_seen_at=stat["last_seen_at"],
        unique_file_count=len(files),
        extension_breakdown=dict(sorted(extensions.items())),
        command_count=int(stat["command_count"]),
        breadth_classification=_classification(len(files), len(extensions), len(directories)),
        top_files=tuple(
            path
            for path, _count in sorted(
                file_mentions.items(),
                key=lambda item: (-int(item[1]), item[0]),
            )[:10]
        ),
        top_directories=dict(
            sorted(directories.items(), key=lambda item: (-int(item[1]), item[0]))[:10]
        ),
        row_count=int(stat["row_count"]),
    )


def _classification(file_count: int, extension_count: int, directory_count: int) -> str:
    if file_count == 0:
        return "no_files"
    if (
        file_count >= BROAD_FILE_THRESHOLD
        or extension_count >= BROAD_EXTENSION_THRESHOLD
        or directory_count >= BROAD_DIRECTORY_THRESHOLD
    ):
        return "broad"
    return "narrow"


def _extract_paths(row: Mapping[str, Any], metadata: Mapping[str, Any], *, repo_root: Path) -> list[str]:
    project_roots = [repo_root]
    for source in (row, metadata):
        project_path = _first_text(source, PROJECT_COLUMNS)
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
        values: list[str] = []
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


def _command(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    for source in (row, metadata):
        for column in COMMAND_COLUMNS:
            value = source.get(column)
            if isinstance(value, str) and value.strip():
                return value
            if isinstance(value, Mapping):
                nested = _first_text(value, COMMAND_COLUMNS)
                if nested:
                    return nested
    for path in (
        ("tool_input", "command"),
        ("input", "command"),
        ("tool", "input", "command"),
        ("tool_use", "input", "command"),
    ):
        nested = _nested_text(metadata, path)
        if nested:
            return nested
    return None


def _metadata(row: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    for column in METADATA_COLUMNS:
        value = row.get(column)
        if isinstance(value, Mapping):
            return dict(value), False
        if isinstance(value, str) and value.strip():
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return {}, True
            return (dict(parsed), False) if isinstance(parsed, Mapping) else ({}, False)
    return {}, False


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


def _missing_optional_columns(columns: set[str]) -> tuple[str, ...]:
    expected_groups = {
        "session_id": SESSION_COLUMNS,
        "timestamp": TIMESTAMP_COLUMNS,
        "project_path": PROJECT_COLUMNS,
        "command": COMMAND_COLUMNS,
        "metadata": METADATA_COLUMNS,
    }
    return tuple(
        name
        for name, variants in expected_groups.items()
        if not any(column in columns for column in variants)
    )


def _load_rows(conn: sqlite3.Connection, table: str, columns: set[str]) -> list[dict[str, Any]]:
    select_columns = ", ".join(sorted(columns))
    timestamp_column = _first_existing(columns, TIMESTAMP_COLUMNS)
    order_sql = f"{timestamp_column or 'rowid'} ASC, rowid ASC"
    cursor = conn.execute(f"SELECT {select_columns} FROM {table} ORDER BY {order_sql}")
    column_names = [description[0] for description in cursor.description]
    return [
        {
            **(
                dict(row)
                if isinstance(row, Mapping)
                else dict(zip(column_names, row, strict=False))
            ),
            "_source_table": table,
        }
        for row in cursor.fetchall()
    ]


def _extension(path: str) -> str:
    name = Path(path).name
    if name in {"Makefile", "Dockerfile"}:
        return "[no extension]"
    suffix = Path(path).suffix.lower()
    return suffix or "[no extension]"


def _top_directory(path: str) -> str:
    parts = Path(path).parts
    return parts[0] if len(parts) > 1 else "[repo root]"


def _first_existing(columns: set[str], candidates: tuple[str, ...]) -> str | None:
    for column in candidates:
        if column in columns:
            return column
    return None


def _first_text(source: Mapping[str, Any], columns: tuple[str, ...]) -> str | None:
    for column in columns:
        value = source.get(column)
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


def _row_sort_key(row: ClaudeSessionFileTouchBreadthRow) -> tuple[int, int, str, str]:
    class_rank = {"broad": 0, "narrow": 1, "no_files": 2}.get(row.breadth_classification, 3)
    return (
        class_rank,
        -row.unique_file_count,
        _reverse_text(row.last_seen_at),
        row.session_id,
    )


def _reverse_text(value: Any) -> str:
    return "".join(chr(0x10FFFF - ord(char)) for char in str(value or ""))


def _mapping(row: Any) -> dict[str, Any]:
    return dict(row) if isinstance(row, Mapping) else dict(row)


def _looks_like_rows(value: Any) -> bool:
    return isinstance(value, Iterable) and not isinstance(
        value,
        (str, bytes, sqlite3.Connection),
    ) and not hasattr(value, "conn")
