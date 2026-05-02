"""Report Claude Code sessions with implementation work but no test evidence."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Mapping


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 20

SOURCE_TABLE_CANDIDATES = (
    "claude_session_events",
    "claude_tool_events",
    "claude_events",
    "claude_messages",
)
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
    "command",
    "output",
    "result",
    "stderr",
    "error_message",
)
PATH_COLUMNS = ("file_path", "filepath", "path", "paths", "files", "file")

IMPLEMENTATION_RE = re.compile(
    r"\b(implement(?:ed|ing)?|fix(?:ed|ing|es)?|bug[- ]?fix|regression|"
    r"behavior(?:al)? change|changed behavior|edit(?:ed|ing)?|updated?|modified|"
    r"refactor(?:ed|ing)?|patch(?:ed|ing)?|added?|removed?|write|wrote)\b",
    re.IGNORECASE,
)
TEST_EVIDENCE_RE = re.compile(
    r"\b(pytest|unittest|python -m pytest|uv run pytest|npm test|pnpm test|yarn test|"
    r"go test|cargo test|rspec|jest|vitest|playwright|validation command|"
    r"validated|verified|test run|tests? passed)\b|(?:^|[\s`\"'])tests?/",
    re.IGNORECASE,
)
TEST_FILE_RE = re.compile(r"(?:^|/)(?:test_[^/]+|[^/]+_test)\.[A-Za-z0-9]+$")
RELATIVE_PATH_RE = re.compile(
    r"(?<![\w./-])(?:\.?/)?(?:src|scripts|tests|docs|config|migrations|schemas?|"
    r"examples|app|apps|lib|libs|packages|tools)/"
    r"[\w@%+=:,./-]*[\w@%+=-]\.[A-Za-z0-9]{1,12}(?::\d+)?"
)
ROOT_FILE_RE = re.compile(
    r"(?<![\w./-])(?:README|Makefile|Dockerfile|pyproject|package|schema|"
    r"requirements|uv\.lock|poetry\.lock|Cargo|go\.mod|go\.sum)"
    r"(?:\.[A-Za-z0-9]{1,12})?(?::\d+)?"
)


@dataclass(frozen=True)
class ClaudeTestEvidenceGapItem:
    """One Claude session that appears to need test follow-up."""

    session_id: str
    date: str
    project_path: str | None
    changed_files: tuple[str, ...]
    evidence_signals: tuple[str, ...]
    missing_evidence_reason: str
    suggested_follow_up: str
    first_timestamp: str | None
    latest_timestamp: str | None
    source_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["changed_files"] = list(self.changed_files)
        payload["evidence_signals"] = list(self.evidence_signals)
        payload["source_tables"] = list(self.source_tables)
        return payload


@dataclass(frozen=True)
class ClaudeTestEvidenceGapReport:
    """Claude session test-evidence gap report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    gaps: tuple[ClaudeTestEvidenceGapItem, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_test_evidence_gap",
            "filters": dict(self.filters),
            "gaps": [gap.to_dict() for gap in self.gaps],
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_claude_test_evidence_gap_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeTestEvidenceGapReport:
    """Scan recent Claude session records for implementation work without tests."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at_dt = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at_dt - timedelta(days=days)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_end": generated_at_dt.isoformat(),
        "lookback_start": cutoff.isoformat(),
    }

    conn = _connection(db_or_conn)
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
        for row in _load_rows(conn, table, schema[table], cutoff=cutoff)
    ]
    sessions = _session_records(rows)
    gaps = tuple(item for item in (_gap_item(session) for session in sessions) if item)
    ordered_gaps = tuple(sorted(gaps, key=_gap_sort_key)[:limit])

    return ClaudeTestEvidenceGapReport(
        generated_at=generated_at_dt.isoformat(),
        filters=filters,
        totals={
            "gap_count": len(gaps),
            "implementation_session_count": sum(1 for session in sessions if session["implementation"]),
            "rows_scanned": len(rows),
            "sessions_scanned": len(sessions),
            "sessions_with_test_evidence": sum(1 for session in sessions if session["test_evidence"]),
        },
        gaps=ordered_gaps,
        source_tables=source_tables,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_claude_test_evidence_gap_json(report: ClaudeTestEvidenceGapReport) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_test_evidence_gap_text(report: ClaudeTestEvidenceGapReport) -> str:
    """Render a concise human-readable report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Claude Test Evidence Gap",
        f"Generated: {report.generated_at}",
        f"Window: days={filters['days']} limit={filters['limit']}",
        (
            "Totals: "
            f"sessions={totals['sessions_scanned']} implementation="
            f"{totals['implementation_session_count']} with_tests="
            f"{totals['sessions_with_test_evidence']} gaps={totals['gap_count']} "
            f"rows={totals['rows_scanned']}"
        ),
    ]
    if report.source_tables:
        lines.append("Source tables: " + ", ".join(report.source_tables))
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing optional columns: " + missing)

    if not report.gaps:
        lines.extend(["", "No Claude test evidence gaps found."])
        return "\n".join(lines)

    lines.extend(["", "Gaps:"])
    for gap in report.gaps:
        files = ", ".join(gap.changed_files[:5]) if gap.changed_files else "-"
        signals = ", ".join(gap.evidence_signals) if gap.evidence_signals else "-"
        lines.append(
            f"- session={gap.session_id} date={gap.date or '-'} "
            f"project={gap.project_path or '-'} files={files}"
        )
        lines.append(f"  signals={signals}")
        lines.append(f"  reason={gap.missing_evidence_reason}")
        lines.append(f"  follow_up={gap.suggested_follow_up}")
    return "\n".join(lines)


def _session_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = defaultdict(_empty_session)
    for row in rows:
        metadata = _metadata(row)
        session_id = _first_text(row, SESSION_COLUMNS) or _first_text(metadata, SESSION_COLUMNS)
        session_id = session_id or "unknown-session"
        record = grouped[session_id]
        record["session_id"] = session_id
        record["source_tables"].add(str(row.get("_source_table") or "unknown"))
        project_path = _first_text(row, ("project_path",)) or _first_text(metadata, ("project_path",))
        if project_path and not record["project_path"]:
            record["project_path"] = project_path
        timestamp = _first_text(row, TIMESTAMP_COLUMNS) or _first_text(metadata, TIMESTAMP_COLUMNS)
        if timestamp:
            record["timestamps"].append(timestamp)

        text = _event_text(row, metadata)
        paths = _extract_paths(row, metadata)
        source_paths = tuple(path for path in paths if not _is_test_path(path))
        record["changed_files"].update(paths)

        tool_name = (_first_text(row, TOOL_COLUMNS) or _first_text(metadata, TOOL_COLUMNS) or "").lower()
        if source_paths and tool_name in {"edit", "write", "multiedit", "notebookedit"}:
            record["implementation"].add("source_edit_tool")
        if source_paths:
            record["implementation"].add("source_file_mentioned")
        if IMPLEMENTATION_RE.search(text):
            record["implementation"].add("implementation_language")
        if TEST_EVIDENCE_RE.search(text) or any(_is_test_path(path) for path in paths):
            record["test_evidence"].add(_test_signal(text, paths))
    return sorted(grouped.values(), key=_session_sort_key)


def _gap_item(session: dict[str, Any]) -> ClaudeTestEvidenceGapItem | None:
    if not session["implementation"] or session["test_evidence"]:
        return None
    timestamps = sorted(session["timestamps"])
    first_timestamp = timestamps[0] if timestamps else None
    latest_timestamp = timestamps[-1] if timestamps else None
    date = (latest_timestamp or first_timestamp or "")[:10]
    changed_files = tuple(sorted(session["changed_files"]))
    return ClaudeTestEvidenceGapItem(
        session_id=session["session_id"],
        date=date,
        project_path=session["project_path"],
        changed_files=changed_files,
        evidence_signals=tuple(sorted(session["implementation"])),
        missing_evidence_reason="implementation_or_fix_signals_without_nearby_test_evidence",
        suggested_follow_up=_follow_up(changed_files),
        first_timestamp=first_timestamp,
        latest_timestamp=latest_timestamp,
        source_tables=tuple(sorted(session["source_tables"])),
    )


def _follow_up(changed_files: tuple[str, ...]) -> str:
    source_files = [path for path in changed_files if not _is_test_path(path)]
    if source_files:
        return f"run targeted tests for {source_files[0]}"
    return "rerun the relevant validation command and attach the output to the session notes"


def _load_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: set[str],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, "id", "rowid"),
        *[_aliased_first_expr(columns, names, names[0]) for names in (
            SESSION_COLUMNS,
            TIMESTAMP_COLUMNS,
            TOOL_COLUMNS,
            STATUS_COLUMNS,
        )],
        _column_expr(columns, "project_path"),
        *[_column_expr(columns, column) for column in (*METADATA_COLUMNS, *TEXT_COLUMNS, *PATH_COLUMNS)],
    ]
    select_sql = ", ".join(dict.fromkeys(select_columns))
    where = []
    params: list[Any] = []
    timestamp_column = _first_existing(columns, TIMESTAMP_COLUMNS)
    if timestamp_column:
        where.append(f"{timestamp_column} >= ?")
        params.append(cutoff.isoformat())
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    order_sql = f"{timestamp_column} ASC, id ASC" if timestamp_column and "id" in columns else "rowid ASC"
    cursor = conn.execute(
        f"""SELECT {select_sql}
            FROM {table}
            {where_sql}
            ORDER BY {order_sql}""",
        params,
    )
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


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in rows}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _missing_optional_columns(columns: set[str]) -> tuple[str, ...]:
    optional = (
        "project_path",
        *TOOL_COLUMNS,
        *STATUS_COLUMNS,
        *METADATA_COLUMNS,
        "response_text",
        "content",
    )
    return tuple(column for column in optional if column not in columns)


def _column_expr(columns: set[str], column: str, fallback: str = "NULL") -> str:
    return column if column in columns else f"{fallback} AS {column}"


def _aliased_first_expr(columns: set[str], names: tuple[str, ...], alias: str) -> str:
    column = _first_existing(columns, names)
    return f"{column} AS {alias}" if column and column != alias else _column_expr(columns, alias)


def _first_existing(columns: set[str], names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in columns), None)


def _empty_session() -> dict[str, Any]:
    return {
        "changed_files": set(),
        "implementation": set(),
        "project_path": None,
        "session_id": "unknown-session",
        "source_tables": set(),
        "test_evidence": set(),
        "timestamps": [],
    }


def _metadata(row: Mapping[str, Any]) -> dict[str, Any]:
    for column in METADATA_COLUMNS:
        value = row.get(column)
        if isinstance(value, Mapping):
            return dict(value)
        if isinstance(value, str) and value.strip():
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, Mapping):
                return dict(parsed)
    return {}


def _event_text(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for source in (row, metadata):
        for column in (*TEXT_COLUMNS, *PATH_COLUMNS, *STATUS_COLUMNS):
            value = source.get(column)
            if isinstance(value, (str, int, float)):
                parts.append(str(value))
            elif isinstance(value, (list, tuple)):
                parts.extend(str(item) for item in value)
    return "\n".join(parts)


def _extract_paths(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> tuple[str, ...]:
    paths: set[str] = set()
    for source in (row, metadata):
        for column in PATH_COLUMNS:
            _add_path_value(paths, source.get(column))
    text = _event_text(row, metadata)
    for regex in (RELATIVE_PATH_RE, ROOT_FILE_RE):
        for match in regex.finditer(text):
            _add_path_value(paths, match.group(0))
    return tuple(sorted(paths))


def _add_path_value(paths: set[str], value: Any) -> None:
    if value is None:
        return
    if isinstance(value, Mapping):
        for key in PATH_COLUMNS:
            _add_path_value(paths, value.get(key))
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _add_path_value(paths, item)
        return
    text = str(value).strip().strip("`'\"")
    if not text:
        return
    for candidate in re.split(r"[\s,]+", text):
        normalized = _normalize_path(candidate)
        if normalized:
            paths.add(normalized)


def _normalize_path(value: str) -> str | None:
    text = value.strip().strip("`'\"").removeprefix("./")
    text = re.sub(r":\d+(?::\d+)?$", "", text)
    if not text or "/" not in text and "." not in text:
        return None
    if text.startswith("/"):
        parts = Path(text).parts
        for marker in ("src", "scripts", "tests", "docs", "config", "migrations", "examples", "app", "lib", "packages", "tools"):
            if marker in parts:
                text = "/".join(parts[parts.index(marker) :])
                break
        else:
            return None
    if text.startswith(("src/", "scripts/", "tests/", "docs/", "config/", "migrations/", "examples/", "app/", "lib/", "packages/", "tools/")):
        return text
    if re.match(r"^(README|Makefile|Dockerfile|pyproject|package|schema|requirements|uv\.lock|poetry\.lock|Cargo|go\.mod|go\.sum)", text):
        return text
    return None


def _is_test_path(path: str) -> bool:
    normalized = path.strip().lower()
    return normalized.startswith("tests/") or bool(TEST_FILE_RE.search(normalized))


def _test_signal(text: str, paths: tuple[str, ...]) -> str:
    if any(_is_test_path(path) for path in paths):
        return "test_file_or_tests_directory"
    match = TEST_EVIDENCE_RE.search(text)
    return match.group(0).strip() if match else "test_evidence"


def _first_text(source: Mapping[str, Any], columns: tuple[str, ...]) -> str | None:
    for column in columns:
        value = source.get(column)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _session_sort_key(session: Mapping[str, Any]) -> tuple[str, str]:
    timestamps = sorted(session.get("timestamps") or [])
    return (timestamps[0] if timestamps else "", str(session.get("session_id") or ""))


def _gap_sort_key(item: ClaudeTestEvidenceGapItem) -> tuple[str, str]:
    return (item.latest_timestamp or item.first_timestamp or "", item.session_id)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
