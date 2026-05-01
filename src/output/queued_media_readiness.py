"""Read-only media readiness audit for visual publish queue rows."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
VALID_PLATFORMS = ("all", "x", "bluesky", "linkedin", "mastodon")
ACTIVE_QUEUE_STATUSES = ("queued", "failed", "held")
READY = "ready"
WARNING = "warning"
BLOCKED = "blocked"
SUPPORTED_EXTENSIONS = (".gif", ".jpeg", ".jpg", ".png", ".webp")
MAX_IMAGE_BYTES_BY_PLATFORM = {
    "x": 5 * 1024 * 1024,
    "bluesky": 1 * 1024 * 1024,
    "linkedin": 5 * 1024 * 1024,
    "mastodon": 8 * 1024 * 1024,
}
BLOCKING_PUBLICATION_CATEGORIES = {"auth", "media", "validation"}
WARN_PUBLICATION_CATEGORIES = {"network", "rate_limit", "unknown"}


@dataclass(frozen=True)
class QueuedMediaReadinessReason:
    """One deterministic media readiness finding."""

    code: str
    message: str
    severity: str
    platform: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class QueuedMediaReadinessItem:
    """Media readiness for one publish queue row."""

    queue_id: int
    content_id: int | None
    platform: str
    target_platforms: tuple[str, ...]
    status: str
    scheduled_at: str | None
    queue_status: str | None
    content_type: str | None
    image_path: str | None
    image_alt_text_present: bool
    image_prompt_present: bool
    file_size_bytes: int | None
    reasons: tuple[QueuedMediaReadinessReason, ...]

    @property
    def blocked(self) -> bool:
        return self.status == BLOCKED

    def to_dict(self) -> dict[str, Any]:
        return {
            "queue_id": self.queue_id,
            "content_id": self.content_id,
            "platform": self.platform,
            "target_platforms": list(self.target_platforms),
            "status": self.status,
            "scheduled_at": self.scheduled_at,
            "queue_status": self.queue_status,
            "content_type": self.content_type,
            "image_path": self.image_path,
            "image_alt_text_present": self.image_alt_text_present,
            "image_prompt_present": self.image_prompt_present,
            "file_size_bytes": self.file_size_bytes,
            "reasons": [reason.to_dict() for reason in self.reasons],
        }


@dataclass(frozen=True)
class QueuedMediaReadinessReport:
    """Read-only queued visual media audit report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    items: tuple[QueuedMediaReadinessItem, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocker_count(self) -> int:
        return self.totals.get(BLOCKED, 0)

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": self.filters,
            "totals": self.totals,
            "items": [item.to_dict() for item in self.items],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_queued_media_readiness_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str = "all",
    now: datetime | None = None,
) -> QueuedMediaReadinessReport:
    """Inspect active queued visual content for local media readiness."""
    if days <= 0:
        raise ValueError("days must be positive")
    selected_platform = str(platform).strip().lower()
    if selected_platform not in VALID_PLATFORMS:
        raise ValueError(f"platform must be one of: {', '.join(VALID_PLATFORMS)}")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: set[str] = set()
    missing_columns: dict[str, tuple[str, ...]] = {}

    rows = _queue_rows(
        conn,
        schema,
        selected_platform,
        cutoff,
        missing_tables,
        missing_columns,
    )
    publication_states = _publication_states(conn, schema, rows)
    items = tuple(
        _classify_row(row, publication_states.get(row["queue_id"], ())) for row in rows
    )
    totals = {
        READY: sum(1 for item in items if item.status == READY),
        WARNING: sum(1 for item in items if item.status == WARNING),
        BLOCKED: sum(1 for item in items if item.status == BLOCKED),
        "total": len(items),
    }
    return QueuedMediaReadinessReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "platform": selected_platform,
            "cutoff": cutoff.isoformat(),
            "queue_statuses": list(ACTIVE_QUEUE_STATUSES),
        },
        totals=totals,
        items=items,
        missing_tables=tuple(sorted(missing_tables)),
        missing_columns=missing_columns,
    )


def format_queued_media_readiness_json(report: QueuedMediaReadinessReport) -> str:
    """Render the audit report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_queued_media_readiness_text(report: QueuedMediaReadinessReport) -> str:
    """Render a stable operator-facing media readiness table."""
    lines = [
        "Queued Media Readiness Audit",
        f"Generated: {report.generated_at}",
        f"Filters: days={report.filters['days']} platform={report.filters['platform']}",
        (
            "Totals: "
            f"ready={report.totals[READY]} "
            f"warning={report.totals[WARNING]} "
            f"blocked={report.totals[BLOCKED]} "
            f"total={report.totals['total']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        details = ", ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + details)
    if not report.items:
        lines.append("No queued visual media items matched the filters.")
        return "\n".join(lines)

    columns = [
        ("queue_id", "QUEUE", 6),
        ("content_id", "CID", 6),
        ("platform", "PLATFORM", 10),
        ("queue_status", "QSTATUS", 8),
        ("status", "STATUS", 7),
        ("primary_reason", "REASON", 28),
        ("image_path", "IMAGE_PATH", 42),
    ]
    lines.append("")
    lines.append("  ".join(label.ljust(width) for _, label, width in columns))
    lines.append("  ".join("-" * width for _, _, width in columns))
    for item in report.items:
        row = item.to_dict()
        row["primary_reason"] = item.reasons[0].code if item.reasons else "ok"
        lines.append(
            "  ".join(_clip(row.get(key), width).ljust(width) for key, _, width in columns)
        )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _queue_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    platform: str,
    cutoff: datetime,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> list[dict[str, Any]]:
    required_tables = {"publish_queue", "generated_content"}
    absent = required_tables.difference(schema)
    if absent:
        missing_tables.update(absent)
        return []

    pq_required = ("id", "content_id", "scheduled_at")
    gc_required = ("id",)
    _record_missing_columns(schema, "publish_queue", pq_required, missing_columns)
    _record_missing_columns(schema, "generated_content", gc_required, missing_columns)
    if (
        "publish_queue" in missing_columns
        and any(column in missing_columns["publish_queue"] for column in pq_required)
    ) or (
        "generated_content" in missing_columns
        and any(column in missing_columns["generated_content"] for column in gc_required)
    ):
        return []

    pq = schema["publish_queue"]
    gc = schema["generated_content"]
    filters = [_status_filter(pq)]
    params: list[Any] = []
    if platform != "all" and "platform" in pq:
        filters.append("pq.platform = ?")
        params.append(platform)
    where = " AND ".join(filter(None, filters))
    rows = conn.execute(
        f"""SELECT
               pq.id AS queue_id,
               pq.content_id AS content_id,
               {_column_expr(pq, "scheduled_at", alias="pq")} AS scheduled_at,
               {_column_expr(pq, "platform", "'all'", alias="pq")} AS platform,
               {_column_expr(pq, "status", "'queued'", alias="pq")} AS queue_status,
               {_column_expr(pq, "created_at", alias="pq")} AS queue_created_at,
               gc.id AS generated_content_id,
               {_column_expr(gc, "content_type", alias="gc")} AS content_type,
               {_column_expr(gc, "image_path", alias="gc")} AS image_path,
               {_column_expr(gc, "image_alt_text", alias="gc")} AS image_alt_text,
               {_column_expr(gc, "image_prompt", alias="gc")} AS image_prompt
           FROM publish_queue pq
           LEFT JOIN generated_content gc ON gc.id = pq.content_id
           WHERE {where}
           ORDER BY {_column_expr(pq, "scheduled_at", alias="pq")} ASC, pq.id ASC""",
        params,
    ).fetchall()
    return [
        dict(row)
        for row in rows
        if _within_days(dict(row), cutoff) and _is_visual_queue_row(dict(row))
    ]


def _record_missing_columns(
    schema: dict[str, set[str]],
    table: str,
    required: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> None:
    missing = tuple(column for column in required if column not in schema.get(table, set()))
    if missing:
        missing_columns[table] = missing


def _publication_states(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    rows: list[dict[str, Any]],
) -> dict[int, tuple[dict[str, Any], ...]]:
    columns = schema.get("content_publications")
    if not rows or not columns or not {"content_id", "platform"}.issubset(columns):
        return {}
    content_ids = sorted({row["content_id"] for row in rows if row["content_id"] is not None})
    if not content_ids:
        return {}
    placeholders = ",".join("?" for _ in content_ids)
    select = {
        "content_id": "content_id",
        "platform": "platform",
        "status": _column_expr(columns, "status"),
        "error": _column_expr(columns, "error"),
        "error_category": _column_expr(columns, "error_category"),
    }
    publication_rows = [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   {select['content_id']} AS content_id,
                   {select['platform']} AS platform,
                   {select['status']} AS status,
                   {select['error']} AS error,
                   {select['error_category']} AS error_category
               FROM content_publications
               WHERE content_id IN ({placeholders})
               ORDER BY content_id ASC, platform ASC""",
            content_ids,
        ).fetchall()
    ]
    by_content: dict[int, list[dict[str, Any]]] = {}
    for state in publication_rows:
        by_content.setdefault(state["content_id"], []).append(state)
    by_queue: dict[int, tuple[dict[str, Any], ...]] = {}
    for row in rows:
        targets = set(_target_platforms(row.get("platform")))
        by_queue[row["queue_id"]] = tuple(
            state
            for state in by_content.get(row["content_id"], [])
            if state.get("platform") in targets
        )
    return by_queue


def _classify_row(
    row: dict[str, Any],
    publication_states: tuple[dict[str, Any], ...],
) -> QueuedMediaReadinessItem:
    reasons: list[QueuedMediaReadinessReason] = []
    image_path = _clean_text(row.get("image_path"))
    target_platforms = _target_platforms(row.get("platform"))
    file_size = _file_size(image_path)

    if row.get("generated_content_id") is None:
        reasons.append(
            _reason("missing_generated_content", "Queue row has no generated_content record.")
        )
    if not target_platforms:
        reasons.append(
            _reason(
                "unsupported_queue_platform",
                f"Queue platform is not supported for media audit: {row.get('platform')}",
            )
        )
    if _clean_text(row.get("queue_status")) == "held":
        reasons.append(
            _reason("queue_held", "Queue item is currently held.", severity=WARNING)
        )
    if not image_path:
        reasons.append(
            _reason("missing_image_path", "Visual queue item has no image_path.")
        )
    else:
        path = Path(image_path).expanduser()
        suffix = path.suffix.lower()
        if suffix not in SUPPORTED_EXTENSIONS:
            reasons.append(
                _reason(
                    "unsupported_image_extension",
                    (
                        f"Image extension {suffix or '[none]'} is not supported; "
                        f"expected one of {', '.join(SUPPORTED_EXTENSIONS)}."
                    ),
                )
            )
        if not path.is_file():
            reasons.append(
                _reason("missing_image_file", f"Image file does not exist: {image_path}")
            )
        elif file_size is not None:
            for target in target_platforms:
                limit = MAX_IMAGE_BYTES_BY_PLATFORM.get(target)
                if limit and file_size > limit:
                    reasons.append(
                        _reason(
                            "image_file_too_large",
                            (
                                f"Image is {file_size} bytes; {target} limit is "
                                f"{limit} bytes."
                            ),
                            platform=target,
                        )
                    )

    if not _clean_text(row.get("image_alt_text")):
        reasons.append(
            _reason("missing_alt_text", "Visual queue item has no image_alt_text.")
        )
    if not _clean_text(row.get("image_prompt")):
        reasons.append(
            _reason(
                "missing_image_prompt",
                "Visual queue item has no image_prompt for operator review.",
                severity=WARNING,
            )
        )
    reasons.extend(_publication_reasons(publication_states))

    if any(reason.severity == BLOCKED for reason in reasons):
        status = BLOCKED
    elif any(reason.severity == WARNING for reason in reasons):
        status = WARNING
    else:
        status = READY

    return QueuedMediaReadinessItem(
        queue_id=int(row["queue_id"]),
        content_id=row.get("content_id"),
        platform=_clean_text(row.get("platform")) or "all",
        target_platforms=target_platforms,
        status=status,
        scheduled_at=row.get("scheduled_at"),
        queue_status=row.get("queue_status"),
        content_type=row.get("content_type"),
        image_path=image_path,
        image_alt_text_present=bool(_clean_text(row.get("image_alt_text"))),
        image_prompt_present=bool(_clean_text(row.get("image_prompt"))),
        file_size_bytes=file_size,
        reasons=tuple(reasons),
    )


def _publication_reasons(
    publication_states: tuple[dict[str, Any], ...],
) -> list[QueuedMediaReadinessReason]:
    reasons: list[QueuedMediaReadinessReason] = []
    for state in publication_states:
        status = _clean_text(state.get("status"))
        category = _clean_text(state.get("error_category")).lower()
        if status != "failed":
            continue
        if category in BLOCKING_PUBLICATION_CATEGORIES:
            severity = BLOCKED
            code = "platform_publication_blocker"
        elif category in WARN_PUBLICATION_CATEGORIES:
            severity = WARNING
            code = "platform_publication_warning"
        else:
            severity = WARNING
            code = "platform_publication_warning"
        error = _clean_text(state.get("error"))
        message = (
            f"{state.get('platform')} publication is failed"
            + (f" with {category}" if category else "")
            + (f": {error}" if error else ".")
        )
        reasons.append(
            _reason(code, message, severity=severity, platform=state.get("platform"))
        )
    return reasons


def _target_platforms(platform: Any) -> tuple[str, ...]:
    value = _clean_text(platform).lower() or "all"
    if value == "all":
        return ("x", "bluesky")
    if value in VALID_PLATFORMS and value != "all":
        return (value,)
    return ()


def _status_filter(columns: set[str]) -> str:
    if "status" not in columns:
        return "1 = 1"
    statuses = ", ".join(f"'{status}'" for status in ACTIVE_QUEUE_STATUSES)
    return f"pq.status IN ({statuses})"


def _within_days(row: dict[str, Any], cutoff: datetime) -> bool:
    candidate = row.get("queue_created_at") or row.get("scheduled_at")
    if not candidate:
        return True
    parsed = _parse_datetime(candidate)
    if parsed is None:
        return True
    return parsed >= cutoff


def _is_visual_queue_row(row: dict[str, Any]) -> bool:
    return bool(_clean_text(row.get("image_path"))) or bool(
        _clean_text(row.get("image_prompt"))
    ) or row.get("content_type") in {"x_visual", "visual"}


def _file_size(image_path: str | None) -> int | None:
    if not image_path:
        return None
    try:
        path = Path(image_path).expanduser()
        return path.stat().st_size if path.is_file() else None
    except OSError:
        return None


def _reason(
    code: str,
    message: str,
    *,
    severity: str = BLOCKED,
    platform: str | None = None,
) -> QueuedMediaReadinessReason:
    return QueuedMediaReadinessReason(
        code=code,
        message=message,
        severity=severity,
        platform=platform,
    )


def _column_expr(
    columns: set[str],
    column: str,
    default: str = "NULL",
    *,
    alias: str | None = None,
) -> str:
    if column not in columns:
        return default
    return f"{alias}.{column}" if alias else column


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clip(value: Any, width: int) -> str:
    text = "-" if value in (None, "") else str(value).replace("\n", " ")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."
