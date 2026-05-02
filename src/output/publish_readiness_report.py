"""Format readiness report for queued publish items."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any

from .x_client import parse_thread_content


READY = "ready"
BLOCKED = "blocked"
WARNING = "warning"
QUEUE_STATUSES = ("queued",)
SUPPORTED_DESTINATIONS = ("x_post", "x_thread", "newsletter", "blog")
CONTENT_TYPE_DESTINATIONS = {
    "x_post": "x_post",
    "x_visual": "x_post",
    "x_thread": "x_thread",
    "newsletter": "newsletter",
    "newsletter_issue": "newsletter",
    "blog": "blog",
    "blog_post": "blog",
    "long_post": "blog",
}


@dataclass(frozen=True)
class PublishFormatFinding:
    """One actionable format-readiness finding for a queued item."""

    item_id: int
    queue_id: int
    destination: str
    destination_id: str | None
    severity: str
    missing_fields: tuple[str, ...] = ()
    invalid_fields: tuple[str, ...] = ()
    fix_hint: str = ""
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["missing_fields"] = list(self.missing_fields)
        payload["invalid_fields"] = list(self.invalid_fields)
        return payload


@dataclass(frozen=True)
class PublishFormatItem:
    """Format-readiness status for one queued publish item."""

    item_id: int
    queue_id: int
    destination: str
    destination_id: str | None
    status: str
    scheduled_at: str | None
    content_type: str | None
    findings: tuple[PublishFormatFinding, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "item_id": self.item_id,
            "queue_id": self.queue_id,
            "destination": self.destination,
            "destination_id": self.destination_id,
            "status": self.status,
            "scheduled_at": self.scheduled_at,
            "content_type": self.content_type,
            "findings": [finding.to_dict() for finding in self.findings],
        }


@dataclass(frozen=True)
class PublishFormatReadinessReport:
    """Grouped publish format readiness report."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    items: tuple[PublishFormatItem, ...]
    findings_by_destination: dict[str, tuple[PublishFormatFinding, ...]]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocker_count(self) -> int:
        return int(self.totals.get(BLOCKED, 0))

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "generated_at": self.generated_at,
            "filters": self.filters,
            "totals": self.totals,
            "items": [item.to_dict() for item in self.items],
            "findings_by_destination": {
                destination: [finding.to_dict() for finding in findings]
                for destination, findings in sorted(self.findings_by_destination.items())
            },
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_publish_format_readiness_report(
    db_or_conn: Any,
    *,
    now: datetime | None = None,
) -> PublishFormatReadinessReport:
    """Validate minimum destination-format fields for queued unpublished items."""
    generated_at = _as_utc(now or datetime.now(timezone.utc)).isoformat()
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: set[str] = set()
    missing_columns: dict[str, tuple[str, ...]] = {}
    rows = _queued_unpublished_rows(conn, schema, missing_tables, missing_columns)
    items = tuple(_validate_row(row) for row in rows)
    findings = tuple(finding for item in items for finding in item.findings)
    by_destination: dict[str, list[PublishFormatFinding]] = {
        destination: [] for destination in SUPPORTED_DESTINATIONS
    }
    for finding in findings:
        by_destination.setdefault(finding.destination, []).append(finding)

    totals_by_destination = {
        destination: {
            "items": sum(1 for item in items if item.destination == destination),
            "findings": len(by_destination.get(destination, ())),
        }
        for destination in sorted(set(SUPPORTED_DESTINATIONS).union(by_destination))
    }
    totals = {
        READY: sum(1 for item in items if item.status == READY),
        WARNING: sum(1 for item in items if item.status == WARNING),
        BLOCKED: sum(1 for item in items if item.status == BLOCKED),
        "items": len(items),
        "findings": len(findings),
        "by_destination": totals_by_destination,
    }
    return PublishFormatReadinessReport(
        artifact_type="publish_format_readiness",
        generated_at=generated_at,
        filters={"queue_statuses": list(QUEUE_STATUSES), "published": 0},
        totals=totals,
        items=items,
        findings_by_destination={
            destination: tuple(destination_findings)
            for destination, destination_findings in by_destination.items()
        },
        missing_tables=tuple(sorted(missing_tables)),
        missing_columns=missing_columns,
    )


def format_publish_format_readiness_json(
    report: PublishFormatReadinessReport,
) -> str:
    """Render the report as deterministic grouped JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _queued_unpublished_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> list[dict[str, Any]]:
    required_tables = {"publish_queue", "generated_content"}
    absent = required_tables.difference(schema)
    if absent:
        missing_tables.update(absent)
        return []

    pq_required = ("id", "content_id")
    gc_required = ("id",)
    _record_missing_columns(schema, "publish_queue", pq_required, missing_columns)
    _record_missing_columns(schema, "generated_content", gc_required, missing_columns)
    if missing_columns:
        return []

    pq = schema["publish_queue"]
    gc = schema["generated_content"]
    filters = ["1 = 1"]
    params: list[Any] = []
    if "status" in pq:
        filters.append("pq.status IN (?)")
        params.append(QUEUE_STATUSES[0])
    if "published" in gc:
        filters.append("COALESCE(gc.published, 0) = 0")

    rows = conn.execute(
        f"""SELECT
               pq.id AS queue_id,
               pq.content_id AS item_id,
               {_column_expr(pq, "scheduled_at", alias="pq")} AS scheduled_at,
               {_column_expr(pq, "platform", "'all'", alias="pq")} AS queue_platform,
               gc.id AS generated_content_id,
               {_column_expr(gc, "content_type", alias="gc")} AS content_type,
               {_column_expr(gc, "content", "''", alias="gc")} AS content,
               {_variant_content_select(schema)} AS selected_variant_content,
               {_variant_metadata_select(schema)} AS selected_variant_metadata
           FROM publish_queue pq
           INNER JOIN generated_content gc ON gc.id = pq.content_id
           {_variant_join(schema)}
           WHERE {" AND ".join(filters)}
           ORDER BY {_column_expr(pq, "scheduled_at", alias="pq")} ASC, pq.id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _validate_row(row: dict[str, Any]) -> PublishFormatItem:
    destination = _destination(row)
    destination_id = _destination_id(row)
    findings: list[PublishFormatFinding] = []
    payload = _payload(row)
    body = _body_text(payload, row.get("content"))

    if not destination_id:
        findings.append(
            _finding(
                row,
                destination,
                destination_id,
                missing_fields=("destination_id",),
                fix_hint="Set publish_queue.platform or a supported content_type before publishing.",
                message="Queued item has no destination identifier.",
            )
        )

    if destination == "x_post":
        if not body:
            findings.append(
                _finding(
                    row,
                    destination,
                    destination_id,
                    missing_fields=("body",),
                    fix_hint="Add nonempty post copy to generated_content.content or the selected X variant.",
                    message="X post body is empty.",
                )
            )
    elif destination == "x_thread":
        parts = parse_thread_content(body)
        missing: list[str] = []
        invalid: list[str] = []
        if not body:
            missing.append("body")
        if len(parts) < 2:
            invalid.append("thread_items")
        if missing or invalid:
            findings.append(
                _finding(
                    row,
                    destination,
                    destination_id,
                    missing_fields=tuple(missing),
                    invalid_fields=tuple(invalid),
                    fix_hint="Provide at least two nonempty TWEET n: sections for the thread.",
                    message="X thread must contain at least two nonempty items.",
                )
            )
    elif destination == "newsletter":
        missing = []
        if not _field_text(payload, ("subject", "email_subject")):
            missing.append("subject")
        if not body:
            missing.append("body")
        if missing:
            findings.append(
                _finding(
                    row,
                    destination,
                    destination_id,
                    missing_fields=tuple(missing),
                    fix_hint="Add newsletter subject and body fields before sending.",
                    message="Newsletter is missing required send fields.",
                )
            )
    elif destination == "blog":
        missing = []
        if not _blog_title(payload, body):
            missing.append("title")
        if not body:
            missing.append("body")
        if missing:
            findings.append(
                _finding(
                    row,
                    destination,
                    destination_id,
                    missing_fields=tuple(missing),
                    fix_hint="Add a blog title and nonempty markdown/body content.",
                    message="Blog post is missing required publishing fields.",
                )
            )
    else:
        findings.append(
            _finding(
                row,
                destination,
                destination_id,
                invalid_fields=("destination",),
                fix_hint=(
                    "Use one of the supported destination formats: "
                    + ", ".join(SUPPORTED_DESTINATIONS)
                    + "."
                ),
                message=f"Unsupported publish destination: {destination}",
            )
        )

    status = BLOCKED if findings else READY
    return PublishFormatItem(
        item_id=int(row["item_id"]),
        queue_id=int(row["queue_id"]),
        destination=destination,
        destination_id=destination_id,
        status=status,
        scheduled_at=row.get("scheduled_at"),
        content_type=row.get("content_type"),
        findings=tuple(findings),
    )


def _finding(
    row: dict[str, Any],
    destination: str,
    destination_id: str | None,
    *,
    missing_fields: tuple[str, ...] = (),
    invalid_fields: tuple[str, ...] = (),
    severity: str = BLOCKED,
    fix_hint: str,
    message: str,
) -> PublishFormatFinding:
    return PublishFormatFinding(
        item_id=int(row["item_id"]),
        queue_id=int(row["queue_id"]),
        destination=destination,
        destination_id=destination_id,
        severity=severity,
        missing_fields=missing_fields,
        invalid_fields=invalid_fields,
        fix_hint=fix_hint,
        message=message,
    )


def _destination(row: dict[str, Any]) -> str:
    content_type = _clean(row.get("content_type")).lower()
    if content_type in CONTENT_TYPE_DESTINATIONS:
        return CONTENT_TYPE_DESTINATIONS[content_type]
    platform = _clean(row.get("queue_platform")).lower()
    if platform in {"newsletter", "blog"}:
        return platform
    if platform == "x":
        return "x_post"
    return content_type or platform or "unknown"


def _destination_id(row: dict[str, Any]) -> str | None:
    return _clean(row.get("queue_platform")) or _clean(row.get("content_type")) or None


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    texts = (
        row.get("selected_variant_content"),
        row.get("content"),
        row.get("selected_variant_metadata"),
    )
    for text in texts:
        if not isinstance(text, str) or not text.strip():
            continue
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return {}


def _body_text(payload: dict[str, Any], fallback: Any) -> str:
    body = _field_text(
        payload,
        ("body", "content", "markdown", "draft", "text", "html"),
    )
    if body:
        return body
    if isinstance(fallback, str) and not _looks_like_json_object(fallback):
        return fallback.strip()
    return ""


def _field_text(payload: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _blog_title(payload: dict[str, Any], body: str) -> str:
    title = _field_text(payload, ("title", "headline"))
    if title:
        return title
    frontmatter_match = re.match(r"^---\s*\n(.*?)\n---", body, flags=re.DOTALL)
    if frontmatter_match:
        for line in frontmatter_match.group(1).splitlines():
            if line.strip().lower().startswith("title:"):
                return line.split(":", 1)[1].strip().strip("\"'")
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return stripped[2:].strip()
    return ""


def _looks_like_json_object(value: str) -> bool:
    text = value.strip()
    return text.startswith("{") and text.endswith("}")


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


def _record_missing_columns(
    schema: dict[str, set[str]],
    table: str,
    required: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> None:
    missing = tuple(column for column in required if column not in schema.get(table, set()))
    if missing:
        missing_columns[table] = missing


def _variant_join(schema: dict[str, set[str]]) -> str:
    columns = schema.get("content_variants", set())
    if not {"content_id", "content"}.issubset(columns):
        return ""
    selected_expr = (
        "CASE WHEN cv.selected = 1 THEN 0 ELSE 1 END,"
        if "selected" in columns
        else ""
    )
    return f"""LEFT JOIN (
               SELECT cv.*,
                      ROW_NUMBER() OVER (
                          PARTITION BY cv.content_id
                          ORDER BY {selected_expr} cv.id DESC
                      ) AS rn
               FROM content_variants cv
           ) cv ON cv.content_id = gc.id AND cv.rn = 1"""


def _variant_content_select(schema: dict[str, set[str]]) -> str:
    columns = schema.get("content_variants", set())
    if {"content_id", "content"}.issubset(columns):
        return "cv.content"
    return "NULL"


def _variant_metadata_select(schema: dict[str, set[str]]) -> str:
    columns = schema.get("content_variants", set())
    if {"content_id", "metadata"}.issubset(columns):
        return "cv.metadata"
    return "NULL"


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


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
