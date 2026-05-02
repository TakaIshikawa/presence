"""Plan claim-check refresh work before publishing generated content."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_STALE_DAYS = 14

MISSING_CLAIM_CHECK = "missing_claim_check"
STALE_PASSED_CHECK = "stale_passed_check"
UNRESOLVED_FAILED_CHECK = "unresolved_failed_check"
UNVERIFIABLE_CLAIM = "unverifiable_claim"

RECOMMENDED_ACTIONS = {
    MISSING_CLAIM_CHECK: "Run claim check before publication.",
    STALE_PASSED_CHECK: "Refresh claim check against current evidence before publication.",
    UNRESOLVED_FAILED_CHECK: "Revise or remove unsupported claims, then rerun claim check.",
    UNVERIFIABLE_CLAIM: "Add verifiable evidence or rerun claim check with explicit claim results.",
}


@dataclass(frozen=True)
class ClaimCheckStalenessItem:
    """One generated content item requiring claim-check planning."""

    content_id: int
    content_type: str
    age_days: int
    claim_status: str
    reason: str
    recommended_action: str
    content_created_at: str | None = None
    claim_check_updated_at: str | None = None
    supported_count: int | None = None
    unsupported_count: int | None = None
    published: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaimCheckStalenessReport:
    """Claim-check staleness plan with filter and schema metadata."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    items: tuple[ClaimCheckStalenessItem, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    @property
    def has_issues(self) -> bool:
        return bool(self.items)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claim_check_staleness_plan",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "has_issues": self.has_issues,
            "item_count": len(self.items),
            "items": [item.to_dict() for item in self.items],
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_claim_check_staleness_plan(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    stale_days: int = DEFAULT_STALE_DAYS,
    include_published: bool = False,
    now: datetime | None = None,
) -> ClaimCheckStalenessReport:
    """Return generated content with missing, stale, failed, or unverifiable checks."""
    if days <= 0:
        raise ValueError("days must be positive")
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    stale_cutoff = generated_at - timedelta(days=stale_days)
    filters = {
        "days": days,
        "cutoff": cutoff.isoformat(),
        "stale_days": stale_days,
        "stale_cutoff": stale_cutoff.isoformat(),
        "include_published": include_published,
    }

    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_rows(conn, schema)
    inspected = 0
    items: list[ClaimCheckStalenessItem] = []
    for row in rows:
        created_at = _parse_datetime(row.get("content_created_at"))
        if created_at is None or created_at < cutoff:
            continue
        if not include_published and _truthy(row.get("published")):
            continue
        inspected += 1
        item = _classify_row(
            row,
            generated_at=generated_at,
            stale_cutoff=stale_cutoff,
            stale_days=stale_days,
        )
        if item is not None:
            items.append(item)

    items.sort(key=_item_sort_key)
    counts = Counter(item.claim_status for item in items)
    return ClaimCheckStalenessReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "content_count": inspected,
            "item_count": len(items),
            "by_claim_status": dict(sorted(counts.items())),
        },
        items=tuple(items),
        missing_tables=(),
        missing_columns={},
    )


def format_claim_check_staleness_json(report: ClaimCheckStalenessReport) -> str:
    """Serialize the claim-check staleness plan as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claim_check_staleness_text(report: ClaimCheckStalenessReport) -> str:
    """Render the claim-check staleness plan for command-line review."""
    totals = report.totals
    lines = [
        "Claim Check Staleness Plan",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['days']} days "
            f"stale_days={report.filters['stale_days']} "
            f"include_published={'yes' if report.filters['include_published'] else 'no'}"
        ),
        (
            "Totals: "
            f"content={totals['content_count']} "
            f"items={totals['item_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        ]
        lines.append(f"Missing columns: {'; '.join(missing)}")
    lines.append("")

    if not report.items:
        lines.append("No claim-check staleness issues found.")
        return "\n".join(lines)

    lines.append("Items:")
    for item in report.items:
        lines.append(
            f"  - content_id={item.content_id} type={item.content_type} "
            f"age_days={item.age_days} status={item.claim_status} "
            f"reason={item.reason}"
        )
        lines.append(f"      action: {item.recommended_action}")
    return "\n".join(lines)


def _classify_row(
    row: dict[str, Any],
    *,
    generated_at: datetime,
    stale_cutoff: datetime,
    stale_days: int,
) -> ClaimCheckStalenessItem | None:
    content_id = int(row["content_id"])
    content_type = str(row.get("content_type") or "unknown")
    created_at = _parse_datetime(row.get("content_created_at"))
    age_days = _age_days(created_at, generated_at)
    published = _truthy(row.get("published"))

    if row.get("claim_content_id") is None:
        return _item(
            content_id,
            content_type,
            age_days,
            MISSING_CLAIM_CHECK,
            "content has no claim-check summary",
            row,
            published=published,
        )

    supported = _int(row.get("supported_count"))
    unsupported = _int(row.get("unsupported_count"))
    checked_at = _parse_datetime(
        row.get("claim_check_updated_at") or row.get("claim_check_created_at")
    )
    if unsupported > 0:
        return _item(
            content_id,
            content_type,
            age_days,
            UNRESOLVED_FAILED_CHECK,
            f"claim check has {unsupported} unsupported claim(s)",
            row,
            supported_count=supported,
            unsupported_count=unsupported,
            published=published,
        )
    if supported <= 0:
        return _item(
            content_id,
            content_type,
            age_days,
            UNVERIFIABLE_CLAIM,
            "claim check has no supported or unsupported claim results",
            row,
            supported_count=supported,
            unsupported_count=unsupported,
            published=published,
        )
    if checked_at is None:
        return _item(
            content_id,
            content_type,
            age_days,
            UNVERIFIABLE_CLAIM,
            "claim check has no usable timestamp",
            row,
            supported_count=supported,
            unsupported_count=unsupported,
            published=published,
        )
    if checked_at < stale_cutoff:
        return _item(
            content_id,
            content_type,
            age_days,
            STALE_PASSED_CHECK,
            f"passing claim check is older than {stale_days} days",
            row,
            supported_count=supported,
            unsupported_count=unsupported,
            published=published,
        )
    return None


def _item(
    content_id: int,
    content_type: str,
    age_days: int,
    claim_status: str,
    reason: str,
    row: dict[str, Any],
    *,
    supported_count: int | None = None,
    unsupported_count: int | None = None,
    published: bool,
) -> ClaimCheckStalenessItem:
    return ClaimCheckStalenessItem(
        content_id=content_id,
        content_type=content_type,
        age_days=age_days,
        claim_status=claim_status,
        reason=reason,
        recommended_action=RECOMMENDED_ACTIONS[claim_status],
        content_created_at=row.get("content_created_at"),
        claim_check_updated_at=row.get("claim_check_updated_at") or row.get("claim_check_created_at"),
        supported_count=supported_count,
        unsupported_count=unsupported_count,
        published=published,
    )


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    gc_columns = schema["generated_content"]
    ccc_columns = schema["content_claim_checks"]
    rows = conn.execute(
        f"""SELECT
               gc.id AS content_id,
               {_column_expr(gc_columns, "content_type", "'unknown'", alias="gc")} AS content_type,
               gc.created_at AS content_created_at,
               {_column_expr(gc_columns, "published", "0", alias="gc")} AS published,
               ccc.content_id AS claim_content_id,
               ccc.supported_count AS supported_count,
               ccc.unsupported_count AS unsupported_count,
               {_column_expr(ccc_columns, "created_at", "NULL", alias="ccc")} AS claim_check_created_at,
               {_column_expr(ccc_columns, "updated_at", "NULL", alias="ccc")} AS claim_check_updated_at
           FROM generated_content gc
           LEFT JOIN content_claim_checks ccc ON ccc.content_id = gc.id
           ORDER BY gc.created_at DESC, gc.id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "generated_content": {"id", "created_at"},
        "content_claim_checks": {"content_id", "supported_count", "unsupported_count"},
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema[table]
    }
    return missing_tables, missing_columns


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> ClaimCheckStalenessReport:
    return ClaimCheckStalenessReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "content_count": 0,
            "item_count": 0,
            "by_claim_status": {},
        },
        items=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


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


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str,
) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _age_days(created_at: datetime | None, generated_at: datetime) -> int:
    if created_at is None:
        return 0
    return max(0, int((generated_at - created_at).total_seconds() // 86400))


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "published"}
    return bool(value)


def _item_sort_key(item: ClaimCheckStalenessItem) -> tuple[Any, ...]:
    priority = {
        UNRESOLVED_FAILED_CHECK: 0,
        MISSING_CLAIM_CHECK: 1,
        UNVERIFIABLE_CLAIM: 2,
        STALE_PASSED_CHECK: 3,
    }
    return (
        priority[item.claim_status],
        -item.age_days,
        item.content_type,
        item.content_id,
    )
