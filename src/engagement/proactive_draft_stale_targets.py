"""Report proactive drafts whose target context is stale or unavailable."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 7
DEFAULT_LIMIT = 100
OPEN_STATUSES = ("approved", "pending")
REASONS = (
    "missing_target_url",
    "stale_target_snapshot",
    "old_draft",
    "needs_refresh",
)


@dataclass(frozen=True)
class ProactiveDraftStaleTarget:
    """One proactive draft with stale or unavailable target metadata."""

    draft_id: int
    status: str
    action_type: str
    platform: str
    target_url: str | None
    target_tweet_id: str | None
    target_author_handle: str | None
    target_fetched_at: str | None
    draft_updated_at: str | None
    target_age_days: float | None
    draft_age_days: float | None
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reasons"] = list(self.reasons)
        return payload


@dataclass(frozen=True)
class ProactiveDraftStaleTargetsReport:
    """Read-only proactive draft stale target report."""

    generated_at: str
    filters: dict[str, Any]
    reason_counts: dict[str, int]
    by_platform: dict[str, int]
    stale_targets: tuple[ProactiveDraftStaleTarget, ...]
    representative_draft_ids: tuple[int, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def total_stale_targets(self) -> int:
        return len(self.stale_targets)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "proactive_draft_stale_targets",
            "by_platform": dict(self.by_platform),
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "reason_counts": dict(self.reason_counts),
            "representative_draft_ids": list(self.representative_draft_ids),
            "stale_targets": [target.to_dict() for target in self.stale_targets],
            "total_stale_targets": self.total_stale_targets,
        }


def build_proactive_draft_stale_targets_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ProactiveDraftStaleTargetsReport:
    """Build a report for proactive drafts needing target refresh."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    filters = {
        "days": days,
        "limit": limit,
        "status": list(OPEN_STATUSES),
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "proactive_actions" not in schema:
        return _empty_report(generated_at, filters, missing_tables=("proactive_actions",))

    required = {"id", "action_type", "status", "draft_text", "created_at"}
    missing_required = tuple(sorted(required - schema["proactive_actions"]))
    if missing_required:
        return _empty_report(
            generated_at,
            filters,
            missing_columns={"proactive_actions": missing_required},
        )

    rows = _load_rows(conn, schema)
    stale_targets = tuple(
        target
        for row in rows
        if (
            target := _target_for_row(
                row,
                days=days,
                now=generated_at,
            )
        )
        is not None
    )[:limit]
    return ProactiveDraftStaleTargetsReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        reason_counts={reason: Counter(reason for target in stale_targets for reason in target.reasons).get(reason, 0) for reason in REASONS},
        by_platform=dict(sorted(Counter(target.platform for target in stale_targets).items())),
        stale_targets=stale_targets,
        representative_draft_ids=tuple(target.draft_id for target in stale_targets[:10]),
        missing_tables=(),
        missing_columns=_optional_missing_columns(schema),
    )


def format_proactive_draft_stale_targets_json(
    report: ProactiveDraftStaleTargetsReport,
) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_proactive_draft_stale_targets_text(
    report: ProactiveDraftStaleTargetsReport,
) -> str:
    """Render a compact human-readable stale target report."""
    lines = [
        "Proactive Draft Stale Targets",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['days']} days "
            f"limit={report.filters['limit']}"
        ),
        f"Stale targets: {report.total_stale_targets}",
        "Reasons: "
        + ", ".join(f"{reason}={count}" for reason, count in report.reason_counts.items()),
    ]
    if report.by_platform:
        lines.append(
            "Platforms: "
            + ", ".join(
                f"{platform}={count}" for platform, count in sorted(report.by_platform.items())
            )
        )
    if report.representative_draft_ids:
        lines.append(
            "Representative draft ids: "
            + ", ".join(str(draft_id) for draft_id in report.representative_draft_ids)
        )
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
            if columns
        ]
        if missing:
            lines.append("Missing columns: " + "; ".join(missing))

    if not report.stale_targets:
        lines.extend(["", "No proactive drafts have stale or unavailable targets."])
        return "\n".join(lines)

    lines.extend(["", "Stale target drafts:"])
    for target in report.stale_targets:
        target_age = "n/a" if target.target_age_days is None else f"{target.target_age_days:.1f}d"
        draft_age = "n/a" if target.draft_age_days is None else f"{target.draft_age_days:.1f}d"
        lines.append(
            f"  - draft_id={target.draft_id} platform={target.platform} "
            f"status={target.status} type={target.action_type} "
            f"target_age={target_age} draft_age={draft_age} "
            f"reasons={', '.join(target.reasons)}"
        )
        lines.append(
            f"      target_url={target.target_url or '-'} "
            f"target_id={target.target_tweet_id or '-'} "
            f"author={target.target_author_handle or '-'}"
        )
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[dict[str, Any]]:
    columns = schema["proactive_actions"]
    select_columns = [
        "pa.id AS draft_id",
        _column_expr(columns, "action_type", "pa", "action_type"),
        _column_expr(columns, "target_tweet_id", "pa", "target_tweet_id"),
        _column_expr(columns, "target_author_handle", "pa", "target_author_handle"),
        _column_expr(columns, "status", "pa", "status"),
        _column_expr(columns, "draft_text", "pa", "draft_text"),
        _column_expr(columns, "created_at", "pa", "created_at"),
        _column_expr(columns, "updated_at", "pa", "updated_at"),
        _column_expr(columns, "platform_metadata", "pa", "platform_metadata"),
        _column_expr(columns, "target_url", "pa", "target_url"),
        _column_expr(columns, "target_fetched_at", "pa", "target_fetched_at"),
    ]
    status_filter = "LOWER(COALESCE(pa.status, 'pending')) IN (?, ?)"
    draft_filter = "pa.draft_text IS NOT NULL AND TRIM(pa.draft_text) != ''"
    order_anchor = "COALESCE(pa.updated_at, pa.created_at)" if "updated_at" in columns else "pa.created_at"
    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM proactive_actions pa
            WHERE {status_filter}
              AND {draft_filter}
            ORDER BY datetime({order_anchor}) ASC, pa.id ASC""",
        OPEN_STATUSES,
    ).fetchall()
    return [dict(row) for row in rows]


def _target_for_row(
    row: dict[str, Any],
    *,
    days: int,
    now: datetime,
) -> ProactiveDraftStaleTarget | None:
    metadata = _metadata_object(row.get("platform_metadata"))
    target_url = _first_text(
        row.get("target_url"),
        _metadata_value(metadata, ("target_url", "url", "tweet_url", "permalink")),
        _nested_metadata_value(metadata, ("target",), ("url", "target_url", "permalink")),
    )
    target_fetched_at = _first_text(
        row.get("target_fetched_at"),
        _metadata_value(metadata, ("target_fetched_at", "fetched_at", "snapshot_at")),
        _nested_metadata_value(metadata, ("target",), ("fetched_at", "target_fetched_at", "snapshot_at")),
    )
    platform = _first_text(
        _metadata_value(metadata, ("platform", "network")),
        _nested_metadata_value(metadata, ("target",), ("platform", "network")),
        "x",
    ) or "x"
    updated_at = _first_text(row.get("updated_at"), row.get("created_at"))
    target_age_days = _age_days(target_fetched_at, now)
    draft_age_days = _age_days(updated_at, now)

    reasons = []
    if not target_url:
        reasons.append("missing_target_url")
    if target_fetched_at is None or (target_age_days is not None and target_age_days >= days):
        reasons.append("stale_target_snapshot")
    if draft_age_days is not None and draft_age_days >= days:
        reasons.append("old_draft")
    if reasons:
        reasons.append("needs_refresh")
    if not reasons:
        return None

    return ProactiveDraftStaleTarget(
        draft_id=int(row["draft_id"]),
        status=_first_text(row.get("status"), "pending") or "pending",
        action_type=_first_text(row.get("action_type"), "unknown") or "unknown",
        platform=platform,
        target_url=target_url,
        target_tweet_id=_first_text(row.get("target_tweet_id")),
        target_author_handle=_first_text(row.get("target_author_handle")),
        target_fetched_at=target_fetched_at,
        draft_updated_at=updated_at,
        target_age_days=target_age_days,
        draft_age_days=draft_age_days,
        reasons=tuple(reasons),
    )


def _optional_missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    optional = {"platform_metadata", "target_url", "target_fetched_at", "updated_at"}
    missing = tuple(sorted(optional - schema.get("proactive_actions", set())))
    return {"proactive_actions": missing} if missing else {}


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ProactiveDraftStaleTargetsReport:
    return ProactiveDraftStaleTargetsReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        reason_counts={reason: 0 for reason in REASONS},
        by_platform={},
        stale_targets=(),
        representative_draft_ids=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3 connection or database wrapper with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }
    return {
        table: {
            str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
            for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")
        }
        for table in tables
    }


def _column_expr(columns: set[str], column: str, alias: str, output: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"NULL AS {output}"


def _metadata_object(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _metadata_value(metadata: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = _first_text(metadata.get(key))
        if value:
            return value
    return None


def _nested_metadata_value(
    metadata: dict[str, Any],
    parents: tuple[str, ...],
    keys: tuple[str, ...],
) -> str | None:
    for parent in parents:
        child = metadata.get(parent)
        if isinstance(child, dict):
            value = _metadata_value(child, keys)
            if value:
                return value
    return None


def _first_text(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = " ".join(str(value).split())
        if text:
            return text
    return None


def _age_days(value: Any, now: datetime) -> float | None:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return None
    return round(max(0.0, (now - parsed).total_seconds() / 86400), 2)


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
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


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
