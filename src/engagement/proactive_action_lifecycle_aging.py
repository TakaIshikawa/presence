"""Report lifecycle aging for proactive actions."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Sequence


DEFAULT_APPROVED_NOT_POSTED_HOURS = 24
DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
DEFAULT_LOW_RELEVANCE_PERCENT = 30
DEFAULT_STALE_PENDING_HOURS = 72

PROACTIVE_COLUMNS = (
    "id",
    "action_type",
    "target_tweet_id",
    "target_tweet_text",
    "target_author_handle",
    "target_author_id",
    "discovery_source",
    "relevance_score",
    "draft_text",
    "status",
    "relationship_context",
    "knowledge_ids",
    "platform_metadata",
    "posted_tweet_id",
    "created_at",
    "reviewed_at",
    "posted_at",
)

TRACKED_STATUSES = ("pending", "approved", "posted", "dismissed")


@dataclass(frozen=True)
class ProactiveActionLifecycleItem:
    """One proactive action included in lifecycle aging analysis."""

    id: int
    status: str
    action_type: str
    discovery_source: str | None
    target_author_handle: str | None
    relevance_score: float | None
    age_hours: float | None
    age_bucket: str
    age_basis: str
    created_at: str | None
    reviewed_at: str | None
    posted_at: str | None
    posted_tweet_id: str | None
    finding_labels: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["finding_labels"] = list(self.finding_labels)
        return payload


@dataclass(frozen=True)
class ProactiveActionLifecycleAgingReport:
    """Deterministic proactive action lifecycle aging report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    findings: tuple[dict[str, Any], ...]
    status_groups: tuple[dict[str, Any], ...]
    author_groups: tuple[dict[str, Any], ...]
    actions: tuple[ProactiveActionLifecycleItem, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "proactive_action_lifecycle_aging",
            "actions": [action.to_dict() for action in self.actions],
            "author_groups": [dict(group) for group in self.author_groups],
            "filters": dict(self.filters),
            "findings": [dict(finding) for finding in self.findings],
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "status_groups": [dict(group) for group in self.status_groups],
            "totals": dict(sorted(self.totals.items())),
        }


def build_proactive_action_lifecycle_aging_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    stale_pending_hours: int = DEFAULT_STALE_PENDING_HOURS,
    approved_not_posted_hours: int = DEFAULT_APPROVED_NOT_POSTED_HOURS,
    low_relevance_percent: int = DEFAULT_LOW_RELEVANCE_PERCENT,
    now: datetime | None = None,
) -> ProactiveActionLifecycleAgingReport:
    """Build a read-only aging report for proactive action lifecycle states."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if stale_pending_hours <= 0:
        raise ValueError("stale_pending_hours must be positive")
    if approved_not_posted_hours <= 0:
        raise ValueError("approved_not_posted_hours must be positive")
    if low_relevance_percent <= 0:
        raise ValueError("low_relevance_percent must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    filters = {
        "approved_not_posted_hours": approved_not_posted_hours,
        "days": days,
        "limit": limit,
        "low_relevance_percent": low_relevance_percent,
        "stale_pending_hours": stale_pending_hours,
        "statuses": list(TRACKED_STATUSES),
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "proactive_actions" not in schema:
        return _empty_report(generated_at, filters, missing_tables=("proactive_actions",))
    missing = tuple(column for column in PROACTIVE_COLUMNS if column not in schema["proactive_actions"])
    if missing:
        return _empty_report(
            generated_at,
            filters,
            missing_columns={"proactive_actions": missing},
        )

    rows = _action_rows(conn, days=days, now=generated_at)
    actions = tuple(
        _action_item(
            row,
            now=generated_at,
            stale_pending_hours=stale_pending_hours,
            approved_not_posted_hours=approved_not_posted_hours,
            low_relevance_percent=low_relevance_percent,
        )
        for row in rows
    )
    findings = tuple(
        sorted(
            (
                finding
                for action in actions
                for finding in _findings_for_action(action)
            ),
            key=_finding_sort_key,
        )
    )
    ordered_actions = tuple(sorted(actions, key=_action_sort_key)[:limit])
    return ProactiveActionLifecycleAgingReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(ordered_actions, findings, rows_scanned=len(rows)),
        findings=findings,
        status_groups=_status_groups(actions),
        author_groups=_author_groups(actions),
        actions=ordered_actions,
    )


def format_proactive_action_lifecycle_aging_json(
    report: ProactiveActionLifecycleAgingReport,
) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_proactive_action_lifecycle_aging_text(
    report: ProactiveActionLifecycleAgingReport,
) -> str:
    """Render a compact human-readable lifecycle aging report."""
    filters = report.filters
    lines = [
        "Proactive Action Lifecycle Aging",
        (
            "Filters: "
            f"days={filters['days']} limit={filters['limit']} "
            f"stale_pending_hours={filters['stale_pending_hours']} "
            f"approved_not_posted_hours={filters['approved_not_posted_hours']} "
            f"low_relevance_percent={filters['low_relevance_percent']}"
        ),
        (
            "Totals: "
            f"actions={report.totals['action_count']} "
            f"findings={report.totals['finding_count']} "
            f"stale_pending={report.totals['stale_pending_count']} "
            f"approved_not_posted={report.totals['approved_not_posted_count']} "
            f"posted_missing_platform_id={report.totals['posted_missing_platform_id_count']} "
            f"low_relevance_pending={report.totals['low_relevance_pending_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        lines.append(
            "Missing columns: "
            + ", ".join(
                f"{table}.{column}"
                for table, columns in sorted(report.missing_columns.items())
                for column in columns
            )
        )
    if not report.actions:
        lines.append("No proactive actions matched.")
        return "\n".join(lines)

    lines.append("Status groups:")
    for group in report.status_groups:
        lines.append(
            f"  - {group['status']} count={group['count']} "
            f"age_buckets={_compact_counts(group['age_buckets'])}"
        )

    lines.append("Items:")
    for action in report.actions:
        handle = action.target_author_handle or "?"
        age = "n/a" if action.age_hours is None else f"{action.age_hours:.1f}h"
        labels = ",".join(action.finding_labels) or "-"
        lines.append(
            f"  - #{action.id} {action.status} {action.action_type} @{handle} "
            f"age={age} bucket={action.age_bucket} findings={labels}"
        )
        lines.append(
            "    "
            f"source={action.discovery_source or 'n/a'} "
            f"relevance={_format_score(action.relevance_score)} "
            f"basis={action.age_basis}"
        )
    return "\n".join(lines)


def _empty_report(
    now: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ProactiveActionLifecycleAgingReport:
    return ProactiveActionLifecycleAgingReport(
        generated_at=now.isoformat(),
        filters=filters,
        totals=_totals((), (), rows_scanned=0),
        findings=(),
        status_groups=(),
        author_groups=(),
        actions=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _action_rows(
    conn: sqlite3.Connection,
    *,
    days: int,
    now: datetime,
) -> list[dict[str, Any]]:
    cutoff = (now - timedelta(days=days)).isoformat()
    cursor = conn.execute(
        f"""SELECT {', '.join(PROACTIVE_COLUMNS)}
            FROM proactive_actions
            WHERE LOWER(COALESCE(status, 'pending')) IN ({', '.join('?' for _ in TRACKED_STATUSES)})
              AND datetime(COALESCE(created_at, ?)) >= datetime(?)
            ORDER BY datetime(COALESCE(created_at, ?)) ASC, id ASC""",
        [*TRACKED_STATUSES, now.isoformat(), cutoff, now.isoformat()],
    )
    return [dict(row) for row in cursor.fetchall()]


def _action_item(
    row: dict[str, Any],
    *,
    now: datetime,
    stale_pending_hours: int,
    approved_not_posted_hours: int,
    low_relevance_percent: int,
) -> ProactiveActionLifecycleItem:
    status = _normalised_status(row.get("status"))
    age_time, age_basis = _age_basis(row, status)
    age_hours = _age_hours(age_time, now)
    relevance_score = _optional_float(row.get("relevance_score"))
    labels = _labels(
        status=status,
        age_hours=age_hours,
        relevance_score=relevance_score,
        posted_tweet_id=row.get("posted_tweet_id"),
        stale_pending_hours=stale_pending_hours,
        approved_not_posted_hours=approved_not_posted_hours,
        low_relevance_percent=low_relevance_percent,
    )
    return ProactiveActionLifecycleItem(
        id=int(row["id"]),
        status=status,
        action_type=_text(row.get("action_type")) or "unknown",
        discovery_source=_text(row.get("discovery_source")),
        target_author_handle=_text(row.get("target_author_handle")),
        relevance_score=relevance_score,
        age_hours=age_hours,
        age_bucket=_age_bucket(age_hours),
        age_basis=age_basis,
        created_at=row.get("created_at"),
        reviewed_at=row.get("reviewed_at"),
        posted_at=row.get("posted_at"),
        posted_tweet_id=_text(row.get("posted_tweet_id")),
        finding_labels=labels,
    )


def _age_basis(row: dict[str, Any], status: str) -> tuple[str | None, str]:
    if status == "posted":
        return row.get("posted_at") or row.get("reviewed_at") or row.get("created_at"), "posted_at"
    if status in {"approved", "dismissed"}:
        return row.get("reviewed_at") or row.get("created_at"), "reviewed_at"
    return row.get("created_at"), "created_at"


def _labels(
    *,
    status: str,
    age_hours: float | None,
    relevance_score: float | None,
    posted_tweet_id: Any,
    stale_pending_hours: int,
    approved_not_posted_hours: int,
    low_relevance_percent: int,
) -> tuple[str, ...]:
    labels: list[str] = []
    if status == "pending" and age_hours is not None and age_hours >= stale_pending_hours:
        labels.append("stale_pending")
    if status == "approved" and age_hours is not None and age_hours >= approved_not_posted_hours:
        labels.append("approved_not_posted")
    if status == "posted" and not _text(posted_tweet_id):
        labels.append("posted_missing_platform_id")
    if (
        status == "pending"
        and relevance_score is not None
        and (relevance_score * 100) <= low_relevance_percent
    ):
        labels.append("low_relevance_pending")
    return tuple(labels)


def _findings_for_action(
    action: ProactiveActionLifecycleItem,
) -> list[dict[str, Any]]:
    return [
        {
            "label": label,
            "action_id": action.id,
            "status": action.status,
            "action_type": action.action_type,
            "target_author_handle": action.target_author_handle,
            "discovery_source": action.discovery_source,
            "age_hours": action.age_hours,
            "age_bucket": action.age_bucket,
            "relevance_score": action.relevance_score,
        }
        for label in action.finding_labels
    ]


def _status_groups(
    actions: Sequence[ProactiveActionLifecycleItem],
) -> tuple[dict[str, Any], ...]:
    grouped: dict[str, list[ProactiveActionLifecycleItem]] = defaultdict(list)
    for action in actions:
        grouped[action.status].append(action)
    return tuple(
        _group_payload(status, grouped.get(status, ()))
        for status in TRACKED_STATUSES
        if grouped.get(status)
    )


def _author_groups(
    actions: Sequence[ProactiveActionLifecycleItem],
) -> tuple[dict[str, Any], ...]:
    grouped: dict[str, list[ProactiveActionLifecycleItem]] = defaultdict(list)
    for action in actions:
        grouped[action.target_author_handle or "unknown"].append(action)
    return tuple(
        {
            "target_author_handle": author,
            **_group_payload(author, grouped[author], include_status=True),
        }
        for author in sorted(grouped)
    )


def _group_payload(
    label: str,
    actions: Sequence[ProactiveActionLifecycleItem],
    *,
    include_status: bool = False,
) -> dict[str, Any]:
    payload = {
        "count": len(actions),
        "age_buckets": _counter_dict(action.age_bucket for action in actions),
        "action_types": _counter_dict(action.action_type for action in actions),
        "discovery_sources": _counter_dict(
            action.discovery_source or "unknown" for action in actions
        ),
        "finding_labels": _counter_dict(
            label for action in actions for label in action.finding_labels
        ),
        "target_author_handles": _counter_dict(
            action.target_author_handle or "unknown" for action in actions
        ),
    }
    if include_status:
        payload["statuses"] = _counter_dict(action.status for action in actions)
    else:
        payload["status"] = label
    return payload


def _totals(
    actions: tuple[ProactiveActionLifecycleItem, ...],
    findings: tuple[dict[str, Any], ...],
    *,
    rows_scanned: int,
) -> dict[str, int]:
    statuses = Counter(action.status for action in actions)
    labels = [finding["label"] for finding in findings]
    return {
        "action_count": len(actions),
        "approved_count": statuses.get("approved", 0),
        "approved_not_posted_count": labels.count("approved_not_posted"),
        "dismissed_count": statuses.get("dismissed", 0),
        "finding_count": len(findings),
        "low_relevance_pending_count": labels.count("low_relevance_pending"),
        "pending_count": statuses.get("pending", 0),
        "posted_count": statuses.get("posted", 0),
        "posted_missing_platform_id_count": labels.count("posted_missing_platform_id"),
        "rows_scanned": rows_scanned,
        "stale_pending_count": labels.count("stale_pending"),
    }


def _counter_dict(values: Sequence[str] | Any) -> dict[str, int]:
    return dict(sorted(Counter(values).items()))


def _action_sort_key(action: ProactiveActionLifecycleItem) -> tuple[Any, ...]:
    return (
        not action.finding_labels,
        -(action.age_hours or 0),
        action.status,
        action.target_author_handle or "",
        action.id,
    )


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    order = {
        "stale_pending": 0,
        "approved_not_posted": 1,
        "posted_missing_platform_id": 2,
        "low_relevance_pending": 3,
    }
    return (
        order.get(str(finding["label"]), 99),
        finding["target_author_handle"] or "",
        finding["action_id"],
    )


def _normalised_status(value: Any) -> str:
    status = (_text(value) or "pending").lower()
    if status in {"published", "completed", "sent"}:
        return "posted"
    if status in {"rejected", "expired"}:
        return "dismissed"
    if status in TRACKED_STATUSES:
        return status
    return "pending"


def _age_bucket(age_hours: float | None) -> str:
    if age_hours is None:
        return "unknown"
    if age_hours < 24:
        return "0-24h"
    if age_hours < 72:
        return "24-72h"
    if age_hours < 168:
        return "3-7d"
    return "7d+"


def _age_hours(value: str | None, now: datetime) -> float | None:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return None
    return round(max(0.0, (now - parsed).total_seconds() / 3600), 2)


def _parse_timestamp(value: str | None) -> datetime | None:
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


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    return text or None


def _format_score(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f}"


def _compact_counts(counts: dict[str, int]) -> str:
    return ",".join(f"{key}:{value}" for key, value in counts.items()) or "-"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)
