"""Report publish failures grouped by normalized reason buckets."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any, Iterable, Mapping

from output.publish_errors import PublishErrorCategory, classify_publish_error


DEFAULT_DAYS = 7


@dataclass(frozen=True)
class PublishFailureItem:
    """One failed publish attempt with normalized reason."""

    item_id: int
    content_id: int
    channel: str
    reason: PublishErrorCategory
    error_excerpt: str
    attempted_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PublishFailureReasonSummary:
    """Summary counts by channel and reason."""

    channel: str
    reason: PublishErrorCategory
    failure_count: int
    report_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PublishFailureReasonsReport:
    """Report of failed publish attempts grouped by reason."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    items: tuple[PublishFailureItem, ...]
    summaries: tuple[PublishFailureReasonSummary, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "publish_failure_reasons",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "items": [item.to_dict() for item in self.items],
            "summaries": [summary.to_dict() for summary in self.summaries],
            "totals": dict(sorted(self.totals.items())),
        }


def build_publish_failure_reasons_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    channel: str | None = None,
    now: datetime | None = None,
) -> PublishFailureReasonsReport:
    """Build a report of publish failures grouped by normalized reason buckets."""
    if days <= 0:
        raise ValueError("days must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)

    if _looks_like_rows(db_or_rows):
        raw_rows = [_mapping(row) for row in db_or_rows]
    else:
        conn = _connection(db_or_rows)
        raw_rows = _load_publication_attempts(conn, cutoff)

    items = _build_failure_items(raw_rows, channel=channel, cutoff=cutoff)
    summaries = _build_summaries(items)

    return PublishFailureReasonsReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "channel": channel,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
        },
        totals={
            "failure_count": len(items),
            "channel_count": len({item.channel for item in items}),
            "rows_scanned": len(raw_rows),
        },
        items=items,
        summaries=summaries,
    )


def format_publish_failure_reasons_json(report: PublishFailureReasonsReport) -> str:
    """Serialize a publish failure reasons report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publish_failure_reasons_text(report: PublishFailureReasonsReport) -> str:
    """Render a concise human-readable publish failure reasons report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Publish Failure Reasons",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={filters['days']} channel={filters['channel']} "
            f"lookback_start={filters['lookback_start']} "
            f"lookback_end={filters['lookback_end']}"
        ),
        (
            "Totals: "
            f"failures={totals['failure_count']} channels={totals['channel_count']} "
            f"rows={totals['rows_scanned']}"
        ),
    ]

    if not report.summaries:
        lines.extend(["", "No publish failures found."])
        return "\n".join(lines)

    lines.extend(["", "Summary by Channel and Reason:"])
    for summary in report.summaries:
        lines.append(
            f"- channel={summary.channel} reason={summary.reason} failures={summary.failure_count}"
        )

    if report.items:
        lines.extend(["", "Failed Items (sample):"])
        for item in report.items[:10]:
            excerpt = item.error_excerpt[:60] + "..." if len(item.error_excerpt) > 60 else item.error_excerpt
            lines.append(
                f"- item_id={item.item_id} channel={item.channel} reason={item.reason} error={excerpt}"
            )
        if len(report.items) > 10:
            lines.append(f"... and {len(report.items) - 10} more failures")

    return "\n".join(lines)


def _build_failure_items(
    rows: Iterable[Mapping[str, Any]],
    *,
    channel: str | None = None,
    cutoff: datetime | None = None,
) -> tuple[PublishFailureItem, ...]:
    """Convert raw rows to PublishFailureItem instances."""
    items: list[PublishFailureItem] = []
    for row in rows:
        success = row.get("success")
        if success not in (0, False, "0"):
            continue

        row_channel = str(row.get("platform") or row.get("channel") or "unknown")
        if channel and row_channel != channel:
            continue

        attempted_at_str = _first_text(row, ("attempted_at", "created_at", "timestamp"))
        if cutoff and attempted_at_str:
            attempted_at = _parse_datetime(attempted_at_str)
            if attempted_at and attempted_at < cutoff:
                continue

        error_text = str(row.get("error") or row.get("error_message") or "")
        reason = classify_publish_error(error_text, platform=row_channel)

        items.append(
            PublishFailureItem(
                item_id=int(row.get("id") or row.get("queue_id") or 0),
                content_id=int(row.get("content_id") or 0),
                channel=row_channel,
                reason=reason,
                error_excerpt=_excerpt(error_text, limit=240),
                attempted_at=attempted_at_str,
            )
        )

    return tuple(sorted(items, key=lambda item: (item.channel, item.reason, item.item_id)))


def _build_summaries(items: tuple[PublishFailureItem, ...]) -> tuple[PublishFailureReasonSummary, ...]:
    """Build per-channel, per-reason summaries."""
    counts: dict[tuple[str, PublishErrorCategory], int] = {}
    for item in items:
        key = (item.channel, item.reason)
        counts[key] = counts.get(key, 0) + 1

    summaries = [
        PublishFailureReasonSummary(
            channel=channel,
            reason=reason,
            failure_count=count,
            report_id=_summary_report_id(channel, reason),
        )
        for (channel, reason), count in counts.items()
    ]

    return tuple(sorted(summaries, key=lambda s: (s.channel, s.reason)))


def _summary_report_id(channel: str, reason: PublishErrorCategory) -> str:
    """Generate a deterministic report ID for a channel/reason pair."""
    digest = hashlib.sha256(f"{channel}:{reason}".encode("utf-8")).hexdigest()[:12]
    return f"publish_failure_reason_{digest}"


def _excerpt(value: Any, *, limit: int = 240) -> str:
    """Truncate text to a maximum length."""
    text = str(value).strip()
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _first_text(row: Mapping[str, Any], columns: tuple[str, ...]) -> str | None:
    """Return the first non-empty string value from the given columns."""
    for column in columns:
        value = row.get(column)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _parse_datetime(value: Any) -> datetime | None:
    """Parse a datetime string into a datetime object."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _ensure_utc(dt: datetime) -> datetime:
    """Ensure datetime is timezone-aware UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> Any:
    """Extract connection from database or connection object."""
    if hasattr(db_or_conn, "conn"):
        return db_or_conn.conn
    return db_or_conn


def _looks_like_rows(value: Any) -> bool:
    """Check if value looks like an iterable of rows rather than a database connection."""
    return not hasattr(value, "execute") and not hasattr(value, "conn") and not isinstance(
        value,
        (str, bytes),
    )


def _mapping(row: Any) -> dict[str, Any]:
    """Convert row to dict."""
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row)


def _load_publication_attempts(conn: Any, cutoff: datetime) -> list[dict[str, Any]]:
    """Load publication_attempts rows from database."""
    try:
        cursor = conn.execute(
            """
            SELECT id, queue_id, content_id, platform, attempted_at, success, error, error_category
            FROM publication_attempts
            WHERE success = 0 AND (attempted_at IS NULL OR attempted_at >= ?)
            ORDER BY attempted_at ASC, id ASC
            """,
            (cutoff.isoformat(),),
        )
        column_names = [description[0] for description in cursor.description]
        return [
            dict(row) if isinstance(row, Mapping) else dict(zip(column_names, row, strict=False))
            for row in cursor.fetchall()
        ]
    except Exception:
        return []
