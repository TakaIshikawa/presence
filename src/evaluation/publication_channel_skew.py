"""Compare recent publication channel mix against target shares."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 25
DEFAULT_TARGETS = {
    "x_post": 0.3,
    "x_thread": 0.2,
    "newsletter": 0.2,
    "blog": 0.2,
    "long_post": 0.1,
}


@dataclass(frozen=True)
class PublicationChannelSkewRow:
    channel: str
    generated_count: int
    queued_count: int
    published_count: int
    total_count: int
    observed_share: float
    target_share: float
    delta: float
    status: str
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PublicationChannelSkewReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    channels: tuple[PublicationChannelSkewRow, ...]
    schema_warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "publication_channel_skew",
            "channels": [row.to_dict() for row in self.channels],
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "schema_warnings": list(self.schema_warnings),
            "totals": dict(sorted(self.totals.items())),
        }


def build_publication_channel_skew_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    target: dict[str, float] | None = None,
    now: datetime | None = None,
) -> PublicationChannelSkewReport:
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    targets = _validate_targets(target or DEFAULT_TARGETS)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"days": days, "limit": limit, "target": targets, "cutoff": cutoff.isoformat()}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    warnings = _schema_warnings(schema)
    if "generated_content" not in schema:
        return _report(generated_at, filters, (), warnings)
    generated = _generated_counts(conn, schema, cutoff)
    queued, published = _publication_counts(conn, schema, cutoff)
    channels = set(targets) | set(generated) | set(queued) | set(published)
    totals_by_channel = {
        channel: generated[channel] + queued[channel] + published[channel]
        for channel in channels
    }
    grand_total = sum(totals_by_channel.values())
    rows = []
    for channel in sorted(channels):
        observed = totals_by_channel[channel] / grand_total if grand_total else 0.0
        target_share = targets.get(channel, 0.0)
        delta = observed - target_share
        status = "overused" if delta > 0.1 else "underused" if delta < -0.1 else "on_target"
        rows.append(
            PublicationChannelSkewRow(
                channel=channel,
                generated_count=generated[channel],
                queued_count=queued[channel],
                published_count=published[channel],
                total_count=totals_by_channel[channel],
                observed_share=round(observed, 4),
                target_share=round(target_share, 4),
                delta=round(delta, 4),
                status=status,
                recommended_action=_action(status, channel),
            )
        )
    rows.sort(key=lambda row: ({"overused": 0, "underused": 1, "on_target": 2}[row.status], -abs(row.delta), row.channel))
    return _report(generated_at, filters, tuple(rows[:limit]), warnings)


def format_publication_channel_skew_json(report: PublicationChannelSkewReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publication_channel_skew_text(report: PublicationChannelSkewReport) -> str:
    lines = [
        "Publication Channel Skew",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['days']} days",
        f"Totals: channels={report.totals['channel_count']} total={report.totals['total_count']}",
    ]
    if report.schema_warnings:
        lines.append("Schema warnings: " + "; ".join(report.schema_warnings))
    if not report.channels:
        lines.append("No publication channel rows found.")
        return "\n".join(lines)
    lines.append("")
    lines.append("Channels:")
    for row in report.channels:
        lines.append(
            f"- {row.channel} status={row.status} observed={row.observed_share:.2f} "
            f"target={row.target_share:.2f} delta={row.delta:+.2f} "
            f"generated={row.generated_count} queued={row.queued_count} published={row.published_count} "
            f"action={row.recommended_action}"
        )
    return "\n".join(lines)


def parse_target_json(raw: str | None) -> dict[str, float] | None:
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
    except ValueError as exc:
        raise ValueError("target must be a JSON object") from exc
    if not isinstance(parsed, dict):
        raise ValueError("target must be a JSON object")
    return _validate_targets(parsed)


def _generated_counts(conn: sqlite3.Connection, schema: dict[str, set[str]], cutoff: datetime) -> Counter[str]:
    columns = schema["generated_content"]
    created_at = _column_expr(columns, "created_at", "NULL", "gc")
    counts: Counter[str] = Counter()
    for row in conn.execute(
        f"SELECT content_type, COUNT(*) AS count FROM generated_content gc WHERE {created_at} IS NULL OR datetime({created_at}) >= datetime(?) GROUP BY content_type",
        (cutoff.isoformat(),),
    ):
        counts[_channel(row["content_type"])] += int(row["count"])
    return counts


def _publication_counts(conn: sqlite3.Connection, schema: dict[str, set[str]], cutoff: datetime) -> tuple[Counter[str], Counter[str]]:
    queued: Counter[str] = Counter()
    published: Counter[str] = Counter()
    if "content_publications" in schema:
        columns = schema["content_publications"]
        updated_at = _column_expr(columns, "updated_at", "NULL", "cp")
        platform = _column_expr(columns, "platform", "NULL", "cp")
        status = _column_expr(columns, "status", "NULL", "cp")
        for row in conn.execute(
            f"""SELECT {platform} AS platform, {status} AS status, COUNT(*) AS count
                FROM content_publications cp
                WHERE {updated_at} IS NULL OR datetime({updated_at}) >= datetime(?)
                GROUP BY {platform}, {status}""",
            (cutoff.isoformat(),),
        ):
            bucket = published if str(row["status"]).lower() == "published" else queued
            bucket[_channel(row["platform"])] += int(row["count"])
    if "publish_queue" in schema:
        columns = schema["publish_queue"]
        created_at = _column_expr(columns, "created_at", "NULL", "pq")
        channel = _column_expr(columns, "content_type", _column_expr(columns, "platform", "NULL", "pq"), "pq")
        for row in conn.execute(
            f"SELECT {channel} AS channel, COUNT(*) AS count FROM publish_queue pq WHERE {created_at} IS NULL OR datetime({created_at}) >= datetime(?) GROUP BY {channel}",
            (cutoff.isoformat(),),
        ):
            queued[_channel(row["channel"])] += int(row["count"])
    return queued, published


def _channel(value: Any) -> str:
    text = str(value or "unknown").lower()
    return {"blog_post": "blog", "linkedin_post": "long_post", "x": "x_post", "twitter": "x_post"}.get(text, text)


def _validate_targets(targets: dict[str, Any]) -> dict[str, float]:
    parsed = {str(key): float(value) for key, value in targets.items()}
    if not parsed or any(value < 0 for value in parsed.values()):
        raise ValueError("target shares must be non-negative and non-empty")
    total = sum(parsed.values())
    if total <= 0:
        raise ValueError("target shares must sum above zero")
    return {key: value / total for key, value in sorted(parsed.items())}


def _action(status: str, channel: str) -> str:
    if status == "overused":
        return f"slow {channel} production"
    if status == "underused":
        return f"schedule more {channel} content"
    return "maintain current mix"


def _report(generated_at: datetime, filters: dict[str, Any], channels: tuple[PublicationChannelSkewRow, ...], warnings: tuple[str, ...]) -> PublicationChannelSkewReport:
    return PublicationChannelSkewReport(generated_at.isoformat(), filters, {"channel_count": len(channels), "total_count": sum(row.total_count for row in channels)}, channels, warnings)


def _schema_warnings(schema: dict[str, set[str]]) -> tuple[str, ...]:
    if "generated_content" not in schema:
        return ("missing table: generated_content",)
    if not {"id", "content_type"}.issubset(schema["generated_content"]):
        return ("missing columns: generated_content(id, content_type)",)
    return ()


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _column_expr(columns: set[str], column: str, fallback: str, alias: str) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _ensure_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
