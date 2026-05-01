"""Reply quality trend and drift reporting."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_BUCKET = "day"
DEFAULT_TOP_N = 5
DEFAULT_LOW_QUALITY_THRESHOLD = 6.0
DEFAULT_MIN_BUCKET_SAMPLE = 2
LOW_QUALITY_FLAGS = {"generic", "sycophantic", "unsafe"}
BUCKETS = {"day", "week"}


@dataclass(frozen=True)
class ReplyDriftBucket:
    """One time bucket of reply quality signals."""

    bucket_start: str
    bucket_end: str
    draft_count: int
    scored_count: int
    reviewed_count: int
    low_quality_count: int
    average_quality_score: float | None
    low_quality_rate: float
    flag_counts: dict[str, int]
    flag_rates: dict[str, float]
    status_counts: dict[str, int]
    reply_ids: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["reply_ids"] = list(self.reply_ids)
        data["flag_counts"] = dict(sorted(self.flag_counts.items()))
        data["flag_rates"] = dict(sorted(self.flag_rates.items()))
        data["status_counts"] = dict(sorted(self.status_counts.items()))
        return data


@dataclass(frozen=True)
class RepeatedLowQualityTarget:
    """A target handle with repeated low-quality draft attempts."""

    target_handle: str
    low_quality_count: int
    draft_count: int
    average_quality_score: float | None
    flags: dict[str, int]
    reply_ids: tuple[int, ...]
    latest_detected_at: str | None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["flags"] = dict(sorted(self.flags.items()))
        data["reply_ids"] = list(self.reply_ids)
        return data


@dataclass(frozen=True)
class ReplySentimentDriftReport:
    """Reply quality drift report plus applied filters."""

    generated_at: str
    days: int
    bucket: str
    platform: str | None
    low_quality_threshold: float
    min_bucket_sample: int
    totals: dict[str, Any]
    buckets: tuple[ReplyDriftBucket, ...]
    warnings: tuple[str, ...]
    repeated_low_quality_targets: tuple[RepeatedLowQualityTarget, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "days": self.days,
            "bucket": self.bucket,
            "platform": self.platform,
            "low_quality_threshold": self.low_quality_threshold,
            "min_bucket_sample": self.min_bucket_sample,
            "totals": self.totals,
            "buckets": [bucket.to_dict() for bucket in self.buckets],
            "warnings": list(self.warnings),
            "repeated_low_quality_targets": [
                target.to_dict() for target in self.repeated_low_quality_targets
            ],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
        }


def build_reply_sentiment_drift_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    bucket: str = DEFAULT_BUCKET,
    platform: str | None = None,
    top_n: int = DEFAULT_TOP_N,
    low_quality_threshold: float = DEFAULT_LOW_QUALITY_THRESHOLD,
    min_bucket_sample: int = DEFAULT_MIN_BUCKET_SAMPLE,
    now: datetime | None = None,
) -> ReplySentimentDriftReport:
    """Build a read-only reply quality drift report."""
    if days < 1:
        raise ValueError("days must be at least 1")
    if bucket not in BUCKETS:
        raise ValueError("bucket must be one of: day, week")
    if top_n < 1:
        raise ValueError("top_n must be at least 1")
    if not 0 <= low_quality_threshold <= 10:
        raise ValueError("low_quality_threshold must be between 0 and 10")
    if min_bucket_sample < 1:
        raise ValueError("min_bucket_sample must be at least 1")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_aware(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    platform_filter = _clean_label(platform)
    missing_tables = tuple(table for table in ("reply_queue",) if table not in schema)
    missing_columns = _missing_columns(schema)
    if missing_tables:
        return _empty_report(
            generated_at,
            days,
            bucket,
            platform_filter,
            low_quality_threshold,
            min_bucket_sample,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    recent_review_events = _recent_review_events(conn, schema, cutoff, generated_at)
    rows = _load_reply_rows(
        conn,
        schema,
        cutoff=cutoff,
        now=generated_at,
        platform=platform_filter,
        recent_review_events=recent_review_events,
    )

    buckets = _build_buckets(
        rows,
        bucket=bucket,
        cutoff=cutoff,
        now=generated_at,
        low_quality_threshold=low_quality_threshold,
    )
    totals = _totals(rows, buckets, low_quality_threshold=low_quality_threshold)
    warnings = _warnings(buckets, min_bucket_sample=min_bucket_sample)
    repeated_targets = _repeated_low_quality_targets(
        rows,
        threshold=low_quality_threshold,
        top_n=top_n,
    )

    return ReplySentimentDriftReport(
        generated_at=generated_at.isoformat(),
        days=days,
        bucket=bucket,
        platform=platform_filter,
        low_quality_threshold=low_quality_threshold,
        min_bucket_sample=min_bucket_sample,
        totals=totals,
        buckets=tuple(buckets),
        warnings=tuple(warnings),
        repeated_low_quality_targets=tuple(repeated_targets),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_reply_sentiment_drift_json(report: ReplySentimentDriftReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_sentiment_drift_text(report: ReplySentimentDriftReport) -> str:
    """Render a stable operator-facing text report."""
    lines = [
        "Reply Sentiment Drift Report",
        f"Generated: {report.generated_at}",
        f"Window: {report.days} days",
        f"Bucket: {report.bucket}",
        f"Low-quality threshold: {report.low_quality_threshold:.1f}",
    ]
    if report.platform:
        lines.append(f"Platform: {report.platform}")
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    if any(report.missing_columns.values()):
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in report.missing_columns.items()
            if columns
        ]
        lines.append(f"Missing optional columns: {'; '.join(missing)}")
    lines.append("")

    totals = report.totals
    lines.append(
        "Totals: "
        f"drafts={totals['draft_count']} "
        f"scored={totals['scored_count']} "
        f"reviewed={totals['reviewed_count']} "
        f"low_quality={totals['low_quality_count']} "
        f"avg={_format_score(totals['average_quality_score'])}"
    )
    lines.append("")

    if not report.buckets:
        lines.append("No reply quality rows found in the selected window.")
        return "\n".join(lines)

    header = (
        f"{'Bucket':<24} {'Drafts':>6} {'Score':>7} {'Low%':>7} "
        f"{'Generic':>7} {'Syco':>7} {'Unsafe':>7} {'Reviewed':>8}"
    )
    lines.extend([header, "-" * len(header)])
    for item in report.buckets:
        lines.append(
            f"{item.bucket_start:<24} "
            f"{item.draft_count:>6} "
            f"{_format_score(item.average_quality_score):>7} "
            f"{item.low_quality_rate:>6.0%} "
            f"{item.flag_rates.get('generic', 0.0):>6.0%} "
            f"{item.flag_rates.get('sycophantic', 0.0):>6.0%} "
            f"{item.flag_rates.get('unsafe', 0.0):>6.0%} "
            f"{item.reviewed_count:>8}"
        )

    if report.warnings:
        lines.extend(["", "Warnings:"])
        lines.extend(f"- {warning}" for warning in report.warnings)
    if report.repeated_low_quality_targets:
        lines.extend(["", "Repeated low-quality targets:"])
        for target in report.repeated_low_quality_targets:
            flags = ", ".join(
                f"{flag}={count}" for flag, count in sorted(target.flags.items())
            )
            suffix = f" flags={flags}" if flags else ""
            lines.append(
                f"- @{target.target_handle}: {target.low_quality_count}/"
                f"{target.draft_count} low-quality avg="
                f"{_format_score(target.average_quality_score)} ids="
                f"{','.join(str(reply_id) for reply_id in target.reply_ids)}{suffix}"
            )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _empty_report(
    generated_at: datetime,
    days: int,
    bucket: str,
    platform: str | None,
    low_quality_threshold: float,
    min_bucket_sample: int,
    *,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ReplySentimentDriftReport:
    return ReplySentimentDriftReport(
        generated_at=generated_at.isoformat(),
        days=days,
        bucket=bucket,
        platform=platform,
        low_quality_threshold=low_quality_threshold,
        min_bucket_sample=min_bucket_sample,
        totals={
            "draft_count": 0,
            "scored_count": 0,
            "reviewed_count": 0,
            "low_quality_count": 0,
            "average_quality_score": None,
            "low_quality_rate": 0.0,
            "flag_counts": {},
            "status_counts": {},
            "platforms": [],
        },
        buckets=(),
        warnings=(),
        repeated_low_quality_targets=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns or {},
    )


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


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    expected = {
        "reply_queue": (
            "id",
            "platform",
            "inbound_author_handle",
            "quality_score",
            "quality_flags",
            "status",
            "detected_at",
            "reviewed_at",
        ),
        "reply_review_events": (
            "reply_queue_id",
            "event_type",
            "old_status",
            "new_status",
            "created_at",
        ),
    }
    return {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema
    }


def _load_reply_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    now: datetime,
    platform: str | None,
    recent_review_events: dict[int, list[datetime]],
) -> list[dict[str, Any]]:
    columns = schema["reply_queue"]
    select_columns = [
        _column_expr(columns, "id"),
        _column_expr(columns, "platform", "'x'"),
        _column_expr(columns, "inbound_author_handle"),
        _column_expr(columns, "quality_score"),
        _column_expr(columns, "quality_flags"),
        _column_expr(columns, "status"),
        _column_expr(columns, "detected_at"),
        _column_expr(columns, "reviewed_at"),
        _column_expr(columns, "posted_at"),
    ]
    rows = conn.execute(
        f"SELECT {', '.join(select_columns)} FROM reply_queue ORDER BY id ASC"
    ).fetchall()

    items: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        reply_id = _int_or_none(row.get("id"))
        if reply_id is None:
            continue
        row_platform = _clean_label(row.get("platform")) or "x"
        if platform and row_platform != platform:
            continue
        detected_at = (
            _parse_timestamp(row.get("detected_at"))
            or _parse_timestamp(row.get("reviewed_at"))
            or _parse_timestamp(row.get("posted_at"))
            or now
        )
        reviewed_at = _parse_timestamp(row.get("reviewed_at"))
        posted_at = _parse_timestamp(row.get("posted_at"))
        event_times = tuple(recent_review_events.get(reply_id, []))
        window_timestamp = _first_in_window(
            (detected_at, reviewed_at, posted_at, *event_times),
            cutoff,
            now,
        )
        if window_timestamp is None:
            continue
        flags = _parse_flags(row.get("quality_flags"))
        score = _float_or_none(row.get("quality_score"))
        items.append(
            {
                "id": reply_id,
                "platform": row_platform,
                "target_handle": _normalize_handle(row.get("inbound_author_handle")),
                "quality_score": score,
                "quality_flags": flags,
                "status": _clean_label(row.get("status")) or "unknown",
                "bucket_at": window_timestamp,
                "detected_at": detected_at,
                "reviewed_at": reviewed_at,
                "posted_at": posted_at,
                "review_event_count": len(event_times),
            }
        )
    items.sort(key=lambda item: (item["bucket_at"], item["id"]))
    return items


def _recent_review_events(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    now: datetime,
) -> dict[int, list[datetime]]:
    if "reply_review_events" not in schema:
        return {}
    columns = schema["reply_review_events"]
    if "reply_queue_id" not in columns:
        return {}
    select_columns = [
        _column_expr(columns, "reply_queue_id"),
        _column_expr(columns, "created_at"),
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
              FROM reply_review_events""",
    ).fetchall()
    events: dict[int, list[datetime]] = defaultdict(list)
    for raw in rows:
        row = dict(raw)
        timestamp = _parse_timestamp(row.get("created_at")) or now
        if cutoff <= timestamp <= now:
            reply_id = _int_or_none(row.get("reply_queue_id"))
            if reply_id is not None:
                events[reply_id].append(timestamp)
    return {reply_id: sorted(times) for reply_id, times in events.items()}


def _build_buckets(
    rows: list[dict[str, Any]],
    *,
    bucket: str,
    cutoff: datetime,
    now: datetime,
    low_quality_threshold: float,
) -> list[ReplyDriftBucket]:
    grouped: dict[datetime, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_bucket_start(row["bucket_at"], bucket)].append(row)
    buckets: list[ReplyDriftBucket] = []
    cursor = _bucket_start(cutoff, bucket)
    final_bucket = _bucket_start(now, bucket)
    while cursor <= final_bucket:
        bucket_end = cursor + (timedelta(days=1) if bucket == "day" else timedelta(days=7))
        bucket_rows = grouped.get(cursor, [])
        if bucket_rows:
            buckets.append(_bucket_summary(cursor, bucket_end, bucket_rows, low_quality_threshold))
        cursor = bucket_end
    return buckets


def _bucket_summary(
    bucket_start: datetime,
    bucket_end: datetime,
    rows: list[dict[str, Any]],
    low_quality_threshold: float,
) -> ReplyDriftBucket:
    scores = [
        float(row["quality_score"])
        for row in rows
        if row.get("quality_score") is not None
    ]
    low_quality = [row for row in rows if _is_low_quality(row, low_quality_threshold)]
    flag_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    reviewed_count = 0
    for row in rows:
        flag_counts.update(row["quality_flags"])
        status_counts[row["status"]] += 1
        if row.get("reviewed_at") is not None or row.get("review_event_count", 0) > 0:
            reviewed_count += 1
    draft_count = len(rows)
    return ReplyDriftBucket(
        bucket_start=bucket_start.isoformat(),
        bucket_end=bucket_end.isoformat(),
        draft_count=draft_count,
        scored_count=len(scores),
        reviewed_count=reviewed_count,
        low_quality_count=len(low_quality),
        average_quality_score=round(sum(scores) / len(scores), 3) if scores else None,
        low_quality_rate=round(len(low_quality) / draft_count, 3) if draft_count else 0.0,
        flag_counts=dict(flag_counts),
        flag_rates={
            flag: round(flag_counts.get(flag, 0) / draft_count, 3)
            for flag in sorted(LOW_QUALITY_FLAGS | set(flag_counts))
        },
        status_counts=dict(status_counts),
        reply_ids=tuple(int(row["id"]) for row in rows),
    )


def _totals(
    rows: list[dict[str, Any]],
    buckets: list[ReplyDriftBucket],
    *,
    low_quality_threshold: float,
) -> dict[str, Any]:
    scores = [
        float(row["quality_score"])
        for row in rows
        if row.get("quality_score") is not None
    ]
    flag_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    for row in rows:
        flag_counts.update(row["quality_flags"])
        status_counts[row["status"]] += 1
    low_quality_count = sum(1 for row in rows if _is_low_quality(row, low_quality_threshold))
    reviewed_count = sum(
        1
        for row in rows
        if row.get("reviewed_at") is not None or row.get("review_event_count", 0) > 0
    )
    return {
        "draft_count": len(rows),
        "scored_count": len(scores),
        "reviewed_count": reviewed_count,
        "low_quality_count": low_quality_count,
        "average_quality_score": round(sum(scores) / len(scores), 3) if scores else None,
        "low_quality_rate": round(low_quality_count / len(rows), 3) if rows else 0.0,
        "flag_counts": dict(sorted(flag_counts.items())),
        "status_counts": dict(sorted(status_counts.items())),
        "platforms": sorted({row["platform"] for row in rows}),
        "bucket_count": len(buckets),
    }


def _warnings(
    buckets: list[ReplyDriftBucket],
    *,
    min_bucket_sample: int,
) -> list[str]:
    eligible = [bucket for bucket in buckets if bucket.scored_count >= min_bucket_sample]
    if len(eligible) < 2:
        return []
    midpoint = max(len(eligible) // 2, 1)
    earlier = eligible[:midpoint]
    later = eligible[midpoint:]
    if not later:
        return []

    warnings: list[str] = []
    early_score = _weighted_avg_score(earlier)
    late_score = _weighted_avg_score(later)
    if early_score is not None and late_score is not None and late_score <= early_score - 0.5:
        warnings.append(
            "average quality worsened: "
            f"{early_score:.2f} -> {late_score:.2f} across eligible buckets"
        )

    early_low = _weighted_rate(earlier, "low_quality")
    late_low = _weighted_rate(later, "low_quality")
    if late_low >= early_low + 0.15:
        warnings.append(
            "low-quality draft rate rose: "
            f"{early_low:.0%} -> {late_low:.0%} across eligible buckets"
        )

    for flag in ("generic", "sycophantic", "unsafe"):
        early_flag = _weighted_rate(earlier, flag)
        late_flag = _weighted_rate(later, flag)
        if late_flag >= early_flag + 0.15 and late_flag > 0:
            warnings.append(
                f"{flag} flag rate rose: "
                f"{early_flag:.0%} -> {late_flag:.0%} across eligible buckets"
            )
    return warnings


def _weighted_avg_score(buckets: list[ReplyDriftBucket]) -> float | None:
    total = sum(bucket.scored_count for bucket in buckets)
    if total == 0:
        return None
    score_sum = sum(
        (bucket.average_quality_score or 0.0) * bucket.scored_count
        for bucket in buckets
    )
    return score_sum / total


def _weighted_rate(buckets: list[ReplyDriftBucket], key: str) -> float:
    total = sum(bucket.draft_count for bucket in buckets)
    if total == 0:
        return 0.0
    if key == "low_quality":
        count = sum(bucket.low_quality_count for bucket in buckets)
    else:
        count = sum(bucket.flag_counts.get(key, 0) for bucket in buckets)
    return count / total


def _repeated_low_quality_targets(
    rows: list[dict[str, Any]],
    *,
    threshold: float,
    top_n: int,
) -> list[RepeatedLowQualityTarget]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        handle = row.get("target_handle") or "(unknown)"
        grouped[handle].append(row)

    targets: list[RepeatedLowQualityTarget] = []
    for handle, handle_rows in grouped.items():
        low_rows = [row for row in handle_rows if _is_low_quality(row, threshold)]
        if len(low_rows) < 2:
            continue
        scores = [
            float(row["quality_score"])
            for row in low_rows
            if row.get("quality_score") is not None
        ]
        flags: Counter[str] = Counter()
        for row in low_rows:
            flags.update(row["quality_flags"])
        targets.append(
            RepeatedLowQualityTarget(
                target_handle=handle,
                low_quality_count=len(low_rows),
                draft_count=len(handle_rows),
                average_quality_score=round(sum(scores) / len(scores), 3) if scores else None,
                flags=dict(flags),
                reply_ids=tuple(int(row["id"]) for row in low_rows),
                latest_detected_at=max(row["detected_at"] for row in low_rows).isoformat(),
            )
        )
    targets.sort(
        key=lambda item: (
            -item.low_quality_count,
            item.target_handle,
            item.reply_ids,
        )
    )
    return targets[:top_n]


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    if column in columns:
        return column
    return f"{default} AS {column}"


def _bucket_start(value: datetime, bucket: str) -> datetime:
    value = _ensure_aware(value)
    if bucket == "week":
        start = value - timedelta(days=value.weekday())
        return start.replace(hour=0, minute=0, second=0, microsecond=0)
    return value.replace(hour=0, minute=0, second=0, microsecond=0)


def _is_low_quality(row: dict[str, Any], threshold: float) -> bool:
    score = row.get("quality_score")
    if score is not None and float(score) < threshold:
        return True
    return bool(set(row.get("quality_flags") or []) & LOW_QUALITY_FLAGS)


def _first_in_window(
    timestamps: tuple[datetime | None, ...],
    cutoff: datetime,
    now: datetime,
) -> datetime | None:
    valid = sorted(
        timestamp for timestamp in timestamps if timestamp is not None and cutoff <= timestamp <= now
    )
    return valid[0] if valid else None


def _parse_flags(raw: Any) -> list[str]:
    if not raw:
        return []
    if isinstance(raw, list):
        return sorted({str(flag).strip().lower() for flag in raw if str(flag).strip()})
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return [str(raw).strip().lower()] if str(raw).strip() else []
    if isinstance(parsed, list):
        return sorted({str(flag).strip().lower() for flag in parsed if str(flag).strip()})
    if isinstance(parsed, str) and parsed.strip():
        return [parsed.strip().lower()]
    return []


def _parse_timestamp(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return _ensure_aware(raw)
    text = str(raw).strip()
    if not text:
        return None
    try:
        return _ensure_aware(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return _ensure_aware(datetime.strptime(text, fmt))
        except ValueError:
            continue
    return None


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clean_label(raw: Any) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip().lower()
    return value or None


def _normalize_handle(raw: Any) -> str:
    value = str(raw or "").strip().lstrip("@").lower()
    return value or "(unknown)"


def _float_or_none(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _int_or_none(raw: Any) -> int | None:
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _format_score(score: float | None) -> str:
    return "n/a" if score is None else f"{score:.2f}"
