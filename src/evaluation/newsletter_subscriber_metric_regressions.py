"""Report suspicious newsletter subscriber metric regressions."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DROP_THRESHOLD = 10
DEFAULT_CHURN_DELTA_THRESHOLD = 0.02
DEFAULT_LIMIT = 100
ARTIFACT_TYPE = "newsletter_subscriber_metric_regressions"
TABLE = "newsletter_subscriber_metrics"
REQUIRED_COLUMNS = {"id", "active_subscriber_count", "churn_rate", "raw_metrics", "fetched_at"}
OPTIONAL_COLUMNS = ("subscriber_count", "unsubscribes", "new_subscribers", "net_subscriber_change", "created_at")
REASONS = (
    "active_subscriber_drop",
    "churn_spike",
    "missing_raw_metrics",
    "fetched_at_ordering_anomaly",
)


def build_newsletter_subscriber_metric_regressions_report(
    rows: list[dict[str, Any]],
    *,
    drop_threshold: int = DEFAULT_DROP_THRESHOLD,
    churn_delta_threshold: float = DEFAULT_CHURN_DELTA_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | str | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic report over consecutive subscriber metric snapshots."""
    if drop_threshold < 0:
        raise ValueError("drop_threshold must be non-negative")
    if churn_delta_threshold < 0:
        raise ValueError("churn_delta_threshold must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _coerce_ts(now) if now is not None else datetime.now(timezone.utc)
    normalized = [_normalize(row, index) for index, row in enumerate(rows)]
    ordered = sorted(normalized, key=_snapshot_sort_key)
    findings = []
    previous: dict[str, Any] | None = None
    seen_fetched_at: set[str] = set()
    for snapshot in ordered:
        findings.extend(_raw_metrics_findings(snapshot))
        findings.extend(_ordering_findings(snapshot, previous, seen_fetched_at))
        if snapshot["fetched_at"]:
            seen_fetched_at.add(snapshot["fetched_at"])
        if previous is not None:
            findings.extend(_comparison_findings(previous, snapshot, drop_threshold, churn_delta_threshold))
        previous = snapshot

    findings.sort(key=_finding_sort_key)
    shown = findings[:limit]
    counts = Counter(item["reason"] for item in findings)
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {
            "drop_threshold": drop_threshold,
            "churn_delta_threshold": churn_delta_threshold,
            "limit": limit,
        },
        "totals": {
            "snapshot_count": len(normalized),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_reason": {reason: counts[reason] for reason in REASONS},
        },
        "findings": _group_findings(shown),
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items()) if columns},
        "empty_state": {
            "is_empty": not findings,
            "message": "No newsletter subscriber metric regressions found." if not findings else None,
        },
    }


def build_newsletter_subscriber_metric_regressions_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load newsletter subscriber metrics from SQLite and build the report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    columns = schema.get(TABLE)
    if columns is None:
        return build_newsletter_subscriber_metric_regressions_report([], missing_tables=[TABLE], **kwargs)

    missing = sorted((REQUIRED_COLUMNS | set(OPTIONAL_COLUMNS)) - columns)
    missing_columns = {TABLE: missing} if missing else {}
    rows = [] if REQUIRED_COLUMNS - columns else _load_rows(conn, columns)
    return build_newsletter_subscriber_metric_regressions_report(rows, missing_columns=missing_columns, **kwargs)


def format_newsletter_subscriber_metric_regressions_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_subscriber_metric_regressions_text(report: dict[str, Any]) -> str:
    """Render the report as terminal-friendly text."""
    totals = report["totals"]
    lines = [
        "Newsletter Subscriber Metric Regressions",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"drop_threshold={report['filters']['drop_threshold']} "
            f"churn_delta_threshold={report['filters']['churn_delta_threshold']} "
            f"limit={report['filters']['limit']}"
        ),
        f"Totals: snapshots={totals['snapshot_count']} findings={totals['finding_count']} shown={totals['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "reason | metric_id | previous_metric_id | fetched_at | active_delta | churn_rate_delta | detail"])
    for group in report["findings"]:
        for item in group["items"]:
            lines.append(
                f"{item['reason']} | {item['metric_id']} | {_display(item.get('previous_metric_id'))} | "
                f"{_display(item['fetched_at'])} | {_display(item.get('active_delta'))} | "
                f"{_display(item.get('churn_rate_delta'))} | {_display(item.get('detail'))}"
            )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    selected = [
        "id",
        _column_expr(columns, "subscriber_count", fallback="NULL") + " AS subscriber_count",
        "active_subscriber_count",
        _column_expr(columns, "unsubscribes", fallback="NULL") + " AS unsubscribes",
        "churn_rate",
        _column_expr(columns, "new_subscribers", fallback="NULL") + " AS new_subscribers",
        _column_expr(columns, "net_subscriber_change", fallback="NULL") + " AS net_subscriber_change",
        "raw_metrics",
        "fetched_at",
        _column_expr(columns, "created_at", fallback="NULL") + " AS created_at",
    ]
    return [
        dict(row)
        for row in conn.execute(
            f"SELECT {', '.join(selected)} FROM {TABLE} ORDER BY datetime(COALESCE(fetched_at, '9999-12-31')) ASC, id ASC"
        ).fetchall()
    ]


def _normalize(row: dict[str, Any], index: int) -> dict[str, Any]:
    fetched_at = _clean(row.get("fetched_at")) or None
    parsed_fetched_at = _parse_ts(fetched_at)
    return {
        "input_index": index,
        "metric_id": _int_or_none(row.get("metric_id") or row.get("id")) or 0,
        "subscriber_count": _int_or_none(row.get("subscriber_count")),
        "active_subscriber_count": _int_or_none(row.get("active_subscriber_count")),
        "unsubscribes": _int_or_none(row.get("unsubscribes")),
        "churn_rate": _float_or_none(row.get("churn_rate")),
        "new_subscribers": _int_or_none(row.get("new_subscribers")),
        "net_subscriber_change": _int_or_none(row.get("net_subscriber_change")),
        "raw_metrics": row.get("raw_metrics"),
        "fetched_at": fetched_at,
        "parsed_fetched_at": parsed_fetched_at,
        "created_at": _clean(row.get("created_at")) or None,
    }


def _raw_metrics_findings(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    raw_metrics = snapshot.get("raw_metrics")
    if raw_metrics is None or _clean(raw_metrics) == "":
        return [_finding("missing_raw_metrics", snapshot, detail="raw_metrics is empty")]
    if isinstance(raw_metrics, dict):
        return [] if raw_metrics else [_finding("missing_raw_metrics", snapshot, detail="raw_metrics is empty")]
    try:
        parsed = json.loads(str(raw_metrics))
    except (TypeError, ValueError):
        return [_finding("missing_raw_metrics", snapshot, detail="raw_metrics is not valid JSON")]
    if not isinstance(parsed, dict) or not parsed:
        return [_finding("missing_raw_metrics", snapshot, detail="raw_metrics is not a populated object")]
    return []


def _ordering_findings(
    snapshot: dict[str, Any],
    previous: dict[str, Any] | None,
    seen_fetched_at: set[str],
) -> list[dict[str, Any]]:
    findings = []
    if snapshot["parsed_fetched_at"] is None:
        findings.append(_finding("fetched_at_ordering_anomaly", snapshot, detail="fetched_at is missing or invalid"))
    if snapshot["fetched_at"] and snapshot["fetched_at"] in seen_fetched_at:
        findings.append(_finding("fetched_at_ordering_anomaly", snapshot, detail="duplicate fetched_at"))
    if previous is not None and snapshot["metric_id"] < previous["metric_id"]:
        findings.append(
            _finding(
                "fetched_at_ordering_anomaly",
                snapshot,
                previous=previous,
                detail="metric id decreases in fetched_at order",
            )
        )
    return findings


def _comparison_findings(
    previous: dict[str, Any],
    current: dict[str, Any],
    drop_threshold: int,
    churn_delta_threshold: float,
) -> list[dict[str, Any]]:
    findings = []
    active_delta = _delta(current["active_subscriber_count"], previous["active_subscriber_count"])
    if active_delta is not None and active_delta <= -drop_threshold:
        findings.append(
            _finding(
                "active_subscriber_drop",
                current,
                previous=previous,
                active_delta=active_delta,
                detail=f"active subscribers dropped by {abs(active_delta)}",
            )
        )
    churn_delta = _delta(current["churn_rate"], previous["churn_rate"])
    if churn_delta is not None and churn_delta >= churn_delta_threshold:
        findings.append(
            _finding(
                "churn_spike",
                current,
                previous=previous,
                churn_rate_delta=churn_delta,
                detail=f"churn rate increased by {churn_delta}",
            )
        )
    return findings


def _finding(reason: str, snapshot: dict[str, Any], *, previous: dict[str, Any] | None = None, **extra: Any) -> dict[str, Any]:
    previous = previous or {}
    return {
        "reason": reason,
        "metric_id": snapshot["metric_id"],
        "previous_metric_id": previous.get("metric_id"),
        "fetched_at": snapshot["fetched_at"],
        "previous_fetched_at": previous.get("fetched_at"),
        "subscriber_count": snapshot["subscriber_count"],
        "previous_subscriber_count": previous.get("subscriber_count"),
        "active_subscriber_count": snapshot["active_subscriber_count"],
        "previous_active_subscriber_count": previous.get("active_subscriber_count"),
        "churn_rate": snapshot["churn_rate"],
        "previous_churn_rate": previous.get("churn_rate"),
        "raw_metrics_present": _raw_metrics_present(snapshot),
        **extra,
    }


def _raw_metrics_present(snapshot: dict[str, Any]) -> bool:
    raw_metrics = snapshot.get("raw_metrics")
    if raw_metrics is None or _clean(raw_metrics) == "":
        return False
    if isinstance(raw_metrics, dict):
        return bool(raw_metrics)
    try:
        parsed = json.loads(str(raw_metrics))
    except (TypeError, ValueError):
        return False
    return isinstance(parsed, dict) and bool(parsed)


def _group_findings(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[item["reason"]].append(item)
    return [{"reason": reason, "count": len(grouped[reason]), "items": grouped[reason]} for reason in REASONS if reason in grouped]


def _snapshot_sort_key(snapshot: dict[str, Any]) -> tuple[Any, ...]:
    parsed = snapshot["parsed_fetched_at"]
    return (parsed.isoformat() if parsed is not None else "9999-12-31T00:00:00+00:00", snapshot["metric_id"], snapshot["input_index"])


def _finding_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (REASONS.index(item["reason"]), _clean(item.get("fetched_at")) or "9999-12-31", _int_sort(item.get("metric_id")), _clean(item.get("detail")))


def _delta(value: int | float | None, previous: int | float | None) -> int | float | None:
    if value is None or previous is None:
        return None
    result = value - previous
    return round(result, 6) if isinstance(value, float) or isinstance(previous, float) else result


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row["name"]): {str(column["name"]) for column in conn.execute(f"PRAGMA table_info({_quote(str(row['name']))})")} for row in rows}


def _column_expr(columns: set[str], column: str, *, fallback: str) -> str:
    return _quote(column) if column in columns else fallback


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _display(value: Any) -> str:
    return "-" if value is None or value == "" else str(value)


def _quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_sort(value: Any) -> int:
    parsed = _int_or_none(value)
    return parsed if parsed is not None else -1


def _parse_ts(value: Any) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    for parser in (
        lambda raw: datetime.fromisoformat(raw.replace("Z", "+00:00")),
        lambda raw: datetime.strptime(raw, "%Y-%m-%d %H:%M:%S"),
        lambda raw: datetime.strptime(raw, "%Y-%m-%d"),
    ):
        try:
            return _utc(parser(text))
        except ValueError:
            continue
    return None


def _coerce_ts(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return _utc(value)
    parsed = _parse_ts(value)
    if parsed is None:
        raise ValueError(f"invalid timestamp: {value}")
    return parsed


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
