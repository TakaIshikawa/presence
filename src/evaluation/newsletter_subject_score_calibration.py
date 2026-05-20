"""Compare newsletter subject scores and selections against engagement outcomes."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_LIMIT = 25
DEFAULT_MIN_SCORE_GAP = 0.2
DEFAULT_LOW_OUTCOME_THRESHOLD = 0.2
DEFAULT_HIGH_OUTCOME_THRESHOLD = 0.4
DEFAULT_MIN_CALIBRATION_GAP = 0.15


def build_newsletter_subject_score_calibration_report(
    rows: list[dict[str, Any]],
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_score_gap: float = DEFAULT_MIN_SCORE_GAP,
    low_outcome_threshold: float = DEFAULT_LOW_OUTCOME_THRESHOLD,
    high_outcome_threshold: float = DEFAULT_HIGH_OUTCOME_THRESHOLD,
    min_calibration_gap: float = DEFAULT_MIN_CALIBRATION_GAP,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Build a deterministic score calibration report from joined candidate rows."""
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if min_score_gap < 0:
        raise ValueError("min_score_gap must be non-negative")
    if low_outcome_threshold < 0 or high_outcome_threshold < 0:
        raise ValueError("outcome thresholds must be non-negative")
    if min_calibration_gap < 0:
        raise ValueError("min_calibration_gap must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    normalized = [_normalize_row(row) for row in rows]
    normalized = [
        row
        for row in normalized
        if row["candidate_created_at_dt"] is None or cutoff <= row["candidate_created_at_dt"] <= generated_at
    ]
    groups = _groups(normalized)

    over_scored_selected = []
    under_scored_unselected = []
    source_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for key, candidates in groups.items():
        candidates_with_outcome = [candidate for candidate in candidates if candidate["has_outcome"]]
        if not candidates_with_outcome:
            continue
        selected = _selected_candidate(candidates_with_outcome)
        selected_score = selected["score_normalized"] if selected else None
        for candidate in candidates_with_outcome:
            source_rows[candidate["source"]].append(candidate)
            gap = round(candidate["score_normalized"] - candidate["outcome_metric"], 4)
            if candidate["selected"] and gap >= min_score_gap and candidate["outcome_metric"] <= low_outcome_threshold:
                over_scored_selected.append(_candidate_item(candidate, key, gap=gap))
            if (
                not candidate["selected"]
                and selected_score is not None
                and candidate["outcome_metric"] >= high_outcome_threshold
                and round(selected_score - candidate["score_normalized"], 4) >= min_score_gap
            ):
                under_scored_unselected.append(
                    _candidate_item(candidate, key, gap=round(selected_score - candidate["score_normalized"], 4))
                )

    source_calibration_gap = []
    for source, source_candidates in source_rows.items():
        avg_score = _avg(candidate["score_normalized"] for candidate in source_candidates)
        avg_outcome = _avg(candidate["outcome_metric"] for candidate in source_candidates)
        gap = round(avg_score - avg_outcome, 4)
        if abs(gap) < min_calibration_gap:
            continue
        source_calibration_gap.append(
            {
                "source": source,
                "candidate_count": len(source_candidates),
                "selected_count": sum(1 for candidate in source_candidates if candidate["selected"]),
                "avg_score": avg_score,
                "avg_outcome_metric": avg_outcome,
                "calibration_gap": gap,
                "direction": "over_scored" if gap > 0 else "under_scored",
            }
        )

    over_scored_selected.sort(key=lambda item: (-item["score_outcome_gap"], item["newsletter_send_id"] or 0, item["candidate_id"]))
    under_scored_unselected.sort(key=lambda item: (-item["selected_score_gap"], item["newsletter_send_id"] or 0, item["candidate_id"]))
    source_calibration_gap.sort(key=lambda item: (-abs(item["calibration_gap"]), item["source"]))

    outcome_groups = {
        key
        for key, candidates in groups.items()
        if any(candidate["has_outcome"] for candidate in candidates)
    }
    return {
        "artifact_type": "newsletter_subject_score_calibration",
        "generated_at": generated_at.isoformat(),
        "missing_tables": list(missing_tables),
        "thresholds": {
            "lookback_days": lookback_days,
            "lookback_start": cutoff.isoformat(),
            "min_score_gap": min_score_gap,
            "low_outcome_threshold": low_outcome_threshold,
            "high_outcome_threshold": high_outcome_threshold,
            "min_calibration_gap": min_calibration_gap,
            "limit": limit,
        },
        "summary": {
            "candidate_count": len(normalized),
            "candidate_pool_count": len(groups),
            "outcome_pool_count": len(outcome_groups),
            "over_scored_selected_count": len(over_scored_selected),
            "under_scored_unselected_count": len(under_scored_unselected),
            "source_calibration_gap_count": len(source_calibration_gap),
        },
        "over_scored_selected": over_scored_selected[:limit],
        "under_scored_unselected": under_scored_unselected[:limit],
        "source_calibration_gap": source_calibration_gap[:limit],
    }


def build_newsletter_subject_score_calibration_report_from_db(
    db_or_conn: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = ("newsletter_subject_candidates", "newsletter_engagement")
    missing_tables = tuple(table for table in required if table not in schema)
    rows = _load_rows(conn, schema) if not missing_tables else []
    return build_newsletter_subject_score_calibration_report(
        rows,
        missing_tables=missing_tables,
        **kwargs,
    )


def format_newsletter_subject_score_calibration_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_subject_score_calibration_text(report: dict[str, Any]) -> str:
    thresholds = report["thresholds"]
    summary = report["summary"]
    lines = [
        "Newsletter Subject Score Calibration",
        f"Generated: {report['generated_at']}",
        (
            f"Window: days={thresholds['lookback_days']} start={thresholds['lookback_start']} "
            f"limit={thresholds['limit']}"
        ),
        (
            f"Thresholds: min_score_gap={thresholds['min_score_gap']:g} "
            f"low_outcome={thresholds['low_outcome_threshold']:g} "
            f"high_outcome={thresholds['high_outcome_threshold']:g}"
        ),
        (
            f"Totals: candidates={summary['candidate_count']} pools={summary['candidate_pool_count']} "
            f"outcome_pools={summary['outcome_pool_count']} "
            f"over_scored_selected={summary['over_scored_selected_count']} "
            f"under_scored_unselected={summary['under_scored_unselected_count']} "
            f"source_gaps={summary['source_calibration_gap_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["over_scored_selected"]:
        lines.extend(["", "Over-scored selected:"])
        for item in report["over_scored_selected"]:
            lines.append(
                f"- send={item['newsletter_send_id'] or '-'} issue={item['issue_id'] or '-'} "
                f"candidate={item['candidate_id']} score={item['score_normalized']} "
                f"outcome={item['outcome_metric']} gap={item['score_outcome_gap']}: {item['subject']}"
            )
    if report["under_scored_unselected"]:
        lines.extend(["", "Under-scored unselected:"])
        for item in report["under_scored_unselected"]:
            lines.append(
                f"- send={item['newsletter_send_id'] or '-'} issue={item['issue_id'] or '-'} "
                f"candidate={item['candidate_id']} score={item['score_normalized']} "
                f"outcome={item['outcome_metric']} selected_gap={item['selected_score_gap']}: {item['subject']}"
            )
    if report["source_calibration_gap"]:
        lines.extend(["", "Source calibration gaps:"])
        for item in report["source_calibration_gap"]:
            lines.append(
                f"- source={item['source']} candidates={item['candidate_count']} "
                f"avg_score={item['avg_score']} avg_outcome={item['avg_outcome_metric']} "
                f"gap={item['calibration_gap']} direction={item['direction']}"
            )
    if (
        not report["over_scored_selected"]
        and not report["under_scored_unselected"]
        and not report["source_calibration_gap"]
        and not report["missing_tables"]
    ):
        lines.append("No newsletter subject score calibration issues found.")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    candidate_columns = schema["newsletter_subject_candidates"]
    engagement_columns = schema["newsletter_engagement"]
    send_columns = schema.get("newsletter_sends", set())
    select_columns = [
        _column_expr(candidate_columns, "id", "c", "candidate_id", default="c.rowid"),
        _column_expr(candidate_columns, "newsletter_send_id", "c", "newsletter_send_id", default="NULL"),
        _column_expr(candidate_columns, "issue_id", "c", "issue_id", default="NULL"),
        _column_expr(candidate_columns, "subject", "c", "subject", default="''"),
        _column_expr(candidate_columns, "score", "c", "score", default="0"),
        _column_expr(candidate_columns, "selected", "c", "selected", default="0"),
        _column_expr(candidate_columns, "source", "c", "source", default="'unknown'"),
        _column_expr(candidate_columns, "rank", "c", "rank", default="NULL"),
        _column_expr(candidate_columns, "created_at", "c", "candidate_created_at", default="NULL"),
        _column_expr(engagement_columns, "opens", "ne", "opens", default="0"),
        _column_expr(engagement_columns, "clicks", "ne", "clicks", default="0"),
        _column_expr(engagement_columns, "unsubscribes", "ne", "unsubscribes", default="0"),
        _column_expr(engagement_columns, "fetched_at", "ne", "engagement_fetched_at", default="NULL"),
        _column_expr(send_columns, "subscriber_count", "ns", "subscriber_count", default="NULL"),
    ]
    send_join = (
        "LEFT JOIN newsletter_sends ns ON ns.id = c.newsletter_send_id"
        if "newsletter_sends" in schema and "id" in send_columns and "newsletter_send_id" in candidate_columns
        else "LEFT JOIN (SELECT NULL AS id, NULL AS subscriber_count) ns ON 0"
    )
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(select_columns)}
                FROM newsletter_subject_candidates c
                LEFT JOIN newsletter_engagement ne
                  ON ne.rowid = (
                    SELECT latest.rowid
                    FROM newsletter_engagement latest
                    WHERE (
                        c.newsletter_send_id IS NOT NULL
                        AND latest.newsletter_send_id = c.newsletter_send_id
                    ) OR (
                        c.issue_id IS NOT NULL
                        AND latest.issue_id = c.issue_id
                    )
                    ORDER BY latest.fetched_at DESC, latest.rowid DESC
                    LIMIT 1
                  )
                {send_join}
                ORDER BY c.rowid ASC"""
        ).fetchall()
    ]


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    opens = _int(row.get("opens"))
    clicks = _int(row.get("clicks"))
    unsubscribes = _int(row.get("unsubscribes"))
    subscriber_count = _int_or_none(row.get("subscriber_count"))
    outcome_metric = _outcome_metric(
        opens=opens,
        clicks=clicks,
        unsubscribes=unsubscribes,
        subscriber_count=subscriber_count,
    )
    score_normalized = _score(row.get("score"))
    return {
        "candidate_id": _int(_first(row, "candidate_id", "id")),
        "newsletter_send_id": _int_or_none(row.get("newsletter_send_id")),
        "issue_id": _text_or_none(row.get("issue_id")),
        "subject": str(row.get("subject") or ""),
        "score": _float(row.get("score")),
        "score_normalized": score_normalized,
        "selected": _truthy(row.get("selected")),
        "source": str(row.get("source") or "unknown").strip() or "unknown",
        "rank": _int_or_none(row.get("rank")),
        "candidate_created_at": row.get("candidate_created_at"),
        "candidate_created_at_dt": _parse_datetime(row.get("candidate_created_at")),
        "engagement_fetched_at": row.get("engagement_fetched_at"),
        "opens": opens,
        "clicks": clicks,
        "unsubscribes": unsubscribes,
        "subscriber_count": subscriber_count,
        "has_outcome": row.get("engagement_fetched_at") is not None,
        "outcome_metric": outcome_metric,
    }


def _groups(rows: list[dict[str, Any]]) -> dict[tuple[str, int | str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, int | str], list[dict[str, Any]]] = {}
    for row in rows:
        if row["newsletter_send_id"] is not None:
            key: tuple[str, int | str] = ("newsletter_send_id", row["newsletter_send_id"])
        else:
            key = ("issue_id", row["issue_id"] or "")
        grouped.setdefault(key, []).append(row)
    return grouped


def _selected_candidate(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    selected = [row for row in rows if row["selected"]]
    if not selected:
        return None
    return sorted(selected, key=lambda row: (row["rank"] is None, row["rank"] or 0, row["candidate_id"]))[0]


def _candidate_item(row: dict[str, Any], key: tuple[str, int | str], *, gap: float) -> dict[str, Any]:
    item = {
        "group_type": key[0],
        "newsletter_send_id": row["newsletter_send_id"],
        "issue_id": row["issue_id"],
        "candidate_id": row["candidate_id"],
        "subject": row["subject"],
        "source": row["source"],
        "selected": row["selected"],
        "score": row["score"],
        "score_normalized": row["score_normalized"],
        "outcome_metric": row["outcome_metric"],
        "opens": row["opens"],
        "clicks": row["clicks"],
        "unsubscribes": row["unsubscribes"],
        "subscriber_count": row["subscriber_count"],
        "engagement_fetched_at": row["engagement_fetched_at"],
    }
    if row["selected"]:
        item["score_outcome_gap"] = gap
    else:
        item["selected_score_gap"] = gap
    return item


def _outcome_metric(*, opens: int, clicks: int, unsubscribes: int, subscriber_count: int | None) -> float:
    if subscriber_count and subscriber_count > 0:
        value = (opens / subscriber_count) + (2 * clicks / subscriber_count) - (3 * unsubscribes / subscriber_count)
        return round(min(1.0, max(0.0, value)), 4)
    activity = opens + clicks + unsubscribes
    if activity <= 0:
        return 0.0
    value = (opens + (2 * clicks) - (3 * unsubscribes)) / max(activity * 3, 1)
    return round(min(1.0, max(0.0, value)), 4)


def _score(value: Any) -> float:
    score = _float(value)
    if score > 1:
        score /= 10
    return round(min(1.0, max(0.0, score)), 4)


def _avg(values: Any) -> float:
    items = list(values)
    return round(sum(items) / len(items), 4) if items else 0.0


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], column: str, alias: str, output: str, *, default: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"{default} AS {output}"


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _text_or_none(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    return text or None


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "selected"}
    return bool(value)
