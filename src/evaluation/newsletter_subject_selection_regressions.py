"""Report selected newsletter subject candidates that lost on score."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Mapping


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_MIN_SCORE_GAP = 0.0
DEFAULT_LIMIT = 25


def build_newsletter_subject_selection_regressions_report(
    candidate_rows: list[dict[str, Any]],
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_score_gap: float = DEFAULT_MIN_SCORE_GAP,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Build a deterministic report from newsletter subject candidate rows."""
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if min_score_gap < 0:
        raise ValueError("min_score_gap must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    lookback_start = generated_at - timedelta(days=lookback_days)
    groups = _group_candidate_rows(candidate_rows)
    regression_items = []
    selected_pool_count = 0

    for key, rows in groups.items():
        selected = _selected_candidate(rows)
        if selected is None:
            continue
        selected_pool_count += 1
        selected_score = _score(selected)
        higher = [
            row
            for row in rows
            if not _truthy(row.get("selected"))
            and _score(row) > selected_score
            and round(_score(row) - selected_score, 8) >= min_score_gap
        ]
        if not higher:
            continue
        higher.sort(key=lambda row: (-_score(row), _candidate_id(row)))
        best = higher[0]
        score_gap = round(_score(best) - selected_score, 4)
        regression_items.append(
            {
                "group_type": key[0],
                "newsletter_send_id": _int_or_none(selected.get("newsletter_send_id")),
                "issue_id": _text_or_none(selected.get("issue_id")),
                "candidate_count": len(rows),
                "selected_candidate": _candidate_payload(selected),
                "best_candidate": _candidate_payload(best),
                "score_gap": score_gap,
                "higher_scored_candidate_count": len(higher),
            }
        )

    regression_items.sort(key=_regression_sort_key)
    shown = regression_items[:limit]
    return {
        "artifact_type": "newsletter_subject_selection_regressions",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "limit": limit,
            "lookback_days": lookback_days,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": lookback_start.isoformat(),
            "min_score_gap": min_score_gap,
        },
        "missing_tables": list(missing_tables),
        "total_regressions": len(regression_items),
        "regression_items": shown,
        "issue_examples": shown[:5],
        "summary": {
            "candidate_count": len(candidate_rows),
            "candidate_pool_count": len(groups),
            "selected_pool_count": selected_pool_count,
            "regressed_selection_count": len(regression_items),
            "shown_regressions": len(shown),
            "regressions_by_group_type": dict(
                sorted(Counter(item["group_type"] for item in regression_items).items())
            ),
        },
    }


def build_newsletter_subject_selection_regressions_report_from_db(
    db_or_conn: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = () if "newsletter_subject_candidates" in schema else ("newsletter_subject_candidates",)
    if missing_tables:
        return build_newsletter_subject_selection_regressions_report(
            [],
            missing_tables=missing_tables,
            **kwargs,
        )

    lookback_days = kwargs.get("lookback_days", DEFAULT_LOOKBACK_DAYS)
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    now = _utc(kwargs.get("now") or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=lookback_days)
    rows = _load_candidate_rows(
        conn,
        schema["newsletter_subject_candidates"],
        cutoff=cutoff,
        window_end=now,
    )
    return build_newsletter_subject_selection_regressions_report(
        rows,
        missing_tables=missing_tables,
        **{**kwargs, "now": now},
    )


def format_newsletter_subject_selection_regressions_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_subject_selection_regressions_text(report: dict[str, Any]) -> str:
    thresholds = report["thresholds"]
    summary = report["summary"]
    lines = [
        "Newsletter Subject Selection Regressions",
        f"Generated: {report['generated_at']}",
        (
            f"Window: days={thresholds['lookback_days']} start={thresholds['lookback_start']} "
            f"end={thresholds['lookback_end']}"
        ),
        (
            f"Thresholds: min_score_gap>={thresholds['min_score_gap']:g} "
            f"limit={thresholds['limit']}"
        ),
        (
            f"Totals: candidates={summary['candidate_count']} pools={summary['candidate_pool_count']} "
            f"selected_pools={summary['selected_pool_count']} "
            f"regressions={report['total_regressions']} shown={summary['shown_regressions']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["regression_items"]:
        lines.append("No newsletter subject selection regressions found.")
        return "\n".join(lines)

    lines.extend(["", "Regressions:"])
    for item in report["regression_items"]:
        selected = item["selected_candidate"]
        best = item["best_candidate"]
        lines.append(
            f"- send={item['newsletter_send_id'] or '-'} issue={item['issue_id'] or '-'} "
            f"group={item['group_type']} gap={item['score_gap']:g} "
            f"selected=#{selected['candidate_id']} score={selected['score']:g} "
            f"best=#{best['candidate_id']} score={best['score']:g}"
        )
        lines.append(f"  selected_subject={selected['subject']}")
        lines.append(f"  best_subject={best['subject']}")
    return "\n".join(lines)


def _load_candidate_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
    window_end: datetime,
) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, "id", "nsc", "candidate_id", default="nsc.rowid"),
        _column_expr(columns, "newsletter_send_id", "nsc", "newsletter_send_id", default="NULL"),
        _column_expr(columns, "issue_id", "nsc", "issue_id", default="NULL"),
        _column_expr(columns, "subject", "nsc", "subject", default="''"),
        _column_expr(columns, "score", "nsc", "score", default="0"),
        _column_expr(columns, "rank", "nsc", "rank", default="NULL"),
        _column_expr(columns, "source", "nsc", "source", default="'unknown'"),
        _column_expr(columns, "selected", "nsc", "selected", default="0"),
        _column_expr(columns, "created_at", "nsc", "created_at", default="NULL"),
    ]
    where = ""
    params: list[Any] = []
    if "created_at" in columns:
        where = "WHERE datetime(nsc.created_at) >= datetime(?) AND datetime(nsc.created_at) <= datetime(?)"
        params = [_sqlite_ts(cutoff), _sqlite_ts(window_end)]
    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM newsletter_subject_candidates nsc
            {where}
            ORDER BY {_order_expr(columns)}""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _group_candidate_rows(
    rows: list[Mapping[str, Any]],
) -> dict[tuple[str, int | str], list[Mapping[str, Any]]]:
    groups: dict[tuple[str, int | str], list[Mapping[str, Any]]] = {}
    for row in rows:
        send_id = _int_or_none(row.get("newsletter_send_id"))
        issue_id = _text_or_none(row.get("issue_id"))
        key: tuple[str, int | str]
        if send_id is not None:
            key = ("newsletter_send_id", send_id)
        else:
            key = ("issue_id", issue_id or "")
        groups.setdefault(key, []).append(row)
    return groups


def _selected_candidate(rows: list[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    selected = [row for row in rows if _truthy(row.get("selected"))]
    if not selected:
        return None
    return sorted(selected, key=_candidate_id)[0]


def _candidate_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": _candidate_id(row),
        "created_at": row.get("created_at"),
        "rank": _int_or_none(row.get("rank")),
        "score": round(_score(row), 4),
        "source": str(row.get("source") or "unknown"),
        "subject": str(row.get("subject") or ""),
    }


def _regression_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        -float(item["score_gap"]),
        item["newsletter_send_id"] is None,
        item["newsletter_send_id"] or 0,
        item["issue_id"] or "",
        item["selected_candidate"]["candidate_id"],
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], column: str, alias: str, output: str, *, default: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"{default} AS {output}"


def _order_expr(columns: set[str]) -> str:
    parts = []
    if "newsletter_send_id" in columns:
        parts.append("nsc.newsletter_send_id ASC")
    if "issue_id" in columns:
        parts.append("nsc.issue_id ASC")
    if "rank" in columns:
        parts.append("nsc.rank ASC")
    if "id" in columns:
        parts.append("nsc.id ASC")
    return ", ".join(parts) or "nsc.rowid ASC"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _sqlite_ts(value: datetime) -> str:
    return _utc(value).strftime("%Y-%m-%d %H:%M:%S")


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _score(row: Mapping[str, Any]) -> float:
    try:
        return float(row.get("score") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _candidate_id(row: Mapping[str, Any]) -> int:
    return _int_or_none(row.get("candidate_id") if "candidate_id" in row else row.get("id")) or 0


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
