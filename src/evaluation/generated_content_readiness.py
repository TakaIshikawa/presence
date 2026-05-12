"""Triage unpublished generated content rows for publish readiness blockers."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 5
DEFAULT_MIN_EVAL_SCORE = 7.0

BLOCKER_CODES = (
    "missing_eval_score",
    "low_eval_score",
    "missing_eval_feedback",
    "empty_content",
    "missing_sources",
    "claim_check_failed",
    "persona_guard_failed",
    "stale_0_7_days",
    "stale_8_14_days",
    "stale_15_30_days",
    "stale_31_plus_days",
)
AGE_BUCKETS = (
    ("stale_0_7_days", 0, 7),
    ("stale_8_14_days", 8, 14),
    ("stale_15_30_days", 15, 30),
    ("stale_31_plus_days", 31, None),
)


def build_generated_content_readiness_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    min_eval_score: float = DEFAULT_MIN_EVAL_SCORE,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a deterministic read-only triage report for unpublished drafts."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if min_eval_score < 0:
        raise ValueError("min_eval_score must be non-negative")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {
        "days": days,
        "limit": limit,
        "min_eval_score": min_eval_score,
        "stale_threshold_days": days,
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [] if "generated_content" in schema else ["generated_content"]
    missing_columns = _missing_columns(schema)
    if missing_tables or "id" in missing_columns.get("generated_content", ()):
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_generated_content(conn, schema)
    claim_rows = _claim_rows(conn, schema, [int(row["id"]) for row in rows])
    persona_rows = _persona_rows(conn, schema, [int(row["id"]) for row in rows])
    groups = _blocker_groups(
        rows,
        claim_rows=claim_rows,
        persona_rows=persona_rows,
        generated_at=generated_at,
        stale_threshold_days=days,
        min_eval_score=min_eval_score,
        limit=limit,
    )
    age_buckets = _age_bucket_counts(rows, generated_at)
    rows_with_blockers = {
        content_id
        for group in groups
        for content_id in group["all_content_ids"]
    }
    for group in groups:
        del group["all_content_ids"]

    return {
        "artifact_type": "generated_content_readiness",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "blocker_groups": len(groups),
            "rows_scanned": len(rows),
            "rows_with_blockers": len(rows_with_blockers),
        },
        "age_buckets": age_buckets,
        "blockers": groups,
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def format_generated_content_readiness_json(report: dict[str, Any]) -> str:
    """Serialize a generated-content readiness report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_generated_content_readiness_text(report: dict[str, Any]) -> str:
    """Render a concise human-readable readiness triage report."""
    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Generated Content Readiness",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days={filters['days']} limit={filters['limit']} "
            f"min_eval_score={filters['min_eval_score']}"
        ),
        (
            f"Totals: scanned={totals['rows_scanned']} "
            f"rows_with_blockers={totals['rows_with_blockers']} "
            f"blocker_groups={totals['blocker_groups']}"
        ),
        "Age buckets: " + _format_count_map(report["age_buckets"]),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append(
            "Missing columns: "
            + "; ".join(
                f"{table}({', '.join(columns)})"
                for table, columns in sorted(report["missing_columns"].items())
                if columns
            )
        )
    if not report["blockers"]:
        lines.extend(["", "No generated content readiness blockers found."])
        return "\n".join(lines)

    lines.extend(["", "Blockers:"])
    for group in report["blockers"]:
        lines.append(
            f"- code={group['code']} count={group['count']} "
            f"representative_ids={_format_ids(group['representative_content_ids'])}"
        )
    return "\n".join(lines)


def _load_generated_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    select_columns = [
        "gc.id AS id",
        _column_expr(columns, "content", "gc", "content"),
        _column_expr(columns, "eval_score", "gc", "eval_score"),
        _column_expr(columns, "eval_feedback", "gc", "eval_feedback"),
        _column_expr(columns, "source_commits", "gc", "source_commits"),
        _column_expr(columns, "source_messages", "gc", "source_messages"),
        _column_expr(columns, "created_at", "gc", "created_at"),
        _column_expr(columns, "published", "gc", "published"),
        _column_expr(columns, "published_url", "gc", "published_url"),
        _column_expr(columns, "published_at", "gc", "published_at"),
    ]
    where = [_unpublished_filter(columns)]
    order = "gc.created_at ASC, gc.id ASC" if "created_at" in columns else "gc.id ASC"
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {", ".join(select_columns)}
                FROM generated_content gc
                WHERE {' AND '.join(where)}
                ORDER BY {order}"""
        ).fetchall()
    ]


def _blocker_groups(
    rows: list[dict[str, Any]],
    *,
    claim_rows: dict[int, dict[str, Any]],
    persona_rows: dict[int, dict[str, Any]],
    generated_at: datetime,
    stale_threshold_days: int,
    min_eval_score: float,
    limit: int,
) -> list[dict[str, Any]]:
    content_ids_by_code: dict[str, list[int]] = {code: [] for code in BLOCKER_CODES}
    for row in rows:
        content_id = int(row["id"])
        score = _number(row.get("eval_score"))
        if score is None:
            content_ids_by_code["missing_eval_score"].append(content_id)
        elif score < min_eval_score:
            content_ids_by_code["low_eval_score"].append(content_id)
        if not _text(row.get("eval_feedback")):
            content_ids_by_code["missing_eval_feedback"].append(content_id)
        if not _text(row.get("content")):
            content_ids_by_code["empty_content"].append(content_id)
        if not _json_list(row.get("source_commits")) and not _json_list(
            row.get("source_messages")
        ):
            content_ids_by_code["missing_sources"].append(content_id)
        claim = claim_rows.get(content_id)
        if claim and int(claim.get("unsupported_count") or 0) > 0:
            content_ids_by_code["claim_check_failed"].append(content_id)
        persona = persona_rows.get(content_id)
        if persona and _persona_failed(persona):
            content_ids_by_code["persona_guard_failed"].append(content_id)

        age_days = _age_days(row.get("created_at"), generated_at)
        if age_days is not None and age_days >= stale_threshold_days:
            content_ids_by_code[_age_bucket_code(age_days)].append(content_id)

    groups = []
    for code in BLOCKER_CODES:
        content_ids = sorted(content_ids_by_code[code])
        if content_ids:
            groups.append(
                {
                    "code": code,
                    "count": len(content_ids),
                    "representative_content_ids": content_ids[:limit],
                    "all_content_ids": content_ids,
                }
            )
    return groups


def _claim_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: list[int],
) -> dict[int, dict[str, Any]]:
    if not content_ids or "content_claim_checks" not in schema:
        return {}
    columns = schema["content_claim_checks"]
    if "content_id" not in columns:
        return {}
    placeholders = ", ".join("?" for _ in content_ids)
    select_columns = [
        "content_id",
        _column_expr(columns, "unsupported_count", "ccc", "unsupported_count"),
    ]
    rows = conn.execute(
        f"""SELECT {", ".join(select_columns)}
            FROM content_claim_checks ccc
            WHERE ccc.content_id IN ({placeholders})""",
        content_ids,
    ).fetchall()
    return {int(row["content_id"]): dict(row) for row in rows}


def _persona_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: list[int],
) -> dict[int, dict[str, Any]]:
    if not content_ids or "content_persona_guard" not in schema:
        return {}
    columns = schema["content_persona_guard"]
    if "content_id" not in columns:
        return {}
    placeholders = ", ".join("?" for _ in content_ids)
    select_columns = [
        "content_id",
        _column_expr(columns, "passed", "cpg", "passed"),
        _column_expr(columns, "status", "cpg", "status"),
    ]
    rows = conn.execute(
        f"""SELECT {", ".join(select_columns)}
            FROM content_persona_guard cpg
            WHERE cpg.content_id IN ({placeholders})""",
        content_ids,
    ).fetchall()
    return {int(row["content_id"]): dict(row) for row in rows}


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    return {
        "artifact_type": "generated_content_readiness",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "blocker_groups": 0,
            "rows_scanned": 0,
            "rows_with_blockers": 0,
        },
        "age_buckets": {code: 0 for code, _start, _end in AGE_BUCKETS},
        "blockers": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _age_bucket_counts(
    rows: list[dict[str, Any]],
    now: datetime,
) -> dict[str, int]:
    counts = Counter(
        _age_bucket_code(age_days)
        for row in rows
        if (age_days := _age_days(row.get("created_at"), now)) is not None
    )
    return {code: counts.get(code, 0) for code, _start, _end in AGE_BUCKETS}


def _age_bucket_code(age_days: int) -> str:
    for code, start, end in AGE_BUCKETS:
        if age_days >= start and (end is None or age_days <= end):
            return code
    return "stale_31_plus_days"


def _age_days(value: Any, now: datetime) -> int | None:
    created = _parse_datetime(value)
    if created is None:
        return None
    return max(0, (now - created).days)


def _persona_failed(row: dict[str, Any]) -> bool:
    status = str(row.get("status") or "").strip().lower()
    passed = row.get("passed")
    if isinstance(passed, str):
        passed_value = passed.strip().lower() not in {"0", "false", "fail", "failed"}
    else:
        passed_value = bool(passed)
    return not passed_value or status in {"fail", "failed", "blocked", "rejected"}


def _unpublished_filter(columns: set[str]) -> str:
    filters = []
    if "published" in columns:
        filters.append("(gc.published IS NULL OR gc.published = 0)")
    if "published_url" in columns:
        filters.append("(gc.published_url IS NULL OR TRIM(gc.published_url) = '')")
    if "published_at" in columns:
        filters.append("(gc.published_at IS NULL OR TRIM(gc.published_at) = '')")
    return " AND ".join(filters) if filters else "1 = 1"


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    if "generated_content" not in schema:
        return {}
    required = {
        "content",
        "created_at",
        "eval_feedback",
        "eval_score",
        "id",
        "source_commits",
        "source_messages",
    }
    missing = sorted(required - schema["generated_content"])
    return {"generated_content": missing} if missing else {}


def _column_expr(
    columns: set[str],
    column: str,
    alias: str,
    output: str | None = None,
) -> str:
    output_name = output or column
    if column in columns:
        return f"{alias}.{column} AS {output_name}"
    return f"NULL AS {output_name}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    ).fetchall()
    return {
        row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")}
        for row in rows
    }


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _format_count_map(values: dict[str, int]) -> str:
    return " ".join(f"{key}={values.get(key, 0)}" for key in sorted(values))


def _format_ids(values: list[int]) -> str:
    return ",".join(str(value) for value in values) if values else "-"
