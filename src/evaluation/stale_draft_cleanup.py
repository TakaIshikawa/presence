"""Plan cleanup for stale generated drafts that were never published."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 50
DEFAULT_MIN_EVAL_SCORE = 5.0
REASONS = (
    "failed_gate",
    "superseded_draft",
    "review_timeout",
    "stale_unpublished",
)
PUBLISHED_STATUSES = {"published", "posted", "success", "succeeded"}
IGNORED_STATUSES = {"abandoned", "cancelled", "canceled", "dismissed", "archived"}
REVIEW_STATUSES = {"held", "needs_review", "pending_review", "review", "queued"}
PASSING_GUARD_STATUSES = {"pass", "passed"}


def build_stale_draft_cleanup_plan(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    min_eval_score: float = DEFAULT_MIN_EVAL_SCORE,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a dry-run cleanup plan for stale unpublished generated content."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if min_eval_score < 0:
        raise ValueError("min_eval_score must be non-negative")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "cutoff": cutoff.isoformat(),
        "dry_run": True,
        "limit": limit,
        "min_eval_score": min_eval_score,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_plan(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    queue_states = _state_rows(conn, schema, "publish_queue")
    publication_states = _state_rows(conn, schema, "content_publications")
    guard_rows = _guard_rows(conn, schema)
    claim_rows = _claim_rows(conn, schema)
    all_rows = _load_generated_content(conn, schema)
    candidates = [
        _hydrate_row(
            row,
            queue_states=queue_states,
            publication_states=publication_states,
            guard_rows=guard_rows,
            claim_rows=claim_rows,
        )
        for row in all_rows
    ]
    superseded_by = _superseded_map(candidates)

    plan_rows = [
        planned
        for planned in (
            _classify_row(
                row,
                now=generated_at,
                cutoff=cutoff,
                min_eval_score=min_eval_score,
                superseded_by=superseded_by.get(row["content_id"]),
            )
            for row in candidates
        )
        if planned is not None
    ]
    plan_rows.sort(key=lambda row: (-row["age_days"], row["draft_id"]))
    total_before_limit = len(plan_rows)
    plan_rows = plan_rows[:limit]

    by_reason = Counter(row["reason"] for row in plan_rows)
    by_disposition = Counter(row["suggested_disposition"] for row in plan_rows)
    return {
        "artifact_type": "stale_draft_cleanup_plan",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "counts": {
            "rows_scanned": len(all_rows),
            "stale_drafts": len(plan_rows),
            "stale_drafts_before_limit": total_before_limit,
            "by_reason": {reason: by_reason.get(reason, 0) for reason in REASONS},
            "by_disposition": dict(sorted(by_disposition.items())),
        },
        "missing_tables": [],
        "missing_columns": {},
        "drafts": plan_rows,
    }


def format_stale_draft_cleanup_json(report: dict[str, Any]) -> str:
    """Serialize the cleanup plan as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_stale_draft_cleanup_text(report: dict[str, Any]) -> str:
    """Render the cleanup plan for command-line review."""
    filters = report["filters"]
    counts = report["counts"]
    by_reason = counts["by_reason"]
    lines = [
        "Stale Draft Cleanup Plan",
        f"Generated: {report['generated_at']}",
        (
            f"Mode: dry-run={int(filters['dry_run'])} days={filters['days']} "
            f"cutoff={filters['cutoff']} min_eval_score={filters['min_eval_score']} "
            f"limit={filters['limit']}"
        ),
        (
            f"Drafts: scanned={counts['rows_scanned']} stale={counts['stale_drafts']} "
            f"before_limit={counts['stale_drafts_before_limit']} "
            f"failed_gate={by_reason.get('failed_gate', 0)} "
            f"superseded={by_reason.get('superseded_draft', 0)} "
            f"review_timeout={by_reason.get('review_timeout', 0)} "
            f"stale_unpublished={by_reason.get('stale_unpublished', 0)}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report["missing_columns"].items())
        )
        lines.append("Missing columns: " + missing)

    if not report["drafts"]:
        lines.append("")
        lines.append("No stale unpublished drafts found.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Drafts:")
    for row in report["drafts"]:
        lines.append(
            f"- draft_id={row['draft_id']} age_days={row['age_days']} "
            f"reason={row['reason']} disposition={row['suggested_disposition']} "
            f"created_at={row['created_at']}"
        )
        if row["evidence"]:
            lines.append("  evidence=" + "; ".join(row["evidence"]))
    return "\n".join(lines)


def _load_generated_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    select_columns = [
        "id",
        _column_expr(columns, "content_type", "'unknown'"),
        _column_expr(columns, "source_commits"),
        _column_expr(columns, "source_messages"),
        _column_expr(columns, "source_activity_ids"),
        _column_expr(columns, "content", "''"),
        _column_expr(columns, "eval_score"),
        _column_expr(columns, "eval_feedback"),
        _column_expr(columns, "published", "0"),
        _column_expr(columns, "published_url"),
        _column_expr(columns, "published_at"),
        _column_expr(columns, "curation_quality"),
        _column_expr(columns, "auto_quality"),
        _column_expr(columns, "created_at"),
    ]
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {", ".join(select_columns)}
                FROM generated_content
                ORDER BY created_at ASC, id ASC"""
        ).fetchall()
    ]


def _hydrate_row(
    row: dict[str, Any],
    *,
    queue_states: dict[int, list[dict[str, Any]]],
    publication_states: dict[int, list[dict[str, Any]]],
    guard_rows: dict[int, dict[str, Any]],
    claim_rows: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    content_id = int(row["id"])
    hydrated = dict(row)
    hydrated["content_id"] = content_id
    hydrated["queue_states"] = queue_states.get(content_id, [])
    hydrated["publication_states"] = publication_states.get(content_id, [])
    hydrated["guard"] = guard_rows.get(content_id)
    hydrated["claim_check"] = claim_rows.get(content_id)
    hydrated["source_keys"] = _source_keys(row)
    return hydrated


def _classify_row(
    row: dict[str, Any],
    *,
    now: datetime,
    cutoff: datetime,
    min_eval_score: float,
    superseded_by: dict[str, Any] | None,
) -> dict[str, Any] | None:
    created = _parse_datetime(row.get("created_at"))
    if created is None or created > cutoff:
        return None
    publication_status = _publication_status(row)
    if _status_contains(publication_status, PUBLISHED_STATUSES | IGNORED_STATUSES):
        return None

    age_days = max(0, (now - created).days)
    evidence: list[str] = []
    reason = "stale_unpublished"
    disposition = "archive_or_regenerate"

    failed_gate = _failed_gate(row, min_eval_score=min_eval_score)
    if failed_gate:
        reason = "failed_gate"
        disposition = "regenerate"
        evidence.extend(failed_gate)
    elif superseded_by is not None:
        reason = "superseded_draft"
        disposition = "archive"
        evidence.append(
            "newer_draft_id="
            f"{superseded_by['content_id']} created_at={superseded_by['created_at']}"
        )
    elif _review_timeout(row):
        reason = "review_timeout"
        disposition = "archive_or_refresh"
        evidence.extend(_review_evidence(row))
    else:
        evidence.append(f"unpublished_for_days>={age_days}")

    return {
        "draft_id": row["content_id"],
        "content_type": _clean(row.get("content_type")),
        "created_at": created.isoformat(),
        "age_days": age_days,
        "reason": reason,
        "suggested_disposition": disposition,
        "publication_status": publication_status,
        "queue_status": _combined_status(row["queue_states"]),
        "superseded_by_draft_id": (
            superseded_by["content_id"] if superseded_by is not None else None
        ),
        "evidence": evidence,
    }


def _failed_gate(row: dict[str, Any], *, min_eval_score: float) -> list[str]:
    evidence: list[str] = []
    guard = row.get("guard")
    if guard and _int_value(guard.get("checked")):
        passed = bool(_int_value(guard.get("passed")))
        status = _status(guard.get("status"))
        if not passed or status not in PASSING_GUARD_STATUSES:
            evidence.append(f"persona_guard_status={status or 'unknown'}")
    claim = row.get("claim_check")
    unsupported = _int_value(claim.get("unsupported_count")) if claim else None
    if unsupported is not None and unsupported > 0:
        evidence.append(f"unsupported_claims={unsupported}")
    eval_score = _float_value(row.get("eval_score"))
    if eval_score is not None and eval_score < min_eval_score:
        evidence.append(f"eval_score={eval_score:g}<min_eval_score={min_eval_score:g}")
    auto_quality = _status(row.get("auto_quality"))
    if auto_quality in {"failed", "fail", "low_quality", "low_resonance"}:
        evidence.append(f"auto_quality={auto_quality}")
    return evidence


def _review_timeout(row: dict[str, Any]) -> bool:
    curation = _status(row.get("curation_quality"))
    statuses = {
        _status(state.get("status"))
        for state in row["queue_states"] + row["publication_states"]
    }
    statuses.discard(None)
    return curation in REVIEW_STATUSES or bool(statuses & REVIEW_STATUSES)


def _review_evidence(row: dict[str, Any]) -> list[str]:
    evidence: list[str] = []
    curation = _status(row.get("curation_quality"))
    if curation:
        evidence.append(f"curation_quality={curation}")
    queue_status = _combined_status(row["queue_states"])
    if queue_status != "none":
        evidence.append(f"queue_status={queue_status}")
    publication_status = _combined_status(row["publication_states"])
    if publication_status != "none":
        evidence.append(f"publication_status={publication_status}")
    return evidence or ["review_state=unknown"]


def _superseded_map(rows: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    by_source: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        for key in row["source_keys"]:
            by_source.setdefault(key, []).append(row)

    superseded: dict[int, dict[str, Any]] = {}
    for row in rows:
        created = _parse_datetime(row.get("created_at"))
        if created is None or not row["source_keys"]:
            continue
        newer: list[dict[str, Any]] = []
        for key in row["source_keys"]:
            newer.extend(
                candidate
                for candidate in by_source.get(key, [])
                if candidate["content_id"] != row["content_id"]
                and candidate.get("content_type") == row.get("content_type")
                and (candidate_created := _parse_datetime(candidate.get("created_at"))) is not None
                and candidate_created > created
                and not _status_contains(_publication_status(candidate), IGNORED_STATUSES)
            )
        if newer:
            newer.sort(
                key=lambda item: (
                    _parse_datetime(item.get("created_at"))
                    or datetime.min.replace(tzinfo=timezone.utc),
                    item["content_id"],
                )
            )
            superseded[row["content_id"]] = newer[-1]
    return superseded


def _state_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    table: str,
) -> dict[int, list[dict[str, Any]]]:
    columns = schema.get(table)
    if not columns or not {"content_id", "status"}.issubset(columns):
        return {}
    select_columns = ["content_id", "status"]
    for optional in ("hold_reason", "error", "error_category", "updated_at", "created_at"):
        if optional in columns:
            select_columns.append(optional)
    states: dict[int, list[dict[str, Any]]] = {}
    for row in conn.execute(
        f"""SELECT {", ".join(select_columns)}
            FROM {_quote_identifier(table)}
            WHERE content_id IS NOT NULL
            ORDER BY content_id ASC, id ASC"""
    ).fetchall():
        content_id = _int_value(row["content_id"])
        if content_id is not None:
            states.setdefault(content_id, []).append(dict(row))
    return states


def _guard_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, dict[str, Any]]:
    columns = schema.get("content_persona_guard")
    if not columns or "content_id" not in columns:
        return {}
    select_columns = [
        "content_id",
        _column_expr(columns, "checked", "0"),
        _column_expr(columns, "passed", "1"),
        _column_expr(columns, "status", "'unknown'"),
        _column_expr(columns, "score", "0"),
    ]
    return {
        int(row["content_id"]): dict(row)
        for row in conn.execute(
            f"""SELECT {", ".join(select_columns)}
                FROM content_persona_guard
                WHERE content_id IS NOT NULL"""
        ).fetchall()
    }


def _claim_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, dict[str, Any]]:
    columns = schema.get("content_claim_checks")
    if not columns or "content_id" not in columns:
        return {}
    select_columns = [
        "content_id",
        _column_expr(columns, "unsupported_count", "0"),
    ]
    return {
        int(row["content_id"]): dict(row)
        for row in conn.execute(
            f"""SELECT {", ".join(select_columns)}
                FROM content_claim_checks
                WHERE content_id IS NOT NULL"""
        ).fetchall()
    }


def _publication_status(row: dict[str, Any]) -> str:
    publication_status = _combined_status(row.get("publication_states", []))
    if _status_contains(publication_status, PUBLISHED_STATUSES) or _legacy_published(row):
        return "published"
    if publication_status != "none":
        return publication_status
    published = row.get("published")
    if isinstance(published, str):
        lowered = published.strip().lower()
        if lowered in {"1", "true", "yes", "published"}:
            return "published"
        if lowered in {"-1", "abandoned", "cancelled", "canceled", "dismissed"}:
            return "abandoned"
    else:
        published_int = _int_value(published)
        if published_int == -1:
            return "abandoned"
        if published_int:
            return "published"
    return "unpublished"


def _legacy_published(row: dict[str, Any]) -> bool:
    return bool(_clean(row.get("published_url")) or _clean(row.get("published_at")))


def _combined_status(states: list[dict[str, Any]]) -> str:
    statuses = sorted(
        dict.fromkeys(
            _status(state.get("status"))
            for state in states
            if _status(state.get("status"))
        )
    )
    if not statuses:
        return "none"
    if any(status in PUBLISHED_STATUSES for status in statuses):
        return "published"
    if len(statuses) == 1:
        return statuses[0]
    return "mixed:" + ",".join(statuses)


def _status_contains(status: str, values: set[str]) -> bool:
    return any(part in values for part in status.removeprefix("mixed:").split(","))


def _source_keys(row: dict[str, Any]) -> frozenset[str]:
    keys: list[str] = []
    for column, prefix in (
        ("source_commits", "commit"),
        ("source_messages", "message"),
        ("source_activity_ids", "activity"),
    ):
        for value in _parse_json_list(row.get(column)):
            cleaned = _clean(value)
            if cleaned:
                keys.append(f"{prefix}:{cleaned}")
    return frozenset(keys)


def _parse_json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]]]:
    required = {"generated_content": {"id", "created_at", "published"}}
    missing_tables = [table for table in sorted(required) if table not in schema]
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _empty_plan(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    return {
        "artifact_type": "stale_draft_cleanup_plan",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "counts": {
            "rows_scanned": 0,
            "stale_drafts": 0,
            "stale_drafts_before_limit": 0,
            "by_reason": {reason: 0 for reason in REASONS},
            "by_disposition": {},
        },
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
        "drafts": [],
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3 connection or database wrapper with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        schema[table] = {
            str(info["name"] if isinstance(info, sqlite3.Row) else info[1])
            for info in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")
        }
    return schema


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str | None = None,
) -> str:
    qualified = f"{alias}.{column}" if alias else column
    return qualified if column in columns else f"{fallback} AS {column}"


def _parse_datetime(value: Any) -> datetime | None:
    cleaned = _clean(value)
    if not cleaned:
        return None
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        return _ensure_utc(datetime.fromisoformat(cleaned))
    except ValueError:
        return None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _status(value: Any) -> str | None:
    cleaned = _clean(value)
    if not cleaned:
        return None
    return cleaned.lower().replace("-", "_").replace(" ", "_")


def _int_value(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_value(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
