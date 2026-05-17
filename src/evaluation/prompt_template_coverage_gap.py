"""Report prompt templates with recent candidates but missing outcomes."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 50


def build_prompt_template_coverage_gap_report(
    candidate_rows: list[dict[str, Any]],
    review_rows: list[dict[str, Any]],
    publication_rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Group recent prompt candidates and surface missing review/publish coverage."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    reviews = _outcome_index(review_rows, cutoff=cutoff, now=generated_at)
    publications = _outcome_index(publication_rows, cutoff=cutoff, now=generated_at)
    groups: dict[tuple[str, str | None], dict[str, Any]] = {}
    scanned = 0

    for row in candidate_rows:
        candidate_at = _parse_dt(_first(row, "candidate_at", "created_at"))
        if candidate_at is not None and (candidate_at < cutoff or candidate_at > generated_at):
            continue
        template = _text(_first(row, "prompt_template", "prompt_type", "content_type")) or "unknown"
        version = _version(_first(row, "prompt_version", "version"))
        content_id = _optional_int(_first(row, "content_id", "id"))
        key = (template, version)
        group = groups.setdefault(key, _empty_group(template, version))
        group["candidate_count"] += 1
        scanned += 1
        if content_id is not None:
            group["candidate_content_ids"].append(content_id)
        if candidate_at and (group["last_candidate_at_dt"] is None or candidate_at > group["last_candidate_at_dt"]):
            group["last_candidate_at_dt"] = candidate_at

        review_at = reviews.get(content_id)
        if review_at is not None:
            group["reviewed_count"] += 1
            if group["last_reviewed_at_dt"] is None or review_at > group["last_reviewed_at_dt"]:
                group["last_reviewed_at_dt"] = review_at
        published_at = publications.get(content_id)
        if published_at is not None:
            group["published_count"] += 1
            if group["last_published_at_dt"] is None or published_at > group["last_published_at_dt"]:
                group["last_published_at_dt"] = published_at

    rows = [_finalize_group(group) for group in groups.values()]
    rows = [row for row in rows if row["gap_reason"] != "covered"]
    rows.sort(key=lambda row: (_reason_rank(row["gap_reason"]), row["last_candidate_at"] or "", row["prompt_template"], row["prompt_version"] or ""), reverse=True)
    rows = rows[:limit]
    return {
        "artifact_type": "prompt_template_coverage_gap",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit, "lookback_start": cutoff.isoformat()},
        "summary": {
            "candidate_group_count": len(groups),
            "scanned_candidate_count": scanned,
            "gap_group_count": len(rows),
            "no_review_count": sum(1 for row in rows if row["gap_reason"] == "no_review"),
            "no_publish_count": sum(1 for row in rows if row["gap_reason"] == "no_publish"),
            "stale_outcome_count": sum(1 for row in rows if row["gap_reason"] == "stale_outcome"),
        },
        "rows": rows,
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
    }


def build_prompt_template_coverage_gap_report_from_db(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load SQLite rows and build the prompt-template coverage gap report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    candidates = _load_candidates(conn, schema) if not gaps["missing_tables"] else []
    return build_prompt_template_coverage_gap_report(
        candidates,
        _load_reviews(conn, schema),
        _load_publications(conn, schema),
        days=days,
        limit=limit,
        now=now,
        schema_gaps=gaps,
    )


def format_prompt_template_coverage_gap_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_prompt_template_coverage_gap_text(report: dict[str, Any]) -> str:
    lines = [
        "Prompt Template Coverage Gap",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['days']} days",
        (
            "Totals: "
            f"candidate_groups={report['summary']['candidate_group_count']} "
            f"gaps={report['summary']['gap_group_count']} "
            f"no_review={report['summary']['no_review_count']} "
            f"no_publish={report['summary']['no_publish_count']} "
            f"stale={report['summary']['stale_outcome_count']}"
        ),
    ]
    if not report["rows"]:
        lines.extend(["", "No prompt template coverage gaps found."])
        return "\n".join(lines)
    lines.extend(["", "template              version  candidates  reviewed  published  last_candidate              reason"])
    for row in report["rows"]:
        lines.append(
            f"{row['prompt_template'][:21]:<21} "
            f"{(row['prompt_version'] or '-')[:7]:<7} "
            f"{row['candidate_count']:<10} "
            f"{row['reviewed_count']:<8} "
            f"{row['published_count']:<9} "
            f"{row['last_candidate_at'] or '-':<27} "
            f"{row['gap_reason']}"
        )
    return "\n".join(lines)


def _empty_group(template: str, version: str | None) -> dict[str, Any]:
    return {
        "prompt_template": template,
        "prompt_version": version,
        "candidate_count": 0,
        "reviewed_count": 0,
        "published_count": 0,
        "candidate_content_ids": [],
        "last_candidate_at_dt": None,
        "last_reviewed_at_dt": None,
        "last_published_at_dt": None,
    }


def _finalize_group(group: dict[str, Any]) -> dict[str, Any]:
    last_outcome = _latest(group["last_reviewed_at_dt"], group["last_published_at_dt"])
    reason = "covered"
    if group["reviewed_count"] == 0:
        reason = "no_review"
    elif group["published_count"] == 0:
        reason = "no_publish"
    elif (
        last_outcome is not None
        and group["last_candidate_at_dt"] is not None
        and last_outcome < group["last_candidate_at_dt"]
    ):
        reason = "stale_outcome"
    return {
        "prompt_template": group["prompt_template"],
        "prompt_version": group["prompt_version"],
        "candidate_count": group["candidate_count"],
        "reviewed_count": group["reviewed_count"],
        "published_count": group["published_count"],
        "last_candidate_at": _iso(group["last_candidate_at_dt"]),
        "last_reviewed_at": _iso(group["last_reviewed_at_dt"]),
        "last_published_at": _iso(group["last_published_at_dt"]),
        "gap_reason": reason,
        "candidate_content_ids": sorted(set(group["candidate_content_ids"])),
    }


def _load_candidates(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    predicted_content_ids: set[int] = set()
    gc = schema.get("generated_content", set())
    if "engagement_predictions" in schema and {"content_id", "prompt_type"}.issubset(schema["engagement_predictions"]):
        ep = schema["engagement_predictions"]
        selected = [
            "ep.content_id",
            "ep.prompt_type AS prompt_template",
            "ep.prompt_version",
            "ep.created_at AS candidate_at" if "created_at" in ep else "gc.created_at AS candidate_at",
            "gc.content_type",
        ]
        rows.extend(
            dict(row)
            for row in conn.execute(
                f"""SELECT {', '.join(selected)}
                    FROM engagement_predictions ep
                    LEFT JOIN generated_content gc ON gc.id = ep.content_id
                    WHERE ep.prompt_type IS NOT NULL AND TRIM(ep.prompt_type) != ''
                    ORDER BY candidate_at ASC, ep.content_id ASC"""
            ).fetchall()
        )
        predicted_content_ids = {
            content_id
            for content_id in (_optional_int(row.get("content_id")) for row in rows)
            if content_id is not None
        }
    if {"id", "content_type"}.issubset(gc):
        created = "created_at" if "created_at" in gc else "NULL AS created_at"
        fallback_rows = [
            dict(row)
            for row in conn.execute(
                f"""SELECT id AS content_id, content_type AS prompt_template,
                           NULL AS prompt_version, {created} AS candidate_at, content_type
                    FROM generated_content
                    WHERE content_type IS NOT NULL AND TRIM(content_type) != ''
                    ORDER BY candidate_at ASC, id ASC"""
            ).fetchall()
        ]
        rows.extend(
            row
            for row in fallback_rows
            if (_optional_int(row.get("content_id")) not in predicted_content_ids)
        )
    return _dedupe_candidates(rows)


def _load_reviews(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    gc = schema.get("generated_content", set())
    if {"id", "curation_quality"}.issubset(gc):
        reviewed_at = "created_at" if "created_at" in gc else "NULL AS reviewed_at"
        rows.extend(
            dict(row)
            for row in conn.execute(
                f"""SELECT id AS content_id, {reviewed_at} AS outcome_at
                    FROM generated_content
                    WHERE curation_quality IS NOT NULL AND TRIM(curation_quality) != ''"""
            ).fetchall()
        )
    if "content_feedback" in schema and {"content_id", "created_at"}.issubset(schema["content_feedback"]):
        rows.extend(dict(row) for row in conn.execute("SELECT content_id, created_at AS outcome_at FROM content_feedback").fetchall())
    return rows


def _load_publications(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    gc = schema.get("generated_content", set())
    if {"id", "published"}.issubset(gc):
        published_at = "published_at" if "published_at" in gc else "created_at"
        rows.extend(
            dict(row)
            for row in conn.execute(
                f"""SELECT id AS content_id, {published_at} AS outcome_at
                    FROM generated_content
                    WHERE COALESCE(published, 0) = 1"""
            ).fetchall()
        )
    if "content_publications" in schema and {"content_id", "status"}.issubset(schema["content_publications"]):
        cp = schema["content_publications"]
        published_at = "published_at" if "published_at" in cp else "updated_at"
        rows.extend(
            dict(row)
            for row in conn.execute(
                f"""SELECT content_id, {published_at} AS outcome_at
                    FROM content_publications
                    WHERE LOWER(COALESCE(status, '')) = 'published'"""
            ).fetchall()
        )
    return rows


def _outcome_index(rows: list[dict[str, Any]], *, cutoff: datetime, now: datetime) -> dict[int, datetime]:
    indexed: dict[int, datetime] = {}
    for row in rows:
        content_id = _optional_int(_first(row, "content_id", "id"))
        outcome_at = _parse_dt(_first(row, "outcome_at", "created_at", "updated_at"))
        if content_id is None or outcome_at is None or outcome_at < cutoff or outcome_at > now:
            continue
        current = indexed.get(content_id)
        if current is None or outcome_at > current:
            indexed[content_id] = outcome_at
    return indexed


def _dedupe_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[tuple[int | None, str, str | None], dict[str, Any]] = {}
    for row in rows:
        template = _text(_first(row, "prompt_template", "prompt_type", "content_type")) or "unknown"
        version = _version(_first(row, "prompt_version", "version"))
        content_id = _optional_int(_first(row, "content_id", "id"))
        key = (content_id, template, version)
        current = selected.get(key)
        if current is None or (_parse_dt(row.get("candidate_at")) or datetime.min.replace(tzinfo=timezone.utc)) > (
            _parse_dt(current.get("candidate_at")) or datetime.min.replace(tzinfo=timezone.utc)
        ):
            selected[key] = row
    return list(selected.values())


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "generated_content" not in schema:
        return {"missing_tables": ["generated_content"], "missing_columns": {}}
    missing = sorted({"id", "content_type"} - schema["generated_content"])
    return {"missing_tables": [], "missing_columns": {"generated_content": missing} if missing else {}}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _reason_rank(reason: str) -> int:
    return {"no_review": 3, "no_publish": 2, "stale_outcome": 1}.get(reason, 0)


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _version(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _latest(*values: datetime | None) -> datetime | None:
    present = [value for value in values if value is not None]
    return max(present) if present else None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None
