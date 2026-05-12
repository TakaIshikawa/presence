"""Prompt-maintenance candidates from durable generated-content feedback."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 5
DEFAULT_MIN_COUNT = 2
PROMPT_SURFACES = {
    "blog_post": "blog_post_v2",
    "x_long_post": "x_long_post_v2",
    "x_post": "x_post_v2",
    "x_thread": "x_thread_v2",
}


def build_content_feedback_prompt_candidates_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    min_count: int = DEFAULT_MIN_COUNT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Group recent reject/revise/prefer feedback into prompt update candidates."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if min_count <= 0:
        raise ValueError("min_count must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_rows(conn, schema, cutoff, generated_at)
    candidates = _candidates(rows, limit, min_count)
    return {
        "artifact_type": "content_feedback_prompt_candidates",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit, "min_count": min_count},
        "totals": {
            "rows_scanned": len(rows),
            "candidate_count": len(candidates),
            "motifs_below_min_count": len(_motif_counts(rows, below=min_count)),
        },
        "candidates": candidates,
        "missing_tables": [
            table
            for table in ("content_feedback",)
            if table not in schema
        ],
    }


def format_content_feedback_prompt_candidates_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_feedback_prompt_candidates_text(report: dict[str, Any]) -> str:
    lines = [
        "Content Feedback Prompt Candidates",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days={report['filters']['days']} "
            f"limit={report['filters']['limit']} min_count={report['filters']['min_count']}"
        ),
        (
            f"Totals: rows={report['totals']['rows_scanned']} "
            f"candidates={report['totals']['candidate_count']}"
        ),
    ]
    if not report["candidates"]:
        lines.append("")
        lines.append("No prompt-maintenance candidates found.")
        return "\n".join(lines)
    lines.extend(["", "Candidates:"])
    for item in report["candidates"]:
        lines.append(
            f"- motif={item['motif']} count={item['count']} "
            f"types={_fmt_counts(item['feedback_type_counts'])} "
            f"content_types={','.join(item['affected_content_types'])} "
            f"surfaces={','.join(item['prompt_surface_suggestions'])} "
            f"ids={','.join(str(v) for v in item['representative_content_ids'])}"
        )
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    if "content_feedback" not in schema:
        return []
    cf_cols = schema["content_feedback"]
    gc_cols = schema.get("generated_content", set())
    join = "LEFT JOIN generated_content gc ON gc.id = cf.content_id" if gc_cols else ""
    content_type_expr = "gc.content_type" if "content_type" in gc_cols else "NULL"
    metadata_expr = "gc.metadata" if "metadata" in gc_cols else "NULL"
    created_expr = "cf.created_at" if "created_at" in cf_cols else "NULL"
    notes_expr = "cf.notes" if "notes" in cf_cols else "NULL"
    replacement_expr = "cf.replacement_text" if "replacement_text" in cf_cols else "NULL"
    rows = conn.execute(
        f"""SELECT cf.id, cf.content_id, cf.feedback_type,
                  {notes_expr} AS notes,
                  {replacement_expr} AS replacement_text,
                  {created_expr} AS created_at,
                  {content_type_expr} AS content_type,
                  {metadata_expr} AS metadata
           FROM content_feedback cf
           {join}
           ORDER BY created_at ASC, cf.id ASC"""
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        created = _parse_dt(row["created_at"]) or now
        if created < cutoff or created > now:
            continue
        note = _first_text(row["notes"], row["replacement_text"])
        out.append(
            {
                "content_id": int(row["content_id"]),
                "feedback_type": str(row["feedback_type"]),
                "motif": _normalize_motif(note),
                "content_type": row["content_type"] or "unknown",
                "metadata": _json_obj(row["metadata"]),
            }
        )
    return out


def _candidates(rows: list[dict[str, Any]], limit: int, min_count: int) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row["motif"], []).append(row)
    candidates = []
    for motif, items in grouped.items():
        if len(items) < min_count:
            continue
        content_types = sorted({item["content_type"] for item in items})
        surfaces = sorted({_surface(item["content_type"], item["metadata"]) for item in items})
        counts = Counter(item["feedback_type"] for item in items)
        ids = []
        for item in sorted(items, key=lambda r: r["content_id"]):
            if item["content_id"] not in ids:
                ids.append(item["content_id"])
        candidates.append(
            {
                "motif": motif,
                "count": len(items),
                "feedback_type_counts": dict(sorted(counts.items())),
                "affected_content_types": content_types,
                "prompt_surface_suggestions": surfaces,
                "representative_content_ids": ids[:limit],
            }
        )
    return sorted(candidates, key=lambda item: (-item["count"], item["motif"]))


def _motif_counts(rows: list[dict[str, Any]], *, below: int) -> dict[str, int]:
    counts = Counter(row["motif"] for row in rows)
    return {motif: count for motif, count in counts.items() if count < below}


def _surface(content_type: str, metadata: dict[str, Any]) -> str:
    hinted = metadata.get("prompt_surface") or metadata.get("prompt_version")
    if isinstance(hinted, str) and hinted.strip():
        return hinted.strip()
    if content_type == "x_post" and metadata.get("long_form"):
        return "x_long_post_v2"
    return PROMPT_SURFACES.get(content_type, f"{content_type}_v2")


def _normalize_motif(text: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()
    words = [w for w in normalized.split() if w not in {"the", "a", "an", "to", "is"}]
    return " ".join(words[:8]) or "unspecified"


def _first_text(*values: Any) -> str:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value
    return "unspecified"


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _fmt_counts(counts: dict[str, int]) -> str:
    return ",".join(f"{key}={value}" for key, value in sorted(counts.items()))
