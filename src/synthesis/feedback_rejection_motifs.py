"""Read-only motif report for rejected or revised generated content."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
import string
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_COUNT = 2
FEEDBACK_TYPES = ("reject", "revise")
MAX_CONTENT_IDS = 8
MAX_SAMPLES = 3
MAX_CANDIDATES_PER_ROW = 40

_WHITESPACE_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9'-]*")
_SECRET_RE = re.compile(r"\b(?:sk|ghp|xoxb|pat)_[A-Za-z0-9_-]{12,}\b")
_URL_RE = re.compile(r"https?://\S+")
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
_PATH_RE = re.compile(r"/Users/\S+")

_STOPWORDS = {
    "a",
    "about",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "can",
    "for",
    "from",
    "has",
    "have",
    "how",
    "in",
    "into",
    "is",
    "it",
    "its",
    "of",
    "on",
    "or",
    "our",
    "should",
    "so",
    "that",
    "the",
    "their",
    "this",
    "to",
    "was",
    "we",
    "with",
    "you",
    "your",
}


def build_feedback_rejection_motifs_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_count: int = DEFAULT_MIN_COUNT,
    content_type: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Rank recurring phrases found in reject/revise feedback and content."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_count <= 0:
        raise ValueError("min_count must be positive")
    if content_type is not None and not content_type.strip():
        raise ValueError("content_type must not be blank")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    now = _aware(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days)
    rows = _feedback_rows(conn, schema, cutoff=cutoff, content_type=content_type, now=now)
    motifs = _rank_motifs(rows, min_count)

    return {
        "artifact_type": "feedback_rejection_motifs",
        "generated_at": now.isoformat(),
        "filters": {
            "days": days,
            "min_count": min_count,
            "content_type": content_type or "all",
            "cutoff": cutoff.isoformat(),
            "feedback_types": list(FEEDBACK_TYPES),
        },
        "summary": {
            "feedback_count": len(rows),
            "content_count": len({row["content_id"] for row in rows}),
            "motif_count": len(motifs),
        },
        "motifs": motifs,
        "empty_state": {
            "is_empty": not rows,
            "schema_present": _required_schema_present(schema),
            "message": (
                "No rejected or revised content feedback found for the selected filters."
                if not rows
                else None
            ),
        },
        "missing_required_tables": [
            table
            for table in ("content_feedback", "generated_content")
            if table not in schema
        ],
        "missing_required_columns": _missing_required_columns(schema),
    }


def format_feedback_rejection_motifs_json(report: dict[str, Any]) -> str:
    """Render the motif report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_feedback_rejection_motifs_text(report: dict[str, Any]) -> str:
    """Render a compact human-readable motif report."""
    filters = report["filters"]
    summary = report["summary"]
    lines = [
        "Feedback rejection motif report",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days={filters['days']} min_count={filters['min_count']} "
            f"content_type={filters['content_type']}"
        ),
        (
            "Totals: "
            f"feedback={summary['feedback_count']} "
            f"content={summary['content_count']} "
            f"motifs={summary['motif_count']}"
        ),
        "",
    ]
    if report["empty_state"]["is_empty"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    if not report["motifs"]:
        lines.append("No repeated rejection motifs met the selected minimum count.")
        return "\n".join(lines)

    lines.append("Motifs")
    for motif in report["motifs"]:
        ids = ", ".join(str(item) for item in motif["content_ids"])
        lines.append(
            "- "
            f"{motif['motif']} count={motif['count']} "
            f"feedback={motif['feedback_type_counts']} "
            f"content_ids={ids}"
        )
        for sample in motif["sample_feedback"]:
            lines.append(f"  sample: {_clip(sample, 120)}")
        if motif["suggested_stale_pattern_candidates"]:
            lines.append(
                "  stale_pattern_candidate: "
                + motif["suggested_stale_pattern_candidates"][0]
            )
    return "\n".join(lines)


def _feedback_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    content_type: str | None,
    now: datetime,
) -> list[dict[str, Any]]:
    if not _required_schema_present(schema):
        return []

    cf = schema["content_feedback"]
    gc = schema["generated_content"]
    notes_expr = _column_expr(cf, "notes", alias="cf")
    replacement_expr = _column_expr(cf, "replacement_text", alias="cf")
    created_expr = _column_expr(cf, "created_at", alias="cf")
    content_type_expr = _column_expr(gc, "content_type", alias="gc")
    content_expr = _column_expr(gc, "content", alias="gc")
    if content_type and "content_type" not in gc:
        return []

    filters = ["cf.feedback_type IN (?, ?)"]
    params: list[Any] = [*FEEDBACK_TYPES]
    if content_type and "content_type" in gc:
        filters.append("gc.content_type = ?")
        params.append(content_type)
    where = "WHERE " + " AND ".join(filters)

    raw_rows = conn.execute(
        f"""SELECT cf.id AS feedback_id,
                  cf.content_id AS content_id,
                  cf.feedback_type AS feedback_type,
                  {notes_expr} AS notes,
                  {replacement_expr} AS replacement_text,
                  {created_expr} AS created_at,
                  {content_type_expr} AS content_type,
                  {content_expr} AS content
             FROM content_feedback cf
             INNER JOIN generated_content gc ON gc.id = cf.content_id
             {where}
             ORDER BY {created_expr} ASC, cf.id ASC""",
        params,
    ).fetchall()

    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        created_at = _parse_timestamp(row["created_at"]) or now
        if created_at < cutoff or created_at > now:
            continue
        rows.append(
            {
                "feedback_id": int(row["feedback_id"]),
                "content_id": int(row["content_id"]),
                "feedback_type": str(row["feedback_type"]),
                "notes": _clean(row["notes"]),
                "replacement_text": _clean(row["replacement_text"]),
                "content_type": row["content_type"] or "unknown",
                "content": _clean(row["content"]),
                "created_at": created_at.isoformat(),
            }
        )
    return rows


def _rank_motifs(rows: list[dict[str, Any]], min_count: int) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for row in rows:
        candidates = _row_candidates(row)
        for motif, source in candidates:
            group = groups.setdefault(
                motif,
                {
                    "motif": motif,
                    "count": 0,
                    "feedback_type_counts": Counter(),
                    "content_type_counts": Counter(),
                    "content_ids": [],
                    "feedback_ids": [],
                    "sample_feedback": [],
                    "source_fields": Counter(),
                },
            )
            group["count"] += 1
            group["feedback_type_counts"][row["feedback_type"]] += 1
            group["content_type_counts"][row["content_type"]] += 1
            group["source_fields"][source] += 1
            _append_unique(group["content_ids"], row["content_id"], MAX_CONTENT_IDS)
            _append_unique(group["feedback_ids"], row["feedback_id"], MAX_CONTENT_IDS)
            sample = _sample_text(row, source)
            if sample:
                _append_unique(group["sample_feedback"], sample, MAX_SAMPLES)

    motifs = [item for item in groups.values() if item["count"] >= min_count]
    for item in motifs:
        item["feedback_type_counts"] = dict(sorted(item["feedback_type_counts"].items()))
        item["content_type_counts"] = dict(sorted(item["content_type_counts"].items()))
        item["source_fields"] = dict(sorted(item["source_fields"].items()))
        item["suggested_stale_pattern_candidates"] = [_stale_pattern_candidate(item["motif"])]

    return sorted(
        motifs,
        key=lambda item: (
            -item["count"],
            -_feedback_source_count(item),
            -len(item["content_ids"]),
            item["motif"],
        ),
    )


def _feedback_source_count(item: dict[str, Any]) -> int:
    source_fields = item.get("source_fields") or {}
    return int(source_fields.get("notes", 0)) + int(source_fields.get("replacement_text", 0))


def _row_candidates(row: dict[str, Any]) -> list[tuple[str, str]]:
    candidates: dict[str, str] = {}
    for field in ("notes", "replacement_text", "content"):
        text = row.get(field) or ""
        if not text:
            continue
        for phrase in _phrases(text, include_short_sentence=field != "content"):
            candidates.setdefault(phrase, field)
            if len(candidates) >= MAX_CANDIDATES_PER_ROW:
                break
    return sorted(candidates.items())


def _phrases(text: str, *, include_short_sentence: bool) -> list[str]:
    normalized = _normalize_phrase(text)
    tokens = _TOKEN_RE.findall(normalized)
    phrases: list[str] = []
    if include_short_sentence:
        first_sentence = _normalize_phrase(re.split(r"(?<=[.!?])\s+", text, maxsplit=1)[0])
        first_tokens = _TOKEN_RE.findall(first_sentence)
        if 2 <= len(first_tokens) <= 10 and _meaningful(first_tokens):
            phrases.append(" ".join(first_tokens))

    max_n = min(5, len(tokens))
    for size in range(5, 1, -1):
        if size > max_n:
            continue
        for index in range(0, len(tokens) - size + 1):
            gram = tokens[index : index + size]
            if not _meaningful(gram):
                continue
            phrase = " ".join(gram)
            if phrase not in phrases:
                phrases.append(phrase)
    return phrases


def _meaningful(tokens: list[str]) -> bool:
    if not tokens:
        return False
    if all(token in _STOPWORDS for token in tokens):
        return False
    if tokens[0] in _STOPWORDS and tokens[-1] in _STOPWORDS:
        return False
    return any(len(token) > 2 and token not in _STOPWORDS for token in tokens)


def _sample_text(row: dict[str, Any], source: str) -> str:
    if source == "replacement_text":
        value = row.get("replacement_text") or row.get("notes") or row.get("content") or ""
    elif source == "content":
        value = row.get("notes") or row.get("content") or ""
    else:
        value = row.get("notes") or row.get("replacement_text") or row.get("content") or ""
    return _clip(value, 180)


def _stale_pattern_candidate(motif: str) -> str:
    escaped = [re.escape(token) for token in motif.split()]
    return r"\b" + r"\s+".join(escaped) + r"\b"


def _normalize_phrase(text: str) -> str:
    text = _clean(text).lower()
    text = text.translate(str.maketrans({char: " " for char in string.punctuation}))
    return _WHITESPACE_RE.sub(" ", text).strip()


def _clean(value: object | None) -> str:
    text = str(value or "")
    text = _SECRET_RE.sub("[secret]", text)
    text = _URL_RE.sub("[url]", text)
    text = _EMAIL_RE.sub("[email]", text)
    text = _PATH_RE.sub("[local-path]", text)
    return _WHITESPACE_RE.sub(" ", text).strip()


def _clip(value: object | None, width: int) -> str:
    text = _WHITESPACE_RE.sub(" ", str(value or "")).strip()
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)].rstrip() + "..."


def _append_unique(items: list[Any], value: Any, limit: int) -> None:
    if value not in items and len(items) < limit:
        items.append(value)


def _parse_timestamp(value: object | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _column_expr(columns: set[str], column: str, default: str = "NULL", *, alias: str) -> str:
    if column in columns:
        return f"{alias}.{column}"
    return default


def _required_schema_present(schema: dict[str, set[str]]) -> bool:
    return not _missing_required_columns(schema)


def _missing_required_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    required = {
        "content_feedback": {"id", "content_id", "feedback_type"},
        "generated_content": {"id"},
    }
    missing: dict[str, list[str]] = {}
    for table, columns in required.items():
        if table not in schema:
            missing[table] = sorted(columns)
            continue
        absent = sorted(columns - schema[table])
        if absent:
            missing[table] = absent
    return missing


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)
