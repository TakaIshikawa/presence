"""Audit newsletter subject candidate metadata drift."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
DEFAULT_SOURCE = "all"
REQUIRED_COLUMNS = {"id", "metadata"}
OPTIONAL_COLUMNS = (
    "newsletter_send_id",
    "issue_id",
    "subject",
    "source",
    "rank",
    "selected",
    "rationale",
    "source_content_ids",
    "created_at",
)
EVALUATION_KEYS = ("model", "provider", "prompt_version")
SOURCE_CONTENT_ID_KEYS = ("source_content_ids", "content_ids")


def build_newsletter_subject_candidate_metadata_drift_report_from_db(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    source: str | Iterable[str] = DEFAULT_SOURCE,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a deterministic metadata drift report from SQLite."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    source_filter = _normalize_filter(source)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    columns = schema.get("newsletter_subject_candidates")
    generated_at = _utc(now) if now is not None else (
        _latest_timestamp(conn, "newsletter_subject_candidates", columns or set(), ("created_at",))
        or datetime.now(timezone.utc)
    )
    if columns is None:
        return _report(generated_at, days, source_filter, limit, [], 0, ["newsletter_subject_candidates"], {})
    missing = sorted(REQUIRED_COLUMNS - columns)
    if missing:
        return _report(generated_at, days, source_filter, limit, [], 0, [], {"newsletter_subject_candidates": missing})

    rows = _load_rows(conn, columns, generated_at - timedelta(days=days))
    matched = [_normalize_row(row) for row in rows if _matches(_normalize_source(row.get("source")), source_filter)]
    findings = [finding for row in matched for finding in _findings(row)]
    findings.sort(key=_finding_sort_key)
    return _report(generated_at, days, source_filter, limit, findings, len(matched), [], {})


def format_newsletter_subject_candidate_metadata_drift_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_subject_candidate_metadata_drift_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    lines = [
        "Newsletter Subject Candidate Metadata Drift",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} source={','.join(report['filters']['source'])} limit={report['filters']['limit']}",
        f"Totals: candidates={summary['candidate_count']} findings={summary['finding_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "candidate_id | source | gap_type | detail"])
    for finding in report["findings"]:
        lines.append(
            f"{finding['candidate_id']} | {finding['source']} | {finding['gap_type']} | {finding.get('detail') or '-'}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str], cutoff: datetime) -> list[dict[str, Any]]:
    selected = ["id AS candidate_id", "metadata"]
    for column in OPTIONAL_COLUMNS:
        selected.append(f"{column}" if column in columns else f"NULL AS {column}")
    where = "WHERE datetime(created_at) >= datetime(?)" if "created_at" in columns else ""
    params = (cutoff.isoformat(),) if where else ()
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM newsletter_subject_candidates
            {where}
            ORDER BY datetime(COALESCE(created_at, '1970-01-01')) ASC, id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "source": _normalize_source(row.get("source")),
        "selected": _truthy(row.get("selected")),
    }


def _findings(row: dict[str, Any]) -> list[dict[str, Any]]:
    raw_metadata = row.get("metadata")
    metadata, error = _metadata_object(raw_metadata)
    if error:
        return [_finding(row, "malformed_metadata", detail=error)]

    findings: list[dict[str, Any]] = []
    if row["source"] != "heuristic":
        missing_keys = [key for key in EVALUATION_KEYS if not _clean(metadata.get(key))]
        if missing_keys:
            findings.append(_finding(row, "missing_evaluation_metadata", detail="missing " + ",".join(missing_keys)))

    row_ids, row_error = _id_list(row.get("source_content_ids"))
    metadata_ids = _metadata_source_content_ids(metadata)
    if metadata_ids is not None and row_error is None and row_ids != metadata_ids:
        findings.append(
            _finding(
                row,
                "source_content_ids_mismatch",
                row_source_content_ids=row_ids,
                metadata_source_content_ids=metadata_ids,
            )
        )
    elif metadata_ids is not None and row_error is not None:
        findings.append(_finding(row, "source_content_ids_mismatch", detail=row_error))

    if row["selected"] and not _clean(row.get("rationale")):
        findings.append(_finding(row, "selected_missing_rationale"))
    if row["selected"] and not _clean(row.get("rank")):
        findings.append(_finding(row, "selected_missing_rank"))
    return findings


def _metadata_object(raw: Any) -> tuple[dict[str, Any], str | None]:
    if raw is None or _clean(raw) == "":
        return {}, None
    if isinstance(raw, dict):
        return raw, None
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError) as exc:
        return {}, f"metadata is not valid JSON: {exc}"
    if not isinstance(parsed, dict):
        return {}, "metadata must be a JSON object"
    return parsed, None


def _metadata_source_content_ids(metadata: dict[str, Any]) -> list[str] | None:
    for key in SOURCE_CONTENT_ID_KEYS:
        if key in metadata:
            return _normalize_ids(metadata.get(key))
    return None


def _id_list(raw: Any) -> tuple[list[str], str | None]:
    if raw is None or _clean(raw) == "":
        return [], None
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError) as exc:
        return [], f"source_content_ids is not valid JSON: {exc}"
    return _normalize_ids(parsed), None


def _normalize_ids(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        values = value
    else:
        values = [value]
    return sorted(_clean(item) for item in values if _clean(item))


def _finding(row: dict[str, Any], gap_type: str, **extra: Any) -> dict[str, Any]:
    return {
        "candidate_id": row.get("candidate_id"),
        "newsletter_send_id": row.get("newsletter_send_id"),
        "issue_id": row.get("issue_id"),
        "subject": row.get("subject"),
        "source": row.get("source"),
        "rank": row.get("rank"),
        "selected": row.get("selected"),
        "created_at": row.get("created_at"),
        "gap_type": gap_type,
        **extra,
    }


def _report(
    generated_at: datetime,
    days: int,
    source_filter: tuple[str, ...],
    limit: int,
    findings: list[dict[str, Any]],
    candidate_count: int,
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    shown = findings[:limit]
    counts = Counter(finding["gap_type"] for finding in findings)
    return {
        "artifact_type": "newsletter_subject_candidate_metadata_drift",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "source": list(source_filter), "limit": limit},
        "summary": {
            "candidate_count": candidate_count,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_gap_type": dict(sorted(counts.items())),
        },
        "groups": [{"gap_type": gap_type, "finding_count": count} for gap_type, count in sorted(counts.items())],
        "findings": shown,
        "missing_tables": sorted(missing_tables),
        "missing_columns": {table: sorted(columns) for table, columns in sorted(missing_columns.items()) if columns},
        "empty_state": {
            "is_empty": not findings,
            "message": "No newsletter subject candidate metadata drift found." if not findings else None,
        },
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _latest_timestamp(conn: sqlite3.Connection, table: str, columns: set[str], names: tuple[str, ...]) -> datetime | None:
    for name in names:
        if name not in columns:
            continue
        row = conn.execute(f"SELECT MAX(datetime({name})) FROM {table}").fetchone()
        if row and row[0]:
            return _utc(datetime.fromisoformat(str(row[0]).replace(" ", "T")))
    return None


def _normalize_filter(value: str | Iterable[str]) -> tuple[str, ...]:
    parts = value.split(",") if isinstance(value, str) else [str(item) for item in value]
    normalized = tuple(sorted({_normalize_source(part) for part in parts if _clean(part)}))
    return normalized or ("all",)


def _matches(value: str, allowed: tuple[str, ...]) -> bool:
    return allowed == ("all",) or value in allowed


def _normalize_source(value: Any) -> str:
    return (_clean(value) or "heuristic").lower()


def _truthy(value: Any) -> bool:
    return _clean(value).lower() in {"1", "true", "yes", "selected"}


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _finding_sort_key(finding: dict[str, Any]) -> tuple[int, tuple[int, Any], str]:
    return (_gap_rank(finding["gap_type"]), _int_or_text(finding.get("candidate_id")), _clean(finding.get("subject")))


def _gap_rank(gap_type: str) -> int:
    return {
        "malformed_metadata": 0,
        "missing_evaluation_metadata": 1,
        "source_content_ids_mismatch": 2,
        "selected_missing_rationale": 3,
        "selected_missing_rank": 4,
    }.get(gap_type, 99)


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_text(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, "" if value is None else str(value))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
