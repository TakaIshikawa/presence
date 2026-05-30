"""Detect content claim checks where reviewers disagree or verdicts reverse."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


ARTIFACT_TYPE = "content_claim_check_reviewer_disagreement"
DEFAULT_LIMIT = 100
DEFAULT_WINDOW_DAYS = 30
DEFAULT_MIN_REVIEWERS = 2
MIXED_CONFIDENCE_DELTA = 0.5

APPROVED_VERDICTS = {"approved", "approve", "supported", "support", "true", "pass", "passed"}
REJECTED_VERDICTS = {"rejected", "reject", "unsupported", "false", "fail", "failed"}


def build_content_claim_check_reviewer_disagreement_report(
    rows: list[dict[str, Any]],
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    min_reviewers: int = DEFAULT_MIN_REVIEWERS,
    claim_id: int | str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | str | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a report of claim checks with conflicting reviewer outcomes."""
    _require_positive("window_days", window_days)
    _require_positive("min_reviewers", min_reviewers)
    _require_positive("limit", limit)

    generated_at = _report_now(now, rows)
    cutoff = generated_at - timedelta(days=window_days)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    scanned_count = 0

    for raw in rows:
        row = _normalize_row(raw)
        if row["claim_id"] is None:
            continue
        if claim_id is not None and str(row["claim_id"]) != str(claim_id):
            continue
        checked_at = row["checked_at_dt"]
        if checked_at is not None and checked_at < cutoff:
            continue
        scanned_count += 1
        grouped[str(row["claim_id"])].append(row)

    findings: list[dict[str, Any]] = []
    for claim_key, checks in grouped.items():
        if len(checks) < 2:
            continue
        reviewers = _reviewer_set(checks)
        if len(reviewers) < min_reviewers:
            continue

        verdicts = sorted({check["verdict"] for check in checks if check["verdict"]})
        base = _base_finding(claim_key, checks, reviewers, verdicts)

        if len(verdicts) > 1:
            findings.append(
                {
                    **base,
                    "reason": "conflicting_verdicts",
                    "detail": "multiple review records have different verdicts",
                }
            )

        reversal = _find_verdict_reversal(checks)
        if reversal is not None:
            previous, current = reversal
            findings.append(
                {
                    **base,
                    "reason": "verdict_reversal",
                    "previous_check_id": previous["check_id"],
                    "previous_verdict": previous["verdict"],
                    "check_id": current["check_id"],
                    "verdict": current["verdict"],
                    "detail": "later verdict reverses earlier approved/rejected state",
                }
            )

        confidences = [check["confidence"] for check in checks if check["confidence"] is not None]
        if confidences and max(confidences) - min(confidences) >= MIXED_CONFIDENCE_DELTA:
            findings.append(
                {
                    **base,
                    "reason": "mixed_confidence_reviews",
                    "confidence_min": min(confidences),
                    "confidence_max": max(confidences),
                    "detail": "review confidence values differ materially",
                }
            )

    findings.sort(key=_finding_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {
            "window_days": window_days,
            "min_reviewers": min_reviewers,
            "claim_id": claim_id,
            "limit": limit,
        },
        "totals": {
            "row_count": scanned_count,
            "claim_count": len(grouped),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_reason": dict(Counter(finding["reason"] for finding in findings)),
        },
        "findings": _group_findings(shown),
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            table: sorted(columns)
            for table, columns in sorted((missing_columns or {}).items())
            if columns
        },
        "empty_state": {
            "is_empty": not findings,
            "message": "No content claim check reviewer disagreement findings found."
            if not findings
            else None,
        },
    }


def build_content_claim_check_reviewer_disagreement_report_from_db(
    db_or_conn: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    """Load content claim check records from SQLite and build the report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "content_claim_checks" not in schema:
        return build_content_claim_check_reviewer_disagreement_report(
            [],
            missing_tables=["content_claim_checks"],
            **kwargs,
        )

    rows = _load_claim_check_rows(conn, schema)
    _attach_claim_context(conn, schema, rows)
    _attach_content_context(conn, schema, rows)
    return build_content_claim_check_reviewer_disagreement_report(
        rows,
        missing_columns=_optional_schema_gaps(schema),
        **kwargs,
    )


def format_content_claim_check_reviewer_disagreement_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_claim_check_reviewer_disagreement_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    totals = report["totals"]
    filters = report["filters"]
    lines = [
        "Content Claim Check Reviewer Disagreement",
        f"Generated: {report['generated_at']}",
        "Filters: "
        f"window_days={filters['window_days']}, "
        f"min_reviewers={filters['min_reviewers']}, "
        f"claim_id={filters['claim_id']}, "
        f"limit={filters['limit']}",
        f"Totals: rows={totals['row_count']} claims={totals['claim_count']} "
        f"findings={totals['finding_count']} shown={totals['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append(
            "Missing columns: "
            + ", ".join(
                f"{table}={','.join(columns)}"
                for table, columns in report["missing_columns"].items()
            )
        )
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    for group in report["findings"]:
        lines.extend(["", f"{group['reason']} ({group['count']})"])
        for item in group["items"]:
            label = item.get("claim_text") or item.get("content_label") or "-"
            lines.append(
                f"- claim={item['claim_id']} reviewers={item['reviewer_count']} "
                f"verdicts={','.join(item['verdicts']) or '-'} label={label}: {item['detail']}"
            )
    return "\n".join(lines)


def _load_claim_check_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[dict[str, Any]]:
    columns = schema["content_claim_checks"]
    select = [
        _select_expr(columns, "id", alias="check_id", fallback="rowid"),
        _select_expr(
            columns,
            "claim_id",
            "content_claim_id",
            "content_id",
            "id",
            alias="claim_id",
            fallback="rowid",
        ),
        _select_expr(columns, "content_id", "generated_content_id", alias="content_id"),
        _select_expr(
            columns,
            "reviewer_id",
            "reviewer",
            "checked_by",
            "actor",
            "created_by",
            alias="reviewer_id",
        ),
        _select_expr(
            columns,
            "verdict",
            "status",
            "outcome",
            "decision",
            alias="verdict",
            fallback=_count_verdict_expr(columns),
        ),
        _select_expr(columns, "confidence", "score", alias="confidence"),
        _select_expr(
            columns,
            "checked_at",
            "reviewed_at",
            "created_at",
            "updated_at",
            alias="checked_at",
        ),
        _select_expr(columns, "claim_text", "annotation_text", "text", alias="claim_text"),
        _select_expr(columns, "content_label", "label", alias="content_label"),
    ]
    query = f"SELECT {', '.join(select)} FROM content_claim_checks ORDER BY claim_id, checked_at"
    return [dict(row) for row in conn.execute(query).fetchall()]


def _attach_claim_context(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    rows: list[dict[str, Any]],
) -> None:
    columns = schema.get("content_claims")
    if not columns or "id" not in columns:
        return
    select = [
        "id AS claim_id",
        _select_expr(columns, "content_id", "generated_content_id", alias="content_id"),
        _select_expr(columns, "claim_text", "text", "annotation_text", "claim", alias="claim_text"),
    ]
    claims = {
        str(row["claim_id"]): dict(row)
        for row in conn.execute(f"SELECT {', '.join(select)} FROM content_claims").fetchall()
    }
    for row in rows:
        claim = claims.get(str(row.get("claim_id")))
        if not claim:
            continue
        if claim.get("claim_text") and not row.get("claim_text"):
            row["claim_text"] = claim["claim_text"]
        if claim.get("content_id") and not row.get("content_id"):
            row["content_id"] = claim["content_id"]


def _attach_content_context(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    rows: list[dict[str, Any]],
) -> None:
    columns = schema.get("generated_content")
    if not columns or "id" not in columns:
        return
    select = [
        "id AS content_id",
        _select_expr(
            columns,
            "content_label",
            "title",
            "content_type",
            "format",
            "status",
            alias="content_label",
        ),
    ]
    labels = {
        str(row["content_id"]): row["content_label"]
        for row in conn.execute(f"SELECT {', '.join(select)} FROM generated_content").fetchall()
    }
    for row in rows:
        label = labels.get(str(row.get("content_id")))
        if label and not row.get("content_label"):
            row["content_label"] = label


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    checked_at = _datetime(row.get("checked_at") or row.get("reviewed_at") or row.get("created_at"))
    return {
        "check_id": row.get("check_id") or row.get("id"),
        "claim_id": row.get("claim_id") or row.get("content_claim_id") or row.get("content_id"),
        "content_id": row.get("content_id"),
        "reviewer_id": row.get("reviewer_id") or row.get("reviewer") or row.get("checked_by"),
        "verdict": _clean(row.get("verdict") or row.get("status") or row.get("outcome")).lower(),
        "confidence": _float(row.get("confidence")),
        "checked_at": checked_at.isoformat() if checked_at else None,
        "checked_at_dt": checked_at,
        "claim_text": row.get("claim_text") or row.get("annotation_text") or row.get("text"),
        "content_label": row.get("content_label"),
    }


def _reviewer_set(checks: list[dict[str, Any]]) -> set[str]:
    reviewers = {
        _clean(check.get("reviewer_id") or check.get("check_id") or index)
        for index, check in enumerate(checks)
    }
    reviewers.discard("")
    return reviewers


def _base_finding(
    claim_key: str,
    checks: list[dict[str, Any]],
    reviewers: set[str],
    verdicts: list[str],
) -> dict[str, Any]:
    first = checks[0]
    return {
        "claim_id": first.get("claim_id") or claim_key,
        "content_id": first.get("content_id"),
        "claim_text": first.get("claim_text"),
        "content_label": first.get("content_label"),
        "reviewer_count": len(reviewers),
        "check_count": len(checks),
        "verdicts": verdicts,
        "checked_at_first": min(
            (check["checked_at"] for check in checks if check["checked_at"]),
            default=None,
        ),
        "checked_at_last": max(
            (check["checked_at"] for check in checks if check["checked_at"]),
            default=None,
        ),
    }


def _find_verdict_reversal(checks: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]] | None:
    ordered = sorted(
        checks,
        key=lambda check: (
            check["checked_at_dt"] or datetime.min.replace(tzinfo=timezone.utc),
            str(check.get("check_id") or ""),
        ),
    )
    previous_decisive: dict[str, Any] | None = None
    for current in ordered:
        if _verdict_state(current["verdict"]) is None:
            continue
        if previous_decisive and _verdict_state(previous_decisive["verdict"]) != _verdict_state(current["verdict"]):
            return previous_decisive, current
        previous_decisive = current
    return None


def _verdict_state(verdict: str) -> str | None:
    if verdict in APPROVED_VERDICTS:
        return "approved"
    if verdict in REJECTED_VERDICTS:
        return "rejected"
    return None


def _count_verdict_expr(columns: set[str]) -> str:
    if {"supported_count", "unsupported_count"}.issubset(columns):
        return (
            "CASE "
            "WHEN supported_count > unsupported_count THEN 'supported' "
            "WHEN unsupported_count > supported_count THEN 'unsupported' "
            "ELSE NULL END"
        )
    return "NULL"


def _select_expr(
    columns: set[str],
    *names: str,
    alias: str,
    fallback: str = "NULL",
) -> str:
    for name in names:
        if name in columns:
            return f"{name} AS {alias}"
    return f"{fallback} AS {alias}"


def _optional_schema_gaps(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    gaps: dict[str, list[str]] = {}
    check_columns = schema.get("content_claim_checks", set())
    if not any(name in check_columns for name in ("claim_id", "content_claim_id", "content_id", "id")):
        gaps["content_claim_checks"] = ["claim_id"]
    if not any(name in check_columns for name in ("verdict", "status", "outcome", "decision")) and not {
        "supported_count",
        "unsupported_count",
    }.issubset(check_columns):
        gaps.setdefault("content_claim_checks", []).append("verdict")
    if "content_claims" in schema and "id" not in schema["content_claims"]:
        gaps["content_claims"] = ["id"]
    if "generated_content" in schema and "id" not in schema["generated_content"]:
        gaps["generated_content"] = ["id"]
    return gaps


def _group_findings(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for finding in findings:
        grouped[finding["reason"]].append(finding)
    return [
        {"reason": reason, "count": len(items), "items": items}
        for reason, items in sorted(grouped.items())
    ]


def _finding_sort_key(finding: dict[str, Any]) -> tuple[str, str, str]:
    return (
        finding["reason"],
        str(finding.get("claim_id") or ""),
        str(finding.get("checked_at_last") or ""),
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    if isinstance(db_or_conn, sqlite3.Connection):
        db_or_conn.row_factory = sqlite3.Row
        return db_or_conn
    conn = sqlite3.connect(db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    parsed = _datetime(value)
    if parsed is None:
        raise ValueError("now must be a datetime or ISO timestamp")
    return parsed


def _report_now(value: datetime | str | None, rows: list[dict[str, Any]]) -> datetime:
    if value is not None:
        return _now(value)
    observed = [
        parsed
        for row in rows
        for key in ("checked_at", "reviewed_at", "created_at", "updated_at")
        if (parsed := _datetime(row.get(key))) is not None
    ]
    return max(observed) if observed else datetime.now(timezone.utc)


def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = _clean(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _clean(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _float(value: Any) -> float | None:
    try:
        return float(value) if _clean(value) else None
    except (TypeError, ValueError):
        return None


def _require_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be positive")
