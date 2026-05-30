"""Report publication attempts that succeeded after earlier failures."""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from ._batch_report_common import clean, connection, dt, empty_state, flatten_missing, json_dumps, now_value, pick, positive, schema

ARTIFACT_TYPE = "publication_attempt_success_after_retry"
DEFAULT_LIMIT = 100
SUCCESS_STATUSES = {"success", "succeeded", "published", "sent", "ok"}


def build_publication_attempt_success_after_retry_report(rows: list[dict[str, Any]], *, limit: int = DEFAULT_LIMIT, now: Any = None, missing_tables=None, missing_columns=None) -> dict[str, Any]:
    positive("limit", limit)
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[(clean(row.get("content_id")), clean(row.get("platform"), "unknown").lower())].append(row)
    findings = []
    for (content_id, platform), attempts in groups.items():
        ordered = sorted(attempts, key=_attempt_sort)
        success_index = next((idx for idx, row in enumerate(ordered) if _is_success(row)), None)
        if success_index is None or success_index == 0:
            continue
        failures = [row for row in ordered[:success_index] if not _is_success(row)]
        if not failures:
            continue
        first_failure_at = dt(failures[0].get("attempted_at") or failures[0].get("created_at"))
        success_at = dt(ordered[success_index].get("attempted_at") or ordered[success_index].get("created_at"))
        elapsed = round((success_at - first_failure_at).total_seconds() / 3600, 2) if first_failure_at and success_at else None
        findings.append(
            {
                "content_id": content_id,
                "platform": platform,
                "success_attempt_id": ordered[success_index].get("attempt_id") or ordered[success_index].get("id"),
                "attempts_before_success": len(failures),
                "elapsed_hours": elapsed,
                "prior_error_categories": dict(sorted(Counter(_error(row) for row in failures).items())),
                "recommendation": "Keep this retry path; review prior error categories for automation opportunities.",
            }
        )
    findings.sort(key=lambda item: (-item["attempts_before_success"], -(item["elapsed_hours"] or 0), item["platform"], item["content_id"]))
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": now_value(now).isoformat(),
        "filters": {"limit": limit},
        "summary": {"attempt_count": len(rows), "finding_count": len(findings), "shown_count": len(shown)},
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items()) if v},
        "empty_state": empty_state(findings, "No publication attempts succeeded after retry.", schema_gap=bool(missing_tables or missing_columns)),
    }


def build_publication_attempt_success_after_retry_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    s = schema(conn)
    if "publication_attempts" not in s:
        return build_publication_attempt_success_after_retry_report([], missing_tables=["publication_attempts"], **kwargs)
    cols = s["publication_attempts"]
    missing = [col for col in ("content_id", "platform") if col not in cols]
    if not ({"success", "status", "outcome", "result"} & cols):
        missing.append("success|status|outcome|result")
    if missing:
        return build_publication_attempt_success_after_retry_report([], missing_columns={"publication_attempts": missing}, **kwargs)
    rows = _load_rows(conn, cols)
    return build_publication_attempt_success_after_retry_report(rows, **kwargs)


def format_publication_attempt_success_after_retry_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_publication_attempt_success_after_retry_text(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = ["Publication Attempt Success After Retry", f"Generated: {report['generated_at']}", f"Totals: attempts={s['attempt_count']} findings={s['finding_count']} shown={s['shown_count']}"]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "content_id | platform | attempts_before_success | elapsed_hours | prior_errors"])
    for item in report["findings"]:
        errors = ", ".join(f"{k}:{v}" for k, v in item["prior_error_categories"].items())
        lines.append(f"{item['content_id']} | {item['platform']} | {item['attempts_before_success']} | {item['elapsed_hours']} | {errors}")
    return "\n".join(lines)


def _load_rows(conn: Any, cols: set[str]) -> list[dict[str, Any]]:
    select = [
        pick(cols, "id", out="attempt_id"),
        "content_id",
        "platform",
        pick(cols, "attempted_at", "created_at", out="attempted_at"),
        pick(cols, "success", out="success"),
        pick(cols, "status", "outcome", "result", out="status"),
        pick(cols, "error_category", "retry_reason", "error_type", out="error_category"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM publication_attempts ORDER BY content_id ASC, platform ASC, attempted_at ASC")]


def _is_success(row: dict[str, Any]) -> bool:
    if clean(row.get("success")).lower() in {"1", "true", "yes"}:
        return True
    return clean(row.get("status")).lower() in SUCCESS_STATUSES


def _error(row: dict[str, Any]) -> str:
    return clean(row.get("error_category") or row.get("status"), "unknown").lower()


def _attempt_sort(row: dict[str, Any]) -> tuple[Any, Any]:
    return (dt(row.get("attempted_at")) or now_value(None), clean(row.get("attempt_id") or row.get("id")))
