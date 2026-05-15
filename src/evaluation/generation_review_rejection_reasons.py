"""Summarize why generated content is rejected during review gates."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_LIMIT = 50
DEFAULT_EXAMPLES_LIMIT = 5
REJECTED_STATUSES = {
    "all_filtered",
    "below_threshold",
    "blocked",
    "failed",
    "rejected",
    "review_rejected",
}


def build_generation_review_rejection_reasons_report(
    review_rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    examples_limit: int = DEFAULT_EXAMPLES_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return ranked normalized rejection reasons from review/evaluation rows."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    if examples_limit < 0:
        raise ValueError("examples_limit must be non-negative")

    generated_at = _utc(now or datetime.now(timezone.utc))
    rejected_records = [_normalize_record(row) for row in review_rows if _is_rejected(row)]
    groups: dict[str, dict[str, Any]] = {}
    for record in rejected_records:
        reason = record["reason_label"]
        group = groups.setdefault(reason, _empty_group(reason))
        group["count"] += 1
        for key, breakdown in (
            ("gate_name", "affected_gates"),
            ("model", "models"),
            ("prompt_version", "prompt_versions"),
            ("content_format", "content_formats"),
        ):
            group[breakdown][record[key]] += 1
        if record["raw_reason"] and record["raw_reason"] not in group["raw_reason_examples"]:
            group["raw_reason_examples"].append(record["raw_reason"])
        group["examples"].append(record)

    reasons = [_finalize_group(group, examples_limit) for group in groups.values()]
    reasons.sort(key=lambda item: (-item["count"], item["reason_label"]))
    ranked = reasons[:limit]
    return {
        "artifact_type": "generation_review_rejection_reasons",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit, "examples_limit": examples_limit},
        "totals": {
            "rows_scanned": len(review_rows),
            "rejected_record_count": len(rejected_records),
            "reason_count": len(reasons),
            "ranked_reason_count": len(ranked),
        },
        "reasons": ranked,
        "empty_state": {
            "is_empty": not reasons,
            "message": "No rejected generation review records found." if not reasons else None,
        },
    }


def build_generation_review_rejection_reasons_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_generation_review_rejection_reasons_report(_load_review_rows(conn, schema), **kwargs)


def format_generation_review_rejection_reasons_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_generation_review_rejection_reasons_text(report: dict[str, Any]) -> str:
    lines = [
        "Generation Review Rejection Reasons",
        f"Generated: {report['generated_at']}",
        f"Filters: limit={report['filters']['limit']} examples_limit={report['filters']['examples_limit']}",
        (
            "Totals: "
            f"rejections={report['totals']['rejected_record_count']} "
            f"reasons={report['totals']['reason_count']} rows={report['totals']['rows_scanned']}"
        ),
    ]
    if not report["reasons"]:
        lines.extend(["", report["empty_state"]["message"]])
        return "\n".join(lines)
    lines.extend(["", "Reasons:", "count  reason                    top_gate              top_model             top_format"])
    for item in report["reasons"]:
        lines.append(
            f"{item['count']:<6} "
            f"{item['reason_label'][:24]:<24} "
            f"{_top_key(item['affected_gates'])[:21]:<21} "
            f"{_top_key(item['models'])[:21]:<21} "
            f"{_top_key(item['content_formats'])[:18]}"
        )
    return "\n".join(lines)


def normalize_generation_rejection_reason(reason_text: Any = None, reason_code: Any = None) -> str:
    """Map code/text variants and blanks into stable normalized reason labels."""
    code = _slug(reason_code)
    text = _text(reason_text)
    combined = " ".join(part for part in (code.replace("_", " "), text.lower()) if part).strip()
    if not combined:
        return "unknown"
    if code in {"below_threshold", "score_threshold", "threshold"} or "below threshold" in combined:
        return "below_threshold"
    if "unsupported" in combined and ("claim" in combined or "source" in combined):
        return "unsupported_claims"
    if "all candidates filtered" in combined or code in {"all_filtered", "filtered"}:
        return "all_filtered"
    if "budget" in combined or "cost" in combined:
        return "budget_exceeded"
    if "persona" in combined or "voice" in combined or "alignment" in combined:
        return "persona_misalignment"
    if "stale" in combined:
        return "stale_pattern"
    if "duplicate" in combined or "dedup" in combined or "repetitive" in combined:
        return "duplicate_or_repetitive"
    if "thread" in combined:
        return "thread_validation_failed"
    if "length" in combined or "too long" in combined or "too short" in combined:
        return "length_constraint"
    if "format" in combined or "schema" in combined or "json" in combined:
        return "format_error"
    if "quality gate" in combined or "gate rejected" in combined or code in {"quality_gate", "quality_gate_rejected"}:
        return "quality_gate_rejected"
    if "low quality" in combined or "quality" in combined:
        return "low_quality"
    return code or _slug(text) or "other"


def _load_review_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if "pipeline_runs" in schema:
        rows.extend(_load_pipeline_rows(conn, schema))
    if "eval_results" in schema:
        rows.extend(_load_eval_result_rows(conn, schema))
    if "content_feedback" in schema:
        rows.extend(_load_feedback_rows(conn, schema))
    return rows


def _load_pipeline_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema["pipeline_runs"]
    selected = [
        "pr.id AS record_id" if "id" in columns else "NULL AS record_id",
        "pr.content_id AS content_id" if "content_id" in columns else "NULL AS content_id",
        "pr.batch_id AS batch_id" if "batch_id" in columns else "NULL AS batch_id",
        "pr.content_type AS content_format" if "content_type" in columns else "NULL AS content_format",
        "pr.outcome AS status" if "outcome" in columns else "NULL AS status",
        "pr.published AS published" if "published" in columns else "NULL AS published",
        "pr.rejection_reason AS reason_text" if "rejection_reason" in columns else "NULL AS reason_text",
        "pr.outcome AS gate_name" if "outcome" in columns else "'pipeline' AS gate_name",
        "pr.created_at AS created_at" if "created_at" in columns else "NULL AS created_at",
    ]
    if "generated_content" in schema and "content_id" in columns:
        gc = schema["generated_content"]
        selected.append("gc.content_format AS joined_content_format" if "content_format" in gc else "NULL AS joined_content_format")
        selected.append("gc.content_type AS joined_content_type" if "content_type" in gc else "NULL AS joined_content_type")
        query = f"""SELECT {', '.join(selected)}
                   FROM pipeline_runs pr
                   LEFT JOIN generated_content gc ON gc.id = pr.content_id"""
    else:
        query = f"SELECT {', '.join(selected)}, NULL AS joined_content_format, NULL AS joined_content_type FROM pipeline_runs"
    return [dict(row) for row in conn.execute(query).fetchall()]


def _load_eval_result_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema["eval_results"]
    selected = [
        "id AS record_id" if "id" in columns else "NULL AS record_id",
        "batch_id" if "batch_id" in columns else "NULL AS batch_id",
        "content_type" if "content_type" in columns else "NULL AS content_format",
        "rejection_reason AS reason_text" if "rejection_reason" in columns else "NULL AS reason_text",
        "generator_model" if "generator_model" in columns else "NULL AS generator_model",
        "evaluator_model" if "evaluator_model" in columns else "NULL AS evaluator_model",
        "prompt_version" if "prompt_version" in columns else "NULL AS prompt_version",
        "final_score" if "final_score" in columns else "NULL AS final_score",
        "created_at" if "created_at" in columns else "NULL AS created_at",
    ]
    return [
        {
            **dict(row),
            "status": "rejected" if _text(dict(row).get("reason_text")) else "accepted",
            "gate_name": "evaluation",
        }
        for row in conn.execute(f"SELECT {', '.join(selected)} FROM eval_results").fetchall()
    ]


def _load_feedback_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema["content_feedback"]
    if "feedback_type" not in columns:
        return []
    selected = [
        "cf.id AS record_id" if "id" in columns else "NULL AS record_id",
        "cf.content_id AS content_id" if "content_id" in columns else "NULL AS content_id",
        "cf.feedback_type AS status",
        "cf.notes AS reason_text" if "notes" in columns else "NULL AS reason_text",
        "cf.tags AS reason_code" if "tags" in columns else "NULL AS reason_code",
        "cf.created_at AS created_at" if "created_at" in columns else "NULL AS created_at",
    ]
    if "generated_content" in schema and "content_id" in columns:
        gc = schema["generated_content"]
        selected.append("gc.content_format AS joined_content_format" if "content_format" in gc else "NULL AS joined_content_format")
        selected.append("gc.content_type AS joined_content_type" if "content_type" in gc else "NULL AS joined_content_type")
        query = f"""SELECT {', '.join(selected)}
                   FROM content_feedback cf
                   LEFT JOIN generated_content gc ON gc.id = cf.content_id
                   WHERE LOWER(COALESCE(feedback_type, '')) = 'reject'"""
    else:
        query = f"""SELECT {', '.join(selected)}, NULL AS joined_content_format, NULL AS joined_content_type
                   FROM content_feedback
                   WHERE LOWER(COALESCE(feedback_type, '')) = 'reject'"""
    rows = []
    for row in conn.execute(query).fetchall():
        data = dict(row)
        data["gate_name"] = "human_review"
        rows.append(data)
    return rows


def _is_rejected(row: dict[str, Any]) -> bool:
    if _text(_first(row, "rejection_reason", "reason_text", "reason", "failure_reason", "reason_code", "rejection_code")):
        return True
    status = _text(_first(row, "status", "review_status", "outcome", "result")).lower()
    if status in REJECTED_STATUSES:
        return True
    published = row.get("published")
    return published in (0, "0", False) and status not in {"published", "accepted", "approved"}


def _normalize_record(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _json_obj(row.get("metadata"))
    reason_text = _first(row, "rejection_reason", "reason_text", "reason", "failure_reason", "error")
    reason_code = _first(row, "reason_code", "rejection_code", "error_code", "code")
    if not reason_code and "tags" in row:
        reason_code = _first_tag(row.get("tags"))
    prompt_version = _text(_first(row, "prompt_version", "prompt", "prompt_id") or metadata.get("prompt_version")) or "unknown"
    model = (
        _text(_first(row, "model", "model_name", "generator_model") or metadata.get("model"))
        or _text(row.get("evaluator_model"))
        or "unknown"
    )
    content_format = (
        _text(_first(row, "joined_content_format", "content_format", "format", "content_type", "joined_content_type"))
        or "unknown"
    )
    gate_name = _text(_first(row, "gate_name", "gate", "stage", "outcome", "status")) or "review"
    record = {
        "record_id": _text(_first(row, "record_id", "id", "run_id")),
        "content_id": _text(_first(row, "content_id", "generated_content_id")),
        "batch_id": _text(row.get("batch_id")),
        "gate_name": gate_name,
        "reason_label": normalize_generation_rejection_reason(reason_text, reason_code),
        "reason_code": _text(reason_code),
        "raw_reason": _text(reason_text),
        "model": model,
        "prompt_version": prompt_version,
        "content_format": content_format,
        "created_at": _text(_first(row, "created_at", "reviewed_at", "evaluated_at", "timestamp")),
    }
    record["example_label"] = _example_label(record)
    return record


def _empty_group(reason_label: str) -> dict[str, Any]:
    return {
        "reason_label": reason_label,
        "count": 0,
        "affected_gates": Counter(),
        "models": Counter(),
        "prompt_versions": Counter(),
        "content_formats": Counter(),
        "raw_reason_examples": [],
        "examples": [],
    }


def _finalize_group(group: dict[str, Any], examples_limit: int) -> dict[str, Any]:
    examples = sorted(group["examples"], key=lambda item: (item["created_at"], item["record_id"]), reverse=True)
    return {
        "reason_label": group["reason_label"],
        "count": group["count"],
        "affected_gates": _counter_dict(group["affected_gates"]),
        "models": _counter_dict(group["models"]),
        "prompt_versions": _counter_dict(group["prompt_versions"]),
        "content_formats": _counter_dict(group["content_formats"]),
        "raw_reason_examples": group["raw_reason_examples"][:examples_limit],
        "affected_examples": [
            {
                "record_id": item["record_id"],
                "content_id": item["content_id"],
                "batch_id": item["batch_id"],
                "gate_name": item["gate_name"],
                "model": item["model"],
                "prompt_version": item["prompt_version"],
                "content_format": item["content_format"],
                "created_at": item["created_at"],
                "example": item["example_label"],
            }
            for item in examples[:examples_limit]
        ],
    }


def _counter_dict(counter: Counter) -> dict[str, int]:
    return dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


def _top_key(values: dict[str, int]) -> str:
    return next(iter(values), "-")


def _example_label(record: dict[str, Any]) -> str:
    label = record["content_id"] or record["batch_id"] or record["record_id"] or "unknown"
    reason = record["raw_reason"] or record["reason_code"] or record["reason_label"]
    return f"{label}: {reason}"[:240]


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _first_tag(value: Any) -> str:
    tags = _parse_list(value)
    return _text(tags[0]) if tags else ""


def _parse_list(value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return [part.strip() for part in value.split(",") if part.strip()]
        return parsed if isinstance(parsed, list) else [parsed]
    return [value]


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _slug(value: Any) -> str:
    text = _text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value).strip()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}
