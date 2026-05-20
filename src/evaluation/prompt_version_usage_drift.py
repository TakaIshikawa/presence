"""Compare prompt_versions usage counts with observed prediction references."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_MIN_DELTA = 1
ISSUE_TYPES = (
    "usage_count_mismatch",
    "unknown_prompt_reference",
    "prompt_hash_mismatch",
    "hash_reused_across_types",
)


def build_prompt_version_usage_drift_report(
    prompt_rows: list[dict[str, Any]],
    prediction_rows: list[dict[str, Any]],
    *,
    prompt_type: str | None = None,
    min_delta: int = DEFAULT_MIN_DELTA,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if min_delta < 0:
        raise ValueError("min_delta must be non-negative")
    generated_at = _utc(now or datetime.now(timezone.utc))
    wanted_type = _clean(prompt_type).lower() or None
    prompts = [row for row in prompt_rows if not wanted_type or _clean(row.get("prompt_type")).lower() == wanted_type]
    predictions = [row for row in prediction_rows if not wanted_type or _clean(row.get("prompt_type")).lower() == wanted_type]
    by_combo = {(_clean(row.get("prompt_type")), _clean(row.get("prompt_version")), _clean(row.get("prompt_hash"))): row for row in prompts}
    by_type_version = {(_clean(row.get("prompt_type")), _clean(row.get("prompt_version"))): row for row in prompts}
    observed: Counter[tuple[str, str, str]] = Counter(
        (_clean(row.get("prompt_type")), _clean(row.get("prompt_version")), _clean(row.get("prompt_hash"))) for row in predictions
    )
    findings: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()

    for row in prompts:
        key = (_clean(row.get("prompt_type")), _clean(row.get("prompt_version")), _clean(row.get("prompt_hash")))
        declared = _to_int(row.get("usage_count")) or 0
        actual = observed[key]
        delta = actual - declared
        if abs(delta) >= min_delta:
            _record(
                findings,
                counts,
                {
                    "issue_type": "usage_count_mismatch",
                    "prompt_id": row.get("prompt_id"),
                    "prompt_type": key[0],
                    "prompt_version": key[1],
                    "prompt_hash": key[2],
                    "usage_count": declared,
                    "observed_count": actual,
                    "delta": delta,
                    "detail": "prompt_versions.usage_count differs from observed engagement_predictions rows",
                },
            )

    for row in predictions:
        key = (_clean(row.get("prompt_type")), _clean(row.get("prompt_version")), _clean(row.get("prompt_hash")))
        if key in by_combo:
            continue
        tv = (key[0], key[1])
        if tv in by_type_version and key[2] and _clean(by_type_version[tv].get("prompt_hash")) != key[2]:
            issue_type = "prompt_hash_mismatch"
            detail = "prediction references a known prompt type/version with a different hash"
        else:
            issue_type = "unknown_prompt_reference"
            detail = "prediction references no prompt_versions type/version/hash combination"
        _record(
            findings,
            counts,
            {
                "issue_type": issue_type,
                "prediction_id": row.get("prediction_id"),
                "prompt_type": key[0],
                "prompt_version": key[1],
                "prompt_hash": key[2],
                "expected_prompt_hash": _clean(by_type_version[tv].get("prompt_hash")) if tv in by_type_version else None,
                "detail": detail,
            },
        )

    hash_types: dict[str, set[str]] = defaultdict(set)
    hash_prompt_ids: dict[str, list[Any]] = defaultdict(list)
    for row in prompts:
        prompt_hash = _clean(row.get("prompt_hash"))
        if not prompt_hash:
            continue
        hash_types[prompt_hash].add(_clean(row.get("prompt_type")))
        hash_prompt_ids[prompt_hash].append(row.get("prompt_id"))
    for prompt_hash, types in sorted(hash_types.items()):
        if len(types) > 1:
            _record(
                findings,
                counts,
                {
                    "issue_type": "hash_reused_across_types",
                    "prompt_hash": prompt_hash,
                    "prompt_types": sorted(types),
                    "prompt_ids": hash_prompt_ids[prompt_hash],
                    "detail": "same prompt_hash appears across multiple prompt types",
                },
            )

    findings.sort(key=_finding_sort_key)
    return {
        "artifact_type": "prompt_version_usage_drift",
        "generated_at": generated_at.isoformat(),
        "filters": {"prompt_type": wanted_type or "all", "min_delta": min_delta},
        "prompt_count": len(prompts),
        "prediction_count": len(predictions),
        "finding_count": len(findings),
        "issue_counts": {issue: counts[issue] for issue in ISSUE_TYPES},
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            table: sorted(columns)
            for table, columns in sorted((missing_columns or {}).items())
            if columns
        },
        "findings": findings,
    }


def build_prompt_version_usage_drift_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {
        "prompt_versions": {"id", "prompt_type", "prompt_version", "prompt_hash", "usage_count"},
        "engagement_predictions": {"id", "prompt_type", "prompt_version", "prompt_hash"},
    }
    missing_tables = sorted(table for table in required if table not in schema)
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    if missing_tables or missing_columns:
        return build_prompt_version_usage_drift_report([], [], missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)
    return build_prompt_version_usage_drift_report(
        _load_prompts(conn),
        _load_predictions(conn),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_prompt_version_usage_drift_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_prompt_version_usage_drift_text(report: dict[str, Any]) -> str:
    lines = [
        "Prompt Version Usage Drift",
        f"Generated: {report['generated_at']}",
        f"Prompt type: {report['filters']['prompt_type']}",
        f"Min delta: {report['filters']['min_delta']}",
        f"Totals: prompts={report['prompt_count']} predictions={report['prediction_count']} findings={report['finding_count']}",
        "Issue counts: " + ", ".join(f"{issue}={report['issue_counts'][issue]}" for issue in ISSUE_TYPES),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{t}.{c}" for t, cols in report["missing_columns"].items() for c in cols))
    if not report["findings"]:
        lines.append("No prompt version usage drift found.")
        return "\n".join(lines)
    lines.extend(["", "Findings:"])
    for finding in report["findings"]:
        lines.append(
            f"- {finding['issue_type']} prompt_type={finding.get('prompt_type', '-')} "
            f"prompt_version={finding.get('prompt_version', '-')} prompt_hash={finding.get('prompt_hash', '-')} "
            f"detail={finding['detail']}"
        )
    return "\n".join(lines)


def _load_prompts(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            """SELECT id AS prompt_id, prompt_type, prompt_version, prompt_hash, usage_count
               FROM prompt_versions
               ORDER BY prompt_type ASC, prompt_version ASC, id ASC"""
        ).fetchall()
    ]


def _load_predictions(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            """SELECT id AS prediction_id, prompt_type, prompt_version, prompt_hash
               FROM engagement_predictions
               ORDER BY id ASC"""
        ).fetchall()
    ]


def _record(findings: list[dict[str, Any]], counts: Counter[str], finding: dict[str, Any]) -> None:
    findings.append(finding)
    counts[finding["issue_type"]] += 1


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _to_int(value: Any) -> int | None:
    try:
        return None if value in (None, "") else int(value)
    except (TypeError, ValueError):
        return None


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    return (ISSUE_TYPES.index(finding["issue_type"]), finding.get("prompt_type") or "", finding.get("prompt_version") or "", finding.get("prediction_id") or finding.get("prompt_id") or 0)
