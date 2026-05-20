"""Validate serialized payloads for publishable artifacts."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Mapping


DEFAULT_LIMIT = 50
DEFAULT_LENGTH_LIMITS = {
    "x": 280,
    "twitter": 280,
    "bluesky": 300,
    "linkedin": 3000,
    "newsletter": 50000,
    "blog": 100000,
}
DEFAULT_REQUIRED_FIELDS = {
    "x": ("text",),
    "twitter": ("text",),
    "bluesky": ("text",),
    "linkedin": ("text",),
    "newsletter": ("subject", "body"),
    "blog": ("title", "body"),
}
TEXT_KEYS = ("text", "body", "content", "message")


def build_publish_artifact_payload_integrity_report(
    db_or_conn: Any,
    *,
    platform: str = "all",
    limit: int = DEFAULT_LIMIT,
    length_limits: Mapping[str, int] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Inspect serialized publish payloads and return deterministic integrity findings."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    selected_platform = _normalize_platform(platform)
    limits = _normalize_limits(length_limits or DEFAULT_LENGTH_LIMITS)
    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows, schema_gaps = _load_payload_rows(conn, schema)
    if selected_platform != "all":
        rows = [row for row in rows if _normalize_platform(row.get("platform")) == selected_platform]
    findings = _findings(rows, limits)
    findings.sort(key=_finding_sort_key)
    shown = findings[:limit]
    reason_counts = Counter(reason for row in findings for reason in row["reason_codes"])
    return {
        "artifact_type": "publish_artifact_payload_integrity",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "length_limits": dict(sorted(limits.items())),
            "limit": limit,
            "platform": selected_platform,
        },
        "summary": {
            "finding_count": len(findings),
            "payload_rows_scanned": len(rows),
            "shown_finding_count": len(shown),
            "by_reason": dict(sorted(reason_counts.items())),
        },
        "findings": shown,
        "schema_gaps": schema_gaps,
    }


def format_publish_artifact_payload_integrity_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publish_artifact_payload_integrity_text(report: dict[str, Any]) -> str:
    filters = report["filters"]
    summary = report["summary"]
    lines = [
        "Publish Artifact Payload Integrity",
        f"Generated: {report['generated_at']}",
        f"Filters: platform={filters['platform']} limit={filters['limit']}",
        (
            "Totals: "
            f"rows={summary['payload_rows_scanned']} "
            f"findings={summary['finding_count']} shown={summary['shown_finding_count']}"
        ),
    ]
    gaps = report.get("schema_gaps") or {}
    if gaps.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(gaps["missing_tables"]))
    missing_columns = [
        f"{table}({', '.join(columns)})"
        for table, columns in sorted((gaps.get("missing_columns") or {}).items())
        if columns
    ]
    if missing_columns:
        lines.append("Missing columns: " + "; ".join(missing_columns))
    if not report["findings"]:
        lines.append("No publish artifact payload integrity issues found.")
        return "\n".join(lines)
    lines.extend(["", "Findings:"])
    for row in report["findings"]:
        lines.append(
            f"- artifact={row['artifact_id']} content={row['content_id'] or '-'} "
            f"platform={row['platform']} variant={row['variant_id'] or '-'} "
            f"reasons={','.join(row['reason_codes'])} detail={row['detail'] or '-'}"
        )
    return "\n".join(lines)


def parse_length_limit(value: str) -> tuple[str, int]:
    platform, sep, limit = value.partition(":")
    normalized = _normalize_platform(platform)
    if not sep or normalized == "all":
        raise ValueError("length limits must use platform:limit")
    try:
        parsed = int(limit)
    except ValueError as exc:
        raise ValueError("length limit must be an integer") from exc
    if parsed <= 0:
        raise ValueError("length limit must be positive")
    return normalized, parsed


def _findings(rows: list[dict[str, Any]], limits: Mapping[str, int]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for row in rows:
        platform = _normalize_platform(row.get("platform"))
        payload, error = _decode_payload(row.get("payload_json"))
        reasons: list[str] = []
        details: list[str] = []
        payload_text = ""
        if error:
            reasons.append("malformed_payload_json")
            details.append(error)
        else:
            missing = [field for field in _required_fields(platform) if not _has_text(payload.get(field))]
            if missing:
                reasons.append("missing_required_platform_fields")
                details.append("missing=" + ",".join(missing))
            payload_text = _payload_text(payload)
            source_text = _optional_text(row.get("variant_content")) or _optional_text(row.get("generated_content")) or ""
            if payload_text and source_text and _normalize_text(payload_text) != _normalize_text(source_text):
                reasons.append("payload_content_text_mismatch")
                details.append("payload text differs from selected content")
            length_limit = limits.get(platform)
            if length_limit is not None and len(payload_text) > length_limit:
                reasons.append("payload_exceeds_platform_length_limit")
                details.append(f"length={len(payload_text)} limit={length_limit}")
            payload_platform = _payload_platform(payload)
            selected_variant_platform = _normalize_platform(row.get("selected_variant_platform"))
            variant_platform = _normalize_platform(row.get("variant_platform"))
            conflict_targets = {value for value in (payload_platform, selected_variant_platform, variant_platform) if value not in {"", "all", platform}}
            if conflict_targets:
                reasons.append("platform_conflicts_with_selected_variant")
                details.append("conflicts=" + ",".join(sorted(conflict_targets)))
        if not reasons:
            continue
        findings.append(
            {
                "artifact_id": _optional_text(row.get("artifact_id")) or "unknown",
                "content_id": _int_or_none(row.get("content_id")),
                "platform": platform,
                "variant_id": _int_or_none(row.get("variant_id")),
                "payload_text_length": len(payload_text),
                "reason_codes": reasons,
                "detail": "; ".join(details),
            }
        )
    return findings


def _load_payload_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if "publish_artifacts" in schema:
        return _load_publish_artifacts(conn, schema)
    if "content_variants" in schema:
        return _load_content_variants(conn, schema)
    return [], {"missing_tables": ["publish_artifacts", "content_variants"], "missing_columns": {}}


def _load_publish_artifacts(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    columns = schema["publish_artifacts"]
    required = {"id", "platform"}
    missing = sorted(required - columns)
    if missing:
        return [], {"missing_tables": [], "missing_columns": {"publish_artifacts": missing}}
    payload_expr = _first_expr(columns, ("payload_json", "payload", "serialized_payload"), "NULL", "pa")
    joins = ""
    selected_variant_platform = "NULL"
    variant_content = "NULL"
    variant_platform = "NULL"
    if "content_variants" in schema and "variant_id" in columns:
        cv = schema["content_variants"]
        joins += " LEFT JOIN content_variants cv ON cv.id = pa.variant_id"
        selected_variant_platform = _raw_expr(cv, "platform", "NULL", "cv")
        variant_platform = selected_variant_platform
        variant_content = _raw_expr(cv, "content", "NULL", "cv")
    generated_content = "NULL"
    if "generated_content" in schema and "content_id" in columns:
        gc = schema["generated_content"]
        joins += " LEFT JOIN generated_content gc ON gc.id = pa.content_id"
        generated_content = _raw_expr(gc, "content", "NULL", "gc")
    rows = conn.execute(
        f"""SELECT pa.id AS artifact_id,
                  {_raw_expr(columns, 'content_id', 'NULL', 'pa')} AS content_id,
                  pa.platform AS platform,
                  {_raw_expr(columns, 'variant_id', 'NULL', 'pa')} AS variant_id,
                  {payload_expr} AS payload_json,
                  {variant_platform} AS variant_platform,
                  {selected_variant_platform} AS selected_variant_platform,
                  {variant_content} AS variant_content,
                  {generated_content} AS generated_content
             FROM publish_artifacts pa{joins}
             ORDER BY pa.platform ASC, pa.id ASC"""
    ).fetchall()
    gaps = {"missing_tables": [], "missing_columns": {}}
    if payload_expr == "NULL":
        gaps["missing_columns"]["publish_artifacts"] = ["payload_json"]
    return [dict(row) for row in rows], gaps


def _load_content_variants(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    columns = schema["content_variants"]
    required = {"id", "content_id", "platform", "content"}
    missing = sorted(required - columns)
    if missing:
        return [], {"missing_tables": [], "missing_columns": {"content_variants": missing}}
    joins = ""
    generated_content = "NULL"
    if "generated_content" in schema:
        gc = schema["generated_content"]
        joins += " LEFT JOIN generated_content gc ON gc.id = cv.content_id"
        generated_content = _raw_expr(gc, "content", "NULL", "gc")
    selected_join = ""
    selected_platform = "cv.platform"
    if "selected" in columns:
        selected_join = " LEFT JOIN content_variants selected_cv ON selected_cv.content_id = cv.content_id AND selected_cv.selected = 1"
        selected_platform = "COALESCE(selected_cv.platform, cv.platform)"
    rows = conn.execute(
        f"""SELECT cv.id AS artifact_id,
                  cv.content_id AS content_id,
                  cv.platform AS platform,
                  cv.id AS variant_id,
                  {_raw_expr(columns, 'metadata', 'NULL', 'cv')} AS payload_json,
                  cv.platform AS variant_platform,
                  {selected_platform} AS selected_variant_platform,
                  cv.content AS variant_content,
                  {generated_content} AS generated_content
             FROM content_variants cv{joins}{selected_join}
             ORDER BY cv.platform ASC, cv.id ASC"""
    ).fetchall()
    gaps = {"missing_tables": [], "missing_columns": {}}
    if "metadata" not in columns:
        gaps["missing_columns"]["content_variants"] = ["metadata"]
    return [dict(row) for row in rows], gaps


def _decode_payload(value: Any) -> tuple[dict[str, Any], str | None]:
    if value in (None, ""):
        return {}, "payload is missing"
    payload = value
    if isinstance(value, str):
        try:
            payload = json.loads(value)
        except json.JSONDecodeError as exc:
            return {}, f"invalid JSON: {exc.msg}"
    if isinstance(payload, Mapping) and any(key in payload for key in ("payload", "payload_json", "publish_payload")):
        nested = payload.get("payload_json") or payload.get("publish_payload") or payload.get("payload")
        if isinstance(nested, str):
            return _decode_payload(nested)
        if isinstance(nested, Mapping):
            payload = nested
    if not isinstance(payload, Mapping):
        return {}, "payload is not an object"
    return dict(payload), None


def _payload_text(payload: Mapping[str, Any]) -> str:
    for key in TEXT_KEYS:
        value = payload.get(key)
        if _has_text(value):
            return str(value)
    return ""


def _payload_platform(payload: Mapping[str, Any]) -> str:
    return _normalize_platform(payload.get("platform") or payload.get("target_platform"))


def _required_fields(platform: str) -> tuple[str, ...]:
    return DEFAULT_REQUIRED_FIELDS.get(platform, ("text",))


def _normalize_limits(values: Mapping[str, int]) -> dict[str, int]:
    normalized: dict[str, int] = {}
    for platform, limit in values.items():
        parsed = int(limit)
        if parsed <= 0:
            raise ValueError("length limits must be positive")
        normalized[_normalize_platform(platform)] = parsed
    return normalized


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _first_expr(columns: set[str], names: tuple[str, ...], default: str, table_alias: str) -> str:
    for name in names:
        if name in columns:
            return f"{table_alias}.{name}"
    return default


def _raw_expr(columns: set[str], column: str, default: str, table_alias: str) -> str:
    return f"{table_alias}.{column}" if column in columns else default


def _finding_sort_key(row: Mapping[str, Any]) -> tuple[str, int, str, str]:
    return (str(row.get("platform") or ""), int(row.get("content_id") or 0), str(row.get("artifact_id") or ""), ",".join(row.get("reason_codes") or ()))


def _normalize_platform(value: Any) -> str:
    text = _optional_text(value)
    return text.lower() if text else "all"


def _normalize_text(value: str) -> str:
    return " ".join(value.split())


def _has_text(value: Any) -> bool:
    return bool(_optional_text(value))


def _optional_text(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    try:
        return None if value in (None, "") else int(value)
    except (TypeError, ValueError):
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
