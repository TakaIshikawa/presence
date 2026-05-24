"""Check generated content disclosure label coverage."""

from __future__ import annotations

from typing import Any

from ._report_utils import clean, connection, expr, json_dumps, loads_list, loads_obj, lower, now_iso, positive, schema


ARTIFACT_TYPE = "generated_content_disclosure_label_coverage"
DEFAULT_LIMIT = 50
DEFAULT_REQUIRED_LABELS = ("ai_generated",)
HIGH_STATUSES = {"published", "queued", "scheduled", "approved"}


def build_generated_content_disclosure_label_coverage_report(
    rows: list[dict[str, Any]],
    *,
    required_labels: list[str] | tuple[str, ...] = DEFAULT_REQUIRED_LABELS,
    limit: int = DEFAULT_LIMIT,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    now: Any = None,
) -> dict[str, Any]:
    positive("limit", limit)
    required = tuple(sorted({lower(x) for x in required_labels if clean(x)})) or DEFAULT_REQUIRED_LABELS
    findings: list[dict[str, Any]] = []
    for row in rows:
        present = _labels(row)
        missing = [label for label in required if label not in present]
        if not missing:
            continue
        status = lower(row.get("publication_status") or row.get("status"), "draft")
        severity = "high" if status in HIGH_STATUSES else "medium"
        findings.append(
            {
                "content_id": str(row.get("content_id")),
                "content_type": clean(row.get("content_type")) or None,
                "platform": clean(row.get("platform")) or None,
                "publication_status": status,
                "missing_labels": missing,
                "present_labels": sorted(present),
                "severity": severity,
            }
        )
    findings.sort(key=lambda r: (0 if r["severity"] == "high" else 1, r["publication_status"], r["content_id"]))
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": now_iso(now),
        "thresholds": {"required_labels": list(required), "limit": limit},
        "summary": {"content_count": len(rows), "finding_count": len(findings), "shown_count": len(shown), "high_severity_count": sum(1 for r in findings if r["severity"] == "high")},
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())},
    }


def build_generated_content_disclosure_label_coverage_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    s = schema(conn)
    missing_tables = [] if "generated_content" in s else ["generated_content"]
    missing_columns: dict[str, list[str]] = {}
    rows = _load_rows(conn, s, missing_columns) if "generated_content" in s else []
    return build_generated_content_disclosure_label_coverage_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_generated_content_disclosure_label_coverage_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_generated_content_disclosure_label_coverage_text(report: dict[str, Any]) -> str:
    lines = ["Generated Content Disclosure Label Coverage", f"Generated: {report['generated_at']}", f"Required: {', '.join(report['thresholds']['required_labels'])}", f"Totals: content={report['summary']['content_count']} findings={report['summary']['finding_count']} shown={report['summary']['shown_count']}"]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append("No disclosure label coverage gaps found.")
        return "\n".join(lines)
    lines.extend(["", "content_id | type | platform | status | missing_labels | present_labels | severity"])
    for r in report["findings"]:
        lines.append(f"{r['content_id']} | {r['content_type'] or '-'} | {r['platform'] or '-'} | {r['publication_status']} | {','.join(r['missing_labels'])} | {','.join(r['present_labels']) or '-'} | {r['severity']}")
    return "\n".join(lines)


def _load_rows(conn: Any, s: dict[str, set[str]], missing: dict[str, list[str]]) -> list[dict[str, Any]]:
    cols = s["generated_content"]
    if "id" not in cols:
        missing["generated_content"] = ["id"]
        return []
    select = [
        "id AS content_id",
        expr(cols, "content_type", "type", default="NULL", out="content_type"),
        expr(cols, "platform", "target_platform", default="NULL", out="platform"),
        expr(cols, "publication_status", "status", default="'draft'", out="publication_status"),
        expr(cols, "metadata", default="NULL", out="metadata"),
        expr(cols, "tags", "labels", default="NULL", out="tags"),
        expr(cols, "disclosure", "disclosure_label", "sponsorship_label", default="NULL", out="disclosure"),
    ]
    return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM generated_content ORDER BY id")]


def _labels(row: dict[str, Any]) -> set[str]:
    labels = {lower(row.get("disclosure"))} if clean(row.get("disclosure")) else set()
    for item in loads_list(row.get("tags")):
        if isinstance(item, str):
            labels.add(lower(item))
    metadata = loads_obj(row.get("metadata"))
    for key in ("labels", "tags", "disclosures", "disclosure_labels"):
        for item in loads_list(metadata.get(key)):
            if isinstance(item, str):
                labels.add(lower(item))
    for key in ("disclosure", "disclosure_label", "sponsorship_label"):
        if clean(metadata.get(key)):
            labels.add(lower(metadata.get(key)))
    return {x for x in labels if x}

