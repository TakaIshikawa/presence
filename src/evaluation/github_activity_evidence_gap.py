"""Report generated content from GitHub activity with missing evidence."""

from __future__ import annotations

import json
import sqlite3
from typing import Any


EVIDENCE_FIELDS = {
    "commit": ("commit_sha", "commit_url"),
    "session": ("session_id", "session_url"),
    "pr": ("pr_url", "pull_request_url", "pr_number"),
    "source": ("source_url", "source_activity_ids"),
}


def build_github_activity_evidence_gap_report(db_or_conn: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "generated_content" not in schema:
        return _report([], missing_tables=["generated_content"])
    rows = [dict(row) for row in conn.execute("SELECT * FROM generated_content").fetchall()]
    findings = []
    for row in rows:
        activity_id = _clean(row.get("github_activity_id") or row.get("activity_id"))
        source_ids = _json_list(row.get("source_activity_ids"))
        if not activity_id and not source_ids:
            if _clean(row.get("content_type")) not in {"github", "blog", "newsletter", "post", ""}:
                continue
        missing = [kind for kind, fields in EVIDENCE_FIELDS.items() if not any(_clean(row.get(field)) for field in fields)]
        if missing:
            findings.append(
                {
                    "content_id": _clean(row.get("id") or row.get("content_id")),
                    "activity_id": activity_id or (source_ids[0] if source_ids else ""),
                    "missing_evidence": missing,
                    "severity": "high" if len(missing) >= 3 else "medium" if len(missing) >= 2 else "low",
                    "title": _clean(row.get("title")),
                }
            )
    findings.sort(key=lambda item: (-len(item["missing_evidence"]), item["content_id"]))
    return _report(findings)


def format_github_activity_evidence_gap_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_github_activity_evidence_gap_text(report: dict[str, Any]) -> str:
    lines = [
        "GitHub Activity Evidence Gap",
        f"Totals: findings={report['totals']['finding_count']} high={report['totals']['high_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append("No GitHub evidence gaps found.")
        return "\n".join(lines)
    for item in report["findings"]:
        lines.append(
            f"  - content={item['content_id']} activity={item['activity_id'] or '-'} "
            f"missing={','.join(item['missing_evidence'])} severity={item['severity']}"
        )
    return "\n".join(lines)


def _report(findings: list[dict[str, Any]], *, missing_tables: list[str] | None = None) -> dict[str, Any]:
    return {
        "artifact_type": "github_activity_evidence_gap",
        "totals": {
            "finding_count": len(findings),
            "high_count": sum(1 for item in findings if item["severity"] == "high"),
        },
        "findings": findings,
        "missing_tables": missing_tables or [],
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _json_list(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError):
        return []
    return [str(item) for item in parsed] if isinstance(parsed, list) else []
