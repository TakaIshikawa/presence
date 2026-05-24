"""Detect claim evidence quotes that no longer match saved evidence text."""

from __future__ import annotations

from collections import Counter
from difflib import SequenceMatcher
import re
import string
from typing import Any

from ._report_utils import clean, connection, expr, json_dumps, now_iso, positive, schema


ARTIFACT_TYPE = "content_claim_evidence_quote_mismatch"
DEFAULT_LIMIT = 50
DEFAULT_DISTANCE_THRESHOLD = 0.25


def build_content_claim_evidence_quote_mismatch_report(
    rows: list[dict[str, Any]],
    *,
    distance_threshold: float = DEFAULT_DISTANCE_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    now: Any = None,
) -> dict[str, Any]:
    positive("limit", limit)
    if not 0 <= distance_threshold <= 1:
        raise ValueError("distance_threshold must be between 0 and 1")
    findings: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for row in rows:
        quote = clean(row.get("quote_text"))
        excerpt = clean(row.get("excerpt_text"))
        issue = None
        distance: float | None = None
        if not quote:
            issue = "missing_quote"
        elif not excerpt:
            issue = "missing_excerpt"
        else:
            nq = _norm(quote)
            ne = _norm(excerpt)
            if nq and nq not in ne:
                distance = round(1.0 - SequenceMatcher(None, nq, ne).ratio(), 4)
                issue = "quote_edit_distance" if distance > distance_threshold else "quote_not_found"
        if issue:
            counts[issue] += 1
            findings.append(
                {
                    "claim_id": clean(row.get("claim_id")),
                    "content_id": clean(row.get("content_id")) or None,
                    "evidence_id": clean(row.get("evidence_id")) or None,
                    "source_url": clean(row.get("source_url")) or None,
                    "issue_type": issue,
                    "distance": distance,
                    "quote_text": quote or None,
                    "excerpt_text": excerpt[:240] or None,
                }
            )
    findings.sort(key=lambda r: (r["issue_type"], -(r["distance"] or 0), r["claim_id"], r["evidence_id"] or ""))
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": now_iso(now),
        "thresholds": {"distance_threshold": distance_threshold, "limit": limit},
        "summary": {
            "row_count": len(rows),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": dict(sorted(counts.items())),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())},
    }


def build_content_claim_evidence_quote_mismatch_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    s = schema(conn)
    missing_tables = [t for t in ("content_claim_checks", "content_claim_evidence") if t not in s]
    missing_columns: dict[str, list[str]] = {}
    rows: list[dict[str, Any]] = []
    if not missing_tables:
        check_cols = s["content_claim_checks"]
        evidence_cols = s["content_claim_evidence"]
        required = {"id"}
        if not required.issubset(check_cols):
            missing_columns["content_claim_checks"] = sorted(required - check_cols)
        join_cols = {"claim_check_id"}
        if not join_cols.issubset(evidence_cols):
            missing_columns["content_claim_evidence"] = sorted(join_cols - evidence_cols)
        if not missing_columns:
            rows = _load_rows(conn, check_cols, evidence_cols)
    return build_content_claim_evidence_quote_mismatch_report(
        rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs
    )


def format_content_claim_evidence_quote_mismatch_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_content_claim_evidence_quote_mismatch_text(report: dict[str, Any]) -> str:
    lines = [
        "Content Claim Evidence Quote Mismatch",
        f"Generated: {report['generated_at']}",
        f"Totals: rows={report['summary']['row_count']} findings={report['summary']['finding_count']} shown={report['summary']['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + "; ".join(f"{t}({', '.join(c)})" for t, c in report["missing_columns"].items()))
    if not report["findings"]:
        lines.append("No content claim evidence quote mismatches found.")
        return "\n".join(lines)
    lines.append("claim_id | evidence_id | issue_type | distance | source_url")
    for r in report["findings"]:
        lines.append(f"{r['claim_id']} | {r['evidence_id'] or '-'} | {r['issue_type']} | {r['distance'] if r['distance'] is not None else '-'} | {r['source_url'] or '-'}")
    return "\n".join(lines)


def _load_rows(conn: Any, check_cols: set[str], evidence_cols: set[str]) -> list[dict[str, Any]]:
    select = [
        "ccc.id AS claim_id",
        expr(check_cols, "content_id", default="NULL", alias="ccc", out="content_id"),
        expr(evidence_cols, "id", "evidence_id", default="cce.rowid", alias="cce", out="evidence_id"),
        expr(evidence_cols, "quote_text", "quote", "claim_quote", default="NULL", alias="cce", out="quote_text"),
        expr(evidence_cols, "excerpt_text", "excerpt", "snippet", "saved_snippet", "fetched_excerpt", default="NULL", alias="cce", out="excerpt_text"),
        expr(evidence_cols, "source_url", "url", default="NULL", alias="cce", out="source_url"),
    ]
    return [
        dict(r)
        for r in conn.execute(
            f"SELECT {', '.join(select)} FROM content_claim_checks ccc JOIN content_claim_evidence cce ON cce.claim_check_id = ccc.id ORDER BY ccc.id, cce.rowid"
        )
    ]


def _norm(value: str) -> str:
    table = str.maketrans("", "", string.punctuation)
    return re.sub(r"\s+", " ", value.lower().translate(table)).strip()
