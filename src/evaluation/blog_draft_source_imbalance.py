"""Detect blog drafts that over-rely on one source, author, or domain."""

from __future__ import annotations

from collections import Counter
import json
import sqlite3
from typing import Any
from urllib.parse import urlparse


DEFAULT_CONCENTRATION_THRESHOLD = 0.6
DEFAULT_MIN_SOURCES = 3


def build_blog_draft_source_imbalance_report(
    db_or_conn: Any,
    *,
    concentration_threshold: float = DEFAULT_CONCENTRATION_THRESHOLD,
    min_sources: int = DEFAULT_MIN_SOURCES,
) -> dict[str, Any]:
    if not 0 < concentration_threshold <= 1:
        raise ValueError("concentration_threshold must be between 0 and 1")
    if min_sources <= 0:
        raise ValueError("min_sources must be positive")
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    filters = {
        "concentration_threshold": concentration_threshold,
        "min_sources": min_sources,
    }
    if "blog_draft_sources" not in schema:
        return _report(filters, [], missing_tables=["blog_draft_sources"])
    rows = [dict(row) for row in conn.execute("SELECT * FROM blog_draft_sources").fetchall()]
    by_draft: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_draft.setdefault(_clean(row.get("draft_id") or row.get("blog_draft_id")), []).append(row)
    findings = []
    for draft_id, items in by_draft.items():
        if len(items) < min_sources:
            continue
        finding = _finding(draft_id, items, concentration_threshold)
        if finding["severity"] != "healthy":
            findings.append(finding)
    findings.sort(key=lambda item: (-item["max_concentration"], item["draft_id"]))
    return _report(filters, findings)


def format_blog_draft_source_imbalance_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_blog_draft_source_imbalance_text(report: dict[str, Any]) -> str:
    lines = [
        "Blog Draft Source Imbalance",
        f"Filters: threshold={report['filters']['concentration_threshold']} min_sources={report['filters']['min_sources']}",
        f"Totals: findings={report['totals']['finding_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append("No imbalanced blog drafts found.")
        return "\n".join(lines)
    for item in report["findings"]:
        lines.append(
            f"  - draft={item['draft_id']} severity={item['severity']} "
            f"dominant={item['dominant_dimension']}:{item['dominant_value']} "
            f"share={item['max_concentration']:.2f}"
        )
    return "\n".join(lines)


def _finding(draft_id: str, items: list[dict[str, Any]], threshold: float) -> dict[str, Any]:
    dimensions = {
        "source": Counter(_clean(item.get("source_id") or item.get("source")) or _clean(item.get("url")) for item in items),
        "author": Counter(_clean(item.get("author")) or "unknown" for item in items),
        "domain": Counter(_domain(item.get("url") or item.get("domain")) for item in items),
    }
    best_dim = "source"
    best_value = ""
    best_count = 0
    for dim, counts in dimensions.items():
        value, count = sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))[0]
        if count > best_count:
            best_dim, best_value, best_count = dim, value, count
    share = best_count / len(items)
    severity = "high" if share >= max(0.85, threshold) else "medium" if share >= threshold else "healthy"
    return {
        "draft_id": draft_id,
        "source_count": len(items),
        "dominant_dimension": best_dim,
        "dominant_value": best_value,
        "dominant_count": best_count,
        "max_concentration": round(share, 4),
        "severity": severity,
        "concentrations": {
            dim: [
                {"value": value, "count": count, "share": round(count / len(items), 4)}
                for value, count in sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))
            ]
            for dim, counts in dimensions.items()
        },
    }


def _report(filters: dict[str, Any], findings: list[dict[str, Any]], *, missing_tables: list[str] | None = None) -> dict[str, Any]:
    return {
        "artifact_type": "blog_draft_source_imbalance",
        "filters": filters,
        "totals": {"finding_count": len(findings)},
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


def _domain(value: Any) -> str:
    text = _clean(value).lower()
    parsed = urlparse(text if "://" in text else f"https://{text}")
    return parsed.netloc.removeprefix("www.") or text or "unknown"
