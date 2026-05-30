"""Measure publication provider latency variability."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Any

from ._report_utils import clean, connection, expr, json_dumps, now_iso, positive, schema, to_float, dt


ARTIFACT_TYPE = "publication_attempt_provider_latency_jitter"
DEFAULT_LIMIT = 50
DEFAULT_LOOKBACK_DAYS = 7
DEFAULT_JITTER_THRESHOLD = 2.0


def build_publication_attempt_provider_latency_jitter_report(rows: list[dict[str, Any]], *, lookback_days: int = DEFAULT_LOOKBACK_DAYS, jitter_threshold: float = DEFAULT_JITTER_THRESHOLD, limit: int = DEFAULT_LIMIT, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None, now: datetime | None = None) -> dict[str, Any]:
    positive("lookback_days", lookback_days); positive("jitter_threshold", jitter_threshold); positive("limit", limit)
    generated = now or datetime.now(timezone.utc); cutoff = generated - timedelta(days=lookback_days); groups: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in rows:
        t = dt(row.get("attempted_at"))
        if t and t < cutoff: continue
        latency = to_float(row.get("latency_ms"))
        if latency is not None: groups[(clean(row.get("platform"), "unknown"), clean(row.get("provider"), "unknown"))].append(latency)
    findings = []
    for (platform, provider), vals in groups.items():
        vals = sorted(vals); avg = mean(vals); p95 = vals[min(len(vals) - 1, int(len(vals) * 0.95))]
        jitter = round((max(vals) / avg), 4) if avg else 0
        if jitter >= jitter_threshold:
            findings.append({"platform": platform, "provider": provider, "attempt_count": len(vals), "avg_ms": round(avg, 2), "min_ms": min(vals), "max_ms": max(vals), "p95_ms": p95, "jitter_ratio": jitter})
    findings.sort(key=lambda r: (-r["jitter_ratio"], -r["attempt_count"], r["platform"], r["provider"]))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_iso(generated), "filters": {"lookback_days": lookback_days, "limit": limit}, "thresholds": {"jitter_threshold": jitter_threshold}, "summary": {"attempt_count": len(rows), "group_count": len(groups), "finding_count": len(findings), "shown_count": len(shown)}, "findings": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}}


def build_publication_attempt_provider_latency_jitter_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); s = schema(conn); table = "publication_attempts"; missing_tables = [] if table in s else [table]; missing_columns = {}; rows = []
    if table in s:
        c = s[table]; req = {"latency_ms"}
        if not req.issubset(c): missing_columns[table] = sorted(req - c)
        else:
            select = [
                expr(c, "platform", default="'unknown'", out="platform"),
                expr(c, "provider", default="'unknown'", out="provider"),
                expr(c, "attempted_at", "created_at", default="NULL", out="attempted_at"),
                "latency_ms",
            ]
            rows = [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY rowid")]
    return build_publication_attempt_provider_latency_jitter_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_publication_attempt_provider_latency_jitter_json(report: dict[str, Any]) -> str: return json_dumps(report)


def format_publication_attempt_provider_latency_jitter_text(report: dict[str, Any]) -> str:
    lines = ["Publication Attempt Provider Latency Jitter", f"Generated: {report['generated_at']}", f"Totals: attempts={report['summary']['attempt_count']} groups={report['summary']['group_count']} findings={report['summary']['finding_count']}"]
    if report["missing_tables"]: lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]: lines.append("No publication attempt provider latency jitter found."); return "\n".join(lines)
    for r in report["findings"]: lines.append(f"- {r['platform']}/{r['provider']} attempts={r['attempt_count']} avg={r['avg_ms']} p95={r['p95_ms']} jitter={r['jitter_ratio']}")
    return "\n".join(lines)
