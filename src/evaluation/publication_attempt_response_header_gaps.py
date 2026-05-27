"""Find publication attempts missing stored response headers."""
from __future__ import annotations
from collections import Counter, defaultdict
from typing import Any
from ._batch_report_common import *

ARTIFACT_TYPE = "publication_attempt_response_header_gaps"
DEFAULT_LIMIT = 50
REQUEST_ID = ("x-request-id", "request-id", "x-correlation-id", "trace-id")
RATE = ("x-ratelimit-limit", "x-rate-limit-limit", "ratelimit-limit", "x-ratelimit-remaining", "retry-after")


def _headers(row: dict[str, Any]) -> tuple[dict[str, str], bool]:
    raw = row.get("response_headers") or row.get("raw_response") or row.get("raw_metadata") or row.get("metadata")
    if isinstance(raw, dict):
        obj = raw
    elif clean(raw):
        try:
            obj = json.loads(str(raw))
        except (TypeError, ValueError):
            return {}, True
        if isinstance(obj, dict) and isinstance(obj.get("headers"), dict):
            obj = obj["headers"]
        if not isinstance(obj, dict):
            return {}, True
    else:
        obj = {}
    return {str(k).lower(): clean(v) for k, v in obj.items()}, False


def _rate_limited(row: dict[str, Any]) -> bool:
    text = " ".join(clean(row.get(k)).lower() for k in ("status", "error", "failure_reason", "message"))
    return to_int(row.get("status_code")) == 429 or "rate" in text and "limit" in text or "429" in text


def build_publication_attempt_response_header_gaps_report(rows: list[dict[str, Any]], *, limit: int = DEFAULT_LIMIT, missing_tables=None, missing_columns=None, now=None):
    positive("limit", limit)
    findings = []
    by_provider = defaultdict(Counter)
    for r in rows:
        headers, invalid = _headers(r)
        provider = clean(r.get("provider") or r.get("platform"), "unknown")
        reasons = []
        if invalid: reasons.append("invalid_header_json")
        if not headers: reasons.append("missing_response_headers")
        if not any(k in headers and headers[k] for k in REQUEST_ID): reasons.append("missing_request_id")
        if _rate_limited(r):
            if not any(k in headers and headers[k] for k in ("retry-after",)): reasons.append("missing_retry_after")
            if not any(k in headers and headers[k] for k in RATE): reasons.append("missing_rate_limit_header")
        for reason in dict.fromkeys(reasons):
            by_provider[provider][reason] += 1
            findings.append({"attempt_id": r.get("attempt_id") or r.get("id"), "provider": provider, "platform": clean(r.get("platform")), "status_code": to_int(r.get("status_code")), "reason": reason, "recommended_action": "persist provider response headers for failure debugging"})
    findings.sort(key=lambda f: (f["provider"], f["reason"], str(f["attempt_id"])))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_iso(now), "filters": {"limit": limit}, "summary": {"attempt_count": len(rows), "gap_count": len(findings), "shown": len(shown), "by_provider": {p: dict(sorted(c.items())) for p, c in sorted(by_provider.items())}}, "response_header_gaps": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}, "empty_state": empty_state(findings, "No publication attempt response header gaps found.", schema_gap=bool(missing_tables or missing_columns))}


def build_publication_attempt_response_header_gaps_report_from_db(db_or_conn: Any, **kw):
    conn = connection(db_or_conn); s = schema(conn); mt = []; mc = {}; rows = []
    table = "publication_attempts" if "publication_attempts" in s else ("publication_attempt_logs" if "publication_attempt_logs" in s else None)
    if not table:
        mt.append("publication_attempts|publication_attempt_logs")
    else:
        c = s[table]
        if not ({"response_headers", "raw_response", "raw_metadata", "metadata"} & c): mc[table] = ["response_headers|raw_response|raw_metadata"]
        rows = load_table(conn, table, c, {"attempt_id": ("id", "attempt_id"), "provider": ("provider", "platform"), "platform": ("platform",), "status_code": ("status_code", "http_status"), "status": ("status",), "error": ("error", "error_message"), "failure_reason": ("failure_reason", "reason"), "message": ("message",), "response_headers": ("response_headers",), "raw_response": ("raw_response",), "raw_metadata": ("raw_metadata", "metadata")})
    return build_publication_attempt_response_header_gaps_report(rows, missing_tables=mt, missing_columns=mc, **kw)


def format_publication_attempt_response_header_gaps_json(r): return json_dumps(r)


def format_publication_attempt_response_header_gaps_text(r):
    s = r["summary"]; lines = ["Publication Attempt Response Header Gaps", f"Generated: {r['generated_at']}", f"Totals: attempts={s['attempt_count']} gaps={s['gap_count']} shown={s['shown']}"]
    if r["missing_tables"]: lines.append("Missing tables: " + ", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: " + flatten_missing(r["missing_columns"]))
    if not r["response_header_gaps"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines += ["", "attempt_id | provider | reason"]
    for f in r["response_header_gaps"]: lines.append(f"{f['attempt_id']} | {f['provider']} | {f['reason']}")
    return "\n".join(lines)
