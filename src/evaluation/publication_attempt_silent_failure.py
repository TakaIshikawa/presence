"""Flag failed publication attempts without actionable diagnostics."""
from __future__ import annotations

from typing import Any

from ._batch_report_common import clean, empty_state, json_dumps, now_value, positive

ARTIFACT_TYPE = "publication_attempt_silent_failure"
DEFAULT_LIMIT = 50
DIAGNOSTIC_FIELDS = ("error_code", "response_code", "diagnostic_message")


def build_publication_attempt_silent_failure_report(rows: list[dict[str, Any]], *, limit: int = DEFAULT_LIMIT, now: Any = None) -> dict[str, Any]:
    positive("limit", limit)
    findings = []
    for row in rows:
        if clean(row.get("status")).lower() not in {"failed", "failure", "error"}:
            continue
        missing = [field for field in DIAGNOSTIC_FIELDS if not clean(row.get(field) or row.get(field.replace("diagnostic_", "")))]
        if len(missing) == len(DIAGNOSTIC_FIELDS):
            findings.append({"attempt_id": clean(row.get("attempt_id") or row.get("id")), "content_id": clean(row.get("content_id")), "platform": clean(row.get("platform") or row.get("channel"), "unknown"), "attempted_at": clean(row.get("attempted_at") or row.get("created_at")), "missing_fields": missing})
    findings.sort(key=lambda f: (f["attempted_at"], f["attempt_id"]))
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_value(now).isoformat(), "summary": {"attempt_count": len(rows), "finding_count": len(findings)}, "findings": findings[:limit], "empty_state": empty_state(findings, "No silent publication failures found.")}


def format_publication_attempt_silent_failure_json(report: dict[str, Any]) -> str:
    return json_dumps(report)
