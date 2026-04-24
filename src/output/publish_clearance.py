"""Final deterministic clearance checks for queued publishing."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from output.attribution_guard import check_publication_attribution_guard
from synthesis.alt_text_guard import validate_alt_text


UNSUPPORTED_CLAIMS = "unsupported_claims"
PERSONA_GUARD_FAILED = "persona_guard_failed"
MISSING_ATTRIBUTION = "missing_attribution"
ALT_TEXT_FAILED = "alt_text_failed"


@dataclass(frozen=True)
class PublicationClearanceResult:
    """Single publish/no-publish result for deterministic final checks."""

    passed: bool
    hold_reason: str | None = None
    checks: dict[str, Any] = field(default_factory=dict)

    @property
    def blocked(self) -> bool:
        return not self.passed


def _as_dict(row: Any) -> dict | None:
    if row is None:
        return None
    if isinstance(row, dict):
        return row
    if hasattr(row, "keys"):
        return dict(row)
    return dict(row)


def _json_list(value: Any) -> list:
    if isinstance(value, list):
        return value
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _json_object(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _fetch_claim_check_summary(db: Any, content_id: int) -> dict | None:
    getter = getattr(db, "get_claim_check_summary", None)
    if callable(getter):
        return _as_dict(getter(content_id))

    conn = getattr(db, "conn", None)
    if conn is None:
        return None
    row = conn.execute(
        "SELECT * FROM content_claim_checks WHERE content_id = ?",
        (content_id,),
    ).fetchone()
    return _as_dict(row)


def _fetch_persona_guard_summary(db: Any, content_id: int) -> dict | None:
    getter = getattr(db, "get_persona_guard_summary", None)
    if callable(getter):
        summary = _as_dict(getter(content_id))
    else:
        conn = getattr(db, "conn", None)
        if conn is None:
            return None
        row = conn.execute(
            "SELECT * FROM content_persona_guard WHERE content_id = ?",
            (content_id,),
        ).fetchone()
        summary = _as_dict(row)

    if not summary:
        return None
    summary["checked"] = bool(summary.get("checked"))
    summary["passed"] = bool(summary.get("passed"))
    summary["reasons"] = _json_list(summary.get("reasons"))
    summary["metrics"] = _json_object(summary.get("metrics"))
    return summary


def _claim_check_status(summary: dict | None) -> dict:
    unsupported_count = int((summary or {}).get("unsupported_count") or 0)
    return {
        "checked": bool(summary),
        "status": "unsupported_claims" if unsupported_count else "supported",
        "unsupported_count": unsupported_count,
        "supported_count": int((summary or {}).get("supported_count") or 0),
        "annotation_text": (summary or {}).get("annotation_text"),
    }


def _persona_guard_status(summary: dict | None) -> dict:
    if not summary:
        return {
            "checked": False,
            "passed": None,
            "status": "not_checked",
            "score": None,
            "reasons": [],
            "metrics": {},
        }
    return {
        "checked": bool(summary.get("checked")),
        "passed": bool(summary.get("passed")),
        "status": summary.get("status") or "unknown",
        "score": summary.get("score"),
        "reasons": summary.get("reasons") or [],
        "metrics": summary.get("metrics") or {},
    }


def _alt_text_guard_mode(mode: str | None) -> str:
    return mode if mode in {"strict", "warning"} else "strict"


def check_publication_clearance(
    db: Any,
    item: dict,
    *,
    platform_texts: dict[str, str | list[str] | tuple[str, ...]] | None = None,
    alt_text_guard_mode: str = "strict",
) -> PublicationClearanceResult:
    """Return whether a queued item is clear for platform publication.

    Hold reasons are deliberately short stable strings for ledger display.
    """
    content_id = int(item["content_id"])
    checks: dict[str, Any] = {}

    claim_check = _claim_check_status(_fetch_claim_check_summary(db, content_id))
    checks["claim_check"] = claim_check
    if claim_check["unsupported_count"] > 0:
        return PublicationClearanceResult(False, UNSUPPORTED_CLAIMS, checks)

    persona_guard = _persona_guard_status(_fetch_persona_guard_summary(db, content_id))
    checks["persona_guard"] = persona_guard
    if persona_guard["status"] == "failed" or persona_guard["passed"] is False:
        return PublicationClearanceResult(False, PERSONA_GUARD_FAILED, checks)

    texts = platform_texts or {"default": item.get("content") or ""}
    attribution_results = {}
    for platform, text in texts.items():
        result = check_publication_attribution_guard(db, content_id, text).as_dict()
        attribution_results[platform] = result
        if result["blocked"]:
            checks["attribution_guard"] = attribution_results
            return PublicationClearanceResult(False, MISSING_ATTRIBUTION, checks)
    checks["attribution_guard"] = attribution_results

    alt_text = validate_alt_text(
        item.get("image_alt_text"),
        image_prompt=item.get("image_prompt"),
        image_path=item.get("image_path"),
        content_type=item.get("content_type"),
    ).as_dict()
    checks["alt_text"] = alt_text
    if (
        _alt_text_guard_mode(alt_text_guard_mode) == "strict"
        and alt_text.get("checked")
        and not alt_text.get("passed", True)
    ):
        return PublicationClearanceResult(False, ALT_TEXT_FAILED, checks)

    return PublicationClearanceResult(True, None, checks)
