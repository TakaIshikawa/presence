"""Session patch application outcome analyzer."""

from __future__ import annotations

from typing import Any, Mapping


FAILURE_REASONS = (
    "context_mismatch",
    "grammar_format_error",
    "missing_file",
    "permission_denial",
    "unknown",
)


def analyze_session_patch_application_outcome(records: object) -> dict[str, Any]:
    """Analyze apply_patch and edit attempt outcomes from session records.

    Args:
        records: List of session or tool-call dictionaries.

    Returns:
        Deterministic aggregate metrics and up to five failure examples.

    Raises:
        ValueError: If records is not a list.
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session/tool-call dictionaries")

    total_sessions = len(
        {
            str(record.get("session_id"))
            for record in records
            if isinstance(record, Mapping) and record.get("session_id") is not None
        }
    )
    patch_attempts = 0
    successful_patches = 0
    failed_patches = 0
    failure_reason_counts = {reason: 0 for reason in FAILURE_REASONS}
    examples: list[dict[str, Any]] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        for attempt in _iter_patch_attempts(record):
            patch_attempts += 1
            if _attempt_succeeded(attempt):
                successful_patches += 1
                continue

            failed_patches += 1
            reason = _classify_failure(attempt)
            failure_reason_counts[reason] += 1
            if len(examples) < 5:
                examples.append(
                    {
                        "session_id": _session_id(record, attempt),
                        "reason": reason,
                        "tool": _tool_name(attempt),
                    }
                )

    return {
        "total_sessions": total_sessions,
        "patch_attempts": patch_attempts,
        "successful_patches": successful_patches,
        "failed_patches": failed_patches,
        "failure_rate_percent": _percent(failed_patches, patch_attempts),
        "failure_reason_counts": failure_reason_counts,
        "examples": examples,
    }


def _iter_patch_attempts(record: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    calls = record.get("tool_calls")
    if isinstance(calls, list):
        return [
            call
            for call in calls
            if isinstance(call, Mapping) and _is_patch_attempt(call)
        ]
    return [record] if _is_patch_attempt(record) else []


def _is_patch_attempt(record: Mapping[str, Any]) -> bool:
    tool = _tool_name(record).lower()
    if tool in {"apply_patch", "functions.apply_patch", "edit", "write", "multi_edit"}:
        return True
    text = _combined_text(record).lower()
    return "apply_patch" in text or "*** begin patch" in text


def _attempt_succeeded(record: Mapping[str, Any]) -> bool:
    for key in ("success", "ok", "succeeded"):
        value = record.get(key)
        if isinstance(value, bool):
            return value

    status = str(record.get("status", "")).lower()
    if status in {"success", "succeeded", "completed", "ok"}:
        return True
    if status in {"failed", "failure", "error"}:
        return False

    exit_code = record.get("exit_code")
    if isinstance(exit_code, int) and not isinstance(exit_code, bool):
        return exit_code == 0

    text = _combined_text(record).lower()
    failure_terms = ("error", "failed", "exception", "permission denied", "no such file")
    return not any(term in text for term in failure_terms)


def _classify_failure(record: Mapping[str, Any]) -> str:
    text = _combined_text(record).lower()
    if any(term in text for term in ("permission denied", "operation not permitted", "outside the sandbox")):
        return "permission_denial"
    if any(term in text for term in ("no such file", "file not found", "does not exist")):
        return "missing_file"
    if any(term in text for term in ("context", "hunk", "did not match", "patch failed", "reject")):
        return "context_mismatch"
    if any(term in text for term in ("grammar", "format", "parse", "malformed", "invalid patch")):
        return "grammar_format_error"
    return "unknown"


def _combined_text(record: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for key in ("stderr", "stdout", "output", "result", "error", "message", "content", "arguments", "input"):
        value = record.get(key)
        if isinstance(value, str):
            parts.append(value)
        elif isinstance(value, Mapping):
            parts.extend(str(v) for v in value.values() if isinstance(v, str))
    return "\n".join(parts)


def _tool_name(record: Mapping[str, Any]) -> str:
    for key in ("tool", "tool_name", "name"):
        value = record.get(key)
        if isinstance(value, str):
            return value
    return ""


def _session_id(record: Mapping[str, Any], attempt: Mapping[str, Any]) -> str | None:
    for source in (attempt, record):
        value = source.get("session_id")
        if value is not None:
            return str(value)
    return None


def _percent(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round(numerator / denominator * 100, 2)
