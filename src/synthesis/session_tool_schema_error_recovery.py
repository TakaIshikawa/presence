"""Session tool schema error recovery analyzer."""

from __future__ import annotations

import re
from typing import Any, Mapping


ERROR_TYPES = (
    "invalid_json",
    "missing_required_parameter",
    "wrong_parameter_type",
    "unknown_tool",
    "schema_validation_failure",
)


def analyze_session_tool_schema_error_recovery(
    records: object,
    *,
    recovery_window: int = 3,
) -> dict[str, Any]:
    """Track malformed tool calls and whether nearby retries recover."""
    if recovery_window <= 0:
        raise ValueError("recovery_window must be positive")
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session/tool-call dictionaries")

    events = _flatten_events(records)
    schema_error_count = 0
    recovered_count = 0
    unrecovered_count = 0
    error_type_counts = {error_type: 0 for error_type in ERROR_TYPES}
    examples: list[dict[str, Any]] = []

    for index, event in enumerate(events):
        error_type = _schema_error_type(event)
        if error_type is None:
            continue

        schema_error_count += 1
        error_type_counts[error_type] += 1
        recovered = _find_recovery(events, index, recovery_window)

        if recovered is None:
            unrecovered_count += 1
        else:
            recovered_count += 1

        if len(examples) < 5:
            examples.append(
                {
                    "session_id": event.get("session_id"),
                    "turn_index": _turn_index(event, index),
                    "tool": _tool_name(event),
                    "error_type": error_type,
                    "recovered": recovered is not None,
                    "recovery_turn_index": recovered.get("turn_index") if recovered else None,
                }
            )

    return {
        "schema_error_count": schema_error_count,
        "recovered_count": recovered_count,
        "unrecovered_count": unrecovered_count,
        "recovery_rate_percent": _percent(recovered_count, schema_error_count),
        "error_type_counts": error_type_counts,
        "examples": examples,
    }


def _flatten_events(records: list[object]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for record_index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue
        session_id = record.get("session_id")
        nested = record.get("tool_calls")
        if not isinstance(nested, list):
            nested = record.get("messages")
        if isinstance(nested, list):
            for item_index, item in enumerate(nested):
                if isinstance(item, Mapping):
                    event = dict(item)
                    event.setdefault("session_id", session_id)
                    event.setdefault("turn_index", item.get("turn_index", item_index))
                    event.setdefault("_order", (record_index, item_index))
                    events.append(event)
        else:
            event = dict(record)
            event.setdefault("turn_index", record_index)
            event.setdefault("_order", (record_index, 0))
            events.append(event)
    return events


def _schema_error_type(event: Mapping[str, Any]) -> str | None:
    text = _combined_text(event).lower()
    if not _looks_like_error(event, text):
        return None
    if any(term in text for term in ("invalid json", "jsondecodeerror", "malformed json", "could not parse json")):
        return "invalid_json"
    if any(term in text for term in ("wrong type", "invalid type", "expected type", "must be a ", "must be an ", "type_error")):
        return "wrong_parameter_type"
    if any(term in text for term in ("missing required", "required parameter", "field required", "missing parameter")):
        return "missing_required_parameter"
    if any(term in text for term in ("unknown tool", "tool not found", "unrecognized tool", "no such tool")):
        return "unknown_tool"
    if any(term in text for term in ("schema validation", "validationerror", "validation error", "does not match schema", "invalid arguments")):
        return "schema_validation_failure"
    return None


def _find_recovery(
    events: list[dict[str, Any]],
    error_index: int,
    recovery_window: int,
) -> dict[str, Any] | None:
    error_event = events[error_index]
    error_turn = _turn_index(error_event, error_index)
    for index, event in enumerate(events[error_index + 1 :], start=error_index + 1):
        if _turn_index(event, index) - error_turn > recovery_window:
            break
        if _same_intent(error_event, event) and _event_succeeded(event):
            return event
    return None


def _same_intent(error_event: Mapping[str, Any], candidate: Mapping[str, Any]) -> bool:
    error_tool = _tool_name(error_event)
    candidate_tool = _tool_name(candidate)
    if error_tool and candidate_tool and error_tool == candidate_tool:
        return True
    error_command = _normalized_command(error_event)
    candidate_command = _normalized_command(candidate)
    if error_command and candidate_command:
        return error_command == candidate_command
    return False


def _event_succeeded(event: Mapping[str, Any]) -> bool:
    if _schema_error_type(event) is not None:
        return False
    for key in ("success", "ok", "succeeded"):
        value = event.get(key)
        if isinstance(value, bool):
            return value
    status = str(event.get("status", "")).lower()
    if status in {"success", "succeeded", "completed", "ok"}:
        return True
    if status in {"failed", "failure", "error"}:
        return False
    exit_code = event.get("exit_code")
    if isinstance(exit_code, int) and not isinstance(exit_code, bool):
        return exit_code == 0
    text = _combined_text(event).lower()
    return bool(text) and not any(term in text for term in ("error", "failed", "exception"))


def _looks_like_error(event: Mapping[str, Any], text: str) -> bool:
    if str(event.get("status", "")).lower() in {"error", "failed", "failure"}:
        return True
    if event.get("success") is False or event.get("ok") is False:
        return True
    return any(
        term in text
        for term in (
            "error",
            "invalid",
            "missing",
            "required",
            "expected",
            "must be",
            "schema",
            "unknown tool",
        )
    )


def _normalized_command(event: Mapping[str, Any]) -> str:
    command = _command_text(event).lower()
    if not command:
        return ""
    command = re.sub(r"\s+", " ", command)
    command = re.sub(r"\{.*\}", "{...}", command)
    return command.strip()


def _command_text(event: Mapping[str, Any]) -> str:
    for key in ("command", "cmd", "input", "arguments"):
        value = event.get(key)
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, Mapping):
            command = value.get("cmd") or value.get("command")
            if isinstance(command, str):
                return command.strip()
    return ""


def _combined_text(event: Mapping[str, Any]) -> str:
    parts: list[str] = [_command_text(event)]
    for key in ("stdout", "stderr", "output", "result", "error", "message", "content"):
        value = event.get(key)
        if isinstance(value, str):
            parts.append(value)
        elif isinstance(value, Mapping):
            parts.extend(str(v) for v in value.values() if isinstance(v, str))
    return "\n".join(parts)


def _tool_name(event: Mapping[str, Any]) -> str:
    for key in ("tool", "tool_name", "name"):
        value = event.get(key)
        if isinstance(value, str):
            return value
    return ""


def _turn_index(event: Mapping[str, Any], fallback: int) -> int:
    value = event.get("turn_index")
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return fallback


def _percent(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator * 100, 2)
