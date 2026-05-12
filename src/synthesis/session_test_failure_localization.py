"""Session test failure localization analyzer."""

from __future__ import annotations

import re
from typing import Any, Mapping


READ_TOOLS = {
    "bash",
    "cat",
    "grep",
    "rg",
    "read",
    "glob",
    "ls",
    "functions.exec_command",
}
EDIT_TOOLS = {
    "apply_patch",
    "edit",
    "write",
    "multi_edit",
    "functions.apply_patch",
}


def analyze_session_test_failure_localization(records: object) -> dict[str, Any]:
    """Measure whether failed test output was localized before edits resumed."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session/tool-call dictionaries")

    events = _flatten_events(records)
    failed_test_commands = 0
    localized_failures = 0
    unlocalized_failures = 0
    localization_signal_counts = {
        "failing_file": 0,
        "test_name": 0,
        "traceback_file": 0,
        "assertion_text": 0,
    }
    examples: list[dict[str, Any]] = []

    for index, event in enumerate(events):
        if not _is_failed_test_command(event):
            continue

        failure = _extract_failure_signals(event)
        failed_test_commands += 1
        signal_hits: set[str] = set()

        for followup in events[index + 1 :]:
            if _is_edit_event(followup):
                break
            if not _is_read_or_search_event(followup):
                continue
            signal_hits.update(_matched_signal_types(followup, failure))

        if signal_hits:
            localized_failures += 1
            for signal in signal_hits:
                localization_signal_counts[signal] += 1
        else:
            unlocalized_failures += 1

        if len(examples) < 5:
            examples.append(
                {
                    "session_id": event.get("session_id"),
                    "command": _command_text(event),
                    "localized": bool(signal_hits),
                    "signals": sorted(signal_hits),
                    "failing_files": sorted(failure["failing_files"])[:3],
                    "test_names": sorted(failure["test_names"])[:3],
                }
            )

    return {
        "failed_test_commands": failed_test_commands,
        "localized_failures": localized_failures,
        "unlocalized_failures": unlocalized_failures,
        "localization_rate_percent": _percent(localized_failures, failed_test_commands),
        "localization_signal_counts": localization_signal_counts,
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
                    event.setdefault("_order", (record_index, item_index))
                    events.append(event)
        else:
            event = dict(record)
            event.setdefault("_order", (record_index, 0))
            events.append(event)
    return events


def _is_failed_test_command(event: Mapping[str, Any]) -> bool:
    command = _command_text(event).lower()
    if not re.search(r"\b(pytest|tox|nox|unittest|npm\s+test|pnpm\s+test|yarn\s+test)\b", command):
        return False
    if _exit_code(event) == 0 or _status_success(event):
        return False
    text = _combined_text(event).lower()
    return (
        _exit_code(event) not in (None, 0)
        or "failed" in text
        or "traceback" in text
        or "assertionerror" in text
    )


def _extract_failure_signals(event: Mapping[str, Any]) -> dict[str, set[str]]:
    output = _combined_text(event)
    paths = set(re.findall(r"(?:(?:\.{1,2}/)?[\w./-]+)?(?:tests?|src|app)/[\w./-]+\.py", output))
    short_py_paths = set(re.findall(r"\b[\w.-]+\.py\b", output))
    test_ids = set(re.findall(r"((?:[\w./-]+\.py::)?test_[\w\[\].:-]+)", output))
    test_names = {test_id.split("::")[-1].split("[")[0] for test_id in test_ids}
    assertion_snippets = _assertion_snippets(output)

    return {
        "failing_files": paths | {path for path in short_py_paths if path.startswith("test_")},
        "traceback_files": set(re.findall(r'File "([^"]+\.py)"', output)) | paths,
        "test_names": {name for name in test_names if name},
        "assertion_texts": assertion_snippets,
    }


def _assertion_snippets(output: str) -> set[str]:
    snippets: set[str] = set()
    for line in output.splitlines():
        stripped = line.strip()
        normalized = stripped.lstrip(">E ")
        lower = normalized.lower()
        if not normalized:
            continue
        if lower.startswith("assert ") or "assertionerror" in lower:
            snippets.add(_meaningful_snippet(normalized))
    return {snippet for snippet in snippets if len(snippet) >= 6}


def _matched_signal_types(event: Mapping[str, Any], failure: Mapping[str, set[str]]) -> set[str]:
    text = _combined_text(event).lower()
    hits: set[str] = set()
    if _contains_any(text, failure["failing_files"]):
        hits.add("failing_file")
    if _contains_any(text, failure["test_names"]):
        hits.add("test_name")
    if _contains_any(text, failure["traceback_files"]):
        hits.add("traceback_file")
    if _contains_assertion_text(text, failure["assertion_texts"]):
        hits.add("assertion_text")
    return hits


def _is_read_or_search_event(event: Mapping[str, Any]) -> bool:
    tool = _tool_name(event).lower()
    command = _command_text(event).lower()
    return tool in READ_TOOLS or command.startswith(("rg ", "grep ", "sed ", "cat ", "nl ", "ls "))


def _is_edit_event(event: Mapping[str, Any]) -> bool:
    tool = _tool_name(event).lower()
    text = _combined_text(event).lower()
    command = _command_text(event).lower()
    return tool in EDIT_TOOLS or "*** begin patch" in text or command.startswith(("python - <<", "perl -0pi", "sed -i"))


def _contains_any(haystack: str, needles: set[str]) -> bool:
    return any(needle and needle.lower() in haystack for needle in needles)


def _contains_assertion_text(haystack: str, assertions: set[str]) -> bool:
    for assertion in assertions:
        if assertion in haystack:
            return True
        tokens = [token for token in assertion.split() if len(token) >= 4 and not token.isdigit()]
        if any(token in haystack for token in tokens):
            return True
    return False


def _meaningful_snippet(text: str) -> str:
    words = re.findall(r"[\w./:-]+", text.lower())
    return " ".join(words[:8])


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
    for key in ("stdout", "stderr", "output", "result", "error", "message", "content", "input"):
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


def _exit_code(event: Mapping[str, Any]) -> int | None:
    value = event.get("exit_code")
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _status_success(event: Mapping[str, Any]) -> bool:
    status = str(event.get("status", "")).lower()
    return status in {"success", "succeeded", "completed", "ok"}


def _percent(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator * 100, 2)
