"""Terminal session cleanup analyzer for workflow reports."""

from __future__ import annotations

import re
from typing import Any, Mapping


URL_RE = re.compile(r"https?://[^\s)]+")


def analyze_terminal_session_cleanup(records: object) -> dict[str, Any]:
    """Detect long-running terminal sessions left unresolved at final answer time."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of terminal event dictionaries")

    sessions: dict[str, dict[str, Any]] = {}
    final_texts: list[str] = []
    examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue
        final_text = _string(record.get("final_answer")) or _string(record.get("final_message"))
        if final_text:
            final_texts.append(final_text)

        if not _is_terminal_event(record):
            continue
        action = _action(record)
        if action not in {"open", "close", "running"}:
            continue

        session_id = _session_id(record, index)
        session = sessions.setdefault(
            session_id,
            {"session_id": session_id, "command": _string(record.get("command")), "opened": False, "closed": False},
        )
        if not session["command"]:
            session["command"] = _string(record.get("command"))
        if action in {"open", "running"}:
            session["opened"] = True
        if action == "close":
            session["closed"] = True

    opened = [session for session in sessions.values() if session["opened"]]
    running = [session for session in opened if not session["closed"]]
    final_answer_mentioned_sessions = len(running) if running and _mentions_running_server(final_texts) else 0
    for session in running[:5]:
        examples.append({"session_id": session["session_id"], "command": session["command"], "reason": "still_running"})

    return {
        "opened_session_count": len(opened),
        "closed_session_count": sum(1 for session in opened if session["closed"]),
        "still_running_session_count": len(running),
        "final_answer_mentioned_sessions": final_answer_mentioned_sessions,
        "unresolved_server_risk_count": max(0, len(running) - final_answer_mentioned_sessions),
        "examples": examples,
    }


def _is_terminal_event(record: Mapping[str, Any]) -> bool:
    tool = _string(record.get("tool")).lower()
    return any(term in tool for term in ("terminal", "exec", "shell", "command"))


def _action(record: Mapping[str, Any]) -> str:
    text = " ".join(
        value.lower()
        for value in (_string(record.get("action")), _string(record.get("status")))
        if value
    )
    if any(term in text for term in ("close", "closed", "stop", "stopped", "exit", "exited", "complete", "completed")):
        return "close"
    if any(term in text for term in ("running", "started", "start", "open", "session")):
        return "running" if "running" in text else "open"
    return ""


def _mentions_running_server(texts: list[str]) -> bool:
    combined = "\n".join(texts).lower()
    return bool(URL_RE.search(combined)) or "running server" in combined or "dev server" in combined


def _session_id(record: Mapping[str, Any], fallback: int) -> str:
    value = record.get("session_id")
    return value.strip() if isinstance(value, str) and value.strip() else str(fallback)


def _string(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""
