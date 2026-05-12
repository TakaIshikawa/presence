"""Session dependency install hygiene analyzer."""

from __future__ import annotations

import re
import shlex
from typing import Any, Mapping


MANIFEST_FILES = {
    "pyproject.toml",
    "requirements.txt",
    "requirements-dev.txt",
    "setup.py",
    "setup.cfg",
    "Pipfile",
    "package.json",
}
LOCKFILES = {
    "uv.lock",
    "poetry.lock",
    "Pipfile.lock",
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
}
DEPENDENCY_FILES = MANIFEST_FILES | LOCKFILES
EDIT_TOOLS = {"apply_patch", "edit", "write", "multi_edit", "functions.apply_patch"}


def analyze_session_dependency_install_hygiene(records: object) -> dict[str, Any]:
    """Analyze package-manager install hygiene in agent sessions."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session/tool-call dictionaries")

    events = _flatten_events(records)
    install_command_count = 0
    sessions_with_install_ids: set[str] = set()
    inspected_manifest_before_install = 0
    manifest_or_lockfile_updated = 0
    risky_global_installs = 0
    examples: list[dict[str, Any]] = []

    for index, event in enumerate(events):
        command = _command_text(event)
        install = _install_info(command)
        if install is None:
            continue

        install_command_count += 1
        session_id = str(event.get("session_id", ""))
        if session_id:
            sessions_with_install_ids.add(session_id)

        inspected_before = _has_prior_manifest_inspection(events, index, session_id)
        updated_after = _has_following_manifest_update(events, index, session_id)
        risky = _is_risky_global_install(install)

        if inspected_before:
            inspected_manifest_before_install += 1
        if updated_after:
            manifest_or_lockfile_updated += 1
        if risky:
            risky_global_installs += 1

        if len(examples) < 5:
            examples.append(
                {
                    "session_id": event.get("session_id"),
                    "command": command,
                    "manager": install["manager"],
                    "inspected_manifest_before_install": inspected_before,
                    "manifest_or_lockfile_updated": updated_after,
                    "uses_project_local_or_isolated_flags": _uses_project_local_or_isolated_flags(install),
                    "risky_global_install": risky,
                }
            )

    return {
        "install_command_count": install_command_count,
        "sessions_with_installs": len(sessions_with_install_ids),
        "inspected_manifest_before_install": inspected_manifest_before_install,
        "manifest_or_lockfile_updated": manifest_or_lockfile_updated,
        "risky_global_installs": risky_global_installs,
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


def _install_info(command: str) -> dict[str, Any] | None:
    if not command:
        return None
    try:
        tokens = shlex.split(command)
    except ValueError:
        tokens = command.split()
    if not tokens:
        return None

    manager_index = 0
    manager = tokens[0]
    if len(tokens) >= 3 and tokens[0] == "python" and tokens[1] == "-m" and tokens[2] == "pip":
        manager_index = 2
        manager = "pip"
    elif tokens[0] in {"sudo", "env"} and len(tokens) > 1:
        manager_index = 1
        manager = tokens[1]

    lower_tokens = [token.lower() for token in tokens]
    manager = manager.lower()
    args = lower_tokens[manager_index + 1 :]

    if manager == "pip" and "install" in args:
        return {"manager": "pip", "tokens": lower_tokens, "args": args}
    if manager == "uv" and ("add" in args or "sync" in args or ("pip" in args and "install" in args)):
        return {"manager": "uv", "tokens": lower_tokens, "args": args}
    if manager in {"npm", "pnpm"} and any(arg in args for arg in ("install", "i", "add")):
        return {"manager": manager, "tokens": lower_tokens, "args": args}
    if manager == "yarn" and ("add" in args or "install" in args or ("global" in args and "add" in args)):
        return {"manager": "yarn", "tokens": lower_tokens, "args": args}
    if manager == "brew" and "install" in args:
        return {"manager": "brew", "tokens": lower_tokens, "args": args}
    if manager in {"apt", "apt-get"} and "install" in args:
        return {"manager": "apt", "tokens": lower_tokens, "args": args}
    return None


def _has_prior_manifest_inspection(events: list[dict[str, Any]], install_index: int, session_id: str) -> bool:
    for event in reversed(events[:install_index]):
        if session_id and str(event.get("session_id", "")) != session_id:
            continue
        if _is_manifest_inspection(event):
            return True
    return False


def _has_following_manifest_update(events: list[dict[str, Any]], install_index: int, session_id: str) -> bool:
    for event in events[install_index + 1 :]:
        if session_id and str(event.get("session_id", "")) != session_id:
            continue
        if _install_info(_command_text(event)) is not None:
            break
        if _is_manifest_update(event):
            return True
    return False


def _is_manifest_inspection(event: Mapping[str, Any]) -> bool:
    command = _command_text(event).lower()
    text = _combined_text(event).lower()
    if not command.startswith(("cat ", "sed ", "nl ", "rg ", "grep ", "ls ", "find ", "head ", "tail ")):
        tool = _tool_name(event).lower()
        if tool not in {"read", "glob", "grep", "bash", "functions.exec_command"}:
            return False
    return _mentions_dependency_file(command) or _mentions_dependency_file(text)


def _is_manifest_update(event: Mapping[str, Any]) -> bool:
    tool = _tool_name(event).lower()
    text = _combined_text(event).lower()
    return (tool in EDIT_TOOLS or "*** begin patch" in text) and _mentions_dependency_file(text)


def _mentions_dependency_file(text: str) -> bool:
    return any(filename.lower() in text for filename in DEPENDENCY_FILES)


def _is_risky_global_install(install: Mapping[str, Any]) -> bool:
    manager = str(install["manager"])
    args = list(install["args"])
    tokens = list(install["tokens"])
    if manager in {"brew", "apt"}:
        return True
    if manager in {"npm", "pnpm"}:
        return "-g" in args or "--global" in args
    if manager == "yarn":
        return "global" in args
    if manager == "pip":
        return not _uses_project_local_or_isolated_flags(install) and not _command_mentions_virtualenv(tokens)
    return False


def _uses_project_local_or_isolated_flags(install: Mapping[str, Any]) -> bool:
    manager = str(install["manager"])
    args = list(install["args"])
    tokens = list(install["tokens"])
    if manager == "pip":
        return any(arg in args for arg in ("-r", "--requirement", "-e", "--editable", "--target", "--prefix", "--constraint", "-c"))
    if manager == "uv":
        return True
    if manager in {"npm", "pnpm"}:
        return "-g" not in args and "--global" not in args
    if manager == "yarn":
        return "global" not in args
    return _command_mentions_virtualenv(tokens)


def _command_mentions_virtualenv(tokens: list[str]) -> bool:
    joined = " ".join(tokens)
    return any(term in joined for term in (".venv", "venv/", "virtualenv", "pipenv run", "poetry run"))


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
