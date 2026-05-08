"""Session command preflight analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


READ_COMMANDS = ("cat", "sed", "nl", "head", "tail", "less", "rg", "grep", "find", "ls", "git status", "git diff")
TEST_COMMANDS = ("pytest", "uv run pytest", "npm test", "pnpm test", "yarn test", "go test", "cargo test")
EDIT_EVENTS = {"edit", "write", "patch"}


@dataclass(frozen=True)
class SessionCommandEvent:
    turn_index: int
    event_type: str
    command: str | None = None
    file_path: str | None = None


@dataclass(frozen=True)
class SessionCommandPreflightReport:
    total_commands: int
    pre_edit_commands: int
    post_edit_commands: int
    preflight_commands: int
    first_edit_turn: int | None
    preflight_quality: str
    insights: tuple[str, ...]


def analyze_session_command_preflight(
    events: Sequence[SessionCommandEvent],
) -> SessionCommandPreflightReport:
    """Measure whether useful read/list/test commands happened before editing."""

    _validate_events(events)
    first_edit_turn = next((event.turn_index for event in events if event.event_type in EDIT_EVENTS), None)
    command_events = [event for event in events if event.event_type == "command"]

    if first_edit_turn is None:
        pre_edit = command_events
        post_edit: list[SessionCommandEvent] = []
    else:
        pre_edit = [event for event in command_events if event.turn_index < first_edit_turn]
        post_edit = [event for event in command_events if event.turn_index >= first_edit_turn]

    preflight_count = sum(1 for event in pre_edit if _is_preflight_command(event.command or ""))
    quality = _classify_quality(preflight_count, len(pre_edit), first_edit_turn)

    return SessionCommandPreflightReport(
        total_commands=len(command_events),
        pre_edit_commands=len(pre_edit),
        post_edit_commands=len(post_edit),
        preflight_commands=preflight_count,
        first_edit_turn=first_edit_turn,
        preflight_quality=quality,
        insights=_build_insights(first_edit_turn, preflight_count, len(pre_edit), len(command_events)),
    )


def _validate_events(events: Sequence[SessionCommandEvent]) -> None:
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple of SessionCommandEvent instances")
    last_turn = -1
    for index, event in enumerate(events):
        if not isinstance(event, SessionCommandEvent):
            raise ValueError(f"events[{index}] must be a SessionCommandEvent")
        if not isinstance(event.turn_index, int) or isinstance(event.turn_index, bool) or event.turn_index < 0:
            raise ValueError("turn_index must be a non-negative integer")
        if event.turn_index < last_turn:
            raise ValueError("events must be ordered by turn_index")
        if event.event_type not in {"command", *EDIT_EVENTS}:
            raise ValueError("event_type must be 'command', 'edit', 'write', or 'patch'")
        if event.event_type == "command" and (not isinstance(event.command, str) or not event.command.strip()):
            raise ValueError("command events require a non-empty command")
        if event.command is not None and not isinstance(event.command, str):
            raise ValueError("command must be a string or None")
        if event.file_path is not None and not isinstance(event.file_path, str):
            raise ValueError("file_path must be a string or None")
        last_turn = event.turn_index


def _is_preflight_command(command: str) -> bool:
    normalized = " ".join(command.lower().split())
    return any(normalized.startswith(prefix) or prefix in normalized for prefix in (*READ_COMMANDS, *TEST_COMMANDS))


def _classify_quality(preflight_count: int, pre_edit_count: int, first_edit_turn: int | None) -> str:
    if first_edit_turn is None:
        return "none" if preflight_count == 0 else "strong"
    if preflight_count == 0:
        return "none"
    if preflight_count >= 2 or (preflight_count >= 1 and pre_edit_count >= 2):
        return "strong"
    return "thin"


def _build_insights(
    first_edit_turn: int | None,
    preflight_count: int,
    pre_edit_count: int,
    total_commands: int,
) -> tuple[str, ...]:
    if total_commands == 0:
        return ("No shell commands were recorded.",)
    if first_edit_turn is not None and preflight_count == 0:
        return ("Edits happened before any read/list/test preflight command.",)
    if preflight_count:
        return (f"Found {preflight_count} useful preflight command(s) before editing.",)
    if pre_edit_count:
        return ("Commands ran before editing, but none looked like read/list/test preflight.",)
    return ("No file edits were recorded.",)
