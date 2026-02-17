"""Parse Claude Code conversation logs."""

import json
from pathlib import Path
from typing import Iterator
from dataclasses import dataclass
from datetime import datetime


@dataclass
class ClaudeMessage:
    session_id: str
    message_uuid: str
    project_path: str
    timestamp: datetime
    prompt_text: str

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "message_uuid": self.message_uuid,
            "project_path": self.project_path,
            "timestamp": self.timestamp.isoformat(),
            "prompt_text": self.prompt_text
        }


class ClaudeLogParser:
    def __init__(self, claude_dir: str = "~/.claude"):
        self.claude_dir = Path(claude_dir).expanduser()
        self.history_file = self.claude_dir / "history.jsonl"
        self.projects_dir = self.claude_dir / "projects"

    def parse_global_history(self) -> Iterator[ClaudeMessage]:
        """Parse the global history.jsonl for quick access to all prompts."""
        if not self.history_file.exists():
            return

        with open(self.history_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    # Global history has: display, timestamp, project, sessionId
                    if "display" in entry and entry.get("display"):
                        yield ClaudeMessage(
                            session_id=entry.get("sessionId", "unknown"),
                            message_uuid=f"{entry.get('sessionId', 'unknown')}_{entry.get('timestamp', 0)}",
                            project_path=entry.get("project", ""),
                            timestamp=datetime.fromtimestamp(entry["timestamp"] / 1000),
                            prompt_text=entry["display"]
                        )
                except json.JSONDecodeError:
                    continue

    def parse_session_file(self, session_path: Path) -> Iterator[ClaudeMessage]:
        """Parse a specific session JSONL file for full conversation details."""
        if not session_path.exists():
            return

        with open(session_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    # Session files have: type, message, uuid, timestamp, sessionId, cwd
                    if entry.get("type") == "user" and "message" in entry:
                        content = entry["message"].get("content", "")
                        if isinstance(content, str) and content:
                            yield ClaudeMessage(
                                session_id=entry.get("sessionId", "unknown"),
                                message_uuid=entry.get("uuid", "unknown"),
                                project_path=entry.get("cwd", ""),
                                timestamp=datetime.fromisoformat(
                                    entry["timestamp"].replace("Z", "+00:00")
                                ),
                                prompt_text=content
                            )
                except (json.JSONDecodeError, KeyError, ValueError):
                    continue

    def get_messages_since(self, since: datetime) -> Iterator[ClaudeMessage]:
        """Get all user messages since a given timestamp."""
        for msg in self.parse_global_history():
            if msg.timestamp >= since:
                yield msg

    def get_messages_for_project(self, project_path: str) -> Iterator[ClaudeMessage]:
        """Get all messages for a specific project."""
        for msg in self.parse_global_history():
            if msg.project_path == project_path:
                yield msg

    def get_recent_sessions(self, limit: int = 10) -> list[str]:
        """Get the most recent session IDs."""
        sessions = {}
        for msg in self.parse_global_history():
            if msg.session_id not in sessions:
                sessions[msg.session_id] = msg.timestamp
            else:
                sessions[msg.session_id] = max(sessions[msg.session_id], msg.timestamp)

        sorted_sessions = sorted(sessions.items(), key=lambda x: x[1], reverse=True)
        return [s[0] for s in sorted_sessions[:limit]]


def get_prompts_around_timestamp(
    timestamp: datetime,
    window_minutes: int = 30,
    claude_dir: str = "~/.claude"
) -> list[ClaudeMessage]:
    """Get prompts within a time window around a given timestamp.

    Useful for finding Claude prompts related to a specific commit.
    """
    from datetime import timedelta

    parser = ClaudeLogParser(claude_dir)
    start = timestamp - timedelta(minutes=window_minutes)
    end = timestamp + timedelta(minutes=window_minutes)

    return [
        msg for msg in parser.parse_global_history()
        if start <= msg.timestamp <= end
    ]
