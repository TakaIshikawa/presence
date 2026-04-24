"""Deterministic Claude session summaries for synthesis context."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from ingestion.claude_logs import ClaudeMessage


DEFAULT_MAX_EXCERPTS = 5
DEFAULT_EXCERPT_CHARS = 180


@dataclass(frozen=True)
class ClaudeSessionSummary:
    session_id: str
    project_path: str
    started_at: datetime
    ended_at: datetime
    prompt_count: int
    prompt_excerpts: tuple[str, ...]
    message_uuids: tuple[str, ...]

    def to_prompt_context(self) -> str:
        lines = [
            f"Claude session {self.session_id}",
            f"Project: {self.project_path or 'unknown'}",
            f"Time: {self.started_at.isoformat()} to {self.ended_at.isoformat()}",
            f"Prompts: {self.prompt_count}",
            "Key prompt excerpts:",
        ]
        lines.extend(f"- {excerpt}" for excerpt in self.prompt_excerpts)
        return "\n".join(lines)

    @property
    def summary_text(self) -> str:
        return self.to_prompt_context()


def _compact_whitespace(text: str) -> str:
    return " ".join(text.split())


def _excerpt(text: str, max_chars: int) -> str:
    compact = _compact_whitespace(text)
    if len(compact) <= max_chars:
        return compact
    return compact[: max(0, max_chars - 3)].rstrip() + "..."


def build_session_summaries(
    messages: Iterable["ClaudeMessage"],
    max_excerpts_per_session: int = DEFAULT_MAX_EXCERPTS,
    excerpt_chars: int = DEFAULT_EXCERPT_CHARS,
) -> list[ClaudeSessionSummary]:
    """Group Claude messages into stable per-session summaries.

    Input messages are expected to have already passed project filtering and
    redaction. This function does not inspect raw logs or call an LLM.
    """
    grouped: dict[str, list["ClaudeMessage"]] = defaultdict(list)
    for message in messages:
        if not message.prompt_text:
            continue
        grouped[message.session_id or "unknown"].append(message)

    summaries: list[ClaudeSessionSummary] = []
    for session_id, session_messages in grouped.items():
        ordered = sorted(
            session_messages,
            key=lambda msg: (msg.timestamp, msg.message_uuid),
        )
        excerpts = tuple(
            _excerpt(msg.prompt_text, excerpt_chars)
            for msg in ordered[:max_excerpts_per_session]
        )
        summaries.append(
            ClaudeSessionSummary(
                session_id=session_id,
                project_path=ordered[0].project_path,
                started_at=ordered[0].timestamp,
                ended_at=ordered[-1].timestamp,
                prompt_count=len(ordered),
                prompt_excerpts=excerpts,
                message_uuids=tuple(msg.message_uuid for msg in ordered),
            )
        )

    return sorted(
        summaries,
        key=lambda summary: (summary.started_at, summary.session_id),
    )


def summaries_to_prompt_context(summaries: Iterable[ClaudeSessionSummary]) -> list[str]:
    return [summary.to_prompt_context() for summary in summaries]
