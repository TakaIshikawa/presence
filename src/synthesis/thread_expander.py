"""Expand high-performing X threads into blog draft candidates."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import anthropic


@dataclass
class SourceCommit:
    sha: str
    repo_name: str = ""
    commit_message: str = ""


@dataclass
class SourceMessage:
    message_uuid: str
    prompt_text: str
    project_path: str = ""


@dataclass
class ThreadExpansionCandidate:
    content_id: int
    original_thread: str
    engagement_score: float
    source_commits: list[str] = field(default_factory=list)
    source_messages: list[str] = field(default_factory=list)
    commit_context: list[SourceCommit] = field(default_factory=list)
    message_context: list[SourceMessage] = field(default_factory=list)
    published_url: str | None = None


@dataclass
class ThreadExpansionResult:
    source_id: int
    content: str
    generation_prompt: str


class ThreadExpander:
    """Anthropic-backed blog draft generator for proven X threads."""

    def __init__(
        self,
        api_key: str,
        model: str,
        timeout: float = 300.0,
        client: Any | None = None,
    ) -> None:
        self.client = client or anthropic.Anthropic(api_key=api_key, timeout=timeout)
        self.model = model

    def expand(self, candidate: ThreadExpansionCandidate) -> ThreadExpansionResult:
        prompt = self.build_prompt(candidate)
        response = self.client.messages.create(
            model=self.model,
            max_tokens=3500,
            messages=[{"role": "user", "content": prompt}],
        )
        content = response.content[0].text.strip()
        return ThreadExpansionResult(
            source_id=candidate.content_id,
            content=content,
            generation_prompt=prompt,
        )

    def build_prompt(self, candidate: ThreadExpansionCandidate) -> str:
        commit_context = self._format_commits(candidate)
        message_context = self._format_messages(candidate)
        published_url = candidate.published_url or "not recorded"

        return f"""You are expanding a high-performing X thread into a blog draft candidate.

Goal:
- Preserve the original thread's point of view and concrete claims.
- Expand only where the source commits, source messages, or original thread support it.
- Write a draft that can be reviewed before publication.
- Do not invent metrics, user quotes, dates, company names, or implementation details.

Output format:
TITLE: <specific title>

<markdown body>

Draft constraints:
- 700-1200 words.
- Use section headings where they improve scanability.
- Keep the voice direct, grounded, and first-person when the source material supports it.
- Include a short note near the end on what the original thread proved or what readers responded to.

Engagement score: {candidate.engagement_score}
Published URL: {published_url}

Original X thread:
{candidate.original_thread}

Source commits:
{commit_context}

Source Claude/user messages:
{message_context}
"""

    def _format_commits(self, candidate: ThreadExpansionCandidate) -> str:
        if not candidate.commit_context and not candidate.source_commits:
            return "- none recorded"

        lines: list[str] = []
        seen = set()
        for commit in candidate.commit_context:
            seen.add(commit.sha)
            repo = f"[{commit.repo_name}] " if commit.repo_name else ""
            message = commit.commit_message or "(message unavailable)"
            lines.append(f"- {commit.sha}: {repo}{message}")
        for sha in candidate.source_commits:
            if sha not in seen:
                lines.append(f"- {sha}: (metadata unavailable)")
        return "\n".join(lines)

    def _format_messages(self, candidate: ThreadExpansionCandidate) -> str:
        if not candidate.message_context and not candidate.source_messages:
            return "- none recorded"

        lines: list[str] = []
        seen = set()
        for message in candidate.message_context:
            seen.add(message.message_uuid)
            project = f" ({message.project_path})" if message.project_path else ""
            prompt_text = _truncate(message.prompt_text, 1200)
            lines.append(f"- {message.message_uuid}{project}: {prompt_text}")
        for message_uuid in candidate.source_messages:
            if message_uuid not in seen:
                lines.append(f"- {message_uuid}: (metadata unavailable)")
        return "\n".join(lines)


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."
