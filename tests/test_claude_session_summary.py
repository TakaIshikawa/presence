"""Tests for deterministic Claude session summaries."""

from datetime import datetime, timedelta, timezone

from ingestion.claude_logs import ClaudeMessage
from ingestion.claude_session_summary import build_session_summaries


BASE = datetime(2026, 4, 5, 10, 0, tzinfo=timezone.utc)


def _message(
    session_id: str,
    text: str,
    offset_minutes: int,
    uuid: str,
    project_path: str = "/repo",
) -> ClaudeMessage:
    return ClaudeMessage(
        session_id=session_id,
        message_uuid=uuid,
        project_path=project_path,
        timestamp=BASE + timedelta(minutes=offset_minutes),
        prompt_text=text,
    )


def test_groups_messages_by_session_with_stable_bounds_and_counts():
    messages = [
        _message("s2", "later session", 30, "u3", "/repo-b"),
        _message("s1", "second prompt", 10, "u2"),
        _message("s1", "first prompt", 0, "u1"),
    ]

    summaries = build_session_summaries(messages)

    assert [summary.session_id for summary in summaries] == ["s1", "s2"]
    first = summaries[0]
    assert first.started_at == BASE
    assert first.ended_at == BASE + timedelta(minutes=10)
    assert first.prompt_count == 2
    assert first.project_path == "/repo"
    assert first.message_uuids == ("u1", "u2")
    assert first.prompt_excerpts == ("first prompt", "second prompt")


def test_summary_text_contains_compact_session_context():
    summary = build_session_summaries([
        _message("s1", "Fix the bug\n\nthen add tests", 0, "u1"),
    ])[0]

    text = summary.to_prompt_context()

    assert "Claude session s1" in text
    assert "Project: /repo" in text
    assert "Prompts: 1" in text
    assert "- Fix the bug then add tests" in text


def test_preserves_redacted_prompt_excerpts_without_rerawing():
    messages = [
        _message("s1", "Use token=[REDACTED_SECRET] from [REDACTED_PATH]", 0, "u1"),
    ]

    summary = build_session_summaries(messages)[0]

    assert summary.prompt_excerpts == (
        "Use token=[REDACTED_SECRET] from [REDACTED_PATH]",
    )
    assert "[REDACTED_SECRET]" in summary.summary_text


def test_limits_and_truncates_excerpts_deterministically():
    messages = [
        _message("s1", "a" * 20, 0, "u1"),
        _message("s1", "b" * 20, 1, "u2"),
        _message("s1", "c" * 20, 2, "u3"),
    ]

    summary = build_session_summaries(
        messages,
        max_excerpts_per_session=2,
        excerpt_chars=8,
    )[0]

    assert summary.prompt_excerpts == ("aaaaa...", "bbbbb...")
