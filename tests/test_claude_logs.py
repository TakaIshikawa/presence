"""Tests for the Claude Code log parser (ingestion/claude_logs.py)."""

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, mock_open

import pytest

from ingestion.claude_logs import ClaudeLogParser, ClaudeMessage, get_prompts_around_timestamp


# ---------------------------------------------------------------------------
# Helpers – build realistic JSONL fixtures
# ---------------------------------------------------------------------------

def _ts_ms(dt: datetime) -> int:
    """Convert datetime to epoch milliseconds (global history format)."""
    return int(dt.timestamp() * 1000)


def _ts_iso(dt: datetime) -> str:
    """Convert datetime to ISO-8601 string with Z suffix (session file format)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


_BASE_TS = datetime(2025, 3, 15, 10, 0, 0, tzinfo=timezone.utc)


def _write_global_history(path, entries):
    """Write a list of dicts as JSONL to *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")


def _make_global_entry(
    display: str,
    session_id: str = "sess-1",
    project: str = "/home/user/project",
    ts: datetime | None = None,
) -> dict:
    ts = ts or _BASE_TS
    return {
        "display": display,
        "timestamp": _ts_ms(ts),
        "project": project,
        "sessionId": session_id,
    }


def _make_session_entry(
    content: str,
    session_id: str = "sess-1",
    uuid: str = "uuid-1",
    cwd: str = "/home/user/project",
    ts: datetime | None = None,
    entry_type: str = "user",
) -> dict:
    ts = ts or _BASE_TS
    return {
        "type": entry_type,
        "message": {"content": content},
        "uuid": uuid,
        "timestamp": _ts_iso(ts),
        "sessionId": session_id,
        "cwd": cwd,
    }


# ---------------------------------------------------------------------------
# ClaudeMessage dataclass
# ---------------------------------------------------------------------------

class TestClaudeMessage:
    def test_fields(self):
        ts = _BASE_TS
        msg = ClaudeMessage(
            session_id="s1",
            message_uuid="u1",
            project_path="/p",
            timestamp=ts,
            prompt_text="hello",
        )
        assert msg.session_id == "s1"
        assert msg.message_uuid == "u1"
        assert msg.project_path == "/p"
        assert msg.timestamp == ts
        assert msg.prompt_text == "hello"

    def test_to_dict(self):
        ts = _BASE_TS
        msg = ClaudeMessage(
            session_id="s1",
            message_uuid="u1",
            project_path="/p",
            timestamp=ts,
            prompt_text="hello",
        )
        d = msg.to_dict()
        assert d == {
            "session_id": "s1",
            "message_uuid": "u1",
            "project_path": "/p",
            "timestamp": ts.isoformat(),
            "prompt_text": "hello",
        }

    def test_to_dict_timestamp_is_iso_string(self):
        msg = ClaudeMessage("s", "u", "/p", _BASE_TS, "text")
        assert isinstance(msg.to_dict()["timestamp"], str)


# ---------------------------------------------------------------------------
# parse_global_history
# ---------------------------------------------------------------------------

class TestParseGlobalHistory:
    def test_basic_parsing(self, tmp_path):
        entries = [
            _make_global_entry("Fix the bug", session_id="s1"),
            _make_global_entry("Add tests", session_id="s2"),
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_global_history())

        assert len(messages) == 2
        assert messages[0].prompt_text == "Fix the bug"
        assert messages[0].session_id == "s1"
        assert messages[1].prompt_text == "Add tests"

    def test_redacts_prompt_text(self, tmp_path):
        entries = [
            _make_global_entry(
                "Use token=ghp_abcdefghijklmnopqrstuvwxyz123456 from /Users/taka/app and email dev@example.com"
            ),
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path))
        msg = next(parser.parse_global_history())

        assert msg.prompt_text == (
            "Use token=[REDACTED_SECRET] from [REDACTED_PATH] and email [REDACTED_EMAIL]"
        )

    def test_accepts_custom_redaction_patterns(self, tmp_path):
        entries = [_make_global_entry("Deploy host-123.internal")]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(
            str(tmp_path),
            redaction_patterns=[
                {
                    "name": "internal_host",
                    "pattern": r"host-\d+\.internal",
                    "placeholder": "[REDACTED_HOST]",
                }
            ],
        )
        msg = next(parser.parse_global_history())

        assert msg.prompt_text == "Deploy [REDACTED_HOST]"

    def test_timestamp_converted_from_epoch_ms(self, tmp_path):
        ts = datetime(2025, 6, 1, 12, 30, 0, tzinfo=timezone.utc)
        entries = [_make_global_entry("prompt", ts=ts)]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path))
        msg = next(parser.parse_global_history())
        assert msg.timestamp == ts

    def test_message_uuid_is_synthetic(self, tmp_path):
        """Global history entries have no uuid; parser synthesises one from sessionId + timestamp."""
        entries = [_make_global_entry("prompt", session_id="abc")]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path))
        msg = next(parser.parse_global_history())
        assert msg.message_uuid.startswith("abc_")

    def test_project_path_extracted(self, tmp_path):
        entries = [_make_global_entry("prompt", project="/my/project")]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path))
        msg = next(parser.parse_global_history())
        assert msg.project_path == "/my/project"

    def test_skips_entry_without_display(self, tmp_path):
        entries = [
            {"timestamp": _ts_ms(_BASE_TS), "sessionId": "s1", "project": "/p"},
            _make_global_entry("valid"),
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_global_history())
        assert len(messages) == 1
        assert messages[0].prompt_text == "valid"

    def test_skips_entry_with_empty_display(self, tmp_path):
        entry = _make_global_entry("")
        # display="" is falsy, should be skipped
        _write_global_history(tmp_path / "history.jsonl", [entry, _make_global_entry("ok")])

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_global_history())
        assert len(messages) == 1

    def test_missing_optional_fields_default(self, tmp_path):
        """Entry with only display + timestamp should still parse with defaults."""
        entry = {"display": "hi", "timestamp": _ts_ms(_BASE_TS)}
        _write_global_history(tmp_path / "history.jsonl", [entry])

        parser = ClaudeLogParser(str(tmp_path))
        msg = next(parser.parse_global_history())
        assert msg.session_id == "unknown"
        assert msg.project_path == ""


# ---------------------------------------------------------------------------
# allowed_project_paths filtering
# ---------------------------------------------------------------------------

class TestAllowedProjectPaths:
    def test_absent_allowlist_preserves_current_behavior(self, tmp_path):
        entries = [
            _make_global_entry("allowed by default", project="/project-a"),
            _make_global_entry("also allowed by default", project="/unrelated"),
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_global_history())

        assert [m.prompt_text for m in messages] == [
            "allowed by default",
            "also allowed by default",
        ]
        assert parser.skipped_project_counts == {}

    def test_matching_project_path_is_ingested(self, tmp_path):
        project = tmp_path / "project-a"
        entries = [
            _make_global_entry("match", project=str(project)),
            _make_global_entry("skip", project=str(tmp_path / "project-b")),
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path), allowed_project_paths=[str(project)])
        messages = list(parser.parse_global_history())

        assert [m.prompt_text for m in messages] == ["match"]

    def test_nested_project_path_is_ingested(self, tmp_path):
        project = tmp_path / "project-a"
        nested = project / "packages" / "api"
        entries = [
            _make_global_entry("nested match", project=str(nested)),
            _make_global_entry("sibling skip", project=str(tmp_path / "project-a-other")),
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path), allowed_project_paths=[str(project)])
        messages = list(parser.parse_global_history())

        assert [m.prompt_text for m in messages] == ["nested match"]

    def test_skipped_unrelated_projects_are_counted_and_logged(self, tmp_path, caplog):
        import logging
        caplog.set_level(logging.INFO)

        project = tmp_path / "project-a"
        unrelated = tmp_path / "unrelated"
        entries = [
            _make_global_entry("match", project=str(project)),
            _make_global_entry("skip one", project=str(unrelated)),
            _make_global_entry("skip two", project=str(unrelated)),
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path), allowed_project_paths=[str(project)])
        messages = list(parser.parse_global_history())
        parser.log_skipped_project_counts("test")

        assert [m.prompt_text for m in messages] == ["match"]
        assert parser.skipped_project_counts[str(unrelated)] == 2
        assert "test: skipped Claude messages from unconfigured projects" in caplog.text
        assert f"{unrelated}: 2" in caplog.text

    def test_session_file_filtering_uses_cwd(self, tmp_path):
        project = tmp_path / "project-a"
        session_file = tmp_path / "session.jsonl"
        entries = [
            _make_session_entry("match", cwd=str(project)),
            _make_session_entry("skip", cwd=str(tmp_path / "project-b")),
        ]
        _write_global_history(session_file, entries)

        parser = ClaudeLogParser(str(tmp_path), allowed_project_paths=[str(project)])
        messages = list(parser.parse_session_file(session_file))

        assert [m.prompt_text for m in messages] == ["match"]


# ---------------------------------------------------------------------------
# parse_session_file
# ---------------------------------------------------------------------------

class TestParseSessionFile:
    def test_basic_parsing(self, tmp_path):
        session_file = tmp_path / "session.jsonl"
        entries = [
            _make_session_entry("What is 1+1?", uuid="u1"),
            _make_session_entry("Explain the code", uuid="u2"),
        ]
        _write_global_history(session_file, entries)

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_session_file(session_file))

        assert len(messages) == 2
        assert messages[0].prompt_text == "What is 1+1?"
        assert messages[0].message_uuid == "u1"
        assert messages[1].prompt_text == "Explain the code"

    def test_redacts_session_file_prompt_text(self, tmp_path):
        session_file = tmp_path / "session.jsonl"
        entries = [
            _make_session_entry("Send Authorization: Bearer abcdefghijklmnopqrstuvwxyz"),
        ]
        _write_global_history(session_file, entries)

        parser = ClaudeLogParser(str(tmp_path))
        msg = next(parser.parse_session_file(session_file))

        assert msg.prompt_text == "Send Authorization: [REDACTED_BEARER]"

    def test_timestamp_parsed_from_iso(self, tmp_path):
        ts = datetime(2025, 7, 10, 8, 0, 0, tzinfo=timezone.utc)
        session_file = tmp_path / "session.jsonl"
        _write_global_history(session_file, [_make_session_entry("prompt", ts=ts)])

        parser = ClaudeLogParser(str(tmp_path))
        msg = next(parser.parse_session_file(session_file))
        assert msg.timestamp == ts

    def test_cwd_becomes_project_path(self, tmp_path):
        session_file = tmp_path / "session.jsonl"
        _write_global_history(
            session_file,
            [_make_session_entry("prompt", cwd="/other/path")],
        )

        parser = ClaudeLogParser(str(tmp_path))
        msg = next(parser.parse_session_file(session_file))
        assert msg.project_path == "/other/path"

    def test_skips_non_user_entries(self, tmp_path):
        session_file = tmp_path / "session.jsonl"
        entries = [
            _make_session_entry("assistant reply", entry_type="assistant"),
            _make_session_entry("user prompt", entry_type="user"),
        ]
        _write_global_history(session_file, entries)

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_session_file(session_file))
        assert len(messages) == 1
        assert messages[0].prompt_text == "user prompt"

    def test_skips_entry_with_non_string_content(self, tmp_path):
        """Content can be a list (tool-use blocks); parser only yields string content."""
        session_file = tmp_path / "session.jsonl"
        list_content = {
            "type": "user",
            "message": {"content": [{"type": "text", "text": "hello"}]},
            "uuid": "u1",
            "timestamp": _ts_iso(_BASE_TS),
            "sessionId": "s1",
            "cwd": "/p",
        }
        string_content = _make_session_entry("valid text", uuid="u2")
        _write_global_history(session_file, [list_content, string_content])

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_session_file(session_file))
        assert len(messages) == 1
        assert messages[0].prompt_text == "valid text"

    def test_skips_entry_with_empty_string_content(self, tmp_path):
        session_file = tmp_path / "session.jsonl"
        empty = _make_session_entry("", uuid="u1")
        valid = _make_session_entry("real prompt", uuid="u2")
        _write_global_history(session_file, [empty, valid])

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_session_file(session_file))
        assert len(messages) == 1

    def test_missing_uuid_defaults(self, tmp_path):
        session_file = tmp_path / "session.jsonl"
        entry = {
            "type": "user",
            "message": {"content": "prompt"},
            "timestamp": _ts_iso(_BASE_TS),
            "sessionId": "s1",
            "cwd": "/p",
        }
        _write_global_history(session_file, [entry])

        parser = ClaudeLogParser(str(tmp_path))
        msg = next(parser.parse_session_file(session_file))
        assert msg.message_uuid == "unknown"

    def test_nonexistent_file_yields_nothing(self, tmp_path):
        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_session_file(tmp_path / "nonexistent.jsonl"))
        assert messages == []


# ---------------------------------------------------------------------------
# get_messages_since
# ---------------------------------------------------------------------------

class TestGetMessagesSince:
    def _setup_history(self, tmp_path, timestamps):
        entries = [
            _make_global_entry(f"prompt-{i}", ts=ts, session_id=f"s{i}")
            for i, ts in enumerate(timestamps)
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)
        return ClaudeLogParser(str(tmp_path))

    def test_filters_messages_after_cutoff(self, tmp_path):
        ts1 = _BASE_TS
        ts2 = _BASE_TS + timedelta(hours=1)
        ts3 = _BASE_TS + timedelta(hours=2)
        parser = self._setup_history(tmp_path, [ts1, ts2, ts3])

        cutoff = _BASE_TS + timedelta(minutes=30)
        messages = list(parser.get_messages_since(cutoff))
        assert len(messages) == 2
        assert messages[0].prompt_text == "prompt-1"
        assert messages[1].prompt_text == "prompt-2"

    def test_inclusive_boundary(self, tmp_path):
        """Messages at exactly the cutoff timestamp should be included (>=)."""
        parser = self._setup_history(tmp_path, [_BASE_TS])
        messages = list(parser.get_messages_since(_BASE_TS))
        assert len(messages) == 1

    def test_cutoff_after_all_messages(self, tmp_path):
        parser = self._setup_history(tmp_path, [_BASE_TS])
        future = _BASE_TS + timedelta(days=1)
        messages = list(parser.get_messages_since(future))
        assert messages == []

    def test_cutoff_before_all_messages(self, tmp_path):
        ts1 = _BASE_TS
        ts2 = _BASE_TS + timedelta(hours=1)
        parser = self._setup_history(tmp_path, [ts1, ts2])
        past = _BASE_TS - timedelta(days=1)
        messages = list(parser.get_messages_since(past))
        assert len(messages) == 2


# ---------------------------------------------------------------------------
# get_session_summaries_in_range
# ---------------------------------------------------------------------------

class TestGetSessionSummariesInRange:
    def test_groups_messages_inside_date_range(self, tmp_path):
        entries = [
            _make_global_entry("before", session_id="s0", ts=_BASE_TS - timedelta(minutes=1)),
            _make_global_entry("first", session_id="s1", ts=_BASE_TS),
            _make_global_entry("second", session_id="s1", ts=_BASE_TS + timedelta(minutes=5)),
            _make_global_entry("other", session_id="s2", ts=_BASE_TS + timedelta(minutes=10)),
            _make_global_entry("end excluded", session_id="s3", ts=_BASE_TS + timedelta(hours=1)),
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path))
        summaries = parser.get_session_summaries_in_range(
            _BASE_TS,
            _BASE_TS + timedelta(hours=1),
        )

        assert [summary.session_id for summary in summaries] == ["s1", "s2"]
        assert summaries[0].prompt_count == 2
        assert summaries[0].prompt_excerpts == ("first", "second")

    def test_preserves_allowlist_filtering_and_redaction(self, tmp_path):
        project = tmp_path / "project-a"
        entries = [
            _make_global_entry(
                "Use token=ghp_abcdefghijklmnopqrstuvwxyz123456",
                session_id="s1",
                project=str(project),
                ts=_BASE_TS,
            ),
            _make_global_entry(
                "unallowed secret token=ghp_abcdefghijklmnopqrstuvwxyz999999",
                session_id="s2",
                project=str(tmp_path / "other"),
                ts=_BASE_TS,
            ),
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path), allowed_project_paths=[str(project)])
        summaries = parser.get_session_summaries_in_range(
            _BASE_TS,
            _BASE_TS + timedelta(minutes=1),
        )

        assert len(summaries) == 1
        assert summaries[0].project_path == str(project)
        assert summaries[0].prompt_excerpts == ("Use token=[REDACTED_SECRET]",)


# ---------------------------------------------------------------------------
# get_messages_for_project
# ---------------------------------------------------------------------------

class TestGetMessagesForProject:
    def test_filters_by_exact_project_path(self, tmp_path):
        entries = [
            _make_global_entry("a", project="/project-a"),
            _make_global_entry("b", project="/project-b"),
            _make_global_entry("c", project="/project-a"),
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.get_messages_for_project("/project-a"))
        assert len(messages) == 2
        assert all(m.project_path == "/project-a" for m in messages)

    def test_no_match_returns_empty(self, tmp_path):
        entries = [_make_global_entry("a", project="/other")]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.get_messages_for_project("/nonexistent"))
        assert messages == []

    def test_empty_project_path_matches_entries_without_project(self, tmp_path):
        entry = {"display": "orphan", "timestamp": _ts_ms(_BASE_TS), "sessionId": "s1"}
        _write_global_history(tmp_path / "history.jsonl", [entry])

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.get_messages_for_project(""))
        assert len(messages) == 1


# ---------------------------------------------------------------------------
# get_recent_sessions
# ---------------------------------------------------------------------------

class TestGetRecentSessions:
    def test_returns_sessions_ordered_by_most_recent(self, tmp_path):
        entries = [
            _make_global_entry("old", session_id="s-old", ts=_BASE_TS),
            _make_global_entry("mid", session_id="s-mid", ts=_BASE_TS + timedelta(hours=1)),
            _make_global_entry("new", session_id="s-new", ts=_BASE_TS + timedelta(hours=2)),
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path))
        sessions = parser.get_recent_sessions()
        assert sessions == ["s-new", "s-mid", "s-old"]

    def test_uses_latest_timestamp_per_session(self, tmp_path):
        """Session with multiple messages uses max timestamp for ordering."""
        entries = [
            _make_global_entry("early", session_id="s1", ts=_BASE_TS),
            _make_global_entry("late", session_id="s1", ts=_BASE_TS + timedelta(hours=5)),
            _make_global_entry("other", session_id="s2", ts=_BASE_TS + timedelta(hours=3)),
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path))
        sessions = parser.get_recent_sessions()
        # s1's max is +5h, s2's is +3h → s1 first
        assert sessions == ["s1", "s2"]

    def test_limit_parameter(self, tmp_path):
        entries = [
            _make_global_entry(f"p{i}", session_id=f"s{i}", ts=_BASE_TS + timedelta(hours=i))
            for i in range(5)
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path))
        sessions = parser.get_recent_sessions(limit=3)
        assert len(sessions) == 3
        assert sessions == ["s4", "s3", "s2"]

    def test_empty_history_returns_empty_list(self, tmp_path):
        _write_global_history(tmp_path / "history.jsonl", [])
        parser = ClaudeLogParser(str(tmp_path))
        assert parser.get_recent_sessions() == []


# ---------------------------------------------------------------------------
# get_prompts_around_timestamp (module-level helper)
# ---------------------------------------------------------------------------

class TestGetPromptsAroundTimestamp:
    def test_returns_messages_within_window(self, tmp_path):
        center = _BASE_TS + timedelta(hours=1)
        entries = [
            _make_global_entry("before-window", ts=_BASE_TS),  # 1h before center
            _make_global_entry("in-window-early", ts=center - timedelta(minutes=10)),
            _make_global_entry("exact", ts=center),
            _make_global_entry("in-window-late", ts=center + timedelta(minutes=10)),
            _make_global_entry("after-window", ts=center + timedelta(hours=1)),
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)

        results = get_prompts_around_timestamp(center, window_minutes=30, claude_dir=str(tmp_path))
        texts = [m.prompt_text for m in results]
        assert "in-window-early" in texts
        assert "exact" in texts
        assert "in-window-late" in texts
        assert "before-window" not in texts
        assert "after-window" not in texts

    def test_inclusive_boundaries(self, tmp_path):
        center = _BASE_TS
        entries = [
            _make_global_entry("at-start", ts=center - timedelta(minutes=30)),
            _make_global_entry("at-end", ts=center + timedelta(minutes=30)),
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)

        results = get_prompts_around_timestamp(center, window_minutes=30, claude_dir=str(tmp_path))
        assert len(results) == 2

    def test_custom_window(self, tmp_path):
        center = _BASE_TS
        entries = [
            _make_global_entry("close", ts=center + timedelta(minutes=5)),
            _make_global_entry("far", ts=center + timedelta(minutes=15)),
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)

        results = get_prompts_around_timestamp(center, window_minutes=10, claude_dir=str(tmp_path))
        assert len(results) == 1
        assert results[0].prompt_text == "close"

    def test_no_matches_returns_empty(self, tmp_path):
        _write_global_history(tmp_path / "history.jsonl", [
            _make_global_entry("distant", ts=_BASE_TS),
        ])
        far_future = _BASE_TS + timedelta(days=30)
        results = get_prompts_around_timestamp(far_future, window_minutes=30, claude_dir=str(tmp_path))
        assert results == []


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_history_file(self, tmp_path):
        _write_global_history(tmp_path / "history.jsonl", [])
        parser = ClaudeLogParser(str(tmp_path))
        assert list(parser.parse_global_history()) == []

    def test_nonexistent_history_file(self, tmp_path):
        parser = ClaudeLogParser(str(tmp_path))
        assert list(parser.parse_global_history()) == []

    def test_malformed_json_lines_skipped(self, tmp_path):
        history = tmp_path / "history.jsonl"
        history.parent.mkdir(parents=True, exist_ok=True)
        with open(history, "w") as f:
            f.write("not valid json\n")
            f.write(json.dumps(_make_global_entry("valid")) + "\n")
            f.write("{truncated\n")
            f.write(json.dumps(_make_global_entry("also valid")) + "\n")

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_global_history())
        assert len(messages) == 2
        assert messages[0].prompt_text == "valid"
        assert messages[1].prompt_text == "also valid"

    def test_malformed_json_lines_logged(self, tmp_path, caplog):
        """Verify that malformed JSON lines are logged at debug level."""
        import logging
        caplog.set_level(logging.DEBUG)

        history = tmp_path / "history.jsonl"
        history.parent.mkdir(parents=True, exist_ok=True)
        with open(history, "w") as f:
            f.write("not valid json\n")
            f.write(json.dumps(_make_global_entry("valid")) + "\n")
            f.write("{truncated\n")

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_global_history())

        # Should have 2 debug log entries for malformed lines
        debug_logs = [r for r in caplog.records if r.levelname == "DEBUG"]
        assert len(debug_logs) == 2
        assert all("Skipping malformed line in global history" in r.message for r in debug_logs)

    def test_malformed_json_in_session_file_skipped(self, tmp_path):
        session_file = tmp_path / "session.jsonl"
        with open(session_file, "w") as f:
            f.write("broken line\n")
            f.write(json.dumps(_make_session_entry("ok")) + "\n")

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_session_file(session_file))
        assert len(messages) == 1

    def test_malformed_json_in_session_file_logged(self, tmp_path, caplog):
        """Verify that malformed JSON lines in session files are logged at debug level."""
        import logging
        caplog.set_level(logging.DEBUG)

        session_file = tmp_path / "session.jsonl"
        with open(session_file, "w") as f:
            f.write("broken line\n")
            f.write(json.dumps(_make_session_entry("ok")) + "\n")
            f.write("{incomplete\n")

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_session_file(session_file))

        # Should have 2 debug log entries for malformed entries
        debug_logs = [r for r in caplog.records if r.levelname == "DEBUG"]
        assert len(debug_logs) == 2
        assert all(f"Skipping malformed entry in session {session_file.name}" in r.message for r in debug_logs)

    def test_blank_lines_skipped(self, tmp_path):
        history = tmp_path / "history.jsonl"
        history.parent.mkdir(parents=True, exist_ok=True)
        with open(history, "w") as f:
            f.write("\n")
            f.write("  \n")
            f.write(json.dumps(_make_global_entry("valid")) + "\n")
            f.write("\n")

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_global_history())
        assert len(messages) == 1

    def test_session_entry_missing_message_key(self, tmp_path):
        """Entry with type=user but no 'message' key should be skipped (KeyError)."""
        session_file = tmp_path / "session.jsonl"
        bad_entry = {
            "type": "user",
            "uuid": "u1",
            "timestamp": _ts_iso(_BASE_TS),
            "sessionId": "s1",
            "cwd": "/p",
        }
        _write_global_history(session_file, [bad_entry, _make_session_entry("ok")])

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_session_file(session_file))
        assert len(messages) == 1

    def test_session_entry_missing_timestamp(self, tmp_path):
        """Entry without timestamp should be skipped (KeyError caught)."""
        session_file = tmp_path / "session.jsonl"
        bad_entry = {
            "type": "user",
            "message": {"content": "prompt"},
            "uuid": "u1",
            "sessionId": "s1",
            "cwd": "/p",
        }
        _write_global_history(session_file, [bad_entry, _make_session_entry("ok")])

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_session_file(session_file))
        assert len(messages) == 1

    def test_session_entry_with_invalid_timestamp(self, tmp_path):
        """Entry with unparseable timestamp should be skipped (ValueError caught)."""
        session_file = tmp_path / "session.jsonl"
        bad_entry = {
            "type": "user",
            "message": {"content": "prompt"},
            "uuid": "u1",
            "timestamp": "not-a-date",
            "sessionId": "s1",
            "cwd": "/p",
        }
        _write_global_history(session_file, [bad_entry, _make_session_entry("ok")])

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_session_file(session_file))
        assert len(messages) == 1

    def test_constructor_with_tilde_expansion(self, tmp_path, monkeypatch):
        """Verify ~ in claude_dir gets expanded."""
        monkeypatch.setenv("HOME", str(tmp_path))
        parser = ClaudeLogParser("~/fakeclaude")
        assert parser.claude_dir == tmp_path / "fakeclaude"

    def test_iterator_is_lazy(self, tmp_path):
        """parse_global_history returns an iterator, not a list."""
        entries = [_make_global_entry(f"p{i}") for i in range(10)]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path))
        result = parser.parse_global_history()
        # Should be a generator/iterator, not a list
        assert hasattr(result, "__next__")


# ---------------------------------------------------------------------------
# File I/O error handling
# ---------------------------------------------------------------------------

class TestFileIOErrorHandling:
    def test_parse_global_history_handles_permission_error(self, tmp_path, caplog):
        """PermissionError on history file is caught and logged."""
        history_file = tmp_path / "history.jsonl"
        _write_global_history(history_file, [_make_global_entry("test")])

        parser = ClaudeLogParser(str(tmp_path))

        with patch("builtins.open", side_effect=PermissionError("Access denied")):
            messages = list(parser.parse_global_history())

        assert messages == []
        assert "Could not read history file" in caplog.text
        assert "Access denied" in caplog.text

    def test_parse_global_history_handles_oserror(self, tmp_path, caplog):
        """General OSError on history file is caught and logged."""
        history_file = tmp_path / "history.jsonl"
        _write_global_history(history_file, [_make_global_entry("test")])

        parser = ClaudeLogParser(str(tmp_path))

        with patch("builtins.open", side_effect=OSError("Disk I/O error")):
            messages = list(parser.parse_global_history())

        assert messages == []
        assert "Could not read history file" in caplog.text
        assert "Disk I/O error" in caplog.text

    def test_parse_session_file_handles_permission_error(self, tmp_path, caplog):
        """PermissionError on session file is caught and logged."""
        session_file = tmp_path / "session.jsonl"
        _write_global_history(session_file, [_make_session_entry("test")])

        parser = ClaudeLogParser(str(tmp_path))

        with patch("builtins.open", side_effect=PermissionError("Access denied")):
            messages = list(parser.parse_session_file(session_file))

        assert messages == []
        assert "Could not read session file" in caplog.text
        assert "Access denied" in caplog.text

    def test_parse_session_file_handles_oserror(self, tmp_path, caplog):
        """General OSError on session file is caught and logged."""
        session_file = tmp_path / "session.jsonl"
        _write_global_history(session_file, [_make_session_entry("test")])

        parser = ClaudeLogParser(str(tmp_path))

        with patch("builtins.open", side_effect=OSError("Disk I/O error")):
            messages = list(parser.parse_session_file(session_file))

        assert messages == []
        assert "Could not read session file" in caplog.text
        assert "Disk I/O error" in caplog.text

    def test_normal_operation_after_error_handling(self, tmp_path):
        """Verify normal operation still works after adding error handling."""
        # Test parse_global_history
        entries = [
            _make_global_entry("First prompt", session_id="s1"),
            _make_global_entry("Second prompt", session_id="s2"),
        ]
        _write_global_history(tmp_path / "history.jsonl", entries)

        parser = ClaudeLogParser(str(tmp_path))
        messages = list(parser.parse_global_history())

        assert len(messages) == 2
        assert messages[0].prompt_text == "First prompt"
        assert messages[1].prompt_text == "Second prompt"

        # Test parse_session_file
        session_file = tmp_path / "session.jsonl"
        session_entries = [
            _make_session_entry("User message 1", uuid="u1"),
            _make_session_entry("User message 2", uuid="u2"),
        ]
        _write_global_history(session_file, session_entries)

        messages = list(parser.parse_session_file(session_file))

        assert len(messages) == 2
        assert messages[0].prompt_text == "User message 1"
        assert messages[1].prompt_text == "User message 2"
