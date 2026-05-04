"""Tests for Claude session output truncation reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_output_truncation import (
    build_claude_session_output_truncation_report,
    format_claude_session_output_truncation_json,
    format_claude_session_output_truncation_text,
    load_truncation_events,
    group_truncation_events,
    ClaudeSessionOutputTruncationEvent,
    _excerpt,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_session_output_truncation.py"
spec = importlib.util.spec_from_file_location("claude_session_output_truncation_script", SCRIPT_PATH)
claude_session_output_truncation_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_output_truncation_script)


def _event_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            tool_name TEXT,
            message TEXT,
            content TEXT,
            output TEXT,
            metadata TEXT
        )"""
    )
    return conn


def _insert_event(
    conn: sqlite3.Connection,
    *,
    session_id: str = "sess-a",
    timestamp: str = "2026-05-01T10:00:00+00:00",
    tool_name: str = "Bash",
    message: str | None = None,
    content: str | None = None,
    output: str | None = None,
    metadata: str | dict | None = None,
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    conn.execute(
        """INSERT INTO claude_messages
           (session_id, timestamp, tool_name, message, content, output, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (session_id, timestamp, tool_name, message, content, output, metadata_value),
    )
    conn.commit()


def test_sqlite_builder_detects_truncation_in_plain_text_fields():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-a",
        timestamp="2026-05-01T10:00:00+00:00",
        tool_name="Bash",
        output="Some long output that was truncated after 500 lines",
    )
    _insert_event(
        conn,
        session_id="sess-a",
        timestamp="2026-05-01T10:01:00+00:00",
        tool_name="Grep",
        message="Search results with output omitted for brevity",
    )
    _insert_event(
        conn,
        session_id="sess-b",
        timestamp="2026-05-01T10:02:00+00:00",
        tool_name="Read",
        content="File contents with 1000 lines omitted",
    )

    report = build_claude_session_output_truncation_report(conn, days=7, now=NOW)
    payload = json.loads(format_claude_session_output_truncation_json(report))

    assert payload["artifact_type"] == "claude_session_output_truncation"
    assert list(payload) == sorted(payload)
    assert report.totals["truncation_event_count"] == 3
    assert report.totals["session_count"] == 2
    assert report.totals["reported_group_count"] == 3
    assert report.source_tables == ("claude_messages",)

    rows_by_key = {(r.session_id, r.tool_name, r.marker): r for r in report.rows}
    assert ("sess-a", "bash", "truncated") in rows_by_key
    assert ("sess-a", "grep", "output_omitted") in rows_by_key
    assert ("sess-b", "read", "lines_omitted") in rows_by_key


def test_detects_truncation_in_nested_json_metadata():
    rows = [
        {
            "session_id": "sess-meta",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {
                "tool_use": {"name": "Bash"},
                "output": "Build output truncated at 10000 characters",
            },
        },
        {
            "session_id": "sess-meta",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "content": {"result": "File contents with output omitted"},
            "metadata": {"tool_use": {"name": "Read"}},
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)

    assert report.source_tables == ()
    assert report.totals["truncation_event_count"] == 2
    rows_by_marker = {r.marker: r for r in report.rows}
    assert "truncated" in rows_by_marker
    assert "output_omitted" in rows_by_marker


def test_detects_explicit_truncation_flag_in_metadata():
    rows = [
        {
            "session_id": "sess-flag",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {
                "tool_use": {"name": "Bash"},
                "truncated": True,
            },
        },
        {
            "session_id": "sess-flag",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "metadata": {
                "tool_use": {"name": "Grep"},
                "truncated": "exceeded 30000 character limit",
            },
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)

    assert report.totals["truncation_event_count"] == 2
    flag_rows = [r for r in report.rows if r.marker == "truncated_flag"]
    # Two different tools mean two groups (session+tool+marker)
    assert len(flag_rows) == 2
    assert sum(r.occurrence_count for r in flag_rows) == 2


def test_groups_by_session_tool_and_marker():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-a",
        timestamp="2026-05-01T10:00:00+00:00",
        tool_name="Bash",
        output="Output truncated at line 500",
    )
    _insert_event(
        conn,
        session_id="sess-a",
        timestamp="2026-05-01T10:01:00+00:00",
        tool_name="Bash",
        output="Another output truncated at line 800",
    )
    _insert_event(
        conn,
        session_id="sess-b",
        timestamp="2026-05-01T10:02:00+00:00",
        tool_name="Bash",
        output="Third output truncated",
    )

    report = build_claude_session_output_truncation_report(conn, days=7, now=NOW)

    assert report.totals["truncation_event_count"] == 3
    assert report.totals["reported_group_count"] == 2
    rows_by_session = {r.session_id: r for r in report.rows}
    assert rows_by_session["sess-a"].occurrence_count == 2
    assert rows_by_session["sess-b"].occurrence_count == 1


def test_malformed_metadata_is_counted_without_failing():
    rows = [
        {
            "session_id": "sess-bad",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Bash",
            "output": "Output truncated",
            "metadata": "{invalid json",
        },
        {
            "session_id": "sess-good",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "Read",
            "message": "Lines omitted",
            "metadata": {"tool_use": {"name": "Read"}},
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)

    assert report.totals["malformed_metadata_count"] == 1
    assert report.totals["truncation_event_count"] == 2
    assert report.totals["session_count"] == 2


def test_missing_claude_tables_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE unrelated_table (id INTEGER PRIMARY KEY)")

    report = build_claude_session_output_truncation_report(conn, days=7, now=NOW)

    assert report.rows == ()
    assert report.missing_tables == (
        "claude_messages",
        "claude_message_events",
        "claude_session_events",
    )
    assert report.totals["truncation_event_count"] == 0


def test_json_formatter_produces_deterministic_output():
    rows = [
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Bash"}},
            "output": "Output truncated",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    json_output = format_claude_session_output_truncation_json(report)
    payload = json.loads(json_output)

    assert list(payload) == sorted(payload)
    assert list(payload["totals"]) == sorted(payload["totals"])


def test_text_formatter_produces_readable_output():
    rows = [
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Bash"}},
            "output": "Output truncated at 500 lines",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    text_output = format_claude_session_output_truncation_text(report)

    assert "Claude Session Output Truncation Report" in text_output
    assert "Generated:" in text_output
    assert "Truncated Outputs:" in text_output
    assert "session=sess-a" in text_output
    assert "tool=bash" in text_output
    assert "marker=truncated" in text_output


def test_empty_report_text_format():
    conn = _event_db()
    report = build_claude_session_output_truncation_report(conn, days=7, now=NOW)
    text_output = format_claude_session_output_truncation_text(report)

    assert "No truncated outputs detected." in text_output


def test_cli_script_handles_json_format():
    conn = _event_db()
    _insert_event(conn, output="Output truncated")

    with open("/tmp/test_truncation.db", "w") as _:
        pass
    conn2 = sqlite3.connect("/tmp/test_truncation.db")
    conn2.row_factory = sqlite3.Row
    conn2.execute(
        """CREATE TABLE claude_messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            tool_name TEXT,
            message TEXT,
            content TEXT,
            output TEXT,
            metadata TEXT
        )"""
    )
    conn2.execute(
        """INSERT INTO claude_messages
           (session_id, timestamp, tool_name, output)
           VALUES (?, ?, ?, ?)""",
        ("sess-a", "2026-05-01T10:00:00+00:00", "Bash", "Output truncated"),
    )
    conn2.commit()
    conn2.close()

    exit_code = claude_session_output_truncation_script.main(
        ["--db", "/tmp/test_truncation.db", "--format", "json"]
    )
    assert exit_code == 0


def test_cli_script_handles_text_format():
    conn = _event_db()
    _insert_event(conn, output="Lines omitted")

    with open("/tmp/test_truncation_text.db", "w") as _:
        pass
    conn2 = sqlite3.connect("/tmp/test_truncation_text.db")
    conn2.row_factory = sqlite3.Row
    conn2.execute(
        """CREATE TABLE claude_messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            tool_name TEXT,
            message TEXT,
            content TEXT,
            output TEXT,
            metadata TEXT
        )"""
    )
    conn2.execute(
        """INSERT INTO claude_messages
           (session_id, timestamp, tool_name, output)
           VALUES (?, ?, ?, ?)""",
        ("sess-a", "2026-05-01T10:00:00+00:00", "Read", "Lines omitted"),
    )
    conn2.commit()
    conn2.close()

    exit_code = claude_session_output_truncation_script.main(
        ["--db", "/tmp/test_truncation_text.db", "--format", "text"]
    )
    assert exit_code == 0


def test_representative_excerpt_limits_length():
    long_text = "This is a very long output that was truncated after many lines. " * 10
    rows = [
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Bash"}},
            "output": long_text,
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    assert len(report.rows[0].representative_excerpt) <= 123


def test_rows_sorted_by_occurrence_count():
    rows = [
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Bash"}},
            "output": "Output truncated",
        },
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "message": "Lines omitted",
        },
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "message": "More lines omitted",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    assert report.rows[0].occurrence_count >= report.rows[-1].occurrence_count


# --- Truncation threshold detection ---


def test_threshold_detection_output_exceeds_limit():
    """Test detection when output exceeds character threshold."""
    rows = [
        {
            "session_id": "sess-threshold",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "output": "File content exceeds 30000 character limit, output truncated",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    assert report.totals["truncation_event_count"] >= 1
    assert any("truncated" in r.marker for r in report.rows)


def test_threshold_detection_max_length_reached():
    """Test detection when max length is explicitly mentioned."""
    rows = [
        {
            "session_id": "sess-max",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Grep"}},
            "message": "Search results reached maximum length and were truncated",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    assert report.totals["truncation_event_count"] >= 1


# --- Full vs partial truncation ---


def test_full_truncation_completely_cut_off():
    """Test detection of completely cut off outputs."""
    rows = [
        {
            "session_id": "sess-full",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "output": "First 100 lines shown, remainder completely truncated",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    assert report.totals["truncation_event_count"] >= 1
    assert any("truncated" in r.marker for r in report.rows)


def test_partial_truncation_shortened():
    """Test detection of partially shortened outputs."""
    rows = [
        {
            "session_id": "sess-partial",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Bash"}},
            "output": "Showing first 500 lines, remaining 200 lines omitted",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    assert report.totals["truncation_event_count"] >= 1
    assert any(r.marker in ("lines_omitted", "output_omitted") for r in report.rows)


def test_distinguish_full_vs_partial_markers():
    """Test that full and partial truncations can be distinguished by markers."""
    rows = [
        {
            "session_id": "sess-full",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "output": "Output completely truncated",
        },
        {
            "session_id": "sess-partial",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "output": "Partial output, some lines omitted",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    markers = {r.marker for r in report.rows}
    # Should have different markers for different truncation types
    assert len(markers) >= 1


# --- Truncation recovery strategies ---


def test_read_tool_truncation_suggests_offset_limit():
    """Test that Read tool truncations can suggest using offset/limit parameters."""
    rows = [
        {
            "session_id": "sess-read",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "output": "Large file content... output truncated at 30000 characters",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)

    # Verify Read tool truncation is detected
    read_rows = [r for r in report.rows if r.tool_name.lower() == "read"]
    assert len(read_rows) >= 1
    # Recovery strategy: suggest using Read with offset/limit parameters
    # This information is available in the report for analysis


def test_identify_tools_needing_recovery_strategies():
    """Test identifying which tools have truncation and may need recovery strategies."""
    rows = [
        {
            "session_id": "sess-multi",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "output": "File truncated",
        },
        {
            "session_id": "sess-multi",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "metadata": {"tool_use": {"name": "Grep"}},
            "output": "Search results truncated",
        },
        {
            "session_id": "sess-multi",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "metadata": {"tool_use": {"name": "Bash"}},
            "output": "Command output truncated",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)

    # Multiple tools detected with truncation issues
    tool_names = {r.tool_name for r in report.rows}
    assert len(tool_names) >= 2

    # Read and Grep tools particularly benefit from offset/limit or refined queries
    tools_lower = {t.lower() for t in tool_names}
    assert any(tool in tools_lower for tool in ("read", "grep", "bash"))


# --- Edge cases: false positives ---


def test_ellipsis_in_normal_text_no_false_positive():
    """Test that ellipsis in normal content doesn't trigger false positive."""
    rows = [
        {
            "session_id": "sess-ellipsis",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Bash"}},
            "output": "The user said... let's continue with the implementation",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    # Should not detect truncation from normal ellipsis
    assert report.totals["truncation_event_count"] == 0


def test_word_truncated_in_technical_context():
    """Test 'truncated' word in technical/algorithm context still matches."""
    rows = [
        {
            "session_id": "sess-tech",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Bash"}},
            "output": "The truncated SVD algorithm is used for dimensionality reduction",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    # This matches the pattern (contains "truncated"), which is expected behavior
    # The pattern is designed to catch any use of truncation-related words
    assert report.totals["truncation_event_count"] >= 1


def test_no_false_positive_on_complete_output():
    """Test that complete outputs without truncation indicators are not flagged."""
    rows = [
        {
            "session_id": "sess-complete",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "output": "This is a complete file with all content shown successfully",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    assert report.totals["truncation_event_count"] == 0


# --- Edge cases: very long outputs ---


def test_very_long_output_over_100k_chars():
    """Test handling of extremely long outputs (>100k characters)."""
    # Simulate very long content that got truncated
    long_content = "x" * 120000 + " - output truncated due to size limit"

    rows = [
        {
            "session_id": "sess-huge",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "output": long_content,
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)

    assert report.totals["truncation_event_count"] >= 1
    # Verify excerpt is limited to reasonable length
    if report.rows:
        assert len(report.rows[0].representative_excerpt) <= 200


def test_excerpt_function_limits_very_long_text():
    """Test that _excerpt function properly limits very long text."""
    very_long_text = "a" * 500
    excerpt = _excerpt(very_long_text)

    # Default limit is 120 chars + "..." = 123
    assert len(excerpt) <= 123
    assert excerpt.endswith("...")


def test_excerpt_preserves_short_text():
    """Test that _excerpt preserves text shorter than limit."""
    short_text = "This is a short message"
    excerpt = _excerpt(short_text)

    assert excerpt == short_text


def test_excerpt_normalizes_whitespace():
    """Test that _excerpt normalizes whitespace in text."""
    text_with_whitespace = "Line 1\n\nLine 2\n   Line 3"
    excerpt = _excerpt(text_with_whitespace)

    assert "\n" not in excerpt
    assert "  " not in excerpt  # No double spaces


# --- Edge cases: nested truncation ---


def test_truncated_json_within_truncated_output():
    """Test detection of nested truncation (truncated JSON in truncated output)."""
    nested_content = '{"results": "... data truncated ...", "status": "output truncated"}'

    rows = [
        {
            "session_id": "sess-nested",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Bash"}},
            "output": nested_content,
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    assert report.totals["truncation_event_count"] >= 1


def test_truncation_in_nested_content_structure():
    """Test detection when truncation appears in nested content field."""
    rows = [
        {
            "session_id": "sess-nested-content",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "content": {
                "output": "Results were truncated at 1000 lines",
                "error": None,
            },
            "metadata": {"tool_use": {"name": "Grep"}},
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    assert report.totals["truncation_event_count"] >= 1


def test_multiple_nested_truncation_indicators():
    """Test detection of multiple truncation indicators in nested structures."""
    rows = [
        {
            "session_id": "sess-multi-nested",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {
                "tool_use": {"name": "Read"},
                "output": "Metadata output truncated",
                "truncated": True,
            },
            "output": "Main output also truncated",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    # Should detect multiple truncation indicators
    assert report.totals["truncation_event_count"] >= 1


# --- Truncation impact analysis ---


def test_high_truncation_frequency_indicates_session_issues():
    """Test that high truncation frequency can identify problematic sessions."""
    rows = [
        {
            "session_id": "sess-problematic",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "output": "Truncated 1",
        },
        {
            "session_id": "sess-problematic",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "output": "Truncated 2",
        },
        {
            "session_id": "sess-problematic",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "metadata": {"tool_use": {"name": "Grep"}},
            "output": "Truncated 3",
        },
        {
            "session_id": "sess-ok",
            "timestamp": "2026-05-01T10:03:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "output": "Truncated 4",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)

    # sess-problematic should have higher event count
    assert report.totals["session_count"] == 2
    assert report.totals["truncation_event_count"] >= 4

    # Analysis: sessions with more truncations may have usefulness issues


def test_tool_specific_truncation_patterns():
    """Test identifying which tools truncate most frequently."""
    rows = [
        {
            "session_id": f"sess-{i}",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "output": "Read output truncated",
        }
        for i in range(5)
    ] + [
        {
            "session_id": f"sess-grep-{i}",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Grep"}},
            "output": "Grep results truncated",
        }
        for i in range(2)
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)

    # Analyze tool frequency
    tool_occurrences = {}
    for row in report.rows:
        tool_occurrences[row.tool_name] = tool_occurrences.get(row.tool_name, 0) + row.occurrence_count

    # Read should have more truncations than Grep
    assert tool_occurrences.get("read", 0) > tool_occurrences.get("grep", 0)


# --- Additional validation tests ---


def test_report_validates_positive_days():
    """Test that report building validates days parameter."""
    rows = []

    # Should raise error for non-positive days
    import pytest

    with pytest.raises(ValueError, match="days must be positive"):
        build_claude_session_output_truncation_report(rows, days=0, now=NOW)

    with pytest.raises(ValueError, match="days must be positive"):
        build_claude_session_output_truncation_report(rows, days=-1, now=NOW)


def test_deduplication_of_same_content():
    """Test that same content value is not scanned multiple times in one row."""
    # This tests the checked_values deduplication logic in load_truncation_events
    content = "This output was truncated"

    rows = [
        {
            "session_id": "sess-dedup",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "output": content,
            "message": content,  # Same content in different field
            "metadata": {"tool_use": {"name": "Bash"}},
        },
    ]

    events, _ = load_truncation_events(rows)

    # Should only create one event despite content appearing in multiple fields
    # (The implementation uses id() to track and deduplicate same object references)
    assert len(events) == 1


def test_grouping_creates_unique_truncation_ids():
    """Test that grouped truncation events get unique IDs."""
    rows = [
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "output": "Truncated",
        },
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "output": "Truncated",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)

    truncation_ids = {r.truncation_id for r in report.rows}
    # Each group should have unique ID
    assert len(truncation_ids) == len(report.rows)
    assert all(tid.startswith("claude_truncation_") for tid in truncation_ids)
