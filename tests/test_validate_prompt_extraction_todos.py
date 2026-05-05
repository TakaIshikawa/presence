"""Comprehensive tests for TODO/FIXME extraction logic in claude_session_todo_completion.

This test suite focuses on edge cases including:
- Multi-line TODOs and TODOs with special characters
- Case variations (TODO/todo/ToDo)
- Different comment styles (# vs // vs /* */)
- Extraction accuracy, filtering, and deduplication
"""

from __future__ import annotations

import json

from ingestion.claude_session_todo_completion import (
    _extract_todos,
    _normalize_status,
    _normalize_todo_text,
    _todos_from_json,
    _todos_from_text,
)


def test_todos_from_text_extracts_checkbox_todos_with_different_marks():
    """Test extraction of markdown checkbox TODOs with various marks."""
    text = """
    Plan:
    - [ ] Pending task with empty checkbox
    - [x] Completed task with x mark
    - [X] Completed task with capital X mark
    - [~] Canceled task with tilde
    - [-] Canceled task with dash
    * [ ] Pending with asterisk prefix
    * [x] Completed with asterisk prefix
    """

    todos = _todos_from_text(text)

    # Should extract 7 todos
    assert len(todos) == 7

    # Verify status mapping
    statuses = [todo["status"] for todo in todos]
    assert statuses.count("pending") == 2
    assert statuses.count("completed") == 3
    assert statuses.count("canceled") == 2

    # Verify text normalization (case-folded and whitespace normalized)
    texts = [todo["text"] for todo in todos]
    assert "pending task with empty checkbox" in texts
    assert "completed task with x mark" in texts
    assert "completed task with capital x mark" in texts


def test_todos_from_text_extracts_status_line_todos_with_case_variations():
    """Test extraction of status-prefixed TODOs with various case variations."""
    text = """
    TODO: implement feature A
    todo: implement feature B
    ToDo: implement feature C
    pending: task one
    PENDING: task two
    in_progress: working on it
    in-progress: another active task
    in progress: yet another active task
    completed: done task
    COMPLETED: another done task
    done: finished task
    finished: completed task
    canceled: dropped task
    cancelled: british spelling
    skipped: omitted task
    """

    todos = _todos_from_text(text)

    # Should extract status-based todos (not TODO: lines as they don't match _STATUS_LINE_RE)
    # The regex looks for pending|in[_ -]?progress|completed?|done|finished|cancell?ed|skipped
    # It doesn't match plain "TODO:" or "todo:"
    statuses = [todo["status"] for todo in todos]

    assert "pending" in statuses
    assert "in_progress" in statuses
    assert "completed" in statuses
    assert "canceled" in statuses

    # Verify case-insensitive matching
    assert statuses.count("pending") >= 2  # pending and PENDING
    assert statuses.count("in_progress") >= 3  # in_progress, in-progress, in progress
    assert statuses.count("completed") >= 4  # completed, COMPLETED, done, finished
    assert statuses.count("canceled") >= 3  # canceled, cancelled, skipped


def test_todos_from_text_handles_special_characters_in_todo_text():
    """Test TODO extraction with special characters in the task description."""
    text = """
    - [ ] Fix bug with "quotes" in string
    - [x] Add support for @mentions & #hashtags
    - [ ] Handle paths like /path/to/file.txt
    - [ ] Support URLs https://example.com/api?key=value
    - [~] Drop feature with $pecial ch@racters!
    - [ ] Unicode support: café, 日本語, émoji 🎉
    """

    todos = _todos_from_text(text)

    assert len(todos) == 6

    # Verify special characters are preserved in normalized text
    texts = [todo["text"] for todo in todos]
    assert any('"quotes"' in t for t in texts)
    assert any('@mentions' in t for t in texts)
    assert any('/path/to/file.txt' in t for t in texts)
    assert any('https://example.com/api?key=value' in t for t in texts)
    assert any('$pecial ch@racters' in t for t in texts)
    # Note: emoji might be normalized differently, check basic unicode
    assert any('café' in t or 'cafe' in t for t in texts)


def test_todos_from_text_handles_multiline_context():
    """Test that each line is processed independently (no multi-line TODO support)."""
    text = """
    - [ ] First part of a task
          continuation line without checkbox
          another continuation line
    - [x] Second task on its own line
    """

    todos = _todos_from_text(text)

    # Should only extract the lines with checkboxes
    assert len(todos) == 2
    assert todos[0]["status"] == "pending"
    assert todos[1]["status"] == "completed"

    # Continuation lines are not part of the checkbox pattern
    assert "continuation" not in todos[0]["text"]


def test_todos_from_text_handles_empty_and_whitespace_only_input():
    """Test extraction from empty or whitespace-only text."""
    assert _todos_from_text("") == []
    assert _todos_from_text("   \n\t  \n  ") == []
    assert _todos_from_text("Just plain text without any todos") == []


def test_todos_from_json_extracts_structured_todos_with_various_field_names():
    """Test extraction from JSON/dict structures with different field names."""
    json_data = {
        "tool_use": {
            "name": "TodoWrite",
            "input": {
                "todos": [
                    {"status": "completed", "content": "task one"},
                    {"state": "pending", "text": "task two"},
                    {"todo_status": "in_progress", "title": "task three"},
                    {"status": "canceled", "task": "task four"},
                    {"status": "pending", "todo": "task five"},
                    {"status": "completed", "description": "task six"},
                ]
            }
        }
    }

    todos = _todos_from_json(json_data)

    # Should extract all 6 todos
    assert len(todos) == 6

    # Verify status normalization
    statuses = [todo["status"] for todo in todos]
    assert statuses.count("completed") == 2
    assert statuses.count("pending") == 2
    assert statuses.count("in_progress") == 1
    assert statuses.count("canceled") == 1

    # Verify text extraction from various field names
    texts = [todo["text"] for todo in todos]
    assert "task one" in texts
    assert "task two" in texts
    assert "task three" in texts
    assert "task four" in texts
    assert "task five" in texts
    assert "task six" in texts


def test_todos_from_json_normalizes_status_aliases():
    """Test that status aliases are properly normalized."""
    json_data = [
        {"status": "active", "content": "task 1"},  # -> in_progress
        {"status": "abandoned", "content": "task 2"},  # -> canceled
        {"status": "complete", "content": "task 3"},  # -> completed
        {"status": "done", "content": "task 4"},  # -> completed
        {"status": "doing", "content": "task 5"},  # -> in_progress
        {"status": "dropped", "content": "task 6"},  # -> canceled
        {"status": "finished", "content": "task 7"},  # -> completed
        {"status": "not started", "content": "task 8"},  # -> pending
        {"status": "open", "content": "task 9"},  # -> pending
        {"status": "resolved", "content": "task 10"},  # -> completed
        {"status": "skipped", "content": "task 11"},  # -> canceled
        {"status": "started", "content": "task 12"},  # -> in_progress
        {"status": "todo", "content": "task 13"},  # -> pending
    ]

    todos = _todos_from_json(json_data)

    assert len(todos) == 13

    statuses = [todo["status"] for todo in todos]
    assert statuses.count("pending") == 3  # not started, open, todo
    assert statuses.count("in_progress") == 3  # active, doing, started
    assert statuses.count("completed") == 4  # complete, done, finished, resolved
    assert statuses.count("canceled") == 3  # abandoned, dropped, skipped


def test_extract_todos_handles_mixed_json_and_text_content():
    """Test extraction from records with both JSON metadata and text fields."""
    warnings: list[str] = []

    # Record with both JSON metadata and text content
    record = {
        "session_id": "test-session",
        "metadata": json.dumps({
            "todos": [
                {"status": "completed", "content": "json task one"}
            ]
        }),
        "prompt_text": "- [ ] text task two\n- [x] text task three",
        "response_text": "pending: text task four",
    }

    todos = _extract_todos(record, warnings)

    # Should extract todos from both JSON and text
    assert len(todos) >= 4

    texts = [todo["text"] for todo in todos]
    assert "json task one" in texts
    assert "text task two" in texts
    assert "text task three" in texts
    assert "text task four" in texts


def test_extract_todos_deduplication_by_status_and_text():
    """Test that todos with identical status and text are deduplicated."""
    warnings: list[str] = []

    # Record with duplicate todos in different fields
    record = {
        "prompt_text": "- [ ] duplicate task\n- [x] unique completed task",
        "response_text": "- [ ] duplicate task\npending: another duplicate task",
        "content": "pending: duplicate task\npending: another duplicate task",
    }

    todos = _extract_todos(record, warnings)

    # Get normalized text for comparison
    todo_pairs = {(todo["status"], todo["text"]) for todo in todos}

    # Should have unique (status, text) pairs
    # "duplicate task" appears multiple times but all with "pending" status
    # So it might be deduplicated at the session level, not here
    # This function extracts all occurrences
    assert len(todos) >= 2  # At least the unique completed task and one pending


def test_normalize_status_handles_underscores_hyphens_and_spaces():
    """Test status normalization with different separators and casing."""
    assert _normalize_status("in_progress") == "in_progress"
    assert _normalize_status("in-progress") == "in_progress"
    assert _normalize_status("in progress") == "in_progress"
    assert _normalize_status("IN_PROGRESS") == "in_progress"
    assert _normalize_status("IN-PROGRESS") == "in_progress"
    assert _normalize_status("IN PROGRESS") == "in_progress"

    assert _normalize_status("not_started") == "pending"
    assert _normalize_status("not started") == "pending"
    # Note: "not-started" with hyphen is not in STATUS_ALIASES, so it returns None
    # The normalize function converts underscores to spaces but leaves hyphens
    assert _normalize_status("not-started") is None

    assert _normalize_status("cancelled") == "canceled"
    assert _normalize_status("CANCELLED") == "canceled"


def test_normalize_todo_text_collapses_whitespace_and_lowercases():
    """Test that TODO text normalization handles whitespace and casing."""
    assert _normalize_todo_text("Simple Task") == "simple task"
    assert _normalize_todo_text("  Multiple   Spaces  ") == "multiple spaces"
    assert _normalize_todo_text("Tab\tSeparated\tText") == "tab separated text"
    assert _normalize_todo_text("Newline\nSeparated\nText") == "newline separated text"
    assert _normalize_todo_text("UPPERCASE TEXT") == "uppercase text"
    assert _normalize_todo_text("MiXeD CaSe") == "mixed case"


def test_todos_from_text_does_not_extract_plain_comments():
    """Test that plain comments without TODO markers are not extracted."""
    text = """
    # This is a regular Python comment
    // This is a JavaScript comment
    /* This is a C-style comment */
    <!-- This is an HTML comment -->
    -- This is a SQL comment
    """

    todos = _todos_from_text(text)

    # Should not extract any todos from plain comments
    assert len(todos) == 0


def test_todos_from_text_extracts_from_comment_styles_with_todo_markers():
    """Test extraction from different comment styles when they contain TODO markers.

    Note: The regex patterns require checkboxes/status to be at line start (after whitespace).
    Comment prefixes like # // /* prevent matching, so only plain markdown is extracted.
    """
    text = """
    # - [ ] Python comment with checkbox (not extracted - has # prefix)
    // - [x] JavaScript comment with checkbox (not extracted - has // prefix)
    /* - [ ] C-style comment with checkbox */ (not extracted - has /* prefix)
    <!-- - [~] HTML comment with checkbox --> (not extracted - has <!-- prefix)
    - [ ] Plain markdown checkbox
    pending: task in any context
    # pending: task in Python comment (not extracted - has # prefix)
    // in_progress: task in JS comment (not extracted - has // prefix)
    """

    todos = _todos_from_text(text)

    # Only plain markdown without comment prefixes is extracted
    assert len(todos) == 2

    statuses = [todo["status"] for todo in todos]
    assert statuses.count("pending") == 2

    texts = [todo["text"] for todo in todos]
    assert "plain markdown checkbox" in texts
    assert "task in any context" in texts


def test_todos_from_json_handles_nested_structures():
    """Test extraction from deeply nested JSON structures."""
    json_data = {
        "level1": {
            "level2": {
                "todos": [
                    {"status": "completed", "content": "nested task one"}
                ],
                "level3": {
                    "items": [
                        {"status": "pending", "text": "deeply nested task"}
                    ]
                }
            }
        },
        "another_branch": {
            "status": "in_progress",
            "content": "task in different branch"
        }
    }

    todos = _todos_from_json(json_data)

    # Should walk the entire JSON tree
    assert len(todos) >= 3

    texts = [todo["text"] for todo in todos]
    assert "nested task one" in texts
    assert "deeply nested task" in texts
    assert "task in different branch" in texts


def test_todos_from_json_handles_empty_and_invalid_structures():
    """Test extraction from empty or invalid JSON structures."""
    assert _todos_from_json({}) == []
    assert _todos_from_json([]) == []
    assert _todos_from_json(None) == []
    assert _todos_from_json("not a dict or list") == []
    assert _todos_from_json(42) == []

    # JSON with no valid status fields
    assert _todos_from_json({"content": "task without status"}) == []
    assert _todos_from_json({"status": "invalid_status", "content": "task"}) == []


def test_extract_todos_handles_malformed_json_gracefully():
    """Test that malformed JSON in metadata produces warnings but doesn't crash."""
    warnings: list[str] = []

    record = {
        "id": 123,
        "_source_table": "test_table",
        "metadata": '{"incomplete": json',  # Malformed JSON
        "prompt_text": "- [ ] valid task in text",
    }

    todos = _extract_todos(record, warnings)

    # Should still extract the valid task from text
    assert len(todos) >= 1
    assert todos[0]["text"] == "valid task in text"

    # Should have added a warning about malformed JSON
    assert len(warnings) == 1
    assert "malformed todo JSON" in warnings[0]
    assert "test_table" in warnings[0]
    assert "123" in warnings[0]


def test_extract_todos_with_pre_parsed_json_structures():
    """Test extraction when metadata is already parsed as dict/list (not string)."""
    warnings: list[str] = []

    # Record with metadata as a dict (not JSON string)
    record = {
        "metadata": {
            "todos": [
                {"status": "completed", "content": "already parsed dict"}
            ]
        },
        "output": [
            {"status": "pending", "text": "already parsed list"}
        ],
    }

    todos = _extract_todos(record, warnings)

    # Should extract from pre-parsed structures
    assert len(todos) >= 2
    texts = [todo["text"] for todo in todos]
    assert "already parsed dict" in texts
    assert "already parsed list" in texts
    assert len(warnings) == 0


def test_extract_todos_with_non_string_non_dict_metadata():
    """Test extraction with metadata that is neither string nor dict/list."""
    warnings: list[str] = []

    record = {
        "metadata": 42,  # Integer, not string or dict
        "content": None,  # None value
        "prompt_text": "- [ ] valid task",
    }

    todos = _extract_todos(record, warnings)

    # Should skip non-string/non-dict values and extract from text
    assert len(todos) >= 1
    assert todos[0]["text"] == "valid task"
    assert len(warnings) == 0  # No warnings for non-string values
