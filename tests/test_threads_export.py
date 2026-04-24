"""Tests for manual Threads publishing artifacts."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from output.threads_export import (  # noqa: E402
    THREADS_CHARACTER_LIMIT,
    build_threads_export,
    build_threads_exports_from_db,
    threads_exports_to_json,
)


def _insert_content(
    db,
    content: str,
    content_type: str = "x_post",
    source_commits: list[str] | None = None,
) -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=source_commits or ["abc123"],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="Good",
    )


def test_single_post_export_uses_generated_content():
    export = build_threads_export(
        {
            "id": 7,
            "content_type": "x_post",
            "content": "A small Threads-ready note.",
            "source_commits": '["abc123"]',
            "source_messages": "[]",
            "source_activity_ids": "[]",
        },
        scheduled_at="2026-04-25T09:00:00+09:00",
    )
    payload = json.loads(threads_exports_to_json([export]))[0]

    assert payload["platform"] == "threads"
    assert payload["content_id"] == 7
    assert payload["source_content_id"] == 7
    assert payload["text"] == "A small Threads-ready note."
    assert "thread_parts" not in payload
    assert payload["scheduled_at"] == "2026-04-25T09:00:00+09:00"
    assert payload["provenance"]["source_commits"] == ["abc123"]
    assert payload["validation_warnings"] == []


def test_thread_export_uses_threads_variant_parts(db):
    content_id = _insert_content(
        db,
        "TWEET 1:\nOriginal hook\nTWEET 2:\nOriginal detail",
        content_type="x_thread",
    )
    db.upsert_content_variant(
        content_id,
        "threads",
        "thread",
        "TWEET 1:\nVariant hook\nTWEET 2:\nVariant detail",
        metadata={"reviewed": True},
    )

    exports = build_threads_exports_from_db(db, content_id=content_id)
    payload = json.loads(threads_exports_to_json(exports))[0]

    assert payload["text"] == "Variant hook\n\nVariant detail"
    assert payload["thread_parts"] == ["Variant hook", "Variant detail"]
    assert payload["variant"]["platform"] == "threads"
    assert payload["variant"]["variant_type"] == "thread"
    assert payload["variant"]["metadata"] == {"reviewed": True}


def test_warning_generation_for_limits_and_missing_provenance():
    long_part = "x" * (THREADS_CHARACTER_LIMIT + 1)
    export = build_threads_export(
        {
            "id": 8,
            "content_type": "x_thread",
            "content": f"TWEET 1:\n{long_part}\nTWEET 2:\nshort",
            "source_commits": "[]",
            "source_messages": "[]",
            "source_activity_ids": "[]",
        }
    )

    assert any("part 1 exceeds Threads character limit" in warning for warning in export.validation_warnings)
    assert "missing provenance" in export.validation_warnings


def test_export_threads_cli_writes_json_stdout(file_db, capsys):
    content_id = _insert_content(file_db, "CLI Threads post.", source_commits=["def456"])

    import export_threads

    exit_code = export_threads.main(
        [
            "--db",
            str(file_db.db_path),
            "--content-id",
            str(content_id),
            "--json",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload[0]["platform"] == "threads"
    assert payload[0]["content_id"] == content_id
    assert payload[0]["text"] == "CLI Threads post."


def test_export_threads_cli_empty_result_exits_cleanly(file_db, capsys):
    import export_threads

    exit_code = export_threads.main(
        [
            "--db",
            str(file_db.db_path),
            "--content-id",
            "9999",
            "--json",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert json.loads(captured.out) == []
    assert captured.err == ""
