"""Tests for curated author overexposure reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path

from evaluation.curated_author_overexposure import (
    build_curated_author_overexposure_report,
    build_curated_author_overexposure_report_from_db,
    format_curated_author_overexposure_json,
    format_curated_author_overexposure_text,
    normalize_curated_author,
)


NOW = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "curated_author_overexposure.py"
spec = importlib.util.spec_from_file_location("curated_author_overexposure_script", SCRIPT_PATH)
curated_author_overexposure_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(curated_author_overexposure_script)


def _row(content_id: int, author: str | None, days_ago: int = 1, **extra):
    return {
        "content_id": str(content_id),
        "author": author,
        "source_type": "generated_content",
        "created_at": (NOW - timedelta(days=days_ago)).isoformat(),
        **extra,
    }


def test_normalizes_author_identifiers_from_fields_urls_handles_and_metadata():
    assert normalize_curated_author({"author": "@Alice"}) == "alice"
    assert normalize_curated_author({"author": "https://twitter.com/Alice/"}) == "alice"
    assert normalize_curated_author({"source_url": "https://bsky.app/profile/alice.example/post/123"}) == "alice.example"
    assert normalize_curated_author({"metadata": {"account": "@Alice"}}) == "alice"
    assert normalize_curated_author({"content": "via @Alice on this thread"}) == "alice"


def test_flags_author_above_share_threshold_with_alternates():
    rows = [
        _row(1, "@Alice"),
        _row(2, "https://x.com/alice"),
        _row(3, "ALICE"),
        _row(4, "@Bob"),
        _row(5, "@Cara"),
    ]

    report = build_curated_author_overexposure_report(
        rows,
        days=30,
        share_threshold=0.6,
        min_items=3,
        now=NOW,
    )
    flagged = report["overexposed_authors"][0]

    assert flagged["author"] == "alice"
    assert flagged["exposure_share"] == 0.6
    assert flagged["affected_content_ids"] == ["1", "2", "3"]
    assert flagged["suggested_alternates"] == ["bob", "cara"]


def test_healthy_mix_and_old_rows_do_not_flag():
    report = build_curated_author_overexposure_report(
        [
            _row(1, "@Alice"),
            _row(2, "@Bob"),
            _row(3, "@Cara"),
            _row(4, "@Alice", days_ago=90),
        ],
        days=30,
        share_threshold=0.5,
        min_items=2,
        now=NOW,
    )

    assert report["overexposed_authors"] == []
    assert report["empty_state"]["is_empty"] is True
    assert "No curated author overexposure found." in format_curated_author_overexposure_text(report)


def test_db_loader_reads_knowledge_generated_content_and_newsletters(db):
    for index in range(3):
        db.conn.execute(
            """INSERT INTO knowledge
               (source_type, source_id, source_url, author, content, insight, approved, published_at, ingested_at)
               VALUES ('curated_x', ?, ?, ?, 'content', 'insight', 1, ?, ?)""",
            (
                f"alice-{index}",
                f"https://x.com/alice/status/{index}",
                "@Alice",
                (NOW - timedelta(days=index + 1)).isoformat(),
                (NOW - timedelta(days=index + 1)).isoformat(),
            ),
        )
    db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published, created_at)
           VALUES (?, 'x_post', 8.0, 0, ?)""",
        ("Draft citing @Bob", (NOW - timedelta(days=1)).isoformat()),
    )
    db.conn.execute(
        """INSERT INTO newsletter_sends
           (issue_id, subject, source_content_ids, status, metadata, sent_at)
           VALUES ('n1', 'Issue', '[1]', 'sent', ?, ?)""",
        (json.dumps({"author": "@Alice"}), (NOW - timedelta(days=2)).isoformat()),
    )
    db.conn.commit()

    report = build_curated_author_overexposure_report_from_db(
        db,
        days=30,
        share_threshold=0.75,
        min_items=3,
        now=NOW,
    )

    assert report["totals"]["row_count"] == 5
    assert report["overexposed_authors"][0]["author"] == "alice"
    assert report["overexposed_authors"][0]["affected_content_ids"] == ["1", "2", "3"]
    assert report["overexposed_authors"][0]["suggested_alternates"] == ["bob"]


def test_json_text_and_cli_validation_are_deterministic(db, file_db, capsys):
    report = build_curated_author_overexposure_report(
        [_row(2, "@Alice"), _row(1, "@Alice"), _row(3, "@Bob")],
        days=30,
        share_threshold=0.5,
        min_items=2,
        now=NOW,
    )
    payload = json.loads(format_curated_author_overexposure_json(report))
    assert list(payload) == sorted(payload)
    assert "affected=1,2" in format_curated_author_overexposure_text(report)

    file_db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight, approved, published_at)
           VALUES ('curated_x', 'cli-1', 'https://x.com/cli/status/1', '@CLI', 'content', 'insight', 1, ?)""",
        ((NOW - timedelta(days=1)).isoformat(),),
    )
    file_db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight, approved, published_at)
           VALUES ('curated_x', 'cli-2', 'https://x.com/cli/status/2', '@CLI', 'content', 'insight', 1, ?)""",
        ((NOW - timedelta(days=2)).isoformat(),),
    )
    file_db.conn.commit()

    assert curated_author_overexposure_script.main(["--db", str(file_db.db_path), "--days", "36500", "--share-threshold", "0.5", "--min-items", "2"]) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["overexposed_authors"][0]["author"] == "cli"

    assert curated_author_overexposure_script.main(["--share-threshold", "2"]) == 2
    assert "value must be between 0 and 1" in capsys.readouterr().err
