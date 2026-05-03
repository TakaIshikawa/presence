"""Tests for reply tone consistency auditing."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from engagement.reply_tone_consistency_audit import (
    build_reply_tone_baseline,
    build_reply_tone_consistency_audit,
    format_reply_tone_consistency_audit_json,
    format_reply_tone_consistency_audit_markdown,
    inspect_reply_tone_consistency,
    inspect_reply_tone_metrics,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_tone_consistency_audit.py"
spec = importlib.util.spec_from_file_location("reply_tone_consistency_audit_script", SCRIPT_PATH)
reply_tone_consistency_audit_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_tone_consistency_audit_script)


@contextmanager
def _script_context(conn):
    yield SimpleNamespace(), conn


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            status TEXT,
            platform TEXT,
            inbound_author_handle TEXT,
            draft_text TEXT,
            detected_at TEXT,
            reviewed_at TEXT,
            posted_at TEXT
        )"""
    )
    return conn


def _insert(conn: sqlite3.Connection, **kwargs) -> int:
    defaults = {
        "status": "pending",
        "platform": "x",
        "inbound_author_handle": "alice",
        "draft_text": "I would check the retry budget before changing the worker timeout.",
        "detected_at": "2026-05-02T10:00:00+00:00",
        "reviewed_at": None,
        "posted_at": None,
    }
    defaults.update(kwargs)
    columns = tuple(defaults)
    placeholders = ", ".join("?" for _ in columns)
    cursor = conn.execute(
        f"INSERT INTO reply_queue ({', '.join(columns)}) VALUES ({placeholders})",
        tuple(defaults[column] for column in columns),
    )
    conn.commit()
    return int(cursor.lastrowid)


def _baseline(conn: sqlite3.Connection) -> None:
    for index, text in enumerate(
        (
            "I would check the retry budget before changing the worker timeout.",
            "The safer path is to measure queue depth, then tune concurrency.",
            "That failure mode usually points at cache invalidation rather than auth.",
        ),
        start=1,
    ):
        _insert(
            conn,
            status="approved" if index < 3 else "posted",
            draft_text=text,
            reviewed_at=f"2026-05-01T0{index}:00:00+00:00",
        )


def test_pure_metrics_and_inspector_flag_generic_promotional_and_deferential_reasons():
    baseline = build_reply_tone_baseline(
        [
            "I would measure queue depth before changing the worker timeout.",
            "That looks like a cache invalidation issue, not auth.",
            "The deploy path should keep the canary and rollback separate.",
        ]
    )
    row = {
        "id": 10,
        "status": "pending",
        "platform": "x",
        "inbound_author_handle": "casey",
        "draft_text": (
            "Thanks for sharing, great point. Sorry, I might be wrong, but you should "
            "check out our product and sign up."
        ),
        "detected_at": "2026-05-02T10:00:00+00:00",
    }

    metrics = inspect_reply_tone_metrics(row["draft_text"])
    finding = inspect_reply_tone_consistency(row, baseline)

    assert metrics.generic_phrase_count == 2
    assert metrics.promotional_phrase_count == 3
    assert metrics.deferential_phrase_count == 2
    assert finding is not None
    assert finding.severity == "high"
    assert finding.reasons == ("overly_generic", "too_promotional", "too_deferential")


def test_report_flags_generic_promotional_deferential_and_excessive_length_drafts():
    conn = _conn()
    _baseline(conn)
    generic_id = _insert(
        conn,
        draft_text="Thanks for sharing, great point. This is helpful context.",
    )
    promo_id = _insert(
        conn,
        draft_text="You can check out our platform, book a demo, and try our workflow.",
    )
    deferential_id = _insert(
        conn,
        draft_text="Sorry, I might be wrong. Happy to clarify if that makes sense.",
    )
    long_id = _insert(
        conn,
        draft_text=(
            "I would start by separating the incident into discovery, mitigation, and follow-up. "
            "Then I would compare the queue depth, retry rate, worker saturation, cache hit rate, "
            "and deployment timeline before changing concurrency because each of those points to "
            "a different rollback or rollout path for the team."
        ),
    )
    _insert(conn, draft_text="I would check queue depth before changing the worker timeout.")

    report = build_reply_tone_consistency_audit(conn, min_baseline=3, now=NOW)
    payload = json.loads(format_reply_tone_consistency_audit_json(report))
    by_id = {item["reply_queue_id"]: item for item in payload["findings"]}

    assert report.ok is False
    assert payload["artifact_type"] == "reply_tone_consistency_audit"
    assert payload["baseline"]["sample_count"] == 3
    assert payload["audited_count"] == 5
    assert payload["finding_count"] == 4
    assert by_id[generic_id]["reasons"] == ["overly_generic"]
    assert by_id[promo_id]["reasons"] == ["too_promotional"]
    assert by_id[promo_id]["severity"] == "high"
    assert by_id[deferential_id]["reasons"] == ["too_deferential"]
    assert by_id[long_id]["reasons"] == ["excessive_length"]
    assert by_id[long_id]["severity"] == "medium"
    assert payload["by_reason"] == {
        "excessive_length": 1,
        "overly_generic": 1,
        "too_deferential": 1,
        "too_promotional": 1,
    }
    assert "# Reply Tone Consistency Audit" in format_reply_tone_consistency_audit_markdown(report)


def test_configurable_lookback_status_platform_limit_and_min_baseline():
    conn = _conn()
    _baseline(conn)
    _insert(conn, draft_text="Thanks for sharing, great point. This is helpful context.")
    _insert(
        conn,
        platform="bluesky",
        draft_text="Check out our product and sign up.",
    )
    _insert(
        conn,
        status="draft",
        draft_text="Sorry, I might be wrong. Happy to clarify if that makes sense.",
    )
    _insert(
        conn,
        draft_text="Check out our product and sign up.",
        detected_at="2026-03-01T10:00:00+00:00",
    )

    report = build_reply_tone_consistency_audit(
        conn,
        days=7,
        min_baseline=3,
        queued_statuses=("pending",),
        platform="x",
        limit=1,
        now=NOW,
    )

    assert report.filters["platform"] == ["x"]
    assert report.audited_count == 1
    assert report.finding_count == 1
    assert report.findings[0].reasons == ("overly_generic",)


def test_insufficient_baseline_skips_audit_with_warning():
    conn = _conn()
    _insert(conn, status="approved", draft_text="I would measure queue depth first.")
    _insert(conn, draft_text="Thanks for sharing, great point. Check out our platform.")

    report = build_reply_tone_consistency_audit(conn, min_baseline=3, now=NOW)
    payload = report.to_dict()

    assert report.ok is True
    assert payload["baseline"]["sample_count"] == 1
    assert payload["audited_count"] == 0
    assert payload["findings"] == []
    assert payload["warnings"] == ["insufficient_baseline: sample_count=1 min_baseline=3"]
    assert "insufficient_baseline" in format_reply_tone_consistency_audit_markdown(report)


def test_missing_reply_queue_or_required_columns_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing_table = build_reply_tone_consistency_audit(conn, now=NOW)

    conn.execute("CREATE TABLE reply_queue (id INTEGER PRIMARY KEY)")
    conn.execute("INSERT INTO reply_queue (id) VALUES (1)")
    conn.commit()
    missing_columns = build_reply_tone_consistency_audit(conn, now=NOW)

    assert missing_table.ok is True
    assert missing_table.missing_tables == ("reply_queue",)
    assert missing_columns.ok is True
    assert missing_columns.missing_columns == {"reply_queue": ("draft_text",)}


def test_cli_outputs_json_and_markdown_and_returns_issue_status(monkeypatch, capsys):
    conn = _conn()
    _baseline(conn)
    _insert(conn, draft_text="Check out our product and sign up.")
    monkeypatch.setattr(
        reply_tone_consistency_audit_script,
        "script_context",
        lambda: _script_context(conn),
    )

    json_exit = reply_tone_consistency_audit_script.main(
        ["--format", "json", "--min-baseline", "3", "--limit", "5"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert json_exit == 1
    assert payload["artifact_type"] == "reply_tone_consistency_audit"
    assert payload["findings"][0]["reasons"] == ["too_promotional"]

    markdown_exit = reply_tone_consistency_audit_script.main(
        ["--format", "markdown", "--min-baseline", "3"]
    )
    markdown = capsys.readouterr().out
    assert markdown_exit == 1
    assert "# Reply Tone Consistency Audit" in markdown
    assert "reply_queue:" in markdown

    invalid = reply_tone_consistency_audit_script.main(["--days", "0"])
    captured = capsys.readouterr()
    assert invalid == 2
    assert "value must be positive" in captured.err
