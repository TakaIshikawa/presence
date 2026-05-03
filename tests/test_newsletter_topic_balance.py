"""Tests for newsletter topic balance reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.newsletter_topic_balance import (
    build_newsletter_topic_balance_report,
    format_newsletter_topic_balance_json,
    format_newsletter_topic_balance_markdown,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_topic_balance.py"
spec = importlib.util.spec_from_file_location("newsletter_topic_balance_script", SCRIPT_PATH)
newsletter_topic_balance_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_topic_balance_script)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    content: str,
    topic: str | None = None,
    confidence: float = 1.0,
    eval_score: float = 8.0,
    created_days_ago: int = 1,
    content_type: str = "x_post",
    content_format: str | None = None,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=eval_score,
        eval_feedback="usable",
        content_format=content_format,
    )
    created_at = (NOW - timedelta(days=created_days_ago)).isoformat()
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at, content_id),
    )
    if topic:
        db.insert_content_topics(content_id, [(topic, "", confidence)])
    db.conn.commit()
    return content_id


def test_explicit_topic_metadata_is_used_before_keyword_fallback(db):
    content_id = _content(
        db,
        content="This pytest fixture made the test workflow easier to debug.",
        topic="Editorial Strategy",
    )

    report = build_newsletter_topic_balance_report(db, days=7, now=NOW)

    assert report.total_items == 1
    assert report.items[0].content_id == content_id
    assert report.items[0].topic == "editorial strategy"
    assert report.items[0].topic_source == "content_topics"
    assert report.topics[0].topic == "editorial strategy"
    assert report.topics[0].count == 1
    assert report.topics[0].share == 1.0


def test_items_without_metadata_receive_deterministic_keyword_buckets(db):
    testing_id = _content(
        db,
        content="A pytest fixture caught the regression before release.",
    )
    model_id = _content(
        db,
        content="The Claude agent prompt needed a clearer tool call boundary.",
    )
    general_id = _content(
        db,
        content="A short note about keeping the weekly draft readable.",
    )

    report = build_newsletter_topic_balance_report(db, days=7, now=NOW)
    by_id = {item.content_id: item for item in report.items}

    assert by_id[testing_id].topic == "testing"
    assert by_id[testing_id].topic_source == "keyword_fallback"
    assert by_id[model_id].topic == "ai-agents"
    assert by_id[general_id].topic == "general"


def test_overrepresented_topics_include_trim_candidates(db):
    keep_id = _content(db, content="Testing coverage for the release path.", topic="testing", eval_score=9.0)
    trim_first = _content(db, content="Another testing fixture update.", topic="testing", eval_score=5.0)
    trim_second = _content(db, content="Regression test notes.", topic="testing", eval_score=6.0)
    _content(db, content="Schema design note.", topic="architecture", eval_score=8.0)
    _content(db, content="Agent prompt note.", topic="ai-agents", eval_score=8.0)

    report = build_newsletter_topic_balance_report(
        db,
        days=7,
        max_topic_share=0.4,
        now=NOW,
    )
    testing = next(row for row in report.topics if row.topic == "testing")

    assert testing.count == 3
    assert testing.share == 0.6
    assert testing.overrepresented is True
    assert testing.item_ids == tuple(sorted((keep_id, trim_first, trim_second)))
    assert testing.recommended_trim_item_ids == (trim_first,)


def test_markdown_is_sorted_by_descending_topic_share(db):
    _content(db, content="Testing note one.", topic="testing")
    _content(db, content="Testing note two.", topic="testing")
    _content(db, content="Architecture note.", topic="architecture")

    report = build_newsletter_topic_balance_report(
        db,
        days=7,
        max_topic_share=0.5,
        now=NOW,
    )
    markdown = format_newsletter_topic_balance_markdown(report)

    testing_line = markdown.index("| testing | 2 | 66.7% |")
    architecture_line = markdown.index("| architecture | 1 | 33.3% |")
    assert testing_line < architecture_line
    assert "## Overrepresented Topics" in markdown
    assert "- testing: 66.7% (2/3); trim candidates:" in markdown


def test_item_id_filter_and_json_output_are_stable(db):
    included = _content(db, content="Testing note.", topic="testing")
    _content(db, content="Architecture note.", topic="architecture")

    report = build_newsletter_topic_balance_report(
        db,
        days=7,
        item_ids=[included],
        now=NOW,
    )
    payload = json.loads(format_newsletter_topic_balance_json(report))

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "newsletter_topic_balance"
    assert payload["filters"]["item_ids"] == [included]
    assert payload["topics"][0]["item_ids"] == [included]


def test_cli_supports_markdown_and_json_format(db, capsys):
    content_id = _content(db, content="Testing CLI report.", topic="testing")

    with patch.object(
        newsletter_topic_balance_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = newsletter_topic_balance_script.main(
            ["--days", "7", "--max-topic-share", "0.5"]
        )

    markdown = capsys.readouterr().out
    assert exit_code == 0
    assert "# Newsletter Topic Balance" in markdown
    assert f"| testing | 1 | 100.0% | {content_id} |" in markdown

    with patch.object(
        newsletter_topic_balance_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = newsletter_topic_balance_script.main(
            ["--item-ids", str(content_id), "--format", "json"]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["items"][0]["content_id"] == content_id
