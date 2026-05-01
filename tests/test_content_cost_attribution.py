"""Tests for content-level model spend attribution."""

from __future__ import annotations

import importlib.util
import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.content_cost_attribution import (
    ContentCostAttribution,
    export_to_json,
    format_text_report,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "content_cost_attribution.py"
)
spec = importlib.util.spec_from_file_location(
    "content_cost_attribution_script",
    SCRIPT_PATH,
)
content_cost_attribution_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(content_cost_attribution_script)

BASE_TIME = datetime.now(timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    content_type: str = "x_post",
    text: str = "Generated content",
    published: bool = False,
    tweet_id: str = "100",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    if published:
        db.mark_published(content_id, f"https://x.com/taka/status/{tweet_id}", tweet_id)
    return content_id


def _usage(
    db,
    *,
    content_id: int | None = None,
    pipeline_run_id: int | None = None,
    operation_name: str = "generate",
    model_name: str = "claude-sonnet",
    input_tokens: int = 100,
    output_tokens: int = 50,
    estimated_cost: float = 0.1,
    days_ago: int = 0,
) -> int:
    usage_id = db.record_model_usage(
        model_name=model_name,
        operation_name=operation_name,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        estimated_cost=estimated_cost,
        content_id=content_id,
        pipeline_run_id=pipeline_run_id,
    )
    db.conn.execute(
        "UPDATE model_usage SET created_at = ? WHERE id = ?",
        ((BASE_TIME - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S"), usage_id),
    )
    db.conn.commit()
    return usage_id


def test_aggregates_cost_tokens_operations_and_outcomes(db):
    content_id = _content(db, content_type="x_post", published=True, tweet_id="cost")
    run_id = db.insert_pipeline_run(
        batch_id="batch-cost",
        content_type="x_post",
        candidates_generated=2,
        best_candidate_index=0,
        best_score_before_refine=7.0,
        best_score_after_refine=8.0,
        final_score=8.0,
        published=True,
        content_id=content_id,
        outcome="published",
    )
    _usage(
        db,
        content_id=content_id,
        operation_name="generate",
        input_tokens=120,
        output_tokens=80,
        estimated_cost=0.12,
    )
    _usage(
        db,
        pipeline_run_id=run_id,
        operation_name="evaluate",
        input_tokens=60,
        output_tokens=40,
        estimated_cost=0.03,
    )
    db.insert_engagement(
        content_id=content_id,
        tweet_id="cost",
        like_count=8,
        retweet_count=1,
        reply_count=1,
        quote_count=0,
        engagement_score=10.0,
    )

    report = ContentCostAttribution(db).build_report(days=30, limit=10)

    assert report.total_content == 1
    assert report.total_estimated_cost == 0.15
    item = report.items[0]
    assert item.content_id == content_id
    assert item.estimated_cost == 0.15
    assert item.input_tokens == 180
    assert item.output_tokens == 120
    assert item.total_tokens == 300
    assert item.outcome == "published"
    assert item.published is True
    assert item.platform_statuses[0].platform == "x"
    assert item.platform_statuses[0].status == "published"
    assert item.engagement_score == 10.0
    assert item.cost_per_engagement == 0.015
    assert [operation.operation_name for operation in item.operations] == [
        "generate",
        "evaluate",
    ]


def test_filters_by_content_type_published_min_cost_and_limit(db):
    published_id = _content(db, content_type="x_post", published=True, tweet_id="pub")
    unpublished_id = _content(db, content_type="x_post", published=False)
    blog_id = _content(db, content_type="blog_post", published=True, tweet_id="blog")
    old_id = _content(db, content_type="x_post", published=True, tweet_id="old")

    _usage(db, content_id=published_id, estimated_cost=0.50)
    _usage(db, content_id=unpublished_id, estimated_cost=0.60)
    _usage(db, content_id=blog_id, estimated_cost=0.70)
    _usage(db, content_id=old_id, estimated_cost=0.90, days_ago=45)

    report = ContentCostAttribution(db).build_report(
        days=30,
        content_type="x_post",
        published="published",
        min_cost=0.4,
        limit=1,
    )
    unpublished_report = ContentCostAttribution(db).build_report(
        days=30,
        content_type="x_post",
        published="unpublished",
        min_cost=0.4,
        limit=10,
    )

    assert [item.content_id for item in report.items] == [published_id]
    assert [item.content_id for item in unpublished_report.items] == [unpublished_id]


def test_formats_stable_json_and_concise_text(db):
    content_id = _content(db, content_type="x_thread", published=False)
    _usage(db, content_id=content_id, operation_name="refine", estimated_cost=0.25)

    report = ContentCostAttribution(db).build_report(days=30)
    payload = json.loads(export_to_json(report))
    text = format_text_report(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["artifact_type"] == "content_cost_attribution"
    assert payload["items"][0]["content_id"] == content_id
    assert "Content Cost Attribution" in text
    assert f"content #{content_id}" in text
    assert "refine" in text


def test_script_outputs_json_and_text(db, capsys):
    content_id = _content(db, content_type="x_post", published=True, tweet_id="script")
    _usage(db, content_id=content_id, estimated_cost=0.33)

    with patch.object(
        content_cost_attribution_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = content_cost_attribution_script.main(["--json", "--published"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["items"][0]["content_id"] == content_id
    assert payload["published"] == "published"

    with patch.object(
        content_cost_attribution_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = content_cost_attribution_script.main(
            ["--content-type", "x_post", "--min-cost", "0.1"]
        )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Content Cost Attribution" in output
    assert "$0.3300" in output
