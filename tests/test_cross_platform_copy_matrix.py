"""Tests for cross-platform copy matrix reporting."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from cross_platform_copy_matrix import main  # noqa: E402
from evaluation.cross_platform_copy_matrix import (  # noqa: E402
    build_cross_platform_copy_matrix,
    build_cross_platform_copy_matrix_report,
    format_cross_platform_copy_matrix_markdown,
)


def _content(db, text: str, *, content_type: str = "x_post") -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )


def _variant(
    db,
    content_id: int,
    platform: str,
    text: str,
    *,
    variant_type: str = "post",
    selected: bool = True,
) -> int:
    variant_id = db.upsert_content_variant(
        content_id=content_id,
        platform=platform,
        variant_type=variant_type,
        content=text,
        metadata={"source": "test"},
    )
    if selected:
        db.select_content_variant(content_id, platform, variant_type)
    return variant_id


def _link_topic_to_campaign(db, *, content_id: int, campaign_id: int) -> int:
    topic_id = db.insert_planned_topic(
        "launch",
        angle="copy coverage",
        campaign_id=campaign_id,
        status="generated",
    )
    db.conn.execute(
        "UPDATE planned_topics SET content_id = ? WHERE id = ?",
        (content_id, topic_id),
    )
    db.conn.commit()
    return topic_id


def test_matrix_counts_multiple_platform_variants_and_source_copy():
    report = build_cross_platform_copy_matrix(
        [
            {
                "id": 10,
                "content_type": "x_post",
                "content": "Ship notes 🚀 https://example.com #Launch",
            }
        ],
        [
            {
                "id": 1,
                "content_id": 10,
                "platform": "x",
                "variant_type": "post",
                "content": "X copy https://x.test #Ship",
                "selected": 1,
            },
            {
                "id": 2,
                "content_id": 10,
                "platform": "linkedin",
                "variant_type": "post",
                "content": "LinkedIn copy #Build",
                "selected": 1,
            },
        ],
        platforms=["x", "linkedin"],
    )

    row = report["rows"][0]
    assert row["source"]["counts"]["graphemes"] == 40
    assert row["source"]["counts"]["urls"] == 1
    assert row["source"]["counts"]["hashtags"] == 1
    assert row["platforms"]["x"]["counts"] == {
        "characters": 27,
        "graphemes": 27,
        "urls": 1,
        "has_links": True,
        "hashtags": 1,
    }
    assert row["platforms"]["linkedin"]["counts"]["hashtags"] == 1
    assert row["platforms"]["x"]["available_variants"][0]["counts"]["urls"] == 1
    assert report["totals"]["gaps"] == 0


def test_missing_platform_and_missing_selected_variant_are_explicit_gaps():
    report = build_cross_platform_copy_matrix(
        [{"id": 11, "content": "Source"}],
        [
            {
                "id": 3,
                "content_id": 11,
                "platform": "bluesky",
                "variant_type": "post",
                "content": "Available but not selected",
                "selected": 0,
            }
        ],
        platforms=["x", "bluesky"],
    )

    platforms = report["rows"][0]["platforms"]
    assert platforms["x"]["gap_reason"] == "missing_variant"
    assert platforms["x"]["counts"] is None
    assert platforms["x"]["available_variants"] == []
    assert platforms["bluesky"]["gap_reason"] == "missing_selected_variant"
    assert platforms["bluesky"]["counts"]["graphemes"] == 26
    assert report["gaps"] == [
        {"content_id": 11, "platform": "x", "reason": "missing_variant"},
        {
            "content_id": 11,
            "platform": "bluesky",
            "reason": "missing_selected_variant",
        },
    ]


def test_selected_variant_is_preferred_over_newer_unselected_variant(db):
    content_id = _content(db, "Original")
    _variant(db, content_id, "x", "Selected copy", variant_type="post", selected=True)
    _variant(
        db,
        content_id,
        "x",
        "Newer unselected copy",
        variant_type="thread",
        selected=False,
    )

    report = build_cross_platform_copy_matrix_report(
        db,
        content_ids=[content_id],
        platforms=["x"],
    )

    entry = report["rows"][0]["platforms"]["x"]
    assert entry["text"] == "Selected copy"
    assert entry["variant"]["variant_type"] == "post"
    assert entry["gap"] is False


def test_campaign_report_includes_one_row_per_content_item_and_platform_gaps(db):
    campaign_id = db.create_campaign(name="Spring Launch", status="active")
    first_id = _content(db, "First source")
    second_id = _content(db, "Second source")
    outside_id = _content(db, "Outside source")
    _link_topic_to_campaign(db, content_id=first_id, campaign_id=campaign_id)
    _link_topic_to_campaign(db, content_id=second_id, campaign_id=campaign_id)
    _variant(db, first_id, "x", "First X", selected=True)
    _variant(db, second_id, "newsletter", "Second newsletter", selected=True)
    _variant(db, outside_id, "x", "Outside X", selected=True)

    report = build_cross_platform_copy_matrix_report(
        db,
        campaign="Spring Launch",
        platforms=["x", "newsletter"],
    )

    assert [row["content_id"] for row in report["rows"]] == [first_id, second_id]
    first, second = report["rows"]
    assert first["platforms"]["newsletter"]["gap_reason"] == "missing_variant"
    assert second["platforms"]["x"]["gap_reason"] == "missing_variant"
    assert report["campaign"]["name"] == "Spring Launch"
    assert report["totals"]["gaps"] == 2


def test_markdown_output_contains_matrix_columns_counts_and_gaps():
    report = build_cross_platform_copy_matrix(
        [{"id": 12, "content_type": "x_post", "content": "Source"}],
        [],
        platforms=["x"],
    )

    markdown = format_cross_platform_copy_matrix_markdown(report)

    assert "# Cross-platform copy matrix" in markdown
    assert "| Content ID | Type | Source | X | Gaps |" in markdown
    assert "Source (6g, 0 url, 0 #)" in markdown
    assert "**GAP** missing_variant" in markdown
    assert "x:missing_variant" in markdown


def test_cli_supports_json_output_and_content_id_filter(db, capsys):
    content_id = _content(db, "CLI source")
    _variant(db, content_id, "blog", "Blog copy #Longform", selected=True)
    fixed_report = build_cross_platform_copy_matrix_report(
        db,
        content_ids=[content_id],
        platforms=["blog"],
    )

    @contextmanager
    def fake_script_context():
        yield SimpleNamespace(), db

    with patch("cross_platform_copy_matrix.script_context", fake_script_context), patch(
        "cross_platform_copy_matrix.build_cross_platform_copy_matrix_report",
        return_value=fixed_report,
    ):
        result = main(
            [
                "--content-id",
                str(content_id),
                "--platform",
                "blog",
                "--format",
                "json",
            ]
        )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["platforms"] == ["blog"]
    assert payload["rows"][0]["platforms"]["blog"]["text"] == "Blog copy #Longform"
