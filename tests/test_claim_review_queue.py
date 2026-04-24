"""Tests for claim review queue exports."""

from __future__ import annotations

import importlib.util
import json
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from evaluation.claim_review_queue import (
    build_claim_review_payload,
    format_json,
    format_markdown,
    list_claim_review_items,
)


NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


def _load_script_module():
    script_path = Path(__file__).resolve().parent.parent / "scripts" / "claim_review_queue.py"
    spec = importlib.util.spec_from_file_location("claim_review_queue_script", script_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _insert_content(
    db,
    *,
    content: str,
    content_type: str = "x_post",
    created_at: str = "2026-04-24T10:00:00+00:00",
    published: int = 0,
    unsupported_count: int = 1,
    annotation_text: str = "metric: Unsupported conversion lift (metric value not found in sources)",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=["sha-review"],
        source_messages=["msg-review"],
        content=content,
        eval_score=7.2,
        eval_feedback="clear",
        claim_check_summary={
            "supported_count": 2,
            "unsupported_count": unsupported_count,
            "annotation_text": annotation_text,
        },
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ?, published = ? WHERE id = ?",
        (created_at, published, content_id),
    )
    db.conn.commit()
    return content_id


def test_default_queue_only_includes_unpublished_content_with_unsupported_claims(db):
    included_id = _insert_content(db, content="Needs claim review.")
    _insert_content(db, content="Supported content.", unsupported_count=0)
    _insert_content(db, content="Published unsupported content.", published=1)

    items = list_claim_review_items(db, days=30, include_published=False, now=NOW)

    assert [item["content_id"] for item in items] == [included_id]
    assert items[0]["content_type"] == "x_post"
    assert items[0]["unsupported_count"] == 1
    assert items[0]["published"] is False
    assert "Unsupported conversion lift" in items[0]["annotation_text"]


def test_include_published_allows_published_unsupported_content(db):
    unpublished_id = _insert_content(db, content="Unpublished unsupported.")
    published_id = _insert_content(db, content="Published unsupported.", published=1)

    payload = build_claim_review_payload(db, days=30, include_published=True, now=NOW)

    assert {item["content_id"] for item in payload["items"]} == {
        unpublished_id,
        published_id,
    }
    assert any(item["published"] is True for item in payload["items"])


def test_queue_filters_by_days(db):
    _insert_content(
        db,
        content="Old unsupported.",
        created_at="2026-03-01T10:00:00+00:00",
    )

    assert list_claim_review_items(db, days=7, now=NOW) == []


def test_markdown_export_includes_review_fields(db):
    content_id = _insert_content(
        db,
        content="Markdown unsupported.",
        content_type="x_thread",
        unsupported_count=2,
        annotation_text="metric: unsupported one\nfactual: unsupported two",
    )
    payload = build_claim_review_payload(db, days=30, now=NOW)

    output = format_markdown(payload)

    assert f"## Content {content_id}" in output
    assert "- Type: x_thread" in output
    assert "- Unsupported claims: 2" in output
    assert "- Annotation summary: metric: unsupported one factual: unsupported two" in output
    assert "```text\nmetric: unsupported one\nfactual: unsupported two\n```" in output


def test_json_export_is_stable_and_tooling_friendly(db):
    content_id = _insert_content(db, content="JSON unsupported.")
    payload = build_claim_review_payload(db, days=30, now=NOW)

    parsed = json.loads(format_json(payload))

    assert parsed["days"] == 30
    assert parsed["include_published"] is False
    assert parsed["unsupported_only"] is True
    assert parsed["items"][0] == {
        "annotation_summary": (
            "metric: Unsupported conversion lift "
            "(metric value not found in sources)"
        ),
        "annotation_text": (
            "metric: Unsupported conversion lift "
            "(metric value not found in sources)"
        ),
        "claim_check_created_at": parsed["items"][0]["claim_check_created_at"],
        "claim_check_updated_at": parsed["items"][0]["claim_check_updated_at"],
        "content": "JSON unsupported.",
        "content_id": content_id,
        "content_type": "x_post",
        "created_at": "2026-04-24T10:00:00+00:00",
        "published": False,
        "published_at": None,
        "published_url": None,
        "supported_count": 2,
        "unsupported_count": 1,
    }


def test_script_writes_output_file(db, tmp_path):
    script = _load_script_module()
    _insert_content(db, content="CLI unsupported.")
    output_path = tmp_path / "review.json"

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch.object(script, "script_context", fake_script_context):
        script.main(["--format", "json", "--output", str(output_path)])

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["items"][0]["content"] == "CLI unsupported."


def test_script_rejects_non_positive_days():
    script = _load_script_module()
    with pytest.raises(SystemExit, match="--days must be at least 1"):
        script.main(["--days", "0"])
