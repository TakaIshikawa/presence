"""Tests for scripts/preview_newsletter.py."""

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

_project_root = Path(__file__).parent.parent
sys.path.insert(0, str(_project_root / "scripts"))
sys.path.insert(0, str(_project_root / "src"))

from preview_newsletter import main


def _config():
    return SimpleNamespace(
        newsletter=SimpleNamespace(
            utm_source="newsletter",
            utm_medium="email",
            utm_campaign_template="weekly-{week_end_compact}",
        )
    )


def _script_context(config, db):
    @contextmanager
    def _ctx():
        yield config, db

    return _ctx


def _insert_published_content(
    db,
    content_type: str,
    content: str,
    published_at: datetime,
    url: str = "",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=["sha-1"],
        source_messages=["msg-1"],
        content=content,
        eval_score=8.0,
        eval_feedback="Good",
    )
    db.conn.execute(
        """UPDATE generated_content
           SET published = 1, published_at = ?, published_url = ?
           WHERE id = ?""",
        (
            published_at.isoformat(),
            url or f"https://takaishikawa.com/content/{content_id}",
            content_id,
        ),
    )
    db.conn.commit()
    return content_id


def test_json_preview_writes_artifact_without_sending(db, tmp_path):
    published_at = datetime(2026, 4, 17, 12, tzinfo=timezone.utc)
    blog_id = _insert_published_content(
        db,
        "blog_post",
        "TITLE: Preview Blog\n\nA useful preview excerpt.",
        published_at,
        url="https://takaishikawa.com/blog/preview.html",
    )
    thread_id = _insert_published_content(
        db,
        "x_thread",
        "TWEET 1:\nThread hook for review\n\nTWEET 2:\nMore detail",
        published_at,
    )
    post_id = _insert_published_content(
        db,
        "x_post",
        "Short published note.",
        published_at,
    )
    output = tmp_path / "newsletter-preview.json"

    with patch(
        "preview_newsletter.script_context",
        return_value=_script_context(_config(), db)(),
    ):
        with patch("output.newsletter.ButtondownClient.send") as mock_send:
            assert main(
                [
                    "--week-start",
                    "2026-04-13",
                    "--week-end",
                    "2026-04-20",
                    "--output",
                    str(output),
                    "--json",
                ]
            ) == 0

    payload = json.loads(output.read_text())
    assert payload["subject"]
    assert "Preview Blog" in payload["body_markdown"]
    assert payload["source_content_ids"] == [blog_id, thread_id, post_id]
    assert payload["utm_metadata"] == {
        "utm_source": "newsletter",
        "utm_medium": "email",
        "utm_campaign_template": "weekly-{week_end_compact}",
        "utm_campaign": "weekly-20260420",
    }
    assert payload["subject_candidates"]
    assert payload["subject_candidates"][0]["subject"] == payload["subject"]
    assert payload["message"] == ""
    mock_send.assert_not_called()


def test_markdown_preview_is_default(db, tmp_path):
    _insert_published_content(
        db,
        "x_post",
        "Markdown preview note.",
        datetime(2026, 4, 17, 12, tzinfo=timezone.utc),
    )
    output = tmp_path / "newsletter-preview.md"

    with patch(
        "preview_newsletter.script_context",
        return_value=_script_context(_config(), db)(),
    ):
        assert main(
            [
                "--week-start",
                "2026-04-13",
                "--week-end",
                "2026-04-20",
                "--output",
                str(output),
            ]
        ) == 0

    rendered = output.read_text()
    assert rendered.startswith("# Newsletter Preview")
    assert "## Subject" in rendered
    assert "## Source Content IDs" in rendered
    assert "## Subject Candidates" in rendered
    assert "Markdown preview note." in rendered


def test_empty_range_writes_clear_empty_artifact(db, tmp_path):
    output = tmp_path / "empty-newsletter.json"

    with patch(
        "preview_newsletter.script_context",
        return_value=_script_context(_config(), db)(),
    ):
        assert main(
            [
                "--week-start",
                "2026-04-13",
                "--week-end",
                "2026-04-20",
                "--output",
                str(output),
                "--json",
            ]
        ) == 0

    payload = json.loads(output.read_text())
    assert payload["subject"] == ""
    assert payload["body_markdown"] == ""
    assert payload["source_content_ids"] == []
    assert payload["subject_candidates"] == []
    assert payload["message"] == "No content published for this date range."
