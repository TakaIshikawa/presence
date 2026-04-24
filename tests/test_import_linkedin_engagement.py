"""Tests for scripts/import_linkedin_engagement.py."""

import importlib.util
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "import_linkedin_engagement.py"
)
spec = importlib.util.spec_from_file_location("import_linkedin_engagement_script", SCRIPT_PATH)
import_linkedin_engagement = importlib.util.module_from_spec(spec)
spec.loader.exec_module(import_linkedin_engagement)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db):
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Post for LinkedIn",
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.upsert_publication_success(
        content_id,
        "linkedin",
        platform_post_id="urn:li:activity:123",
        platform_url="https://www.linkedin.com/feed/update/urn:li:activity:123",
        published_at="2026-04-20T10:00:00+00:00",
    )
    return content_id


def test_script_imports_csv_and_reports_summary(db, tmp_path, capsys):
    content_id = _content(db)
    path = tmp_path / "linkedin.csv"
    path.write_text(
        "URL,impressions,likes,comments,shares\n"
        "https://www.linkedin.com/feed/update/urn:li:activity:123,10,1,0,0\n",
        encoding="utf-8",
    )

    with patch.object(
        import_linkedin_engagement,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = import_linkedin_engagement.main(
            ["--csv", str(path), "--fetched-at", "2026-04-24T12:00:00+00:00"]
        )

    assert exit_code == 0
    assert "Inserted 1 LinkedIn engagement snapshot." in capsys.readouterr().out
    snapshots = db.get_linkedin_engagement(content_id)
    assert len(snapshots) == 1
    assert snapshots[0]["fetched_at"] == "2026-04-24T12:00:00+00:00"


def test_script_dry_run_does_not_write(db, tmp_path, capsys):
    content_id = _content(db)
    path = tmp_path / "linkedin.csv"
    path.write_text(
        "Activity ID,Views,Reactions,Comments,Shares\n"
        "123,10,1,0,0\n",
        encoding="utf-8",
    )

    with patch.object(
        import_linkedin_engagement,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = import_linkedin_engagement.main(["--csv", str(path), "--dry-run"])

    assert exit_code == 0
    assert "Would insert 1 LinkedIn engagement snapshot." in capsys.readouterr().out
    assert db.get_linkedin_engagement(content_id) == []


def test_script_reports_unmatched_rows(db, tmp_path, capsys):
    path = tmp_path / "linkedin.csv"
    path.write_text(
        "URL,impressions,likes,comments,shares\n"
        "https://www.linkedin.com/feed/update/urn:li:activity:404,10,1,0,0\n",
        encoding="utf-8",
    )

    with patch.object(
        import_linkedin_engagement,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = import_linkedin_engagement.main(["--csv", str(path)])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Inserted 0 LinkedIn engagement snapshots." in output
    assert "Unmatched rows: 1" in output
    assert "row 2" in output
