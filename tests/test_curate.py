"""Tests for curate.py CLI command functions."""

import sys
from pathlib import Path

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from curate import VALID_FLAGS, cmd_clear, cmd_flag, cmd_list, cmd_stats


def _seed_published_posts(db, count=3):
    """Insert published posts into the DB and return their IDs."""
    ids = []
    for i in range(count):
        content_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[f"sha{i}"],
            source_messages=[f"uuid{i}"],
            content=f"Post number {i} about AI and software engineering",
            eval_score=7.0 + i * 0.5,
            eval_feedback="Good",
        )
        db.mark_published(content_id, f"https://x.com/user/status/{1000 + i}")
        ids.append(content_id)
    return ids


# --- cmd_list ---


class TestCmdList:
    def test_populated_db(self, db, capsys):
        ids = _seed_published_posts(db, count=2)
        cmd_list(db)
        output = capsys.readouterr().out
        # Should show both posts with IDs
        assert f"[{ids[0]:>3}]" in output
        assert f"[{ids[1]:>3}]" in output

    def test_empty_db(self, db, capsys):
        cmd_list(db)
        output = capsys.readouterr().out
        assert "No published posts found." in output

    def test_content_truncated(self, db, capsys):
        content_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=["sha1"],
            source_messages=["uuid1"],
            content="A" * 200,
            eval_score=8.0,
            eval_feedback="Good",
        )
        db.mark_published(content_id, "https://x.com/user/status/999")
        cmd_list(db)
        output = capsys.readouterr().out
        # 70-char truncation + "..."
        assert "A" * 70 in output
        assert "A" * 71 not in output


# --- cmd_flag ---


class TestCmdFlag:
    def test_valid_flag(self, db, capsys):
        ids = _seed_published_posts(db, count=1)
        cmd_flag(db, ids[0], "good")
        output = capsys.readouterr().out
        assert "Flagged" in output
        assert "'good'" in output
        # Verify in DB
        row = db.conn.execute(
            "SELECT curation_quality FROM generated_content WHERE id = ?",
            (ids[0],),
        ).fetchone()
        assert row["curation_quality"] == "good"

    def test_invalid_flag(self, db):
        ids = _seed_published_posts(db, count=1)
        with pytest.raises(SystemExit):
            cmd_flag(db, ids[0], "invalid_flag")

    def test_nonexistent_content_id(self, db):
        with pytest.raises(SystemExit):
            cmd_flag(db, 9999, "good")


# --- cmd_clear ---


class TestCmdClear:
    def test_clears_flag(self, db, capsys):
        ids = _seed_published_posts(db, count=1)
        db.set_curation_quality(ids[0], "good")
        cmd_clear(db, ids[0])
        output = capsys.readouterr().out
        assert "Cleared" in output
        # Verify in DB
        row = db.conn.execute(
            "SELECT curation_quality FROM generated_content WHERE id = ?",
            (ids[0],),
        ).fetchone()
        assert row["curation_quality"] is None

    def test_clear_roundtrip(self, db):
        ids = _seed_published_posts(db, count=1)
        db.set_curation_quality(ids[0], "too_specific")
        row = db.conn.execute(
            "SELECT curation_quality FROM generated_content WHERE id = ?",
            (ids[0],),
        ).fetchone()
        assert row["curation_quality"] == "too_specific"
        cmd_clear(db, ids[0])
        row = db.conn.execute(
            "SELECT curation_quality FROM generated_content WHERE id = ?",
            (ids[0],),
        ).fetchone()
        assert row["curation_quality"] is None


# --- cmd_stats ---


class TestCmdStats:
    def test_mixed_data(self, db, capsys):
        ids = _seed_published_posts(db, count=3)
        db.set_curation_quality(ids[0], "good")
        db.set_curation_quality(ids[1], "too_specific")
        # ids[2] left unreviewed
        cmd_stats(db)
        output = capsys.readouterr().out
        assert "good" in output
        assert "too_specific" in output
        assert "unreviewed" in output

    def test_empty_db(self, db, capsys):
        cmd_stats(db)
        output = capsys.readouterr().out
        assert "Manual curation" in output
        assert "Auto-classification" in output
