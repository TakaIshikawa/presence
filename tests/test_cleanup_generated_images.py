"""Tests for cleanup_generated_images.py."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from cleanup_generated_images import cleanup_generated_images, main


NOW = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)


def _touch(path: Path, *, age_days: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"image")
    ts = (NOW - timedelta(days=age_days)).timestamp()
    os.utime(path, (ts, ts))
    return path


def _insert_content(db, image_path: str | None) -> None:
    db.conn.execute(
        """INSERT INTO generated_content
           (content_type, source_commits, source_messages, content, eval_score, eval_feedback, image_path)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        ("x_visual", "[]", "[]", "visual content", 8.0, "ok", image_path),
    )
    db.conn.commit()


def test_dry_run_reports_old_unreferenced_without_deleting(file_db, tmp_path):
    image_dir = tmp_path / "generated_images"
    orphan = _touch(image_dir / "orphan.png", age_days=45)
    referenced = _touch(image_dir / "referenced.png", age_days=45)
    recent = _touch(image_dir / "recent.png", age_days=2)
    notes = _touch(image_dir / "notes.txt", age_days=45)
    _insert_content(file_db, str(referenced))

    result = cleanup_generated_images(
        db_path=file_db.db_path,
        image_dir=image_dir,
        days=30,
        delete=False,
        now=NOW,
    )

    assert result.dry_run is True
    assert result.scanned == 3
    assert result.deleted == []
    assert result.old_unreferenced == [str(orphan)]
    assert orphan.exists()
    assert referenced.exists()
    assert recent.exists()
    assert notes.exists()


def test_delete_removes_only_old_unreferenced_files(file_db, tmp_path):
    image_dir = tmp_path / "generated_images"
    orphan = _touch(image_dir / "orphan.jpg", age_days=31)
    referenced = _touch(image_dir / "referenced.jpg", age_days=31)
    recent = _touch(image_dir / "recent.jpg", age_days=1)
    _insert_content(file_db, str(referenced))

    result = cleanup_generated_images(
        db_path=file_db.db_path,
        image_dir=image_dir,
        days=30,
        delete=True,
        now=NOW,
    )

    assert result.dry_run is False
    assert result.old_unreferenced == [str(orphan)]
    assert result.deleted == [str(orphan)]
    assert not orphan.exists()
    assert referenced.exists()
    assert recent.exists()


def test_relative_image_path_is_protected(file_db, tmp_path, monkeypatch):
    image_dir = tmp_path / "generated_images"
    referenced = _touch(image_dir / "relative.webp", age_days=60)
    _insert_content(file_db, "relative.webp")
    monkeypatch.chdir(tmp_path)

    result = cleanup_generated_images(
        db_path=file_db.db_path,
        image_dir=image_dir,
        days=30,
        delete=True,
        now=NOW,
    )

    assert result.old_unreferenced == []
    assert result.deleted == []
    assert referenced.exists()


def test_rejects_negative_days(file_db, tmp_path):
    image_dir = tmp_path / "generated_images"
    image_dir.mkdir()

    with pytest.raises(ValueError, match="non-negative"):
        cleanup_generated_images(
            db_path=file_db.db_path,
            image_dir=image_dir,
            days=-1,
            now=NOW,
        )


def test_main_emits_json_for_delete(file_db, tmp_path, capsys):
    image_dir = tmp_path / "generated_images"
    orphan = _touch(image_dir / "orphan.png", age_days=40)

    argv = [
        "cleanup_generated_images.py",
        "--db-path",
        str(file_db.db_path),
        "--image-dir",
        str(image_dir),
        "--days",
        "30",
        "--delete",
        "--json",
    ]
    with patch.object(sys, "argv", argv):
        assert main() == 0

    output = json.loads(capsys.readouterr().out)
    assert output["dry_run"] is False
    assert output["old_unreferenced"] == [str(orphan)]
    assert output["deleted"] == [str(orphan)]
    assert not orphan.exists()
