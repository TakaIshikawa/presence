"""Tests for the content variant operator CLI."""

from __future__ import annotations

from contextlib import contextmanager

import pytest

from scripts import content_variants


@contextmanager
def _script_context(db):
    yield None, db


def _insert_content(db, content: str = "Original copy") -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
    )


def test_add_variant_upserts_and_prints_sorted_metadata(db, monkeypatch, capsys):
    content_id = _insert_content(db)
    monkeypatch.setattr(
        content_variants,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = content_variants.main(
        [
            "add",
            "--content-id",
            str(content_id),
            "--platform",
            "x",
            "--variant-type",
            "post",
            "--text",
            "X copy",
            "--metadata-json",
            '{"z": 1, "a": 2}',
        ]
    )

    assert exit_code == 0
    assert db.get_content_variant(content_id, "x", "post")["content"] == "X copy"
    assert 'metadata: {"a": 2, "z": 1}' in capsys.readouterr().out


def test_list_variants_filters_by_platform(db, monkeypatch, capsys):
    content_id = _insert_content(db)
    db.upsert_content_variant(content_id, "x", "post", "X copy")
    db.upsert_content_variant(content_id, "bluesky", "post", "Bluesky copy")
    monkeypatch.setattr(
        content_variants,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = content_variants.main(
        [
            "list",
            "--content-id",
            str(content_id),
            "--platform",
            "bluesky",
        ]
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Bluesky copy" in output
    assert "X copy" not in output


def test_show_variant_prints_detail(db, monkeypatch, capsys):
    content_id = _insert_content(db)
    db.upsert_content_variant(content_id, "newsletter", "summary", "Newsletter copy")
    monkeypatch.setattr(
        content_variants,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = content_variants.main(
        [
            "show",
            "--content-id",
            str(content_id),
            "--platform",
            "newsletter",
            "--variant-type",
            "summary",
        ]
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "platform: newsletter" in output
    assert "Newsletter copy" in output


def test_select_variant_marks_selected(db, monkeypatch, capsys):
    content_id = _insert_content(db)
    db.upsert_content_variant(content_id, "x", "post", "X post")
    db.upsert_content_variant(content_id, "x", "thread", "X thread")
    monkeypatch.setattr(
        content_variants,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = content_variants.main(
        [
            "select",
            "--content-id",
            str(content_id),
            "--platform",
            "x",
            "--variant-type",
            "thread",
        ]
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "selected: yes" in output
    assert db.get_selected_content_variant(content_id, "x")["variant_type"] == "thread"


def test_missing_content_returns_clear_error(db, monkeypatch, capsys):
    monkeypatch.setattr(
        content_variants,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = content_variants.main(
        [
            "list",
            "--content-id",
            "999",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "generated_content id 999 does not exist" in captured.err


def test_invalid_metadata_json_exits_with_clear_error(capsys):
    with pytest.raises(SystemExit):
        content_variants.main(
            [
                "add",
                "--content-id",
                "1",
                "--platform",
                "x",
                "--variant-type",
                "post",
                "--text",
                "X copy",
                "--metadata-json",
                "{bad json",
            ]
        )

    assert "invalid metadata JSON" in capsys.readouterr().err
