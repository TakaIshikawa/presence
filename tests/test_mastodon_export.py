"""Tests for manual Mastodon publishing artifacts."""

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from output.mastodon_export import (
    MastodonExportError,
    MastodonExportOptions,
    build_mastodon_export,
    build_mastodon_export_from_db,
    count_graphemes,
    format_mastodon_markdown,
    mastodon_export_to_json,
)


def _insert_content(
    db,
    content: str,
    content_type: str = "x_post",
    **kwargs,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="Good",
        **kwargs,
    )
    db.set_curation_quality(content_id, "good")
    return content_id


class _Context:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return None, self.db

    def __exit__(self, exc_type, exc, tb):
        return False


def test_single_post_under_500_exports_as_one_status():
    export = build_mastodon_export(
        {
            "id": 7,
            "content_type": "x_post",
            "content": "A compact Mastodon post for manual review.",
        }
    )

    assert export.status_count == 1
    assert export.statuses[0].text == "A compact Mastodon post for manual review."
    assert export.statuses[0].characters <= 500
    assert not export.statuses[0].text.startswith("1/1 ")


def test_long_content_splits_into_numbered_statuses_under_limit():
    text = " ".join(
        f"Sentence {index} explains one deploy review lesson."
        for index in range(1, 55)
    )

    export = build_mastodon_export(
        {"id": 8, "content_type": "x_post", "content": text}
    )

    assert export.status_count > 1
    for status in export.statuses:
        assert status.text.startswith(f"{status.index}/{status.total} ")
        assert count_graphemes(status.text) <= 500
    rebuilt = " ".join(status.text.split(" ", 1)[1] for status in export.statuses)
    assert rebuilt == text


def test_content_warning_is_attached_to_each_status_and_serialized():
    export = build_mastodon_export(
        {
            "id": 9,
            "content_type": "x_post",
            "content": "Post with an operator-selected content warning.",
        },
        options=MastodonExportOptions(cw="Incident detail"),
    )

    assert export.statuses[0].cw == "Incident detail"
    assert "CW: Incident detail" in format_mastodon_markdown(export)
    payload = json.loads(mastodon_export_to_json(export))
    assert payload["statuses"][0]["cw"] == "Incident detail"


def test_visual_content_includes_media_alt_text():
    export = build_mastodon_export(
        {
            "id": 10,
            "content_type": "x_visual",
            "content": "Visual post for Mastodon.",
            "image_path": "/tmp/presence-images/launch.png",
            "image_prompt": "Launch metrics dashboard",
            "image_alt_text": "Launch metrics dashboard with labels and trend annotations.",
        }
    )

    assert export.media[0].path == "/tmp/presence-images/launch.png"
    assert export.media[0].alt_text == (
        "Launch metrics dashboard with labels and trend annotations."
    )
    markdown = format_mastodon_markdown(export)
    assert "## Media" in markdown
    assert "Alt text: Launch metrics dashboard with labels and trend annotations." in markdown


def test_visual_content_rejects_missing_alt_text_by_default():
    with pytest.raises(MastodonExportError, match="missing_alt_text"):
        build_mastodon_export(
            {
                "id": 11,
                "content_type": "x_visual",
                "content": "Visual post without alt text.",
                "image_path": "/tmp/presence-images/launch.png",
                "image_prompt": "Launch metrics dashboard",
            }
        )


def test_thread_markers_are_removed_before_mastodon_splitting():
    export = build_mastodon_export(
        {
            "id": 12,
            "content_type": "x_thread",
            "content": "TWEET 1:\nFirst point.\nTWEET 2:\nSecond point.",
        }
    )

    assert "TWEET" not in export.statuses[0].text
    assert "First point." in export.statuses[0].text
    assert "Second point." in export.statuses[0].text


def test_build_from_db_and_json_serialization(db):
    content_id = _insert_content(db, "Database-backed Mastodon export.")

    export = build_mastodon_export_from_db(db, content_id=content_id)
    payload = json.loads(mastodon_export_to_json(export))

    assert payload["content_id"] == content_id
    assert payload["status_count"] == 1
    assert payload["statuses"][0]["characters"] <= 500


def test_export_mastodon_cli_writes_markdown_to_output_dir(db, tmp_path, capsys):
    content_id = _insert_content(db, "CLI Markdown Mastodon artifact.")

    import export_mastodon

    with patch("export_mastodon.script_context", return_value=_Context(db)):
        exit_code = export_mastodon.main(
            [
                "--content-id",
                str(content_id),
                "--output-dir",
                str(tmp_path),
            ]
        )

    captured = capsys.readouterr()
    artifact_path = tmp_path / f"mastodon-{content_id}.md"
    assert exit_code == 0
    assert f"Mastodon artifact: {artifact_path}" in captured.err
    assert artifact_path.read_text().startswith("# Mastodon Draft")


def test_export_mastodon_cli_writes_json_to_output_dir(db, tmp_path, capsys):
    content_id = _insert_content(db, "CLI JSON Mastodon artifact.")

    import export_mastodon

    with patch("export_mastodon.script_context", return_value=_Context(db)):
        exit_code = export_mastodon.main(
            [
                "--content-id",
                str(content_id),
                "--output-dir",
                str(tmp_path),
                "--json",
            ]
        )

    captured = capsys.readouterr()
    artifact_path = tmp_path / f"mastodon-{content_id}.json"
    payload = json.loads(artifact_path.read_text())
    assert exit_code == 0
    assert f"Mastodon artifact: {artifact_path}" in captured.err
    assert payload["content_id"] == content_id


def test_export_mastodon_cli_dry_run_does_not_write(db, tmp_path, capsys):
    content_id = _insert_content(db, "Dry-run Mastodon artifact.")

    import export_mastodon

    with patch("export_mastodon.script_context", return_value=_Context(db)):
        exit_code = export_mastodon.main(
            [
                "--content-id",
                str(content_id),
                "--output-dir",
                str(tmp_path),
                "--json",
                "--dry-run",
            ]
        )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Dry run; no Mastodon artifact written" in captured.err
    assert json.loads(captured.out)["content_id"] == content_id
    assert not (tmp_path / f"mastodon-{content_id}.json").exists()
