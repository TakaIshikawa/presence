import json
import sys
from pathlib import Path
from unittest.mock import patch

from synthesis.visual_readiness import build_visual_readiness_report


sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))


def _insert_visual(
    db,
    *,
    image_path: str | None,
    image_alt_text: str | None,
    image_prompt: str = "Launch metrics dashboard with conversion trend annotations",
    content_type: str = "x_visual",
) -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content="Visual post copy",
        eval_score=8.0,
        eval_feedback="Good",
        image_path=image_path,
        image_prompt=image_prompt,
        image_alt_text=image_alt_text,
    )


def test_report_flags_missing_image_file(db, tmp_path):
    missing_path = tmp_path / "missing.png"
    content_id = _insert_visual(
        db,
        image_path=str(missing_path),
        image_alt_text="Launch metrics dashboard with conversion trend annotations and labels.",
    )

    report = build_visual_readiness_report(db, days=30)

    item = report.items[0]
    assert item.content_id == content_id
    assert item.status == "missing_file"
    assert item.ready is False
    assert item.file.exists is False
    assert [issue.code for issue in item.issues] == ["missing_image_file"]
    assert report.summary["missing_file"] == 1


def test_report_flags_missing_alt_text(db, tmp_path):
    image_path = tmp_path / "visual.png"
    image_path.write_bytes(b"png bytes")
    _insert_visual(db, image_path=str(image_path), image_alt_text="")

    report = build_visual_readiness_report(db, days=30)

    item = report.items[0]
    assert item.status == "needs_alt_text"
    assert item.file.size_bytes == len(b"png bytes")
    assert "missing_alt_text" in {issue.code for issue in item.issues}
    assert report.summary["needs_alt_text"] == 1


def test_report_flags_short_weak_alt_text(db, tmp_path):
    image_path = tmp_path / "dashboard.png"
    image_path.write_bytes(b"png bytes")
    _insert_visual(db, image_path=str(image_path), image_alt_text="A screenshot")

    report = build_visual_readiness_report(db, days=30)

    item = report.items[0]
    assert item.status == "needs_alt_text"
    issue_codes = {issue.code for issue in item.issues}
    assert "alt_text_too_short" in issue_codes
    assert "generic_alt_text" in issue_codes


def test_report_marks_acceptable_visual_post_ready(db, tmp_path):
    image_path = tmp_path / "launch.png"
    image_path.write_bytes(b"png bytes")
    _insert_visual(
        db,
        image_path=str(image_path),
        image_alt_text=(
            "Launch metrics dashboard with conversion trend annotations and status labels."
        ),
    )

    report = build_visual_readiness_report(db, days=30)

    item = report.items[0]
    assert item.status == "ready"
    assert item.ready is True
    assert item.issues == ()
    assert item.alt_text["status"] == "passed"
    assert report.summary["ready"] == 1


def test_report_serializes_json_payload(db, tmp_path):
    image_path = tmp_path / "launch.png"
    image_path.write_bytes(b"png bytes")
    content_id = _insert_visual(
        db,
        image_path=str(image_path),
        image_alt_text=(
            "Launch metrics dashboard with conversion trend annotations and status labels."
        ),
    )

    payload = json.loads(json.dumps(build_visual_readiness_report(db).as_dict()))

    assert payload["summary"]["total"] == 1
    assert payload["summary"]["ready"] == 1
    assert payload["items"][0]["content_id"] == content_id
    assert payload["items"][0]["file"]["exists"] is True
    assert payload["items"][0]["alt_text"]["passed"] is True


def test_cli_filters_to_content_id_and_missing_only(db, tmp_path, capsys):
    ready_path = tmp_path / "ready.png"
    ready_path.write_bytes(b"png bytes")
    ready_id = _insert_visual(
        db,
        image_path=str(ready_path),
        image_alt_text=(
            "Launch metrics dashboard with conversion trend annotations and status labels."
        ),
    )
    missing_id = _insert_visual(
        db,
        image_path=str(tmp_path / "missing.png"),
        image_alt_text=(
            "Launch metrics dashboard with conversion trend annotations and status labels."
        ),
    )

    import visual_readiness

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with (
        patch("visual_readiness.script_context", return_value=Context()),
        patch("visual_readiness.update_monitoring"),
    ):
        exit_code = visual_readiness.main(
            ["--content-id", str(ready_id), "--missing-only", "--json"]
        )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["summary"]["total"] == 0
    assert payload["items"] == []

    with (
        patch("visual_readiness.script_context", return_value=Context()),
        patch("visual_readiness.update_monitoring"),
    ):
        exit_code = visual_readiness.main(
            ["--content-id", str(missing_id), "--missing-only", "--json"]
        )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["summary"]["total"] == 1
    assert payload["items"][0]["content_id"] == missing_id
    assert payload["items"][0]["status"] == "missing_file"
