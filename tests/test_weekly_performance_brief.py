"""Tests for scripts/weekly_performance_brief.py."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.performance_brief import PerformanceBriefBuilder
from test_performance_brief import seed_performance_brief
from weekly_performance_brief import (
    artifact_path,
    format_json_brief,
    main,
    write_artifact,
)


def test_format_json_brief_outputs_machine_readable_data(db):
    seed_performance_brief(db)
    brief = PerformanceBriefBuilder(db).build("2026-04-20")

    data = json.loads(format_json_brief(brief))

    assert data["week_start"] == "2026-04-20"
    assert data["published_count"] == 2
    assert data["platform_summary"]["x"]["publication_count"] == 2


def test_artifact_path_uses_week_start_and_mode(tmp_path):
    assert artifact_path(tmp_path, "2026-04-20", "json").name == (
        "weekly_performance_brief_2026-04-20.json"
    )
    assert artifact_path(tmp_path, "2026-04-20", "markdown").name == (
        "weekly_performance_brief_2026-04-20.md"
    )


def test_write_artifact_writes_markdown_and_json(db, tmp_path):
    seed_performance_brief(db)
    brief = PerformanceBriefBuilder(db).build("2026-04-20")

    markdown_path = write_artifact(brief, tmp_path, "markdown")
    json_path = write_artifact(brief, tmp_path, "json")

    assert markdown_path.read_text(encoding="utf-8").startswith(
        "# Weekly Performance Brief"
    )
    assert json.loads(json_path.read_text(encoding="utf-8"))["week_start"] == (
        "2026-04-20"
    )


def test_main_prints_json(db, capsys):
    seed_performance_brief(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("weekly_performance_brief.script_context", fake_script_context):
        main(["--week-start", "2026-04-20", "--json"])

    output = json.loads(capsys.readouterr().out)
    assert output["published_count"] == 2


def test_main_writes_output_dir(db, tmp_path, capsys):
    seed_performance_brief(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("weekly_performance_brief.script_context", fake_script_context):
        main([
            "--week-start",
            "2026-04-20",
            "--output-dir",
            str(tmp_path),
            "--markdown",
        ])

    output_path = Path(capsys.readouterr().out.strip())
    assert output_path.exists()
    assert output_path.name == "weekly_performance_brief_2026-04-20.md"
