"""Tests for GitHub commit synthesis coverage reporting."""

from __future__ import annotations

import csv
import importlib.util
import json
from pathlib import Path

from ingestion.github_commit_synthesis_coverage import (
    build_github_commit_synthesis_coverage_report,
    format_github_commit_synthesis_coverage_csv,
    format_github_commit_synthesis_coverage_json,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "github_commit_synthesis_coverage.py"
)
spec = importlib.util.spec_from_file_location(
    "github_commit_synthesis_coverage_script",
    SCRIPT_PATH,
)
github_commit_synthesis_coverage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(github_commit_synthesis_coverage_script)


def _commit(
    db,
    *,
    repo: str = "alpha/app",
    sha: str,
    timestamp: str = "2026-05-01T10:00:00+00:00",
) -> int:
    return db.insert_commit(
        repo_name=repo,
        commit_sha=sha,
        commit_message=f"feat: ship {sha}",
        timestamp=timestamp,
        author="dev@example.com",
    )


def _content(db, source_commits: list[str], *, published: bool = False) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=source_commits,
        source_messages=[],
        content="Generated post",
        eval_score=8.0,
        eval_feedback="ok",
    )
    if published:
        db.conn.execute(
            """UPDATE generated_content
               SET published = 1, published_at = '2026-05-02T09:00:00+00:00'
               WHERE id = ?""",
            (content_id,),
        )
        db.conn.commit()
    return content_id


def _idea(db, commit_sha: str) -> int:
    return db.add_content_idea(
        note="Tell the commit story",
        topic="commit",
        source="github_hotfix_commit_seed",
        source_metadata={"source_id": f"commit:{commit_sha}", "commit_sha": commit_sha},
    )


def test_full_coverage_counts_synthesized_and_published_commits(db):
    _commit(db, sha="aaa111")
    _commit(db, sha="bbb222")
    _content(db, ["aaa111", "bbb222"], published=True)

    report = build_github_commit_synthesis_coverage_report(
        db,
        start_date="2026-05-01",
        end_date="2026-05-01",
    )
    payload = json.loads(format_github_commit_synthesis_coverage_json(report))

    assert payload["artifact_type"] == "github_commit_synthesis_coverage"
    assert payload["rows"] == [
        {
            "date": "2026-05-01",
            "repo": "alpha/app",
            "ingested": 2,
            "synthesized": 2,
            "published": 2,
            "uncovered": 0,
        }
    ]


def test_partial_coverage_groups_by_day_and_repo_with_stable_order(db):
    _commit(
        db,
        repo="beta/api",
        sha="uncovered",
        timestamp="2026-05-02T11:00:00+00:00",
    )
    _commit(
        db,
        repo="alpha/app",
        sha="idea-only",
        timestamp="2026-05-01T10:00:00+00:00",
    )
    _commit(
        db,
        repo="beta/api",
        sha="generated",
        timestamp="2026-05-01T12:00:00+00:00",
    )
    _idea(db, "idea-only")
    _content(db, ["generated"])

    report = build_github_commit_synthesis_coverage_report(db)

    assert [row.to_dict() for row in report.rows] == [
        {
            "date": "2026-05-01",
            "repo": "alpha/app",
            "ingested": 1,
            "synthesized": 1,
            "published": 0,
            "uncovered": 0,
        },
        {
            "date": "2026-05-01",
            "repo": "beta/api",
            "ingested": 1,
            "synthesized": 1,
            "published": 0,
            "uncovered": 0,
        },
        {
            "date": "2026-05-02",
            "repo": "beta/api",
            "ingested": 1,
            "synthesized": 0,
            "published": 0,
            "uncovered": 1,
        },
    ]


def test_repository_filter_limits_rows(db):
    _commit(db, repo="alpha/app", sha="alpha111")
    _commit(db, repo="beta/api", sha="beta222")
    _content(db, ["alpha111"], published=True)
    _content(db, ["beta222"], published=True)

    report = build_github_commit_synthesis_coverage_report(
        db,
        repo="beta/api",
        start_date="2026-05-01",
        end_date="2026-05-01",
    )

    assert [row.to_dict() for row in report.rows] == [
        {
            "date": "2026-05-01",
            "repo": "beta/api",
            "ingested": 1,
            "synthesized": 1,
            "published": 1,
            "uncovered": 0,
        }
    ]
    assert report.filters == {
        "start_date": "2026-05-01",
        "end_date": "2026-05-01",
        "repo": "beta/api",
    }


def test_csv_output_and_cli_support_format_option(file_db, capsys):
    _commit(
        file_db,
        repo="alpha/app",
        sha="aaa111",
        timestamp="2026-05-01T10:00:00+00:00",
    )
    _commit(
        file_db,
        repo="alpha/app",
        sha="bbb222",
        timestamp="2026-05-01T11:00:00+00:00",
    )
    _idea(file_db, "aaa111")

    report = build_github_commit_synthesis_coverage_report(file_db)
    direct_rows = list(
        csv.DictReader(format_github_commit_synthesis_coverage_csv(report).splitlines())
    )
    assert direct_rows == [
        {
            "date": "2026-05-01",
            "repo": "alpha/app",
            "ingested": "2",
            "synthesized": "1",
            "published": "0",
            "uncovered": "1",
        }
    ]

    assert (
        github_commit_synthesis_coverage_script.main(
            [
                "--db",
                str(file_db.db_path),
                "--start-date",
                "2026-05-01",
                "--end-date",
                "2026-05-01",
                "--format",
                "csv",
            ]
        )
        == 0
    )
    cli_rows = list(csv.DictReader(capsys.readouterr().out.splitlines()))
    assert cli_rows == direct_rows


def test_cli_rejects_invalid_date(file_db, capsys):
    assert (
        github_commit_synthesis_coverage_script.main(
            ["--db", str(file_db.db_path), "--start-date", "2026/05/01"]
        )
        == 1
    )
    assert "start_date must be YYYY-MM-DD" in capsys.readouterr().err
