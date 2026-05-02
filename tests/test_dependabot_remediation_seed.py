"""Tests for exporting Dependabot remediation seeds."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from synthesis.dependabot_remediation_seed import (
    build_dependabot_remediation_seed_report,
    format_dependabot_remediation_seed_json,
    format_dependabot_remediation_seed_markdown,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "export_dependabot_remediation_seeds.py"
)
spec = importlib.util.spec_from_file_location("export_dependabot_remediation_seeds_cli", SCRIPT_PATH)
export_dependabot_remediation_seeds_cli = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_dependabot_remediation_seeds_cli)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content_idea_count(db) -> int:
    return db.conn.execute("SELECT COUNT(*) FROM content_ideas").fetchone()[0]


def _alert_activity(
    db,
    *,
    repo: str = "taka/presence",
    number: int = 7,
    package: str = "urllib3",
    ecosystem: str = "pip",
    severity: str = "high",
    state: str = "open",
    ghsa_id: str = "GHSA-1234-5678-90AB",
    cve_id: str = "CVE-2026-0001",
    updated_at: str = "2026-04-30T12:00:00+00:00",
    patched_versions: str = ">=2.0.7",
    score: float = 8.1,
    metadata: dict | None = None,
) -> int:
    url = f"https://github.com/{repo}/security/dependabot/{number}"
    payload = {
        "activity_id": f"{repo}#{number}:dependabot_alert",
        "external_id": f"dependabot_alert:{repo}:{number}",
        "alert_number": number,
        "package": package,
        "ecosystem": ecosystem,
        "severity": severity,
        "state": state,
        "ghsa_id": ghsa_id,
        "cve_id": cve_id,
        "manifest_path": "requirements.txt",
        "patched_versions": patched_versions,
        "advisory_url": f"https://api.github.com/advisories/{ghsa_id}",
        "advisory_summary": f"{package} has a vulnerable redirect handling path.",
        "cvss": {"score": score},
        "html_url": url,
    }
    if metadata:
        payload.update(metadata)
    payload = {key: value for key, value in payload.items() if value not in (None, "", [])}
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type="dependabot_alert",
        number=number,
        title=f"Dependabot {severity} alert for {package} ({ecosystem})",
        state=state,
        author="dependabot",
        url=url,
        updated_at=updated_at,
        created_at="2026-04-29T10:00:00+00:00",
        body=payload["advisory_summary"],
        labels=[ecosystem, severity, state],
        metadata=payload,
    )


def test_exports_grouped_remediation_seeds_without_writing_content_ideas(db):
    _alert_activity(db, number=7)
    _alert_activity(db, number=8, ghsa_id="GHSA-OTHER", cve_id="CVE-2026-0002")
    before_count = _content_idea_count(db)

    report = build_dependabot_remediation_seed_report(db, days=14, now=NOW)

    assert _content_idea_count(db) == before_count
    assert len(report.seeds) == 1
    seed = report.seeds[0]
    assert seed.repo_name == "taka/presence"
    assert seed.package == "urllib3"
    assert seed.ecosystem == "pip"
    assert seed.severity == "high"
    assert seed.alert_count == 2
    assert seed.fixed_version == ">=2.0.7"
    assert seed.score == 8.1
    assert seed.alert_urls == (
        "https://github.com/taka/presence/security/dependabot/7",
        "https://github.com/taka/presence/security/dependabot/8",
    )
    assert seed.advisories == ("GHSA-1234-5678-90AB", "GHSA-OTHER")
    assert "fixed-version upgrade" in seed.suggested_angle
    assert "high severity alert for urllib3 (pip)" in seed.risk_summary


def test_filters_by_minimum_severity_repo_and_limit(db):
    _alert_activity(db, number=1, severity="medium", package="requests", ghsa_id="GHSA-MED")
    _alert_activity(db, number=2, severity="critical", package="django", ghsa_id="GHSA-CRIT")
    _alert_activity(db, repo="acme/widget", number=3, severity="critical", package="rails", ecosystem="bundler")

    report = build_dependabot_remediation_seed_report(
        db,
        days=14,
        severity="high",
        repo="taka/presence",
        limit=1,
        now=NOW,
    )

    assert [seed.package for seed in report.seeds] == ["django"]
    assert report.repo == "taka/presence"
    assert report.limit == 1
    with pytest.raises(ValueError, match="severity"):
        build_dependabot_remediation_seed_report(db, severity="severe", now=NOW)


def test_json_and_markdown_output_are_stable(db):
    _alert_activity(db, number=7)

    report = build_dependabot_remediation_seed_report(db, days=14, now=NOW)
    payload = json.loads(format_dependabot_remediation_seed_json(report))
    markdown = format_dependabot_remediation_seed_markdown(report)

    assert payload["artifact_type"] == "dependabot_remediation_seeds"
    assert payload["seed_count"] == 1
    assert payload["seeds"][0]["fixed_version"] == ">=2.0.7"
    assert payload["seeds"][0]["alert_urls"] == [
        "https://github.com/taka/presence/security/dependabot/7"
    ]
    assert list(payload.keys()) == sorted(payload.keys())
    assert markdown == "\n".join(
        [
            "# Dependabot Remediation Seeds",
            "",
            "Window: 2026-04-17T12:00:00+00:00 to 2026-05-01T12:00:00+00:00",
            "Minimum severity: medium",
            "",
            "Seeds: 1",
            "",
            "## 1. taka/presence: urllib3 (pip)",
            "",
            "- Severity: high",
            "- Alerts: 1",
            "- Score: 8.1",
            "- Fixed version: >=2.0.7",
            "- Advisories: GHSA-1234-5678-90AB",
            "- Risk summary: high severity alert for urllib3 (pip): urllib3 has a vulnerable redirect handling path. CVSS score 8.1.",
            "- Suggested angle: Show the fixed-version upgrade for urllib3 in pip, why the high risk matters, and the verification checklist after updating 1 alert.",
            "- Alert URLs: https://github.com/taka/presence/security/dependabot/7",
        ]
    )


def test_reads_sqlite_rows_without_database_wrapper():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE github_activity (
            id INTEGER PRIMARY KEY,
            repo_name TEXT NOT NULL,
            activity_type TEXT NOT NULL,
            number INTEGER NOT NULL,
            title TEXT NOT NULL,
            state TEXT,
            url TEXT,
            updated_at TEXT NOT NULL,
            metadata TEXT,
            labels TEXT
        );
        """
    )
    metadata = {
        "package": "rack",
        "ecosystem": "bundler",
        "severity": "critical",
        "patched_versions": ">=3.0.0",
        "advisory_summary": "rack allows request smuggling.",
        "cvss": {"score": "9.8"},
        "html_url": "https://github.com/acme/widget/security/dependabot/5",
    }
    conn.execute(
        """INSERT INTO github_activity
           (repo_name, activity_type, number, title, state, url, updated_at, metadata, labels)
           VALUES (?, 'dependabot_alert', 5, 'Dependabot critical alert', 'open', ?, ?, ?, ?)""",
        (
            "acme/widget",
            "https://github.com/acme/widget/security/dependabot/5",
            "2026-04-30T10:00:00+00:00",
            json.dumps(metadata),
            json.dumps(["bundler", "critical"]),
        ),
    )
    conn.commit()

    report = build_dependabot_remediation_seed_report(conn, days=14, severity="critical", now=NOW)

    assert len(report.seeds) == 1
    assert report.seeds[0].package == "rack"
    assert report.seeds[0].score == 9.8


def test_cli_supports_requested_flags(db, capsys):
    _alert_activity(db)

    with patch.object(
        export_dependabot_remediation_seeds_cli,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        export_dependabot_remediation_seeds_cli,
        "build_dependabot_remediation_seed_report",
        wraps=lambda db, **kwargs: build_dependabot_remediation_seed_report(db, now=NOW, **kwargs),
    ):
        assert (
            export_dependabot_remediation_seeds_cli.main(
                [
                    "--days",
                    "14",
                    "--severity",
                    "high",
                    "--repo",
                    "taka/presence",
                    "--limit",
                    "5",
                    "--format",
                    "json",
                ]
            )
            == 0
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["severity"] == "high"
    assert payload["repo"] == "taka/presence"
    assert payload["seeds"][0]["package"] == "urllib3"
