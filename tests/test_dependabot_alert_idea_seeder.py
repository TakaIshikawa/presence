"""Tests for seeding content ideas from GitHub Dependabot alert activity."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from synthesis.dependabot_alert_idea_seeder import (
    SOURCE_NAME,
    format_dependabot_alert_idea_results_json,
    format_dependabot_alert_idea_results_text,
    seed_dependabot_alert_ideas,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "seed_dependabot_alert_ideas.py"
spec = importlib.util.spec_from_file_location("seed_dependabot_alert_ideas_cli", SCRIPT_PATH)
seed_dependabot_alert_ideas_cli = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(seed_dependabot_alert_ideas_cli)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


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
    fixed_at: str | None = None,
    dismissed_at: str | None = None,
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
        "patched_versions": ">=2.0.7",
        "advisory_summary": f"{package} has a vulnerable redirect handling path.",
        "fixed_at": fixed_at,
        "dismissed_at": dismissed_at,
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
        closed_at=fixed_at or dismissed_at,
        body=payload["advisory_summary"],
        labels=[ecosystem, severity, state],
        metadata=payload,
    )


def test_seed_dependabot_alert_ideas_groups_package_advisory_cluster(db):
    _alert_activity(db, number=7)
    _alert_activity(db, number=8)
    _alert_activity(db, number=9, package="django", ghsa_id="GHSA-DJANGO")

    results = seed_dependabot_alert_ideas(db, days=14, now=NOW)

    assert [(result.status, result.package, result.alert_count) for result in results] == [
        ("created", "urllib3", 2),
        ("created", "django", 1),
    ]
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 2
    first = ideas[0]
    assert first["source"] == SOURCE_NAME
    assert first["topic"] == "security"
    assert first["priority"] == "high"
    assert "security-maintenance review" in first["note"]
    metadata = json.loads(first["source_metadata"])
    assert metadata["source"] == SOURCE_NAME
    assert metadata["package"] == "urllib3"
    assert metadata["advisory"] == "GHSA-1234-5678-90AB"
    assert metadata["activity_ids"] == [
        "taka/presence#7:dependabot_alert",
        "taka/presence#8:dependabot_alert",
    ]
    assert metadata["alert_identifiers"] == [
        "dependabot_alert:taka/presence:7",
        "dependabot_alert:taka/presence:8",
    ]
    assert metadata["alert_cluster_id"]


def test_seed_dependabot_alert_ideas_reuses_existing_active_cluster(db):
    _alert_activity(db)

    first = seed_dependabot_alert_ideas(db, days=14, now=NOW)
    second = seed_dependabot_alert_ideas(db, days=14, now=NOW)

    assert first[0].status == "created"
    assert second[0].status == "skipped"
    assert second[0].reason == "open duplicate"
    assert second[0].idea_id == first[0].idea_id
    assert len(db.get_content_ideas(status=None)) == 1


def test_seed_dependabot_alert_ideas_filters_severity_and_resolved_rows(db):
    _alert_activity(db, number=1, severity="low", ghsa_id="GHSA-LOW")
    _alert_activity(db, number=2, severity="medium", ghsa_id="GHSA-MED")
    _alert_activity(db, number=3, severity="high", state="dismissed", dismissed_at="2026-04-30T12:00:00+00:00")
    _alert_activity(db, number=4, severity="critical", state="fixed", fixed_at="2026-04-30T12:00:00+00:00")

    results = seed_dependabot_alert_ideas(
        db,
        days=14,
        min_severity="medium",
        dry_run=True,
        now=NOW,
    )

    assert [(result.package, result.severity, result.status, result.reason) for result in results] == [
        ("urllib3", "critical", "skipped", "fixed alert"),
        ("urllib3", "high", "skipped", "dismissed alert"),
        ("urllib3", "medium", "proposed", "dry run"),
        ("urllib3", "low", "skipped", "below medium severity"),
    ]
    assert db.get_content_ideas(status="open") == []


def test_seed_dependabot_alert_ideas_supports_all_min_severities_deterministically(db):
    _alert_activity(db, number=1, severity="low", ghsa_id="GHSA-LOW")
    _alert_activity(db, number=2, severity="medium", ghsa_id="GHSA-MED")
    _alert_activity(db, number=3, severity="high", ghsa_id="GHSA-HIGH")
    _alert_activity(db, number=4, severity="critical", ghsa_id="GHSA-CRIT")

    assert [
        result.severity
        for result in seed_dependabot_alert_ideas(db, min_severity="low", dry_run=True, now=NOW)
        if result.status == "proposed"
    ] == ["critical", "high", "medium", "low"]
    assert [
        result.severity
        for result in seed_dependabot_alert_ideas(db, min_severity="critical", dry_run=True, now=NOW)
        if result.status == "proposed"
    ] == ["critical"]
    with pytest.raises(ValueError, match="min_severity"):
        seed_dependabot_alert_ideas(db, min_severity="severe", dry_run=True, now=NOW)


def test_seed_dependabot_alert_ideas_dry_run_reports_candidates_without_writes(db):
    _alert_activity(db)

    results = seed_dependabot_alert_ideas(db, days=14, dry_run=True, now=NOW)

    assert [(result.status, result.reason) for result in results] == [("proposed", "dry run")]
    assert results[0].source_metadata["alert_identifiers"] == ["dependabot_alert:taka/presence:7"]
    assert db.get_content_ideas(status="open") == []


def test_formatters_and_cli_support_json_and_text_output(db, capsys):
    _alert_activity(db)
    results = seed_dependabot_alert_ideas(db, days=14, dry_run=True, now=NOW)

    payload = json.loads(format_dependabot_alert_idea_results_json(results))
    assert payload[0]["status"] == "proposed"
    text = format_dependabot_alert_idea_results_text(results)
    assert "created=0 proposed=1 skipped=0" in text
    assert "urllib3" in text

    with patch.object(
        seed_dependabot_alert_ideas_cli,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        seed_dependabot_alert_ideas_cli,
        "seed_dependabot_alert_ideas",
        wraps=lambda db, **kwargs: seed_dependabot_alert_ideas(db, now=NOW, **kwargs),
    ):
        assert (
            seed_dependabot_alert_ideas_cli.main(
                [
                    "--days",
                    "14",
                    "--min-severity",
                    "high",
                    "--limit",
                    "1",
                    "--dry-run",
                    "--format",
                    "json",
                ]
            )
            == 0
        )

    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload[0]["status"] == "proposed"
    assert cli_payload[0]["severity"] == "high"
