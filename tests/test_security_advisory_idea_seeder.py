"""Tests for GitHub security advisory idea seeding."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.security_advisory_idea_seeder import (
    SOURCE_NAME,
    build_security_advisory_idea_candidates,
    format_security_advisory_ideas_json,
    format_security_advisory_ideas_text,
    seed_security_advisory_ideas,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "seed_security_advisory_ideas.py"
spec = importlib.util.spec_from_file_location("seed_security_advisory_ideas_script", SCRIPT_PATH)
seed_security_advisory_ideas_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(seed_security_advisory_ideas_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _advisory(
    db,
    *,
    repo_name: str = "taka/presence",
    number: str = "GHSA-1111-2222-3333",
    title: str = "urllib3 redirect handling vulnerability",
    severity: str = "high",
    state: str = "published",
    days_ago: int = 1,
    affected_packages: list[dict] | None = None,
    cves: list[str] | None = None,
    ghsa_ids: list[str] | None = None,
    withdrawn: bool = False,
) -> int:
    updated_at = (NOW - timedelta(days=days_ago)).isoformat()
    affected_packages = affected_packages if affected_packages is not None else [
        {
            "ecosystem": "pip",
            "name": "urllib3",
            "vulnerable_version_range": "<2.0.7",
            "patched_versions": ">=2.0.7",
        }
    ]
    metadata = {
        "activity_id": f"{repo_name}#{number}:security_advisory",
        "ghsa_id": number,
        "ghsa_ids": ghsa_ids or [number],
        "cves": cves or ["CVE-2026-0001"],
        "severity": severity,
        "state": state,
        "affected_packages": affected_packages,
        "package_names": [package["name"] for package in affected_packages if package.get("name")],
        "advisory_url": f"https://github.com/{repo_name}/security/advisories/{number}",
        "updated_at": updated_at,
    }
    if withdrawn:
        metadata["withdrawn_at"] = updated_at
        state = "withdrawn"
    return db.upsert_github_activity(
        repo_name=repo_name,
        activity_type="security_advisory",
        number=number,
        title=title,
        state=state,
        author="github",
        url=metadata["advisory_url"],
        updated_at=updated_at,
        created_at=(NOW - timedelta(days=days_ago, hours=1)).isoformat(),
        closed_at=updated_at if withdrawn else None,
        body=f"Details for {title}",
        labels=[severity, state],
        metadata=metadata,
    )


def test_dry_run_ranks_by_severity_specificity_and_recency_without_writing(db):
    _advisory(db, number="GHSA-MED", severity="medium", days_ago=1)
    _advisory(
        db,
        number="GHSA-CRIT-VAGUE",
        severity="critical",
        days_ago=1,
        affected_packages=[{"name": "django"}],
    )
    _advisory(
        db,
        number="GHSA-CRIT-SPECIFIC",
        severity="critical",
        days_ago=2,
        affected_packages=[
            {
                "ecosystem": "pip",
                "name": "django",
                "vulnerable_version_range": "<4.2.21",
                "patched_versions": ">=4.2.21",
            }
        ],
    )

    report = seed_security_advisory_ideas(db, dry_run=True, now=NOW)

    assert [result.advisory_id for result in report.results] == [
        "GHSA-CRIT-SPECIFIC",
        "GHSA-CRIT-VAGUE",
        "GHSA-MED",
    ]
    assert [result.status for result in report.results] == ["dry-run", "dry-run", "dry-run"]
    assert report.results[0].priority == "high"
    assert db.get_content_ideas(status="open") == []


def test_skips_withdrawn_advisories_unless_explicitly_included(db):
    _advisory(db, number="GHSA-WITHDRAWN", severity="critical", withdrawn=True)

    skipped = seed_security_advisory_ideas(db, dry_run=True, now=NOW)
    included = seed_security_advisory_ideas(db, dry_run=True, include_withdrawn=True, now=NOW)

    assert skipped.results[0].status == "skipped"
    assert skipped.results[0].reason == "withdrawn advisory"
    assert included.results[0].status == "dry-run"


def test_insert_mode_creates_deduplicated_content_ideas_with_fingerprint_metadata(db):
    _advisory(db, number="GHSA-INSERT", severity="high")

    report = seed_security_advisory_ideas(db, dry_run=False, now=NOW)

    assert [(result.status, result.idea_id) for result in report.results] == [("created", 1)]
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    assert ideas[0]["source"] == SOURCE_NAME
    metadata = json.loads(ideas[0]["source_metadata"])
    assert metadata["source"] == SOURCE_NAME
    assert metadata["source_id"] == metadata["advisory_fingerprint"]
    assert metadata["advisory_fingerprint_id"] == metadata["advisory_fingerprint"]
    assert metadata["activity_id"] == "taka/presence#GHSA-INSERT:security_advisory"
    assert metadata["repo_name"] == "taka/presence"
    assert metadata["severity"] == "high"
    assert metadata["affected_packages"][0]["name"] == "urllib3"

    second = seed_security_advisory_ideas(db, dry_run=False, now=NOW)
    assert second.results[0].status == "skipped"
    assert second.results[0].reason == "active duplicate"
    assert second.results[0].idea_id == 1
    assert len(db.get_content_ideas(status=None)) == 1


def test_filters_json_and_text_output_are_stable(db):
    _advisory(db, repo_name="taka/presence", number="GHSA-ONE", severity="low")
    _advisory(db, repo_name="acme/widget", number="GHSA-TWO", severity="critical")

    candidates = build_security_advisory_idea_candidates(
        db,
        repo="taka/presence",
        limit=1,
        now=NOW,
    )
    report = seed_security_advisory_ideas(db, repo="taka/presence", limit=1, now=NOW)
    payload = json.loads(format_security_advisory_ideas_json(report))
    text = format_security_advisory_ideas_text(report)

    assert [candidate.repo_name for candidate in candidates] == ["taka/presence"]
    assert payload["artifact_type"] == "security_advisory_idea_seed"
    assert payload["filters"]["repo"] == "taka/presence"
    assert payload["summary"]["dry_run"] == 1
    assert payload["results"][0]["source_metadata"]["advisory_fingerprint"]
    assert "dry_run=1" in text
    assert "GHSA-ONE" in text


def test_cli_supports_dry_run_insert_and_include_withdrawn(db, capsys):
    _advisory(db, number="GHSA-CLI", severity="critical", withdrawn=True)

    with patch.object(
        seed_security_advisory_ideas_script,
        "script_context",
        side_effect=lambda: _script_context(db),
    ), patch.object(
        seed_security_advisory_ideas_script,
        "seed_security_advisory_ideas",
        wraps=lambda db, **kwargs: seed_security_advisory_ideas(db, now=NOW, **kwargs),
    ):
        dry_exit = seed_security_advisory_ideas_script.main(
            ["--days", "7", "--repo", "taka/presence", "--limit", "1", "--format", "json"]
        )
        dry_payload = json.loads(capsys.readouterr().out)
        insert_exit = seed_security_advisory_ideas_script.main(
            [
                "--days",
                "7",
                "--repo",
                "taka/presence",
                "--limit",
                "1",
                "--include-withdrawn",
                "--insert",
                "--format",
                "json",
            ]
        )
        insert_payload = json.loads(capsys.readouterr().out)

    assert dry_exit == 0
    assert insert_exit == 0
    assert dry_payload["summary"]["skipped"] == 1
    assert insert_payload["summary"]["created"] == 1
    assert insert_payload["filters"]["include_withdrawn"] is True
