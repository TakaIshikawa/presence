"""Tests for static blog canonical URL auditing."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from output.blog_canonical_audit import (
    build_blog_canonical_audit_report,
    build_blog_canonical_audit_report_from_records,
    format_blog_canonical_audit_json,
    format_blog_canonical_audit_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "audit_blog_canonicals.py"
spec = importlib.util.spec_from_file_location("audit_blog_canonicals_script", SCRIPT_PATH)
audit_blog_canonicals_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(audit_blog_canonicals_script)


def _write_post(
    root: Path,
    filename: str,
    *,
    title: str,
    canonical_url: str | None,
    slug: str | None = None,
    source_content_ids: str = "[101]",
) -> Path:
    lines = ["---", f'title: "{title}"']
    if slug is not None:
        lines.append(f'slug: "{slug}"')
    if canonical_url is not None:
        lines.append(f'canonical_url: "{canonical_url}"')
    lines.append(f"source_content_ids: {source_content_ids}")
    lines.extend(["---", "", "Body."])
    path = root / filename
    path.write_text("\n".join(lines))
    return path


def test_duplicate_canonicals_titles_and_generated_content_refs_are_reported(tmp_path):
    first = _write_post(
        tmp_path,
        "first-post.md",
        title="First Post",
        canonical_url="https://example.com/blog/first-post/",
        source_content_ids="[42]",
    )
    second = _write_post(
        tmp_path,
        "second-post.md",
        title="First Post",
        canonical_url="https://example.com/blog/first-post",
        source_content_ids="[42]",
    )

    report = build_blog_canonical_audit_report(tmp_path)
    codes = [issue.code for issue in report.issues]

    assert report.ok is False
    assert codes.count("duplicate_canonical_url") == 2
    assert codes.count("duplicate_title") == 2
    assert codes.count("duplicate_generated_content_reference") == 2
    duplicate = next(issue for issue in report.issues if issue.code == "duplicate_canonical_url")
    assert duplicate.severity == "error"
    assert duplicate.file_path in {str(first), str(second)}
    assert duplicate.related_paths
    assert duplicate.remediation_hint


def test_missing_canonical_and_slug_mismatches_include_file_paths(tmp_path):
    missing = _write_post(
        tmp_path,
        "missing-canonical.md",
        title="Missing Canonical",
        canonical_url=None,
    )
    mismatch = _write_post(
        tmp_path,
        "expected-slug.md",
        title="Different Title",
        canonical_url="https://example.com/blog/canonical-slug",
        slug="wrong-slug",
    )

    report = build_blog_canonical_audit_report(tmp_path)
    by_code = {issue.code: issue for issue in report.issues}

    assert by_code["missing_canonical_url"].file_path == str(missing)
    assert by_code["slug_file_mismatch"].file_path == str(mismatch)
    assert by_code["slug_canonical_mismatch"].file_path == str(mismatch)
    assert by_code["title_slug_mismatch"].severity == "warning"


def test_malformed_frontmatter_is_reported_as_blocking_issue(tmp_path):
    bad = tmp_path / "bad.md"
    bad.write_text(
        """---
title: "Bad"
not valid
---

Body.
"""
    )

    report = build_blog_canonical_audit_report(tmp_path)

    assert report.ok is False
    assert report.blocking_issue_count == 1
    assert report.issues[0].code == "invalid_frontmatter_line"
    assert report.issues[0].file_path == str(bad)


def test_clean_output_and_json_are_deterministic(tmp_path):
    _write_post(
        tmp_path,
        "clean-output.md",
        title="Clean Output",
        canonical_url="https://example.com/blog/clean-output",
        slug="clean-output",
        source_content_ids="[7, 8]",
    )

    report = build_blog_canonical_audit_report(tmp_path)
    payload = json.loads(format_blog_canonical_audit_json(report))
    text = format_blog_canonical_audit_text(report)

    assert report.ok is True
    assert report.issues == ()
    assert payload["ok"] is True
    assert payload["entries"][0]["generated_content_ids"] == [7, 8]
    assert payload["issues"] == []
    assert "No canonical URL or publication identity issues found." in text


def test_plain_dictionary_records_are_audited():
    report = build_blog_canonical_audit_report_from_records(
        [
            {
                "title": "Clean Record",
                "slug": "clean-record",
                "canonical_url": "https://example.com/blog/clean-record",
                "generated_content_id": 11,
            },
            {
                "title": "Tracked Record",
                "slug": "tracked-record",
                "canonical_url": "https://example.com/blog/tracked-record?utm_source=newsletter",
                "generated_content_id": 12,
            },
            {
                "title": "Local Record",
                "slug": "local-record",
                "canonical_url": "http://localhost:8000/blog/local-record",
                "generated_content_id": 13,
            },
            {
                "title": "Missing Record",
                "slug": "missing-record",
                "generated_content_id": 14,
            },
        ]
    )
    codes = [issue.code for issue in report.issues]

    assert report.ok is False
    assert "tracking_parameter_canonical_url" in codes
    assert "unstable_canonical_host" in codes
    assert "missing_canonical_url" in codes
    assert report.entries[0].canonical_url == "https://example.com/blog/clean-record"


def test_metadata_records_and_duplicate_canonicals_are_audited():
    report = build_blog_canonical_audit_report_from_records(
        [
            {
                "file_path": "drafts/first.md",
                "metadata": {
                    "title": "First",
                    "slug": "first",
                    "canonical_url": "https://example.com/blog/first/",
                    "source_content_ids": [31],
                },
            },
            {
                "file_path": "drafts/second.md",
                "metadata": {
                    "title": "Second",
                    "slug": "second",
                    "canonical_url": "ftp://example.com/blog/second",
                    "source_content_ids": [32],
                },
            },
            {
                "file_path": "drafts/duplicate.md",
                "metadata": {
                    "title": "Duplicate",
                    "slug": "first",
                    "canonical_url": "https://example.com/blog/first",
                    "source_content_ids": [33],
                },
            },
        ],
        root_path="fixture-records",
    )
    codes = [issue.code for issue in report.issues]

    assert report.root_path == "fixture-records"
    assert codes.count("duplicate_canonical_url") == 2
    assert "invalid_canonical_url" in codes


def test_cli_json_and_strict_exit_behavior(tmp_path, capsys):
    _write_post(
        tmp_path,
        "needs-canonical.md",
        title="Needs Canonical",
        canonical_url=None,
    )

    result = audit_blog_canonicals_script.main(["--path", str(tmp_path), "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert result == 0
    assert payload["blocking_issue_count"] == 1
    assert payload["issues"][0]["severity"] == "error"
    assert payload["issues"][0]["file_path"].endswith("needs-canonical.md")
    assert payload["issues"][0]["remediation_hint"]

    strict_result = audit_blog_canonicals_script.main(["--path", str(tmp_path), "--strict"])
    assert strict_result == 1
