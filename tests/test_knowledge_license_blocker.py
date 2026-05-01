"""Tests for publish-safety knowledge license checks."""

from __future__ import annotations

import importlib.util
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.knowledge_license_blocker import check_knowledge_license


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "check_knowledge_license.py"
)
spec = importlib.util.spec_from_file_location(
    "check_knowledge_license_script",
    SCRIPT_PATH,
)
check_knowledge_license_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_knowledge_license_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str = "Generated content") -> int:
    return db.insert_generated_content(
        "x_post",
        ["sha"],
        ["uuid"],
        text,
        8.0,
        "ok",
    )


def _knowledge(
    db,
    *,
    source_id: str,
    license: str = "open",
    source_url: str | None = "https://example.test/source",
    approved: int = 1,
    attribution_required: int = 0,
    insight: str | None = None,
    author: str = "Ada",
) -> int:
    return db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            license, attribution_required, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            source_id,
            source_url,
            author,
            f"Content for {source_id}",
            insight,
            license,
            attribution_required,
            approved,
        ),
    ).lastrowid


def test_passes_with_no_linked_knowledge(db):
    report = check_knowledge_license(db, _content(db), platform="x")

    assert report.status == "pass"
    assert report.passed is True
    assert report.blocked is False
    assert report.linked_knowledge_count == 0
    assert report.as_dict()["findings"] == []


def test_passes_with_approved_open_knowledge(db):
    content_id = _content(db)
    knowledge_id = _knowledge(db, source_id="open", license="open")
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    report = check_knowledge_license(db, content_id)

    assert report.status == "pass"
    assert report.findings == []
    assert report.attribution_groups == []


def test_blocks_restricted_and_unapproved_knowledge(db):
    content_id = _content(db)
    restricted_id = _knowledge(db, source_id="restricted", license="restricted")
    unapproved_id = _knowledge(
        db,
        source_id="unapproved",
        license="open",
        approved=0,
    )
    db.insert_content_knowledge_links(
        content_id,
        [(restricted_id, 0.9), (unapproved_id, 0.8)],
    )

    report = check_knowledge_license(db, content_id)

    assert report.status == "block"
    assert report.passed is False
    assert report.blocked is True
    assert [finding.kind for finding in report.findings] == [
        "restricted_license",
        "unapproved_knowledge",
    ]


def test_missing_attribution_source_url_warns_or_blocks_in_strict_mode(db):
    content_id = _content(db)
    knowledge_id = _knowledge(
        db,
        source_id="missing-url",
        license="attribution_required",
        source_url=None,
        attribution_required=1,
    )
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    report = check_knowledge_license(db, content_id, strict=False)
    strict_report = check_knowledge_license(db, content_id, strict=True)

    assert report.status == "warn"
    assert report.passed is True
    assert report.findings[0].severity == "warn"
    assert strict_report.status == "block"
    assert strict_report.blocked is True
    assert strict_report.findings[0].severity == "block"


def test_groups_attribution_snippets_by_source_url(db):
    content_id = _content(db)
    first_id = _knowledge(
        db,
        source_id="first",
        license="attribution_required",
        source_url="https://example.test/a",
        attribution_required=1,
        insight="First useful point.",
        author="Ada",
    )
    second_id = _knowledge(
        db,
        source_id="second",
        license="attribution_required",
        source_url="https://example.test/a",
        attribution_required=1,
        insight="Second useful point.",
        author="Grace",
    )
    third_id = _knowledge(
        db,
        source_id="third",
        license="attribution_required",
        source_url="https://example.test/b",
        attribution_required=1,
        insight="Third useful point.",
    )
    db.insert_content_knowledge_links(
        content_id,
        [(first_id, 0.7), (second_id, 0.9), (third_id, 0.8)],
    )

    report = check_knowledge_license(db, content_id, platform="newsletter")

    assert report.status == "pass"
    assert report.platform == "newsletter"
    assert [group.source_url for group in report.attribution_groups] == [
        "https://example.test/a",
        "https://example.test/b",
    ]
    assert [
        snippet.knowledge_id for snippet in report.attribution_groups[0].snippets
    ] == [second_id, first_id]
    assert report.attribution_groups[0].snippets[0].snippet == "Second useful point."


def test_cli_outputs_json_text_and_nonzero_when_blocked(db, capsys):
    content_id = _content(db)
    knowledge_id = _knowledge(db, source_id="restricted", license="restricted")
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    with patch.object(
        check_knowledge_license_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = check_knowledge_license_script.main(
            ["--content-id", str(content_id), "--platform", "x", "--json"]
        )

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out)
    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["status"] == "block"
    assert payload["findings"][0]["kind"] == "restricted_license"

    with patch.object(
        check_knowledge_license_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = check_knowledge_license_script.main(
            ["--content-id", str(content_id), "--platform", "x"]
        )

    assert exit_code == 1
    output = capsys.readouterr().out
    assert "Knowledge license check: block" in output
    assert "restricted_license" in output
