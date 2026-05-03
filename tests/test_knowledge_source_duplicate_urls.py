"""Tests for knowledge source duplicate URL reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from knowledge.source_duplicate_urls import (
    build_knowledge_duplicate_url_report,
    build_knowledge_duplicate_url_report_from_fixture,
    format_knowledge_duplicate_url_json,
    format_knowledge_duplicate_url_text,
    normalize_source_url,
)


NOW = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "report_knowledge_duplicate_urls.py"
)
spec = importlib.util.spec_from_file_location("report_knowledge_duplicate_urls", SCRIPT_PATH)
report_knowledge_duplicate_urls = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(report_knowledge_duplicate_urls)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_knowledge(
    db,
    *,
    source_id: str,
    source_url: str,
    source_type: str = "curated_article",
    title: str | None = None,
) -> int:
    metadata = json.dumps({"link_metadata": {"title": title}}, sort_keys=True) if title else None
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            approved, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            source_type,
            source_id,
            source_url,
            "Reporter",
            f"content for {source_id}",
            f"insight for {source_id}",
            1,
            metadata,
        ),
    )
    db.conn.commit()
    return cursor.lastrowid


def test_normalizer_removes_tracking_params_fragments_slashes_and_mobile_prefixes():
    assert (
        normalize_source_url(
            "HTTPS://M.Example.COM/articles/post/?utm_source=news&ref=home&b=2&a=1#section"
        )
        == "https://example.com/articles/post?a=1&b=2"
    )
    assert (
        normalize_source_url("https://www.example.com/article/?UTM_Campaign=spring")
        == "https://example.com/article"
    )
    assert normalize_source_url("ftp://example.com/article") is None


def test_report_groups_records_that_canonicalize_to_same_url(db):
    first_id = _insert_knowledge(
        db,
        source_id="alpha",
        source_url="https://Example.com/articles/alpha/?utm_medium=email#quote",
        title="Alpha",
    )
    second_id = _insert_knowledge(
        db,
        source_id="alpha-copy",
        source_url="https://example.com/articles/alpha",
        title="Alpha copy",
    )
    _insert_knowledge(
        db,
        source_id="beta",
        source_url="https://example.com/articles/beta/",
        title="Beta",
    )

    report = build_knowledge_duplicate_url_report(db, now=NOW)

    assert report.totals["rows_scanned"] == 3
    assert report.totals["cluster_count"] == 1
    cluster = report.clusters[0]
    assert cluster.normalized_url == "https://example.com/articles/alpha"
    assert [source.knowledge_id for source in cluster.sources] == [first_id, second_id]
    assert [source.title for source in cluster.sources] == ["Alpha", "Alpha copy"]


def test_distinct_articles_on_same_domain_remain_separate(db):
    _insert_knowledge(db, source_id="first", source_url="https://example.com/posts/first/")
    _insert_knowledge(db, source_id="second", source_url="https://example.com/posts/second/")

    report = build_knowledge_duplicate_url_report(db, now=NOW)

    assert report.clusters == ()
    assert report.totals["duplicate_source_count"] == 0


def test_query_parameters_that_are_not_tracking_keep_records_separate():
    rows = [
        {
            "id": 1,
            "source_type": "curated_article",
            "source_id": "first",
            "source_url": "https://example.com/search?q=alpha&utm_source=x",
        },
        {
            "id": 2,
            "source_type": "curated_article",
            "source_id": "second",
            "source_url": "https://example.com/search?q=beta",
        },
    ]

    report = build_knowledge_duplicate_url_report(rows, now=NOW)

    assert report.clusters == ()
    assert report.totals["url_source_count"] == 2


def test_fixture_json_and_cli_output_are_deterministic(tmp_path, db, capsys):
    fixture = tmp_path / "sources.jsonl"
    fixture.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "id": 10,
                        "source_type": "curated_article",
                        "source_id": "fixture-a",
                        "source_url": "https://mobile.example.com/story/?utm_campaign=x",
                        "metadata": {"link_metadata": {"title": "Fixture A"}},
                    }
                ),
                json.dumps(
                    {
                        "id": 11,
                        "source_type": "curated_article",
                        "source_id": "fixture-b",
                        "source_url": "https://example.com/story",
                    }
                ),
            ]
        )
    )

    report = build_knowledge_duplicate_url_report_from_fixture(fixture, now=NOW)
    payload = json.loads(format_knowledge_duplicate_url_json(report))

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "knowledge_source_duplicate_urls"
    assert payload["clusters"][0]["normalized_url"] == "https://example.com/story"
    assert payload["clusters"][0]["sources"][0]["title"] == "Fixture A"
    assert "Knowledge source duplicate URLs" in format_knowledge_duplicate_url_text(report)

    _insert_knowledge(
        db,
        source_id="cli-a",
        source_url="https://example.com/cli/?utm_source=x",
        source_type="curated_newsletter",
    )
    _insert_knowledge(
        db,
        source_id="cli-b",
        source_url="https://example.com/cli",
        source_type="curated_newsletter",
    )
    with patch.object(
        report_knowledge_duplicate_urls,
        "script_context",
        wraps=lambda: _script_context(db),
    ):
        exit_code = report_knowledge_duplicate_urls.main(
            ["--source-type", "curated_newsletter", "--format", "json"]
        )

    assert exit_code == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["totals"]["cluster_count"] == 1
    assert cli_payload["clusters"][0]["normalized_url"] == "https://example.com/cli"
    assert report_knowledge_duplicate_urls.main(["--limit", "-1"]) == 2
    assert "value must be nonnegative" in capsys.readouterr().err
