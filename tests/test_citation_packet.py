"""Tests for generated-content citation packets."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.citation_packet import (
    build_citation_packet,
    format_json_packet,
    format_markdown_packet,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "build_citation_packet.py"
spec = importlib.util.spec_from_file_location("build_citation_packet_script", SCRIPT_PATH)
build_citation_packet_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(build_citation_packet_script)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _knowledge(
    db,
    *,
    source_id: str,
    content: str,
    insight: str,
    source_url: str = "https://example.test/redis",
    title: str = "Redis Search Notes",
    license: str = "open",
    approved: int = 1,
) -> int:
    metadata = {
        "link_metadata": {
            "canonical_url": source_url,
            "title": title,
            "site_name": "Example",
        }
    }
    return db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            license, attribution_required, approved, published_at, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            source_id,
            source_url,
            "Ada",
            content,
            insight,
            license,
            0,
            approved,
            (NOW - timedelta(days=12)).isoformat(),
            json.dumps(metadata),
        ),
    ).lastrowid


def _seed_packet_content(db) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=(
            "Redis added vector indexing. "
            "Redis and Postgres added vector indexing. "
            "MySQL removed JSONB indexing."
        ),
        eval_score=8.1,
        eval_feedback="claim heavy",
        claim_check_summary={
            "supported_count": 1,
            "unsupported_count": 2,
            "annotation_text": "factual: MySQL removed JSONB indexing. (factual terms not found in sources)",
        },
    )
    knowledge_id = _knowledge(
        db,
        source_id="redis-vector",
        content="Redis added vector indexing for search workloads.",
        insight="Redis added vector indexing.",
    )
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.93)])
    return content_id


def test_packet_includes_unsupported_and_weak_claims_by_default(db):
    content_id = _seed_packet_content(db)

    packet = build_citation_packet(db, content_id, now=NOW)

    assert packet.available is True
    assert packet.claim_count == 3
    assert packet.included_claim_count == 2
    assert [claim.support_status for claim in packet.claims] == [
        "unsupported",
        "weakly_supported",
    ]
    weak = packet.claims[1]
    assert weak.text == "Redis and Postgres added vector indexing."
    assert weak.sources[0].title == "Redis Search Notes"
    assert weak.sources[0].canonical_url == "https://example.test/redis"
    assert weak.sources[0].freshness["age_days"] == 12.0
    assert weak.sources[0].license["status"] == "open"
    assert weak.sources[0].metadata["link_metadata"]["site_name"] == "Example"


def test_packet_can_include_supported_claims(db):
    content_id = _seed_packet_content(db)

    packet = build_citation_packet(db, content_id, include_supported=True, now=NOW)

    assert packet.included_claim_count == 3
    assert packet.supported_count == 1
    assert packet.claims[-1].support_status == "supported"
    assert packet.claims[-1].text == "Redis added vector indexing."


def test_markdown_is_stable_with_one_section_per_claim(db):
    content_id = _seed_packet_content(db)

    output = format_markdown_packet(
        build_citation_packet(db, content_id, include_supported=True, now=NOW)
    )

    assert output.startswith(f"# Citation Packet: Content #{content_id}")
    assert output.count("## Claim ") == 3
    assert "## Claim 1: unsupported" in output
    assert "## Claim 2: weakly_supported" in output
    assert "## Claim 3: supported" in output
    assert "- URL: https://example.test/redis" in output
    assert "- License: open, approved=yes" in output


def test_json_preserves_source_metadata(db):
    content_id = _seed_packet_content(db)

    payload = json.loads(format_json_packet(build_citation_packet(db, content_id, now=NOW)))

    assert list(payload.keys()) == sorted(payload.keys())
    source = payload["claims"][1]["sources"][0]
    assert source["metadata"]["link_metadata"]["title"] == "Redis Search Notes"
    assert source["license"]["approved"] is True
    assert source["freshness"]["source_timestamp"] == "2026-04-19T12:00:00+00:00"


def test_missing_claim_or_citation_tables_returns_unavailable_signal():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            content TEXT,
            created_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO generated_content (id, content_type, content) VALUES (1, 'x_post', 'Redis added vectors.')"
    )

    packet = build_citation_packet(conn, 1, now=NOW)
    output = format_markdown_packet(packet)

    assert packet.available is False
    assert packet.missing_required_tables == [
        "content_claim_checks",
        "content_knowledge_links",
        "knowledge",
    ]
    assert "Unavailable" in output
    assert "Missing required tables: content_claim_checks, content_knowledge_links, knowledge" in output


def test_cli_outputs_markdown_and_json(db, capsys):
    content_id = _seed_packet_content(db)

    with patch.object(
        build_citation_packet_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = build_citation_packet_script.main(["--content-id", str(content_id)])

    assert exit_code == 0
    assert capsys.readouterr().out.startswith(f"# Citation Packet: Content #{content_id}")

    with patch.object(
        build_citation_packet_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = build_citation_packet_script.main(
            ["--content-id", str(content_id), "--format", "json", "--include-supported"]
        )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["include_supported"] is True
    assert payload["included_claim_count"] == 3
