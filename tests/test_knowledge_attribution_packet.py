"""Tests for generated-content knowledge attribution packets."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from knowledge.attribution_packet import (
    build_attribution_packet,
    format_attribution_packet_json,
    format_attribution_packet_text,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "build_attribution_packet.py"
)
spec = importlib.util.spec_from_file_location("build_attribution_packet", SCRIPT_PATH)
build_attribution_packet_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(build_attribution_packet_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, *, content_type: str = "x_post") -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content="A source-backed draft for publishing.",
        eval_score=8.0,
        eval_feedback="ready",
    )


def _knowledge(
    db,
    *,
    source_id: str,
    content: str = "Detailed source evidence for the draft.",
    insight: str = "Source insight for attribution.",
    source_url: str | None = "https://example.test/source",
    author: str | None = "Ada",
    license: str | None = "open",
    attribution_required: int = 0,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            license, attribution_required, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)""",
        (
            "curated_article",
            source_id,
            source_url,
            author,
            content,
            insight,
            license,
            attribution_required,
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_builder_returns_all_links_in_relevance_then_id_order(db):
    content_id = _content(db)
    low_id = _knowledge(db, source_id="low", source_url="https://example.test/low")
    first_id = _knowledge(db, source_id="first", source_url="https://example.test/first")
    second_id = _knowledge(db, source_id="second", source_url="https://example.test/second")
    db.insert_content_knowledge_links(
        content_id,
        [(low_id, 0.2), (second_id, 0.9), (first_id, 0.9)],
    )

    packet = build_attribution_packet(db, content_id)

    assert [source.knowledge_id for source in packet.sources] == [
        first_id,
        second_id,
        low_id,
    ]
    assert packet.source_count == 3
    assert packet.warning_count == 0


def test_open_attribution_required_and_restricted_sources_emit_warnings(db):
    content_id = _content(db, content_type="newsletter")
    open_id = _knowledge(
        db,
        source_id="open",
        license="open",
        attribution_required=0,
    )
    attribution_id = _knowledge(
        db,
        source_id="attr",
        source_url="https://example.test/attr",
        license="attribution_required",
        attribution_required=1,
    )
    restricted_id = _knowledge(
        db,
        source_id="restricted",
        source_url="https://example.test/restricted",
        license="restricted",
        attribution_required=0,
    )
    db.insert_content_knowledge_links(
        content_id,
        [(open_id, 0.91), (attribution_id, 0.82), (restricted_id, 0.73)],
    )

    packet = build_attribution_packet(db, content_id)
    payload = json.loads(format_attribution_packet_json(packet))
    text = format_attribution_packet_text(packet)

    assert payload["sources"][0]["license"] == "open"
    assert payload["sources"][0]["warnings"] == []
    assert "Source requires attribution." in payload["sources"][1]["warnings"]
    assert "Source license is restricted" in payload["sources"][2]["warnings"][0]
    assert f"knowledge #{attribution_id}: Source requires attribution." in text
    assert f"knowledge #{restricted_id}: Source license is restricted" in text
    assert "License: restricted" in text


def test_missing_url_and_license_metadata_warns_in_json_and_text(db):
    content_id = _content(db, content_type="blog_post")
    knowledge_id = _knowledge(
        db,
        source_id="missing-url",
        source_url=None,
        author=None,
        license=None,
        attribution_required=1,
    )
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.7)])

    packet = build_attribution_packet(db, content_id)
    payload = json.loads(format_attribution_packet_json(packet))
    text = format_attribution_packet_text(packet)

    warnings = payload["sources"][0]["warnings"]
    assert "Attribution-required source is missing source_url." in warnings
    assert "Attribution-required source is missing author." in warnings
    assert "Source license metadata is missing; treated as attribution_required." in warnings
    assert "URL: -" in text
    assert "Attribution-required source is missing source_url." in text


def test_include_open_flag_filters_open_sources_without_warnings(db):
    content_id = _content(db)
    open_id = _knowledge(db, source_id="open", license="open", attribution_required=0)
    attr_id = _knowledge(
        db,
        source_id="attr",
        license="attribution_required",
        attribution_required=1,
    )
    db.insert_content_knowledge_links(content_id, [(open_id, 0.9), (attr_id, 0.8)])

    packet = build_attribution_packet(db, content_id, include_open=False)

    assert [source.knowledge_id for source in packet.sources] == [attr_id]
    assert packet.include_open is False


def test_empty_packet_for_content_with_no_knowledge_links(db):
    content_id = _content(db)

    packet = build_attribution_packet(db, content_id)
    text = format_attribution_packet_text(packet)
    payload = json.loads(format_attribution_packet_json(packet))

    assert packet.sources == []
    assert packet.source_count == 0
    assert "- none" in text
    assert payload["sources"] == []


def test_missing_content_raises_clear_error(db):
    with pytest.raises(ValueError, match="Content ID 9999 not found"):
        build_attribution_packet(db, 9999)


def test_cli_outputs_text_json_empty_and_missing_content(db, capsys):
    content_id = _content(db)
    knowledge_id = _knowledge(
        db,
        source_id="cli",
        license="attribution_required",
        attribution_required=1,
    )
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.8)])

    with patch.object(
        build_attribution_packet_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = build_attribution_packet_script.main(
            ["--content-id", str(content_id)]
        )

    assert exit_code == 0
    assert capsys.readouterr().out.startswith(
        f"Attribution Packet: Content #{content_id}"
    )

    empty_id = _content(db)
    with patch.object(
        build_attribution_packet_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = build_attribution_packet_script.main(
            ["--content-id", str(empty_id), "--format", "json", "--include-open"]
        )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out)["sources"] == []

    with patch.object(
        build_attribution_packet_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = build_attribution_packet_script.main(["--content-id", "9999"])

    assert exit_code == 1
    assert "error: Content ID 9999 not found" in capsys.readouterr().err
