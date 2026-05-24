from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from output.reply_draft_qa_packet_export import build_reply_draft_qa_packet_export_from_db, format_reply_draft_qa_packet_export_json, format_reply_draft_qa_packet_export_jsonl

SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "export_reply_draft_qa_packets.py"
spec = importlib.util.spec_from_file_location("export_reply_draft_qa_packets_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_reply_qa_packets_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE reply_drafts (id INTEGER PRIMARY KEY, platform TEXT, status TEXT, text TEXT, source_context TEXT, persona TEXT, metadata TEXT)")
    conn.execute("CREATE TABLE reply_draft_citations (draft_id INTEGER, url TEXT, title TEXT)")
    conn.execute("INSERT INTO reply_drafts VALUES (1, 'x', 'pending', 'hi', 'ctx', 'warm', '{\"risk_flags\":[\"claim\"]}')")
    conn.execute("INSERT INTO reply_draft_citations VALUES (1, 'https://e.test', 'E')")
    export = build_reply_draft_qa_packet_export_from_db(conn, platform="x")
    assert export["packets"][0]["citations"][0]["url"] == "https://e.test"
    assert json.loads(format_reply_draft_qa_packet_export_json(export))["artifact_type"] == "reply_draft_qa_packet_export"
    assert json.loads(format_reply_draft_qa_packet_export_jsonl(export).splitlines()[0])["draft_id"] == 1
    conn.commit()
    dest = sqlite3.connect(tmp_path / "reply.sqlite")
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(tmp_path / "reply.sqlite"), "--format", "jsonl"]) == 0
    assert json.loads(capsys.readouterr().out)["draft_id"] == 1


def test_missing_optional_and_validation():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE reply_drafts (id INTEGER PRIMARY KEY, status TEXT, text TEXT)")
    conn.execute("INSERT INTO reply_drafts VALUES (1, 'pending', 'hi')")
    export = build_reply_draft_qa_packet_export_from_db(conn)
    assert "missing optional citation table" in export["diagnostics"]
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
