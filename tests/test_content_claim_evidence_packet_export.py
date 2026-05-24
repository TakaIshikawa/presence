from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from output.content_claim_evidence_packet_export import build_content_claim_evidence_packet_export_from_db, format_content_claim_evidence_packet_export_json, format_content_claim_evidence_packet_export_jsonl

SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "export_content_claim_evidence_packets.py"
spec = importlib.util.spec_from_file_location("export_content_claim_evidence_packets_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_claim_evidence_packets_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE content_claim_checks (id INTEGER PRIMARY KEY, content_id INTEGER, claim_text TEXT, verdict TEXT, reviewer_notes TEXT)")
    conn.execute("CREATE TABLE content_claim_evidence (claim_id INTEGER, url TEXT, snippet TEXT, label TEXT)")
    conn.execute("INSERT INTO content_claim_checks VALUES (1, 10, 'claim', 'supported', 'token=secret')")
    conn.execute("INSERT INTO content_claim_evidence VALUES (1, 'https://e.test', 'secret=abc evidence', 'supports')")
    export = build_content_claim_evidence_packet_export_from_db(conn, verdict="supported")
    packet = export["packets"][0]
    assert packet["evidence_items"][0]["url"] == "https://e.test"
    assert "[REDACTED]" in packet["reviewer_notes"]
    assert json.loads(format_content_claim_evidence_packet_export_json(export))["artifact_type"] == "content_claim_evidence_packet_export"
    assert json.loads(format_content_claim_evidence_packet_export_jsonl(export).splitlines()[0])["claim_id"] == 1
    conn.commit()
    dest = sqlite3.connect(tmp_path / "claim.sqlite")
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(tmp_path / "claim.sqlite"), "--format", "jsonl", "--verdict", "supported"]) == 0
    assert json.loads(capsys.readouterr().out)["claim_id"] == 1


def test_pending_missing_and_validation():
    assert build_content_claim_evidence_packet_export_from_db(sqlite3.connect(":memory:"))["missing_tables"]
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE content_claim_checks (id INTEGER PRIMARY KEY, claim_text TEXT)")
    conn.execute("INSERT INTO content_claim_checks VALUES (1, 'pending claim')")
    assert not build_content_claim_evidence_packet_export_from_db(conn)["packets"]
    assert build_content_claim_evidence_packet_export_from_db(conn, include_pending=True)["packets"][0]["packet_warnings"]
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
