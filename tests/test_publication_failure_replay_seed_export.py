from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from output.publication_failure_replay_seed_export import build_publication_failure_replay_seed_export_from_db, format_publication_failure_replay_seed_export_json, format_publication_failure_replay_seed_export_jsonl

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "export_publication_failure_replay_seeds.py"
spec = importlib.util.spec_from_file_location("export_publication_failure_replay_seeds_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE publication_attempts (id INTEGER PRIMARY KEY, content_id INTEGER, platform TEXT, status TEXT, error TEXT, payload TEXT, created_at TEXT)")
    return conn


def test_replay_seed_json_jsonl_and_cli(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO publication_attempts VALUES (1, 10, 'x', 'failed', 'timeout token=abc', '{\"token\":\"abc\",\"ref\":\"p\"}', ?)", (NOW.isoformat(),))
    export = build_publication_failure_replay_seed_export_from_db(conn, now=NOW)
    assert export["rows"][0]["failure_reason"].startswith("timeout")
    assert len(export["rows"][0]["payload_digest"]) == 16
    assert json.loads(format_publication_failure_replay_seed_export_json(export))["artifact_type"] == "publication_failure_replay_seed_export"
    assert json.loads(format_publication_failure_replay_seed_export_jsonl(export).splitlines()[0])["attempt_id"] == 1
    conn.commit()
    dest = sqlite3.connect(tmp_path / "pub.sqlite")
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(tmp_path / "pub.sqlite"), "--format", "jsonl", "--platform", "x"]) == 0
    assert json.loads(capsys.readouterr().out)["platform"] == "x"


def test_missing_empty_and_validation():
    assert build_publication_failure_replay_seed_export_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"]
    assert build_publication_failure_replay_seed_export_from_db(_conn(), now=NOW)["empty_state"]["is_empty"]
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
