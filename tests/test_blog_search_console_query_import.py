from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path

from ingestion.blog_search_console_query_import import (
    import_blog_search_console_queries,
    parse_blog_search_console_queries,
    parse_blog_search_console_query_payload,
    upsert_blog_search_console_queries,
)

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "import_blog_search_console_queries.py"
spec = importlib.util.spec_from_file_location("import_blog_search_console_queries_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_search_console_import_cli(tmp_path, capsys):
    raw = '{"queries":[{"url":"https://example.com/a?x=1#f","query":"codex","date":"2026-05-01","clicks":"2","impressions":"10","ctr":"20%","position":"4.5","country":"US","device":"DESKTOP"}]}'
    rows = parse_blog_search_console_queries(raw)
    assert rows[0]["canonical_path"] == "/a"
    assert rows[0]["ctr"] == 0.2

    conn = sqlite3.connect(":memory:")
    summary = upsert_blog_search_console_queries(conn, rows)
    assert summary["summary"]["applied_count"] == 1
    upsert_blog_search_console_queries(conn, [{**rows[0], "clicks": 9}])
    assert conn.execute("SELECT clicks FROM blog_search_console_queries").fetchone()[0] == 9

    dry_run = upsert_blog_search_console_queries(conn, rows, dry_run=True)
    assert dry_run["summary"]["updated_count"] == 1
    assert dry_run["summary"]["applied_count"] == 0

    input_path = tmp_path / "q.json"
    input_path.write_text(raw)
    db_path = tmp_path / "db.sqlite"
    assert script.main(["--db", str(db_path), "--input", str(input_path), "--dry-run", "--format", "text"]) == 0


def test_legacy_payload_parser_and_positional_cli(tmp_path, capsys):
    rows = parse_blog_search_console_query_payload(
        "date,page,query,clicks,impressions,ctr,position\n2026-05-26,HTTPS://Ex.COM/A?q=1,Test,1,10,0.1,2\n"
    )
    assert rows[0]["canonical_path"] == "/A"
    assert rows[0]["query"] == "Test"

    conn = sqlite3.connect(":memory:")
    result = import_blog_search_console_queries(conn, rows)
    assert result["summary"]["applied_count"] == 1

    input_path = tmp_path / "in.jsonl"
    input_path.write_text(json.dumps({"observed_at": "2026", "page_url": "https://e/x", "query": "q"}) + "\n")
    db_path = tmp_path / "d.sqlite"
    assert script.main([str(input_path), "--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "blog_search_console_query_import"
