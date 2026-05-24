from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from output.blog_internal_link_opportunity_export import build_blog_internal_link_opportunity_export_from_db, format_blog_internal_link_opportunity_export_csv, format_blog_internal_link_opportunity_export_json

SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "export_blog_internal_link_opportunities.py"
spec = importlib.util.spec_from_file_location("export_blog_internal_link_opportunities_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_blog_link_export_excludes_existing_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE blog_posts (id INTEGER PRIMARY KEY, title TEXT, topics TEXT, series TEXT)")
    conn.execute("CREATE TABLE blog_internal_links (source_post_id INTEGER, target_post_id INTEGER)")
    conn.executemany("INSERT INTO blog_posts VALUES (?, ?, ?, ?)", [(1, "A", '["ai","ops"]', "s"), (2, "B", '["ai"]', "s"), (3, "C", '["ops"]', "z")])
    conn.execute("INSERT INTO blog_internal_links VALUES (1, 2)")
    export = build_blog_internal_link_opportunity_export_from_db(conn, topic="ai")
    assert all(not (r["source_post_id"] == 1 and r["target_post_id"] == 2) for r in export["rows"])
    assert export["rows"]
    assert json.loads(format_blog_internal_link_opportunity_export_json(export))["artifact_type"] == "blog_internal_link_opportunity_export"
    assert "source_post_id,target_post_id" in format_blog_internal_link_opportunity_export_csv(export)
    conn.commit()
    dest = sqlite3.connect(tmp_path / "blog.sqlite")
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(tmp_path / "blog.sqlite"), "--topic", "ai"]) == 0
    assert json.loads(capsys.readouterr().out)["rows"]


def test_missing_and_validation():
    assert build_blog_internal_link_opportunity_export_from_db(sqlite3.connect(":memory:"))["missing_tables"]
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
