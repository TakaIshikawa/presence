from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.generated_content_disclosure_label_coverage import build_generated_content_disclosure_label_coverage_report_from_db, format_generated_content_disclosure_label_coverage_json, format_generated_content_disclosure_label_coverage_text

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "generated_content_disclosure_label_coverage.py"
spec = importlib.util.spec_from_file_location("generated_content_disclosure_label_coverage_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_disclosure_coverage_and_cli(tmp_path, capsys):
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript("CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content_type TEXT, platform TEXT, status TEXT, metadata TEXT, tags TEXT, disclosure TEXT);")
    c.execute("INSERT INTO generated_content VALUES (1,'post','x','published','{}','[]',NULL)")
    c.execute("INSERT INTO generated_content VALUES (2,'post','x','draft','{}','[\"ai_generated\"]',NULL)")
    c.execute("INSERT INTO generated_content VALUES (3,'post','x','queued','{\"labels\":[\"ai_generated\"]}','[]',NULL)")
    report = build_generated_content_disclosure_label_coverage_report_from_db(c, required_labels=["ai_generated"])
    assert [r["content_id"] for r in report["findings"]] == ["1"]
    assert report["findings"][0]["severity"] == "high"
    assert json.loads(format_generated_content_disclosure_label_coverage_json(report))["artifact_type"] == "generated_content_disclosure_label_coverage"
    assert "Generated Content Disclosure" in format_generated_content_disclosure_label_coverage_text(report)
    c.commit()
    path = tmp_path / "db.sqlite"
    with sqlite3.connect(path) as out:
        c.backup(out)
    assert script.main(["--db", str(path), "--format", "text", "--required-label", "ai_generated"]) == 0
    assert "Generated Content Disclosure" in capsys.readouterr().out
    assert script.main(["--db", str(path), "--limit", "0"]) == 2


def test_missing_generated_content_table():
    report = build_generated_content_disclosure_label_coverage_report_from_db(sqlite3.connect(":memory:"))
    assert report["missing_tables"] == ["generated_content"]

