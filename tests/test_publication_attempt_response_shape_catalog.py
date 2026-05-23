from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.publication_attempt_response_shape_catalog import build_publication_attempt_response_shape_catalog_report, build_publication_attempt_response_shape_catalog_report_from_db

NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_response_shape_catalog.py"
spec = importlib.util.spec_from_file_location("publication_attempt_response_shape_catalog_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_catalogs_shapes_and_findings():
    report = build_publication_attempt_response_shape_catalog_report([
        {"id": 1, "platform": "x", "success": 1, "response_metadata": '{"ok":true}', "error_category": None},
        {"id": 2, "platform": "x", "success": 0, "response_metadata": "{bad", "error_category": "network"},
        {"id": 3, "platform": "x", "success": 0, "response_metadata": '{"error":"x"}', "error_category": "network"},
    ], max_metadata_bytes=5, rare_shape_threshold=1, now=NOW)
    assert report["artifact_type"] == "publication_attempt_response_shape_catalog"
    assert report["shape_summaries"]
    assert report["totals"]["by_reason"]["malformed_json"] == 1
    assert report["totals"]["by_reason"]["missing_success_response_key"] == 1
    assert report["totals"]["by_reason"]["oversized_metadata"] >= 1
    assert report["totals"]["by_reason"]["rare_shape"] >= 1


def test_db_loader_and_cli(tmp_path, capsys):
    path = tmp_path / "pa.sqlite"
    conn = sqlite3.connect(path)
    conn.executescript("""CREATE TABLE publication_attempts (id INTEGER, platform TEXT, success INTEGER, error_category TEXT, response_metadata TEXT);
    INSERT INTO publication_attempts VALUES (1, 'x', 1, NULL, '{"id":"1"}');""")
    conn.commit()
    assert build_publication_attempt_response_shape_catalog_report_from_db(conn, now=NOW)["shape_summaries"][0]["count"] == 1
    assert script.main(["--db", str(path), "--format", "json", "--max-metadata-bytes", "100", "--rare-shape-threshold", "1"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "publication_attempt_response_shape_catalog"
