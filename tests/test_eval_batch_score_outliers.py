from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.eval_batch_score_outliers import build_eval_batch_score_outliers_report, build_eval_batch_score_outliers_report_from_db

NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "eval_batch_score_outliers.py"
spec = importlib.util.spec_from_file_location("eval_batch_score_outliers_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_flags_outliers_and_metadata_gaps():
    rows = [{"id": i, "batch_id": 1, "final_score": s, "threshold": 0.7, "batch_threshold": 0.7, "candidate_count": 1, "prompt_count": 1, "final_content": "x"} for i, s in enumerate([0.1, 0.7, 0.71, 0.72, 0.99], 1)]
    rows.append({"id": 6, "batch_id": 1, "final_score": None, "threshold": 0.6, "batch_threshold": 0.7, "candidate_count": 0, "prompt_count": 1, "final_content": "accepted"})
    report = build_eval_batch_score_outliers_report(rows, z_threshold=1.4, min_batch_size=3, now=NOW)
    assert report["artifact_type"] == "eval_batch_score_outliers"
    assert report["totals"]["by_reason"]["low_score_outlier"] >= 1
    assert report["totals"]["by_reason"]["missing_final_score"] == 1
    assert report["totals"]["by_reason"]["threshold_mismatch"] == 1
    assert report["totals"]["by_reason"]["count_anomaly"] == 1


def test_db_loader_and_cli(tmp_path, capsys):
    path = tmp_path / "eval.sqlite"
    conn = sqlite3.connect(path)
    conn.executescript("""CREATE TABLE eval_batches (id INTEGER, threshold REAL);
    CREATE TABLE eval_results (id INTEGER, batch_id INTEGER, final_score REAL, threshold REAL, candidate_count INTEGER, prompt_count INTEGER, final_content TEXT);
    INSERT INTO eval_batches VALUES (1, 0.7);
    INSERT INTO eval_results VALUES (1, 1, NULL, 0.7, 1, 1, 'x');""")
    conn.commit()
    assert build_eval_batch_score_outliers_report_from_db(conn, now=NOW)["totals"]["by_reason"]["missing_final_score"] == 1
    assert script.main(["--db", str(path), "--format", "json", "--z-threshold", "1.5", "--min-batch-size", "1"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "eval_batch_score_outliers"
