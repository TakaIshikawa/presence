from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.engagement_prediction_component_completeness import build_engagement_prediction_component_completeness_report, build_engagement_prediction_component_completeness_report_from_db


NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "engagement_prediction_component_completeness.py"
spec = importlib.util.spec_from_file_location("engagement_prediction_component_completeness_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_flags_prediction_component_gaps():
    report = build_engagement_prediction_component_completeness_report([
        {"id": 1, "content_id": 1, "hook_strength": None, "specificity": 1.2, "emotional_resonance": 0.5, "novelty": 0.5, "actionability": 0.5, "prompt_version": "", "prompt_hash": "", "published_at": "2026-05-18T00:00:00+00:00", "predicted_score": 0.2, "actual_engagement_score": 0.5, "prediction_error": 0.1}
    ], max_backfill_age_hours=24, now=NOW)
    assert report["artifact_type"] == "engagement_prediction_component_completeness"
    assert report["totals"]["by_reason"]["missing_component"] == 1
    assert report["totals"]["by_reason"]["invalid_component_range"] == 1
    assert report["totals"]["by_reason"]["missing_prompt_identity"] == 1
    assert report["totals"]["by_reason"]["prediction_error_mismatch"] == 1


def test_db_loader_and_cli(tmp_path, capsys):
    path = tmp_path / "ep.sqlite"
    conn = sqlite3.connect(path)
    conn.executescript("""CREATE TABLE generated_content (id INTEGER PRIMARY KEY, published_at TEXT);
    CREATE TABLE engagement_predictions (id INTEGER, content_id INTEGER, predicted_score REAL, hook_strength REAL, specificity REAL, emotional_resonance REAL, novelty REAL, actionability REAL, prompt_version TEXT, prompt_hash TEXT, actual_engagement_score REAL, prediction_error REAL);
    INSERT INTO generated_content VALUES (1, '2026-05-18T00:00:00+00:00');
    INSERT INTO engagement_predictions VALUES (1, 1, 0.2, NULL, 0.2, 0.2, 0.2, 0.2, NULL, NULL, NULL, NULL);""")
    conn.commit()
    assert build_engagement_prediction_component_completeness_report_from_db(conn, max_backfill_age_hours=24, now=NOW)["totals"]["by_reason"]["stale_actual_backfill"] == 1
    assert script.main(["--db", str(path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "engagement_prediction_component_completeness"
