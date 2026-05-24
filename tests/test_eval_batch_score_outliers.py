from datetime import datetime, timezone
import sqlite3

from evaluation.eval_batch_score_outliers import build_eval_batch_score_outliers_report, build_eval_batch_score_outliers_report_from_db

NOW = datetime(2026, 5, 1, tzinfo=timezone.utc)


def test_eval_batch_score_outliers():
    rows = [{"result_id": i, "batch_id": 1, "final_score": s, "batch_threshold": 0.5, "result_threshold": 0.6, "candidate_count": 1, "prompt_count": 2} for i, s in enumerate([0.5, 0.51, 0.52, 0.0, 1.0], 1)]
    rows.append({"result_id": 6, "batch_id": 1, "final_score": None, "accepted": 1})
    report = build_eval_batch_score_outliers_report(rows, z_threshold=1.0, min_batch_size=2, now=NOW)
    reasons = {i["reason"] for g in report["findings"] for i in g["items"]}
    assert {"missing_final_score", "threshold_mismatch", "count_anomaly"} <= reasons
    assert "score_outlier_low" in reasons or "score_outlier_high" in reasons
    assert build_eval_batch_score_outliers_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == ["eval_batches", "eval_results"]
