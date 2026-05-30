from __future__ import annotations
import importlib.util, json, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from evaluation.model_usage_cache_savings_opportunities import build_model_usage_cache_savings_opportunities_report_from_db, format_model_usage_cache_savings_opportunities_json, format_model_usage_cache_savings_opportunities_text
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "model_usage_cache_savings_opportunities.py"; spec = importlib.util.spec_from_file_location("script_model_usage_cache_savings_opportunities", SCRIPT); script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c = sqlite3.connect(":memory:"); c.row_factory = sqlite3.Row
    c.executescript("CREATE TABLE model_usage (id TEXT, prompt TEXT, model TEXT, provider TEXT, task_type TEXT, input_tokens INTEGER, output_tokens INTEGER, cost REAL, created_at TEXT);")
    for i in range(3): c.execute("INSERT INTO model_usage VALUES (?,?,?,?,?,?,?,?,?)", (str(i), "Summarize this text", "gpt", "openai", "summary", 100, 20, 0.03, "2026-05-20T00:00:00Z"))
    c.commit(); return c
def test_cache_savings_report_and_cli(tmp_path, capsys):
    r = build_model_usage_cache_savings_opportunities_report_from_db(_db(), now=datetime(2026,5,27,tzinfo=timezone.utc))
    g = r["repeated_call_groups"][0]
    assert r["artifact_type"] == "model_usage_cache_savings_opportunities" and g["total_calls"] == 3 and g["duplicate_calls"] == 2
    assert r["estimated_savings"]["cacheable_cost"] > 0
    assert json.loads(format_model_usage_cache_savings_opportunities_json(r))["artifact_type"] == r["artifact_type"]
    assert "Model Usage Cache" in format_model_usage_cache_savings_opportunities_text(r)
    db = tmp_path / "db.sqlite"; out = sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(["--db", str(db), "--format", "text"]) == 0 and capsys.readouterr().out
    assert script.main(["--db", str(db), "--limit", "0"]) == 2
def test_missing_schema():
    assert build_model_usage_cache_savings_opportunities_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
