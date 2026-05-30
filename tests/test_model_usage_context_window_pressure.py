from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from evaluation.model_usage_context_window_pressure import build_model_usage_context_window_pressure_report_from_db, format_model_usage_context_window_pressure_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"model_usage_context_window_pressure.py"; spec=importlib.util.spec_from_file_location("model_usage_context_window_pressure_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_context_pressure_and_cli(tmp_path,capsys):
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE model_usage (id INTEGER, operation TEXT, model TEXT, prompt_tokens INTEGER, completion_tokens INTEGER, total_tokens INTEGER, context_limit INTEGER)")
    c.executemany("INSERT INTO model_usage VALUES (?,?,?,?,?,?,?)",[(1,"draft","custom",900,5,905,1000),(2,"draft","custom",950,5,955,1000),(3,"draft","custom",1100,1,1101,1000),(4,"summ","gpt-4",7000,20,7020,None)])
    r=build_model_usage_context_window_pressure_report_from_db(c, pressure_threshold=.8)
    assert {"high_context_pressure","over_context_limit","missing_context_limit","rising_operation_pressure"} <= {x["issue_type"] for x in r["findings"]}
    assert "Model Usage Context" in format_model_usage_context_window_pressure_text(r)
    c.commit(); path=tmp_path/"db.sqlite"
    with sqlite3.connect(path) as out: c.backup(out)
    assert script.main(["--db",str(path),"--format","text","--pressure-threshold",".8"])==0
    assert "Context Window" in capsys.readouterr().out
    assert script.main(["--db",str(path),"--pressure-threshold","2"])==2
def test_model_usage_schema_gap():
    r=build_model_usage_context_window_pressure_report_from_db(sqlite3.connect(":memory:"))
    assert r["missing_tables"]==["model_usage"]
