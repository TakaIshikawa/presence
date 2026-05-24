from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from evaluation.pipeline_stage_artifact_missingness import build_pipeline_stage_artifact_missingness_report_from_db, format_pipeline_stage_artifact_missingness_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"pipeline_stage_artifact_missingness.py"; spec=importlib.util.spec_from_file_location("pipeline_stage_artifact_missingness_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_artifact_missingness_and_cli(tmp_path,capsys):
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE pipeline_stages (id TEXT, run_id TEXT, stage_name TEXT, expected_artifact_types TEXT); CREATE TABLE pipeline_artifacts (id TEXT, stage_id TEXT, run_id TEXT, artifact_type TEXT, byte_size INTEGER);")
    c.execute("INSERT INTO pipeline_stages VALUES ('s1','r1','draft','html,json')")
    c.execute("INSERT INTO pipeline_artifacts VALUES ('a1','s1','r1','html',0)")
    c.execute("INSERT INTO pipeline_artifacts VALUES ('a2','s1','r1','html',10)")
    c.execute("INSERT INTO pipeline_artifacts VALUES ('a3','missing','r2','json',10)")
    r=build_pipeline_stage_artifact_missingness_report_from_db(c)
    assert {"missing_expected_artifact","zero_byte_payload","duplicate_artifact_type","orphan_artifact"} == {x["issue_type"] for x in r["findings"]}
    assert "Pipeline Stage Artifact" in format_pipeline_stage_artifact_missingness_text(r)
    c.commit(); path=tmp_path/"db.sqlite"
    with sqlite3.connect(path) as out: c.backup(out)
    assert script.main(["--db",str(path),"--format","text"])==0
    assert "Missingness" in capsys.readouterr().out
    assert script.main(["--db",str(path),"--limit","0"])==2
def test_pipeline_schema_gap():
    r=build_pipeline_stage_artifact_missingness_report_from_db(sqlite3.connect(":memory:"))
    assert r["missing_tables"]==["pipeline_artifacts","pipeline_stages"]
