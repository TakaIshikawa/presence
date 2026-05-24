from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.pipeline_run_artifact_retention_gaps import build_pipeline_run_artifact_retention_gaps_report_from_db, format_pipeline_run_artifact_retention_gaps_json, format_pipeline_run_artifact_retention_gaps_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'pipeline_run_artifact_retention_gaps.py'; spec=importlib.util.spec_from_file_location('script_pipeline_run_artifact_retention_gaps',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row; c.executescript('CREATE TABLE pipeline_runs (id TEXT, stage TEXT, status TEXT, created_at TEXT); CREATE TABLE pipeline_artifacts (id TEXT, run_id TEXT, status TEXT, size_bytes INTEGER, url TEXT, created_at TEXT);'); c.execute('INSERT INTO pipeline_runs VALUES (?,?,?,?)',('r1','build','success','2026-01-01')); c.execute('INSERT INTO pipeline_artifacts VALUES (?,?,?,?,?,?)',('a1','r1','ok',999999999,'','2026-01-01')); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_pipeline_run_artifact_retention_gaps_report_from_db(_db())
    assert r['artifact_type']=='pipeline_run_artifact_retention_gaps'
    assert r['findings']
    assert json.loads(format_pipeline_run_artifact_retention_gaps_json(r))['artifact_type']=='pipeline_run_artifact_retention_gaps'
    assert 'Pipeline' in format_pipeline_run_artifact_retention_gaps_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','text']+['--max-age-days', '1', '--max-size-bytes', '10'])==0
    assert capsys.readouterr().out
    assert script.main(['--db',str(db),'--limit','0'])==2
def test_missing_schema():
    r=build_pipeline_run_artifact_retention_gaps_report_from_db(sqlite3.connect(':memory:'))
    assert r['missing_tables'] or r['missing_columns']
