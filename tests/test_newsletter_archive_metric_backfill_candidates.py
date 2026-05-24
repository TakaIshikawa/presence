from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.newsletter_archive_metric_backfill_candidates import build_newsletter_archive_metric_backfill_candidates_report_from_db, format_newsletter_archive_metric_backfill_candidates_json, format_newsletter_archive_metric_backfill_candidates_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'newsletter_archive_metric_backfill_candidates.py'; spec=importlib.util.spec_from_file_location('script_newsletter_archive_metric_backfill_candidates',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row; c.executescript('CREATE TABLE newsletter_issues (id TEXT, status TEXT, sent_at TEXT, audience_size INTEGER); CREATE TABLE newsletter_metrics (issue_id TEXT, metric_type TEXT, value INTEGER);'); c.execute('INSERT INTO newsletter_issues VALUES (?,?,?,?)',('n1','sent','2026-01-01T00:00:00+00:00',1000)); c.execute('INSERT INTO newsletter_metrics VALUES (?,?,?)',('n1','opens',10)); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_newsletter_archive_metric_backfill_candidates_report_from_db(_db())
    assert r['artifact_type']=='newsletter_archive_metric_backfill_candidates'
    assert r['findings']
    assert json.loads(format_newsletter_archive_metric_backfill_candidates_json(r))['artifact_type']=='newsletter_archive_metric_backfill_candidates'
    assert 'Newsletter' in format_newsletter_archive_metric_backfill_candidates_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','text']+['--min-age-hours', '1'])==0
    assert capsys.readouterr().out
    assert script.main(['--db',str(db),'--limit','0'])==2
def test_missing_schema():
    r=build_newsletter_archive_metric_backfill_candidates_report_from_db(sqlite3.connect(':memory:'))
    assert r['missing_tables'] or r['missing_columns']
