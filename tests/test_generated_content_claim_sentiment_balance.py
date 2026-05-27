from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation.generated_content_claim_sentiment_balance import build_generated_content_claim_sentiment_balance_report_from_db
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"generated_content_claim_sentiment_balance.py"; spec=importlib.util.spec_from_file_location("generated_content_claim_sentiment_balance_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE generated_content (id TEXT, content_type TEXT, content TEXT, created_at TEXT)"); c.execute("INSERT INTO generated_content VALUES (?,?,?,?)",("c1","blog","This always proves every product is best excellent superior guaranteed.", "2026-05-26T00:00:00+00:00")); c.commit(); return c
def test_scores_terms_and_cli(tmp_path,capsys):
 r=build_generated_content_claim_sentiment_balance_report_from_db(_db(),positive_threshold=2)
 assert {"overly_positive","missing_qualifiers"} <= set(r["findings"][0]["reasons"]); assert r["findings"][0]["positive_terms"]["best"]==1
 db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close(); assert script.main(["--db",str(db),"--positive-threshold","2","--format","json"])==0; assert json.loads(capsys.readouterr().out)["artifact_type"]=="generated_content_claim_sentiment_balance"
