from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.visual_asset_rendition_completeness import build_visual_asset_rendition_completeness_report_from_db, format_visual_asset_rendition_completeness_json, format_visual_asset_rendition_completeness_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"visual_asset_rendition_completeness.py"; spec=importlib.util.spec_from_file_location("script_visual_asset_rendition_completeness",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE visual_assets (id TEXT, content_id TEXT, channel TEXT, alt_text TEXT, metadata TEXT); CREATE TABLE visual_asset_renditions (asset_id TEXT, rendition_type TEXT);")
    c.execute("INSERT INTO visual_assets VALUES (?,?,?,?,?)",("a1","c1","newsletter","",'{\"renditions\":[\"thumbnail\"]}')); c.execute("INSERT INTO visual_asset_renditions VALUES (?,?)",("a1","thumbnail")); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_visual_asset_rendition_completeness_report_from_db(_db(),channel="newsletter"); assert r["artifact_type"]=="visual_asset_rendition_completeness"; assert r["findings"]; assert r["findings"][0]["missing_renditions"]==["newsletter_inline"]
    assert json.loads(format_visual_asset_rendition_completeness_json(r))["artifact_type"]=="visual_asset_rendition_completeness"; assert "Visual" in format_visual_asset_rendition_completeness_text(r)
    db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close(); assert script.main(["--db",str(db),"--format","text","--channel","newsletter"])==0; assert capsys.readouterr().out; assert script.main(["--db",str(db),"--limit","0"])==2
def test_missing_schema():
    assert build_visual_asset_rendition_completeness_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
