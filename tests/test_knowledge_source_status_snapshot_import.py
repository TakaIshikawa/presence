from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from knowledge.source_status_snapshot_import import parse_source_status_snapshots, build_source_status_snapshot_import_preview, import_source_status_snapshots
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_knowledge_source_status_snapshots.py"; spec=importlib.util.spec_from_file_location("import_knowledge_source_status_snapshots_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_source_status_import_json_csv_dedupe_dry_run_and_cli(tmp_path,capsys):
 p=tmp_path/"in.json"; p.write_text(json.dumps([{"source_url":"HTTPS://Example.com/a","status_code":200,"content_hash":"h","checked_at":"2026-05-24T00:00:00+00:00","title":"A","canonical_url":"https://example.com/a"},{"source_url":"https://example.com/a","status_code":200,"checked_at":"2026-05-24T00:00:00+00:00"},{"source_url":"bad","status_code":"x","checked_at":"bad"}]))
 rows=parse_source_status_snapshots(p); preview=build_source_status_snapshot_import_preview(rows)
 assert preview["summary"]=={"input_count":3,"valid_count":1,"invalid_count":1,"duplicate_count":1}
 db=sqlite3.connect(":memory:"); assert import_source_status_snapshots(db,rows,dry_run=True)["imported_count"]==0
 assert import_source_status_snapshots(db,rows)["imported_count"]==1
 csvp=tmp_path/"in.csv"; csvp.write_text("source_url,status_code,checked_at\nhttps://b.example,404,2026-05-24T00:00:00+00:00\n")
 assert parse_source_status_snapshots(csvp)[0]["source_url"]=="https://b.example/"
 path=tmp_path/"db.sqlite"; assert script.main(["--db",str(path),"--input",str(p),"--format","text","--dry-run"])==0
 assert "Status Snapshot Import" in capsys.readouterr().out
 assert script.main(["--db",str(path),"--input",str(p),"--strict"])==1
def test_source_status_cli_validation(tmp_path):
 assert script.main(["--db",str(tmp_path/"x.sqlite")])==2
