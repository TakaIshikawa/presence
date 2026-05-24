from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
import pytest
from knowledge.source_redirect_snapshot_import import import_knowledge_source_redirect_snapshots, load_redirect_snapshot_rows
SCRIPT_PATH=Path(__file__).resolve().parent.parent/'scripts'/'import_knowledge_source_redirect_snapshots.py'
spec=importlib.util.spec_from_file_location('import_knowledge_source_redirect_snapshots_script', SCRIPT_PATH); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_import_json_jsonl_dedupe_and_cli(tmp_path, capsys):
 arr=tmp_path/'in.json'; arr.write_text(json.dumps([{'source_id':'1','url':'https://a','final_url':'https://b','status_code':200,'redirect_count':1,'checked_at':'2026-05-24T00:00:00+00:00'}]))
 assert load_redirect_snapshot_rows(arr)[0]['source_id']=='1'
 db=sqlite3.connect(':memory:'); s=import_knowledge_source_redirect_snapshots(db, load_redirect_snapshot_rows(arr)); assert s['imported']==1
 s=import_knowledge_source_redirect_snapshots(db, load_redirect_snapshot_rows(arr)); assert s['skipped']==1
 jl=tmp_path/'in.jsonl'; jl.write_text('{"url":"https://c","redirect_count":3,"checked_at":"x"}\n')
