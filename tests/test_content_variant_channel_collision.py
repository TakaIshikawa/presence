from __future__ import annotations
from datetime import datetime, timezone
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.content_variant_channel_collision import build_content_variant_channel_collision_report, build_content_variant_channel_collision_report_from_db, format_content_variant_channel_collision_json, format_content_variant_channel_collision_text
NOW=datetime(2026,5,24,12,tzinfo=timezone.utc)
SCRIPT_PATH=Path(__file__).resolve().parent.parent/"scripts"/"content_variant_channel_collision.py"; spec=importlib.util.spec_from_file_location("content_variant_channel_collision_script",SCRIPT_PATH); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_flags_exact_and_near_identical_across_platforms_only():
    rows=[{"variant_id":1,"content_id":"c1","platform":"x","variant_text":"Launch the guide today with practical retry tips."},{"variant_id":2,"content_id":"c1","platform":"linkedin","variant_text":"Launch the guide today with practical retry tips."},{"variant_id":3,"content_id":"c2","platform":"x","variant_text":"Retry guide has practical launch tips for teams today."},{"variant_id":4,"content_id":"c2","platform":"bluesky","variant_text":"Retry guide has practical launch tips for teams now."},{"variant_id":5,"content_id":"c2","platform":"x","variant_text":"A distinct short post for this channel."}]
    r=build_content_variant_channel_collision_report(rows,similarity_threshold=0.7,now=NOW)
    assert [(f["content_id"],f["collision_type"]) for f in r["findings"]]==[("c1","exact"),("c2","near_identical")]
    assert r["findings"][0]["similarity"]==1.0
    assert r["findings"][0]["recommendation"]
def test_db_formatters_cli_and_missing_schema(tmp_path,capsys):
    conn=sqlite3.connect(":memory:"); conn.execute("CREATE TABLE content_variants (id INTEGER, content_id TEXT, platform TEXT, content TEXT)")
    conn.executemany("INSERT INTO content_variants VALUES (?,?,?,?)",[(1,"c1","x","Same text for every channel."),(2,"c1","linkedin","Same text for every channel.")])
    r=build_content_variant_channel_collision_report_from_db(conn,now=NOW)
    assert r["findings"][0]["left_variant_id"]==1
    assert json.loads(format_content_variant_channel_collision_json(r))["artifact_type"]=="content_variant_channel_collision"
    assert "Content Variant Channel Collision" in format_content_variant_channel_collision_text(r)
    db=tmp_path/"variants.sqlite"; conn.commit(); out=sqlite3.connect(db); conn.backup(out); out.close()
    assert script.main(["--db",str(db),"--similarity-threshold","0.9"])==0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"]==1
    assert build_content_variant_channel_collision_report_from_db(sqlite3.connect(":memory:"),now=NOW)["missing_tables"]==["content_variants"]
