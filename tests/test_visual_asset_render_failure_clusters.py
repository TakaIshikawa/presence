from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from datetime import datetime, timezone
from evaluation.visual_asset_render_failure_clusters import build_visual_asset_render_failure_clusters_report_from_db, format_visual_asset_render_failure_clusters_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"visual_asset_render_failure_clusters.py"; spec=importlib.util.spec_from_file_location("visual_asset_render_failure_clusters_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_visual_failure_clusters_and_cli(tmp_path,capsys):
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE visual_assets (id TEXT, template TEXT, generator TEXT, dimensions TEXT, status TEXT, error_message TEXT, created_at TEXT)")
 c.execute("INSERT INTO visual_assets VALUES ('a1','hero','gen','1200x600','failed','Timeout id abcdef123456 at /tmp/a.png 2026-05-24T00:00:00Z','2026-05-24T00:00:00+00:00')")
 c.execute("INSERT INTO visual_assets VALUES ('a2','hero','gen','1200x600','failed','Timeout id deadbeef9999 at /tmp/b.png 2026-05-24T00:01:00Z','2026-05-24T00:00:00+00:00')")
 r=build_visual_asset_render_failure_clusters_report_from_db(c,min_cluster_size=2,now=datetime(2026,5,25,tzinfo=timezone.utc))
 assert r["findings"][0]["failure_count"]==2 and "<id>" in r["findings"][0]["error_signature"]
 assert "Failure Clusters" in format_visual_asset_render_failure_clusters_text(r)
 c.commit(); path=tmp_path/"db.sqlite"
 with sqlite3.connect(path) as out: c.backup(out)
 assert script.main(["--db",str(path),"--format","text","--min-cluster-size","2"])==0; assert "Failure Clusters" in capsys.readouterr().out
 assert script.main(["--db",str(path),"--min-cluster-size","0"])==2
def test_visual_schema_gap(): assert build_visual_asset_render_failure_clusters_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]==["visual_assets"]
