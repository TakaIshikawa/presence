from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
import pytest
from ingestion.tiktok_video_metric_import import parse_tiktok_video_metrics, upsert_tiktok_video_metrics
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_tiktok_video_metrics.py"; spec=importlib.util.spec_from_file_location("import_tiktok_video_metrics_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_tiktok_video_metric_import_cli(tmp_path):
    rows=parse_tiktok_video_metrics('{"metrics":[{"account_handle":"@a","video_id":"v1","url":"HTTPS://TikTok.COM/@a/video/1","views":"10","plays":"11","comments":"2","shares":"3","saves":"4","collected_at":"t"}]}')
    assert rows[0]["account_handle"]=="a" and rows[0]["view_count"]==10 and rows[0]["comment_count"]==2
    assert parse_tiktok_video_metrics("video_id,captured_at,plays\nv2,t,5\n")[0]["view_count"]==5
    with pytest.raises(ValueError,match="video_id and captured_at"): parse_tiktok_video_metrics('{"video_id":"v"}')
    c=sqlite3.connect(":memory:"); upsert_tiktok_video_metrics(c,rows); upsert_tiktok_video_metrics(c,[{**rows[0],"like_count":9}]); assert c.execute("SELECT like_count FROM tiktok_video_metrics").fetchone()[0]==9
    p=tmp_path/"m.jsonl"; p.write_text('{"video_id":"v3","captured_at":"t"}\n'); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--dry-run","--format","text"])==0
