from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
import pytest
from ingestion.youtube_comment_snapshot_import import parse_youtube_comment_snapshots, upsert_youtube_comment_snapshots
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_youtube_comment_snapshots.py"; spec=importlib.util.spec_from_file_location("import_youtube_comment_snapshots_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_youtube_comment_snapshot_import_cli(tmp_path,capsys):
    rows=parse_youtube_comment_snapshots('{"comments":[{"videoId":"v1","commentId":"c1","authorDisplayName":"Alice","textDisplay":"Nice","publishedAt":"2026-05-01T00:00:00Z","likeCount":"3","replyCount":"2","fetched_at":"2026-05-02T00:00:00Z"}]}')
    assert rows[0]["video_id"]=="v1" and rows[0]["comment_id"]=="c1"; assert rows[0]["author"]=="Alice"; assert rows[0]["text"]=="Nice"; assert rows[0]["like_count"]==3 and rows[0]["reply_count"]==2
    assert parse_youtube_comment_snapshots('[{"video_id":"v2","comment_id":"c2","body":"Body"}]')[0]["text"]=="Body"
    assert parse_youtube_comment_snapshots("video_id,comment_id,likes,replies\nv3,c3,5,4\n")[0]["like_count"]==5
    assert parse_youtube_comment_snapshots('{"video_id":"v4","id":"c4","snapshot_at":"t"}')[0]["imported_at"]=="t"
    with pytest.raises(ValueError,match="video_id and comment_id"): parse_youtube_comment_snapshots('{"video_id":"v"}')
    with pytest.raises(ValueError,match="video_id and comment_id"): parse_youtube_comment_snapshots('{"comment_id":"c"}')
    c=sqlite3.connect(":memory:"); upsert_youtube_comment_snapshots(c,rows); upsert_youtube_comment_snapshots(c,[{**rows[0],"like_count":9,"reply_count":7}]); assert c.execute("SELECT like_count, reply_count FROM youtube_comment_snapshots").fetchone()==(9,7)
    p=tmp_path/"comments.jsonl"; p.write_text('{"video_id":"v5","comment_id":"c5","likes":1}\n'); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--dry-run","--format","json"])==0
    out=json.loads(capsys.readouterr().out); assert out["parsed_count"]==1 and out["upserted_count"]==0 and out["dry_run"] is True
