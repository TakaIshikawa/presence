from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path

import pytest

from ingestion.instagram_post_insight_import import parse_instagram_post_insights, upsert_instagram_post_insights


SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "import_instagram_post_insights.py"
spec = importlib.util.spec_from_file_location("import_instagram_post_insights_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_parse_csv_json_array_object_and_jsonl():
    csv_rows = parse_instagram_post_insights(
        "media_id,captured_at,account_handle,impressions,reach,likes,comments,saves,shares\n"
        "m1,2026-05-01T00:00:00Z,@acct,\"1,200\",0,,2,3,4\n"
    )
    assert csv_rows[0]["account_handle"] == "acct"
    assert csv_rows[0]["impressions"] == 1200
    assert csv_rows[0]["reach"] == 0
    assert csv_rows[0]["likes"] == 0

    array_rows = parse_instagram_post_insights(
        '[{"media_id":"m2","captured_at":"t","insights":{"impressions":"5","likes":"0"}}]'
    )
    assert array_rows[0]["impressions"] == 5
    assert array_rows[0]["likes"] == 0

    object_rows = parse_instagram_post_insights(
        '{"insights":[{"id":"m3","snapshot_at":"t","impression_count":"6","comment_count":"1"}]}'
    )
    assert object_rows[0]["media_id"] == "m3"
    assert object_rows[0]["comments"] == 1

    jsonl_rows = parse_instagram_post_insights('{"media_id":"m4","captured_at":"t","shares":"2"}\n')
    assert jsonl_rows[0]["shares"] == 2


def test_upsert_refreshes_existing_snapshot_and_cli(tmp_path, capsys):
    rows = parse_instagram_post_insights(
        '{"insights":[{"account_handle":"@a","media_id":"m1","permalink":"https://instagram.com/p/1","media_type":"IMAGE",'
        '"posted_at":"2026-05-01T00:00:00Z","impressions":"10","reach":"9","likes":"1","comments":"0","saves":"0","shares":"0","captured_at":"t"}]}'
    )
    conn = sqlite3.connect(":memory:")
    upsert_instagram_post_insights(conn, rows)
    upsert_instagram_post_insights(conn, [{**rows[0], "likes": 7}])
    assert conn.execute("SELECT likes FROM instagram_post_insights WHERE media_id='m1'").fetchone()[0] == 7

    path = tmp_path / "ig.jsonl"
    path.write_text('{"media_id":"m2","captured_at":"t","impressions":"4"}\n', encoding="utf-8")
    db = tmp_path / "db.sqlite"
    assert script.main(["--db", str(db), "--input", str(path), "--dry-run", "--format", "json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["artifact_type"] == "instagram_post_insight_import"
    assert out["parsed_count"] == 1 and out["dry_run"] is True


def test_missing_required_fields_error():
    with pytest.raises(ValueError, match="media_id and captured_at"):
        parse_instagram_post_insights('{"media_id":"m1"}')
