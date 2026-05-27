from __future__ import annotations

import importlib.util, sqlite3
from pathlib import Path

import pytest

from ingestion.github_release_asset_import import parse_github_release_assets, upsert_github_release_assets

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "import_github_release_assets.py"
spec = importlib.util.spec_from_file_location("import_github_release_assets_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_release_asset_insert_update_and_preserve_metadata():
    rows = parse_github_release_assets(
        '{"repo_name":"OWNER/Repo","release_tag":"v1","assets":[{"name":"app.zip","download_count":7,"size":11,"content_type":"application/zip","browser_download_url":"https://github.com/o/r/releases/download/v1/app.zip","updated_at":"2026-05-26T00:00:00Z"}],"captured_at":"2026-05-27T00:00:00Z"}'
    )
    assert rows[0]["repo_name"] == "owner/repo"
    conn = sqlite3.connect(":memory:")
    assert upsert_github_release_assets(conn, rows)["upserted_count"] == 1
    upsert_github_release_assets(conn, [{**rows[0], "download_count":9, "content_type":"application/octet-stream"}])
    stored = conn.execute("SELECT download_count, content_type, browser_download_url FROM github_release_assets").fetchone()
    assert stored == (9, "application/octet-stream", "https://github.com/o/r/releases/download/v1/app.zip")
    assert conn.execute("SELECT count(*) FROM github_release_assets").fetchone()[0] == 1


def test_release_asset_rejects_negative_metrics():
    with pytest.raises(ValueError, match="download_count"):
        parse_github_release_assets('{"repo_name":"o/r","release_tag":"v1","asset_name":"a","download_count":-1,"size_bytes":0,"captured_at":"now"}')
    with pytest.raises(ValueError, match="size_bytes"):
        parse_github_release_assets('{"repo_name":"o/r","release_tag":"v1","asset_name":"a","download_count":0,"size_bytes":-1,"captured_at":"now"}')


def test_release_asset_csv_jsonl_and_cli_summary(tmp_path, capsys):
    csv_rows = parse_github_release_assets("repo_name,release_tag,asset_name,download_count,size_bytes,captured_at\no/r,v1,a,1,2,2026-05-27\n")
    jsonl_rows = parse_github_release_assets('{"repo_name":"o/r","release_tag":"v1","asset_name":"b","download_count":0,"size_bytes":3,"captured_at":"2026-05-27"}\n')
    assert [r["asset_name"] for r in csv_rows + jsonl_rows] == ["a", "b"]
    src = tmp_path / "assets.json"
    src.write_text('[{"repo_name":"o/r","release_tag":"v1","asset_name":"a","download_count":1,"size_bytes":2,"captured_at":"2026-05-27"}]')
    db = tmp_path / "db.sqlite"
    assert script.main(["--db", str(db), "--input", str(src), "--format", "text"]) == 0
    assert "GitHub Release Asset Import" in capsys.readouterr().out
    bad = tmp_path / "bad.json"
    bad.write_text('{"repo_name":"o/r","release_tag":"v1","asset_name":"a","download_count":-1,"size_bytes":2,"captured_at":"2026-05-27"}')
    assert script.main(["--db", str(db), "--input", str(bad)]) == 1
