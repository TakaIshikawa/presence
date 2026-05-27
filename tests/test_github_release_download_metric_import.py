from __future__ import annotations

import importlib.util, json, sqlite3
from pathlib import Path

from ingestion.github_release_download_metric_import import parse_github_release_download_metrics, upsert_github_release_download_metrics

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "import_github_release_download_metrics.py"
spec = importlib.util.spec_from_file_location("import_github_release_download_metrics_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)

def test_parses_github_api_release_assets_and_upserts():
    rows = parse_github_release_download_metrics(json.dumps({"repository": "OWNER/Repo", "tag_name": "v1", "fetched_at": "2026-01-01T00:00:00Z", "assets": [{"name": "app.zip", "download_count": "12", "size": "34", "content_type": "application/zip", "browser_download_url": "https://example.test/app.zip"}]}))
    assert rows == [{"repository": "owner/repo", "release_tag": "v1", "asset_name": "app.zip", "snapshot_at": "2026-01-01T00:00:00Z", "download_count": 12, "asset_size_bytes": 34, "content_type": "application/zip", "browser_download_url": "https://example.test/app.zip", "fetched_at": "2026-01-01T00:00:00Z"}]
    conn = sqlite3.connect(":memory:")
    upsert_github_release_download_metrics(conn, rows)
    upsert_github_release_download_metrics(conn, [{**rows[0], "download_count": 15}])
    assert conn.execute("SELECT COUNT(*), download_count FROM github_release_download_metrics").fetchone() == (1, 15)

def test_parses_csv_jsonl_and_cli(tmp_path):
    csv_rows = parse_github_release_download_metrics("repository,release_tag,asset_name,snapshot_at,download_count,asset_size_bytes\nOwner/Repo,v2,bin.tgz,2026-01-02,7,100\n")
    jsonl_rows = parse_github_release_download_metrics('{"repository":"https://github.com/Owner/Repo","release_tag":"v3","asset_name":"bin.tgz","snapshot_at":"2026-01-03","downloads":"8","size":"101"}\n')
    assert csv_rows[0]["repository"] == jsonl_rows[0]["repository"] == "owner/repo"
    assert jsonl_rows[0]["download_count"] == 8 and jsonl_rows[0]["asset_size_bytes"] == 101
    path = tmp_path / "metrics.jsonl"
    path.write_text('{"repository":"x/y","release_tag":"v1","asset_name":"a","snapshot_at":"now"}')
    db = tmp_path / "db.sqlite"
    assert script.main(["--db", str(db), "--input", str(path), "--format", "text"]) == 0
    bad = tmp_path / "bad.json"
    bad.write_text('{"repository":"x/y"}')
    assert script.main(["--db", str(db), "--input", str(bad)]) == 1
