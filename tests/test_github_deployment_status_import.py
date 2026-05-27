from __future__ import annotations

import importlib.util, sqlite3
from pathlib import Path

import pytest

from ingestion.github_deployment_status_import import parse_github_deployment_statuses, upsert_github_deployment_statuses

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "import_github_deployment_statuses.py"
spec = importlib.util.spec_from_file_location("import_github_deployment_statuses_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_deployment_status_parse_sort_and_upsert_history():
    rows = parse_github_deployment_statuses(
        '{"repo":"OWNER/Repo","deployment_id":42,"environment":"prod","sha":"abc","statuses":[{"state":"success","created_at":"2026-05-27T02:00:00Z","target_url":"https://ci/2"},{"state":"pending","created_at":"2026-05-27T01:00:00Z","creator":{"login":"octo"}}]}'
    )
    assert [(r["repo"], r["deployment_id"], r["created_at"], r["status"]) for r in rows] == [
        ("owner/repo", "42", "2026-05-27T01:00:00Z", "pending"),
        ("owner/repo", "42", "2026-05-27T02:00:00Z", "success"),
    ]
    conn = sqlite3.connect(":memory:")
    upsert_github_deployment_statuses(conn, rows)
    upsert_github_deployment_statuses(conn, [{**rows[0], "target_url": "https://ci/updated"}])
    assert conn.execute("SELECT count(*) FROM github_deployment_statuses").fetchone()[0] == 2
    assert conn.execute("SELECT target_url FROM github_deployment_statuses WHERE status='pending'").fetchone()[0] == "https://ci/updated"


def test_deployment_status_csv_jsonl_and_required_fields(tmp_path, capsys):
    csv_rows = parse_github_deployment_statuses("repo,deployment_id,state,created_at\no/r,2,success,2026-05-27\n")
    jsonl_rows = parse_github_deployment_statuses('{"repo":"o/r","deployment_id":"1","status":"pending","created_at":"2026-05-26"}\n')
    assert [r["deployment_id"] for r in csv_rows + jsonl_rows] == ["2", "1"]
    with pytest.raises(ValueError, match="repo, deployment_id"):
        parse_github_deployment_statuses('{"repo":"o/r","deployment_id":"1"}')
    src = tmp_path / "statuses.json"
    src.write_text('[{"repo":"o/r","deployment_id":"1","status":"pending","created_at":"2026-05-26"}]')
    db = tmp_path / "db.sqlite"
    assert script.main(["--db", str(db), "--input", str(src), "--format", "text"]) == 0
    assert "GitHub Deployment Status Import" in capsys.readouterr().out
    bad = tmp_path / "bad.json"
    bad.write_text('{"repo":"o/r","deployment_id":"1"}')
    assert script.main(["--db", str(db), "--input", str(bad)]) == 1
