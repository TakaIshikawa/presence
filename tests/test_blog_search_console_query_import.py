from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.blog_search_console_query_import import parse_blog_search_console_queries, upsert_blog_search_console_queries
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "import_blog_search_console_queries.py"; spec = importlib.util.spec_from_file_location("import_blog_search_console_queries_script", SCRIPT); script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_search_console_import_cli(tmp_path):
    raw = '{"queries":[{"url":"https://example.com/a?x=1#f","query":"codex","date":"2026-05-01","clicks":"2","impressions":"10","ctr":"20%","position":"4.5","country":"US","device":"DESKTOP"}]}'
    rows = parse_blog_search_console_queries(raw)
    assert rows[0]["canonical_path"] == "/a" and rows[0]["ctr"] == 0.2
    c = sqlite3.connect(":memory:"); upsert_blog_search_console_queries(c, rows); upsert_blog_search_console_queries(c, [{**rows[0], "clicks": 9}])
    assert c.execute("SELECT clicks FROM blog_search_console_queries").fetchone()[0] == 9
    p = tmp_path / "q.json"; p.write_text(raw); db = tmp_path / "db.sqlite"
    assert script.main(["--db", str(db), "--input", str(p), "--dry-run", "--format", "text"]) == 0
