from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.linkedin_page_metric_import import parse_linkedin_page_metrics, upsert_linkedin_page_metrics
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "import_linkedin_page_metrics.py"; spec = importlib.util.spec_from_file_location("import_linkedin_page_metrics_script", SCRIPT); script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_linkedin_page_metric_import_cli(tmp_path):
    raw = '{"organizations":[{"organization_urn":"urn:li:organization:1","snapshot_at":"2026-05-01T00:00:00Z","followers":"10","impressions":"-4","clicks":"3","reactions":"2","comments":"1","shares":"5","source":"api"}]}'
    rows = parse_linkedin_page_metrics(raw)
    assert rows[0]["follower_count"] == 10 and rows[0]["impressions"] == 0
    c = sqlite3.connect(":memory:"); upsert_linkedin_page_metrics(c, rows); upsert_linkedin_page_metrics(c, [{**rows[0], "clicks": 99}])
    assert c.execute("SELECT clicks FROM linkedin_page_metrics").fetchone()[0] == 99
    p = tmp_path / "l.json"; p.write_text(raw); db = tmp_path / "db.sqlite"
    assert script.main(["--db", str(db), "--input", str(p), "--dry-run", "--format", "text"]) == 0
