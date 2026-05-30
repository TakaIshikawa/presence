from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.newsletter_archive_metric_import import parse_newsletter_archive_metrics, upsert_newsletter_archive_metrics
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "import_newsletter_archive_metrics.py"; spec = importlib.util.spec_from_file_location("import_newsletter_archive_metrics_script", SCRIPT); script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_newsletter_archive_metric_import_cli(tmp_path):
    raw = '{"items":[{"issue_id":"i1","archive_url":"HTTPS://Example.COM/a?x=1#frag","metric_date":"2026-05-01","views":"10","unique_views":"7","shares":"2","referrals":"1","source":"beehiiv"}]}'
    rows = parse_newsletter_archive_metrics(raw)
    assert rows[0]["archive_url"] == "https://example.com/a?x=1"
    c = sqlite3.connect(":memory:"); upsert_newsletter_archive_metrics(c, rows); upsert_newsletter_archive_metrics(c, [{**rows[0], "views": 99}])
    assert c.execute("SELECT views FROM newsletter_archive_metrics").fetchone()[0] == 99
    p = tmp_path / "m.json"; p.write_text(raw); db = tmp_path / "db.sqlite"
    assert script.main(["--db", str(db), "--input", str(p), "--dry-run", "--format", "text"]) == 0
