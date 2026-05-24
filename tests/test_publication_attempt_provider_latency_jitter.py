from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from evaluation.publication_attempt_provider_latency_jitter import build_publication_attempt_provider_latency_jitter_report_from_db, format_publication_attempt_provider_latency_jitter_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"publication_attempt_provider_latency_jitter.py"; spec=importlib.util.spec_from_file_location("publication_attempt_provider_latency_jitter_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_latency_jitter_and_cli(tmp_path,capsys):
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE publication_attempts (platform TEXT, provider TEXT, attempted_at TEXT, latency_ms REAL)")
    for v in (100,110,900): c.execute("INSERT INTO publication_attempts VALUES ('x','api','2026-05-24T00:00:00+00:00',?)",(v,))
    r=build_publication_attempt_provider_latency_jitter_report_from_db(c, jitter_threshold=2, now=__import__("datetime").datetime(2026,5,25,tzinfo=__import__("datetime").timezone.utc))
    assert r["findings"][0]["jitter_ratio"] >= 2
    assert "Latency Jitter" in format_publication_attempt_provider_latency_jitter_text(r)
    c.commit(); path=tmp_path/"db.sqlite"
    with sqlite3.connect(path) as out: c.backup(out)
    assert script.main(["--db",str(path),"--format","text","--jitter-threshold","2"])==0
    assert "Latency Jitter" in capsys.readouterr().out
    assert script.main(["--db",str(path),"--lookback-days","0"])==2
def test_latency_schema_gap():
    r=build_publication_attempt_provider_latency_jitter_report_from_db(sqlite3.connect(":memory:"))
    assert r["missing_tables"]==["publication_attempts"]
