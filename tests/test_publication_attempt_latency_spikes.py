from __future__ import annotations
import importlib.util,sqlite3
from datetime import datetime,timezone
from pathlib import Path
from evaluation.publication_attempt_latency_spikes import build_publication_attempt_latency_spikes_report
NOW=datetime(2026,6,1,tzinfo=timezone.utc)
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"publication_attempt_latency_spikes.py"; spec=importlib.util.spec_from_file_location("script_publication_attempt_latency_spikes",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_computes_duration_and_spike_ratio():
 rows=[{"id":"a","platform":"x","duration_ms":1000},{"id":"b","platform":"x","attempted_at":"2026-06-01T00:00:00+00:00","completed_at":"2026-06-01T00:00:05+00:00"}]
 r=build_publication_attempt_latency_spikes_report(rows,min_duration_ms=1000,now=NOW)
 assert r["spikes"][0]["attempt_id"]=="b" and r["spikes"][0]["baseline_ms"]
def test_platform_filter():
 assert build_publication_attempt_latency_spikes_report([{"id":"a","platform":"x","duration_ms":5000}],platform="bsky",now=NOW)["spikes"]==[]
def test_cli(tmp_path,capsys):
 db=tmp_path/"db.sqlite"; c=sqlite3.connect(db); c.execute("CREATE TABLE publication_attempts (id TEXT, platform TEXT, duration_ms INTEGER)"); c.executemany("INSERT INTO publication_attempts VALUES (?,?,?)",[("a","x",1000),("b","x",5000)]); c.commit(); c.close()
 assert script.main(["--db",str(db),"--format","text","--min-duration-ms","1000"])==0; assert capsys.readouterr().out
