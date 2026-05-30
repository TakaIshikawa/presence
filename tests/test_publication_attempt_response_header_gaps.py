from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.publication_attempt_response_header_gaps import build_publication_attempt_response_header_gaps_report_from_db, format_publication_attempt_response_header_gaps_json, format_publication_attempt_response_header_gaps_text
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_response_header_gaps.py"; spec = importlib.util.spec_from_file_location("script_publication_attempt_response_header_gaps", SCRIPT); script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def _db():
    c = sqlite3.connect(":memory:"); c.row_factory = sqlite3.Row
    c.executescript("CREATE TABLE publication_attempts (id TEXT, provider TEXT, platform TEXT, status_code INTEGER, failure_reason TEXT, response_headers TEXT, raw_response TEXT, raw_metadata TEXT);")
    c.execute("INSERT INTO publication_attempts VALUES (?,?,?,?,?,?,?,?)", ("a1", "ghost", "blog", 429, "rate limit", '{"content-type":"text/plain"}', None, None))
    c.execute("INSERT INTO publication_attempts VALUES (?,?,?,?,?,?,?,?)", ("a2", "x", "x", 500, "failed", "{bad", None, None))
    c.execute("INSERT INTO publication_attempts VALUES (?,?,?,?,?,?,?,?)", ("a3", "ok", "blog", 200, "", '{"x-request-id":"r1"}', None, None))
    c.commit(); return c

def test_response_header_gaps_report_and_cli(tmp_path, capsys):
    r = build_publication_attempt_response_header_gaps_report_from_db(_db())
    reasons = {f["reason"] for f in r["response_header_gaps"]}
    assert r["artifact_type"] == "publication_attempt_response_header_gaps"
    assert {"missing_request_id", "missing_retry_after", "missing_rate_limit_header", "invalid_header_json"} <= reasons
    assert r["summary"]["by_provider"]["ghost"]["missing_request_id"] == 1
    assert json.loads(format_publication_attempt_response_header_gaps_json(r))["artifact_type"] == r["artifact_type"]
    assert "Publication Attempt Response Header Gaps" in format_publication_attempt_response_header_gaps_text(r)
    db = tmp_path / "db.sqlite"; out = sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(["--db", str(db), "--format", "text"]) == 0 and capsys.readouterr().out
    assert script.main(["--db", str(db), "--limit", "0"]) == 2

def test_missing_schema():
    assert build_publication_attempt_response_header_gaps_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
