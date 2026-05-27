from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.content_claim_numeric_evidence_gaps import build_content_claim_numeric_evidence_gaps_report_from_db, format_content_claim_numeric_evidence_gaps_json, format_content_claim_numeric_evidence_gaps_text
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "content_claim_numeric_evidence_gaps.py"; spec = importlib.util.spec_from_file_location("script_content_claim_numeric_evidence_gaps", SCRIPT); script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c = sqlite3.connect(":memory:"); c.row_factory = sqlite3.Row
    c.executescript("CREATE TABLE generated_content (id TEXT, body TEXT, citations TEXT); CREATE TABLE claim_checks (content_id TEXT, evidence_text TEXT);")
    c.execute("INSERT INTO generated_content VALUES (?,?,?)", ("c1", "Revenue grew 42% to $1,200 in 2026 after 3 months, twice as fast as 2:1 benchmark.", ""))
    c.execute("INSERT INTO claim_checks VALUES (?,?)", ("c1", "Source confirms 42%"))
    c.commit(); return c
def test_numeric_evidence_report_and_cli(tmp_path, capsys):
    r = build_content_claim_numeric_evidence_gaps_report_from_db(_db())
    tokens = {f["numeric_token"] for f in r["numeric_evidence_gaps"]}
    assert r["artifact_type"] == "content_claim_numeric_evidence_gaps"
    assert "$1,200" in tokens and "2026" in tokens and "3 months" in tokens and "2:1" in tokens
    assert json.loads(format_content_claim_numeric_evidence_gaps_json(r))["artifact_type"] == r["artifact_type"]
    assert "Content Claim Numeric Evidence Gaps" in format_content_claim_numeric_evidence_gaps_text(r)
    db = tmp_path / "db.sqlite"; out = sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(["--db", str(db), "--format", "text"]) == 0 and capsys.readouterr().out
    assert script.main(["--db", str(db), "--limit", "0"]) == 2
def test_missing_schema():
    assert build_content_claim_numeric_evidence_gaps_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
