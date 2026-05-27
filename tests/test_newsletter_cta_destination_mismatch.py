from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.newsletter_cta_destination_mismatch import build_newsletter_cta_destination_mismatch_report


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_cta_destination_mismatch.py"
spec = importlib.util.spec_from_file_location("newsletter_cta_destination_mismatch_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_matching_mismatched_and_unknown_ctas(tmp_path, capsys):
    report = build_newsletter_cta_destination_mismatch_report(
        [
            {"issue_id": "i1", "cta_text": "Subscribe now", "destination_url": "https://news.test/subscribe"},
            {"issue_id": "i1", "cta_text": "Subscribe today", "destination_url": "https://news.test/archive"},
            {"issue_id": "i2", "cta_text": "Reply with your question", "destination_url": "https://news.test/"},
            {"issue_id": "i3", "cta_text": "Sponsor offer", "destination_url": "https://news.test/deal", "sponsor_domain": "sponsor.test"},
            {"issue_id": "i4", "cta_text": "Mystery link", "destination_url": "https://news.test/"},
        ]
    )
    assert {key for row in report["rows"] for key in row} >= {"issue_id", "cta_text", "destination_url", "expected_destination_kind", "observed_destination_kind", "severity"}
    assert [(row["expected_destination_kind"], row["observed_destination_kind"]) for row in report["rows"]] == [("subscribe", "read_more"), ("reply", "home"), ("sponsor", "home")]
    assert report["known_cta_kinds"] == ["subscribe", "reply", "read_more", "sponsor", "feedback"]

    db_path = tmp_path / "ctas.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE newsletter_ctas (issue_id TEXT, cta_text TEXT, destination_url TEXT)")
    conn.execute("INSERT INTO newsletter_ctas VALUES ('i5','Read more','https://news.test/subscribe')")
    conn.commit()
    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["rows"][0]["issue_id"] == "i5"
