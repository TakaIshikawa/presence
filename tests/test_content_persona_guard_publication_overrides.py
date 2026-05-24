from datetime import datetime, timezone
import sqlite3

from evaluation.content_persona_guard_publication_overrides import build_content_persona_guard_publication_overrides_report, build_content_persona_guard_publication_overrides_report_from_db

NOW = datetime(2026, 5, 1, tzinfo=timezone.utc)


def test_persona_guard_publication_overrides():
    report = build_content_persona_guard_publication_overrides_report([
        {"content_id": 1, "guard_status": "failed", "publication_status": "published"},
        {"content_id": 2, "publication_status": "published"},
        {"content_id": 3, "guard_status": "failed", "publication_status": "queued"},
    ], now=NOW)
    assert report["artifact_type"] == "content_persona_guard_publication_overrides"
    assert {"failed_guard_published", "unchecked_guard_published", "failed_guard_queued"} <= {f["reason"] for f in report["findings"]}
    assert build_content_persona_guard_publication_overrides_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == ["content_persona_guard", "generated_content"]
