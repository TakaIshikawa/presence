from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.visual_asset_license_expiry_risk import build_visual_asset_license_expiry_risk_report


NOW = datetime(2026, 5, 20, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "visual_asset_license_expiry_risk.py"
spec = importlib.util.spec_from_file_location("visual_asset_license_expiry_risk_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_expired_before_publish_no_expiry_and_valid_cases(tmp_path, capsys):
    report = build_visual_asset_license_expiry_risk_report(
        [
            {"asset_id": "expired", "license_name": "Stock", "expires_at": "2026-05-01T00:00:00+00:00", "scheduled_publish_at": "2026-05-10T00:00:00+00:00", "affected_content_id": "c1"},
            {"asset_id": "before", "license_name": "Stock", "expires_at": "2026-05-25T00:00:00+00:00", "scheduled_publish_at": "2026-06-01T00:00:00+00:00", "affected_content_id": "c2"},
            {"asset_id": "missing", "license_name": "Unknown", "scheduled_publish_at": "2026-06-01T00:00:00+00:00", "affected_content_id": "c3"},
            {"asset_id": "valid", "license_name": "Owned", "expires_at": "2027-01-01T00:00:00+00:00", "scheduled_publish_at": "2026-06-01T00:00:00+00:00", "affected_content_id": "c4"},
        ],
        now=NOW,
    )
    assert {key for row in report["rows"] for key in row} >= {"asset_id", "license_name", "expires_at", "scheduled_publish_at", "affected_content_id", "days_until_expiry", "risk_level"}
    assert [row["risk_level"] for row in report["rows"]] == ["expired", "expires_before_publish", "unknown_expiry"]
    ignored = build_visual_asset_license_expiry_risk_report([{"asset_id": "missing"}], no_expiry_policy="ignore", now=NOW)
    assert ignored["rows"] == []

    db_path = tmp_path / "assets.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE visual_assets (id TEXT, license_name TEXT, expires_at TEXT, scheduled_publish_at TEXT, content_id TEXT)")
    conn.execute("INSERT INTO visual_assets VALUES ('a1','Stock','2026-05-01T00:00:00+00:00','2026-05-10T00:00:00+00:00','c1')")
    conn.commit()
    assert script.main(["--db", str(db_path), "--format", "json", "--now", NOW.isoformat()]) == 0
    assert json.loads(capsys.readouterr().out)["rows"][0]["asset_id"] == "a1"


def test_validation_errors():
    with pytest.raises(ValueError, match="no_expiry_policy"):
        build_visual_asset_license_expiry_risk_report([], no_expiry_policy="bad")
