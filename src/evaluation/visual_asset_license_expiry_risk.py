"""Report visual assets whose license windows risk scheduled publication."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


ARTIFACT_TYPE = "visual_asset_license_expiry_risk"


def build_visual_asset_license_expiry_risk_report(
    assets: list[dict[str, Any]],
    *,
    no_expiry_policy: str = "warn",
    now: datetime | None = None,
) -> dict[str, Any]:
    if no_expiry_policy not in {"warn", "ignore"}:
        raise ValueError("no_expiry_policy must be warn or ignore")
    generated_at = _utc(now or datetime.now(timezone.utc))
    rows = []
    for asset in assets:
        expires_at = _dt(asset.get("expires_at") or asset.get("license_expires_at"))
        scheduled = _dt(asset.get("scheduled_publish_at") or asset.get("publish_at"))
        if not expires_at:
            if no_expiry_policy == "ignore":
                continue
            days = None
            risk = "unknown_expiry"
        else:
            anchor = scheduled or generated_at
            days = (expires_at.date() - anchor.date()).days
            if expires_at < generated_at:
                risk = "expired"
            elif scheduled and expires_at < scheduled:
                risk = "expires_before_publish"
            else:
                continue
        rows.append(
            {
                "asset_id": asset.get("asset_id") or asset.get("id"),
                "license_name": asset.get("license_name") or asset.get("license"),
                "expires_at": expires_at.isoformat() if expires_at else None,
                "scheduled_publish_at": scheduled.isoformat() if scheduled else None,
                "affected_content_id": asset.get("affected_content_id") or asset.get("content_id"),
                "days_until_expiry": days,
                "risk_level": risk,
            }
        )
    rows.sort(key=lambda row: ({"expired": 0, "expires_before_publish": 1, "unknown_expiry": 2}[row["risk_level"]], row["days_until_expiry"] if row["days_until_expiry"] is not None else 999999, str(row["asset_id"])))
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": generated_at.isoformat(), "filters": {"no_expiry_policy": no_expiry_policy}, "totals": {"asset_count": len(assets), "row_count": len(rows)}, "rows": rows}


def build_visual_asset_license_expiry_risk_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    table = "visual_assets" if "visual_assets" in schema else "assets" if "assets" in schema else None
    if table is None:
        return build_visual_asset_license_expiry_risk_report([], **kwargs) | {"missing_tables": ["visual_assets"]}
    cols = schema[table]
    rows = [dict(row) for row in conn.execute(f"SELECT {_expr(cols, ('id','asset_id'), 'rowid')} AS asset_id, {_expr(cols, ('license_name','license'), 'NULL')} AS license_name, {_expr(cols, ('expires_at','license_expires_at'), 'NULL')} AS expires_at, {_expr(cols, ('scheduled_publish_at','publish_at'), 'NULL')} AS scheduled_publish_at, {_expr(cols, ('content_id','affected_content_id'), 'NULL')} AS affected_content_id FROM {table} ORDER BY rowid")]
    return build_visual_asset_license_expiry_risk_report(rows, **kwargs) | {"missing_tables": []}


def format_visual_asset_license_expiry_risk_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_visual_asset_license_expiry_risk_text(report: dict[str, Any]) -> str:
    lines = ["Visual Asset License Expiry Risk", f"Generated: {report['generated_at']}", f"Rows: {report['totals']['row_count']}"]
    lines.extend(f"{row['asset_id']} | {row['license_name']} | {row['risk_level']} | {row['days_until_expiry']}" for row in report["rows"])
    return "\n".join(lines)


def _dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    if isinstance(db_or_conn, sqlite3.Connection):
        db_or_conn.row_factory = sqlite3.Row
        return db_or_conn
    conn = sqlite3.connect(db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _expr(columns: set[str], names: tuple[str, ...], fallback: str) -> str:
    return next((name for name in names if name in columns), fallback)
