"""Report visual asset coverage and reuse balance across output."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_REUSE_WINDOW_DAYS = 14
DEFAULT_LIMIT = 25


@dataclass(frozen=True)
class VisualAssetUsageFinding:
    finding_type: str
    label: str
    channel: str | None
    asset_id: str | None
    usage_count: int
    share: float
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class VisualAssetUsageBalanceReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    findings: tuple[VisualAssetUsageFinding, ...]
    schema_warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "visual_asset_usage_balance",
            "filters": dict(self.filters),
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "schema_warnings": list(self.schema_warnings),
            "totals": dict(sorted(self.totals.items())),
        }


def build_visual_asset_usage_balance_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    reuse_window_days: int = DEFAULT_REUSE_WINDOW_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> VisualAssetUsageBalanceReport:
    if days <= 0:
        raise ValueError("days must be positive")
    if reuse_window_days <= 0:
        raise ValueError("reuse_window_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"days": days, "reuse_window_days": reuse_window_days, "limit": limit, "cutoff": cutoff.isoformat()}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    warnings = _schema_warnings(schema)
    if "generated_content" not in schema:
        return _report(generated_at, filters, (), warnings, content_count=0)
    rows = _content_rows(conn, schema, cutoff)
    findings = _findings(rows, reuse_window_days, generated_at)[:limit]
    return _report(generated_at, filters, tuple(findings), warnings, content_count=len(rows))


def format_visual_asset_usage_balance_json(report: VisualAssetUsageBalanceReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_visual_asset_usage_balance_text(report: VisualAssetUsageBalanceReport) -> str:
    lines = [
        "Visual Asset Usage Balance",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['days']} days",
        f"Reuse window: {report.filters['reuse_window_days']} days",
        f"Totals: content={report.totals['content_count']} findings={report.totals['finding_count']}",
    ]
    if report.schema_warnings:
        lines.append("Schema warnings: " + "; ".join(report.schema_warnings))
    if not report.findings:
        lines.append("No visual asset usage balance issues found.")
        return "\n".join(lines)
    lines.append("")
    lines.append("Findings:")
    for finding in report.findings:
        lines.append(
            f"- type={finding.finding_type} label={finding.label} channel={finding.channel or '-'} "
            f"asset={finding.asset_id or '-'} count={finding.usage_count} share={finding.share:.2f} "
            f"action={finding.recommended_action}"
        )
    return "\n".join(lines)


def _content_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], cutoff: datetime) -> list[dict[str, Any]]:
    gc = schema["generated_content"]
    created_at = _column_expr(gc, "created_at", "NULL", "gc")
    rows = [
        dict(row)
        for row in conn.execute(
            f"""SELECT gc.id,
                      {_column_expr(gc, 'content_type', "'unknown'", 'gc')} AS content_type,
                      {_column_expr(gc, 'image_path', 'NULL', 'gc')} AS image_path,
                      {_column_expr(gc, 'image_prompt', 'NULL', 'gc')} AS image_prompt,
                      {created_at} AS created_at
               FROM generated_content gc
               WHERE {created_at} IS NULL OR datetime({created_at}) >= datetime(?)
               ORDER BY {created_at} DESC, gc.id DESC""",
            (cutoff.isoformat(),),
        )
    ]
    publications = _publication_channels(conn, schema)
    for row in rows:
        row["channel"] = publications.get(int(row["id"]), _channel(row.get("content_type")))
        row["asset_id"] = row.get("image_path") or row.get("image_prompt")
    return rows


def _publication_channels(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[int, str]:
    if "content_publications" not in schema or not {"content_id", "platform"}.issubset(schema["content_publications"]):
        return {}
    return {
        int(row["content_id"]): _channel(row["platform"])
        for row in conn.execute("SELECT content_id, platform FROM content_publications ORDER BY updated_at DESC, id DESC")
    }


def _findings(rows: list[dict[str, Any]], reuse_window_days: int, now: datetime) -> list[VisualAssetUsageFinding]:
    total = len(rows) or 1
    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    channel_counts = Counter(str(row["channel"]) for row in rows)
    channel_visuals = Counter(str(row["channel"]) for row in rows if row.get("asset_id"))
    findings: list[VisualAssetUsageFinding] = []
    missing = [row for row in rows if _expects_visual(row) and not row.get("asset_id")]
    if missing:
        findings.append(VisualAssetUsageFinding("missing_asset_identifier", "content without asset id", None, None, len(missing), round(len(missing) / total, 4), "attach or record visual asset id"))
    for row in rows:
        if row.get("asset_id"):
            by_asset[str(row["asset_id"])].append(row)
    for asset_id, asset_rows in by_asset.items():
        if len(asset_rows) >= 3:
            findings.append(VisualAssetUsageFinding("over_reused_asset", asset_id, None, asset_id, len(asset_rows), round(len(asset_rows) / total, 4), "rotate in fresh visual assets"))
        dates = sorted((_parse_datetime(row.get("created_at")) for row in asset_rows), key=lambda value: value or now)
        for previous, current in zip(dates, dates[1:]):
            if previous and current and (current - previous).days <= reuse_window_days:
                findings.append(VisualAssetUsageFinding("cooldown_reuse", asset_id, None, asset_id, len(asset_rows), round(len(asset_rows) / total, 4), "delay reuse until cooldown window expires"))
                break
    for channel, count in channel_counts.items():
        coverage = channel_visuals[channel] / count if count else 0
        if count >= 2 and coverage < 0.5:
            findings.append(VisualAssetUsageFinding("low_visual_coverage", channel, channel, None, count - channel_visuals[channel], round(coverage, 4), "add visuals to more posts in this channel"))
    return sorted(findings, key=lambda item: (_risk_rank(item.finding_type), -item.usage_count, item.label))


def _expects_visual(row: dict[str, Any]) -> bool:
    return str(row.get("content_type") or "") in {"x_visual", "newsletter", "blog_post", "blog"} or str(row.get("channel")) in {"newsletter", "blog"}


def _channel(value: Any) -> str:
    text = str(value or "unknown").lower()
    return {"blog_post": "blog", "x": "x_post", "twitter": "x_post"}.get(text, text)


def _risk_rank(kind: str) -> int:
    return {"missing_asset_identifier": 0, "cooldown_reuse": 1, "over_reused_asset": 2, "low_visual_coverage": 3}.get(kind, 9)


def _report(generated_at: datetime, filters: dict[str, Any], findings: tuple[VisualAssetUsageFinding, ...], warnings: tuple[str, ...], *, content_count: int) -> VisualAssetUsageBalanceReport:
    return VisualAssetUsageBalanceReport(generated_at.isoformat(), filters, {"content_count": content_count, "finding_count": len(findings)}, findings, warnings)


def _schema_warnings(schema: dict[str, set[str]]) -> tuple[str, ...]:
    if "generated_content" not in schema:
        return ("missing table: generated_content",)
    if not {"id", "content_type"}.issubset(schema["generated_content"]):
        return ("missing columns: generated_content(id, content_type)",)
    return ()


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _column_expr(columns: set[str], column: str, fallback: str, alias: str) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
