"""Report drift between legacy and per-platform publication state."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
PUBLISHED_STATUS = "published"


def build_content_publication_state_drift_report(
    generated_rows: list[dict[str, Any]],
    publication_rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic publication-state drift report from loaded rows."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    publications_by_content: dict[int, list[dict[str, Any]]] = {}
    for row in publication_rows:
        content_id = _int_or_none(row.get("content_id"))
        if content_id is None:
            continue
        publications_by_content.setdefault(content_id, []).append(row)

    drift_items: list[dict[str, Any]] = []
    for row in generated_rows:
        content_id = _int_or_none(row.get("content_id") or row.get("id"))
        if content_id is None:
            continue
        legacy_published = _bool(row.get("published"))
        platform = _legacy_platform(row)
        publications = publications_by_content.get(content_id, [])
        platform_publications = [pub for pub in publications if _platform(pub) == platform]
        if legacy_published and not platform_publications:
            drift_items.append(_item(row, None, platform, "legacy_published_missing_platform_row"))
        for publication in platform_publications:
            drift_items.extend(_legacy_publication_issues(row, publication))

    generated_by_id = {
        content_id: row
        for row in generated_rows
        if (content_id := _int_or_none(row.get("content_id") or row.get("id"))) is not None
    }
    for publication in publication_rows:
        if _status(publication) != PUBLISHED_STATUS:
            continue
        content_id = _int_or_none(publication.get("content_id"))
        row = generated_by_id.get(content_id or -1, {})
        if not _clean(publication.get("platform_post_id")):
            drift_items.append(_item(row, publication, _platform(publication), "published_missing_platform_post_id"))
        if not _clean(publication.get("platform_url")):
            drift_items.append(_item(row, publication, _platform(publication), "published_missing_platform_url"))

    drift_items.sort(key=_sort_key)
    shown = drift_items[:limit]
    return {
        "artifact_type": "content_publication_state_drift",
        "generated_at": generated_at.isoformat(),
        "thresholds": {"limit": limit},
        "summary": {
            "generated_content_count": len(generated_rows),
            "content_publication_count": len(publication_rows),
            "drift_count": len(drift_items),
            "shown_count": len(shown),
            "by_issue_type": dict(sorted(Counter(item["issue_type"] for item in drift_items).items())),
        },
        "drift_items": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items())},
    }


def build_content_publication_state_drift_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [table for table in ("generated_content", "content_publications") if table not in schema]
    missing_columns: dict[str, list[str]] = {}
    generated_rows: list[dict[str, Any]] = []
    publication_rows: list[dict[str, Any]] = []

    if not missing_tables:
        if "id" not in schema["generated_content"]:
            missing_columns["generated_content"] = ["id"]
        if "content_id" not in schema["content_publications"]:
            missing_columns["content_publications"] = ["content_id"]
        if not missing_columns:
            generated_rows = _load_generated_content(conn, schema)
            publication_rows = _load_content_publications(conn, schema)

    return build_content_publication_state_drift_report(
        generated_rows,
        publication_rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_content_publication_state_drift_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_publication_state_drift_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Content Publication State Drift",
        f"Generated: {report['generated_at']}",
        f"Limit: {report['thresholds']['limit']}",
        (
            "Totals: "
            f"generated={summary['generated_content_count']} "
            f"publications={summary['content_publication_count']} "
            f"drift={summary['drift_count']} shown={summary['shown_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append(
            "Missing columns: "
            + "; ".join(f"{table}({', '.join(columns)})" for table, columns in report["missing_columns"].items())
        )
    if not report["drift_items"]:
        lines.append("No content publication state drift found.")
        return "\n".join(lines)

    lines.extend(["", "content_id | platform | issue_type | publication_id | legacy_tweet_id | platform_post_id | legacy_url | platform_url"])
    for item in report["drift_items"]:
        lines.append(
            f"{item['content_id'] or '-'} | {item['platform']} | {item['issue_type']} | "
            f"{item['publication_id'] or '-'} | {item['legacy_tweet_id'] or '-'} | "
            f"{item['platform_post_id'] or '-'} | {item['legacy_published_url'] or '-'} | {item['platform_url'] or '-'}"
        )
    return "\n".join(lines)


def _legacy_publication_issues(row: dict[str, Any], publication: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    legacy_ts = _parse_dt(row.get("published_at"))
    platform_ts = _parse_dt(publication.get("published_at"))
    if legacy_ts and platform_ts and legacy_ts != platform_ts:
        issues.append(_item(row, publication, _platform(publication), "conflicting_published_at"))

    legacy_tweet_id = _clean(row.get("tweet_id"))
    platform_post_id = _clean(publication.get("platform_post_id"))
    if legacy_tweet_id and platform_post_id and legacy_tweet_id != platform_post_id:
        issues.append(_item(row, publication, _platform(publication), "legacy_tweet_id_mismatch"))

    legacy_url = _clean(row.get("published_url"))
    platform_url = _clean(publication.get("platform_url"))
    if legacy_url and platform_url and legacy_url != platform_url:
        issues.append(_item(row, publication, _platform(publication), "legacy_published_url_mismatch"))
    return issues


def _item(row: dict[str, Any], publication: dict[str, Any] | None, platform: str, issue_type: str) -> dict[str, Any]:
    return {
        "content_id": _int_or_none(row.get("content_id") or row.get("id") or (publication or {}).get("content_id")),
        "platform": platform,
        "issue_type": issue_type,
        "publication_id": _int_or_none((publication or {}).get("publication_id") or (publication or {}).get("id")),
        "legacy_published": _bool(row.get("published")),
        "legacy_published_at": _iso_or_none(row.get("published_at")),
        "platform_published_at": _iso_or_none((publication or {}).get("published_at")),
        "legacy_tweet_id": _clean(row.get("tweet_id")) or None,
        "platform_post_id": _clean((publication or {}).get("platform_post_id")) or None,
        "legacy_published_url": _clean(row.get("published_url")) or None,
        "platform_url": _clean((publication or {}).get("platform_url")) or None,
        "publication_status": _status(publication or {}),
    }


def _load_generated_content(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cols = schema["generated_content"]
    select = [
        "id AS content_id",
        _expr(cols, "content_type", "content_type", "'unknown'"),
        _expr(cols, "published", "published", "0"),
        _expr(cols, "published_url", "published_url", "NULL"),
        _expr(cols, "tweet_id", "tweet_id", "NULL"),
        _expr(cols, "published_at", "published_at", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM generated_content ORDER BY id ASC")]


def _load_content_publications(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cols = schema["content_publications"]
    select = [
        _expr(cols, "id", "publication_id", "NULL"),
        "content_id AS content_id",
        _expr(cols, "platform", "platform", "'unknown'"),
        _expr(cols, "status", "status", "'unknown'"),
        _expr(cols, "platform_post_id", "platform_post_id", "NULL"),
        _expr(cols, "platform_url", "platform_url", "NULL"),
        _expr(cols, "published_at", "published_at", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM content_publications ORDER BY content_id ASC, rowid ASC")]


def _legacy_platform(row: dict[str, Any]) -> str:
    content_type = _clean(row.get("content_type")).lower()
    if content_type.startswith("x_") or _clean(row.get("tweet_id")):
        return "x"
    if "blog" in content_type:
        return "blog"
    return "unknown"


def _platform(row: dict[str, Any]) -> str:
    return _clean(row.get("platform"), "unknown").lower()


def _status(row: dict[str, Any]) -> str:
    return _clean(row.get("status"), "unknown").lower()


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (item["content_id"] or 0, item["platform"], item["issue_type"], item["publication_id"] or 0)


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{column} AS {output}" if column in columns else f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    for candidate in (text, text.replace(" ", "T", 1)):
        try:
            return _utc(datetime.fromisoformat(candidate))
        except ValueError:
            continue
    return None


def _iso_or_none(value: Any) -> str | None:
    parsed = _parse_dt(value)
    return parsed.isoformat() if parsed else (_clean(value) or None)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "published"}
    return bool(value)
