"""Compare GitHub activity rows against generated or published content."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
_SHA_RE = re.compile(r"\b[0-9a-f]{7,40}\b", re.IGNORECASE)


def build_github_activity_to_post_conversion_report(
    activity_rows: list[dict[str, Any]],
    content_rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return per-activity content conversion records from in-memory rows."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    indexed_content = [_normalize_content(row) for row in content_rows]
    records = []
    for row in activity_rows:
        activity = _normalize_activity(row)
        if not activity["activity_id"]:
            continue
        matches = [content for content in indexed_content if _content_matches(activity, content)]
        age_days = _age_days(generated_at, activity["activity_at"])
        status = "converted" if matches else "unconverted"
        records.append(
            {
                "activity_id": activity["activity_id"],
                "activity_type": activity["activity_type"],
                "repository": activity["repository"],
                "commit_sha": activity["commit_sha"],
                "activity_at": _iso(activity["activity_at"]),
                "age_days": age_days,
                "age_bucket": _age_bucket(age_days),
                "conversion_status": status,
                "matched_content_ids": [item["content_id"] for item in matches],
                "published_statuses": sorted({item["published_status"] for item in matches if item["published_status"]}),
            }
        )
    records.sort(key=_sort_key)
    ranked = records[:limit]
    totals = {
        "activity_count": len(records),
        "record_count": len(ranked),
        "converted": sum(1 for item in records if item["conversion_status"] == "converted"),
        "unconverted": sum(1 for item in records if item["conversion_status"] == "unconverted"),
        "by_activity_type": _by_type(records),
    }
    return {
        "artifact_type": "github_activity_to_post_conversion",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit},
        "totals": totals,
        "activities": ranked,
        "empty_state": {
            "is_empty": not records,
            "message": "No GitHub activity rows found." if not records else None,
        },
    }


def build_github_activity_to_post_conversion_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_github_activity_to_post_conversion_report(
        _load_activities(conn, schema),
        _load_content(conn, schema),
        **kwargs,
    )


def format_github_activity_to_post_conversion_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_github_activity_to_post_conversion_text(report: dict[str, Any]) -> str:
    lines = [
        "GitHub Activity To Post Conversion",
        f"Generated: {report['generated_at']}",
        (
            "Totals: "
            f"activities={report['totals']['activity_count']} converted={report['totals']['converted']} "
            f"unconverted={report['totals']['unconverted']}"
        ),
    ]
    if not report["activities"]:
        lines.extend(["", report["empty_state"]["message"]])
        return "\n".join(lines)
    lines.extend(["", "Activities:", "status       type     age_bucket  age_d  content_ids  activity"])
    for item in report["activities"]:
        lines.append(
            f"{item['conversion_status']:<12} {item['activity_type'][:8]:<8} "
            f"{item['age_bucket']:<11} {item['age_days']:<6.1f} "
            f"{','.join(item['matched_content_ids']) or '-':<12} {item['activity_id']}"
        )
    return "\n".join(lines)


def _normalize_activity(row: dict[str, Any]) -> dict[str, Any]:
    activity_id = _text(
        row.get("activity_id")
        or row.get("source_activity_id")
        or _logical_id(row)
        or row.get("id")
    )
    return {
        "activity_id": activity_id,
        "activity_type": _text(row.get("activity_type") or row.get("type") or "unknown"),
        "repository": _text(row.get("repository") or row.get("repo_name")),
        "commit_sha": _text(row.get("commit_sha") or row.get("sha")),
        "activity_at": _parse_dt(row.get("activity_at") or row.get("created_at") or row.get("updated_at")),
    }


def _normalize_content(row: dict[str, Any]) -> dict[str, Any]:
    text = _text(row.get("content") or row.get("body") or row.get("text"))
    source_activity_ids = {str(item) for item in _list(row.get("source_activity_ids") or row.get("activity_ids"))}
    source_commits = {str(item).lower() for item in _list(row.get("source_commits") or row.get("commit_shas"))}
    source_commits.update(match.lower() for match in _SHA_RE.findall(text))
    published_status = _published_status(row)
    return {
        "content_id": _text(row.get("content_id") or row.get("id") or row.get("post_id")),
        "source_activity_ids": source_activity_ids,
        "source_commits": source_commits,
        "published_status": published_status,
    }


def _content_matches(activity: dict[str, Any], content: dict[str, Any]) -> bool:
    if activity["activity_id"] and activity["activity_id"] in content["source_activity_ids"]:
        return True
    sha = activity["commit_sha"].lower()
    return bool(sha and any(candidate.startswith(sha) or sha.startswith(candidate) for candidate in content["source_commits"]))


def _load_activities(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema.get("github_activity")
    if not columns:
        return []
    selected = [
        "id",
        "repo_name" if "repo_name" in columns else "NULL AS repo_name",
        "activity_type" if "activity_type" in columns else "NULL AS activity_type",
        "number" if "number" in columns else "NULL AS number",
        "commit_sha" if "commit_sha" in columns else "NULL AS commit_sha",
        "created_at" if "created_at" in columns else "NULL AS created_at",
        "updated_at" if "updated_at" in columns else "NULL AS updated_at",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM github_activity").fetchall()]


def _load_content(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema.get("generated_content")
    if not columns:
        return []
    selected = [
        "id",
        "content" if "content" in columns else "NULL AS content",
        "source_activity_ids" if "source_activity_ids" in columns else "NULL AS source_activity_ids",
        "source_commits" if "source_commits" in columns else "NULL AS source_commits",
        "published" if "published" in columns else "0 AS published",
        "published_at" if "published_at" in columns else "NULL AS published_at",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content").fetchall()]


def _logical_id(row: dict[str, Any]) -> str:
    repo = _text(row.get("repository") or row.get("repo_name"))
    number = _text(row.get("number"))
    kind = _text(row.get("activity_type") or row.get("type"))
    return f"{repo}#{number}:{kind}" if repo and number and kind else ""


def _published_status(row: dict[str, Any]) -> str:
    status = _text(row.get("published_status") or row.get("status"))
    if status:
        return status
    return "published" if _truthy(row.get("published")) or row.get("published_at") else "draft"


def _age_days(now: datetime, value: datetime | None) -> float:
    return round(max((now - value).total_seconds() / 86400, 0), 2) if value else 0.0


def _age_bucket(age_days: float) -> str:
    if age_days <= 7:
        return "week_1"
    if age_days <= 30:
        return "month_1"
    if age_days <= 90:
        return "quarter_1"
    return "stale"


def _by_type(records: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    totals: dict[str, dict[str, int]] = {}
    for record in records:
        item = totals.setdefault(record["activity_type"], {"converted": 0, "unconverted": 0})
        item[record["conversion_status"]] += 1
    return totals


def _sort_key(item: dict[str, Any]) -> tuple[int, float, str]:
    return (0 if item["conversion_status"] == "unconverted" else 1, -item["age_days"], item["activity_id"])


def _list(value: Any) -> list[Any]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return [value]
    return parsed if isinstance(parsed, list) else [parsed]


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return _utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _truthy(value: Any) -> bool:
    return str(value).lower() in {"1", "true", "yes", "published"}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}
