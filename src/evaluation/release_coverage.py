"""Report GitHub releases that have not yet produced generated content."""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any


def build_release_coverage_report(
    db: Any,
    *,
    days: int = 30,
    repo: str | None = None,
    min_age_hours: float = 12,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Compare recent GitHub release activity with generated-content coverage."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_age_hours < 0:
        raise ValueError("min_age_hours must be non-negative")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    conn = _connection(db)
    schema = _schema(conn)
    filters = {"days": days, "repo": repo, "min_age_hours": float(min_age_hours)}
    missing_required_tables = [
        table for table in ("github_activity", "generated_content") if table not in schema
    ]
    if missing_required_tables:
        return _empty_report(generated_at, filters, missing_required_tables)

    releases = _recent_release_rows(db, conn, schema, days=days, repo=repo, now=generated_at)
    release_items = [
        _release_item(row, generated_at)
        for row in releases
        if _release_age_hours(row, generated_at) >= min_age_hours
    ]
    release_items.sort(
        key=lambda item: (
            item["released_at"] or "",
            item["repo_name"] or "",
            item["tag_name"] or "",
            item["id"],
        ),
        reverse=True,
    )

    generated_rows = _generated_content_rows(conn, schema)
    publications = _publication_rows(conn, schema)
    items = [
        _attach_matches(item, generated_rows, publications)
        for item in release_items
    ]
    covered_count = sum(1 for item in items if item["coverage_status"] == "covered")
    uncovered_count = len(items) - covered_count

    return {
        "artifact_type": "release_coverage",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "counts": {
            "total_releases": len(items),
            "covered": covered_count,
            "uncovered": uncovered_count,
        },
        "missing_required_tables": missing_required_tables,
        "items": items,
    }


def format_json_report(report: dict[str, Any]) -> str:
    """Serialize the release coverage report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_text_report(report: dict[str, Any]) -> str:
    """Render a deterministic terminal report."""
    filters = report["filters"]
    counts = report["counts"]
    lines = [
        "Release Coverage Report",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"days={filters['days']} "
            f"repo={filters['repo'] or 'all'} "
            f"min_age_hours={_format_hours(filters['min_age_hours'])}"
        ),
        (
            "Counts: "
            f"releases={counts['total_releases']} "
            f"covered={counts['covered']} "
            f"uncovered={counts['uncovered']}"
        ),
    ]
    if report.get("missing_required_tables"):
        lines.append(
            "Missing required tables: "
            + ", ".join(report["missing_required_tables"])
        )
    if not report["items"]:
        lines.append("")
        lines.append("No eligible releases found.")
        return "\n".join(lines)

    uncovered = [item for item in report["items"] if item["coverage_status"] == "uncovered"]
    covered = [item for item in report["items"] if item["coverage_status"] == "covered"]

    lines.append("")
    lines.append("Uncovered Releases")
    if not uncovered:
        lines.append("  none")
    for item in uncovered:
        lines.append(f"  - {_release_label(item)}")

    lines.append("")
    lines.append("Covered Releases")
    if not covered:
        lines.append("  none")
    for item in covered:
        content = ", ".join(
            f"#{match['content_id']} {match['content_type'] or '-'} {match['publication_status']}"
            for match in item["matched_content"]
        )
        lines.append(f"  - {_release_label(item)} content={content}")
    return "\n".join(lines)


def _connection(db: Any) -> sqlite3.Connection:
    conn = db.conn if hasattr(db, "conn") else db
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        schema[table] = {
            str(info[1]) for info in conn.execute(f"PRAGMA table_info({table})")
        }
    return schema


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_required_tables: list[str],
) -> dict[str, Any]:
    return {
        "artifact_type": "release_coverage",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "counts": {"total_releases": 0, "covered": 0, "uncovered": 0},
        "missing_required_tables": missing_required_tables,
        "items": [],
    }


def _recent_release_rows(
    db: Any,
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    days: int,
    repo: str | None,
    now: datetime,
) -> list[dict[str, Any]]:
    method = getattr(db, "get_recent_github_releases", None)
    if callable(method):
        try:
            return [
                dict(row)
                for row in method(days=days, repo_name=repo, limit=None, now=now)
            ]
        except TypeError:
            pass

    columns = schema["github_activity"]
    cutoff = (now - timedelta(days=days)).isoformat()
    where = ["activity_type = 'release'"]
    params: list[Any] = []
    timestamp_column = _first_column(columns, ("updated_at", "ingested_at", "created_at_github"))
    if timestamp_column:
        where.append(f"{timestamp_column} >= ?")
        params.append(cutoff)
    if repo and "repo_name" in columns:
        where.append("repo_name = ?")
        params.append(repo)
    elif repo:
        return []
    order = _order_clause(columns)
    rows = conn.execute(
        f"SELECT * FROM github_activity WHERE {' AND '.join(where)} ORDER BY {order}",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _generated_content_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    select = ["id"]
    for column in (
        "content_type",
        "content",
        "source_activity_ids",
        "source_metadata",
        "metadata",
        "published",
        "published_at",
        "published_url",
        "tweet_id",
        "created_at",
    ):
        if column in columns:
            select.append(column)
    rows = conn.execute(
        f"SELECT {', '.join(select)} FROM generated_content ORDER BY id ASC"
    ).fetchall()
    return [dict(row) for row in rows]


def _publication_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, list[dict[str, Any]]]:
    columns = schema.get("content_publications")
    if not columns or "content_id" not in columns:
        return {}
    select = ["content_id"]
    for column in ("platform", "status", "published_at", "platform_url", "updated_at"):
        if column in columns:
            select.append(column)
    rows = conn.execute(
        f"SELECT {', '.join(select)} FROM content_publications ORDER BY content_id ASC, platform ASC"
    ).fetchall()
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        item = dict(row)
        grouped.setdefault(int(item["content_id"]), []).append(
            {
                "platform": item.get("platform"),
                "status": item.get("status"),
                "published_at": item.get("published_at"),
                "platform_url": item.get("platform_url"),
                "updated_at": item.get("updated_at"),
            }
        )
    return grouped


def _release_item(row: dict[str, Any], now: datetime) -> dict[str, Any]:
    metadata = _json_object(row.get("metadata")) or {}
    released_at = (
        _clean(metadata.get("published_at"))
        or _clean(row.get("updated_at"))
        or _clean(row.get("created_at_github"))
        or _clean(row.get("ingested_at"))
    )
    tag_name = (
        _clean(metadata.get("tag_name"))
        or _clean(row.get("tag_name"))
        or _clean(row.get("number"))
    )
    repo_name = _clean(row.get("repo_name")) or ""
    number = _clean(row.get("number")) or tag_name
    activity_id = f"{repo_name}#{number}:release"
    return {
        "id": int(row.get("id") or 0),
        "activity_id": activity_id,
        "repo_name": repo_name,
        "tag_name": tag_name,
        "release_id": metadata.get("release_id") or number,
        "title": _clean(row.get("title")) or "",
        "state": _clean(row.get("state")),
        "url": _clean(row.get("url")),
        "released_at": released_at,
        "age_hours": round(_age_hours(released_at, now), 2),
        "coverage_status": "uncovered",
        "matched_content": [],
    }


def _attach_matches(
    release: dict[str, Any],
    generated_rows: list[dict[str, Any]],
    publications: dict[int, list[dict[str, Any]]],
) -> dict[str, Any]:
    matches = []
    for row in generated_rows:
        reasons = _match_reasons(release, row)
        if not reasons:
            continue
        content_id = int(row.get("id") or 0)
        publication_rows = publications.get(content_id, [])
        legacy = _legacy_publication(row)
        all_publications = publication_rows + ([legacy] if legacy else [])
        matches.append(
            {
                "content_id": content_id,
                "content_type": row.get("content_type"),
                "created_at": row.get("created_at"),
                "publication_status": _publication_status(row, all_publications),
                "published_at": _first_published_at(row, all_publications),
                "published_url": row.get("published_url"),
                "match_reasons": reasons,
                "publications": all_publications,
            }
        )
    matches.sort(key=lambda item: item["content_id"])
    covered = dict(release)
    covered["matched_content"] = matches
    covered["coverage_status"] = "covered" if matches else "uncovered"
    return covered


def _match_reasons(release: dict[str, Any], row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    refs = {str(ref) for ref in _json_list(row.get("source_activity_ids")) if ref is not None}
    source_refs = {
        str(release["id"]),
        str(release["activity_id"]),
    }
    metadata_refs = source_refs | {str(release.get("release_id") or "")}
    if refs & source_refs:
        reasons.append("source_activity_ids")

    metadata_values = _metadata_values(row)
    for value in metadata_refs | {release.get("repo_name") or "", release.get("tag_name") or ""}:
        if value and value in metadata_values:
            reasons.append("source_metadata")
            break

    content = str(row.get("content") or "").casefold()
    tag = str(release.get("tag_name") or "").strip()
    title = str(release.get("title") or "").strip()
    repo = str(release.get("repo_name") or "").strip()
    if tag and _contains_token(content, tag):
        reasons.append("content_tag_reference")
    elif title and len(title) >= 8 and title.casefold() in content:
        reasons.append("content_title_reference")
    elif repo and tag and repo.casefold() in content and tag.casefold() in content:
        reasons.append("content_repo_tag_reference")

    return sorted(set(reasons))


def _metadata_values(row: dict[str, Any]) -> set[str]:
    values: set[str] = set()
    for column in ("source_metadata", "metadata"):
        parsed = _json_any(row.get(column))
        _collect_json_scalars(parsed, values)
    return values


def _collect_json_scalars(value: Any, values: set[str]) -> None:
    if isinstance(value, dict):
        for item in value.values():
            _collect_json_scalars(item, values)
    elif isinstance(value, list):
        for item in value:
            _collect_json_scalars(item, values)
    elif value is not None:
        values.add(str(value))


def _legacy_publication(row: dict[str, Any]) -> dict[str, Any] | None:
    published = row.get("published")
    if not _truthy(published) and not row.get("published_at") and not row.get("published_url"):
        return None
    return {
        "platform": "legacy",
        "status": "published" if _truthy(published) or row.get("published_at") else "unknown",
        "published_at": row.get("published_at"),
        "platform_url": row.get("published_url"),
        "updated_at": row.get("published_at"),
    }


def _publication_status(
    row: dict[str, Any],
    publications: list[dict[str, Any]],
) -> str:
    statuses = {str(item.get("status") or "").lower() for item in publications}
    if "published" in statuses or _truthy(row.get("published")) or row.get("published_at"):
        return "published"
    if statuses & {"queued", "retrying", "scheduled", "held"}:
        return "queued"
    if "failed" in statuses:
        return "failed"
    if publications:
        return sorted(statuses)[0] if statuses else "unknown"
    return "draft"


def _first_published_at(
    row: dict[str, Any],
    publications: list[dict[str, Any]],
) -> str | None:
    timestamps = [
        item.get("published_at")
        for item in publications
        if str(item.get("status") or "").lower() == "published" and item.get("published_at")
    ]
    timestamps.sort()
    return timestamps[0] if timestamps else row.get("published_at")


def _release_age_hours(row: dict[str, Any], now: datetime) -> float:
    return _age_hours(_release_item(row, now)["released_at"], now)


def _age_hours(value: Any, now: datetime) -> float:
    parsed = _parse_datetime(value)
    if parsed is None:
        return float("inf")
    return max((now - parsed).total_seconds() / 3600, 0.0)


def _parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            try:
                dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _json_any(value: Any) -> Any:
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return None


def _json_object(value: Any) -> dict[str, Any] | None:
    parsed = _json_any(value)
    return parsed if isinstance(parsed, dict) else None


def _json_list(value: Any) -> list[Any]:
    parsed = _json_any(value)
    return parsed if isinstance(parsed, list) else []


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "published"}
    return bool(value)


def _contains_token(haystack: str, needle: str) -> bool:
    pattern = r"(?<![\w.])" + re.escape(needle.casefold()) + r"(?![\w.])"
    return re.search(pattern, haystack) is not None


def _first_column(columns: set[str], names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in columns), None)


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "updated_at" in columns:
        parts.append("updated_at DESC")
    if "id" in columns:
        parts.append("id DESC")
    return ", ".join(parts) or "rowid DESC"


def _format_hours(value: Any) -> str:
    number = float(value)
    return str(int(number)) if number.is_integer() else str(number)


def _release_label(item: dict[str, Any]) -> str:
    tag = item.get("tag_name") or "-"
    title = item.get("title") or "-"
    released = item.get("released_at") or "-"
    url = item.get("url") or "-"
    return (
        f"{item['repo_name']} {tag} [{item['state'] or '-'}] "
        f"released={released} title={title} url={url}"
    )
