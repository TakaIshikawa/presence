"""Report mismatches across generated content, publication state, and queue state."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
ARTIFACT_TYPE = "content_publication_state_mismatch"
ISSUE_TYPES = (
    "missing_publication_record",
    "published_missing_platform_metadata",
    "queue_publication_status_disagreement",
    "generated_published_flag_disagreement",
)
PUBLISHED_STATUSES = {"published", "success", "succeeded", "sent", "completed"}
UNPUBLISHED_PUBLICATION_STATUSES = {"queued", "pending", "failed", "error", "cancelled", "held"}


def build_content_publication_state_mismatch_report(
    generated_rows: list[dict[str, Any]],
    publication_rows: list[dict[str, Any]],
    queue_rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic report of publication state mismatches."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    generated = [_normalize_generated(row) for row in generated_rows]
    publications = [_normalize_publication(row) for row in publication_rows]
    queue = [_normalize_queue(row) for row in queue_rows]

    publications_by_content: dict[int, list[dict[str, Any]]] = {}
    publications_by_key: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for publication in publications:
        content_id = publication["content_id"]
        if content_id is None:
            continue
        publications_by_content.setdefault(content_id, []).append(publication)
        publications_by_key.setdefault((content_id, publication["platform"]), []).append(publication)

    findings: list[dict[str, Any]] = []
    for row in generated:
        content_id = row["content_id"]
        if content_id is None:
            continue
        content_publications = publications_by_content.get(content_id, [])
        if row["generated_published"] is True and not content_publications:
            findings.append(
                _finding(
                    "missing_publication_record",
                    content_id=content_id,
                    platform=row["platform"],
                    generated=row,
                )
            )

        published_publications = [pub for pub in content_publications if _is_published_status(pub["publication_status"])]
        has_published_publication = bool(published_publications)
        if row["generated_published"] is True:
            for publication in content_publications:
                if not _is_published_status(publication["publication_status"]):
                    findings.append(
                        _finding(
                            "generated_published_flag_disagreement",
                            content_id=content_id,
                            platform=publication["platform"],
                            generated=row,
                            publication=publication,
                        )
                    )
        elif row["generated_published"] is False and has_published_publication:
            for publication in published_publications:
                findings.append(
                    _finding(
                        "generated_published_flag_disagreement",
                        content_id=content_id,
                        platform=publication["platform"],
                        generated=row,
                        publication=publication,
                    )
                )

    for publication in publications:
        if not _is_published_status(publication["publication_status"]):
            continue
        missing_fields = [
            field
            for field in ("platform_post_id", "platform_url", "published_at")
            if not _clean(publication.get(field))
        ]
        if missing_fields:
            findings.append(
                _finding(
                    "published_missing_platform_metadata",
                    content_id=publication["content_id"],
                    platform=publication["platform"],
                    publication=publication,
                    missing_fields=missing_fields,
                )
            )

    for item in queue:
        if item["content_id"] is None or not _is_published_status(item["queue_status"]):
            continue
        candidates = _matching_publications(publications_by_key, item["content_id"], item["platform"])
        for publication in candidates:
            if publication["publication_status"] in UNPUBLISHED_PUBLICATION_STATUSES:
                findings.append(
                    _finding(
                        "queue_publication_status_disagreement",
                        content_id=item["content_id"],
                        platform=publication["platform"],
                        publication=publication,
                        queue=item,
                    )
                )

    findings.sort(key=_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit},
        "summary": {
            "generated_content_count": len(generated_rows),
            "content_publication_count": len(publication_rows),
            "publish_queue_count": len(queue_rows),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": _counts(findings),
            "sample_keys": [{"content_id": row["content_id"], "platform": row["platform"]} for row in shown[:10]],
        },
        "findings": _group_findings(shown),
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items())},
        "empty_state": {
            "is_empty": not findings,
            "message": "No content publication state mismatches found." if not findings else None,
        },
    }


def build_content_publication_state_mismatch_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load SQLite state and build the mismatch report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {
        "generated_content": {"id"},
        "content_publications": {"content_id", "platform", "status"},
        "publish_queue": {"content_id", "status"},
    }
    missing_tables = [table for table in required if table not in schema]
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and not columns.issubset(schema[table])
    }
    if missing_tables or missing_columns:
        return build_content_publication_state_mismatch_report(
            [],
            [],
            [],
            missing_tables=missing_tables,
            missing_columns=missing_columns,
            **kwargs,
        )

    return build_content_publication_state_mismatch_report(
        _load_generated(conn, schema["generated_content"]),
        _load_publications(conn, schema["content_publications"]),
        _load_queue(conn, schema["publish_queue"]),
        **kwargs,
    )


def format_content_publication_state_mismatch_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_publication_state_mismatch_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    lines = [
        "Content Publication State Mismatch",
        f"Generated: {report['generated_at']}",
        f"Limit: {report['filters']['limit']}",
        (
            "Totals: "
            f"generated={summary['generated_content_count']} "
            f"publications={summary['content_publication_count']} "
            f"queue={summary['publish_queue_count']} "
            f"findings={summary['finding_count']} shown={summary['shown_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not any(report["findings"].values()):
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "content_id | platform | issue_type | generated_published | publication_status | queue_status | missing_fields"])
    for issue_type in ISSUE_TYPES:
        for item in report["findings"].get(issue_type, []):
            lines.append(
                f"{item['content_id']} | {item['platform']} | {item['issue_type']} | "
                f"{_display(item.get('generated_published'))} | {_display(item.get('publication_status'))} | "
                f"{_display(item.get('queue_status'))} | {', '.join(item.get('missing_fields') or []) or '-'}"
            )
    return "\n".join(lines)


def _load_generated(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        "id AS content_id",
        _expr(columns, "content_type", "content_type", "'unknown'"),
        _expr(columns, "published", "published", "0"),
        _expr(columns, "tweet_id", "tweet_id", "NULL"),
        _expr(columns, "published_url", "published_url", "NULL"),
        _expr(columns, "published_at", "published_at", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM generated_content ORDER BY id ASC")]


def _load_publications(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        _expr(columns, "id", "publication_id", "NULL"),
        "content_id AS content_id",
        "platform AS platform",
        "status AS publication_status",
        _expr(columns, "platform_post_id", "platform_post_id", "NULL"),
        _expr(columns, "platform_url", "platform_url", "NULL"),
        _expr(columns, "published_at", "published_at", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM content_publications ORDER BY content_id ASC, platform ASC, rowid ASC")]


def _load_queue(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        _expr(columns, "id", "queue_id", "NULL"),
        "content_id AS content_id",
        _expr(columns, "platform", "platform", "'unknown'"),
        "status AS queue_status",
        _expr(columns, "published_at", "queue_published_at", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM publish_queue ORDER BY content_id ASC, rowid ASC")]


def _normalize_generated(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "content_id": _int_or_none(row.get("content_id") or row.get("id")),
        "platform": _legacy_platform(row),
        "generated_published": _bool_or_none(row.get("published")),
        "generated_published_at": row.get("published_at"),
        "published_url": _clean(row.get("published_url")) or None,
        "tweet_id": _clean(row.get("tweet_id")) or None,
    }


def _normalize_publication(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "publication_id": _int_or_none(row.get("publication_id") or row.get("id")),
        "content_id": _int_or_none(row.get("content_id")),
        "platform": _platform(row.get("platform")),
        "publication_status": _clean(row.get("publication_status") or row.get("status")).lower() or "unknown",
        "platform_post_id": _clean(row.get("platform_post_id")) or None,
        "platform_url": _clean(row.get("platform_url")) or None,
        "published_at": _clean(row.get("published_at")) or None,
    }


def _normalize_queue(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "queue_id": _int_or_none(row.get("queue_id") or row.get("id")),
        "content_id": _int_or_none(row.get("content_id")),
        "platform": _platform(row.get("platform")),
        "queue_status": _clean(row.get("queue_status") or row.get("status")).lower() or "unknown",
        "queue_published_at": _clean(row.get("queue_published_at") or row.get("published_at")) or None,
    }


def _finding(
    issue_type: str,
    *,
    content_id: int | None,
    platform: str,
    generated: dict[str, Any] | None = None,
    publication: dict[str, Any] | None = None,
    queue: dict[str, Any] | None = None,
    missing_fields: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "issue_type": issue_type,
        "content_id": content_id,
        "platform": platform,
        "generated_published": (generated or {}).get("generated_published"),
        "publication_id": (publication or {}).get("publication_id"),
        "publication_status": (publication or {}).get("publication_status"),
        "queue_id": (queue or {}).get("queue_id"),
        "queue_status": (queue or {}).get("queue_status"),
        "platform_post_id": (publication or {}).get("platform_post_id"),
        "platform_url": (publication or {}).get("platform_url"),
        "published_at": (publication or {}).get("published_at"),
        "missing_fields": missing_fields or [],
    }


def _matching_publications(
    publications_by_key: dict[tuple[int, str], list[dict[str, Any]]],
    content_id: int,
    platform: str,
) -> list[dict[str, Any]]:
    if platform == "all":
        return [row for (cid, _platform_name), rows in publications_by_key.items() if cid == content_id for row in rows]
    return publications_by_key.get((content_id, platform), [])


def _group_findings(findings: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped = {issue_type: [] for issue_type in ISSUE_TYPES}
    for finding in findings:
        grouped[finding["issue_type"]].append(finding)
    return grouped


def _counts(findings: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(finding["issue_type"] for finding in findings)
    return {issue_type: counts[issue_type] for issue_type in ISSUE_TYPES}


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (ISSUE_TYPES.index(item["issue_type"]), item["content_id"] or 0, item["platform"], item.get("publication_id") or 0, item.get("queue_id") or 0)


def _is_published_status(value: Any) -> bool:
    return _clean(value).lower() in PUBLISHED_STATUSES


def _legacy_platform(row: dict[str, Any]) -> str:
    content_type = _clean(row.get("content_type")).lower()
    if content_type.startswith("x_") or _clean(row.get("tweet_id")):
        return "x"
    if "blue" in content_type:
        return "bluesky"
    if "blog" in content_type:
        return "blog"
    return "unknown"


def _platform(value: Any) -> str:
    return _clean(value).lower() or "unknown"


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{_quote_identifier(column)} AS {output}" if column in columns else f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({_quote_identifier(str(row[0]))})")} for row in rows}


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _display(value: Any) -> str:
    if value is None or value == "":
        return "-"
    return str(value)


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    text = _clean(value).lower()
    if text in {"1", "true", "yes", "published"}:
        return True
    if text in {"0", "false", "no", "unpublished"}:
        return False
    return None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
