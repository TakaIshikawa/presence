"""Flag source domains that are overused in recent content windows."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import urlparse


DEFAULT_LIMIT = 50
DEFAULT_WINDOW_SIZE = 10
DEFAULT_MAX_ROLLING_SHARE = 0.5
URL_RE = re.compile(r"https?://[^\s<>)\"']+")


def build_source_domain_rotation_gaps_report(
    citation_rows: list[dict[str, Any]],
    knowledge_rows: list[dict[str, Any]] | None = None,
    *,
    window_size: int = DEFAULT_WINDOW_SIZE,
    max_rolling_share: float = DEFAULT_MAX_ROLLING_SHARE,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if window_size <= 0:
        raise ValueError("window_size must be positive")
    if not 0 < max_rolling_share <= 1:
        raise ValueError("max_rolling_share must be between 0 and 1")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    content = [_normalize_content(row) for row in citation_rows]
    content = [row for row in content if row["domains"]]
    content.sort(key=lambda row: (row["created_at"] or datetime.min.replace(tzinfo=timezone.utc), row["content_id"]))
    recent = content[-window_size:]
    recent_domain_counts = Counter(domain for row in recent for domain in row["domains"])
    recent_content_count = len(recent)
    affected: dict[str, list[str]] = defaultdict(list)
    for row in recent:
        for domain in row["domains"]:
            affected[domain].append(row["content_id"])

    candidates = _candidate_domains(knowledge_rows or [])
    gaps = []
    for domain, recent_count in recent_domain_counts.items():
        rolling_share = recent_count / recent_content_count if recent_content_count else 0.0
        longest_streak = _longest_streak(content, domain)
        score = _rotation_gap_score(rolling_share, longest_streak, max_rolling_share)
        if score <= 0:
            continue
        gaps.append(
            {
                "domain": domain,
                "recent_count": recent_count,
                "rolling_share": round(rolling_share, 4),
                "longest_streak": longest_streak,
                "affected_content_ids": affected[domain],
                "rotation_gap_score": score,
                "candidate_alternative_domains": [item for item in candidates if item != domain][:5],
            }
        )

    gaps.sort(key=lambda item: (-item["rotation_gap_score"], -item["recent_count"], item["domain"]))
    shown = gaps[:limit]
    return {
        "artifact_type": "source_domain_rotation_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {"window_size": window_size, "max_rolling_share": max_rolling_share, "limit": limit},
        "totals": {
            "content_count": len(content),
            "recent_content_count": recent_content_count,
            "gap_count": len(gaps),
            "shown_count": len(shown),
        },
        "gaps": shown,
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
        "empty_state": {
            "is_empty": not gaps,
            "message": "No source domain rotation gaps found." if not gaps else None,
        },
    }


def build_source_domain_rotation_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    citation_rows = _load_citations(conn, schema)
    knowledge_rows = _load_knowledge(conn, schema)
    return build_source_domain_rotation_gaps_report(citation_rows, knowledge_rows, schema_gaps=gaps, **kwargs)


def format_source_domain_rotation_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_source_domain_rotation_gaps_text(report: dict[str, Any]) -> str:
    lines = [
        "Source Domain Rotation Gaps",
        f"Generated: {report['generated_at']}",
        f"Totals: recent={report['totals']['recent_content_count']} gaps={report['totals']['gap_count']}",
    ]
    if not report["gaps"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "domain | recent_count | rolling_share | longest_streak | score | affected_content_ids | alternatives"])
    for row in report["gaps"]:
        lines.append(
            f"{row['domain']} | {row['recent_count']} | {row['rolling_share']:.4f} | {row['longest_streak']} | "
            f"{row['rotation_gap_score']} | {','.join(row['affected_content_ids'])} | {','.join(row['candidate_alternative_domains']) or '-'}"
        )
    return "\n".join(lines)


format_source_domain_rotation_gaps_table = format_source_domain_rotation_gaps_text


def _normalize_content(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _json_object(_first(row, "metadata", "raw_metadata"))
    text = _text(_first(row, "content", "body", "text", "html", "title"))
    raw_domains = _items(_first(row, "domains", "source_domains", "citation_domains") or metadata.get("domains") or metadata.get("source_domains"))
    urls = _items(_first(row, "urls", "source_urls", "citation_urls") or metadata.get("urls") or metadata.get("source_urls")) + URL_RE.findall(text)
    domains = {_domain(item) for item in raw_domains + urls}
    domains.discard("")
    return {
        "content_id": _text(_first(row, "content_id", "id", "newsletter_id", "generated_content_id")) or "unknown",
        "created_at": _parse_ts(_first(row, "created_at", "published_at", "sent_at", "scheduled_at")),
        "domains": sorted(domains),
    }


def _candidate_domains(rows: list[dict[str, Any]]) -> list[str]:
    counts = Counter()
    for row in rows:
        metadata = _json_object(_first(row, "metadata", "raw_metadata"))
        values = _items(_first(row, "domain", "source_domain", "url", "source_url") or metadata.get("domain") or metadata.get("url"))
        for value in values:
            domain = _domain(value)
            if domain:
                counts[domain] += 1
    return [domain for domain, _count in counts.most_common()]


def _longest_streak(content: list[dict[str, Any]], domain: str) -> int:
    longest = current = 0
    for row in content:
        if domain in row["domains"]:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _rotation_gap_score(rolling_share: float, longest_streak: int, max_rolling_share: float) -> int:
    share_excess = max(rolling_share - max_rolling_share, 0)
    streak_excess = max(longest_streak - 1, 0)
    return int(round(share_excess * 100 + streak_excess * 15))


def _load_citations(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for table in ("generated_content", "newsletters", "newsletter_issues"):
        if table not in schema:
            continue
        cols = schema[table]
        select = [
            _select(cols, ("id", "content_id", "newsletter_id"), "id"),
            _select(cols, ("created_at", "published_at", "sent_at", "scheduled_at"), "created_at"),
            _select(cols, ("content", "body", "text", "html", "title"), "content"),
            _select(cols, ("source_urls", "citation_urls", "urls"), "source_urls"),
            _select(cols, ("source_domains", "citation_domains", "domains"), "source_domains"),
            _select(cols, ("metadata", "raw_metadata"), "metadata"),
        ]
        rows.extend(dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table}").fetchall())
    return rows


def _load_knowledge(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = next((name for name in ("knowledge_sources", "sources", "knowledge") if name in schema), "")
    if not table:
        return []
    cols = schema[table]
    select = [
        _select(cols, ("domain", "source_domain"), "domain"),
        _select(cols, ("url", "source_url"), "url"),
        _select(cols, ("metadata", "raw_metadata"), "metadata"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table}").fetchall()]


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    if any(table in schema for table in ("generated_content", "newsletters", "newsletter_issues")):
        return {"missing_tables": [], "missing_columns": {}}
    return {"missing_tables": ["generated_content_or_newsletters"], "missing_columns": {}}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate if candidate == alias else f"{candidate} AS {alias}"
    return f"NULL AS {alias}"


def _first(row: dict[str, Any], *keys: str) -> Any:
    return next((row[key] for key in keys if key in row and row[key] not in (None, "")), None)


def _domain(value: Any) -> str:
    text = _text(value).lower()
    if not text:
        return ""
    if "://" not in text and "/" not in text:
        return text.removeprefix("www.")
    parsed = urlparse(text if "://" in text else f"https://{text}")
    return (parsed.netloc or parsed.path.split("/", 1)[0]).removeprefix("www.")


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple | set):
        return list(value)
    if isinstance(value, str):
        parsed = _json_object_or_list(value)
        if isinstance(parsed, list):
            return parsed
        return [part.strip() for part in value.split(",") if part.strip()]
    return [value]


def _json_object(value: Any) -> dict[str, Any]:
    parsed = _json_object_or_list(value)
    return parsed if isinstance(parsed, dict) else {}


def _json_object_or_list(value: Any) -> Any:
    if isinstance(value, dict | list):
        return value
    if not value:
        return {}
    try:
        return json.loads(str(value))
    except (TypeError, ValueError):
        return {}


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _utc(value)
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
