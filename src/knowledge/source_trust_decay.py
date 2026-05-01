"""Read-only trust decay reporting for curated knowledge sources."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any
from urllib.parse import urlparse


@dataclass(frozen=True)
class SourceTrustDecayItem:
    id: int
    source_type: str
    identifier: str
    name: str | None
    decay_score: int
    status: str
    drivers: list[str]
    recommendation: str
    license: str | None
    active: bool | None
    source_status: str | None
    last_seen_at: str | None
    last_fetched_at: str | None
    last_cited_at: str | None
    last_failure_at: str | None
    consecutive_failures: int | None
    knowledge_count: int | None
    citation_count: int | None
    age_days: float | None
    fetch_age_days: float | None
    citation_age_days: float | None


def build_source_trust_decay_report(
    db: Any,
    *,
    days: int = 90,
    limit: int | None = None,
    include_healthy: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Identify curated sources whose usefulness is likely decaying."""

    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _normalize_datetime(now or datetime.now(timezone.utc))
    conn = _connection(db)
    schema = _schema(conn)
    filters = {"days": days, "limit": limit, "include_healthy": include_healthy}
    if "curated_sources" not in schema:
        return _empty_report(generated_at, filters, ["curated_sources"], _missing_optional(schema))

    knowledge = _load_knowledge_stats(conn, schema)
    citations, citation_usage_known = _load_citation_stats(conn, schema)
    rows = _load_source_rows(conn, schema)
    items = [
        _score_source(
            row,
            knowledge=knowledge.get(_source_key(row)),
            citations=citations,
            citation_usage_known=citation_usage_known,
            days=days,
            now=generated_at,
        )
        for row in rows
    ]
    if not include_healthy:
        items = [item for item in items if item.status != "healthy"]
    items.sort(key=_sort_key)
    if limit is not None:
        items = items[:limit]

    counts = {"critical": 0, "review": 0, "watch": 0, "healthy": 0}
    for item in items:
        counts[item.status] += 1

    return {
        "artifact_type": "source_trust_decay",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "counts": {
            "sources": len(items),
            "critical": counts["critical"],
            "review": counts["review"],
            "watch": counts["watch"],
            "healthy": counts["healthy"],
        },
        "missing_required_tables": [],
        "unknown_optional_signals": _missing_optional(schema),
        "items": [asdict(item) for item in items],
    }


def format_json_report(report: dict[str, Any]) -> str:
    """Serialize the report as deterministic JSON."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_text_report(report: dict[str, Any]) -> str:
    """Render a stable terminal report."""

    filters = report["filters"]
    counts = report["counts"]
    lines = [
        "Source Trust Decay Report",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"days={filters['days']} "
            f"limit={filters['limit'] or 'none'} "
            f"include_healthy={_yes_no(filters['include_healthy'])}"
        ),
        (
            "Counts: "
            f"sources={counts['sources']} "
            f"critical={counts['critical']} "
            f"review={counts['review']} "
            f"watch={counts['watch']} "
            f"healthy={counts['healthy']}"
        ),
    ]
    if report.get("missing_required_tables"):
        lines.append("Missing required tables: " + ", ".join(report["missing_required_tables"]))
    if report.get("unknown_optional_signals"):
        lines.append("Unknown optional signals: " + ", ".join(report["unknown_optional_signals"]))
    if not report["items"]:
        lines.append("")
        lines.append("No decaying curated sources found.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Sources")
    for item in report["items"]:
        lines.append(
            f"  - #{item['id']} {item['source_type']} {_source_label(item)} "
            f"score={item['decay_score']} status={item['status']} "
            f"last_seen={item['last_seen_at'] or '-'} "
            f"last_fetched={item['last_fetched_at'] or '-'} "
            f"last_cited={item['last_cited_at'] or '-'} "
            f"drivers={','.join(item['drivers']) or '-'} "
            f"recommendation={item['recommendation']}"
        )
    return "\n".join(lines)


def _score_source(
    row: dict[str, Any],
    *,
    knowledge: dict[str, Any] | None,
    citations: dict[int, dict[str, Any]],
    citation_usage_known: bool,
    days: int,
    now: datetime,
) -> SourceTrustDecayItem:
    drivers: list[str] = []
    score = 0

    source_status = _clean(row.get("status")) or "active"
    active = _bool_or_none(row.get("active"))
    fetch_status = _clean(row.get("last_fetch_status"))
    failures = _int_or_none(row.get("consecutive_failures"))
    last_failure_at = _clean(row.get("last_failure_at"))
    last_success_at = _clean(row.get("last_success_at"))
    last_seen_at = _max_timestamp(
        last_success_at,
        _clean(row.get("published_at")),
        _clean(row.get("feed_last_modified")),
        knowledge.get("newest_knowledge_at") if knowledge else None,
    ) or _clean(row.get("created_at"))
    last_fetched_at = _max_timestamp(last_success_at, last_failure_at)
    knowledge_count = int(knowledge.get("knowledge_count") or 0) if knowledge else 0
    citation = _combined_citation_stats(knowledge, citations, citation_usage_known)
    citation_count = citation["citation_count"]
    last_cited_at = citation["last_cited_at"]

    if source_status in {"paused", "rejected"} or active is False:
        score += 70
        drivers.append("quarantined_or_inactive")
    elif fetch_status == "quarantined":
        score += 65
        drivers.append("quarantined_fetch")
    if fetch_status == "failure":
        score += 35
        drivers.append("last_fetch_failed")
    if failures is not None and failures >= 3:
        score += 45
        drivers.append("repeated_failures")
    elif failures is not None and failures > 0:
        score += 18
        drivers.append("fetch_failures")

    license_value = _clean(row.get("license"))
    if not license_value:
        score += 10
        drivers.append("missing_license")

    age_days = _age_days(last_seen_at, now)
    fetch_age_days = _age_days(last_fetched_at, now)
    citation_age_days = _age_days(last_cited_at, now)

    if knowledge_count == 0:
        score += 18
        drivers.append("no_knowledge_items")
    elif age_days is None:
        score += 12
        drivers.append("unknown_last_seen")
    elif age_days > days * 2:
        score += 35
        drivers.append("very_old_source")
    elif age_days > days:
        score += 24
        drivers.append("old_source")

    if last_fetched_at is None:
        score += 12
        drivers.append("unknown_last_fetch")
    elif fetch_age_days is not None and fetch_age_days > days:
        score += 18
        drivers.append("stale_fetch")

    if citation_count is None:
        drivers.append("unknown_citation_usage")
    elif citation_count == 0:
        score += 16
        drivers.append("never_cited")
    elif citation_age_days is not None and citation_age_days > days:
        score += 12
        drivers.append("stale_citation_usage")
    elif citation_age_days is not None and citation_age_days <= days:
        score = max(0, score - 12)
        drivers.append("recently_cited")

    if fetch_age_days is not None and fetch_age_days <= days:
        score = max(0, score - 10)
        drivers.append("recently_fetched")

    score = min(100, score)
    strong_negative = bool(
        {"quarantined_or_inactive", "quarantined_fetch", "last_fetch_failed", "repeated_failures"}
        & set(drivers)
    )
    if not strong_negative and "recently_cited" in drivers and "recently_fetched" in drivers:
        score = min(score, 29)

    status = _status(score)
    return SourceTrustDecayItem(
        id=int(row.get("id") or 0),
        source_type=_clean(row.get("source_type")) or "",
        identifier=_clean(row.get("identifier")) or "",
        name=_clean(row.get("name")),
        decay_score=score,
        status=status,
        drivers=drivers,
        recommendation=_recommendation(status, drivers),
        license=license_value,
        active=active,
        source_status=source_status,
        last_seen_at=last_seen_at,
        last_fetched_at=last_fetched_at,
        last_cited_at=last_cited_at,
        last_failure_at=last_failure_at,
        consecutive_failures=failures,
        knowledge_count=knowledge_count,
        citation_count=citation_count,
        age_days=_round_days(age_days),
        fetch_age_days=_round_days(fetch_age_days),
        citation_age_days=_round_days(citation_age_days),
    )


def _load_source_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema["curated_sources"]
    select = [
        _column_expr(columns, "id"),
        _column_expr(columns, "source_type"),
        _column_expr(columns, "identifier"),
        _column_expr(columns, "name"),
        _column_expr(columns, "license"),
        _column_expr(columns, "active"),
        _column_expr(columns, "status"),
        _column_expr(columns, "last_fetch_status"),
        _column_expr(columns, "consecutive_failures"),
        _column_expr(columns, "last_success_at"),
        _column_expr(columns, "last_failure_at"),
        _column_expr(columns, "published_at"),
        _column_expr(columns, "feed_last_modified"),
        _column_expr(columns, "created_at"),
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(select)}
            FROM curated_sources
            ORDER BY source_type ASC, identifier ASC, id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _load_knowledge_stats(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[tuple[str, str], dict[str, Any]]:
    columns = schema.get("knowledge")
    if not columns:
        return {}
    rows = conn.execute(
        f"""SELECT {_column_expr(columns, "id")} AS id,
                  {_column_expr(columns, "source_type")} AS source_type,
                  {_column_expr(columns, "source_id")} AS source_id,
                  {_column_expr(columns, "source_url")} AS source_url,
                  {_column_expr(columns, "author")} AS author,
                  {_column_expr(columns, "published_at")} AS published_at,
                  {_column_expr(columns, "ingested_at")} AS ingested_at,
                  {_column_expr(columns, "created_at")} AS created_at
           FROM knowledge
           ORDER BY id ASC"""
    ).fetchall()
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        for key in _candidate_source_keys(row):
            group = grouped.setdefault(
                key,
                {"knowledge_count": 0, "newest_knowledge_at": None, "knowledge_ids": []},
            )
            group["knowledge_count"] += 1
            group["knowledge_ids"].append(int(row["id"]))
            item_timestamp = (
                _clean(row.get("published_at")),
                _clean(row.get("ingested_at")),
                _clean(row.get("created_at")),
            )
            group["newest_knowledge_at"] = _max_timestamp(
                group["newest_knowledge_at"],
                next((value for value in item_timestamp if value), None),
            )
    return grouped


def _load_citation_stats(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> tuple[dict[int, dict[str, Any]], bool]:
    if "knowledge" not in schema:
        return {}, False
    stats: dict[int, dict[str, Any]] = {}
    usage_known = False
    for table in ("content_knowledge_links", "reply_knowledge_links"):
        columns = schema.get(table)
        if not columns or "knowledge_id" not in columns:
            continue
        usage_known = True
        rows = conn.execute(
            f"""SELECT knowledge_id,
                      COUNT(*) AS citation_count,
                      MAX({_column_expr(columns, "created_at")}) AS last_cited_at
                FROM {table}
                GROUP BY knowledge_id"""
        ).fetchall()
        for row in rows:
            knowledge_id = int(row["knowledge_id"])
            current = stats.setdefault(
                knowledge_id,
                {"citation_count": 0, "last_cited_at": None},
            )
            current["citation_count"] += int(row["citation_count"] or 0)
            current["last_cited_at"] = _max_timestamp(
                current["last_cited_at"],
                _clean(row["last_cited_at"]),
            )
    return stats, usage_known


def _combined_citation_stats(
    knowledge: dict[str, Any] | None,
    citations: dict[int, dict[str, Any]],
    usage_known: bool,
) -> dict[str, Any]:
    if knowledge is None:
        return {"citation_count": 0 if usage_known else None, "last_cited_at": None}
    if not usage_known:
        return {"citation_count": None, "last_cited_at": None}
    count = 0
    last_cited_at = None
    for knowledge_id in knowledge.get("knowledge_ids") or []:
        stat = citations.get(int(knowledge_id))
        if not stat:
            continue
        count += int(stat.get("citation_count") or 0)
        last_cited_at = _max_timestamp(last_cited_at, stat.get("last_cited_at"))
    return {"citation_count": count, "last_cited_at": last_cited_at}


def _candidate_source_keys(row: dict[str, Any]) -> set[tuple[str, str]]:
    source_type = _clean(row.get("source_type")) or ""
    curated_type = {
        "curated_x": "x_account",
        "curated_article": "blog",
        "curated_newsletter": "newsletter",
    }.get(source_type)
    if not curated_type:
        return set()
    values = {
        _normalize_identifier(row.get("author")),
        _normalize_identifier(row.get("source_id")),
        _normalize_identifier(_host(row.get("source_url"))),
        _normalize_identifier(_host(row.get("source_id"))),
    }
    values.discard("")
    return {(curated_type, value) for value in values}


def _source_key(row: dict[str, Any]) -> tuple[str, str]:
    return (
        _clean(row.get("source_type")) or "",
        _normalize_identifier(row.get("identifier")),
    )


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[str(table)] = {
            str(column[1]) for column in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
    return schema


def _connection(db: Any) -> sqlite3.Connection:
    conn = getattr(db, "conn", db)
    conn.row_factory = sqlite3.Row
    return conn


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_required: list[str],
    unknown_optional: list[str],
) -> dict[str, Any]:
    return {
        "artifact_type": "source_trust_decay",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "counts": {"sources": 0, "critical": 0, "review": 0, "watch": 0, "healthy": 0},
        "missing_required_tables": missing_required,
        "unknown_optional_signals": unknown_optional,
        "items": [],
    }


def _missing_optional(schema: dict[str, set[str]]) -> list[str]:
    return [
        name
        for name in ("knowledge", "content_knowledge_links", "reply_knowledge_links")
        if name not in schema
    ]


def _column_expr(columns: set[str], column: str) -> str:
    return column if column in columns else f"NULL AS {column}"


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    return _normalize_datetime(parsed)


def _max_timestamp(*values: Any) -> str | None:
    pairs = [
        (parsed, _clean(value))
        for value in values
        if (parsed := _parse_datetime(value)) is not None
    ]
    if not pairs:
        return None
    pairs.sort(key=lambda pair: pair[0])
    return pairs[-1][1]


def _age_days(value: Any, now: datetime) -> float | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    return max((now - parsed).total_seconds(), 0.0) / 86400


def _round_days(value: float | None) -> float | None:
    return None if value is None else round(value, 2)


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_identifier(value: Any) -> str:
    text = (_clean(value) or "").lower().strip()
    if text.startswith("@"):
        text = text[1:]
    if text.startswith("http://") or text.startswith("https://"):
        text = _host(text)
    return text.rstrip("/")


def _host(value: Any) -> str:
    text = _clean(value) or ""
    if not text:
        return ""
    parsed = urlparse(text if "://" in text else f"https://{text}")
    return (parsed.netloc or parsed.path).lower().removeprefix("www.").split("/")[0]


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "active"}:
        return True
    if text in {"0", "false", "no", "inactive"}:
        return False
    return None


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _status(score: int) -> str:
    if score >= 80:
        return "critical"
    if score >= 50:
        return "review"
    if score >= 30:
        return "watch"
    return "healthy"


def _recommendation(status: str, drivers: list[str]) -> str:
    driver_set = set(drivers)
    if driver_set & {"quarantined_or_inactive", "quarantined_fetch", "repeated_failures"}:
        return "inspect health and keep source out of retrieval until fetches recover"
    if status == "review":
        return "review source freshness and refresh or replace stale knowledge"
    if status == "watch":
        return "monitor source and refresh before planned reuse"
    return "keep source active"


def _sort_key(item: SourceTrustDecayItem) -> tuple[int, int, int, str, str, int]:
    status_rank = {"critical": 0, "review": 1, "watch": 2, "healthy": 3}[item.status]
    health_rank = 0 if set(item.drivers) & {
        "quarantined_or_inactive",
        "quarantined_fetch",
        "last_fetch_failed",
        "repeated_failures",
    } else 1
    return (status_rank, health_rank, -item.decay_score, item.source_type, item.identifier, item.id)


def _source_label(item: dict[str, Any]) -> str:
    identifier = str(item.get("identifier") or "")
    if item.get("source_type") == "x_account" and identifier and not identifier.startswith("@"):
        return f"@{identifier}"
    return identifier or "-"


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"
