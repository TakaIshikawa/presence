"""Report curated sources at risk of paywall or access gating."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Mapping
from urllib.parse import urlparse


DEFAULT_DAYS = 30
DEFAULT_MIN_CONFIDENCE = 0.5
BLOCKED_STATUSES = {401, 402, 403, 407, 423, 429, 451}
PAYWALL_TERMS = (
    "paywall",
    "subscriber",
    "subscription required",
    "members only",
    "premium",
    "metered",
)
LOGIN_TERMS = ("login required", "sign in", "signin", "authentication required", "unauthorized")


@dataclass(frozen=True)
class CuratedSourcePaywallRisk:
    source_id: int | None
    identifier: str
    url: str | None
    source_type: str
    risk_type: str
    confidence: float
    evidence: str
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CuratedSourcePaywallRiskReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    risks: tuple[CuratedSourcePaywallRisk, ...]
    empty_state: dict[str, Any]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "curated_source_paywall_risk",
            "empty_state": dict(self.empty_state),
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "risks": [risk.to_dict() for risk in self.risks],
            "totals": dict(self.totals),
        }


def build_curated_source_paywall_risk_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    source_type: str | None = None,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    now: datetime | None = None,
) -> CuratedSourcePaywallRiskReport:
    """Build a conservative access-risk report for curated knowledge sources."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_confidence < 0 or min_confidence > 1:
        raise ValueError("min_confidence must be between 0 and 1")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "lookback_start": cutoff.isoformat(),
        "min_confidence": min_confidence,
        "source_type": source_type,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = tuple(
        table for table in ("curated_sources", "knowledge") if table not in schema
    )
    missing_columns = _missing_columns(schema)
    if missing_tables:
        return _report(
            generated_at=generated_at,
            filters=filters,
            risks=(),
            source_count=0,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    sources = _load_sources(conn, schema["curated_sources"], source_type=source_type)
    knowledge = _load_knowledge(conn, schema["knowledge"], cutoff=cutoff.isoformat())
    risks = [
        risk
        for source in sources
        for risk in _source_risks(source, knowledge)
        if risk.confidence >= min_confidence
    ]
    risks.sort(key=_risk_sort_key)
    return _report(
        generated_at=generated_at,
        filters=filters,
        risks=tuple(risks),
        source_count=len(sources),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_curated_source_paywall_risk_json(report: CuratedSourcePaywallRiskReport) -> str:
    """Render curated source paywall risk as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_curated_source_paywall_risk_text(report: CuratedSourcePaywallRiskReport) -> str:
    """Render curated source paywall risk for terminal review."""
    lines = [
        "Curated Source Paywall Risk",
        (
            f"Window: {report.filters['days']} days; "
            f"source_type={report.filters.get('source_type') or 'all'}; "
            f"min_confidence={report.filters['min_confidence']}"
        ),
        (
            f"Sources scanned: {report.totals['sources_scanned']}; "
            f"risks={report.totals['risk_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    lines.append("")

    if not report.risks:
        lines.append(report.empty_state["message"])
        return "\n".join(lines)

    for risk in report.risks:
        lines.append(
            "- "
            f"{risk.risk_type} source={risk.source_id or '-'} "
            f"type={risk.source_type} confidence={risk.confidence:.2f} "
            f"url={risk.url or '-'} evidence={risk.evidence}"
        )
        lines.append(f"  action={risk.recommended_action}")
    return "\n".join(lines)


def _source_risks(
    source: Mapping[str, Any],
    knowledge_rows: list[dict[str, Any]],
) -> list[CuratedSourcePaywallRisk]:
    metadata = _metadata(source.get("metadata"))
    flattened = _flatten_metadata(metadata)
    text = " ".join(
        str(value or "")
        for value in (
            source.get("identifier"),
            source.get("name"),
            source.get("feed_url"),
            source.get("canonical_url"),
            source.get("homepage_url"),
            source.get("link_title"),
            source.get("site_name"),
            source.get("last_fetch_status"),
            source.get("last_error"),
            flattened,
        )
    ).casefold()
    source_url = _source_url(source)
    risks: list[CuratedSourcePaywallRisk] = []

    status = _status_code(source, metadata)
    if status in BLOCKED_STATUSES:
        risks.append(
            _risk(
                source,
                "blocked_status",
                0.95,
                f"HTTP/status code {status}",
                "Replace, refresh credentials for, or exclude this source before citation.",
            )
        )
    if _contains_any(text, LOGIN_TERMS) or _bool_metadata(metadata, ("login_required", "requires_login")):
        risks.append(
            _risk(
                source,
                "login_required",
                0.9,
                "metadata or fetch text indicates login-gated access",
                "Verify access without an authenticated session and avoid citing gated excerpts.",
            )
        )
    if _contains_any(text, PAYWALL_TERMS) or _paywall_domain(source_url):
        risks.append(
            _risk(
                source,
                "paywall_keyword",
                0.78,
                "source URL or metadata contains paywall/subscriber language",
                "Prefer an accessible canonical source or capture a permissible excerpt before citation.",
            )
        )
    if not _has_recent_excerpt(source, knowledge_rows):
        risks.append(
            _risk(
                source,
                "missing_excerpt",
                0.62,
                "no recent accessible knowledge excerpt found for this source",
                "Recrawl the source and store an accessible excerpt before generated content cites it.",
            )
        )

    deduped: dict[str, CuratedSourcePaywallRisk] = {}
    for risk in risks:
        current = deduped.get(risk.risk_type)
        if current is None or risk.confidence > current.confidence:
            deduped[risk.risk_type] = risk
    return list(deduped.values())


def _risk(
    source: Mapping[str, Any],
    risk_type: str,
    confidence: float,
    evidence: str,
    action: str,
) -> CuratedSourcePaywallRisk:
    return CuratedSourcePaywallRisk(
        source_id=_int_or_none(source.get("id")),
        identifier=_clean(source.get("identifier")) or "",
        url=_source_url(source),
        source_type=_clean(source.get("source_type")) or "unknown",
        risk_type=risk_type,
        confidence=round(confidence, 2),
        evidence=evidence,
        recommended_action=action,
    )


def _has_recent_excerpt(
    source: Mapping[str, Any],
    knowledge_rows: list[dict[str, Any]],
) -> bool:
    for row in knowledge_rows:
        if not _knowledge_matches_source(row, source):
            continue
        if _clean(row.get("content")) or _clean(row.get("insight")) or _metadata(row.get("metadata")).get("excerpt"):
            return True
    return False


def _load_sources(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    source_type: str | None,
) -> list[dict[str, Any]]:
    wanted = (
        "id",
        "source_type",
        "identifier",
        "name",
        "feed_url",
        "canonical_url",
        "homepage_url",
        "link_title",
        "site_name",
        "status",
        "last_fetch_status",
        "last_error",
        "metadata",
        "created_at",
    )
    where = []
    params: list[Any] = []
    if source_type:
        where.append("source_type = ?")
        params.append(source_type)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""SELECT {', '.join(_column_expr(columns, column) for column in wanted)}
            FROM curated_sources
            {where_sql}
            ORDER BY source_type ASC, identifier ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _load_knowledge(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: str,
) -> list[dict[str, Any]]:
    wanted = (
        "id",
        "source_type",
        "source_id",
        "source_url",
        "author",
        "content",
        "insight",
        "published_at",
        "ingested_at",
        "created_at",
        "metadata",
    )
    time_columns = [
        column for column in ("ingested_at", "created_at", "published_at") if column in columns
    ]
    if len(time_columns) > 1:
        time_expr = "COALESCE(" + ", ".join(time_columns) + ")"
    elif time_columns:
        time_expr = time_columns[0]
    else:
        time_expr = ""
    where = f"WHERE {time_expr} >= ?" if time_expr else ""
    params = [cutoff] if where else []
    rows = conn.execute(
        f"""SELECT {', '.join(_column_expr(columns, column) for column in wanted)}
            FROM knowledge
            {where}""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _knowledge_matches_source(row: Mapping[str, Any], source: Mapping[str, Any]) -> bool:
    source_type = _clean(source.get("source_type")) or ""
    expected = {
        "x_account": "curated_x",
        "blog": "curated_article",
        "newsletter": "curated_newsletter",
    }.get(source_type)
    if expected and row.get("source_type") != expected:
        return False

    identifier = (_clean(source.get("identifier")) or "").lstrip("@").casefold()
    source_id = (_clean(row.get("source_id")) or "").lstrip("@").casefold()
    author = (_clean(row.get("author")) or "").lstrip("@").casefold()
    if identifier and identifier in {source_id, author}:
        return True
    source_hosts = {
        host
        for host in (
            _host(source.get("identifier")),
            _host(source.get("feed_url")),
            _host(source.get("canonical_url")),
            _host(source.get("homepage_url")),
        )
        if host
    }
    return bool(_host(row.get("source_url")) in source_hosts)


def _report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    risks: tuple[CuratedSourcePaywallRisk, ...],
    source_count: int,
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> CuratedSourcePaywallRiskReport:
    by_type: dict[str, int] = {}
    for risk in risks:
        by_type[risk.risk_type] = by_type.get(risk.risk_type, 0) + 1
    return CuratedSourcePaywallRiskReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "by_risk_type": dict(sorted(by_type.items())),
            "risk_count": len(risks),
            "sources_scanned": source_count,
        },
        risks=risks,
        empty_state={
            "is_empty": not risks,
            "message": (
                "No curated source paywall risks found."
                if not missing_tables
                else "Curated source or knowledge schema is unavailable."
            ),
        },
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    expected = {
        "curated_sources": (
            "id",
            "source_type",
            "identifier",
            "feed_url",
            "canonical_url",
            "last_fetch_status",
            "last_error",
        ),
        "knowledge": ("source_type", "source_id", "source_url", "content", "insight"),
    }
    return {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema
    }


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _flatten_metadata(value: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for item in value.values():
        if isinstance(item, Mapping):
            parts.append(_flatten_metadata(item))
        elif isinstance(item, list):
            parts.extend(str(part) for part in item)
        else:
            parts.append(str(item))
    return " ".join(parts)


def _status_code(source: Mapping[str, Any], metadata: Mapping[str, Any]) -> int | None:
    for key in ("status_code", "http_status", "last_status_code", "last_http_status"):
        parsed = _int_or_none(source.get(key))
        if parsed is not None:
            return parsed
    for key in ("status_code", "http_status", "last_status_code", "last_http_status"):
        parsed = _int_or_none(metadata.get(key))
        if parsed is not None:
            return parsed
    link_metadata = metadata.get("link_metadata")
    if isinstance(link_metadata, Mapping):
        for key in ("status_code", "http_status"):
            parsed = _int_or_none(link_metadata.get(key))
            if parsed is not None:
                return parsed
    return None


def _bool_metadata(metadata: Mapping[str, Any], keys: tuple[str, ...]) -> bool:
    for key in keys:
        value = metadata.get(key)
        if isinstance(value, bool):
            return value
        if str(value).casefold() in {"true", "yes", "1"}:
            return True
    return False


def _source_url(source: Mapping[str, Any]) -> str | None:
    for key in ("canonical_url", "feed_url", "homepage_url", "identifier"):
        value = _clean(source.get(key))
        if not value:
            continue
        if "://" in value:
            return value
        if key == "identifier" and source.get("source_type") == "x_account":
            return f"https://x.com/{value.lstrip('@')}"
        if "." in value:
            return f"https://{value}"
    return None


def _paywall_domain(url: str | None) -> bool:
    host = _host(url)
    return any(token in host for token in ("substack.com", "medium.com", "wsj.com", "ft.com"))


def _contains_any(text: str, terms: tuple[str, ...]) -> bool:
    return any(term in text for term in terms)


def _risk_sort_key(risk: CuratedSourcePaywallRisk) -> tuple[float, str, str]:
    return (-risk.confidence, risk.risk_type, risk.identifier.casefold())


def _column_expr(columns: set[str], column: str) -> str:
    return column if column in columns else f"NULL AS {column}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _host(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    parsed = urlparse(text if "://" in text else f"https://{text}")
    host = (parsed.netloc or parsed.path.split("/", 1)[0]).casefold()
    return host[4:] if host.startswith("www.") else host


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
