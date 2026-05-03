"""Detect license metadata conflicts between knowledge and curated sources."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any
from urllib.parse import urlparse


DEFAULT_LIMIT = 100
LICENSE_ALL = "all"
KNOWN_LICENSES = {"open", "attribution_required", "restricted"}
KNOWLEDGE_SOURCE_TYPES = {
    "curated_x": "x_account",
    "curated_article": "blog",
    "curated_newsletter": "newsletter",
}


@dataclass(frozen=True)
class SourceLicenseConflictFinding:
    """One inconsistent knowledge/curated source license pair."""

    severity: str
    reason: str
    match_type: str
    knowledge_id: int
    curated_source_id: int
    source_url: str | None
    identifier: str | None
    knowledge_license: str
    curated_license: str
    knowledge_attribution_required: bool
    curated_attribution_required: bool
    recommended_correction: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SourceLicenseConflictReport:
    """Read-only source license conflict report."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    findings: tuple[SourceLicenseConflictFinding, ...]
    missing_required_tables: tuple[str, ...] = ()
    missing_required_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def finding_count(self) -> int:
        return len(self.findings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "totals": dict(sorted(self.totals.items())),
            "finding_count": self.finding_count,
            "findings": [finding.to_dict() for finding in self.findings],
            "missing_required_tables": list(self.missing_required_tables),
            "missing_required_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_required_columns or {}).items())
            },
        }


def build_source_license_conflict_report(
    db_or_conn: Any,
    *,
    license_filter: str = LICENSE_ALL,
    source_type: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> SourceLicenseConflictReport:
    """Compare knowledge rows against curated sources and report license conflicts."""
    normalized_license_filter = _normalize_license(license_filter)
    if normalized_license_filter != LICENSE_ALL and normalized_license_filter not in KNOWN_LICENSES:
        raise ValueError("license must be open, attribution_required, restricted, or all")
    if source_type is not None and not source_type.strip():
        raise ValueError("source_type must not be blank")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    filters = {
        "license": normalized_license_filter,
        "source_type": source_type.strip() if source_type else None,
        "limit": limit,
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return SourceLicenseConflictReport(
            artifact_type="source_license_conflicts",
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals=_totals(()),
            findings=(),
            missing_required_tables=missing_tables,
            missing_required_columns=missing_columns,
        )

    sources = _load_curated_sources(
        conn,
        license_filter=normalized_license_filter,
        source_type=filters["source_type"],
    )
    source_index = _SourceIndex(sources)
    findings: list[SourceLicenseConflictFinding] = []
    seen_pairs: set[tuple[int, int]] = set()
    for row in _load_knowledge_rows(conn):
        matches = source_index.matches(row)
        for source, match_type in matches:
            pair = (int(row["id"]), int(source["id"]))
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            finding = _classify_conflict(row, source, match_type)
            if finding is not None:
                findings.append(finding)
                if len(findings) >= limit:
                    break
        if len(findings) >= limit:
            break

    return SourceLicenseConflictReport(
        artifact_type="source_license_conflicts",
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(tuple(findings)),
        findings=tuple(findings),
    )


def format_source_license_conflict_json(report: SourceLicenseConflictReport) -> str:
    """Serialize the conflict report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_source_license_conflict_text(report: SourceLicenseConflictReport) -> str:
    """Render source license conflicts for terminal review."""
    lines = [
        "Source License Conflicts",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"license={report.filters['license']} "
            f"source_type={report.filters['source_type'] or 'all'} "
            f"limit={report.filters['limit']}"
        ),
        (
            "Totals: "
            f"findings={report.totals['finding_count']} "
            f"high={report.totals['high_count']} "
            f"medium={report.totals['medium_count']} "
            f"low={report.totals['low_count']}"
        ),
    ]
    if report.missing_required_tables:
        lines.append("Missing required tables: " + ", ".join(report.missing_required_tables))
    if report.missing_required_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_required_columns.items())
        )
        lines.append("Missing required columns: " + missing)
    if not report.findings:
        lines.append("No source license conflicts found.")
        return "\n".join(lines)

    lines.append("Findings:")
    for finding in report.findings:
        lines.append(
            f"- severity={finding.severity} reason={finding.reason} "
            f"match={finding.match_type} knowledge_id={finding.knowledge_id} "
            f"curated_source_id={finding.curated_source_id} "
            f"identifier={finding.identifier or '-'} "
            f"knowledge_license={finding.knowledge_license} "
            f"curated_license={finding.curated_license} "
            f"url={finding.source_url or '-'}"
        )
        lines.append(f"  correction={finding.recommended_correction}")
    return "\n".join(lines)


class _SourceIndex:
    def __init__(self, sources: list[dict[str, Any]]) -> None:
        self.by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self.by_domain: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for source in sources:
            source_type = _clean(source.get("source_type")) or ""
            identifier = _normalize_identifier(source.get("identifier"))
            if identifier:
                self.by_key.setdefault((source_type, identifier), []).append(source)
            for value in (
                source.get("identifier"),
                source.get("feed_url"),
                source.get("canonical_url"),
            ):
                host = _normalize_host(value)
                if host:
                    self.by_domain.setdefault((source_type, host), []).append(source)

    def matches(self, row: dict[str, Any]) -> list[tuple[dict[str, Any], str]]:
        curated_type = KNOWLEDGE_SOURCE_TYPES.get(_clean(row.get("source_type")) or "")
        if not curated_type:
            return []
        candidates: list[tuple[dict[str, Any], str]] = []
        for value in (row.get("source_id"), row.get("author")):
            identifier = _normalize_identifier(value)
            if identifier:
                candidates.extend(
                    (source, "identifier")
                    for source in self.by_key.get((curated_type, identifier), [])
                )
        for value in (row.get("source_url"), row.get("source_id")):
            host = _normalize_host(value)
            if host:
                candidates.extend(
                    (source, "domain")
                    for source in self.by_domain.get((curated_type, host), [])
                )
        return candidates


def _classify_conflict(
    row: dict[str, Any],
    source: dict[str, Any],
    match_type: str,
) -> SourceLicenseConflictFinding | None:
    knowledge_license = _normalize_license(row.get("license")) or "attribution_required"
    curated_license = _normalize_license(source.get("license")) or "attribution_required"
    knowledge_attr = _knowledge_attribution_required(row, knowledge_license)
    curated_attr = _curated_attribution_required(curated_license)

    reason: str | None = None
    severity = "low"
    if curated_license == "restricted" and (
        knowledge_license != "restricted" or not knowledge_attr
    ):
        severity = "high"
        reason = "restricted_curated_source_marked_reusable"
    elif curated_license == "attribution_required" and (
        knowledge_license == "open" or not knowledge_attr
    ):
        severity = "medium"
        reason = "missing_required_attribution"
    elif curated_license == "open" and (
        knowledge_license != "open" or knowledge_attr != curated_attr
    ):
        severity = "low"
        reason = "knowledge_more_restrictive_than_curated_source"
    elif knowledge_license != curated_license or knowledge_attr != curated_attr:
        severity = "medium"
        reason = "license_metadata_mismatch"

    if reason is None:
        return None

    return SourceLicenseConflictFinding(
        severity=severity,
        reason=reason,
        match_type=match_type,
        knowledge_id=int(row["id"]),
        curated_source_id=int(source["id"]),
        source_url=_clean(row.get("source_url")),
        identifier=_clean(source.get("identifier")),
        knowledge_license=knowledge_license,
        curated_license=curated_license,
        knowledge_attribution_required=knowledge_attr,
        curated_attribution_required=curated_attr,
        recommended_correction=_recommended_correction(curated_license, curated_attr),
    )


def _load_curated_sources(
    conn: sqlite3.Connection,
    *,
    license_filter: str,
    source_type: str | None,
) -> list[dict[str, Any]]:
    columns = _schema(conn)["curated_sources"]
    filters: list[str] = []
    params: list[Any] = []
    if license_filter != LICENSE_ALL:
        filters.append("LOWER(COALESCE(NULLIF(TRIM(license), ''), 'attribution_required')) = ?")
        params.append(license_filter)
    if source_type is not None:
        filters.append("source_type = ?")
        params.append(source_type)
    where = "WHERE " + " AND ".join(filters) if filters else ""
    rows = conn.execute(
        f"""SELECT id, source_type, identifier, license,
                  {_column_expr(columns, "feed_url")},
                  {_column_expr(columns, "canonical_url")}
            FROM curated_sources
            {where}
            ORDER BY source_type ASC, identifier ASC, id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _load_knowledge_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    columns = _schema(conn)["knowledge"]
    rows = conn.execute(
        f"""SELECT id, source_type, source_id,
                  {_column_expr(columns, "source_url")},
                  {_column_expr(columns, "author")},
                  license, attribution_required
           FROM knowledge
           ORDER BY id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "curated_sources": (
            "id",
            "source_type",
            "identifier",
            "license",
        ),
        "knowledge": (
            "id",
            "source_type",
            "source_id",
            "license",
            "attribution_required",
        ),
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns: dict[str, tuple[str, ...]] = {}
    for table, columns in required.items():
        if table not in schema:
            continue
        absent = tuple(column for column in columns if column not in schema[table])
        if absent:
            missing_columns[table] = absent
    return missing_tables, missing_columns


def _column_expr(columns: set[str], column: str) -> str:
    if column in columns:
        return column
    return f"NULL AS {column}"


def _totals(findings: tuple[SourceLicenseConflictFinding, ...]) -> dict[str, int]:
    by_severity = Counter(finding.severity for finding in findings)
    return {
        "finding_count": len(findings),
        "high_count": by_severity.get("high", 0),
        "medium_count": by_severity.get("medium", 0),
        "low_count": by_severity.get("low", 0),
    }


def _recommended_correction(curated_license: str, curated_attr: bool) -> str:
    attr_value = 1 if curated_attr else 0
    return (
        "Update knowledge metadata to "
        f"license='{curated_license}' and attribution_required={attr_value}."
    )


def _knowledge_attribution_required(row: dict[str, Any], license_value: str) -> bool:
    raw = row.get("attribution_required")
    if raw is None:
        return license_value != "open"
    return bool(raw)


def _curated_attribution_required(license_value: str) -> bool:
    return license_value in {"attribution_required", "restricted"}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[str(table)] = {
            str(column[1]) for column in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
    return schema


def _normalize_license(value: Any) -> str:
    return (_clean(value) or "").casefold()


def _normalize_identifier(value: Any) -> str:
    text = (_clean(value) or "").casefold()
    if text.startswith("@"):
        text = text[1:]
    if text.startswith("www."):
        text = text[4:]
    return text.rstrip("/")


def _normalize_host(value: Any) -> str:
    text = _clean(value) or ""
    if not text:
        return ""
    parsed = urlparse(text if "://" in text else f"https://{text}")
    host = parsed.netloc or parsed.path.split("/", 1)[0]
    return _normalize_identifier(host)


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
