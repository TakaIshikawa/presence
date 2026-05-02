"""Report whether newsletter introductions match clicked links."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from html import unescape
import json
import re
import sqlite3
from typing import Any, Mapping


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 25
DEFAULT_MIN_CLICKS = 3
LOW_ALIGNMENT_THRESHOLD = 0.2
TOP_LINK_COUNT = 3
_TIMESTAMP_COLUMNS = ("sent_at", "created_at")
_INTRO_KEYS = (
    "intro",
    "introduction",
    "opening",
    "preview",
    "preview_text",
    "preheader",
    "summary",
    "description",
)
_TAG_RE = re.compile(r"<[^>]+>")
_SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL)
_WORD_RE = re.compile(r"[A-Za-z0-9]+(?:[-'][A-Za-z0-9]+)?")
_STOPWORDS = {
    "about",
    "after",
    "again",
    "also",
    "and",
    "are",
    "but",
    "can",
    "com",
    "for",
    "from",
    "has",
    "have",
    "here",
    "how",
    "https",
    "into",
    "its",
    "more",
    "not",
    "now",
    "our",
    "over",
    "read",
    "that",
    "the",
    "this",
    "through",
    "today",
    "was",
    "what",
    "when",
    "where",
    "with",
    "www",
    "you",
    "your",
}


@dataclass(frozen=True)
class NewsletterIntroClickedLink:
    """One top clicked link scored against the intro."""

    link_url: str
    raw_url: str
    content_id: int | None
    source_kind: str
    clicks: int
    unique_clicks: int | None
    alignment_score: float
    overlap_terms: tuple[str, ...]
    link_terms: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["overlap_terms"] = list(self.overlap_terms)
        payload["link_terms"] = list(self.link_terms)
        return payload


@dataclass(frozen=True)
class NewsletterIntroClickAlignmentFinding:
    """Intro-to-click alignment for one sent newsletter."""

    newsletter_send_id: int
    issue_id: str
    subject: str
    timestamp: str
    intro_source: str
    intro_text: str
    intro_terms: tuple[str, ...]
    total_clicks: int
    ranked_clicks: int
    alignment_score: float
    warnings: tuple[str, ...]
    reasons: tuple[str, ...]
    top_links: tuple[NewsletterIntroClickedLink, ...]

    @property
    def is_flagged(self) -> bool:
        return bool(self.warnings)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["intro_terms"] = list(self.intro_terms)
        payload["is_flagged"] = self.is_flagged
        payload["reasons"] = list(self.reasons)
        payload["top_links"] = [link.to_dict() for link in self.top_links]
        payload["warnings"] = list(self.warnings)
        return payload


@dataclass(frozen=True)
class NewsletterIntroClickAlignmentReport:
    """Newsletter intro click-alignment report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    findings: tuple[NewsletterIntroClickAlignmentFinding, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_intro_click_alignment",
            "filters": dict(self.filters),
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_newsletter_intro_click_alignment_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    min_clicks: int = DEFAULT_MIN_CLICKS,
    now: datetime | None = None,
) -> NewsletterIntroClickAlignmentReport:
    """Load recent sent newsletters and rank intro-to-click mismatch risk."""
    days = _positive_int(days, "days")
    limit = _positive_int(limit, "limit")
    min_clicks = _positive_int(min_clicks, "min_clicks")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {
        "days": days,
        "limit": limit,
        "low_alignment_threshold": LOW_ALIGNMENT_THRESHOLD,
        "min_clicks": min_clicks,
        "top_link_count": TOP_LINK_COUNT,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    rows = _load_rows(conn, schema, days=days, limit=limit, now=generated_at)
    return _build_report_from_rows(
        rows,
        generated_at=generated_at,
        filters=filters,
        min_clicks=min_clicks,
        missing_tables=(),
        missing_columns={},
    )


def format_newsletter_intro_click_alignment_json(
    report: NewsletterIntroClickAlignmentReport,
) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_intro_click_alignment_text(
    report: NewsletterIntroClickAlignmentReport,
) -> str:
    """Render a concise human-readable report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Newsletter Intro Click Alignment",
        f"Generated: {report.generated_at}",
        (
            f"Window: days={filters['days']} limit={filters['limit']} "
            f"min_clicks={filters['min_clicks']}"
        ),
        (
            f"Totals: sends={totals['sends_scanned']} "
            f"with_intro={totals['sends_with_intro']} "
            f"with_clicks={totals['sends_with_clicks']} "
            f"included={totals['included_send_count']} "
            f"flagged={totals['flagged_count']}"
        ),
    ]
    if totals["excluded_below_min_clicks_count"]:
        lines.append(
            f"Excluded below min clicks: {totals['excluded_below_min_clicks_count']}"
        )
    if totals["malformed_metadata_count"]:
        lines.append(f"Malformed metadata rows: {totals['malformed_metadata_count']}")
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + missing)
    lines.append("")

    if not report.findings:
        if report.missing_tables or report.missing_columns:
            lines.append(
                "No newsletter intro click-alignment findings available until "
                "schema gaps are resolved."
            )
        else:
            lines.append("No recent sent newsletters met the intro and click thresholds.")
        return "\n".join(lines)

    lines.append("Ranked issues:")
    for finding in report.findings:
        warnings = ", ".join(finding.warnings) if finding.warnings else "aligned"
        reasons = "; ".join(finding.reasons) if finding.reasons else "-"
        lines.append(
            f"- {finding.issue_id or finding.newsletter_send_id} "
            f"subject={finding.subject or '-'} clicks={finding.total_clicks} "
            f"score={finding.alignment_score:.2f} warnings={warnings} reasons={reasons}"
        )
        for link in finding.top_links:
            overlap = ",".join(link.overlap_terms) if link.overlap_terms else "-"
            lines.append(
                f"  - clicks={link.clicks} score={link.alignment_score:.2f} "
                f"overlap={overlap} url={link.link_url}"
            )
    return "\n".join(lines)


def _build_report_from_rows(
    rows: list[dict[str, Any]],
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    min_clicks: int,
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> NewsletterIntroClickAlignmentReport:
    sends: dict[int, dict[str, Any]] = {}
    malformed_send_ids: set[int] = set()
    for row in rows:
        send_id = int(row.get("newsletter_send_id") or 0)
        send = sends.setdefault(
            send_id,
            {
                "newsletter_send_id": send_id,
                "issue_id": str(row.get("issue_id") or ""),
                "subject": str(row.get("subject") or ""),
                "timestamp": str(row.get("timestamp") or ""),
                "metadata": row.get("metadata"),
                "links": [],
            },
        )
        if row.get("link_url"):
            send["links"].append(row)

    findings: list[NewsletterIntroClickAlignmentFinding] = []
    sends_with_intro = 0
    sends_with_clicks = 0
    excluded_below_min = 0
    total_clicks_all = 0

    for send in sends.values():
        metadata, malformed = _metadata_object(send.get("metadata"))
        if malformed:
            malformed_send_ids.add(int(send["newsletter_send_id"]))
        intro_text, intro_source = _intro_text(metadata)
        intro_terms = tuple(_terms(intro_text))
        if intro_terms:
            sends_with_intro += 1

        links = [_link_with_clicks(row) for row in send["links"]]
        links = [link for link in links if link["clicks"] > 0]
        total_clicks = sum(int(link["clicks"]) for link in links)
        total_clicks_all += total_clicks
        if total_clicks:
            sends_with_clicks += 1
        if total_clicks < min_clicks:
            if total_clicks:
                excluded_below_min += 1
            continue

        ranked_links = sorted(
            links,
            key=lambda link: (-int(link["clicks"]), str(link.get("link_url") or "")),
        )[:TOP_LINK_COUNT]
        findings.append(
            _finding(
                send,
                intro_text=intro_text,
                intro_source=intro_source,
                intro_terms=intro_terms,
                links=ranked_links,
                total_clicks=total_clicks,
            )
        )

    ranked_findings = tuple(
        sorted(
            findings,
            key=lambda finding: (
                finding.alignment_score,
                -finding.total_clicks,
                finding.timestamp,
                finding.newsletter_send_id,
            ),
        )[: filters["limit"]]
    )

    return NewsletterIntroClickAlignmentReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "sends_scanned": len(sends),
            "sends_with_intro": sends_with_intro,
            "sends_with_clicks": sends_with_clicks,
            "included_send_count": len(findings),
            "excluded_below_min_clicks_count": excluded_below_min,
            "malformed_metadata_count": len(malformed_send_ids),
            "finding_count": len(ranked_findings),
            "flagged_count": sum(1 for finding in ranked_findings if finding.is_flagged),
            "total_clicks": total_clicks_all,
        },
        findings=ranked_findings,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _finding(
    send: Mapping[str, Any],
    *,
    intro_text: str,
    intro_source: str,
    intro_terms: tuple[str, ...],
    links: list[dict[str, Any]],
    total_clicks: int,
) -> NewsletterIntroClickAlignmentFinding:
    scored_links = tuple(_scored_link(link, intro_terms) for link in links)
    ranked_clicks = sum(link.clicks for link in scored_links)
    if ranked_clicks:
        score = round(
            sum(link.alignment_score * link.clicks for link in scored_links) / ranked_clicks,
            2,
        )
    else:
        score = 0.0

    warnings: list[str] = []
    reasons: list[str] = []
    if not intro_terms:
        warnings.append("missing_intro_terms")
        reasons.append("intro metadata did not contain usable terms")
    low_links = [
        link for link in scored_links if link.alignment_score < LOW_ALIGNMENT_THRESHOLD
    ]
    if score < LOW_ALIGNMENT_THRESHOLD:
        warnings.append("low_intro_click_alignment")
        reasons.append(
            f"weighted top-link overlap {score:.2f} is below {LOW_ALIGNMENT_THRESHOLD:.2f}"
        )
    if low_links:
        top = low_links[0]
        reasons.append(f"top unrelated clicked link: {top.link_url}")

    return NewsletterIntroClickAlignmentFinding(
        newsletter_send_id=int(send.get("newsletter_send_id") or 0),
        issue_id=str(send.get("issue_id") or ""),
        subject=str(send.get("subject") or ""),
        timestamp=str(send.get("timestamp") or ""),
        intro_source=intro_source,
        intro_text=intro_text,
        intro_terms=intro_terms,
        total_clicks=total_clicks,
        ranked_clicks=ranked_clicks,
        alignment_score=score,
        warnings=tuple(dict.fromkeys(warnings)),
        reasons=tuple(dict.fromkeys(reasons)),
        top_links=scored_links,
    )


def _scored_link(
    row: Mapping[str, Any],
    intro_terms: tuple[str, ...],
) -> NewsletterIntroClickedLink:
    link_terms = tuple(_terms(_link_text(row)))
    intro_set = set(intro_terms)
    link_set = set(link_terms)
    overlap = tuple(sorted(intro_set & link_set))
    denominator = min(len(intro_set), len(link_set))
    score = round(len(overlap) / denominator, 2) if denominator else 0.0
    return NewsletterIntroClickedLink(
        link_url=str(row.get("link_url") or ""),
        raw_url=str(row.get("raw_url") or ""),
        content_id=_optional_int(row.get("content_id")),
        source_kind=str(row.get("source_kind") or ""),
        clicks=int(row.get("clicks") or 0),
        unique_clicks=_optional_int(row.get("unique_clicks")),
        alignment_score=score,
        overlap_terms=overlap,
        link_terms=link_terms[:12],
    )


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    days: int,
    limit: int,
    now: datetime,
) -> list[dict[str, Any]]:
    send_columns = schema["newsletter_sends"]
    click_columns = schema["newsletter_link_clicks"]
    gc_columns = schema.get("generated_content", set())
    click_expr = (
        "COALESCE(nlc.unique_clicks, nlc.clicks)"
        if "unique_clicks" in click_columns
        else "nlc.clicks"
    )
    unique_clicks_expr = _column_expr(
        click_columns,
        "unique_clicks",
        "NULL",
        alias="nlc",
    )
    content_select = "NULL AS content_text, NULL AS content_type, NULL AS published_url"
    content_join = ""
    if (
        "generated_content" in schema
        and "content_id" in click_columns
        and {"id", "content"} <= gc_columns
    ):
        content_select = (
            f"{_column_expr(gc_columns, 'content', 'NULL', alias='gc')} AS content_text, "
            f"{_column_expr(gc_columns, 'content_type', 'NULL', alias='gc')} AS content_type, "
            f"{_column_expr(gc_columns, 'published_url', 'NULL', alias='gc')} AS published_url"
        )
        content_join = "LEFT JOIN generated_content gc ON gc.id = nlc.content_id"

    rows = conn.execute(
        f"""WITH recent_sends AS (
               SELECT
                   ns.id AS newsletter_send_id,
                   {_column_expr(send_columns, "issue_id", "''", alias="ns")} AS issue_id,
                   {_column_expr(send_columns, "subject", "''", alias="ns")} AS subject,
                   {_column_expr(send_columns, "status", "'sent'", alias="ns")} AS status,
                   ns.metadata AS metadata,
                   {_timestamp_expr(send_columns, alias="ns")} AS timestamp
               FROM newsletter_sends ns
               WHERE datetime({_timestamp_expr(send_columns, alias="ns")}) >= datetime(?, ?)
                 {_status_clause(send_columns)}
               ORDER BY datetime({_timestamp_expr(send_columns, alias="ns")}) DESC, ns.id DESC
               LIMIT ?
           ),
           latest_clicks AS (
               SELECT nlc.*
               FROM newsletter_link_clicks nlc
               WHERE nlc.id = (
                   SELECT latest.id
                   FROM newsletter_link_clicks latest
                   WHERE latest.newsletter_send_id = nlc.newsletter_send_id
                     AND latest.link_url = nlc.link_url
                   ORDER BY datetime(latest.fetched_at) DESC, latest.id DESC
                   LIMIT 1
               )
           )
           SELECT rs.newsletter_send_id, rs.issue_id, rs.subject, rs.status,
                  rs.metadata, rs.timestamp,
                  nlc.link_url,
                  {_column_expr(click_columns, "raw_url", "NULL", alias="nlc")} AS raw_url,
                  {_column_expr(click_columns, "content_id", "NULL", alias="nlc")} AS content_id,
                  {_column_expr(click_columns, "source_kind", "''", alias="nlc")} AS source_kind,
                  {click_expr} AS clicks,
                  {unique_clicks_expr} AS unique_clicks,
                  {content_select}
           FROM recent_sends rs
           LEFT JOIN latest_clicks nlc ON nlc.newsletter_send_id = rs.newsletter_send_id
           {content_join}
           ORDER BY datetime(rs.timestamp) DESC, rs.newsletter_send_id DESC,
                    clicks DESC, nlc.link_url ASC""",
        ((now.replace(microsecond=0)).isoformat(), f"-{days} days", limit),
    ).fetchall()
    return [dict(row) for row in rows]


def _metadata_object(raw_value: Any) -> tuple[Mapping[str, Any], bool]:
    if raw_value in (None, ""):
        return {}, False
    if isinstance(raw_value, Mapping):
        return raw_value, False
    try:
        parsed = json.loads(raw_value)
    except (TypeError, json.JSONDecodeError):
        return {}, True
    if not isinstance(parsed, Mapping):
        return {}, True
    return parsed, False


def _intro_text(metadata: Mapping[str, Any]) -> tuple[str, str]:
    found = _metadata_text(metadata)
    if found:
        key, value = found
        return value, f"newsletter_sends.metadata.{key}"
    return "", ""


def _metadata_text(value: Any, *, prefix: str = "") -> tuple[str, str] | None:
    if isinstance(value, Mapping):
        for key in _INTRO_KEYS:
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return (f"{prefix}.{key}".strip("."), _collapse_spaces(_strip_html(item)))
        for key, item in value.items():
            if isinstance(item, (Mapping, list, tuple)):
                found = _metadata_text(item, prefix=f"{prefix}.{key}".strip("."))
                if found:
                    return found
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            if isinstance(item, (Mapping, list, tuple)):
                found = _metadata_text(item, prefix=f"{prefix}.{index}".strip("."))
                if found:
                    return found
    return None


def _link_with_clicks(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload["clicks"] = int(payload.get("clicks") or 0)
    return payload


def _link_text(row: Mapping[str, Any]) -> str:
    return " ".join(
        str(row.get(key) or "")
        for key in (
            "link_url",
            "raw_url",
            "source_kind",
            "content_type",
            "published_url",
            "content_text",
        )
    )


def _terms(value: str) -> tuple[str, ...]:
    terms: list[str] = []
    seen: set[str] = set()
    for word in _WORD_RE.findall(_strip_html(value).lower().replace("_", " ")):
        term = word.strip("'").replace("'", "")
        if len(term) < 3 or term in _STOPWORDS or term.isdigit():
            continue
        if term not in seen:
            seen.add(term)
            terms.append(term)
    return tuple(terms)


def _strip_html(value: str) -> str:
    text = _SCRIPT_STYLE_RE.sub(" ", value or "")
    text = re.sub(r"(?i)<br\s*/?>|</p>|</div>|</li>|</h[1-6]>", " ", text)
    text = _TAG_RE.sub(" ", text)
    return unescape(text)


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required_tables = ("newsletter_sends", "newsletter_link_clicks")
    missing_tables = tuple(table for table in required_tables if table not in schema)
    if missing_tables:
        return missing_tables, {}

    missing_columns: dict[str, tuple[str, ...]] = {}
    send_columns = schema["newsletter_sends"]
    send_missing = {"id", "metadata"} - send_columns
    if not any(column in send_columns for column in _TIMESTAMP_COLUMNS):
        send_missing.add("sent_at")
    if send_missing:
        missing_columns["newsletter_sends"] = tuple(sorted(send_missing))

    click_columns = schema["newsletter_link_clicks"]
    click_missing = {"id", "newsletter_send_id", "link_url", "fetched_at"} - click_columns
    if not ({"clicks", "unique_clicks"} & click_columns):
        click_missing.add("clicks|unique_clicks")
    if click_missing:
        missing_columns["newsletter_link_clicks"] = tuple(sorted(click_missing))
    return (), missing_columns


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> NewsletterIntroClickAlignmentReport:
    return NewsletterIntroClickAlignmentReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "sends_scanned": 0,
            "sends_with_intro": 0,
            "sends_with_clicks": 0,
            "included_send_count": 0,
            "excluded_below_min_clicks_count": 0,
            "malformed_metadata_count": 0,
            "finding_count": 0,
            "flagged_count": 0,
            "total_clicks": 0,
        },
        findings=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[str(table)] = {column[1] for column in conn.execute(f"PRAGMA table_info({table})")}
    return schema


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str,
    *,
    alias: str | None = None,
) -> str:
    if column not in columns:
        return fallback
    return f"{alias}.{column}" if alias else column


def _timestamp_expr(columns: set[str], *, alias: str | None = None) -> str:
    prefix = f"{alias}." if alias else ""
    present = [column for column in _TIMESTAMP_COLUMNS if column in columns]
    if len(present) == 1:
        return f"{prefix}{present[0]}"
    return f"COALESCE({prefix}sent_at, {prefix}created_at)"


def _status_clause(columns: set[str]) -> str:
    if "status" not in columns:
        return ""
    return "AND LOWER(COALESCE(ns.status, 'sent')) = 'sent'"


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _positive_int(value: int, name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be positive") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _collapse_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()
