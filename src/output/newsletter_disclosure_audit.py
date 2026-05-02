"""Audit newsletter drafts for sponsorship language without disclosure."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any, Iterable, Sequence


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
SEVERITIES = ("high", "medium", "low")
SPONSORSHIP_TERMS = (
    "sponsored",
    "sponsor",
    "paid promotion",
    "paid partnership",
    "paid placement",
    "affiliate",
    "referral",
    "refer a friend",
    "partner",
    "partnership",
    "promo code",
)
HIGH_SEVERITY_TERMS = frozenset(
    {"sponsored", "sponsor", "paid promotion", "paid partnership", "paid placement"}
)
MEDIUM_SEVERITY_TERMS = frozenset(
    {"affiliate", "referral", "refer a friend", "partner", "partnership", "promo code"}
)
DISCLOSURE_TERMS = (
    "sponsored by",
    "this issue is sponsored",
    "paid partnership",
    "paid promotion",
    "paid placement",
    "affiliate link",
    "affiliate links",
    "we may earn a commission",
    "i may earn a commission",
    "referral link",
    "advertisement",
    "ad:",
    "sponsor disclosure",
    "partner disclosure",
)
TEXT_COLUMNS = ("subject", "body", "content", "html", "text", "markdown", "preview")
NEWSLETTER_VARIANT_MARKERS = ("newsletter", "email")


@dataclass(frozen=True)
class NewsletterDisclosureFinding:
    """One newsletter send with sponsorship language and no disclosure."""

    newsletter_send_id: int
    issue_id: str
    subject: str
    status: str
    sent_at: str
    severity: str
    matched_terms: tuple[str, ...]
    sources: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "newsletter_send_id": self.newsletter_send_id,
            "issue_id": self.issue_id,
            "subject": self.subject,
            "status": self.status,
            "sent_at": self.sent_at,
            "severity": self.severity,
            "matched_terms": list(self.matched_terms),
            "sources": list(self.sources),
        }


@dataclass(frozen=True)
class NewsletterDisclosureAuditReport:
    """Read-only newsletter sponsor disclosure audit."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    findings: tuple[NewsletterDisclosureFinding, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_disclosure_audit",
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "totals": dict(self.totals),
            "findings": [finding.to_dict() for finding in self.findings],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_newsletter_disclosure_audit_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    status: str | Sequence[str] | None = None,
    limit: int | None = DEFAULT_LIMIT,
    sponsorship_terms: Sequence[str] = SPONSORSHIP_TERMS,
    disclosure_terms: Sequence[str] = DISCLOSURE_TERMS,
    now: datetime | None = None,
) -> NewsletterDisclosureAuditReport:
    """Return newsletter sends that appear sponsored but lack disclosures."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit < 0:
        raise ValueError("limit must be non-negative")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    statuses = _normalise_statuses(status)
    filters = {
        "days": days,
        "sent_after": cutoff.isoformat(),
        "status": list(statuses),
        "limit": limit,
        "sponsorship_terms": list(sponsorship_terms),
        "disclosure_terms": list(disclosure_terms),
    }
    if "newsletter_sends" not in schema:
        return _empty_report(generated_at, filters, ("newsletter_sends",), {})

    required = {"id"}
    missing = tuple(sorted(required - schema["newsletter_sends"]))
    if missing:
        return _empty_report(
            generated_at,
            filters,
            (),
            {"newsletter_sends": missing},
        )

    sends = _load_sends(
        conn,
        schema,
        cutoff=cutoff,
        statuses=statuses,
        limit=limit,
    )
    variant_texts = _load_variant_texts(conn, schema, sends)
    findings = []
    for send in sends:
        send_id = int(send["id"])
        texts = _send_texts(send)
        texts.extend(variant_texts.get(send_id, ()))
        finding = _audit_send(
            send,
            texts,
            sponsorship_terms=sponsorship_terms,
            disclosure_terms=disclosure_terms,
        )
        if finding is not None:
            findings.append(finding)

    sorted_findings = tuple(
        sorted(
            findings,
            key=lambda item: (
                SEVERITIES.index(item.severity),
                item.sent_at,
                item.newsletter_send_id,
            ),
        )
    )
    return NewsletterDisclosureAuditReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "send_count": len(sends),
            "finding_count": len(sorted_findings),
            "severity_totals": _severity_counts(sorted_findings),
            "missing_tables": 0,
        },
        findings=sorted_findings,
    )


def format_newsletter_disclosure_audit_json(
    report: NewsletterDisclosureAuditReport,
) -> str:
    """Serialize the audit as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_disclosure_audit_text(
    report: NewsletterDisclosureAuditReport,
) -> str:
    """Render a compact human-readable disclosure audit."""
    severity_totals = report.totals.get("severity_totals", {})
    lines = [
        "Newsletter Disclosure Audit",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['sent_after']} -> {report.generated_at}",
        (
            "Filters: "
            f"status={','.join(report.filters['status']) or 'all'} "
            f"limit={report.filters['limit']}"
        ),
        (
            "Totals: "
            f"sends={report.totals['send_count']} "
            f"findings={report.totals['finding_count']} "
            + " ".join(
                f"{severity}={severity_totals.get(severity, 0)}"
                for severity in SEVERITIES
            )
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
        return "\n".join(lines)
    if report.missing_columns:
        details = ", ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + details)
        return "\n".join(lines)
    if not report.findings:
        lines.append("No newsletter disclosure gaps found.")
        return "\n".join(lines)

    for finding in report.findings:
        lines.append("")
        lines.append(
            f"Send {finding.newsletter_send_id} issue={finding.issue_id or '-'} "
            f"status={finding.status or '-'} sent_at={finding.sent_at or '-'} "
            f"severity={finding.severity}: {finding.subject}"
        )
        lines.append(f"  terms={','.join(finding.matched_terms)}")
        lines.append(f"  sources={','.join(finding.sources)}")
    return "\n".join(lines)


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> NewsletterDisclosureAuditReport:
    return NewsletterDisclosureAuditReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "send_count": 0,
            "finding_count": 0,
            "severity_totals": _empty_severity_counts(),
            "missing_tables": len(missing_tables),
        },
        findings=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _audit_send(
    send: dict[str, Any],
    texts: list[tuple[str, str]],
    *,
    sponsorship_terms: Sequence[str],
    disclosure_terms: Sequence[str],
) -> NewsletterDisclosureFinding | None:
    combined = "\n".join(text for _source, text in texts)
    if not combined:
        return None
    matched_disclosures = _matched_terms(combined, disclosure_terms)
    if matched_disclosures:
        return None

    matched_by_source: dict[str, set[str]] = {}
    for source, text in texts:
        matches = _matched_terms(text, sponsorship_terms)
        if matches:
            matched_by_source.setdefault(source, set()).update(matches)
    if not matched_by_source:
        return None

    matched_terms = tuple(sorted({term for terms in matched_by_source.values() for term in terms}))
    return NewsletterDisclosureFinding(
        newsletter_send_id=int(send["id"]),
        issue_id=send.get("issue_id") or "",
        subject=send.get("subject") or "",
        status=send.get("status") or "",
        sent_at=send.get("sent_at") or "",
        severity=_severity(matched_terms),
        matched_terms=matched_terms,
        sources=tuple(sorted(matched_by_source)),
    )


def _matched_terms(text: str, terms: Sequence[str]) -> tuple[str, ...]:
    normalised = _normalise_text(text)
    matches = []
    for term in terms:
        cleaned = _normalise_text(term)
        if not cleaned:
            continue
        pattern = re.compile(rf"(?<![a-z0-9]){re.escape(cleaned)}(?![a-z0-9])")
        if pattern.search(normalised):
            matches.append(cleaned)
    return tuple(sorted(set(matches)))


def _severity(terms: Iterable[str]) -> str:
    term_set = set(terms)
    if term_set & HIGH_SEVERITY_TERMS:
        return "high"
    if term_set & MEDIUM_SEVERITY_TERMS:
        return "medium"
    return "low"


def _load_sends(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    statuses: tuple[str, ...],
    limit: int | None,
) -> list[dict[str, Any]]:
    columns = schema["newsletter_sends"]
    selected = [
        column
        for column in (
            "id",
            "issue_id",
            "subject",
            "status",
            "sent_at",
            "source_content_ids",
            "metadata",
            "body",
            "content",
            "html",
            "text",
            "markdown",
            "preview",
        )
        if column in columns
    ]
    where = []
    params: list[Any] = []
    if "sent_at" in columns:
        where.append("datetime(sent_at) >= datetime(?)")
        params.append(cutoff.isoformat())
    if statuses and "status" in columns:
        where.append(f"status IN ({','.join('?' for _ in statuses)})")
        params.extend(statuses)
    sql = f"SELECT {', '.join(selected)} FROM newsletter_sends"
    if where:
        sql += " WHERE " + " AND ".join(where)
    order_sent = "datetime(sent_at) DESC, " if "sent_at" in columns else ""
    sql += f" ORDER BY {order_sent}id DESC"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    cursor = conn.execute(sql, params)
    return [_row_dict(cursor, row) for row in cursor.fetchall()]


def _load_variant_texts(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    sends: list[dict[str, Any]],
) -> dict[int, list[tuple[str, str]]]:
    if not sends or "content_variants" not in schema:
        return {}
    columns = schema["content_variants"]
    required = {"content_id", "platform", "variant_type", "content"}
    if not required.issubset(columns):
        return {}

    send_sources = {
        int(send["id"]): _parse_source_content_ids(send.get("source_content_ids"))
        for send in sends
    }
    content_ids = sorted({content_id for ids in send_sources.values() for content_id in ids})
    if not content_ids:
        return {}

    selected = [
        column
        for column in ("id", "content_id", "platform", "variant_type", "content", "metadata")
        if column in columns
    ]
    placeholders = ",".join("?" for _ in content_ids)
    cursor = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM content_variants
            WHERE content_id IN ({placeholders})
            ORDER BY content_id, platform, variant_type, id""",
        content_ids,
    )
    by_content: dict[int, list[dict[str, Any]]] = {}
    for row in cursor.fetchall():
        item = _row_dict(cursor, row)
        if _is_newsletter_variant(item):
            by_content.setdefault(int(item["content_id"]), []).append(item)

    texts: dict[int, list[tuple[str, str]]] = {}
    for send_id, source_ids in send_sources.items():
        for content_id in source_ids:
            for variant in by_content.get(content_id, ()):
                source = (
                    "content_variants:"
                    f"{variant.get('platform') or ''}:{variant.get('variant_type') or ''}"
                )
                texts.setdefault(send_id, []).append((source, variant.get("content") or ""))
                metadata = _parse_json(variant.get("metadata"))
                if isinstance(metadata, (dict, list, tuple)):
                    texts[send_id].extend(
                        _metadata_texts(metadata, prefix=f"{source}.metadata")
                    )
    return texts


def _send_texts(send: dict[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for column in TEXT_COLUMNS:
        value = send.get(column)
        if value:
            texts.append((f"newsletter_sends.{column}", str(value)))
    metadata = _parse_json(send.get("metadata"))
    if isinstance(metadata, (dict, list, tuple)):
        texts.extend(_metadata_texts(metadata, prefix="newsletter_sends.metadata"))
    return texts


def _metadata_texts(value: Any, *, prefix: str) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            child_prefix = f"{prefix}.{key}"
            if isinstance(item, str):
                texts.append((child_prefix, item))
            elif isinstance(item, (dict, list, tuple)):
                texts.extend(_metadata_texts(item, prefix=child_prefix))
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            child_prefix = f"{prefix}[{index}]"
            if isinstance(item, str):
                texts.append((child_prefix, item))
            elif isinstance(item, (dict, list, tuple)):
                texts.extend(_metadata_texts(item, prefix=child_prefix))
    return texts


def _is_newsletter_variant(row: dict[str, Any]) -> bool:
    haystack = " ".join(
        str(row.get(key) or "").casefold()
        for key in ("platform", "variant_type")
    )
    if any(marker in haystack for marker in NEWSLETTER_VARIANT_MARKERS):
        return True
    metadata = _parse_json(row.get("metadata"))
    if isinstance(metadata, dict):
        metadata_text = json.dumps(metadata, sort_keys=True).casefold()
        return any(marker in metadata_text for marker in NEWSLETTER_VARIANT_MARKERS)
    return False


def _severity_counts(findings: Iterable[NewsletterDisclosureFinding]) -> dict[str, int]:
    counts = Counter(finding.severity for finding in findings)
    return {severity: counts.get(severity, 0) for severity in SEVERITIES}


def _empty_severity_counts() -> dict[str, int]:
    return {severity: 0 for severity in SEVERITIES}


def _parse_source_content_ids(raw_value: Any) -> tuple[int, ...]:
    parsed = _parse_json(raw_value)
    if not isinstance(parsed, list):
        return ()
    ids: list[int] = []
    for item in parsed:
        try:
            ids.append(int(item))
        except (TypeError, ValueError):
            continue
    return tuple(ids)


def _parse_json(raw_value: Any) -> Any:
    if raw_value is None or raw_value == "":
        return None
    if isinstance(raw_value, (dict, list)):
        return raw_value
    try:
        return json.loads(str(raw_value))
    except (TypeError, json.JSONDecodeError):
        return None


def _normalise_statuses(status: str | Sequence[str] | None) -> tuple[str, ...]:
    if status is None:
        return ()
    values = [status] if isinstance(status, str) else list(status)
    normalised = []
    for value in values:
        for item in str(value).split(","):
            cleaned = item.strip()
            if cleaned:
                normalised.append(cleaned)
    return tuple(sorted(set(normalised)))


def _normalise_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text).casefold()).strip()


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or Database-like object")
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        columns = conn.execute(f"PRAGMA table_info({table})").fetchall()
        schema[str(table)] = {
            column["name"] if isinstance(column, sqlite3.Row) else column[1]
            for column in columns
        }
    return schema


def _row_dict(cursor: sqlite3.Cursor, row: Any) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return dict(row)
    names = [description[0] for description in cursor.description or ()]
    return dict(zip(names, row))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
