"""Report recurring newsletter deliverability-risk phrases across sends."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 60
DEFAULT_THRESHOLD = 2
MAX_EXAMPLES_PER_FINDING = 5

PHRASE_CATALOG: dict[str, tuple[str, ...]] = {
    "urgency": (
        "act now",
        "before it's gone",
        "don't miss",
        "last chance",
        "limited time",
        "urgent",
    ),
    "hype": (
        "amazing",
        "best ever",
        "free",
        "guaranteed",
        "revolutionary",
    ),
    "financial_claims": (
        "cash",
        "double your",
        "make money",
        "risk free",
        "save money",
    ),
    "clickbait": (
        "secret",
        "shocking",
        "this one trick",
        "what happens next",
        "you won't believe",
    ),
}

EXCESSIVE_PUNCTUATION = "excessive punctuation"

_WORD_RE = re.compile(r"[a-z0-9]+")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_EXCESSIVE_PUNCTUATION_RE = re.compile(r"!!!+|\?\?+|[!?]{3,}")
_BODY_METADATA_MARKERS = (
    "body",
    "content",
    "footer",
    "html",
    "intro",
    "markdown",
    "section",
    "text",
)
_PREVIEW_METADATA_MARKERS = ("preview", "preheader")


@dataclass(frozen=True)
class NewsletterSpamTriggerExample:
    """One newsletter send containing a repeated deliverability trigger."""

    issue_id: str
    subject: str
    sent_at: str | None
    field: str
    matched_phrase: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterSpamTriggerFinding:
    """A trigger phrase repeated across newsletter sends."""

    category: str
    normalized_phrase: str
    send_count: int
    field_counts: dict[str, int]
    examples: tuple[NewsletterSpamTriggerExample, ...]
    recommendation: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "examples": [example.to_dict() for example in self.examples],
            "field_counts": dict(sorted(self.field_counts.items())),
            "normalized_phrase": self.normalized_phrase,
            "recommendation": self.recommendation,
            "send_count": self.send_count,
        }


@dataclass(frozen=True)
class NewsletterSpamTriggerDriftReport:
    """Recurring spam-trigger report plus filters and schema metadata."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    findings: tuple[NewsletterSpamTriggerFinding, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None
    warnings: tuple[str, ...] = ()

    @property
    def has_findings(self) -> bool:
        return bool(self.findings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_spam_trigger_drift",
            "filters": dict(self.filters),
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "has_findings": self.has_findings,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(sorted(self.totals.items())),
            "warnings": list(self.warnings),
        }


def build_newsletter_spam_trigger_drift_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    threshold: int = DEFAULT_THRESHOLD,
    now: datetime | None = None,
) -> NewsletterSpamTriggerDriftReport:
    """Build a deterministic report of repeated spam-trigger phrases."""
    if days <= 0:
        raise ValueError("days must be positive")
    if threshold <= 0:
        raise ValueError("threshold must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "threshold": threshold,
        "window_end": generated_at.isoformat(),
        "window_start": cutoff.isoformat(),
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_sends(conn, schema["newsletter_sends"], cutoff=cutoff)
    findings, warnings, trigger_send_ids = _find_trigger_drift(
        rows,
        threshold=threshold,
    )
    return NewsletterSpamTriggerDriftReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "finding_count": len(findings),
            "malformed_metadata_count": sum(
                1 for warning in warnings if warning.startswith("malformed_metadata:")
            ),
            "send_count": len(rows),
            "triggered_send_count": len(trigger_send_ids),
            "trigger_match_count": sum(finding.send_count for finding in findings),
        },
        findings=tuple(findings),
        missing_tables=(),
        missing_columns={},
        warnings=tuple(sorted(warnings)),
    )


def format_newsletter_spam_trigger_drift_json(
    report: NewsletterSpamTriggerDriftReport,
) -> str:
    """Serialize a spam-trigger drift report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_spam_trigger_drift_text(
    report: NewsletterSpamTriggerDriftReport,
) -> str:
    """Render a spam-trigger drift report for terminal review."""
    totals = report.totals
    lines = [
        "Newsletter Spam Trigger Drift",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['window_start']} to "
            f"{report.filters['window_end']} ({report.filters['days']} days)"
        ),
        f"Threshold: {report.filters['threshold']} sends",
        (
            "Totals: "
            f"sends={totals['send_count']} "
            f"triggered_sends={totals['triggered_send_count']} "
            f"findings={totals['finding_count']} "
            f"malformed_metadata={totals['malformed_metadata_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
        return "\n".join(lines)
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        ]
        lines.append(f"Missing columns: {'; '.join(missing)}")
        return "\n".join(lines)

    lines.append("")
    if not report.findings:
        lines.append("No newsletter spam trigger drift found.")
    else:
        lines.append("Spam trigger drift findings:")
        for finding in report.findings:
            field_counts = ", ".join(
                f"{field}={count}" for field, count in sorted(finding.field_counts.items())
            )
            lines.append(
                "  - "
                f"{finding.category}: {finding.normalized_phrase!r} "
                f"sends={finding.send_count} fields={field_counts}"
            )
            lines.append(f"      recommendation: {finding.recommendation}")
            for example in finding.examples:
                lines.append(
                    "      "
                    f"issue={example.issue_id or '-'} "
                    f"sent_at={example.sent_at or '-'} "
                    f"field={example.field} "
                    f"matched={example.matched_phrase!r} "
                    f"subject={example.subject!r}"
                )
    if report.warnings:
        lines.append("")
        lines.append("Warnings:")
        lines.extend(f"  - {warning}" for warning in report.warnings)
    return "\n".join(lines)


def _find_trigger_drift(
    rows: list[dict[str, Any]],
    *,
    threshold: int,
) -> tuple[list[NewsletterSpamTriggerFinding], set[str], set[int]]:
    buckets: dict[tuple[str, str], dict[int, list[NewsletterSpamTriggerExample]]] = (
        defaultdict(lambda: defaultdict(list))
    )
    warnings: set[str] = set()
    triggered_send_ids: set[int] = set()
    catalog = _normalized_catalog()

    for row in rows:
        send_id = int(row["newsletter_send_id"])
        fields, field_warnings = _scannable_fields(row)
        warnings.update(field_warnings)
        for field, text in fields:
            for category, phrase in _matches(text, catalog):
                buckets[(category, phrase)][send_id].append(
                    NewsletterSpamTriggerExample(
                        issue_id=str(row.get("issue_id") or ""),
                        subject=_collapse_spaces(row.get("subject") or ""),
                        sent_at=row.get("sent_at"),
                        field=field,
                        matched_phrase=phrase,
                    )
                )
                triggered_send_ids.add(send_id)

    findings: list[NewsletterSpamTriggerFinding] = []
    for (category, phrase), by_send in buckets.items():
        if len(by_send) < threshold:
            continue
        examples = _examples(by_send)
        findings.append(
            NewsletterSpamTriggerFinding(
                category=category,
                normalized_phrase=phrase,
                send_count=len(by_send),
                field_counts=_field_counts(by_send),
                examples=examples[:MAX_EXAMPLES_PER_FINDING],
                recommendation=_recommendation(category, phrase),
            )
        )
    findings.sort(key=_finding_sort_key)
    return findings, warnings, triggered_send_ids


def _scannable_fields(row: Mapping[str, Any]) -> tuple[list[tuple[str, str]], set[str]]:
    fields: list[tuple[str, str]] = []
    subject = _collapse_spaces(row.get("subject") or "")
    if subject:
        fields.append(("subject", subject))

    warnings: set[str] = set()
    metadata = _parse_metadata(row.get("metadata"))
    if metadata is _MALFORMED:
        warnings.add(f"malformed_metadata:{row.get('newsletter_send_id')}")
        return fields, warnings
    fields.extend(_metadata_fields(metadata, prefix="metadata"))
    return fields, warnings


def _matches(
    text: str,
    catalog: dict[str, tuple[tuple[str, str], ...]],
) -> list[tuple[str, str]]:
    normalized = _normalize_text(text)
    matches: list[tuple[str, str]] = []
    for category, phrases in catalog.items():
        for phrase, phrase_pattern in phrases:
            if re.search(phrase_pattern, normalized):
                matches.append((category, phrase))
    if _EXCESSIVE_PUNCTUATION_RE.search(str(text or "")):
        matches.append(("excessive_punctuation", EXCESSIVE_PUNCTUATION))
    return matches


def _metadata_fields(value: Any, *, prefix: str) -> list[tuple[str, str]]:
    fields: list[tuple[str, str]] = []
    if isinstance(value, Mapping):
        for key, item in sorted(value.items(), key=lambda pair: str(pair[0])):
            path = f"{prefix}.{key}"
            key_lower = str(key).casefold()
            if isinstance(item, str) and _is_scannable_metadata_key(key_lower):
                fields.append((path, item))
            elif isinstance(item, (Mapping, list, tuple)):
                fields.extend(_metadata_fields(item, prefix=path))
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            path = f"{prefix}[{index}]"
            if isinstance(item, str) and _is_scannable_metadata_key(prefix):
                fields.append((path, item))
            elif isinstance(item, (Mapping, list, tuple)):
                fields.extend(_metadata_fields(item, prefix=path))
    return fields


def _is_scannable_metadata_key(key: str) -> bool:
    return any(marker in key for marker in _PREVIEW_METADATA_MARKERS) or any(
        marker in key for marker in _BODY_METADATA_MARKERS
    )


def _examples(
    by_send: dict[int, list[NewsletterSpamTriggerExample]],
) -> tuple[NewsletterSpamTriggerExample, ...]:
    examples = [items[0] for _send_id, items in sorted(by_send.items()) if items]
    return tuple(
        sorted(
            examples,
            key=lambda example: (
                example.sent_at or "",
                example.issue_id,
                example.field,
            ),
            reverse=True,
        )
    )


def _field_counts(
    by_send: dict[int, list[NewsletterSpamTriggerExample]],
) -> dict[str, int]:
    counts: dict[str, set[int]] = defaultdict(set)
    for send_id, examples in by_send.items():
        for example in examples:
            counts[example.field].add(send_id)
    return {field: len(send_ids) for field, send_ids in counts.items()}


def _finding_sort_key(finding: NewsletterSpamTriggerFinding) -> tuple[Any, ...]:
    latest = max((example.sent_at or "" for example in finding.examples), default="")
    return (-finding.send_count, finding.category, finding.normalized_phrase, latest)


def _recommendation(category: str, phrase: str) -> str:
    if category == "excessive_punctuation":
        return "Replace repeated punctuation with specific proof or a calmer call to action."
    if category == "urgency":
        return f"Retire the urgency phrase '{phrase}' and use a concrete deadline only when real."
    if category == "financial_claims":
        return f"Replace the financial claim '{phrase}' with verifiable, qualified language."
    if category == "clickbait":
        return f"Replace the clickbait phrase '{phrase}' with a direct description of the value."
    return f"Tone down the hype phrase '{phrase}' and lead with evidence instead."


def _load_sends(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    status_filter = "AND status = 'sent'" if "status" in columns else ""
    rows = conn.execute(
        f"""SELECT
               id AS newsletter_send_id,
               issue_id,
               subject,
               sent_at,
               metadata
           FROM newsletter_sends
           WHERE sent_at >= ?
             {status_filter}
           ORDER BY sent_at DESC, id DESC""",
        (cutoff.isoformat(),),
    ).fetchall()
    return [dict(row) for row in rows]


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {"newsletter_sends": {"id", "issue_id", "metadata", "sent_at", "subject"}}
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema[table]
    }
    return missing_tables, missing_columns


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> NewsletterSpamTriggerDriftReport:
    return NewsletterSpamTriggerDriftReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "finding_count": 0,
            "malformed_metadata_count": 0,
            "send_count": 0,
            "trigger_match_count": 0,
            "triggered_send_count": 0,
        },
        findings=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _normalized_catalog() -> dict[str, tuple[tuple[str, str], ...]]:
    catalog: dict[str, tuple[tuple[str, str], ...]] = {}
    for category, phrases in PHRASE_CATALOG.items():
        normalized_phrases = []
        for phrase in phrases:
            normalized = _normalize_phrase(phrase)
            pattern = r"\b" + r"\s+".join(map(re.escape, normalized.split())) + r"\b"
            normalized_phrases.append((normalized, pattern))
        catalog[category] = tuple(normalized_phrases)
    return catalog


def _normalize_phrase(value: str) -> str:
    return " ".join(_WORD_RE.findall(str(value or "").casefold().replace("\u2019", "'")))


def _normalize_text(value: str) -> str:
    normalized = _normalize_phrase(
        _NON_ALNUM_RE.sub(" ", str(value or "").casefold().replace("\u2019", "'"))
    )
    return f" {normalized} "


_MALFORMED = object()


def _parse_metadata(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, (Mapping, list, tuple)):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return _MALFORMED
    return parsed if isinstance(parsed, (Mapping, list, tuple)) else None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _collapse_spaces(value: Any) -> str:
    return " ".join(str(value or "").split())
