"""Detect repeated newsletter preview/preheader openings."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any, Mapping


DEFAULT_DAYS = 30
DEFAULT_THRESHOLD = 2
PREVIEW_METADATA_KEYS = ("preview_text", "preheader", "description", "summary")


@dataclass(frozen=True)
class NewsletterPreviewFatigueExample:
    """One newsletter send contributing to a repeated preview pattern."""

    newsletter_send_id: int
    issue_id: str
    subject: str
    sent_at: str | None
    preview_text: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterPreviewFatigueGroup:
    """A repeated normalized preview opening."""

    normalized_opening: str
    punctuation_pattern: str
    repeat_count: int
    sample_previews: tuple[str, ...]
    examples: tuple[NewsletterPreviewFatigueExample, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "examples": [example.to_dict() for example in self.examples],
            "normalized_opening": self.normalized_opening,
            "punctuation_pattern": self.punctuation_pattern,
            "repeat_count": self.repeat_count,
            "sample_previews": list(self.sample_previews),
        }


@dataclass(frozen=True)
class NewsletterPreviewFatigueReport:
    """Preview fatigue report plus filter and schema metadata."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    groups: tuple[NewsletterPreviewFatigueGroup, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    @property
    def has_repeats(self) -> bool:
        return bool(self.groups)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_preview_fatigue",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "groups": [group.to_dict() for group in self.groups],
            "has_repeats": self.has_repeats,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "repeat_group_count": len(self.groups),
            "totals": dict(self.totals),
        }


def build_newsletter_preview_fatigue_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    threshold: int = DEFAULT_THRESHOLD,
    now: datetime | None = None,
) -> NewsletterPreviewFatigueReport:
    """Return repeated preview/preheader openings in recent newsletter sends."""
    if days <= 0:
        raise ValueError("days must be positive")
    if threshold <= 0:
        raise ValueError("threshold must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "cutoff": cutoff.isoformat(),
        "threshold": threshold,
    }
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    sends = _load_sends(conn, schema, cutoff=cutoff)
    return _build_report_from_rows(
        sends,
        generated_at=generated_at,
        filters=filters,
        threshold=threshold,
        missing_tables=(),
        missing_columns={},
    )


def format_newsletter_preview_fatigue_json(
    report: NewsletterPreviewFatigueReport,
) -> str:
    """Serialize the preview fatigue report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_preview_fatigue_text(
    report: NewsletterPreviewFatigueReport,
) -> str:
    """Render the preview fatigue report for command-line review."""
    totals = report.totals
    lines = [
        "Newsletter Preview Fatigue",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['days']} days "
            f"threshold={report.filters['threshold']}"
        ),
        (
            "Totals: "
            f"sends={totals['send_count']} "
            f"with_preview={totals['preview_count']} "
            f"repeat_groups={totals['repeat_group_count']}"
        ),
    ]
    if totals["malformed_metadata_count"]:
        lines.append(f"Malformed metadata rows: {totals['malformed_metadata_count']}")
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in report.missing_columns.items()
        ]
        lines.append(f"Missing columns: {'; '.join(missing)}")
    lines.append("")

    if not report.groups:
        lines.append("No repeated newsletter preview openings found.")
        return "\n".join(lines)

    lines.append("Repeated preview openings:")
    for group in report.groups:
        punctuation = group.punctuation_pattern or "-"
        lines.append(
            f"  - opening={group.normalized_opening!r} "
            f"punctuation={punctuation!r} count={group.repeat_count}"
        )
        for example in group.examples:
            lines.append(
                f"      send={example.newsletter_send_id} "
                f"issue={example.issue_id or '-'} "
                f"subject={example.subject or '-'} "
                f"sent_at={example.sent_at or '-'} "
                f"preview={example.preview_text!r}"
            )
    return "\n".join(lines)


def _build_report_from_rows(
    sends: list[dict[str, Any]],
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    threshold: int,
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> NewsletterPreviewFatigueReport:
    buckets: dict[tuple[str, str], list[NewsletterPreviewFatigueExample]] = defaultdict(list)
    malformed_metadata_count = 0
    preview_count = 0

    for send in sends:
        metadata, malformed = _metadata_object(send.get("metadata"))
        if malformed:
            malformed_metadata_count += 1
        preview_text = _preview_text(metadata)
        if not preview_text:
            continue
        normalized_opening, punctuation_pattern = _preview_key(preview_text)
        if not normalized_opening:
            continue
        preview_count += 1
        buckets[(normalized_opening, punctuation_pattern)].append(
            NewsletterPreviewFatigueExample(
                newsletter_send_id=int(send["newsletter_send_id"]),
                issue_id=str(send.get("issue_id") or ""),
                subject=str(send.get("subject") or ""),
                sent_at=send.get("sent_at"),
                preview_text=preview_text,
            )
        )

    groups = [
        NewsletterPreviewFatigueGroup(
            normalized_opening=normalized_opening,
            punctuation_pattern=punctuation_pattern,
            repeat_count=len(examples),
            sample_previews=tuple(_sample_previews(examples)),
            examples=tuple(examples),
        )
        for (normalized_opening, punctuation_pattern), examples in buckets.items()
        if len(examples) >= threshold
    ]
    groups.sort(
        key=lambda group: (
            -group.repeat_count,
            group.normalized_opening,
            group.punctuation_pattern,
        )
    )
    return NewsletterPreviewFatigueReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "send_count": len(sends),
            "preview_count": preview_count,
            "repeat_group_count": len(groups),
            "repeated_send_count": sum(group.repeat_count for group in groups),
            "malformed_metadata_count": malformed_metadata_count,
        },
        groups=tuple(groups),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _load_sends(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    columns = schema["newsletter_sends"]
    rows = conn.execute(
        f"""SELECT
               ns.id AS newsletter_send_id,
               {_column_expr(columns, "issue_id", "''", alias="ns")} AS issue_id,
               {_column_expr(columns, "subject", "''", alias="ns")} AS subject,
               {_column_expr(columns, "sent_at", "NULL", alias="ns")} AS sent_at,
               ns.metadata AS metadata
           FROM newsletter_sends ns
           WHERE ns.sent_at >= ?
           ORDER BY ns.sent_at DESC, ns.id DESC""",
        (cutoff.isoformat(),),
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


def _preview_text(metadata: Mapping[str, Any]) -> str:
    for key in PREVIEW_METADATA_KEYS:
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return _collapse_spaces(value)
    return ""


def _preview_key(preview_text: str) -> tuple[str, str]:
    text = _collapse_spaces(preview_text)
    opening = _opening_clause(text)
    normalized = _normalize_opening(opening)
    punctuation = _punctuation_pattern(text[: max(len(opening) + 1, 80)])
    return normalized, punctuation


def _opening_clause(text: str) -> str:
    match = re.search(r"[.!?;:,]|--|[-\u2013\u2014]", text)
    if match and match.start() > 0:
        return text[: match.start()]
    words = text.split()
    return " ".join(words[:8])


def _normalize_opening(value: str) -> str:
    normalized = value.lower().replace("\u2019", "'")
    normalized = re.sub(r"[^a-z0-9']+", " ", normalized)
    normalized = re.sub(r"\b(?:a|an|the)\b", " ", normalized)
    return _collapse_spaces(normalized).strip("' ")


def _punctuation_pattern(value: str) -> str:
    chars: list[str] = []
    for char in value:
        if char in ".!?;:,":
            chars.append(char)
        elif char in "-\u2013\u2014":
            chars.append("-")
    return "".join(chars[:8])


def _sample_previews(examples: list[NewsletterPreviewFatigueExample]) -> list[str]:
    samples: list[str] = []
    seen: set[str] = set()
    for example in examples:
        if example.preview_text in seen:
            continue
        seen.add(example.preview_text)
        samples.append(example.preview_text)
        if len(samples) == 3:
            break
    return samples


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {"newsletter_sends": {"id", "metadata", "sent_at"}}
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
) -> NewsletterPreviewFatigueReport:
    return NewsletterPreviewFatigueReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "send_count": 0,
            "preview_count": 0,
            "repeat_group_count": 0,
            "repeated_send_count": 0,
            "malformed_metadata_count": 0,
        },
        groups=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


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


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str,
) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _collapse_spaces(value: str) -> str:
    return " ".join(str(value).split())
