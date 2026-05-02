"""Identify repeated low-value inbound reply sources."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_COUNT = 2
DEFAULT_LIMIT = 25
DEFAULT_EXAMPLE_LIMIT = 3

TABLE = "reply_queue"
LOW_QUALITY_FLAGS = {
    "duplicate",
    "eval error",
    "eval_error",
    "generic",
    "hashtags",
    "low quality",
    "low value",
    "low_quality",
    "low-value",
    "low_value",
    "no response",
    "no-response",
    "no_response",
    "parse_error",
    "spam",
    "stage mismatch",
    "stage_mismatch",
    "sycophantic",
}
NO_RESPONSE_FLAGS = {
    "duplicate",
    "low value",
    "low-value",
    "low_value",
    "no response",
    "no-response",
    "no_response",
    "spam",
}

SUSPICIOUS_PHRASES = (
    "airdrop",
    "book a call",
    "check my profile",
    "check this out",
    "click here",
    "crypto",
    "crypto giveaway",
    "dm me",
    "earn money",
    "follow back",
    "forex",
    "free money",
    "guaranteed",
    "investment opportunity",
    "limited time",
    "nft",
    "onlyfans",
    "telegram",
    "whatsapp",
    "work from home",
)

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_URL_RE = re.compile(r"(?:https?://|www\.)\S+", re.IGNORECASE)


@dataclass(frozen=True)
class ReplySpamSourceFinding:
    """One platform/author pair creating repeated low-value review load."""

    platform: str
    inbound_author_handle: str
    score: int
    counts: dict[str, int]
    first_seen_at: str | None
    last_seen_at: str | None
    example_reply_ids: tuple[int, ...]
    example_inbound_texts: tuple[str, ...]
    duplicate_fingerprints: tuple[str, ...]
    quality_flags: tuple[str, ...]
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["example_reply_ids"] = list(self.example_reply_ids)
        payload["example_inbound_texts"] = list(self.example_inbound_texts)
        payload["duplicate_fingerprints"] = list(self.duplicate_fingerprints)
        payload["quality_flags"] = list(self.quality_flags)
        return payload


@dataclass(frozen=True)
class ReplySpamSourceReport:
    """Summary of repeated spammy inbound reply sources."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    findings: tuple[ReplySpamSourceFinding, ...]
    source_table: str | None = TABLE
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_spam_source_report",
            "filters": dict(self.filters),
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "source_table": self.source_table,
            "totals": dict(sorted(self.totals.items())),
        }


def build_reply_spam_source_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_count: int = DEFAULT_MIN_COUNT,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ReplySpamSourceReport:
    """Build a deterministic, read-only report of repeated spammy reply sources."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_count <= 0:
        raise ValueError("min_count must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": cutoff.isoformat(),
        "min_count": min_count,
    }
    missing_columns: dict[str, tuple[str, ...]] = {}
    missing_tables: tuple[str, ...] = ()

    if _looks_like_rows(db_or_rows):
        rows = [_normalize_row(_mapping(row), now=generated_at, columns=set(_mapping(row))) for row in db_or_rows]
        source_table: str | None = "rows"
    else:
        conn = _connection(db_or_rows)
        columns = _table_columns(conn, TABLE)
        if not columns:
            return ReplySpamSourceReport(
                generated_at=generated_at.isoformat(),
                filters=filters,
                totals=_empty_totals(),
                findings=(),
                source_table=None,
                missing_tables=(TABLE,),
                missing_columns={},
            )
        missing = _missing_columns(columns)
        if missing:
            missing_columns = {TABLE: missing}
        rows = _load_rows(conn, columns, now=generated_at)
        source_table = TABLE

    rows = [row for row in rows if cutoff <= row["timestamp"] <= generated_at]
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        handle = _normalize_handle(row.get("inbound_author_handle"))
        if not handle:
            continue
        grouped[(row["platform"], handle)].append(row)

    all_findings = [
        _finding(platform, handle, matches)
        for (platform, handle), matches in grouped.items()
        if len(matches) >= min_count
    ]
    findings = [
        finding
        for finding in all_findings
        if finding.score > 0 or finding.counts["spam_indicator_count"] > 0
    ]
    findings.sort(key=_finding_sort_key)
    findings = findings[:limit]

    return ReplySpamSourceReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "rows_scanned": len(rows),
            "sources_scanned": len(grouped),
            "sources_flagged": len(all_findings),
            "sources_reported": len(findings),
            "spam_indicator_count": sum(
                finding.counts["spam_indicator_count"] for finding in all_findings
            ),
        },
        findings=tuple(findings),
        source_table=source_table,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_reply_spam_source_report_json(report: ReplySpamSourceReport) -> str:
    """Serialize the spam-source report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_spam_source_report_text(report: ReplySpamSourceReport) -> str:
    """Render a concise human-readable spam-source report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Reply Spam Source Report",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={filters['days']} min_count={filters['min_count']} "
            f"limit={filters['limit']}"
        ),
        (
            "Totals: "
            f"rows={totals['rows_scanned']} sources={totals['sources_scanned']} "
            f"flagged={totals['sources_flagged']} reported={totals['sources_reported']} "
            f"indicators={totals['spam_indicator_count']}"
        ),
    ]
    if report.source_table:
        lines.append(f"Source table: {report.source_table}")
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    missing_columns = [
        f"{table}({', '.join(columns)})"
        for table, columns in (report.missing_columns or {}).items()
        if columns
    ]
    if missing_columns:
        lines.append("Missing optional columns: " + "; ".join(missing_columns))
    if not report.findings:
        lines.extend(["", "No repeated spammy reply sources matched."])
        return "\n".join(lines)

    lines.extend(["", "Sources:"])
    for finding in report.findings:
        counts = finding.counts
        lines.append(
            f"- {finding.platform}/@{finding.inbound_author_handle} "
            f"score={finding.score} total={counts['total']} "
            f"spam={counts['spam_intent_count']} phrases={counts['suspicious_phrase_hit_count']} "
            f"urls={counts['url_heavy_mention_count']} duplicates={counts['duplicate_mention_count']} "
            f"low_quality_flags={counts['low_quality_draft_flag_count']} "
            f"action={finding.recommended_action}"
        )
        for example in finding.example_inbound_texts:
            lines.append(f"  example: {example}")
    return "\n".join(lines)


def _finding(platform: str, handle: str, rows: list[dict[str, Any]]) -> ReplySpamSourceFinding:
    ordered = sorted(rows, key=_row_sort_key)
    fingerprints = Counter(row["inbound_fingerprint"] for row in ordered if row["inbound_fingerprint"])
    duplicate_fingerprints = tuple(sorted(fp for fp, count in fingerprints.items() if count > 1))
    duplicate_mentions = sum(fingerprints[fp] for fp in duplicate_fingerprints)
    quality_flags = sorted({flag for row in ordered for flag in row["quality_flags"]})
    low_quality_draft_flag_count = sum(
        1 for row in ordered for flag in row["quality_flags"] if flag in LOW_QUALITY_FLAGS
    )
    no_response_quality_flag_count = sum(
        1 for row in ordered for flag in row["quality_flags"] if flag in NO_RESPONSE_FLAGS
    )
    counts = {
        "total": len(ordered),
        "spam_intent_count": sum(1 for row in ordered if row["intent"] == "spam"),
        "suspicious_phrase_hit_count": sum(row["suspicious_phrase_hits"] for row in ordered),
        "url_heavy_mention_count": sum(1 for row in ordered if row["url_heavy"]),
        "duplicate_fingerprint_count": len(duplicate_fingerprints),
        "duplicate_mention_count": duplicate_mentions,
        "low_quality_draft_flag_count": low_quality_draft_flag_count,
        "low_quality_draft_row_count": sum(
            1 for row in ordered if set(row["quality_flags"]) & LOW_QUALITY_FLAGS
        ),
        "no_response_quality_flag_count": no_response_quality_flag_count,
    }
    counts["spam_indicator_count"] = (
        counts["spam_intent_count"]
        + counts["suspicious_phrase_hit_count"]
        + counts["url_heavy_mention_count"]
        + counts["duplicate_mention_count"]
        + counts["low_quality_draft_flag_count"]
    )
    score = _score(counts)
    return ReplySpamSourceFinding(
        platform=platform,
        inbound_author_handle=handle,
        score=score,
        counts=counts,
        first_seen_at=ordered[0]["timestamp"].isoformat() if ordered else None,
        last_seen_at=ordered[-1]["timestamp"].isoformat() if ordered else None,
        example_reply_ids=tuple(
            row["reply_queue_id"] for row in ordered[:DEFAULT_EXAMPLE_LIMIT] if row["reply_queue_id"] is not None
        ),
        example_inbound_texts=tuple(
            _excerpt(row["inbound_text"]) for row in ordered[:DEFAULT_EXAMPLE_LIMIT] if row["inbound_text"]
        ),
        duplicate_fingerprints=duplicate_fingerprints[:DEFAULT_EXAMPLE_LIMIT],
        quality_flags=tuple(quality_flags),
        recommended_action=_recommended_action(score, counts),
    )


def _score(counts: dict[str, int]) -> int:
    raw = (
        counts["spam_intent_count"] * 20
        + counts["suspicious_phrase_hit_count"] * 8
        + counts["url_heavy_mention_count"] * 10
        + counts["duplicate_mention_count"] * 7
        + counts["low_quality_draft_flag_count"] * 6
        + counts["no_response_quality_flag_count"] * 8
        + min(counts["total"], 10) * 2
    )
    return min(100, raw)


def _recommended_action(score: int, counts: dict[str, int]) -> str:
    if score >= 80 or counts["spam_intent_count"] >= 3:
        return "consider_source_mute_or_filter"
    if counts["duplicate_mention_count"] >= 3:
        return "deduplicate_or_batch_review"
    if score >= 35:
        return "review_author_source"
    return "monitor"


def _load_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    now: datetime,
) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, "id"),
        _column_expr(columns, "platform", "'x'"),
        _column_expr(columns, "inbound_author_handle"),
        _column_expr(columns, "inbound_text"),
        _column_expr(columns, "intent", "'other'"),
        _column_expr(columns, "quality_flags"),
        _column_expr(columns, "quality_score"),
        _column_expr(columns, "detected_at"),
        _column_expr(columns, "reviewed_at"),
        _column_expr(columns, "posted_at"),
    ]
    order = "datetime(detected_at) ASC, id ASC" if "detected_at" in columns and "id" in columns else "rowid ASC"
    cursor = conn.execute(f"SELECT {', '.join(select_columns)} FROM {TABLE} ORDER BY {order}")
    return [_normalize_row(dict(row), now=now, columns=columns) for row in cursor.fetchall()]


def _normalize_row(row: Mapping[str, Any], *, now: datetime, columns: set[str]) -> dict[str, Any]:
    inbound_text = str(row.get("inbound_text") or "")
    timestamp = (
        _parse_timestamp(row.get("detected_at"))
        or _parse_timestamp(row.get("reviewed_at"))
        or _parse_timestamp(row.get("posted_at"))
        or now
    )
    flags = _parse_flags(row.get("quality_flags"))
    return {
        "reply_queue_id": _int_or_none(row.get("id")),
        "platform": _clean_label(row.get("platform")) or "x",
        "inbound_author_handle": row.get("inbound_author_handle"),
        "inbound_text": inbound_text,
        "intent": _clean_label(row.get("intent")) or "other",
        "quality_flags": flags,
        "quality_score": _float_or_none(row.get("quality_score")),
        "timestamp": timestamp,
        "suspicious_phrase_hits": _suspicious_phrase_hits(inbound_text),
        "url_heavy": _is_url_heavy(inbound_text),
        "inbound_fingerprint": fingerprint_inbound_text(inbound_text),
    }


def fingerprint_inbound_text(text: str) -> str:
    """Return a stable fingerprint for duplicate inbound reply text."""
    normalized = _URL_RE.sub(" url ", str(text).casefold())
    tokens = [token for token in _TOKEN_RE.findall(normalized) if token not in {"a", "an", "the", "to"}]
    return " ".join(tokens)


def _suspicious_phrase_hits(text: str) -> int:
    normalized = " ".join(str(text).casefold().split())
    return sum(1 for phrase in SUSPICIOUS_PHRASES if phrase in normalized)


def _is_url_heavy(text: str) -> bool:
    url_count = len(_URL_RE.findall(str(text)))
    if url_count == 0:
        return False
    token_count = len(_TOKEN_RE.findall(_URL_RE.sub(" ", str(text))))
    return url_count >= 2 or token_count <= 8 or url_count / max(token_count, 1) >= 0.2


def _parse_flags(raw: Any) -> list[str]:
    if raw is None or str(raw).strip() == "":
        return []
    parsed: Any
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    return sorted(
        {
            str(item).strip().casefold().replace("_", " ")
            for item in parsed
            if isinstance(item, str) and item.strip()
        }
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _missing_columns(columns: set[str]) -> tuple[str, ...]:
    expected = (
        "id",
        "platform",
        "inbound_author_handle",
        "inbound_text",
        "intent",
        "quality_flags",
        "quality_score",
        "detected_at",
    )
    return tuple(column for column in expected if column not in columns)


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    if column in columns:
        return column
    return f"{default} AS {column}"


def _normalize_handle(value: Any) -> str | None:
    normalized = str(value or "").strip().lstrip("@").casefold()
    return normalized or None


def _clean_label(value: Any) -> str | None:
    normalized = str(value or "").strip().casefold()
    return normalized or None


def _parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _row_sort_key(row: Mapping[str, Any]) -> tuple[str, int]:
    timestamp = row.get("timestamp")
    timestamp_text = timestamp.isoformat() if isinstance(timestamp, datetime) else ""
    return (timestamp_text, int(row.get("reply_queue_id") or 0))


def _finding_sort_key(finding: ReplySpamSourceFinding) -> tuple[Any, ...]:
    counts = finding.counts
    return (
        -finding.score,
        -counts["spam_indicator_count"],
        -counts["total"],
        finding.platform,
        finding.inbound_author_handle,
    )


def _excerpt(value: str, limit: int = 120) -> str:
    compact = " ".join(str(value).split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 1].rstrip() + "..."


def _empty_totals() -> dict[str, int]:
    return {
        "rows_scanned": 0,
        "sources_scanned": 0,
        "sources_flagged": 0,
        "sources_reported": 0,
        "spam_indicator_count": 0,
    }


def _looks_like_rows(value: Any) -> bool:
    if isinstance(value, (sqlite3.Connection, str, bytes)) or hasattr(value, "conn"):
        return False
    return isinstance(value, Iterable)


def _mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row)
