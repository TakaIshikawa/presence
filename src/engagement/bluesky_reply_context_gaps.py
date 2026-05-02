"""Audit queued Bluesky replies for missing imported thread context."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any

from engagement.bluesky_thread_context import parse_platform_metadata


DEFAULT_DAYS = 7
DEFAULT_LIMIT = 100
DEFAULT_STATUS = "pending"
EXAMPLE_LIMIT = 5

CLASSIFICATIONS = (
    "ready",
    "missing_root_context",
    "missing_parent_context",
    "missing_author_context",
    "stale_context",
)


@dataclass(frozen=True)
class BlueskyReplyContextGapItem:
    """Context audit result for one queued Bluesky reply."""

    id: int
    reply_id: str
    status: str
    author: str
    classification: str
    reasons: tuple[str, ...]
    detected_at: str
    draft_preview: str
    context_refs: dict[str, str | None]

    @property
    def is_blocking(self) -> bool:
        return self.classification != "ready"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "reply_id": self.reply_id,
            "status": self.status,
            "author": self.author,
            "classification": self.classification,
            "reasons": list(self.reasons),
            "detected_at": self.detected_at,
            "draft_preview": self.draft_preview,
            "context_refs": dict(sorted(self.context_refs.items())),
        }


@dataclass(frozen=True)
class BlueskyReplyContextGapReport:
    """Aggregated read-only Bluesky reply context gap report."""

    ok: bool
    generated_at: str
    filters: dict[str, Any]
    audited_count: int
    gap_count: int
    by_classification: dict[str, int]
    by_status: dict[str, dict[str, int]]
    representative_reply_ids: dict[str, tuple[int, ...]]
    items: tuple[BlueskyReplyContextGapItem, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocking_issue_count(self) -> int:
        return self.gap_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "bluesky_reply_context_gaps",
            "ok": self.ok,
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "audited_count": self.audited_count,
            "gap_count": self.gap_count,
            "blocking_issue_count": self.blocking_issue_count,
            "by_classification": dict(sorted(self.by_classification.items())),
            "by_status": {
                status: dict(sorted(counts.items()))
                for status, counts in sorted(self.by_status.items())
            },
            "representative_reply_ids": {
                name: list(ids)
                for name, ids in sorted(self.representative_reply_ids.items())
            },
            "items": [item.to_dict() for item in self.items],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def audit_bluesky_reply_context_gaps(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    status: str = DEFAULT_STATUS,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> BlueskyReplyContextGapReport:
    """Classify Bluesky reply drafts by imported thread context readiness."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit < 0:
        raise ValueError("limit must be non-negative")
    if not status:
        raise ValueError("status is required")

    conn = _connection(db_or_conn)
    now = _as_utc(now or datetime.now(timezone.utc))
    filters = {"days": days, "status": status, "limit": limit}
    columns = _table_columns(conn, "reply_queue")
    if not columns:
        return _empty_report(now, filters, missing_tables=("reply_queue",))

    required = ("platform", "platform_metadata")
    missing = tuple(column for column in required if column not in columns)
    if missing:
        return _empty_report(now, filters, missing_columns={"reply_queue": missing})

    rows = _reply_rows(conn, columns, days=days, status=status, limit=limit, now=now)
    items = tuple(_classify_row(row, columns) for row in rows)
    gaps = [item for item in items if item.is_blocking]
    return BlueskyReplyContextGapReport(
        ok=not gaps,
        generated_at=now.isoformat(),
        filters=filters,
        audited_count=len(items),
        gap_count=len(gaps),
        by_classification=dict(Counter(item.classification for item in items)),
        by_status=_counts_by_status(items),
        representative_reply_ids=_representative_reply_ids(items),
        items=items,
    )


def format_bluesky_reply_context_gaps_json(report: BlueskyReplyContextGapReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_bluesky_reply_context_gaps_text(report: BlueskyReplyContextGapReport) -> str:
    """Render a compact human-readable context gap report."""
    lines = [
        "Bluesky Reply Context Gap Audit",
        (
            "Filters: "
            f"status={report.filters.get('status')} days={report.filters.get('days')} "
            f"limit={report.filters.get('limit')}"
        ),
        f"Audited: {report.audited_count}",
        f"Gaps: {report.gap_count}",
    ]
    if report.by_classification:
        lines.append(
            "By classification: "
            + ", ".join(
                f"{key}={value}" for key, value in sorted(report.by_classification.items())
            )
        )
    if report.by_status:
        status_parts = []
        for status, counts in sorted(report.by_status.items()):
            inner = ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))
            status_parts.append(f"{status}({inner})")
        lines.append("By status: " + "; ".join(status_parts))
    if report.representative_reply_ids:
        lines.append(
            "Examples: "
            + ", ".join(
                f"{key}={list(ids)}"
                for key, ids in sorted(report.representative_reply_ids.items())
            )
        )
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        lines.append(
            "Missing columns: "
            + ", ".join(
                f"{table}.{column}"
                for table, columns in sorted(report.missing_columns.items())
                for column in columns
            )
        )
    if not report.items:
        lines.append("No Bluesky reply drafts matched.")
        return "\n".join(lines)

    flagged = [item for item in report.items if item.is_blocking]
    if not flagged:
        lines.append("No context gaps.")
        return "\n".join(lines)

    lines.append("")
    for item in flagged:
        lines.append(
            f"#{item.id} {item.status} @{item.author or 'unknown'} "
            f"{item.classification}: {', '.join(item.reasons)}"
        )
    return "\n".join(lines)


def _classify_row(row: dict[str, Any], columns: set[str]) -> BlueskyReplyContextGapItem:
    metadata = parse_platform_metadata(row.get("platform_metadata"))
    refs = _context_refs(row, metadata)
    reasons: list[str] = []

    stale_reasons = _stale_reasons(row, refs)
    if stale_reasons:
        classification = "stale_context"
        reasons = stale_reasons
    else:
        root_missing = _missing_root_reasons(row, refs)
        parent_missing = _missing_parent_reasons(refs)
        author_missing = _missing_author_reasons(row, refs)
        if root_missing:
            classification = "missing_root_context"
            reasons = root_missing
        elif parent_missing:
            classification = "missing_parent_context"
            reasons = parent_missing
        elif author_missing:
            classification = "missing_author_context"
            reasons = author_missing
        else:
            classification = "ready"
            reasons = ()

    return BlueskyReplyContextGapItem(
        id=int(row.get("id") or 0),
        reply_id=str(row.get("inbound_tweet_id") or row.get("id") or ""),
        status=str(row.get("status") if "status" in columns else "pending"),
        author=str(row.get("inbound_author_handle") if "inbound_author_handle" in columns else ""),
        classification=classification,
        reasons=tuple(reasons),
        detected_at=str(row.get("detected_at") if "detected_at" in columns else ""),
        draft_preview=_shorten(str(row.get("draft_text") or ""), 96),
        context_refs=refs,
    )


def _context_refs(row: dict[str, Any], metadata: dict[str, Any]) -> dict[str, str | None]:
    root_ref = _metadata_ref(metadata, "root")
    parent_ref = _metadata_ref(metadata, "parent")
    root_uri = _clean(root_ref.get("uri") or metadata.get("root_uri") or row.get("our_platform_id"))
    parent_uri = _clean(
        parent_ref.get("uri")
        or metadata.get("parent_post_uri")
        or metadata.get("parent_uri")
    )
    return {
        "inbound_uri": _clean(row.get("inbound_tweet_id")),
        "inbound_cid": _clean(row.get("inbound_cid")),
        "root_uri": root_uri,
        "root_cid": _clean(root_ref.get("cid") or metadata.get("root_cid")),
        "root_post_text": _clean(metadata.get("root_post_text") or row.get("our_post_text")),
        "root_author_handle": _clean(metadata.get("root_author_handle")),
        "parent_uri": parent_uri,
        "parent_cid": _clean(parent_ref.get("cid") or metadata.get("parent_cid")),
        "parent_post_text": _clean(metadata.get("parent_post_text")),
        "parent_author_handle": _clean(metadata.get("parent_author_handle")),
        "our_platform_id": _clean(row.get("our_platform_id")),
    }


def _metadata_ref(metadata: dict[str, Any], name: str) -> dict[str, Any]:
    for key in (f"reply_{name}", name):
        value = metadata.get(key)
        if isinstance(value, dict):
            return value
    return {}


def _stale_reasons(row: dict[str, Any], refs: dict[str, str | None]) -> list[str]:
    reasons: list[str] = []
    our_platform_id = refs.get("our_platform_id")
    root_uri = refs.get("root_uri")
    if our_platform_id and root_uri and our_platform_id != root_uri:
        reasons.append("root_uri differs from our_platform_id")
    inbound_uri = refs.get("inbound_uri")
    parent_uri = refs.get("parent_uri")
    if inbound_uri and parent_uri and inbound_uri == parent_uri:
        reasons.append("parent context points at inbound reply")
    row_root_text = _clean(row.get("our_post_text"))
    root_text = refs.get("root_post_text")
    if row_root_text and root_text and row_root_text != root_text:
        reasons.append("root_post_text differs from our_post_text")
    return reasons


def _missing_root_reasons(row: dict[str, Any], refs: dict[str, str | None]) -> list[str]:
    reasons: list[str] = []
    if not refs.get("root_uri"):
        reasons.append("missing root_uri")
    if not refs.get("root_cid"):
        reasons.append("missing root_cid")
    if not refs.get("root_post_text") and not _clean(row.get("our_post_text")):
        reasons.append("missing root_post_text")
    return reasons


def _missing_parent_reasons(refs: dict[str, str | None]) -> list[str]:
    reasons: list[str] = []
    if not refs.get("inbound_cid"):
        reasons.append("missing inbound_cid")
    if not refs.get("parent_uri"):
        reasons.append("missing parent_uri")
    if not refs.get("parent_cid"):
        reasons.append("missing parent_cid")
    if not refs.get("parent_post_text"):
        reasons.append("missing parent_post_text")
    return reasons


def _missing_author_reasons(row: dict[str, Any], refs: dict[str, str | None]) -> list[str]:
    reasons: list[str] = []
    if not _clean(row.get("inbound_author_handle") or row.get("inbound_author_id")):
        reasons.append("missing inbound_author")
    if not refs.get("root_author_handle"):
        reasons.append("missing root_author_handle")
    if not refs.get("parent_author_handle"):
        reasons.append("missing parent_author_handle")
    return reasons


def _reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    days: int,
    status: str,
    limit: int | None,
    now: datetime,
) -> list[dict[str, Any]]:
    where = ["platform = 'bluesky'"]
    params: list[Any] = []
    if "draft_text" in columns:
        where.append("draft_text IS NOT NULL")
        where.append("TRIM(draft_text) != ''")
    if "status" in columns and status != "all":
        where.append("COALESCE(status, 'pending') = ?")
        params.append(status)
    if "detected_at" in columns:
        cutoff = now - timedelta(days=days)
        where.append("(detected_at IS NULL OR datetime(detected_at) >= datetime(?))")
        params.append(cutoff.isoformat())

    query = "SELECT * FROM reply_queue WHERE " + " AND ".join(where)
    query += " ORDER BY " + _order_clause(columns)
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _counts_by_status(
    items: tuple[BlueskyReplyContextGapItem, ...]
) -> dict[str, dict[str, int]]:
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    for item in items:
        counts[item.status][item.classification] += 1
    return {status: dict(counter) for status, counter in counts.items()}


def _representative_reply_ids(
    items: tuple[BlueskyReplyContextGapItem, ...],
) -> dict[str, tuple[int, ...]]:
    examples: dict[str, list[int]] = defaultdict(list)
    for item in items:
        if not item.is_blocking or len(examples[item.classification]) >= EXAMPLE_LIMIT:
            continue
        examples[item.classification].append(item.id)
    return {name: tuple(ids) for name, ids in examples.items()}


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "detected_at" in columns:
        parts.append("datetime(detected_at) DESC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _empty_report(
    now: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> BlueskyReplyContextGapReport:
    return BlueskyReplyContextGapReport(
        ok=True,
        generated_at=now.isoformat(),
        filters=filters,
        audited_count=0,
        gap_count=0,
        by_classification={},
        by_status={},
        representative_reply_ids={},
        items=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _shorten(value: str, limit: int) -> str:
    text = " ".join(value.split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."
