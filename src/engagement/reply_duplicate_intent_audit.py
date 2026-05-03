"""Audit queued reply drafts for duplicate targets or duplicate intent."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any, Iterable, Sequence


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 50
DEFAULT_STATUSES = ("pending",)

AUDIT_COLUMNS = (
    "id",
    "status",
    "platform",
    "inbound_tweet_id",
    "inbound_cid",
    "inbound_url",
    "inbound_author_handle",
    "inbound_author_id",
    "draft_text",
    "intent",
    "relationship_context",
    "quality_score",
    "detected_at",
)

_URL_RE = re.compile(r"https?://\S+")
_HANDLE_RE = re.compile(r"(?<!\w)@[a-z0-9_.-]+", re.IGNORECASE)
_NON_WORD_RE = re.compile(r"[^\w\s]")
_SPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class ReplyDuplicateDraft:
    """One queued draft participating in a duplicate-intent group."""

    reply_queue_id: int
    status: str
    platform: str
    inbound_target_key: str | None
    recipient_key: str | None
    normalized_text: str
    intent: str
    relationship_key: str | None
    quality_score: float | None
    detected_at: str | None
    draft_preview: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "reply_queue_id": self.reply_queue_id,
            "status": self.status,
            "platform": self.platform,
            "inbound_target_key": self.inbound_target_key,
            "recipient_key": self.recipient_key,
            "normalized_text": self.normalized_text,
            "intent": self.intent,
            "relationship_key": self.relationship_key,
            "quality_score": self.quality_score,
            "detected_at": self.detected_at,
            "draft_preview": self.draft_preview,
        }


@dataclass(frozen=True)
class ReplyDuplicateIntentGroup:
    """A duplicate cluster with a canonical draft and drafts to review."""

    canonical_draft_id: int
    duplicate_draft_ids: tuple[int, ...]
    normalized_intent_keys: tuple[str, ...]
    confidence: float
    reasons: tuple[str, ...]
    drafts: tuple[ReplyDuplicateDraft, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "canonical_draft_id": self.canonical_draft_id,
            "duplicate_draft_ids": list(self.duplicate_draft_ids),
            "normalized_intent_keys": list(self.normalized_intent_keys),
            "confidence": self.confidence,
            "reasons": list(self.reasons),
            "drafts": [draft.to_dict() for draft in self.drafts],
        }


@dataclass(frozen=True)
class ReplyDuplicateIntentAuditReport:
    """Aggregated duplicate-intent audit report."""

    ok: bool
    generated_at: str
    filters: dict[str, Any]
    audited_count: int
    duplicate_group_count: int
    duplicate_draft_count: int
    by_reason: dict[str, int]
    groups: tuple[ReplyDuplicateIntentGroup, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocking_issue_count(self) -> int:
        return self.duplicate_draft_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_duplicate_intent_audit",
            "ok": self.ok,
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "audited_count": self.audited_count,
            "duplicate_group_count": self.duplicate_group_count,
            "duplicate_draft_count": self.duplicate_draft_count,
            "blocking_issue_count": self.blocking_issue_count,
            "by_reason": dict(sorted(self.by_reason.items())),
            "groups": [group.to_dict() for group in self.groups],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_reply_duplicate_intent_audit(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    statuses: Sequence[str] | None = None,
    platform: str | Sequence[str] | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ReplyDuplicateIntentAuditReport:
    """Find queued reply drafts that duplicate a target or recipient/text intent."""

    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    selected_statuses = _normalize_values(statuses or DEFAULT_STATUSES)
    platforms = _normalize_values((platform,) if isinstance(platform, str) else platform)
    filters = {
        "days": days,
        "statuses": list(selected_statuses),
        "platform": list(platforms),
        "limit": limit,
    }
    conn = _connection(db_or_conn)
    columns = _table_columns(conn, "reply_queue")
    if not columns:
        return _empty_report(generated_at, filters, missing_tables=("reply_queue",))

    required = ("id", "draft_text")
    missing_required = tuple(column for column in required if column not in columns)
    if missing_required:
        return _empty_report(
            generated_at,
            filters,
            missing_columns={"reply_queue": missing_required},
        )

    rows = _reply_rows(
        conn,
        columns,
        days=days,
        statuses=selected_statuses,
        platforms=platforms,
        now=generated_at,
    )
    drafts = []
    for row in rows:
        draft = _draft_from_row(row)
        if draft.normalized_text:
            drafts.append(draft)
    groups = _duplicate_groups(drafts, limit=limit)
    return ReplyDuplicateIntentAuditReport(
        ok=not groups,
        generated_at=generated_at.isoformat(),
        filters=filters,
        audited_count=len(drafts),
        duplicate_group_count=len(groups),
        duplicate_draft_count=sum(len(group.duplicate_draft_ids) for group in groups),
        by_reason=dict(Counter(reason for group in groups for reason in group.reasons)),
        groups=tuple(groups),
    )


def format_reply_duplicate_intent_audit_json(report: ReplyDuplicateIntentAuditReport) -> str:
    """Render deterministic JSON for automation."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_duplicate_intent_audit_markdown(report: ReplyDuplicateIntentAuditReport) -> str:
    """Render a compact markdown audit report."""

    filters = report.filters
    lines = [
        "# Reply Duplicate Intent Audit",
        "",
        (
            f"- Generated: {report.generated_at}\n"
            f"- Filters: days={filters['days']} statuses={','.join(filters['statuses'])} "
            f"platform={_display_filter(filters['platform'])} limit={filters['limit']}\n"
            f"- Audited drafts: {report.audited_count}\n"
            f"- Duplicate groups: {report.duplicate_group_count}\n"
            f"- Duplicate drafts to review: {report.duplicate_draft_count}"
        ),
    ]
    if report.by_reason:
        lines.append("")
        lines.append("## Reason Counts")
        for reason, count in sorted(report.by_reason.items()):
            lines.append(f"- `{reason}`: {count}")
    if report.missing_tables:
        lines.append("")
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        lines.append("")
        lines.append(
            "Missing columns: "
            + ", ".join(
                f"{table}.{column}"
                for table, columns in sorted(report.missing_columns.items())
                for column in columns
            )
        )
    if not report.groups and not report.missing_tables:
        lines.append("")
        lines.append("No duplicate reply draft intents matched.")
        return "\n".join(lines)

    for index, group in enumerate(report.groups, start=1):
        lines.append("")
        lines.append(f"## Group {index}")
        lines.append(
            f"- Canonical: `reply_queue:{group.canonical_draft_id}`\n"
            f"- Review duplicates: {', '.join(f'`reply_queue:{item}`' for item in group.duplicate_draft_ids)}\n"
            f"- Confidence: {group.confidence:.2f}\n"
            f"- Reasons: {', '.join(f'`{reason}`' for reason in group.reasons)}\n"
            f"- Intent keys: {', '.join(f'`{key}`' for key in group.normalized_intent_keys)}"
        )
        for draft in group.drafts:
            marker = "keep" if draft.reply_queue_id == group.canonical_draft_id else "review"
            score = "-" if draft.quality_score is None else f"{draft.quality_score:.1f}"
            lines.append(
                f"- {marker}: `reply_queue:{draft.reply_queue_id}` "
                f"recipient=`{draft.recipient_key or '-'}` target=`{draft.inbound_target_key or '-'}` "
                f"score={score} detected={draft.detected_at or '-'} preview={draft.draft_preview!r}"
            )
    return "\n".join(lines)


def _duplicate_groups(
    drafts: Sequence[ReplyDuplicateDraft],
    *,
    limit: int,
) -> list[ReplyDuplicateIntentGroup]:
    key_to_ids: dict[str, set[int]] = defaultdict(set)
    by_id = {draft.reply_queue_id: draft for draft in drafts}
    for draft in drafts:
        for key in _keys_for_draft(draft):
            key_to_ids[key].add(draft.reply_queue_id)

    parent = {draft.reply_queue_id: draft.reply_queue_id for draft in drafts}
    for ids in key_to_ids.values():
        if len(ids) < 2:
            continue
        ordered = sorted(ids)
        first = ordered[0]
        for other in ordered[1:]:
            _union(parent, first, other)

    components: dict[int, list[ReplyDuplicateDraft]] = defaultdict(list)
    for draft in drafts:
        components[_find(parent, draft.reply_queue_id)].append(draft)

    groups: list[ReplyDuplicateIntentGroup] = []
    for component in components.values():
        if len(component) < 2:
            continue
        component_ids = {draft.reply_queue_id for draft in component}
        matched_keys = tuple(
            sorted(key for key, ids in key_to_ids.items() if len(ids & component_ids) >= 2)
        )
        if not matched_keys:
            continue
        reasons = tuple(sorted({_reason_for_key(key) for key in matched_keys}))
        ordered_drafts = tuple(sorted(component, key=_draft_display_sort_key))
        canonical = min(component, key=_canonical_sort_key)
        duplicate_ids = tuple(
            draft.reply_queue_id
            for draft in ordered_drafts
            if draft.reply_queue_id != canonical.reply_queue_id
        )
        groups.append(
            ReplyDuplicateIntentGroup(
                canonical_draft_id=canonical.reply_queue_id,
                duplicate_draft_ids=duplicate_ids,
                normalized_intent_keys=matched_keys,
                confidence=1.0 if "same_target_mention" in reasons else 0.94,
                reasons=reasons,
                drafts=ordered_drafts,
            )
        )
    groups.sort(key=lambda group: (-group.confidence, group.canonical_draft_id))
    return groups[:limit]


def _draft_from_row(row: dict[str, Any]) -> ReplyDuplicateDraft:
    platform = _normalize_label(row.get("platform")) or "x"
    target_key = _target_key(row, platform)
    recipient_key = _recipient_key(row)
    relationship_key = _relationship_key(row.get("relationship_context"))
    normalized_text = normalize_reply_intent_text(row.get("draft_text"))
    return ReplyDuplicateDraft(
        reply_queue_id=int(row.get("id") or 0),
        status=_normalize_label(row.get("status")) or "pending",
        platform=platform,
        inbound_target_key=target_key,
        recipient_key=recipient_key,
        normalized_text=normalized_text,
        intent=_normalize_label(row.get("intent")) or "other",
        relationship_key=relationship_key,
        quality_score=_float_or_none(row.get("quality_score")),
        detected_at=_clean(row.get("detected_at")),
        draft_preview=_shorten(str(row.get("draft_text") or ""), 96),
    )


def normalize_reply_intent_text(text: Any) -> str:
    """Normalize draft text for deterministic duplicate-intent grouping."""

    if text is None:
        return ""
    normalized = str(text).casefold()
    normalized = normalized.replace("&amp;", " and ")
    normalized = _URL_RE.sub(" ", normalized)
    normalized = _HANDLE_RE.sub(" ", normalized)
    normalized = normalized.replace("'", "")
    normalized = _NON_WORD_RE.sub(" ", normalized)
    return _SPACE_RE.sub(" ", normalized).strip()


def normalize_reply_recipient(value: Any) -> str | None:
    """Normalize recipient handles/IDs for duplicate-intent grouping."""

    cleaned = _clean(value)
    if not cleaned:
        return None
    return _normalize_label(cleaned.removeprefix("@"))


def _keys_for_draft(draft: ReplyDuplicateDraft) -> tuple[str, ...]:
    keys = []
    if draft.inbound_target_key:
        keys.append(f"target:{draft.inbound_target_key}")
    if draft.recipient_key and draft.normalized_text:
        keys.append(f"recipient_text:{draft.recipient_key}:{draft.normalized_text}")
    return tuple(keys)


def _reason_for_key(key: str) -> str:
    if key.startswith("target:"):
        return "same_target_mention"
    return "same_recipient_normalized_text"


def _target_key(row: dict[str, Any], platform: str) -> str | None:
    for column in ("inbound_tweet_id", "inbound_cid", "inbound_url"):
        value = _clean(row.get(column))
        if value:
            normalized = _normalize_label(value.rstrip("/"))
            return f"{platform}:{normalized}"
    return None


def _recipient_key(row: dict[str, Any]) -> str | None:
    return normalize_reply_recipient(row.get("inbound_author_handle")) or normalize_reply_recipient(
        row.get("inbound_author_id")
    )


def _relationship_key(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        data = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return normalize_reply_intent_text(text) or None
        if not isinstance(parsed, dict):
            return normalize_reply_intent_text(text) or None
        data = parsed
    handle = normalize_reply_recipient(data.get("handle") or data.get("target_handle"))
    author_id = normalize_reply_recipient(data.get("author_id") or data.get("target_author_id"))
    stage = _normalize_label(data.get("stage"))
    tier = _normalize_label(data.get("tier"))
    parts = [part for part in (handle or author_id, stage, tier) if part]
    return ":".join(parts) if parts else None


def _canonical_sort_key(draft: ReplyDuplicateDraft) -> tuple[float, str, int]:
    quality = draft.quality_score if draft.quality_score is not None else -1.0
    return (-quality, _timestamp_sort_value(draft.detected_at), draft.reply_queue_id)


def _draft_display_sort_key(draft: ReplyDuplicateDraft) -> tuple[str, int]:
    return (_timestamp_sort_value(draft.detected_at), draft.reply_queue_id)


def _timestamp_sort_value(value: str | None) -> str:
    if not value:
        return "9999-12-31T23:59:59+00:00"
    return value.replace(" ", "T")


def _reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    days: int,
    statuses: tuple[str, ...],
    platforms: tuple[str, ...],
    now: datetime,
) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, column, _default_for_column(column)) for column in AUDIT_COLUMNS
    ]
    where = []
    params: list[Any] = []
    if statuses and "status" in columns:
        placeholders = ",".join("?" for _ in statuses)
        where.append(f"LOWER(COALESCE(status, 'pending')) IN ({placeholders})")
        params.extend(statuses)
    if "detected_at" in columns:
        cutoff = now - timedelta(days=days)
        where.append("(detected_at IS NULL OR datetime(detected_at) >= datetime(?))")
        params.append(cutoff.isoformat())
    if platforms and "platform" in columns:
        placeholders = ",".join("?" for _ in platforms)
        where.append(f"LOWER(COALESCE(platform, 'x')) IN ({placeholders})")
        params.extend(platforms)
    query = f"SELECT {', '.join(select_columns)} FROM reply_queue"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY " + ("datetime(detected_at) ASC, " if "detected_at" in columns else "") + "id ASC"
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    if column in columns:
        return f"{_quote_identifier(column)} AS {_quote_identifier(column)}"
    return f"{default} AS {_quote_identifier(column)}"


def _default_for_column(column: str) -> str:
    defaults = {
        "status": "'pending'",
        "platform": "'x'",
        "intent": "'other'",
    }
    return defaults.get(column, "NULL")


def _normalize_values(values: Sequence[str] | None) -> tuple[str, ...]:
    if not values:
        return ()
    return tuple(sorted({_normalize_label(value) for value in values if _normalize_label(value)}))


def _normalize_label(value: Any) -> str:
    return _SPACE_RE.sub(" ", str(value or "").strip().casefold()).strip()


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _shorten(value: str, limit: int) -> str:
    normalized = _SPACE_RE.sub(" ", value.strip())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "..."


def _find(parent: dict[int, int], item: int) -> int:
    while parent[item] != item:
        parent[item] = parent[parent[item]]
        item = parent[item]
    return item


def _union(parent: dict[int, int], left: int, right: int) -> None:
    left_root = _find(parent, left)
    right_root = _find(parent, right)
    if left_root != right_root:
        parent[max(left_root, right_root)] = min(left_root, right_root)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3 connection or database wrapper with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")}
    except sqlite3.Error:
        return set()


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _display_filter(value: Iterable[str]) -> str:
    items = list(value)
    return ",".join(items) if items else "all"


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ReplyDuplicateIntentAuditReport:
    return ReplyDuplicateIntentAuditReport(
        ok=True,
        generated_at=generated_at.isoformat(),
        filters=filters,
        audited_count=0,
        duplicate_group_count=0,
        duplicate_draft_count=0,
        by_reason={},
        groups=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
