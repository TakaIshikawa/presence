"""Report synthesis gate rejections grouped by normalized reason buckets."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any, Iterable, Mapping


DEFAULT_DAYS = 7
DEFAULT_LIMIT_EXAMPLES = 5


@dataclass(frozen=True)
class GateRejectionItem:
    """One rejected synthesis candidate with normalized reason."""

    run_id: int | None
    batch_id: str | None
    content_type: str | None
    reason_label: str
    reason_excerpt: str
    content_format: str | None
    created_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GateRejectionReasonSummary:
    """Summary counts by content type and normalized reason."""

    content_type: str
    reason_label: str
    rejection_count: int
    latest_timestamp: str | None
    recent_examples: tuple[str, ...]
    report_id: str

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "recent_examples": list(self.recent_examples),
        }


@dataclass(frozen=True)
class GateRejectionReasonsReport:
    """Report of synthesis gate rejections grouped by normalized reason."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    items: tuple[GateRejectionItem, ...]
    summaries: tuple[GateRejectionReasonSummary, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "synthesis_gate_rejection_reasons",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "items": [item.to_dict() for item in self.items],
            "summaries": [summary.to_dict() for summary in self.summaries],
            "totals": dict(sorted(self.totals.items())),
        }


def normalize_rejection_reason(reason: str | None) -> str:
    """Normalize rejection reasons into stable labels."""
    if not reason:
        return "unknown"

    reason_lower = reason.lower()

    # Budget rejections
    if "budget" in reason_lower or "cost" in reason_lower:
        return "budget_exceeded"

    # Filter rejections
    if "filtered" in reason_lower and ("repetitive" in reason_lower or "stale" in reason_lower or "duplicate" in reason_lower):
        return "all_filtered"
    if "claim" in reason_lower and ("unsupported" in reason_lower or "unsourced" in reason_lower):
        return "unsupported_claims"

    # Quality gate rejections
    if "threshold" in reason_lower or "score" in reason_lower:
        return "below_threshold"
    if "rejected" in reason_lower or "gate" in reason_lower:
        return "quality_gate_rejected"

    # Persona/voice rejections
    if "persona" in reason_lower or "voice" in reason_lower or "alignment" in reason_lower:
        return "persona_misalignment"

    # Stale patterns
    if "stale" in reason_lower:
        return "stale_pattern"

    # Thread validation
    if "thread" in reason_lower:
        return "thread_validation_failed"

    # Other common patterns
    if "length" in reason_lower:
        return "length_constraint"
    if "format" in reason_lower:
        return "format_error"

    return "other"


def build_gate_rejection_reasons_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit_examples: int = DEFAULT_LIMIT_EXAMPLES,
    now: datetime | None = None,
) -> GateRejectionReasonsReport:
    """Build a report of synthesis gate rejections grouped by normalized reason buckets."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit_examples < 0:
        raise ValueError("limit_examples must be non-negative")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)

    if _looks_like_rows(db_or_rows):
        raw_rows = [_mapping(row) for row in db_or_rows]
    else:
        conn = _connection(db_or_rows)
        raw_rows = _load_pipeline_runs(conn, cutoff)

    items = _build_rejection_items(raw_rows, cutoff=cutoff)
    summaries = _build_summaries(items, limit_examples=limit_examples)

    return GateRejectionReasonsReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "limit_examples": limit_examples,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
        },
        totals={
            "rejection_count": len(items),
            "content_type_count": len({item.content_type for item in items if item.content_type}),
            "reason_count": len({item.reason_label for item in items}),
            "rows_scanned": len(raw_rows),
        },
        items=items,
        summaries=summaries,
    )


def format_gate_rejection_reasons_json(report: GateRejectionReasonsReport) -> str:
    """Serialize a gate rejection reasons report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_gate_rejection_reasons_text(report: GateRejectionReasonsReport) -> str:
    """Render a concise human-readable gate rejection reasons report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Synthesis Gate Rejection Reasons",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={filters['days']} limit_examples={filters['limit_examples']} "
            f"lookback_start={filters['lookback_start']} "
            f"lookback_end={filters['lookback_end']}"
        ),
        (
            "Totals: "
            f"rejections={totals['rejection_count']} content_types={totals['content_type_count']} "
            f"unique_reasons={totals['reason_count']} rows={totals['rows_scanned']}"
        ),
    ]

    if not report.summaries:
        lines.extend(["", "No gate rejections found."])
        return "\n".join(lines)

    lines.extend(["", "Summary by Content Type and Reason:"])
    for summary in report.summaries:
        lines.append(
            f"- content_type={summary.content_type} reason={summary.reason_label} "
            f"count={summary.rejection_count} latest={summary.latest_timestamp or 'N/A'}"
        )
        if summary.recent_examples:
            for i, example in enumerate(summary.recent_examples, 1):
                excerpt = example[:80] + "..." if len(example) > 80 else example
                lines.append(f"    {i}. {excerpt}")

    return "\n".join(lines)


def _build_rejection_items(
    rows: Iterable[Mapping[str, Any]],
    *,
    cutoff: datetime | None = None,
) -> tuple[GateRejectionItem, ...]:
    """Convert raw rows to GateRejectionItem instances."""
    items: list[GateRejectionItem] = []
    for row in rows:
        rejection_reason = row.get("rejection_reason")
        if not rejection_reason:
            continue

        created_at_str = _first_text(row, ("created_at", "timestamp"))
        if cutoff and created_at_str:
            created_at = _parse_datetime(created_at_str)
            if created_at and created_at < cutoff:
                continue

        reason_label = normalize_rejection_reason(rejection_reason)
        content_type = str(row.get("content_type") or "unknown")

        items.append(
            GateRejectionItem(
                run_id=_safe_int(row.get("id")),
                batch_id=row.get("batch_id"),
                content_type=content_type,
                reason_label=reason_label,
                reason_excerpt=_excerpt(rejection_reason, limit=240),
                content_format=row.get("content_format"),
                created_at=created_at_str,
            )
        )

    return tuple(sorted(items, key=lambda item: (item.content_type or "", item.reason_label, item.created_at or "")))


def _build_summaries(
    items: tuple[GateRejectionItem, ...],
    *,
    limit_examples: int = DEFAULT_LIMIT_EXAMPLES,
) -> tuple[GateRejectionReasonSummary, ...]:
    """Build per-content-type, per-reason summaries with recent examples."""
    # Group by content_type and reason_label
    groups: dict[tuple[str, str], list[GateRejectionItem]] = {}
    for item in items:
        key = (item.content_type or "unknown", item.reason_label)
        if key not in groups:
            groups[key] = []
        groups[key].append(item)

    summaries = []
    for (content_type, reason_label), group_items in groups.items():
        # Sort by timestamp descending to get recent examples
        sorted_items = sorted(
            group_items,
            key=lambda x: x.created_at or "",
            reverse=True,
        )
        latest_timestamp = sorted_items[0].created_at if sorted_items else None

        # Collect unique recent examples
        recent_examples: list[str] = []
        seen_excerpts: set[str] = set()
        for item in sorted_items:
            if len(recent_examples) >= limit_examples:
                break
            excerpt = item.reason_excerpt
            if excerpt not in seen_excerpts:
                recent_examples.append(excerpt)
                seen_excerpts.add(excerpt)

        summaries.append(
            GateRejectionReasonSummary(
                content_type=content_type,
                reason_label=reason_label,
                rejection_count=len(group_items),
                latest_timestamp=latest_timestamp,
                recent_examples=tuple(recent_examples),
                report_id=_summary_report_id(content_type, reason_label),
            )
        )

    return tuple(sorted(summaries, key=lambda s: (s.content_type, s.reason_label)))


def _summary_report_id(content_type: str, reason_label: str) -> str:
    """Generate a deterministic report ID for a content_type/reason pair."""
    digest = hashlib.sha256(f"{content_type}:{reason_label}".encode("utf-8")).hexdigest()[:12]
    return f"gate_rejection_reason_{digest}"


def _excerpt(value: Any, *, limit: int = 240) -> str:
    """Truncate text to a maximum length."""
    text = str(value).strip()
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _first_text(row: Mapping[str, Any], columns: tuple[str, ...]) -> str | None:
    """Return the first non-empty string value from the given columns."""
    for column in columns:
        value = row.get(column)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _safe_int(value: Any) -> int | None:
    """Safely convert value to int or return None."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _parse_datetime(value: Any) -> datetime | None:
    """Parse a datetime string into a datetime object."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _ensure_utc(dt: datetime) -> datetime:
    """Ensure datetime is timezone-aware UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> Any:
    """Extract connection from database or connection object."""
    if hasattr(db_or_conn, "conn"):
        return db_or_conn.conn
    return db_or_conn


def _looks_like_rows(value: Any) -> bool:
    """Check if value looks like an iterable of rows rather than a database connection."""
    return not hasattr(value, "execute") and not hasattr(value, "conn") and not isinstance(
        value,
        (str, bytes),
    )


def _mapping(row: Any) -> dict[str, Any]:
    """Convert row to dict."""
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row)


def _load_pipeline_runs(conn: Any, cutoff: datetime) -> list[dict[str, Any]]:
    """Load pipeline_runs rows from database with rejection reasons."""
    try:
        cursor = conn.execute(
            """
            SELECT id, batch_id, content_type, rejection_reason, content_id,
                   outcome, filter_stats, created_at
            FROM pipeline_runs
            WHERE rejection_reason IS NOT NULL
                AND (created_at IS NULL OR created_at >= ?)
            ORDER BY created_at DESC, id DESC
            """,
            (cutoff.isoformat(),),
        )
        column_names = [description[0] for description in cursor.description]
        return [
            dict(row) if isinstance(row, Mapping) else dict(zip(column_names, row, strict=False))
            for row in cursor.fetchall()
        ]
    except Exception:
        return []
