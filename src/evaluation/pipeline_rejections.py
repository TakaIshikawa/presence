"""Pipeline rejection taxonomy reporting.

Aggregates ``pipeline_runs`` rejection reasons and filter statistics into a
stable taxonomy that is easier to track than raw human-readable reasons.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from storage.db import Database


FILTER_CATEGORIES = {
    "char_limit_rejected": "filter.char_limit",
    "repetition_rejected": "filter.repetition",
    "stale_pattern_rejected": "filter.stale_pattern",
    "topic_saturated_rejected": "filter.topic_saturation",
    "semantic_dedup_rejected": "filter.semantic_dedup",
    "claim_check_rejected": "filter.claim_check",
}

REJECTION_REASON_EXAMPLES_LIMIT = 5


@dataclass
class RejectionCategory:
    """Aggregated count for one normalized rejection category."""

    category: str
    count: int = 0
    content_types: dict[str, int] = field(default_factory=dict)
    raw_examples: list[str] = field(default_factory=list)
    filter_keys: dict[str, int] = field(default_factory=dict)


@dataclass
class ParseWarning:
    """Malformed filter_stats row surfaced in JSON output."""

    run_id: int | None
    batch_id: str | None
    content_type: str | None
    message: str


@dataclass
class PipelineRejectionReport:
    """Taxonomy report for pipeline rejections over a time window."""

    days: int
    content_type: str | None
    period_start: datetime | None
    period_end: datetime
    total_runs: int
    rejected_runs: int
    categories: list[RejectionCategory]
    parse_warnings: list[ParseWarning]


class PipelineRejectionAnalytics:
    """Analyze why pipeline runs failed to publish."""

    def __init__(self, db: Database):
        self.db = db

    def report(
        self,
        days: int = 30,
        content_type: str | None = None,
        min_count: int = 1,
    ) -> PipelineRejectionReport:
        """Build a rejection taxonomy report.

        ``filter_stats`` counters are added as their own categories, so category
        totals can exceed ``rejected_runs``.
        """
        rows = self._fetch_rows(days=days, content_type=content_type)
        rejected_rows = [row for row in rows if self._is_rejected_run(row)]
        categories: dict[str, RejectionCategory] = {}
        warnings: list[ParseWarning] = []

        for row in rejected_rows:
            reason = (row.get("rejection_reason") or "").strip()
            outcome_category = normalize_rejection_reason(
                reason=reason,
                outcome=row.get("outcome"),
                final_score=row.get("final_score"),
            )
            self._add_count(
                categories,
                outcome_category,
                row.get("content_type") or "unknown",
                1,
                reason or row.get("outcome") or "No rejection reason recorded",
            )

            parsed_filter_stats = parse_filter_stats(row, warnings)
            if parsed_filter_stats:
                for category, key, count in iter_filter_rejections(parsed_filter_stats):
                    self._add_count(
                        categories,
                        category,
                        row.get("content_type") or "unknown",
                        count,
                        reason or row.get("outcome") or f"filter_stats.{key}",
                        filter_key=key,
                    )

        ranked_categories = sorted(
            (category for category in categories.values() if category.count >= min_count),
            key=lambda category: (-category.count, category.category),
        )

        period_start = _oldest_created_at(rows)
        return PipelineRejectionReport(
            days=days,
            content_type=content_type,
            period_start=period_start,
            period_end=datetime.now(timezone.utc),
            total_runs=len(rows),
            rejected_runs=len(rejected_rows),
            categories=ranked_categories,
            parse_warnings=warnings,
        )

    def _fetch_rows(self, days: int, content_type: str | None) -> list[dict]:
        params: list[Any] = [f"-{days} days"]
        content_filter = ""
        if content_type:
            content_filter = " AND content_type = ?"
            params.append(content_type)

        cursor = self.db.conn.execute(
            f"""SELECT id, batch_id, content_type, outcome, published, final_score,
                       rejection_reason, filter_stats, created_at
                FROM pipeline_runs
                WHERE created_at >= datetime('now', ?){content_filter}
                ORDER BY created_at DESC""",
            params,
        )
        return [dict(row) for row in cursor.fetchall()]

    @staticmethod
    def _is_rejected_run(row: dict) -> bool:
        outcome = (row.get("outcome") or "").strip().lower()
        published = row.get("published")
        return bool(
            row.get("rejection_reason")
            or outcome in {"below_threshold", "all_filtered", "dry_run"}
            or (published is not None and int(published) == 0 and outcome != "published")
        )

    @staticmethod
    def _add_count(
        categories: dict[str, RejectionCategory],
        category_name: str,
        content_type: str,
        count: int,
        raw_reason: str,
        filter_key: str | None = None,
    ) -> None:
        category = categories.setdefault(
            category_name,
            RejectionCategory(category=category_name),
        )
        category.count += count
        category.content_types[content_type] = category.content_types.get(content_type, 0) + count
        if raw_reason and raw_reason not in category.raw_examples:
            if len(category.raw_examples) < REJECTION_REASON_EXAMPLES_LIMIT:
                category.raw_examples.append(raw_reason)
        if filter_key:
            category.filter_keys[filter_key] = category.filter_keys.get(filter_key, 0) + count


def normalize_rejection_reason(
    reason: str | None,
    outcome: str | None = None,
    final_score: float | None = None,
) -> str:
    """Map raw rejection text and outcome values into stable categories."""
    reason_text = (reason or "").strip()
    text = reason_text.lower()
    normalized_outcome = (outcome or "").strip().lower()

    if normalized_outcome == "dry_run" or "dry run" in text or "dry-run" in text:
        return "dry_run"
    if normalized_outcome == "all_filtered" or "all candidates filtered" in text:
        return "all_filtered"
    if (
        normalized_outcome == "below_threshold"
        or "below threshold" in text
        or (final_score is not None and _mentions_threshold(text))
    ):
        return "below_threshold"
    if "persona guard failed" in text:
        return "filter.persona_guard"
    if "thread validation failed" in text:
        return "filter.thread_validation"
    if "model usage budget exceeded" in text or "budget gate" in text:
        return "budget"
    if "rate limited" in text:
        return "rate_limited"
    if "post failed" in text:
        return "publish_failed"
    if "write failed" in text:
        return "write_failed"
    if "git push failed" in text:
        return "git_push_failed"
    if not reason_text and normalized_outcome:
        return normalized_outcome
    return "unknown"


def parse_filter_stats(row: dict, warnings: list[ParseWarning]) -> dict | None:
    """Parse filter_stats JSON and collect row-level warnings."""
    raw = row.get("filter_stats")
    if raw in (None, ""):
        return None
    if isinstance(raw, dict):
        return raw

    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as exc:
        warnings.append(
            ParseWarning(
                run_id=row.get("id"),
                batch_id=row.get("batch_id"),
                content_type=row.get("content_type"),
                message=f"Malformed filter_stats JSON: {exc}",
            )
        )
        return None

    if not isinstance(parsed, dict):
        warnings.append(
            ParseWarning(
                run_id=row.get("id"),
                batch_id=row.get("batch_id"),
                content_type=row.get("content_type"),
                message="Malformed filter_stats JSON: expected object",
            )
        )
        return None
    return parsed


def iter_filter_rejections(stats: dict) -> list[tuple[str, str, int]]:
    """Return normalized filter categories from parsed filter_stats."""
    rejections: list[tuple[str, str, int]] = []
    for key, value in stats.items():
        if key in FILTER_CATEGORIES and isinstance(value, (int, float)) and value > 0:
            rejections.append((FILTER_CATEGORIES[key], key, int(value)))
        elif key.endswith("_rejected") and isinstance(value, (int, float)) and value > 0:
            rejections.append((f"filter.{key.removesuffix('_rejected')}", key, int(value)))

    persona_guard = stats.get("persona_guard")
    if isinstance(persona_guard, dict) and persona_guard.get("passed") is False:
        rejections.append(("filter.persona_guard", "persona_guard", 1))

    if stats.get("thread_validation_valid") is False:
        rejections.append(("filter.thread_validation", "thread_validation_valid", 1))

    final_claims = stats.get("claim_check_final_unsupported")
    if isinstance(final_claims, list) and final_claims:
        rejections.append(("filter.claim_check_final", "claim_check_final_unsupported", len(final_claims)))

    return rejections


def _mentions_threshold(text: str) -> bool:
    return bool(re.search(r"\bthreshold\b", text))


def _oldest_created_at(rows: list[dict]) -> datetime | None:
    if not rows:
        return None

    created_at = rows[-1].get("created_at")
    if not created_at:
        return None
    try:
        parsed = datetime.fromisoformat(created_at)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed
