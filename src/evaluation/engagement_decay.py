"""Engagement decay reporting for published social posts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal


Platform = Literal["all", "x", "bluesky"]

VALID_PLATFORMS = {"all", "x", "bluesky"}
PLATFORM_ALIASES = {
    "all": "all",
    "x": "x",
    "twitter": "x",
    "bluesky": "bluesky",
    "bsky": "bluesky",
}

DECLINE_RECOMMENDATION = "revisit_decline"
REPURPOSE_RECOMMENDATION = "repurpose_flattened"
FOLLOW_UP_RECOMMENDATION = "follow_up_flattened"
MONITOR_RECOMMENDATION = "monitor_growth"


@dataclass(frozen=True)
class EngagementDecayRow:
    """Engagement decay metrics for one post on one platform."""

    content_id: int
    platform: str
    content_type: str
    content_preview: str
    first_fetched_at: str
    latest_fetched_at: str
    snapshot_count: int
    first_score: float
    latest_score: float
    score_delta: float
    hours_observed: float
    decay_rate_per_day: float
    recommendation: str


@dataclass(frozen=True)
class EngagementDecayReport:
    """Engagement decay query result and applied metadata."""

    days: int
    platform: str
    limit: int | None
    row_count: int
    generated_at: str
    flattened_delta_threshold: float
    follow_up_rate_threshold: float
    rows: list[EngagementDecayRow]


@dataclass(frozen=True)
class _Snapshot:
    content_id: int
    platform: str
    content_type: str
    content_preview: str
    score: float
    fetched_at: str


def normalize_platform(platform: str | None) -> Platform:
    """Normalize public platform filter names."""
    normalized = PLATFORM_ALIASES.get((platform or "all").strip().lower())
    if normalized not in VALID_PLATFORMS:
        raise ValueError("platform must be one of: all, x, bluesky")
    return normalized  # type: ignore[return-value]


class EngagementDecayAnalyzer:
    """Find posts whose engagement momentum has flattened or declined."""

    def __init__(
        self,
        db,
        flattened_delta_threshold: float = 1.0,
        follow_up_rate_threshold: float = 0.5,
    ) -> None:
        self.db = db
        self.flattened_delta_threshold = flattened_delta_threshold
        self.follow_up_rate_threshold = follow_up_rate_threshold

    def analyze(
        self,
        days: int = 14,
        platform: str = "all",
        limit: int | None = None,
        now: datetime | None = None,
    ) -> EngagementDecayReport:
        """Compare earliest and latest snapshots for published posts.

        A row is returned only when a published post has at least two engagement
        snapshots for the selected platform within the requested window.
        """
        normalized_platform = normalize_platform(platform)
        if days <= 0:
            raise ValueError("days must be greater than zero")
        if limit is not None and limit <= 0:
            raise ValueError("limit must be greater than zero")

        observed_now = now or datetime.now(timezone.utc)
        if observed_now.tzinfo is None:
            observed_now = observed_now.replace(tzinfo=timezone.utc)
        cutoff = observed_now - timedelta(days=days)

        snapshots = self._fetch_snapshots(
            cutoff=cutoff.isoformat(),
            now=observed_now.isoformat(),
            platform=normalized_platform,
        )
        grouped: dict[tuple[str, int], list[_Snapshot]] = {}
        for snapshot in snapshots:
            grouped.setdefault((snapshot.platform, snapshot.content_id), []).append(snapshot)

        rows: list[EngagementDecayRow] = []
        for (_platform, _content_id), group in grouped.items():
            if len(group) < 2:
                continue
            group.sort(key=lambda item: item.fetched_at)
            first = group[0]
            latest = group[-1]
            hours_observed = max(
                (_parse_datetime(latest.fetched_at) - _parse_datetime(first.fetched_at)).total_seconds()
                / 3600,
                0.0,
            )
            score_delta = latest.score - first.score
            decay_rate_per_day = (score_delta / hours_observed * 24) if hours_observed else 0.0
            rows.append(
                EngagementDecayRow(
                    content_id=first.content_id,
                    platform=first.platform,
                    content_type=first.content_type,
                    content_preview=first.content_preview,
                    first_fetched_at=first.fetched_at,
                    latest_fetched_at=latest.fetched_at,
                    snapshot_count=len(group),
                    first_score=round(first.score, 2),
                    latest_score=round(latest.score, 2),
                    score_delta=round(score_delta, 2),
                    hours_observed=round(hours_observed, 2),
                    decay_rate_per_day=round(decay_rate_per_day, 2),
                    recommendation=self._recommendation(
                        first_score=first.score,
                        score_delta=score_delta,
                        decay_rate_per_day=decay_rate_per_day,
                    ),
                )
            )

        rows.sort(
            key=lambda row: (
                row.decay_rate_per_day,
                row.score_delta,
                -row.first_score,
                row.platform,
                row.content_id,
            )
        )
        if limit is not None:
            rows = rows[:limit]

        return EngagementDecayReport(
            days=days,
            platform=normalized_platform,
            limit=limit,
            row_count=len(rows),
            generated_at=observed_now.isoformat(),
            flattened_delta_threshold=self.flattened_delta_threshold,
            follow_up_rate_threshold=self.follow_up_rate_threshold,
            rows=rows,
        )

    def _fetch_snapshots(
        self,
        cutoff: str,
        now: str,
        platform: Platform,
    ) -> list[_Snapshot]:
        clauses = []
        params: list[object] = []
        if platform in {"all", "x"}:
            clauses.append(
                """SELECT gc.id AS content_id,
                          'x' AS platform,
                          gc.content_type AS content_type,
                          gc.content AS content,
                          pe.engagement_score AS engagement_score,
                          pe.fetched_at AS fetched_at
                   FROM post_engagement pe
                   INNER JOIN generated_content gc ON gc.id = pe.content_id
                   LEFT JOIN content_publications cp
                     ON cp.content_id = gc.id
                    AND cp.platform = 'x'
                    AND cp.status = 'published'
                   WHERE pe.engagement_score IS NOT NULL
                     AND pe.fetched_at >= ?
                     AND pe.fetched_at <= ?
                     AND (gc.published = 1 OR cp.id IS NOT NULL)"""
            )
            params.extend([cutoff, now])
        if platform in {"all", "bluesky"}:
            clauses.append(
                """SELECT gc.id AS content_id,
                          'bluesky' AS platform,
                          gc.content_type AS content_type,
                          gc.content AS content,
                          be.engagement_score AS engagement_score,
                          be.fetched_at AS fetched_at
                   FROM bluesky_engagement be
                   INNER JOIN generated_content gc ON gc.id = be.content_id
                   LEFT JOIN content_publications cp
                     ON cp.content_id = gc.id
                    AND cp.platform = 'bluesky'
                    AND cp.status = 'published'
                   WHERE be.engagement_score IS NOT NULL
                     AND be.fetched_at >= ?
                     AND be.fetched_at <= ?
                     AND (gc.published = 1 OR cp.id IS NOT NULL)"""
            )
            params.extend([cutoff, now])

        if not clauses:
            return []

        cursor = self.db.conn.execute(
            " UNION ALL ".join(clauses) + " ORDER BY platform, content_id, fetched_at",
            params,
        )
        return [
            _Snapshot(
                content_id=row["content_id"],
                platform=row["platform"],
                content_type=row["content_type"],
                content_preview=_preview(row["content"]),
                score=float(row["engagement_score"] or 0.0),
                fetched_at=row["fetched_at"],
            )
            for row in cursor.fetchall()
        ]

    def _recommendation(
        self,
        first_score: float,
        score_delta: float,
        decay_rate_per_day: float,
    ) -> str:
        if score_delta < 0:
            return DECLINE_RECOMMENDATION
        if first_score >= 10 and score_delta <= self.flattened_delta_threshold:
            return REPURPOSE_RECOMMENDATION
        if first_score >= 5 and decay_rate_per_day <= self.follow_up_rate_threshold:
            return FOLLOW_UP_RECOMMENDATION
        return MONITOR_RECOMMENDATION


def engagement_decay_report_to_dict(report: EngagementDecayReport) -> dict[str, object]:
    """Serialize an engagement decay report for JSON output."""
    return {
        "status": "ok" if report.rows else "empty",
        "days": report.days,
        "platform": report.platform,
        "limit": report.limit,
        "row_count": report.row_count,
        "generated_at": report.generated_at,
        "flattened_delta_threshold": report.flattened_delta_threshold,
        "follow_up_rate_threshold": report.follow_up_rate_threshold,
        "rows": [asdict(row) for row in report.rows],
    }


def format_engagement_decay_json(report: EngagementDecayReport) -> str:
    """Format an engagement decay report as stable JSON."""
    return json.dumps(engagement_decay_report_to_dict(report), indent=2, sort_keys=True)


def format_engagement_decay_table(report: EngagementDecayReport) -> str:
    """Format an engagement decay report as a stable text table."""
    lines = [
        "Engagement Decay Report",
        "=" * 90,
        f"Lookback: last {report.days} days",
        f"Platform: {report.platform}",
    ]
    if report.limit is not None:
        lines.append(f"Limit:    {report.limit}")
    lines.append("")

    if not report.rows:
        lines.append("No published posts had at least two engagement snapshots in the requested window.")
        return "\n".join(lines)

    headers = [
        "Platform",
        "Content ID",
        "First",
        "Latest",
        "Delta",
        "Hours",
        "Per Day",
        "Recommendation",
        "Preview",
    ]
    rendered_rows = [
        [
            row.platform,
            str(row.content_id),
            f"{row.first_score:.2f}",
            f"{row.latest_score:.2f}",
            f"{row.score_delta:.2f}",
            f"{row.hours_observed:.2f}",
            f"{row.decay_rate_per_day:.2f}",
            row.recommendation,
            row.content_preview,
        ]
        for row in report.rows
    ]
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rendered_rows))
        for index in range(len(headers))
    ]
    lines.append(
        "  ".join(header.ljust(widths[index]) for index, header in enumerate(headers))
    )
    lines.append("  ".join("-" * width for width in widths))
    for row in rendered_rows:
        lines.append(
            "  ".join(value.ljust(widths[index]) for index, value in enumerate(row))
        )
    return "\n".join(lines)


def _preview(content: str | None, max_length: int = 64) -> str:
    text = " ".join((content or "").split())
    if len(text) <= max_length:
        return text
    return text[: max_length - 3].rstrip() + "..."


def _parse_datetime(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed
