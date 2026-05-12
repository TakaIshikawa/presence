"""Report generated-content themes with weak knowledge source coverage."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_MIN_GAP_SCORE = 1
DEFAULT_MIN_SOURCES = 2
DEFAULT_MAX_SOURCE_AGE_DAYS = 90


@dataclass(frozen=True)
class SourceCoverageGap:
    """One weakly covered theme."""

    theme: str
    candidate_count: int
    matched_source_count: int
    freshest_source_age_days: int | None
    gap_score: int
    reason_codes: tuple[str, ...]
    suggested_next_ingestion_action: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reason_codes"] = list(self.reason_codes)
        return payload


@dataclass(frozen=True)
class SourceCoverageGapsReport:
    """Knowledge source coverage gaps report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    gaps: tuple[SourceCoverageGap, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "knowledge_source_coverage_gaps",
            "filters": dict(self.filters),
            "gaps": [gap.to_dict() for gap in self.gaps],
            "generated_at": self.generated_at,
            "gap_count": len(self.gaps),
            "totals": dict(self.totals),
        }


def build_source_coverage_gaps_report(
    db_or_conn: Any,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_gap_score: int = DEFAULT_MIN_GAP_SCORE,
    min_sources: int = DEFAULT_MIN_SOURCES,
    max_source_age_days: int = DEFAULT_MAX_SOURCE_AGE_DAYS,
    now: datetime | None = None,
) -> SourceCoverageGapsReport:
    """Compare recent generated content themes with available knowledge sources."""
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if min_gap_score < 0:
        raise ValueError("min_gap_score must be non-negative")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    conn = _connection(db_or_conn)
    themes = _load_themes(conn, cutoff)
    sources = _load_sources(conn)
    gaps = [
        _build_gap(theme, content_ids, sources, generated_at, min_sources, max_source_age_days)
        for theme, content_ids in themes.items()
    ]
    gaps = [gap for gap in gaps if gap.gap_score >= min_gap_score]
    gaps.sort(key=lambda gap: (-gap.gap_score, -gap.candidate_count, gap.theme))
    return SourceCoverageGapsReport(
        generated_at=generated_at.isoformat(),
        filters={
            "lookback_days": lookback_days,
            "cutoff": cutoff.isoformat(),
            "min_gap_score": min_gap_score,
            "min_sources": min_sources,
            "max_source_age_days": max_source_age_days,
        },
        totals={
            "theme_count": len(themes),
            "knowledge_source_count": len(sources),
            "gap_count": len(gaps),
        },
        gaps=tuple(gaps),
    )


def format_source_coverage_gaps_json(report: SourceCoverageGapsReport) -> str:
    """Serialize as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_source_coverage_gaps_text(report: SourceCoverageGapsReport) -> str:
    """Render a compact source coverage gaps report."""
    lines = [
        "Knowledge Source Coverage Gaps",
        f"Generated: {report.generated_at}",
        (
            f"Window: lookback_days={report.filters['lookback_days']} "
            f"min_gap_score={report.filters['min_gap_score']}"
        ),
        (
            f"Totals: themes={report.totals['theme_count']} "
            f"sources={report.totals['knowledge_source_count']} gaps={report.totals['gap_count']}"
        ),
    ]
    if not report.gaps:
        lines.extend(["", "No knowledge source coverage gaps found."])
        return "\n".join(lines)
    lines.extend(["", "Gaps:"])
    for gap in report.gaps:
        age = "-" if gap.freshest_source_age_days is None else str(gap.freshest_source_age_days)
        lines.append(
            f"- theme={gap.theme} candidates={gap.candidate_count} "
            f"sources={gap.matched_source_count} freshest_age_days={age} "
            f"gap_score={gap.gap_score} reasons={','.join(gap.reason_codes)}"
        )
        lines.append(f"  next={gap.suggested_next_ingestion_action}")
    return "\n".join(lines)


def _load_themes(conn: sqlite3.Connection, cutoff: datetime) -> dict[str, set[int]]:
    tables = {row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not {"generated_content", "content_topics"} <= tables:
        return {}
    rows = conn.execute(
        """SELECT lower(trim(ct.topic)) AS theme, gc.id AS content_id
             FROM content_topics ct
             INNER JOIN generated_content gc ON gc.id = ct.content_id
             WHERE datetime(gc.created_at) >= datetime(?)
               AND trim(ct.topic) != ''
             ORDER BY theme ASC, gc.id ASC""",
        (cutoff.isoformat(),),
    ).fetchall()
    themes: dict[str, set[int]] = {}
    for row in rows:
        themes.setdefault(row["theme"], set()).add(int(row["content_id"]))
    return themes


def _load_sources(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    tables = {row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "knowledge" not in tables:
        return []
    return [
        dict(row)
        for row in conn.execute(
            """SELECT id, content, insight, approved, published_at, ingested_at, created_at
                 FROM knowledge
                 WHERE COALESCE(approved, 0) = 1
                 ORDER BY id ASC"""
        ).fetchall()
    ]


def _build_gap(
    theme: str,
    content_ids: set[int],
    sources: list[dict[str, Any]],
    now: datetime,
    min_sources: int,
    max_source_age_days: int,
) -> SourceCoverageGap:
    matched = [source for source in sources if _matches_theme(source, theme)]
    ages = [
        (now - parsed).days
        for parsed in (_parse_dt(source.get("published_at") or source.get("ingested_at") or source.get("created_at")) for source in matched)
        if parsed is not None
    ]
    freshest_age = min(ages) if ages else None
    reason_codes: list[str] = []
    gap_score = 0
    if not matched:
        reason_codes.append("uncovered_theme")
        gap_score += 70
    elif len(matched) < min_sources:
        reason_codes.append("low_source_count")
        gap_score += 35 + (min_sources - len(matched)) * 10
    if matched and (freshest_age is None or freshest_age > max_source_age_days):
        reason_codes.append("stale_only_coverage")
        gap_score += 40
    if len(content_ids) > 1:
        reason_codes.append("repeated_candidate_theme")
        gap_score += min(20, len(content_ids) * 3)
    return SourceCoverageGap(
        theme=theme,
        candidate_count=len(content_ids),
        matched_source_count=len(matched),
        freshest_source_age_days=freshest_age,
        gap_score=min(100, gap_score),
        reason_codes=tuple(reason_codes),
        suggested_next_ingestion_action=_suggest_action(reason_codes, theme),
    )


def _matches_theme(source: dict[str, Any], theme: str) -> bool:
    haystack = f"{source.get('content') or ''} {source.get('insight') or ''}".lower()
    return theme in haystack


def _suggest_action(reason_codes: list[str], theme: str) -> str:
    if "uncovered_theme" in reason_codes:
        return f"ingest new approved sources for {theme}"
    if "stale_only_coverage" in reason_codes:
        return f"refresh or recrawl sources for {theme}"
    return f"add more corroborating sources for {theme}"


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
