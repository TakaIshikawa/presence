"""Plan visual asset opportunities for recent blog-oriented content."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 10
PREVIEW_LENGTH = 140

_TECH_RE = re.compile(
    r"\b(api|architecture|database|sqlite|schema|queue|scheduler|latency|pipeline|"
    r"workflow|migration|reliability|diagram|system|model|prompt|variant)\b",
    re.IGNORECASE,
)
_LESSON_RE = re.compile(r"\b(learned|lesson|mistake|takeaway|changed my mind|next time)\b", re.I)
_EVIDENCE_RE = re.compile(r"\b(commit|source|trace|metric|data|before|after|example|proof)\b", re.I)


@dataclass(frozen=True)
class BlogVisualExcludedItem:
    """A scanned blog-oriented row that does not need a new visual plan."""

    content_id: int
    reason: str
    content_type: str | None = None
    image_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BlogVisualOpportunity:
    """One recommended visual asset opportunity for a blog-oriented row."""

    content_id: int
    priority: str
    score: float
    recommended_visual_type: str
    title_card_suitability: str
    source_evidence_to_include: tuple[str, ...]
    rationale: tuple[str, ...]
    content_type: str | None
    text_source: str
    content_preview: str
    word_count: int
    engagement_score: float
    planned_topic_id: int | None = None
    planned_topic: str | None = None
    planned_angle: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["source_evidence_to_include"] = list(self.source_evidence_to_include)
        data["rationale"] = list(self.rationale)
        return data


@dataclass(frozen=True)
class BlogVisualOpportunityReport:
    """Read-only blog visual opportunity plan."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    counts: dict[str, int]
    opportunity_ids: tuple[int, ...]
    opportunities: tuple[BlogVisualOpportunity, ...]
    excluded: tuple[BlogVisualExcludedItem, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "counts": self.counts,
            "excluded": [item.to_dict() for item in self.excluded],
            "filters": self.filters,
            "generated_at": self.generated_at,
            "opportunities": [item.to_dict() for item in self.opportunities],
            "opportunity_ids": list(self.opportunity_ids),
        }


def build_blog_visual_opportunity_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> BlogVisualOpportunityReport:
    """Build a deterministic read-only visual opportunity plan for blog content."""

    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    start = generated_at - timedelta(days=days)
    rows = _load_recent_blog_rows(db_or_conn, start=start, end=generated_at)
    opportunities, excluded = _build_opportunities(rows)
    selected = tuple(opportunities[:limit])
    return BlogVisualOpportunityReport(
        artifact_type="blog_visual_opportunities",
        generated_at=generated_at.isoformat(),
        filters={"days": days, "limit": limit},
        counts={
            "scanned": len(rows),
            "eligible": len(opportunities),
            "selected": len(selected),
            "covered": sum(1 for item in excluded if item.reason == "already_has_image_path"),
            "excluded": len(excluded),
        },
        opportunity_ids=tuple(item.content_id for item in selected),
        opportunities=selected,
        excluded=tuple(sorted(excluded, key=lambda item: item.content_id)),
    )


def format_blog_visual_opportunity_json(report: BlogVisualOpportunityReport) -> str:
    """Serialize a blog visual opportunity report as deterministic JSON."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_blog_visual_opportunity_text(report: BlogVisualOpportunityReport) -> str:
    """Format a blog visual opportunity report for operator review."""

    lines = [
        "Blog Visual Opportunities",
        f"Generated: {report.generated_at}",
        f"Filters: days={report.filters['days']} limit={report.filters['limit']}",
        (
            f"Counts: scanned={report.counts['scanned']} eligible={report.counts['eligible']} "
            f"selected={report.counts['selected']} covered={report.counts['covered']} "
            f"excluded={report.counts['excluded']}"
        ),
    ]
    if not report.opportunities:
        lines.append("")
        lines.append("No blog visual opportunities matched the filters.")
    else:
        lines.append("")
        lines.append("Opportunities:")
        for item in report.opportunities:
            planned = f" planned_topic={item.planned_topic_id}" if item.planned_topic_id else ""
            evidence = ", ".join(item.source_evidence_to_include) or "-"
            lines.append(
                f"- content={item.content_id} priority={item.priority} score={item.score:g} "
                f"type={item.recommended_visual_type} title_card={item.title_card_suitability}"
                f"{planned}"
            )
            lines.append(f"  {item.content_preview}")
            lines.append(f"  evidence: {evidence}")
            lines.append(f"  rationale: {'; '.join(item.rationale)}")

    if report.excluded:
        lines.append("")
        lines.append("Excluded:")
        for item in report.excluded:
            path = f" image_path={item.image_path}" if item.image_path else ""
            lines.append(f"- content={item.content_id} reason={item.reason}{path}")
    return "\n".join(lines)


def _load_recent_blog_rows(
    db_or_conn: Any,
    *,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    rows = conn.execute(
        """WITH blog_variants AS (
               SELECT content_id, content, platform, variant_type, metadata, selected, created_at,
                      ROW_NUMBER() OVER (
                          PARTITION BY content_id
                          ORDER BY
                              CASE WHEN platform = 'blog' AND selected = 1 THEN 0
                                   WHEN platform = 'blog' THEN 1
                                   WHEN selected = 1 THEN 2
                                   ELSE 3
                              END,
                              created_at DESC,
                              id DESC
                      ) AS rn
               FROM content_variants
               WHERE platform = 'blog' OR variant_type LIKE '%blog%'
           ),
           latest_engagement AS (
               SELECT content_id, SUM(score) AS engagement_score
               FROM (
                   SELECT content_id, engagement_score AS score FROM post_engagement
                   UNION ALL
                   SELECT content_id, engagement_score AS score FROM linkedin_engagement
                   UNION ALL
                   SELECT content_id, engagement_score AS score FROM bluesky_engagement
                   UNION ALL
                   SELECT content_id, engagement_score AS score FROM mastodon_engagement
               )
               WHERE score IS NOT NULL
               GROUP BY content_id
           )
           SELECT gc.id,
                  gc.content_type,
                  gc.content_format,
                  gc.content,
                  gc.eval_score,
                  gc.source_commits,
                  gc.source_messages,
                  gc.source_activity_ids,
                  gc.image_path,
                  gc.image_prompt,
                  gc.created_at,
                  gc.published_at,
                  bv.content AS variant_content,
                  bv.platform AS variant_platform,
                  bv.variant_type,
                  bv.metadata AS variant_metadata,
                  COALESCE(le.engagement_score, 0) AS engagement_score,
                  pt.id AS planned_topic_id,
                  pt.topic AS planned_topic,
                  pt.angle AS planned_angle,
                  pt.source_material AS planned_source_material,
                  pt.target_date AS planned_target_date
           FROM generated_content gc
           LEFT JOIN blog_variants bv ON bv.content_id = gc.id AND bv.rn = 1
           LEFT JOIN latest_engagement le ON le.content_id = gc.id
           LEFT JOIN planned_topics pt ON pt.content_id = gc.id
           WHERE COALESCE(gc.published_at, gc.created_at) >= ?
             AND COALESCE(gc.published_at, gc.created_at) <= ?
             AND (gc.content_type IN ('blog', 'blog_post', 'long_post') OR bv.content_id IS NOT NULL)
           ORDER BY COALESCE(gc.published_at, gc.created_at) DESC, gc.id DESC""",
        (start.isoformat(), end.isoformat()),
    ).fetchall()
    return [dict(row) for row in rows]


def _build_opportunities(
    rows: list[dict[str, Any]],
) -> tuple[list[BlogVisualOpportunity], list[BlogVisualExcludedItem]]:
    opportunities: list[BlogVisualOpportunity] = []
    excluded: list[BlogVisualExcludedItem] = []
    for row in rows:
        content_id = int(row["id"])
        image_path = _clean_text(row.get("image_path"))
        if image_path:
            excluded.append(
                BlogVisualExcludedItem(
                    content_id=content_id,
                    reason="already_has_image_path",
                    content_type=row.get("content_type"),
                    image_path=image_path,
                )
            )
            continue

        text, text_source = _blog_text(row)
        if not text:
            excluded.append(
                BlogVisualExcludedItem(
                    content_id=content_id,
                    reason="missing_blog_text",
                    content_type=row.get("content_type"),
                )
            )
            continue

        word_count = len(re.findall(r"\S+", text))
        engagement_score = round(float(row.get("engagement_score") or 0.0), 2)
        score, score_reasons = _score_row(row, word_count=word_count, engagement_score=engagement_score)
        visual_type = _visual_type(row, text)
        title_card = _title_card_suitability(row, word_count=word_count)
        evidence = _source_evidence(row, text)
        rationale = (
            *score_reasons,
            f"recommended {visual_type}",
            f"title card suitability {title_card}",
            f"text source: {text_source}",
        )
        opportunities.append(
            BlogVisualOpportunity(
                content_id=content_id,
                priority=_priority(score),
                score=round(score, 2),
                recommended_visual_type=visual_type,
                title_card_suitability=title_card,
                source_evidence_to_include=evidence,
                rationale=tuple(rationale),
                content_type=row.get("content_type"),
                text_source=text_source,
                content_preview=_preview(text),
                word_count=word_count,
                engagement_score=engagement_score,
                planned_topic_id=_int_or_none(row.get("planned_topic_id")),
                planned_topic=row.get("planned_topic"),
                planned_angle=row.get("planned_angle"),
            )
        )
    opportunities.sort(key=lambda item: (-item.score, item.content_id))
    return opportunities, excluded


def _blog_text(row: dict[str, Any]) -> tuple[str, str]:
    variant = _clean_text(row.get("variant_content"))
    if variant:
        platform = row.get("variant_platform") or "variant"
        variant_type = row.get("variant_type") or "copy"
        return variant, f"content_variants:{platform}:{variant_type}"
    original = _clean_text(row.get("content"))
    if original:
        return original, "generated_content.content"
    return "", ""


def _score_row(row: dict[str, Any], *, word_count: int, engagement_score: float) -> tuple[float, tuple[str, ...]]:
    score = 25.0
    reasons = ["missing image_path"]
    if word_count >= 700:
        score += 25.0
        reasons.append(f"long blog draft ({word_count} words)")
    elif word_count >= 250:
        score += 15.0
        reasons.append(f"substantial blog draft ({word_count} words)")
    else:
        score += 6.0
        reasons.append(f"short blog draft ({word_count} words)")

    if engagement_score:
        score += min(engagement_score, 40.0)
        reasons.append(f"engagement history {engagement_score:g}")

    eval_score = float(row.get("eval_score") or 0.0)
    if eval_score:
        score += min(eval_score * 2.0, 20.0)
        reasons.append(f"evaluation score {eval_score:g}")

    if row.get("planned_topic_id"):
        score += 12.0
        topic = _clean_text(row.get("planned_topic")) or f"#{row['planned_topic_id']}"
        reasons.append(f"planned topic context: {topic}")
    if _clean_text(row.get("variant_content")):
        score += 5.0
        reasons.append("blog-related variant available")
    return score, tuple(reasons)


def _visual_type(row: dict[str, Any], text: str) -> str:
    planned_topic = _clean_text(row.get("planned_topic"))
    planned_angle = _clean_text(row.get("planned_angle"))
    if planned_topic and planned_angle:
        return "title card"
    if _TECH_RE.search(text):
        return "concept diagram"
    if _LESSON_RE.search(text):
        return "pull-quote card"
    if _EVIDENCE_RE.search(text) or _source_evidence(row, text):
        return "evidence card"
    return "social preview image"


def _title_card_suitability(row: dict[str, Any], *, word_count: int) -> str:
    if _clean_text(row.get("planned_topic")) and _clean_text(row.get("planned_angle")):
        return "high"
    if _clean_text(row.get("planned_topic")) or word_count >= 250:
        return "medium"
    return "low"


def _source_evidence(row: dict[str, Any], text: str) -> tuple[str, ...]:
    evidence: list[str] = []
    planned_source = _clean_text(row.get("planned_source_material"))
    if planned_source:
        evidence.append(f"planned source material: {_preview(planned_source, 80)}")
    for label, key in (
        ("commit", "source_commits"),
        ("message", "source_messages"),
        ("activity", "source_activity_ids"),
    ):
        values = _parse_json_list(row.get(key))
        if values:
            evidence.append(f"{label}: {', '.join(values[:3])}")
    if _clean_text(row.get("image_prompt")):
        evidence.append("existing image prompt")
    if not evidence and _EVIDENCE_RE.search(text):
        evidence.append("specific examples or metrics from the draft")
    return tuple(evidence[:4])


def _priority(score: float) -> str:
    if score >= 75:
        return "high"
    if score >= 50:
        return "medium"
    return "low"


def _parse_json_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        parsed = value
    else:
        try:
            parsed = json.loads(str(value))
        except (TypeError, json.JSONDecodeError):
            return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _preview(value: str, width: int = PREVIEW_LENGTH) -> str:
    text = _clean_text(value)
    if len(text) <= width:
        return text
    return text[: width - 3].rstrip() + "..."


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
