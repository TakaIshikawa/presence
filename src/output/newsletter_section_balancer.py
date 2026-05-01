"""Plan a balanced weekly newsletter layout from recent generated content."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_MAX_ITEMS = 5
PREVIEW_LENGTH = 140

SECTION_SHIPPED_WORK = "shipped work"
SECTION_TECHNICAL_NOTE = "technical note"
SECTION_LESSON_LEARNED = "lesson learned"
SECTION_EXTERNAL_LINK = "external link"
SECTION_CALL_TO_ACTION = "call-to-action"

SOURCE_COMMIT = "commit"
SOURCE_MESSAGE = "message"
SOURCE_GITHUB_ACTIVITY = "github_activity"
SOURCE_ORIGINAL = "generated_content"

_URL_RE = re.compile(r"https?://\S+")
_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_-]*")
_CTA_RE = re.compile(
    r"\b(reply|tell me|try this|join|subscribe|sign up|read more|what would you|"
    r"what do you|share|send me|let me know)\b",
    re.IGNORECASE,
)
_LESSON_RE = re.compile(r"\b(learned|lesson|mistake|next time|changed my mind|takeaway)\b", re.I)
_SHIPPED_RE = re.compile(r"\b(shipped|launched|released|built|added|fixed|implemented)\b", re.I)
_TECH_RE = re.compile(
    r"\b(api|database|sqlite|schema|queue|scheduler|pytest|migration|latency|"
    r"reliability|pipeline|cli|metadata|variant|source|thread|model)\b",
    re.IGNORECASE,
)
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "into",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "this",
    "to",
    "with",
}


@dataclass(frozen=True)
class NewsletterExcludedItem:
    """A recent generated content row excluded from newsletter planning."""

    content_id: int
    reason: str
    content_type: str | None = None
    content_format: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterCandidate:
    """One publishable candidate considered for the newsletter layout."""

    content_id: int
    section_label: str
    content_type: str | None
    content_format: str
    source_type: str
    topics: tuple[str, ...]
    publishable_text: str
    text_source: str
    created_at: str | None
    published_at: str | None
    publication_statuses: tuple[str, ...]
    engagement_score: float
    base_score: float
    rationale: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["topics"] = list(self.topics)
        data["publication_statuses"] = list(self.publication_statuses)
        data["content_preview"] = _preview(self.publishable_text)
        del data["publishable_text"]
        return data


@dataclass(frozen=True)
class NewsletterPlanItem:
    """One selected newsletter item with final ordering metadata."""

    order: int
    content_id: int
    section_label: str
    content_type: str | None
    content_format: str
    source_type: str
    topics: tuple[str, ...]
    text_source: str
    engagement_score: float
    score: float
    rationale: tuple[str, ...]
    content_preview: str

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["topics"] = list(self.topics)
        data["rationale"] = list(self.rationale)
        return data


@dataclass(frozen=True)
class NewsletterSectionBalanceReport:
    """Read-only newsletter section balancing plan."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    counts: dict[str, int]
    selected_item_ids: tuple[int, ...]
    items: tuple[NewsletterPlanItem, ...]
    excluded: tuple[NewsletterExcludedItem, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "counts": self.counts,
            "excluded": [item.to_dict() for item in self.excluded],
            "filters": self.filters,
            "generated_at": self.generated_at,
            "items": [item.to_dict() for item in self.items],
            "selected_item_ids": list(self.selected_item_ids),
        }


def build_newsletter_section_balance_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    max_items: int = DEFAULT_MAX_ITEMS,
    now: datetime | None = None,
) -> NewsletterSectionBalanceReport:
    """Build a deterministic read-only weekly newsletter layout plan."""

    if days <= 0:
        raise ValueError("days must be positive")
    if max_items <= 0:
        raise ValueError("max_items must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    start = generated_at - timedelta(days=days)
    rows = _load_recent_content(db_or_conn, start=start, end=generated_at)
    candidates, excluded = _build_candidates(rows)
    ordered = _balance_order(candidates, max_items=max_items)
    items = tuple(_plan_item(candidate, index) for index, candidate in enumerate(ordered, start=1))
    selected_ids = tuple(item.content_id for item in items)
    return NewsletterSectionBalanceReport(
        artifact_type="newsletter_section_balance",
        generated_at=generated_at.isoformat(),
        filters={"days": days, "max_items": max_items},
        counts={
            "scanned": len(rows),
            "eligible": len(candidates),
            "selected": len(items),
            "excluded": len(excluded),
        },
        selected_item_ids=selected_ids,
        items=items,
        excluded=tuple(sorted(excluded, key=lambda item: item.content_id)),
    )


def format_newsletter_section_balance_json(report: NewsletterSectionBalanceReport) -> str:
    """Serialize a newsletter balance report as deterministic JSON."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_section_balance_text(report: NewsletterSectionBalanceReport) -> str:
    """Format a newsletter section balance report for operator review."""

    lines = [
        "Newsletter Section Balance",
        f"Generated: {report.generated_at}",
        f"Filters: days={report.filters['days']} max_items={report.filters['max_items']}",
        (
            f"Counts: scanned={report.counts['scanned']} eligible={report.counts['eligible']} "
            f"selected={report.counts['selected']} excluded={report.counts['excluded']}"
        ),
    ]
    if not report.items:
        lines.append("")
        lines.append("No publishable newsletter items matched the filters.")
    else:
        lines.append("")
        lines.append("Layout:")
        for item in report.items:
            topics = ",".join(item.topics) if item.topics else "-"
            lines.append(
                f"- {item.order}. content={item.content_id} section={item.section_label} "
                f"format={item.content_format} source={item.source_type} topics={topics} "
                f"score={item.score:g}"
            )
            lines.append(f"  {item.content_preview}")
            lines.append(f"  rationale: {'; '.join(item.rationale)}")

    if report.excluded:
        lines.append("")
        lines.append("Excluded:")
        for item in report.excluded:
            lines.append(f"- content={item.content_id} reason={item.reason}")
    return "\n".join(lines)


def _load_recent_content(
    db_or_conn: Any,
    *,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    start_text = start.isoformat()
    end_text = end.isoformat()
    rows = conn.execute(
        """WITH selected_variants AS (
               SELECT content_id, content, platform, variant_type, selected, created_at,
                      ROW_NUMBER() OVER (
                          PARTITION BY content_id
                          ORDER BY
                              CASE
                                  WHEN platform = 'newsletter' AND selected = 1 THEN 0
                                  WHEN platform = 'newsletter' THEN 1
                                  WHEN selected = 1 THEN 2
                                  ELSE 3
                              END,
                              created_at DESC,
                              id DESC
                      ) AS rn
               FROM content_variants
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
           ),
           topic_summary AS (
               SELECT content_id, json_group_array(topic) AS topics
               FROM (
                   SELECT DISTINCT content_id, topic
                   FROM content_topics
                   ORDER BY content_id ASC, topic ASC
               )
               GROUP BY content_id
           ),
           publication_summary AS (
               SELECT content_id, json_group_array(platform || ':' || status) AS publication_statuses
               FROM (
                   SELECT content_id, platform, status
                   FROM content_publications
                   ORDER BY content_id ASC, platform ASC
               )
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
                  gc.created_at,
                  gc.published_at,
                  gc.published_url,
                  gc.published,
                  sv.content AS variant_content,
                  sv.platform AS variant_platform,
                  sv.variant_type,
                  sv.selected AS variant_selected,
                  COALESCE(le.engagement_score, 0) AS engagement_score,
                  topic_summary.topics,
                  publication_summary.publication_statuses
           FROM generated_content gc
           LEFT JOIN selected_variants sv ON sv.content_id = gc.id AND sv.rn = 1
           LEFT JOIN latest_engagement le ON le.content_id = gc.id
           LEFT JOIN topic_summary ON topic_summary.content_id = gc.id
           LEFT JOIN publication_summary ON publication_summary.content_id = gc.id
           WHERE COALESCE(gc.published_at, gc.created_at) >= ?
             AND COALESCE(gc.published_at, gc.created_at) <= ?
           ORDER BY COALESCE(gc.published_at, gc.created_at) DESC, gc.id DESC""",
        (start_text, end_text),
    ).fetchall()
    return [dict(row) for row in rows]


def _build_candidates(
    rows: list[dict[str, Any]],
) -> tuple[list[NewsletterCandidate], list[NewsletterExcludedItem]]:
    candidates: list[NewsletterCandidate] = []
    excluded: list[NewsletterExcludedItem] = []
    for row in rows:
        content_id = int(row["id"])
        text, text_source = _publishable_text(row)
        if not text:
            excluded.append(
                NewsletterExcludedItem(
                    content_id=content_id,
                    reason="missing_publishable_text",
                    content_type=row.get("content_type"),
                    content_format=row.get("content_format"),
                )
            )
            continue

        topics = tuple(_parse_json_list(row.get("topics")))
        source_type = _primary_source_type(row)
        section_label, section_reason = _section_for_row(row, text, source_type)
        content_format = _content_format(row)
        score, score_reasons = _base_score(row, text_source=text_source)
        rationale = (
            section_reason,
            *score_reasons,
            f"text source: {text_source}",
        )
        candidates.append(
            NewsletterCandidate(
                content_id=content_id,
                section_label=section_label,
                content_type=row.get("content_type"),
                content_format=content_format,
                source_type=source_type,
                topics=topics,
                publishable_text=text,
                text_source=text_source,
                created_at=row.get("created_at"),
                published_at=row.get("published_at"),
                publication_statuses=tuple(_parse_json_list(row.get("publication_statuses"))),
                engagement_score=round(float(row.get("engagement_score") or 0.0), 2),
                base_score=score,
                rationale=tuple(rationale),
            )
        )
    candidates.sort(key=lambda item: (-item.base_score, item.content_id))
    return candidates, excluded


def _balance_order(candidates: list[NewsletterCandidate], *, max_items: int) -> list[NewsletterCandidate]:
    remaining = list(candidates)
    selected: list[NewsletterCandidate] = []
    while remaining and len(selected) < max_items:
        previous = selected[-1] if selected else None
        best = max(
            remaining,
            key=lambda item: (
                _placement_score(item, previous),
                -item.content_id,
            ),
        )
        selected.append(best)
        remaining.remove(best)
    return selected


def _placement_score(
    item: NewsletterCandidate,
    previous: NewsletterCandidate | None,
) -> float:
    if previous is None:
        return item.base_score
    penalty = 0.0
    if item.content_format == previous.content_format:
        penalty += 12.0
    if item.source_type == previous.source_type:
        penalty += 10.0
    if item.section_label == previous.section_label:
        penalty += 6.0
    if set(item.topics) & set(previous.topics):
        penalty += 8.0
    return item.base_score - penalty


def _plan_item(candidate: NewsletterCandidate, order: int) -> NewsletterPlanItem:
    rationale = list(candidate.rationale)
    if order > 1:
        rationale.append("ordered to reduce adjacent repetition of source type, format, section, and topic")
    return NewsletterPlanItem(
        order=order,
        content_id=candidate.content_id,
        section_label=candidate.section_label,
        content_type=candidate.content_type,
        content_format=candidate.content_format,
        source_type=candidate.source_type,
        topics=candidate.topics,
        text_source=candidate.text_source,
        engagement_score=candidate.engagement_score,
        score=round(candidate.base_score, 2),
        rationale=tuple(rationale),
        content_preview=_preview(candidate.publishable_text),
    )


def _publishable_text(row: dict[str, Any]) -> tuple[str, str]:
    variant = _clean_text(row.get("variant_content"))
    if variant:
        platform = row.get("variant_platform") or "variant"
        variant_type = row.get("variant_type") or "copy"
        return variant, f"content_variants:{platform}:{variant_type}"
    original = _clean_text(row.get("content"))
    if original:
        return original, "generated_content.content"
    return "", ""


def _section_for_row(row: dict[str, Any], text: str, source_type: str) -> tuple[str, str]:
    content_type = str(row.get("content_type") or "")
    content_format = str(row.get("content_format") or "")
    if _URL_RE.search(text) or source_type == SOURCE_GITHUB_ACTIVITY:
        return SECTION_EXTERNAL_LINK, "classified as external link from URL or GitHub activity source"
    if _CTA_RE.search(text):
        return SECTION_CALL_TO_ACTION, "classified as call-to-action from action-oriented copy"
    if _LESSON_RE.search(text) or content_format in {"lesson", "reflection", "postmortem"}:
        return SECTION_LESSON_LEARNED, "classified as lesson learned from reflective language or format"
    if _SHIPPED_RE.search(text) or source_type == SOURCE_COMMIT:
        return SECTION_SHIPPED_WORK, "classified as shipped work from implementation language or commit source"
    if content_type in {"x_thread", "blog_post", "blog_seed"} or _TECH_RE.search(text):
        return SECTION_TECHNICAL_NOTE, "classified as technical note from content type or technical terms"
    return SECTION_LESSON_LEARNED, "classified as lesson learned fallback for general narrative copy"


def _base_score(row: dict[str, Any], *, text_source: str) -> tuple[float, tuple[str, ...]]:
    score = 0.0
    reasons = []
    eval_score = float(row.get("eval_score") or 0.0)
    if eval_score:
        score += eval_score * 5.0
        reasons.append(f"evaluation score {eval_score:g}")
    engagement_score = float(row.get("engagement_score") or 0.0)
    if engagement_score:
        score += min(engagement_score, 50.0)
        reasons.append(f"engagement score {engagement_score:g}")
    if row.get("published_at") or row.get("published"):
        score += 8.0
        reasons.append("has publication metadata")
    if text_source.startswith("content_variants:newsletter"):
        score += 7.0
        reasons.append("newsletter-specific variant available")
    elif text_source.startswith("content_variants:"):
        score += 3.0
        reasons.append("stored content variant available")
    if not reasons:
        reasons.append("default deterministic score")
    return round(score, 2), tuple(reasons)


def _primary_source_type(row: dict[str, Any]) -> str:
    if _parse_json_list(row.get("source_activity_ids")):
        return SOURCE_GITHUB_ACTIVITY
    if _parse_json_list(row.get("source_commits")):
        return SOURCE_COMMIT
    if _parse_json_list(row.get("source_messages")):
        return SOURCE_MESSAGE
    return SOURCE_ORIGINAL


def _content_format(row: dict[str, Any]) -> str:
    return (
        _clean_text(row.get("content_format"))
        or _clean_text(row.get("variant_type"))
        or _clean_text(row.get("content_type"))
        or "unknown"
    )


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
    return sorted({str(item).strip() for item in parsed if str(item).strip()})


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
