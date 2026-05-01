"""Build reusable newsletter sections from published X threads."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 5
DEFAULT_MIN_SCORE = 0.0
FALLBACK_ENGAGEMENT_SCORE = 0.0
FALLBACK_SCORE_REASON = "No post_engagement row exists; fallback score is 0.0."


class ThreadNewsletterSectionError(ValueError):
    """Raised when section builder inputs are invalid."""


@dataclass(frozen=True)
class ThreadNewsletterSection:
    """One newsletter-ready section derived from a published X thread."""

    source_content_id: int
    headline: str
    summary: str
    bullets: list[str]
    url: str | None
    engagement_score: float
    score_source: str
    published_at: str | None
    topics: list[str]


@dataclass(frozen=True)
class ThreadNewsletterSectionExport:
    """Stable export container for thread newsletter sections."""

    artifact_type: str
    days: int
    min_score: float
    limit: int
    topics: list[str]
    fallback_score: float
    fallback_score_reason: str
    sections: list[ThreadNewsletterSection]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class ThreadNewsletterSectionBuilder:
    """Select high-performing published X threads and shape them for newsletters."""

    def __init__(self, db: Any) -> None:
        self.db = db

    def build_export(
        self,
        *,
        days: int = DEFAULT_DAYS,
        min_score: float = DEFAULT_MIN_SCORE,
        topics: list[str] | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> ThreadNewsletterSectionExport:
        """Return a deterministic export of newsletter sections.

        Threads without engagement snapshots use ``FALLBACK_ENGAGEMENT_SCORE`` so
        newly published threads can still be selected when the caller's minimum
        score allows it.
        """

        rows = self.select_threads(
            days=days,
            min_score=min_score,
            topics=topics,
            limit=limit,
        )
        return ThreadNewsletterSectionExport(
            artifact_type="thread_newsletter_sections",
            days=days,
            min_score=float(min_score),
            limit=limit,
            topics=_normalize_topics(topics),
            fallback_score=FALLBACK_ENGAGEMENT_SCORE,
            fallback_score_reason=FALLBACK_SCORE_REASON,
            sections=[_section_from_row(row) for row in rows],
        )

    def select_threads(
        self,
        *,
        days: int = DEFAULT_DAYS,
        min_score: float = DEFAULT_MIN_SCORE,
        topics: list[str] | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> list[dict[str, Any]]:
        """Select recent published X threads ordered by score and recency."""

        if days <= 0:
            raise ThreadNewsletterSectionError("days must be positive")
        if limit <= 0:
            return []

        topic_filters = _normalize_topics(topics)
        if topics is not None and not topic_filters:
            return []

        clauses = [
            "gc.content_type = 'x_thread'",
            "gc.published = 1",
            "COALESCE(gc.published_at, gc.created_at) >= datetime('now', ?)",
            "(pe.fetched_at IS NULL OR pe.fetched_at >= datetime('now', ?))",
            f"COALESCE(pe.engagement_score, {FALLBACK_ENGAGEMENT_SCORE}) >= ?",
        ]
        params: list[Any] = [f"-{days} days", f"-{days} days", min_score]

        topic_join = ""
        if topic_filters:
            placeholders = ", ".join("?" for _ in topic_filters)
            topic_join = (
                "INNER JOIN content_topics filter_topics "
                "ON filter_topics.content_id = gc.id "
            )
            clauses.append(f"filter_topics.topic IN ({placeholders})")
            params.extend(topic_filters)

        where_sql = " AND ".join(clauses)
        rows = self.db.conn.execute(
            f"""WITH latest_engagement AS (
                   SELECT content_id, engagement_score, fetched_at
                   FROM (
                       SELECT content_id, engagement_score, fetched_at,
                              ROW_NUMBER() OVER (
                                  PARTITION BY content_id
                                  ORDER BY fetched_at DESC, id DESC
                              ) AS rn
                       FROM post_engagement
                   )
                   WHERE rn = 1
               ),
               topic_summary AS (
                   SELECT content_id,
                          json_group_array(topic) AS topics
                   FROM (
                       SELECT DISTINCT content_id, topic
                       FROM content_topics
                       ORDER BY content_id ASC, topic ASC
                   )
                   GROUP BY content_id
               )
               SELECT DISTINCT gc.id, gc.content, gc.published_url, gc.tweet_id,
                      gc.published_at, gc.created_at, pe.engagement_score,
                      pe.fetched_at AS engagement_fetched_at,
                      topic_summary.topics
               FROM generated_content gc
               LEFT JOIN latest_engagement pe ON pe.content_id = gc.id
               LEFT JOIN topic_summary ON topic_summary.content_id = gc.id
               {topic_join}
               WHERE {where_sql}
               ORDER BY COALESCE(pe.engagement_score, {FALLBACK_ENGAGEMENT_SCORE}) DESC,
                        COALESCE(gc.published_at, gc.created_at) DESC,
                        gc.id DESC
               LIMIT ?""",
            (*params, limit),
        ).fetchall()
        return [dict(row) for row in rows]


def export_to_json(export: ThreadNewsletterSectionExport) -> str:
    """Serialize the export as stable JSON."""

    return json.dumps(export.as_dict(), indent=2, sort_keys=True)


def format_markdown(export: ThreadNewsletterSectionExport) -> str:
    """Render sections as Markdown suitable for a newsletter draft."""

    lines = [
        "# Thread Newsletter Sections",
        "",
        f"- Lookback days: {export.days}",
        f"- Minimum score: {export.min_score:g}",
        f"- Fallback score: {export.fallback_score:g}",
    ]
    if export.topics:
        lines.append(f"- Topics: {', '.join(export.topics)}")
    lines.append("")

    if not export.sections:
        lines.append("No thread newsletter sections selected.")
        return "\n".join(lines).rstrip() + "\n"

    for index, section in enumerate(export.sections, start=1):
        lines.extend(
            [
                f"## {index}. {section.headline}",
                "",
                section.summary,
                "",
            ]
        )
        if section.bullets:
            lines.extend(f"- {bullet}" for bullet in section.bullets)
            lines.append("")
        if section.url:
            lines.extend(["Read the original thread:", section.url, ""])
        lines.extend(
            [
                "<!-- "
                f"source_content_id={section.source_content_id}; "
                f"engagement_score={section.engagement_score:g}; "
                f"score_source={section.score_source}; "
                f"published_at={section.published_at or ''}"
                " -->",
                "",
            ]
        )

    return "\n".join(lines).rstrip() + "\n"


def _section_from_row(row: dict[str, Any]) -> ThreadNewsletterSection:
    pieces = _thread_pieces(row.get("content"))
    headline = _headline(pieces)
    summary = _summary(pieces)
    bullets = _bullets(pieces, headline=headline, summary=summary)
    score = row.get("engagement_score")
    score_source = "post_engagement" if score is not None else "fallback"
    return ThreadNewsletterSection(
        source_content_id=int(row["id"]),
        headline=headline,
        summary=summary,
        bullets=bullets,
        url=_canonical_url(row),
        engagement_score=round(float(score if score is not None else FALLBACK_ENGAGEMENT_SCORE), 2),
        score_source=score_source,
        published_at=row.get("published_at") or row.get("created_at"),
        topics=_parse_topics(row.get("topics")),
    )


def _thread_pieces(content: Any) -> list[str]:
    text = str(content or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    raw_parts = re.split(
        r"\n{2,}|^\s*(?:tweet|post)\s*\d+\s*[:.)-]\s*",
        text,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    pieces = [_clean_piece(part) for part in raw_parts]
    pieces = [piece for piece in pieces if piece]
    if len(pieces) <= 1:
        lines = [_clean_piece(line) for line in text.split("\n")]
        pieces = [line for line in lines if line]
    return pieces or ["Untitled thread"]


def _clean_piece(value: str) -> str:
    text = re.sub(r"(?i)^\s*(?:tweet|post)\s*\d+\s*[:.)-]\s*", "", value or "")
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"\s+", " ", text).strip(" -\t")
    return text


def _headline(pieces: list[str]) -> str:
    first = pieces[0]
    sentence = re.split(r"(?<=[.!?])\s+", first, maxsplit=1)[0].strip()
    return _truncate(sentence or first, 96)


def _summary(pieces: list[str]) -> str:
    if len(pieces) >= 2:
        return _truncate(pieces[1], 240)
    return _truncate(pieces[0], 240)


def _bullets(pieces: list[str], *, headline: str, summary: str) -> list[str]:
    candidates = []
    for piece in pieces:
        cleaned = _truncate(piece, 180)
        if cleaned and cleaned not in {headline, summary}:
            candidates.append(cleaned)
    if not candidates and summary != headline:
        candidates.append(summary)
    return candidates[:3]


def _canonical_url(row: dict[str, Any]) -> str | None:
    published_url = _clean_url(row.get("published_url"))
    if published_url:
        return published_url
    tweet_id = str(row.get("tweet_id") or "").strip()
    if tweet_id:
        return f"https://x.com/i/web/status/{tweet_id}"
    return None


def _clean_url(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _parse_topics(value: Any) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    return sorted({str(item).strip() for item in parsed if str(item).strip()})


def _normalize_topics(topics: list[str] | None) -> list[str]:
    if topics is None:
        return []
    return sorted({str(topic).strip() for topic in topics if str(topic).strip()})


def _truncate(value: str, limit: int) -> str:
    text = value.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."
