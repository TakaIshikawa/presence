"""Find internal blog links that could be added to existing posts."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import urlparse


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 25
DEFAULT_MIN_SCORE = 2


@dataclass(frozen=True)
class BlogInternalLinkOpportunity:
    source_post_id: int
    target_post_id: int
    source_title: str
    target_title: str
    target_url: str | None
    reason_labels: tuple[str, ...]
    score: int
    suggested_anchor_text: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reason_labels"] = list(self.reason_labels)
        return payload


@dataclass(frozen=True)
class BlogInternalLinkOpportunitiesReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    opportunities: tuple[BlogInternalLinkOpportunity, ...]
    schema_warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "blog_internal_link_opportunities",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "opportunities": [item.to_dict() for item in self.opportunities],
            "schema_warnings": list(self.schema_warnings),
            "totals": dict(sorted(self.totals.items())),
        }


def build_blog_internal_link_opportunities_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    min_score: int = DEFAULT_MIN_SCORE,
    now: datetime | None = None,
) -> BlogInternalLinkOpportunitiesReport:
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if min_score <= 0:
        raise ValueError("min_score must be positive")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"days": days, "limit": limit, "min_score": min_score, "cutoff": cutoff.isoformat()}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    warnings = _schema_warnings(schema)
    if warnings:
        return _report(generated_at, filters, (), warnings, post_count=0)
    posts = _load_posts(conn, schema, cutoff)
    campaigns = _campaigns(conn, schema, [post["id"] for post in posts])
    opportunities = _opportunities(posts, campaigns, min_score)[:limit]
    return _report(generated_at, filters, tuple(opportunities), (), post_count=len(posts))


def format_blog_internal_link_opportunities_json(report: BlogInternalLinkOpportunitiesReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_blog_internal_link_opportunities_text(report: BlogInternalLinkOpportunitiesReport) -> str:
    lines = [
        "Blog Internal Link Opportunities",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['days']} days",
        (
            "Totals: "
            f"posts={report.totals['post_count']} "
            f"opportunities={report.totals['opportunity_count']}"
        ),
    ]
    if report.schema_warnings:
        lines.append("Schema warnings: " + "; ".join(report.schema_warnings))
    if not report.opportunities:
        lines.append("No blog internal link opportunities found.")
        return "\n".join(lines)
    lines.append("")
    lines.append("Opportunities:")
    for item in report.opportunities:
        lines.append(
            f"- source={item.source_post_id} target={item.target_post_id} "
            f"score={item.score} reasons={','.join(item.reason_labels)} "
            f"anchor={item.suggested_anchor_text}"
        )
    return "\n".join(lines)


def _load_posts(conn: sqlite3.Connection, schema: dict[str, set[str]], cutoff: datetime) -> list[dict[str, Any]]:
    gc = schema["generated_content"]
    created_at = _column_expr(gc, "created_at", "NULL", "gc")
    published_at = _column_expr(gc, "published_at", "NULL", "gc")
    published_url = _column_expr(gc, "published_url", "NULL", "gc")
    rows = conn.execute(
        f"""SELECT gc.id, gc.content_type, gc.content, {created_at} AS created_at,
                  {published_at} AS published_at, {published_url} AS published_url
           FROM generated_content gc
           WHERE gc.content_type IN ('blog', 'blog_post')
             AND ({created_at} IS NULL OR datetime({created_at}) >= datetime(?)
                  OR {published_at} IS NULL OR datetime({published_at}) >= datetime(?))
           ORDER BY {created_at} DESC, gc.id DESC""",
        (cutoff.isoformat(), cutoff.isoformat()),
    ).fetchall()
    posts = [dict(row) for row in rows]
    if "content_publications" in schema and {"content_id", "platform_url"}.issubset(schema["content_publications"]):
        url_rows = conn.execute(
            "SELECT content_id, platform_url FROM content_publications WHERE platform IN ('blog', 'website')"
        ).fetchall()
        urls = {int(row["content_id"]): row["platform_url"] for row in url_rows if row["platform_url"]}
        for post in posts:
            post["published_url"] = post.get("published_url") or urls.get(int(post["id"]))
    return posts


def _campaigns(conn: sqlite3.Connection, schema: dict[str, set[str]], ids: list[int]) -> dict[int, set[str]]:
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    result: dict[int, set[str]] = defaultdict(set)
    if "content_campaigns" in schema and {"content_id", "campaign"}.issubset(schema["content_campaigns"]):
        for row in conn.execute(f"SELECT content_id, campaign FROM content_campaigns WHERE content_id IN ({placeholders})", ids):
            if row["campaign"]:
                result[int(row["content_id"])].add(str(row["campaign"]).lower())
    if {"planned_topics", "content_campaigns"}.issubset(schema) and {"content_id", "campaign_id"}.issubset(schema["planned_topics"]) and {"id", "name"}.issubset(schema["content_campaigns"]):
        for row in conn.execute(
            f"""SELECT pt.content_id, cc.name AS campaign
                FROM planned_topics pt
                JOIN content_campaigns cc ON cc.id = pt.campaign_id
                WHERE pt.content_id IN ({placeholders})""",
            ids,
        ):
            if row["campaign"]:
                result[int(row["content_id"])].add(str(row["campaign"]).lower())
    return result


def _opportunities(posts: list[dict[str, Any]], campaigns: dict[int, set[str]], min_score: int) -> list[BlogInternalLinkOpportunity]:
    items: list[BlogInternalLinkOpportunity] = []
    for source in posts:
        source_text = str(source.get("content") or "")
        for target in posts:
            if source["id"] == target["id"]:
                continue
            target_url = target.get("published_url")
            if target_url and target_url in source_text:
                continue
            labels: list[str] = []
            score = 0
            shared_campaigns = campaigns.get(int(source["id"]), set()) & campaigns.get(int(target["id"]), set())
            if shared_campaigns:
                labels.append("shared_campaign")
                score += 3
            source_domains = _domains(source_text)
            target_domains = _domains(str(target.get("content") or ""))
            if source_domains & target_domains:
                labels.append("shared_source_domain")
                score += 2
            overlap = _tokens(_title(source)) & _tokens(_title(target))
            if overlap:
                labels.append("title_token_overlap")
                score += min(3, len(overlap))
            topic_overlap = _tokens(source_text) & _tokens(_title(target))
            if len(topic_overlap) >= 2:
                labels.append("topic_overlap")
                score += 1
            if score >= min_score:
                items.append(
                    BlogInternalLinkOpportunity(
                        source_post_id=int(source["id"]),
                        target_post_id=int(target["id"]),
                        source_title=_title(source),
                        target_title=_title(target),
                        target_url=target_url,
                        reason_labels=tuple(labels),
                        score=score,
                        suggested_anchor_text=_anchor(_title(target)),
                    )
                )
    return sorted(items, key=lambda item: (-item.score, item.source_post_id, item.target_post_id))


def _title(post: dict[str, Any]) -> str:
    first = str(post.get("content") or "").strip().splitlines()[0:1]
    title = first[0].lstrip("# ").strip() if first else ""
    return title[:80] or f"Post {post['id']}"


def _anchor(title: str) -> str:
    return re.sub(r"\s+", " ", title).strip()[:60]


def _domains(text: str) -> set[str]:
    return {urlparse(match).netloc.lower() for match in re.findall(r"https?://[^\s)>\"]+", text) if urlparse(match).netloc}


def _tokens(text: str) -> set[str]:
    stop = {"the", "and", "for", "with", "from", "this", "that", "into", "your", "blog", "post"}
    return {token for token in re.findall(r"[a-z0-9]{4,}", text.lower()) if token not in stop}


def _report(generated_at: datetime, filters: dict[str, Any], opportunities: tuple[BlogInternalLinkOpportunity, ...], warnings: tuple[str, ...], *, post_count: int) -> BlogInternalLinkOpportunitiesReport:
    return BlogInternalLinkOpportunitiesReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={"post_count": post_count, "opportunity_count": len(opportunities)},
        opportunities=opportunities,
        schema_warnings=warnings,
    )


def _schema_warnings(schema: dict[str, set[str]]) -> tuple[str, ...]:
    if "generated_content" not in schema:
        return ("missing table: generated_content",)
    missing = {"id", "content_type", "content"} - schema["generated_content"]
    return (f"missing columns: generated_content({', '.join(sorted(missing))})",) if missing else ()


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _column_expr(columns: set[str], column: str, fallback: str, alias: str) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
