"""Report topical concentration in newsletter candidate content."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
from typing import Any, Iterable


DEFAULT_DAYS = 14
DEFAULT_MAX_SHARE = 0.4

_TOKEN_RE = re.compile(r"[^a-z0-9+#.-]+")
_KEYWORD_BUCKETS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("architecture", ("architecture", "architectural", "design", "boundary", "module", "service")),
    ("testing", ("test", "tests", "testing", "pytest", "fixture", "coverage", "assert")),
    ("debugging", ("debug", "debugging", "bug", "fix", "fixed", "trace", "diagnose", "regression")),
    ("ai-agents", ("agent", "agents", "claude", "llm", "prompt", "model", "tool call")),
    ("developer-tools", ("cli", "tool", "tools", "script", "workflow command", "dev tool")),
    ("performance", ("performance", "latency", "speed", "cache", "optimize", "slow", "throughput")),
    ("data-modeling", ("schema", "database", "sqlite", "migration", "table")),
    ("devops", ("deploy", "ci", "pipeline", "cron", "infra", "docker", "release")),
    ("open-source", ("open source", "oss", "contributor", "license", "repository")),
    ("product-thinking", ("product", "user", "ux", "customer", "roadmap", "feature")),
    ("workflow", ("workflow", "process", "automation", "handoff", "review", "routine")),
)


@dataclass(frozen=True)
class NewsletterTopicBalanceItem:
    """One candidate item classified into a topic bucket."""

    content_id: int
    topic: str
    topic_source: str
    content_type: str | None
    content_format: str | None
    created_at: str | None
    eval_score: float | None
    content_preview: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterTopicBalanceRow:
    """Aggregated topic share for candidate newsletter items."""

    topic: str
    count: int
    share: float
    item_ids: tuple[int, ...]
    topic_sources: tuple[str, ...]
    overrepresented: bool
    recommended_trim_item_ids: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "item_ids": list(self.item_ids),
            "overrepresented": self.overrepresented,
            "recommended_trim_item_ids": list(self.recommended_trim_item_ids),
            "share": round(self.share, 6),
            "topic": self.topic,
            "topic_sources": list(self.topic_sources),
        }


@dataclass(frozen=True)
class NewsletterTopicBalanceReport:
    """Read-only topic balance report for newsletter assembly candidates."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    total_items: int
    max_topic_share: float
    topics: tuple[NewsletterTopicBalanceRow, ...]
    overrepresented_topics: tuple[NewsletterTopicBalanceRow, ...]
    items: tuple[NewsletterTopicBalanceItem, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "filters": self.filters,
            "generated_at": self.generated_at,
            "items": [item.to_dict() for item in self.items],
            "max_topic_share": self.max_topic_share,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "overrepresented_topics": [
                row.to_dict() for row in self.overrepresented_topics
            ],
            "topics": [row.to_dict() for row in self.topics],
            "total_items": self.total_items,
        }


def build_newsletter_topic_balance_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    max_topic_share: float = DEFAULT_MAX_SHARE,
    item_ids: Iterable[int] | None = None,
    now: datetime | None = None,
) -> NewsletterTopicBalanceReport:
    """Build a deterministic topic concentration report for newsletter candidates."""

    if days <= 0:
        raise ValueError("days must be positive")
    max_topic_share = float(max_topic_share)
    if max_topic_share <= 0 or max_topic_share > 1:
        raise ValueError("max_topic_share must be greater than 0 and at most 1")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    normalized_item_ids = tuple(sorted({int(item_id) for item_id in item_ids or ()}))
    if any(item_id <= 0 for item_id in normalized_item_ids):
        raise ValueError("item_ids must be positive")

    conn = getattr(db_or_conn, "conn", db_or_conn)
    missing_tables, missing_columns = _schema_gaps(conn)
    filters = {
        "days": days,
        "item_ids": list(normalized_item_ids),
        "max_topic_share": max_topic_share,
    }
    if missing_tables or missing_columns:
        return NewsletterTopicBalanceReport(
            artifact_type="newsletter_topic_balance",
            generated_at=generated_at.isoformat(),
            filters=filters,
            total_items=0,
            max_topic_share=max_topic_share,
            topics=(),
            overrepresented_topics=(),
            items=(),
            missing_tables=tuple(missing_tables),
            missing_columns=missing_columns,
        )

    start = generated_at - timedelta(days=days)
    rows = _load_candidate_rows(
        conn,
        start=start,
        end=generated_at,
        item_ids=normalized_item_ids,
    )
    topics_by_content = _load_topic_metadata(conn, [int(row["id"]) for row in rows])
    items = tuple(
        _classify_item(row, topics_by_content.get(int(row["id"]), ())) for row in rows
    )
    topic_rows = _topic_rows(items, max_topic_share=max_topic_share)
    return NewsletterTopicBalanceReport(
        artifact_type="newsletter_topic_balance",
        generated_at=generated_at.isoformat(),
        filters=filters,
        total_items=len(items),
        max_topic_share=max_topic_share,
        topics=topic_rows,
        overrepresented_topics=tuple(row for row in topic_rows if row.overrepresented),
        items=items,
        missing_tables=(),
        missing_columns={},
    )


def format_newsletter_topic_balance_json(report: NewsletterTopicBalanceReport) -> str:
    """Serialize a topic balance report as deterministic JSON."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_topic_balance_markdown(report: NewsletterTopicBalanceReport) -> str:
    """Format a topic balance report as stable Markdown."""

    lines = [
        "# Newsletter Topic Balance",
        "",
        f"- Generated: {report.generated_at}",
        f"- Items: {report.total_items}",
        f"- Max topic share: {_pct(report.max_topic_share)}",
    ]
    if report.filters.get("item_ids"):
        lines.append(
            "- Candidate IDs: "
            + ", ".join(str(item_id) for item_id in report.filters["item_ids"])
        )
    else:
        lines.append(f"- Lookback: {report.filters['days']} days")

    if report.missing_tables or report.missing_columns:
        lines.append("")
        lines.append("## Schema Gaps")
        if report.missing_tables:
            lines.append("- Missing tables: " + ", ".join(report.missing_tables))
        for table, columns in sorted((report.missing_columns or {}).items()):
            lines.append(f"- Missing columns in {table}: {', '.join(columns)}")
        return "\n".join(lines)

    lines.extend(["", "## Topics", ""])
    if not report.topics:
        lines.append("No newsletter candidate items matched the filters.")
        return "\n".join(lines)

    lines.extend(
        [
            "| Topic | Count | Share | Items | Trim candidates |",
            "| --- | ---: | ---: | --- | --- |",
        ]
    )
    for row in report.topics:
        trim = ", ".join(str(item_id) for item_id in row.recommended_trim_item_ids) or "-"
        items = ", ".join(str(item_id) for item_id in row.item_ids)
        lines.append(f"| {row.topic} | {row.count} | {_pct(row.share)} | {items} | {trim} |")

    lines.extend(["", "## Overrepresented Topics", ""])
    if not report.overrepresented_topics:
        lines.append("None.")
    else:
        for row in report.overrepresented_topics:
            trim = ", ".join(str(item_id) for item_id in row.recommended_trim_item_ids)
            lines.append(
                f"- {row.topic}: {_pct(row.share)} ({row.count}/{report.total_items}); "
                f"trim candidates: {trim}"
            )
    return "\n".join(lines)


def _load_candidate_rows(
    conn: Any,
    *,
    start: datetime,
    end: datetime,
    item_ids: tuple[int, ...],
) -> list[dict[str, Any]]:
    if item_ids:
        placeholders = ",".join("?" for _ in item_ids)
        rows = conn.execute(
            f"""SELECT id, content_type, content_format, content, eval_score, created_at, published_at
                FROM generated_content
                WHERE id IN ({placeholders})
                  AND COALESCE(published, 0) != -1
                ORDER BY id ASC""",
            item_ids,
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT id, content_type, content_format, content, eval_score, created_at, published_at
               FROM generated_content
               WHERE COALESCE(published_at, created_at) >= ?
                 AND COALESCE(published_at, created_at) <= ?
                 AND COALESCE(published, 0) != -1
               ORDER BY COALESCE(published_at, created_at) DESC, id DESC""",
            (start.isoformat(), end.isoformat()),
        ).fetchall()
    return [dict(row) for row in rows]


def _load_topic_metadata(
    conn: Any,
    content_ids: list[int],
) -> dict[int, tuple[dict[str, Any], ...]]:
    if not content_ids:
        return {}
    unique_ids = sorted(set(content_ids))
    placeholders = ",".join("?" for _ in unique_ids)
    rows = conn.execute(
        f"""SELECT content_id, topic, subtopic, confidence
            FROM content_topics
            WHERE content_id IN ({placeholders})
            ORDER BY content_id ASC, confidence DESC, lower(topic) ASC, id ASC""",
        unique_ids,
    ).fetchall()
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        topic = _clean_label(row["topic"])
        if topic:
            grouped[int(row["content_id"])].append({**dict(row), "topic": topic})
    return {content_id: tuple(items) for content_id, items in grouped.items()}


def _classify_item(
    row: dict[str, Any],
    topic_rows: tuple[dict[str, Any], ...],
) -> NewsletterTopicBalanceItem:
    topic, source = _topic_for_row(row, topic_rows)
    return NewsletterTopicBalanceItem(
        content_id=int(row["id"]),
        topic=topic,
        topic_source=source,
        content_type=row.get("content_type"),
        content_format=row.get("content_format"),
        created_at=row.get("published_at") or row.get("created_at"),
        eval_score=_float_or_none(row.get("eval_score")),
        content_preview=_preview(row.get("content")),
    )


def _topic_for_row(
    row: dict[str, Any],
    topic_rows: tuple[dict[str, Any], ...],
) -> tuple[str, str]:
    if topic_rows:
        return str(topic_rows[0]["topic"]), "content_topics"

    text = " ".join(
        str(part or "")
        for part in (row.get("content_type"), row.get("content_format"), row.get("content"))
    )
    bucket = _fallback_bucket(text)
    return bucket, "keyword_fallback"


def _fallback_bucket(text: str) -> str:
    normalized = f" {_TOKEN_RE.sub(' ', (text or '').lower())} "
    scores: list[tuple[str, int]] = []
    for bucket, keywords in _KEYWORD_BUCKETS:
        score = 0
        for keyword in keywords:
            if " " in keyword or len(keyword) <= 3:
                matched = f" {keyword} " in normalized
            else:
                matched = f" {keyword} " in normalized or keyword in normalized
            if matched:
                score += 1
        if score:
            scores.append((bucket, score))
    if not scores:
        return "general"
    scores.sort(key=lambda item: (-item[1], item[0]))
    return scores[0][0]


def _topic_rows(
    items: tuple[NewsletterTopicBalanceItem, ...],
    *,
    max_topic_share: float,
) -> tuple[NewsletterTopicBalanceRow, ...]:
    total = len(items)
    if total == 0:
        return ()
    by_topic: dict[str, list[NewsletterTopicBalanceItem]] = defaultdict(list)
    for item in items:
        by_topic[item.topic].append(item)

    counts = Counter({topic: len(topic_items) for topic, topic_items in by_topic.items()})
    rows = []
    max_allowed = max(int(total * max_topic_share), 0)
    for topic, count in counts.items():
        topic_items = by_topic[topic]
        share = count / total
        overrepresented = share > max_topic_share
        trim_count = max(0, count - max_allowed) if overrepresented else 0
        trim_ids = tuple(item.content_id for item in _trim_candidates(topic_items)[:trim_count])
        rows.append(
            NewsletterTopicBalanceRow(
                topic=topic,
                count=count,
                share=share,
                item_ids=tuple(sorted(item.content_id for item in topic_items)),
                topic_sources=tuple(sorted({item.topic_source for item in topic_items})),
                overrepresented=overrepresented,
                recommended_trim_item_ids=trim_ids,
            )
        )
    rows.sort(key=lambda row: (-row.share, -row.count, row.topic))
    return tuple(rows)


def _trim_candidates(
    items: list[NewsletterTopicBalanceItem],
) -> list[NewsletterTopicBalanceItem]:
    return sorted(
        items,
        key=lambda item: (
            item.eval_score if item.eval_score is not None else -1.0,
            item.created_at or "",
            item.content_id,
        ),
    )


def _schema_gaps(conn: Any) -> tuple[list[str], dict[str, tuple[str, ...]]]:
    schema = _schema(conn)
    required = {
        "generated_content": {
            "id",
            "content",
            "content_type",
            "content_format",
            "created_at",
            "eval_score",
            "published",
            "published_at",
        },
        "content_topics": {"content_id", "topic", "subtopic", "confidence"},
    }
    missing_tables = [table for table in required if table not in schema]
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _schema(conn: Any) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = [row["name"] if hasattr(row, "keys") else row[0] for row in rows]
    return {
        name: {
            column["name"] if hasattr(column, "keys") else column[1]
            for column in conn.execute(f"PRAGMA table_info({name})").fetchall()
        }
        for name in names
    }


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clean_label(value: Any) -> str:
    return " ".join(str(value or "").strip().split()).lower()


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _preview(value: Any, limit: int = 120) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _pct(value: float) -> str:
    return f"{value * 100:.1f}%"
