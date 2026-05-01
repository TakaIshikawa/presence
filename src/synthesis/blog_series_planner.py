"""Plan candidate multi-post blog series from related content evidence."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_MIN_ITEMS = 3


@dataclass(frozen=True)
class BlogSeriesCandidate:
    """One read-only recommendation for a blog series."""

    series_key: str
    title_suggestion: str
    included_content_ids: tuple[int, ...]
    newsletter_send_ids: tuple[int, ...]
    knowledge_ids: tuple[int, ...]
    topics: tuple[str, ...]
    content_types: tuple[str, ...]
    evidence_count: int
    freshness: str
    latest_activity_at: str | None
    missing_evidence_warnings: tuple[str, ...]
    recommended_next_artifact: str

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["included_content_ids"] = list(self.included_content_ids)
        data["newsletter_send_ids"] = list(self.newsletter_send_ids)
        data["knowledge_ids"] = list(self.knowledge_ids)
        data["topics"] = list(self.topics)
        data["content_types"] = list(self.content_types)
        data["missing_evidence_warnings"] = list(self.missing_evidence_warnings)
        return data


@dataclass(frozen=True)
class BlogSeriesPlan:
    """Read-only report of candidate blog series."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    candidates: tuple[BlogSeriesCandidate, ...]
    missing_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "totals": dict(self.totals),
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "missing_tables": list(self.missing_tables),
        }


def build_blog_series_plan(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_items: int = DEFAULT_MIN_ITEMS,
    now: datetime | None = None,
) -> BlogSeriesPlan:
    """Return deterministic blog series candidates from recent related content."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_items <= 1:
        raise ValueError("min-items must be greater than 1")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    missing = tuple(
        table
        for table in (
            "generated_content",
            "content_topics",
            "content_knowledge_links",
            "newsletter_sends",
        )
        if table not in schema
    )
    if "generated_content" in missing:
        return _empty_plan(generated_at, days, min_items, cutoff, missing)

    all_content = _load_content(conn, schema)
    topics_by_content = _load_topics(conn, schema)
    knowledge_by_content = _load_knowledge_links(conn, schema)
    newsletter_sends = _load_newsletter_sends(conn, schema, cutoff)

    in_scope_ids = {
        content_id
        for content_id, row in all_content.items()
        if _effective_activity_at(row) and _effective_activity_at(row) >= cutoff
    }
    for send in newsletter_sends:
        in_scope_ids.update(content_id for content_id in send["source_content_ids"] if content_id in all_content)

    if not in_scope_ids:
        return _empty_plan(generated_at, days, min_items, cutoff, missing)

    parent = {content_id: content_id for content_id in in_scope_ids}
    cluster_keys: dict[str, set[int]] = defaultdict(set)
    for content_id in sorted(in_scope_ids):
        row = all_content[content_id]
        topics = topics_by_content.get(content_id, ())
        knowledge_ids = knowledge_by_content.get(content_id, ())
        if not topics and not knowledge_ids:
            cluster_keys[f"solo:{content_id}"].add(content_id)
        for topic in topics:
            cluster_keys[f"topic:{topic}"].add(content_id)
            cluster_keys[f"type-topic:{_content_family(row.get('content_type'))}:{topic}"].add(content_id)
        for knowledge_id in knowledge_ids:
            cluster_keys[f"knowledge:{knowledge_id}"].add(content_id)
    for send in newsletter_sends:
        found = [content_id for content_id in send["source_content_ids"] if content_id in in_scope_ids]
        for content_id in found:
            cluster_keys[f"newsletter:{send['id']}"].add(content_id)

    for ids in cluster_keys.values():
        ordered = sorted(ids)
        for content_id in ordered[1:]:
            _union(parent, ordered[0], content_id)

    component_ids: dict[int, set[int]] = defaultdict(set)
    for content_id in sorted(in_scope_ids):
        component_ids[_find(parent, content_id)].add(content_id)

    candidates = []
    excluded_weak = 0
    for ids in component_ids.values():
        if len(ids) < min_items:
            excluded_weak += 1
            continue
        candidate = _build_candidate(
            sorted(ids),
            all_content=all_content,
            topics_by_content=topics_by_content,
            knowledge_by_content=knowledge_by_content,
            newsletter_sends=newsletter_sends,
            now=generated_at,
        )
        candidates.append(candidate)

    ranked = tuple(
        sorted(
            candidates,
            key=lambda item: (
                -item.evidence_count,
                _freshness_rank(item.freshness),
                item.title_suggestion,
                item.included_content_ids,
            ),
        )
    )
    return BlogSeriesPlan(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "min_items": min_items,
            "activity_since": cutoff.isoformat(),
        },
        totals={
            "content_items_considered": len(in_scope_ids),
            "newsletter_sends_considered": len(newsletter_sends),
            "candidate_count": len(ranked),
            "excluded_weak_candidates": excluded_weak,
            "missing_tables": len(missing),
        },
        candidates=ranked,
        missing_tables=missing,
    )


def format_blog_series_plan_json(plan: BlogSeriesPlan) -> str:
    """Serialize a blog series plan as deterministic JSON."""
    return json.dumps(plan.to_dict(), indent=2, sort_keys=True)


def format_blog_series_plan_text(plan: BlogSeriesPlan) -> str:
    """Format blog series candidates for terminal review."""
    lines = [
        "Blog Series Planner",
        f"Generated: {plan.generated_at}",
        (
            f"Activity since: {plan.filters['activity_since']} "
            f"(days={plan.filters['days']}, min_items={plan.filters['min_items']})"
        ),
        (
            "Summary: "
            f"candidates={plan.totals['candidate_count']} "
            f"content={plan.totals['content_items_considered']} "
            f"newsletters={plan.totals['newsletter_sends_considered']} "
            f"weak={plan.totals['excluded_weak_candidates']}"
        ),
    ]
    if plan.missing_tables:
        lines.append("Missing tables: " + ", ".join(plan.missing_tables))
    if not plan.candidates:
        lines.append("No blog series candidates found.")
        return "\n".join(lines)

    lines.append("Candidates:")
    for candidate in plan.candidates:
        topics = ", ".join(candidate.topics) if candidate.topics else "untagged"
        warnings = ", ".join(candidate.missing_evidence_warnings) or "none"
        lines.append(
            f"- {candidate.title_suggestion} "
            f"evidence={candidate.evidence_count} freshness={candidate.freshness}"
        )
        lines.append(
            "  content_ids: "
            + ", ".join(str(content_id) for content_id in candidate.included_content_ids)
        )
        lines.append(f"  topics: {topics}")
        lines.append(f"  next: {candidate.recommended_next_artifact}")
        lines.append(f"  warnings: {warnings}")
    return "\n".join(lines)


def _build_candidate(
    content_ids: list[int],
    *,
    all_content: dict[int, dict[str, Any]],
    topics_by_content: dict[int, tuple[str, ...]],
    knowledge_by_content: dict[int, tuple[int, ...]],
    newsletter_sends: list[dict[str, Any]],
    now: datetime,
) -> BlogSeriesCandidate:
    content_id_set = set(content_ids)
    topic_counts = Counter(
        topic
        for content_id in content_ids
        for topic in topics_by_content.get(content_id, ())
    )
    topics = tuple(topic for topic, _count in sorted(topic_counts.items(), key=lambda item: (-item[1], item[0])))
    knowledge_ids = tuple(
        sorted(
            {
                knowledge_id
                for content_id in content_ids
                for knowledge_id in knowledge_by_content.get(content_id, ())
            }
        )
    )
    content_types = tuple(sorted({str(all_content[content_id].get("content_type") or "") for content_id in content_ids}))
    related_sends = [
        send
        for send in newsletter_sends
        if any(content_id in content_id_set for content_id in send["source_content_ids"])
    ]
    newsletter_send_ids = tuple(int(send["id"]) for send in related_sends)
    latest_activity = _latest_activity(content_ids, related_sends, all_content)
    warnings = _warnings(
        content_ids,
        related_sends,
        topics=topics,
        knowledge_ids=knowledge_ids,
        content_types=content_types,
    )
    return BlogSeriesCandidate(
        series_key=_series_key(topics, knowledge_ids, content_ids),
        title_suggestion=_title_suggestion(topics, content_types),
        included_content_ids=tuple(content_ids),
        newsletter_send_ids=newsletter_send_ids,
        knowledge_ids=knowledge_ids,
        topics=topics,
        content_types=content_types,
        evidence_count=len(content_ids) + len(newsletter_send_ids) + len(knowledge_ids),
        freshness=_freshness(latest_activity, now),
        latest_activity_at=latest_activity.isoformat() if latest_activity else None,
        missing_evidence_warnings=warnings,
        recommended_next_artifact=_recommended_next_artifact(content_types, newsletter_send_ids, topics),
    )


def _warnings(
    content_ids: list[int],
    newsletter_sends: list[dict[str, Any]],
    *,
    topics: tuple[str, ...],
    knowledge_ids: tuple[int, ...],
    content_types: tuple[str, ...],
) -> tuple[str, ...]:
    warnings: set[str] = set()
    if not topics:
        warnings.add("no_topic_labels")
    if not knowledge_ids:
        warnings.add("no_knowledge_links")
    if len(content_types) <= 1:
        warnings.add("single_content_type")
    if not newsletter_sends:
        warnings.add("no_newsletter_sends")
    found_ids = set(content_ids)
    for send in newsletter_sends:
        warnings.update(send["parse_warnings"])
        if any(content_id not in found_ids for content_id in send["source_content_ids"]):
            warnings.add("newsletter_has_outside_sources")
        if send["missing_source_ids"]:
            warnings.add("newsletter_has_missing_sources")
    return tuple(sorted(warnings))


def _recommended_next_artifact(
    content_types: tuple[str, ...],
    newsletter_send_ids: tuple[int, ...],
    topics: tuple[str, ...],
) -> str:
    if "blog_post" not in content_types:
        return "blog_series_outline"
    if not newsletter_send_ids:
        return "newsletter_series_pitch"
    if len(content_types) == 1:
        return "cross_platform_follow_up"
    if len(topics) > 1:
        return "pillar_post_with_subposts"
    return "blog_series_follow_up"


def _load_content(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[int, dict[str, Any]]:
    columns = schema["generated_content"]
    selected = [
        _column_expr(columns, "id"),
        _column_expr(columns, "content_type"),
        _column_expr(columns, "content"),
        _column_expr(columns, "published_at"),
        _column_expr(columns, "created_at"),
    ]
    rows = conn.execute(f"SELECT {', '.join(selected)} FROM generated_content ORDER BY id ASC").fetchall()
    return {int(_value(row, "id", 0)): _row_dict(row) for row in rows}


def _load_topics(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[int, tuple[str, ...]]:
    if "content_topics" not in schema or not {"content_id", "topic"}.issubset(schema["content_topics"]):
        return {}
    rows = conn.execute(
        "SELECT content_id, topic FROM content_topics ORDER BY content_id ASC, topic ASC, id ASC"
    ).fetchall()
    result: dict[int, list[str]] = defaultdict(list)
    for row in rows:
        topic = _normalize_label(_value(row, "topic", 1))
        if topic:
            result[int(_value(row, "content_id", 0))].append(topic)
    return {content_id: tuple(dict.fromkeys(values)) for content_id, values in result.items()}


def _load_knowledge_links(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[int, tuple[int, ...]]:
    if "content_knowledge_links" not in schema:
        return {}
    columns = schema["content_knowledge_links"]
    if not {"content_id", "knowledge_id"}.issubset(columns):
        return {}
    rows = conn.execute(
        """SELECT content_id, knowledge_id
           FROM content_knowledge_links
           ORDER BY content_id ASC, knowledge_id ASC, id ASC"""
    ).fetchall()
    result: dict[int, list[int]] = defaultdict(list)
    for row in rows:
        knowledge_id = _value(row, "knowledge_id", 1)
        if knowledge_id is not None:
            result[int(_value(row, "content_id", 0))].append(int(knowledge_id))
    return {content_id: tuple(dict.fromkeys(values)) for content_id, values in result.items()}


def _load_newsletter_sends(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
) -> list[dict[str, Any]]:
    if "newsletter_sends" not in schema:
        return []
    columns = schema["newsletter_sends"]
    if not {"id", "source_content_ids"}.issubset(columns):
        return []
    selected = [
        _column_expr(columns, "id"),
        _column_expr(columns, "issue_id"),
        _column_expr(columns, "subject"),
        _column_expr(columns, "source_content_ids"),
        _column_expr(columns, "sent_at"),
    ]
    rows = conn.execute(f"SELECT {', '.join(selected)} FROM newsletter_sends ORDER BY sent_at DESC, id DESC").fetchall()
    sends = []
    for row in rows:
        data = _row_dict(row)
        sent_at = _parse_datetime(data.get("sent_at"))
        if sent_at is None or sent_at < cutoff:
            continue
        source_ids, parse_warnings = _parse_content_ids(data.get("source_content_ids"))
        sends.append(
            {
                "id": int(data["id"]),
                "issue_id": data.get("issue_id") or "",
                "subject": data.get("subject") or "",
                "sent_at": sent_at,
                "source_content_ids": source_ids,
                "parse_warnings": parse_warnings,
                "missing_source_ids": (),
            }
        )
    existing_ids = set(_load_content(conn, schema))
    return [
        {
            **send,
            "missing_source_ids": tuple(
                content_id for content_id in send["source_content_ids"] if content_id not in existing_ids
            ),
        }
        for send in sends
    ]


def _latest_activity(
    content_ids: list[int],
    newsletter_sends: list[dict[str, Any]],
    all_content: dict[int, dict[str, Any]],
) -> datetime | None:
    dates = [
        parsed
        for content_id in content_ids
        for parsed in [_effective_activity_at(all_content[content_id])]
        if parsed is not None
    ]
    dates.extend(send["sent_at"] for send in newsletter_sends if send.get("sent_at"))
    return max(dates) if dates else None


def _effective_activity_at(row: dict[str, Any]) -> datetime | None:
    dates = [_parse_datetime(row.get("published_at")), _parse_datetime(row.get("created_at"))]
    dates = [date for date in dates if date is not None]
    return max(dates) if dates else None


def _freshness(latest_activity: datetime | None, now: datetime) -> str:
    if latest_activity is None:
        return "unknown"
    age_days = max(0, int((now - latest_activity).total_seconds() // 86400))
    if age_days <= 14:
        return "fresh"
    if age_days <= 45:
        return "recent"
    return "stale"


def _freshness_rank(value: str) -> int:
    return {"fresh": 0, "recent": 1, "stale": 2, "unknown": 3}.get(value, 4)


def _title_suggestion(topics: tuple[str, ...], content_types: tuple[str, ...]) -> str:
    if topics:
        primary = topics[0].replace("-", " ").title()
    else:
        primary = _content_family(content_types[0] if content_types else "content").replace("_", " ").title()
    return f"{primary} Series"


def _series_key(topics: tuple[str, ...], knowledge_ids: tuple[int, ...], content_ids: list[int]) -> str:
    if topics:
        return "topic:" + topics[0]
    if knowledge_ids:
        return "knowledge:" + str(knowledge_ids[0])
    return "content:" + "-".join(str(content_id) for content_id in content_ids[:3])


def _content_family(content_type: Any) -> str:
    value = str(content_type or "unknown").strip().lower()
    if value in {"x_post", "bluesky_post", "linkedin_post", "mastodon_post"}:
        return "short_post"
    if value in {"x_thread", "thread"}:
        return "thread"
    if "newsletter" in value:
        return "newsletter"
    if "blog" in value:
        return "blog"
    return value or "unknown"


def _normalize_label(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _empty_plan(
    generated_at: datetime,
    days: int,
    min_items: int,
    cutoff: datetime,
    missing: tuple[str, ...],
) -> BlogSeriesPlan:
    return BlogSeriesPlan(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "min_items": min_items,
            "activity_since": cutoff.isoformat(),
        },
        totals={
            "content_items_considered": 0,
            "newsletter_sends_considered": 0,
            "candidate_count": 0,
            "excluded_weak_candidates": 0,
            "missing_tables": len(missing),
        },
        candidates=(),
        missing_tables=missing,
    )


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    names = [str(_value(row, "name", 0)) for row in rows]
    return {
        name: {
            str(_value(column, "name", 1))
            for column in conn.execute(f"PRAGMA table_info({name})").fetchall()
        }
        for name in names
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _row_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "keys"):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def _value(row: Any, key: str, index: int) -> Any:
    if hasattr(row, "keys") and key in row.keys():
        return row[key]
    return row[index]


def _column_expr(columns: set[str], name: str) -> str:
    return name if name in columns else f"NULL AS {name}"


def _parse_content_ids(value: Any) -> tuple[list[int], tuple[str, ...]]:
    if value in (None, ""):
        return [], ()
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError):
        return [], ("malformed_source_content_ids",)
    if not isinstance(parsed, list):
        return [], ("malformed_source_content_ids",)
    result = []
    malformed = False
    for item in parsed:
        try:
            content_id = int(item)
        except (TypeError, ValueError):
            malformed = True
            continue
        if content_id <= 0:
            malformed = True
            continue
        result.append(content_id)
    warnings = ("malformed_source_content_ids",) if malformed else ()
    return result, warnings


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _find(parent: dict[int, int], item: int) -> int:
    while parent[item] != item:
        parent[item] = parent[parent[item]]
        item = parent[item]
    return item


def _union(parent: dict[int, int], left: int, right: int) -> None:
    left_root = _find(parent, left)
    right_root = _find(parent, right)
    if left_root == right_root:
        return
    if left_root < right_root:
        parent[right_root] = left_root
    else:
        parent[left_root] = right_root
