"""Assemble read-only campaign handoff packets for review."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


STATUS_OK = "ok"
STATUS_NOT_FOUND = "not_found"
STATUS_AMBIGUOUS = "ambiguous"

SUMMARY_MAX_CHARS = 220
_WHITESPACE_RE = re.compile(r"\s+")
_SLUG_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class CampaignHandoffPacket:
    """Reviewable point-in-time bundle for one campaign."""

    artifact_type: str
    generated_at: str
    status: str
    filters: dict[str, Any]
    campaign: dict[str, Any] | None
    planned_topic_status_counts: dict[str, int]
    planned_topics: tuple[dict[str, Any], ...]
    generated_content: tuple[dict[str, Any], ...]
    publish_queue_state: dict[str, Any]
    evidence_readiness: dict[str, Any]
    engagement_summaries: dict[str, Any]
    availability: dict[str, bool]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None
    message: str | None = None
    matches: tuple[dict[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "availability": dict(sorted(self.availability.items())),
            "campaign": self.campaign,
            "engagement_summaries": self.engagement_summaries,
            "evidence_readiness": self.evidence_readiness,
            "filters": self.filters,
            "generated_at": self.generated_at,
            "generated_content": list(self.generated_content),
            "matches": list(self.matches),
            "message": self.message,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "planned_topic_status_counts": self.planned_topic_status_counts,
            "planned_topics": list(self.planned_topics),
            "publish_queue_state": self.publish_queue_state,
            "status": self.status,
        }


def build_campaign_handoff_packet(
    db_or_conn: Any,
    *,
    campaign_id: int | None = None,
    campaign: str | None = None,
    now: datetime | None = None,
) -> CampaignHandoffPacket:
    """Build a read-only handoff packet for exactly one campaign when resolvable."""

    if campaign_id is not None and campaign_id <= 0:
        raise ValueError("campaign_id must be positive")
    if campaign is not None and not campaign.strip():
        raise ValueError("campaign must not be blank")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _as_utc(now or datetime.now(timezone.utc)).isoformat()
    missing_tables: set[str] = set()
    missing_columns: dict[str, tuple[str, ...]] = {}
    filters = {"campaign_id": campaign_id, "campaign": campaign}

    resolved, status, message, matches = _resolve_campaign(
        conn,
        schema,
        campaign_id=campaign_id,
        campaign=campaign,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
    availability = _availability(schema)

    if status != STATUS_OK or resolved is None:
        return CampaignHandoffPacket(
            artifact_type="campaign_handoff_packet",
            generated_at=generated_at,
            status=status,
            filters=filters,
            campaign=None,
            planned_topic_status_counts={"total": 0},
            planned_topics=(),
            generated_content=(),
            publish_queue_state=_empty_publish_state(),
            evidence_readiness=_empty_evidence_state(),
            engagement_summaries=_empty_engagement_state(),
            availability=availability,
            missing_tables=tuple(sorted(missing_tables)),
            missing_columns=missing_columns,
            message=message,
            matches=tuple(matches),
        )

    topic_rows = _load_planned_topics(
        conn,
        schema,
        int(resolved["id"]),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
    _record_optional_requirements(schema, missing_tables, missing_columns)
    content_ids = sorted(
        {
            int(row["content_id"])
            for row in topic_rows
            if row.get("content_id") is not None
        }
    )
    publication_state = _load_publication_state(
        conn,
        schema,
        content_ids,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
    queue_state = _load_publish_queue_state(
        conn,
        schema,
        content_ids,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
    engagement = _load_engagement_summaries(
        conn,
        schema,
        content_ids,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
    evidence = _build_evidence_readiness(
        conn,
        schema,
        topic_rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )

    content = _generated_content_items(topic_rows, publication_state, queue_state, engagement)
    topics = _planned_topic_items(topic_rows, publication_state, queue_state, evidence)
    publish_state = _publish_state_summary(queue_state, publication_state)

    return CampaignHandoffPacket(
        artifact_type="campaign_handoff_packet",
        generated_at=generated_at,
        status=STATUS_OK,
        filters=filters,
        campaign=_json_ready(resolved),
        planned_topic_status_counts=_status_counts(topic_rows),
        planned_topics=tuple(topics),
        generated_content=tuple(content),
        publish_queue_state=publish_state,
        evidence_readiness=evidence["summary"],
        engagement_summaries={
            "summary": engagement["summary"],
            "by_content": engagement["by_content"],
        },
        availability=availability,
        missing_tables=tuple(sorted(missing_tables)),
        missing_columns=missing_columns,
    )


def format_campaign_handoff_packet_json(packet: CampaignHandoffPacket) -> str:
    """Serialize a campaign handoff packet as deterministic JSON."""
    return json.dumps(packet.to_dict(), indent=2, sort_keys=True)


def format_campaign_handoff_packet_text(packet: CampaignHandoffPacket) -> str:
    """Render a campaign handoff packet for terminal review."""
    lines = [
        "Campaign Handoff Packet",
        f"Generated: {packet.generated_at}",
        f"Status: {packet.status}",
    ]
    if packet.message:
        lines.append(f"Message: {packet.message}")
    if packet.missing_tables:
        lines.append("Missing tables: " + ", ".join(packet.missing_tables))
    if packet.missing_columns:
        details = ", ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(packet.missing_columns.items())
        )
        lines.append("Missing columns: " + details)
    if packet.status != STATUS_OK:
        if packet.matches:
            lines.append("Matches:")
            for match in packet.matches:
                lines.append(f"- #{match.get('id')} {match.get('name')} [{match.get('status')}]")
        return "\n".join(lines)

    campaign = packet.campaign or {}
    counts = packet.planned_topic_status_counts
    queue = packet.publish_queue_state["summary"]
    evidence = packet.evidence_readiness
    engagement = packet.engagement_summaries["summary"]
    lines.extend(
        [
            f"Campaign: #{campaign.get('id')} {campaign.get('name')}",
            f"Goal: {campaign.get('goal') or 'n/a'}",
            (
                "Topics: "
                + ", ".join(f"{key}={counts[key]}" for key in sorted(counts))
            ),
            (
                f"Content: generated={len(packet.generated_content)} "
                f"queued={queue['queued']} held={queue['held']} "
                f"published={queue['published']}"
            ),
            (
                f"Evidence: ready={evidence['ready']} thin={evidence['thin']} "
                f"missing={evidence['missing']} unavailable={evidence['unavailable']}"
            ),
            (
                f"Engagement: content={engagement['content_count']} "
                f"snapshots={engagement['snapshot_count']} "
                f"avg_score={_display_float(engagement['avg_engagement_score'])}"
            ),
        ]
    )
    if packet.planned_topics:
        lines.append("Planned topics:")
        for topic in packet.planned_topics:
            lines.append(
                f"- #{topic['planned_topic_id']} [{topic['status']}] "
                f"{topic['topic']} target={topic['target_date'] or 'unscheduled'} "
                f"content={topic['content_id'] or 'none'} "
                f"evidence={topic['evidence_status']}"
            )
    if packet.generated_content:
        lines.append("Generated content:")
        for item in packet.generated_content:
            lines.append(
                f"- #{item['content_id']} {item['content_type'] or 'content'} "
                f"topic=#{item['planned_topic_id']} "
                f"publications={len(item['publication_states'])} "
                f"queue={len(item['queue_items'])}"
            )
    return "\n".join(lines)


def _resolve_campaign(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    campaign_id: int | None,
    campaign: str | None,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> tuple[dict[str, Any] | None, str, str | None, list[dict[str, Any]]]:
    if not _require_columns(
        schema,
        "content_campaigns",
        ("id", "name"),
        missing_tables,
        missing_columns,
    ):
        return None, STATUS_NOT_FOUND, "content_campaigns table is unavailable", []

    if campaign_id is not None:
        cursor = conn.execute(
            "SELECT * FROM content_campaigns WHERE id = ?",
            (campaign_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None, STATUS_NOT_FOUND, f"Campaign {campaign_id} not found", []
        return _json_ready(_row_dict(row, cursor.description)), STATUS_OK, None, []

    if campaign is None:
        return None, STATUS_NOT_FOUND, "campaign_id or campaign filter is required", []

    text = campaign.strip()
    columns = schema["content_campaigns"]
    predicates = ["name = ?"]
    params: list[Any] = [text]
    if "slug" in columns:
        predicates.append("slug = ?")
        params.append(text)
    rows = _fetch_dicts(
        conn,
        f"""SELECT *
            FROM content_campaigns
            WHERE {' OR '.join(predicates)}
            ORDER BY created_at ASC, id ASC""",
        params,
    )
    if not rows:
        all_rows = _fetch_dicts(
            conn,
            "SELECT * FROM content_campaigns ORDER BY created_at ASC, id ASC",
            [],
        )
        rows = [row for row in all_rows if _slug(row.get("name")) == _slug(text)]
    if not rows:
        return None, STATUS_NOT_FOUND, f"Campaign {text!r} not found", []
    if len(rows) > 1:
        matches = [
            {
                "id": row.get("id"),
                "name": row.get("name"),
                "status": row.get("status"),
                "start_date": row.get("start_date"),
                "end_date": row.get("end_date"),
            }
            for row in rows
        ]
        return None, STATUS_AMBIGUOUS, f"Campaign {text!r} matched {len(rows)} rows", matches
    return _json_ready(rows[0]), STATUS_OK, None, []


def _load_planned_topics(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    campaign_id: int,
    *,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> list[dict[str, Any]]:
    required = ("id", "campaign_id", "topic", "status", "content_id")
    if not _require_columns(schema, "planned_topics", required, missing_tables, missing_columns):
        return []
    pt = schema["planned_topics"]
    gc = schema.get("generated_content", set())
    select_columns = [
        "pt.id AS planned_topic_id",
        "pt.campaign_id",
        "pt.topic",
        _column_expr(pt, "angle", alias="angle"),
        _column_expr(pt, "source_material", alias="source_material"),
        _column_expr(pt, "target_date", alias="target_date"),
        "pt.status",
        "pt.content_id",
        _column_expr(pt, "created_at", alias="planned_at"),
    ]
    join = ""
    if "generated_content" in schema and {"id"}.issubset(gc):
        join = "LEFT JOIN generated_content gc ON gc.id = pt.content_id"
        select_columns.extend(
            [
                _column_expr(gc, "content_type", prefix="gc", alias="content_type"),
                _column_expr(gc, "content", prefix="gc", alias="content"),
                _column_expr(gc, "content_format", prefix="gc", alias="content_format"),
                _column_expr(gc, "eval_score", prefix="gc", alias="eval_score"),
                _column_expr(gc, "published", prefix="gc", alias="legacy_published"),
                _column_expr(gc, "published_url", prefix="gc", alias="legacy_published_url"),
                _column_expr(gc, "published_at", prefix="gc", alias="legacy_published_at"),
                _column_expr(gc, "created_at", prefix="gc", alias="content_created_at"),
            ]
        )
    else:
        if "generated_content" not in schema:
            missing_tables.add("generated_content")
        for alias in (
            "content_type",
            "content",
            "content_format",
            "eval_score",
            "legacy_published",
            "legacy_published_url",
            "legacy_published_at",
            "content_created_at",
        ):
            select_columns.append(f"NULL AS {alias}")

    order_terms = ["pt.id ASC"]
    if "created_at" in pt:
        order_terms.insert(0, "pt.created_at ASC")
    if "target_date" in pt:
        order_terms.insert(0, "pt.target_date ASC NULLS LAST")

    return _fetch_dicts(
        conn,
        f"""SELECT {', '.join(select_columns)}
            FROM planned_topics pt
            {join}
            WHERE pt.campaign_id = ?
            ORDER BY {', '.join(order_terms)}""",
        [campaign_id],
    )


def _load_publication_state(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: list[int],
    *,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> dict[int, list[dict[str, Any]]]:
    if not content_ids:
        return {}
    required = ("content_id", "platform", "status")
    if not _require_columns(
        schema,
        "content_publications",
        required,
        missing_tables,
        missing_columns,
    ):
        return {}
    placeholders = ", ".join("?" for _ in content_ids)
    rows = _fetch_dicts(
        conn,
        f"""SELECT *
            FROM content_publications
            WHERE content_id IN ({placeholders})
            ORDER BY content_id ASC, platform ASC, id ASC""",
        list(content_ids),
    )
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[int(row["content_id"])].append(_json_ready(row))
    return dict(grouped)


def _load_publish_queue_state(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: list[int],
    *,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> dict[int, list[dict[str, Any]]]:
    if not content_ids:
        return {}
    required = ("content_id", "status", "platform")
    if not _require_columns(schema, "publish_queue", required, missing_tables, missing_columns):
        return {}
    placeholders = ", ".join("?" for _ in content_ids)
    rows = _fetch_dicts(
        conn,
        f"""SELECT *
            FROM publish_queue
            WHERE content_id IN ({placeholders})
            ORDER BY scheduled_at ASC, created_at ASC, id ASC""",
        list(content_ids),
    )
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[int(row["content_id"])].append(_json_ready(row))
    return dict(grouped)


def _load_engagement_summaries(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: list[int],
    *,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> dict[str, Any]:
    by_content: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for table, platform, fields in (
        ("post_engagement", "x", ("content_id", "engagement_score", "fetched_at")),
        ("bluesky_engagement", "bluesky", ("content_id", "engagement_score", "fetched_at")),
        ("linkedin_engagement", "linkedin", ("content_id", "engagement_score", "fetched_at")),
        ("mastodon_engagement", "mastodon", ("content_id", "engagement_score", "fetched_at")),
    ):
        for row in _load_engagement_rows(
            conn,
            schema,
            table,
            platform,
            fields,
            content_ids,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        ):
            by_content[int(row["content_id"])].append(row)

    for rows in by_content.values():
        rows.sort(key=lambda row: (row.get("fetched_at") or "", row["platform"], row.get("id") or 0))
    latest = [rows[-1] for rows in by_content.values() if rows]
    scores = [
        float(row["engagement_score"])
        for rows in by_content.values()
        for row in rows
        if row.get("engagement_score") is not None
    ]
    by_platform = Counter(row["platform"] for rows in by_content.values() for row in rows)
    return {
        "by_content": {content_id: rows for content_id, rows in by_content.items()},
        "summary": {
            "content_count": len(by_content),
            "snapshot_count": sum(len(rows) for rows in by_content.values()),
            "latest_snapshot_count": len(latest),
            "avg_engagement_score": round(sum(scores) / len(scores), 2) if scores else None,
            "max_engagement_score": round(max(scores), 2) if scores else None,
            "platform_counts": dict(sorted(by_platform.items())),
        },
    }


def _load_engagement_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    table: str,
    platform: str,
    required: tuple[str, ...],
    content_ids: list[int],
    *,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> list[dict[str, Any]]:
    if not content_ids:
        return []
    if not _require_columns(schema, table, required, missing_tables, missing_columns):
        return []
    placeholders = ", ".join("?" for _ in content_ids)
    rows = _fetch_dicts(
        conn,
        f"""SELECT *
            FROM {table}
            WHERE content_id IN ({placeholders})
            ORDER BY content_id ASC, fetched_at ASC, id ASC""",
        list(content_ids),
    )
    for row in rows:
        row["platform"] = platform
    return [_json_ready(row) for row in rows]


def _build_evidence_readiness(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    topic_rows: list[dict[str, Any]],
    *,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> dict[str, Any]:
    claim_checks = _load_claim_checks(
        conn,
        schema,
        [
            int(row["content_id"])
            for row in topic_rows
            if row.get("content_id") is not None
        ],
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
    by_topic: dict[int, dict[str, Any]] = {}
    counts = Counter({"ready": 0, "thin": 0, "missing": 0, "unavailable": 0})
    for row in topic_rows:
        content_id = int(row["content_id"]) if row.get("content_id") is not None else None
        source_refs = _source_material_refs(row.get("source_material"))
        claim = claim_checks.get(content_id) if content_id is not None else None
        status = _evidence_status(source_refs, claim, content_id)
        counts[status] += 1
        by_topic[int(row["planned_topic_id"])] = {
            "status": status,
            "source_material_count": len(source_refs),
            "has_generated_content": content_id is not None,
            "claim_check": claim,
        }
    return {
        "by_topic": by_topic,
        "summary": {
            "ready": counts["ready"],
            "thin": counts["thin"],
            "missing": counts["missing"],
            "unavailable": counts["unavailable"],
            "topic_count": len(topic_rows),
        },
    }


def _load_claim_checks(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: list[int],
    *,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> dict[int, dict[str, Any]]:
    if not content_ids:
        return {}
    required = ("content_id", "supported_count", "unsupported_count")
    if not _require_columns(
        schema,
        "content_claim_checks",
        required,
        missing_tables,
        missing_columns,
    ):
        return {}
    placeholders = ", ".join("?" for _ in content_ids)
    rows = _fetch_dicts(
        conn,
        f"""SELECT *
            FROM content_claim_checks
            WHERE content_id IN ({placeholders})
            ORDER BY content_id ASC""",
        list(content_ids),
    )
    return {int(row["content_id"]): _json_ready(row) for row in rows}


def _planned_topic_items(
    rows: list[dict[str, Any]],
    publications: dict[int, list[dict[str, Any]]],
    queue: dict[int, list[dict[str, Any]]],
    evidence: dict[str, Any],
) -> list[dict[str, Any]]:
    items = []
    evidence_by_topic = evidence["by_topic"]
    for row in rows:
        content_id = int(row["content_id"]) if row.get("content_id") is not None else None
        topic_evidence = evidence_by_topic.get(int(row["planned_topic_id"]), {})
        items.append(
            {
                "planned_topic_id": row["planned_topic_id"],
                "topic": row.get("topic"),
                "angle": row.get("angle"),
                "target_date": row.get("target_date"),
                "status": row.get("status") or "planned",
                "content_id": content_id,
                "planned_at": row.get("planned_at"),
                "source_material": row.get("source_material"),
                "source_material_count": topic_evidence.get("source_material_count", 0),
                "evidence_status": topic_evidence.get("status", "unavailable"),
                "queue_statuses": sorted({item.get("status") for item in queue.get(content_id or -1, []) if item.get("status")}),
                "publication_statuses": sorted(
                    {
                        item.get("status")
                        for item in publications.get(content_id or -1, [])
                        if item.get("status")
                    }
                ),
            }
        )
    return [_json_ready(item) for item in items]


def _generated_content_items(
    rows: list[dict[str, Any]],
    publications: dict[int, list[dict[str, Any]]],
    queue: dict[int, list[dict[str, Any]]],
    engagement: dict[str, Any],
) -> list[dict[str, Any]]:
    items: dict[int, dict[str, Any]] = {}
    engagement_by_content = engagement["by_content"]
    for row in rows:
        if row.get("content_id") is None:
            continue
        content_id = int(row["content_id"])
        items.setdefault(
            content_id,
            {
                "content_id": content_id,
                "planned_topic_id": row.get("planned_topic_id"),
                "topic": row.get("topic"),
                "angle": row.get("angle"),
                "content_type": row.get("content_type"),
                "content_format": row.get("content_format"),
                "eval_score": row.get("eval_score"),
                "created_at": row.get("content_created_at"),
                "legacy_published": row.get("legacy_published"),
                "legacy_published_url": row.get("legacy_published_url"),
                "legacy_published_at": row.get("legacy_published_at"),
                "content_excerpt": _shorten(row.get("content")),
                "publication_states": publications.get(content_id, []),
                "queue_items": queue.get(content_id, []),
                "engagement_snapshots": engagement_by_content.get(content_id, []),
            },
        )
    return [
        _json_ready(item)
        for item in sorted(
            items.values(),
            key=lambda item: (item.get("created_at") or "", item["content_id"]),
        )
    ]


def _publish_state_summary(
    queue: dict[int, list[dict[str, Any]]],
    publications: dict[int, list[dict[str, Any]]],
) -> dict[str, Any]:
    queue_counts = Counter(
        item.get("status") or "unknown"
        for rows in queue.values()
        for item in rows
    )
    publication_counts = Counter(
        item.get("status") or "unknown"
        for rows in publications.values()
        for item in rows
    )
    return {
        "summary": {
            "queue_items": sum(queue_counts.values()),
            "publication_states": sum(publication_counts.values()),
            "queued": queue_counts["queued"] + publication_counts["queued"],
            "held": queue_counts["held"],
            "published": queue_counts["published"] + publication_counts["published"],
            "failed": queue_counts["failed"] + publication_counts["failed"],
            "cancelled": queue_counts["cancelled"],
        },
        "queue_status_counts": dict(sorted(queue_counts.items())),
        "publication_status_counts": dict(sorted(publication_counts.items())),
    }


def _status_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter((row.get("status") or "planned") for row in rows)
    payload = dict(sorted(counts.items()))
    payload["total"] = len(rows)
    return payload


def _evidence_status(
    source_refs: tuple[str, ...],
    claim_check: dict[str, Any] | None,
    content_id: int | None,
) -> str:
    if claim_check is not None:
        if int(claim_check.get("unsupported_count") or 0) > 0:
            return "thin"
        if int(claim_check.get("supported_count") or 0) > 0 or source_refs:
            return "ready"
    if source_refs:
        return "thin" if content_id is not None else "ready"
    return "missing"


def _source_material_refs(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple, set)):
        return tuple(str(item).strip() for item in value if str(item).strip())
    text = str(value).strip()
    if not text:
        return ()
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        parsed = None
    refs: list[str] = []
    if isinstance(parsed, dict):
        for item in parsed.values():
            refs.extend(_source_material_refs(item))
    elif isinstance(parsed, list):
        refs.extend(str(item).strip() for item in parsed if str(item).strip())
    else:
        refs.extend(part for part in re.split(r"[\s,]+", text) if part)
    return tuple(dict.fromkeys(refs))


def _empty_publish_state() -> dict[str, Any]:
    return {
        "summary": {
            "queue_items": 0,
            "publication_states": 0,
            "queued": 0,
            "held": 0,
            "published": 0,
            "failed": 0,
            "cancelled": 0,
        },
        "queue_status_counts": {},
        "publication_status_counts": {},
    }


def _empty_evidence_state() -> dict[str, Any]:
    return {"ready": 0, "thin": 0, "missing": 0, "unavailable": 0, "topic_count": 0}


def _empty_engagement_state() -> dict[str, Any]:
    return {
        "summary": {
            "content_count": 0,
            "snapshot_count": 0,
            "latest_snapshot_count": 0,
            "avg_engagement_score": None,
            "max_engagement_score": None,
            "platform_counts": {},
        }
    }


def _availability(schema: dict[str, set[str]]) -> dict[str, bool]:
    return {
        table: table in schema
        for table in (
            "content_campaigns",
            "planned_topics",
            "generated_content",
            "publish_queue",
            "content_publications",
            "content_claim_checks",
            "post_engagement",
            "bluesky_engagement",
            "linkedin_engagement",
            "mastodon_engagement",
        )
    }


def _record_optional_requirements(
    schema: dict[str, set[str]],
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> None:
    requirements = {
        "generated_content": ("id",),
        "publish_queue": ("content_id", "platform", "status"),
        "content_publications": ("content_id", "platform", "status"),
        "content_claim_checks": ("content_id", "supported_count", "unsupported_count"),
        "post_engagement": ("content_id", "engagement_score", "fetched_at"),
        "bluesky_engagement": ("content_id", "engagement_score", "fetched_at"),
        "linkedin_engagement": ("content_id", "engagement_score", "fetched_at"),
        "mastodon_engagement": ("content_id", "engagement_score", "fetched_at"),
    }
    for table, columns in requirements.items():
        _require_columns(schema, table, columns, missing_tables, missing_columns)


def _require_columns(
    schema: dict[str, set[str]],
    table: str,
    columns: tuple[str, ...],
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> bool:
    if table not in schema:
        missing_tables.add(table)
        return False
    missing = tuple(sorted(column for column in columns if column not in schema[table]))
    if missing:
        existing = set(missing_columns.get(table, ()))
        missing_columns[table] = tuple(sorted(existing.union(missing)))
        return False
    return True


def _column_expr(
    columns: set[str],
    column: str,
    *,
    prefix: str = "pt",
    alias: str | None = None,
) -> str:
    output = alias or column
    if column in columns:
        return f"{prefix}.{column} AS {output}"
    return f"NULL AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    tables = [row["name"] if hasattr(row, "keys") else row[0] for row in rows]
    return {
        table: {
            column["name"] if hasattr(column, "keys") else column[1]
            for column in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        for table in tables
    }


def _fetch_dicts(
    conn: sqlite3.Connection,
    query: str,
    params: list[Any],
) -> list[dict[str, Any]]:
    cursor = conn.execute(query, tuple(params))
    columns = [column[0] for column in cursor.description or []]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _row_dict(row: Any, description: Any) -> dict[str, Any]:
    if hasattr(row, "keys"):
        return dict(row)
    columns = [column[0] for column in description or []]
    return dict(zip(columns, row))


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, bytes):
        return f"<{len(value)} bytes>"
    return value


def _shorten(value: Any, limit: int = SUMMARY_MAX_CHARS) -> str:
    text = _WHITESPACE_RE.sub(" ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _slug(value: Any) -> str:
    return _SLUG_RE.sub("-", str(value or "").strip().lower()).strip("-")


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _display_float(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):.2f}"
