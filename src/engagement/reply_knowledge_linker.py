"""Link queued replies to relevant knowledge rows."""

from __future__ import annotations

import json
from typing import Any, Mapping


DEFAULT_LIMIT = 20
DEFAULT_MIN_SCORE = 0.72
VALID_REPORT_FORMATS = {"json", "text"}


def link_reply_knowledge(
    db: Any,
    reply_id: int | None = None,
    status: str = "pending",
    limit: int = DEFAULT_LIMIT,
    min_score: float = DEFAULT_MIN_SCORE,
    dry_run: bool = True,
    *,
    search_provider: Any | None = None,
) -> dict[str, Any]:
    """Find and optionally persist relevant knowledge links for reply drafts.

    ``search_provider`` is expected to expose ``search_similar(query, limit=...,
    min_similarity=...)`` and return ``(KnowledgeItem, score)`` style results.
    Passing it explicitly keeps tests deterministic while the CLI can provide a
    real ``KnowledgeStore``.
    """
    if limit <= 0:
        raise ValueError("limit must be positive")
    if not 0 <= min_score <= 1:
        raise ValueError("min_score must be between 0 and 1")
    if reply_id is None and not status:
        raise ValueError("status is required when reply_id is not set")

    conn = _connection(db)
    provider = search_provider or getattr(db, "knowledge_store", None)
    replies = _load_replies(conn, reply_id=reply_id, status=status, limit=limit)

    reply_reports: list[dict[str, Any]] = []
    totals = {
        "replies_scanned": len(replies),
        "search_result_count": 0,
        "linked_count": 0,
        "proposed_count": 0,
        "existing_count": 0,
        "excluded_below_min_score_count": 0,
        "empty_search_count": 0,
    }

    for reply in replies:
        query = build_reply_knowledge_query(reply)
        existing_ids = _existing_knowledge_ids(conn, int(reply["id"]))
        normalized_results = _search(provider, query, limit=limit, min_score=min_score)
        totals["search_result_count"] += len(normalized_results)
        if not normalized_results:
            totals["empty_search_count"] += 1

        accepted: list[dict[str, Any]] = []
        existing: list[dict[str, Any]] = []
        excluded: list[dict[str, Any]] = []
        seen_ids: set[int] = set()

        for result in normalized_results:
            knowledge_id = result["knowledge_id"]
            if knowledge_id is None or knowledge_id in seen_ids:
                continue
            seen_ids.add(knowledge_id)
            if result["score"] < min_score:
                excluded.append(result)
                continue
            if knowledge_id in existing_ids:
                existing.append(result)
                continue
            accepted.append(result)

        inserted_count = 0
        if accepted and not dry_run:
            _insert_links(conn, int(reply["id"]), accepted)
            inserted_count = len(accepted)

        totals["excluded_below_min_score_count"] += len(excluded)
        totals["existing_count"] += len(existing)
        if dry_run:
            totals["proposed_count"] += len(accepted)
        else:
            totals["linked_count"] += inserted_count

        reply_reports.append(
            {
                "reply_id": int(reply["id"]),
                "status": reply.get("status"),
                "author_handle": reply.get("inbound_author_handle"),
                "query": query,
                "search_result_count": len(normalized_results),
                "proposed_links": accepted,
                "inserted_count": inserted_count,
                "existing_links": existing,
                "excluded_below_min_score": excluded,
            }
        )

    return {
        "dry_run": dry_run,
        "reply_id": reply_id,
        "status": status,
        "limit": limit,
        "min_score": min_score,
        "totals": totals,
        "replies": reply_reports,
    }


def build_reply_knowledge_query(reply: Mapping[str, Any]) -> str:
    """Build a stable semantic query from the review-relevant reply fields."""
    parts = [
        ("inbound", reply.get("inbound_text")),
        ("author", reply.get("inbound_author_handle")),
        ("relationship", _relationship_text(reply.get("relationship_context"))),
        ("draft", reply.get("draft_text")),
    ]
    return "\n".join(
        f"{label}: {str(value).strip()}"
        for label, value in parts
        if value is not None and str(value).strip()
    )


def format_reply_knowledge_report_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True, default=str)


def format_reply_knowledge_report_text(report: Mapping[str, Any]) -> str:
    totals = report["totals"]
    mode = "dry-run" if report.get("dry_run") else "apply"
    lines = [
        "Reply knowledge link report",
        f"Mode: {mode}",
        f"Replies scanned: {totals['replies_scanned']}",
        f"Search results: {totals['search_result_count']}",
        f"Proposed links: {totals['proposed_count']}",
        f"Inserted links: {totals['linked_count']}",
        f"Already linked: {totals['existing_count']}",
        f"Excluded below min score: {totals['excluded_below_min_score_count']}",
        f"Empty searches: {totals['empty_search_count']}",
    ]
    if not report["replies"]:
        lines.extend(["", "No reply rows matched the selected target."])
        return "\n".join(lines)

    for item in report["replies"]:
        lines.append("")
        lines.append(
            f"Reply #{item['reply_id']} @{item.get('author_handle') or '?'} "
            f"status={item.get('status') or '?'}"
        )
        links = item["proposed_links"]
        if not links:
            lines.append("  No new links.")
        for link in links:
            lines.append(
                "  knowledge #{knowledge_id} score={score:.3f} {label}".format(
                    knowledge_id=link["knowledge_id"],
                    score=link["score"],
                    label=link.get("label") or "",
                ).rstrip()
            )
        if item["excluded_below_min_score"]:
            lines.append(
                f"  excluded_below_min_score={len(item['excluded_below_min_score'])}"
            )
    return "\n".join(lines)


def _connection(db: Any) -> Any:
    return getattr(db, "conn", db)


def _load_replies(
    conn: Any,
    *,
    reply_id: int | None,
    status: str,
    limit: int,
) -> list[dict[str, Any]]:
    columns = _table_columns(conn, "reply_queue")
    if not columns:
        return []
    select_columns = [
        column
        for column in (
            "id",
            "status",
            "inbound_author_handle",
            "inbound_text",
            "relationship_context",
            "draft_text",
        )
        if column in columns
    ]
    if "id" not in select_columns:
        return []
    if reply_id is not None:
        rows = conn.execute(
            f"SELECT {', '.join(select_columns)} FROM reply_queue WHERE id = ?",
            (reply_id,),
        ).fetchall()
    else:
        order_by = "datetime(detected_at) ASC, id ASC" if "detected_at" in columns else "id ASC"
        rows = conn.execute(
            f"""SELECT {', '.join(select_columns)} FROM reply_queue
                WHERE status = ?
                ORDER BY {order_by}
                LIMIT ?""",
            (status, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def _existing_knowledge_ids(conn: Any, reply_id: int) -> set[int]:
    if not _table_columns(conn, "reply_knowledge_links"):
        return set()
    rows = conn.execute(
        "SELECT knowledge_id FROM reply_knowledge_links WHERE reply_queue_id = ?",
        (reply_id,),
    ).fetchall()
    return {int(row["knowledge_id"]) for row in rows if row["knowledge_id"] is not None}


def _insert_links(conn: Any, reply_id: int, links: list[dict[str, Any]]) -> None:
    for link in links:
        conn.execute(
            """INSERT INTO reply_knowledge_links
               (reply_queue_id, knowledge_id, relevance_score)
               VALUES (?, ?, ?)""",
            (reply_id, link["knowledge_id"], link["score"]),
        )
    conn.commit()


def _search(provider: Any | None, query: str, *, limit: int, min_score: float) -> list[dict[str, Any]]:
    if provider is None or not hasattr(provider, "search_similar"):
        return []
    try:
        raw_results = provider.search_similar(query, limit=limit, min_similarity=0.0)
    except TypeError:
        raw_results = provider.search_similar(query, limit=limit)
    return [_normalize_search_result(result) for result in raw_results]


def _normalize_search_result(result: Any) -> dict[str, Any]:
    item: Any
    score: float
    if hasattr(result, "item"):
        item = result.item
        score = float(getattr(result, "combined_score", getattr(result, "score", 0.0)))
    else:
        item, raw_score = result
        score = float(raw_score)

    knowledge_id = _value(item, "id")
    return {
        "knowledge_id": int(knowledge_id) if knowledge_id is not None else None,
        "score": score,
        "source_type": _value(item, "source_type"),
        "source_id": _value(item, "source_id"),
        "author": _value(item, "author"),
        "label": _result_label(item),
    }


def _result_label(item: Any) -> str:
    source_type = _value(item, "source_type")
    source_id = _value(item, "source_id")
    author = _value(item, "author")
    bits = [str(bit) for bit in (source_type, source_id, author) if bit]
    return " ".join(bits)


def _value(item: Any, key: str) -> Any:
    if isinstance(item, Mapping):
        return item.get(key)
    return getattr(item, key, None)


def _relationship_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        return json.dumps(value, sort_keys=True)
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return value
    if isinstance(parsed, Mapping):
        return json.dumps(parsed, sort_keys=True)
    return str(parsed)


def _table_columns(conn: Any, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except Exception:
        return set()
