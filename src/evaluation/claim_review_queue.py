"""Export generated content with unsupported claim-check annotations."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any


def _normalize_now(now: datetime | None) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now


def _bool_from_db(value: Any) -> bool:
    return bool(int(value or 0))


def _annotation_summary(annotation_text: str | None, limit: int = 240) -> str:
    text = " ".join((annotation_text or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def list_claim_review_items(
    db: Any,
    *,
    days: int = 30,
    include_published: bool = False,
    unsupported_only: bool = True,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Return generated content rows with persisted claim-check summaries."""
    if days < 1:
        raise ValueError("days must be at least 1")

    cutoff = (_normalize_now(now) - timedelta(days=days)).isoformat()
    filters = ["datetime(gc.created_at) >= datetime(?)"]
    params: list[Any] = [cutoff]

    if unsupported_only:
        filters.append("ccc.unsupported_count > 0")
    if not include_published:
        filters.append("COALESCE(gc.published, 0) != 1")

    rows = db.conn.execute(
        f"""SELECT
                  gc.id AS content_id,
                  gc.content_type,
                  gc.content,
                  gc.created_at,
                  gc.published,
                  gc.published_url,
                  gc.published_at,
                  ccc.supported_count,
                  ccc.unsupported_count,
                  ccc.annotation_text,
                  ccc.created_at AS claim_check_created_at,
                  ccc.updated_at AS claim_check_updated_at
           FROM content_claim_checks ccc
           INNER JOIN generated_content gc ON gc.id = ccc.content_id
           WHERE {' AND '.join(filters)}
           ORDER BY
               CASE WHEN ccc.unsupported_count > 0 THEN 0 ELSE 1 END ASC,
               ccc.unsupported_count DESC,
               gc.created_at DESC,
               gc.id ASC""",
        tuple(params),
    ).fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        data = dict(row)
        annotation_text = data.get("annotation_text")
        item = {
            "content_id": int(data["content_id"]),
            "content_type": data["content_type"],
            "content": data["content"],
            "created_at": data["created_at"],
            "published": _bool_from_db(data.get("published")),
            "published_url": data.get("published_url"),
            "published_at": data.get("published_at"),
            "supported_count": int(data.get("supported_count") or 0),
            "unsupported_count": int(data.get("unsupported_count") or 0),
            "annotation_text": annotation_text,
            "annotation_summary": _annotation_summary(annotation_text),
            "claim_check_created_at": data.get("claim_check_created_at"),
            "claim_check_updated_at": data.get("claim_check_updated_at"),
        }
        items.append(item)
    return items


def build_claim_review_payload(
    db: Any,
    *,
    days: int = 30,
    include_published: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a stable JSON-serializable claim review queue payload."""
    return {
        "days": days,
        "include_published": include_published,
        "unsupported_only": True,
        "items": list_claim_review_items(
            db,
            days=days,
            include_published=include_published,
            unsupported_only=True,
            now=now,
        ),
    }


def format_json(payload: dict[str, Any]) -> str:
    """Serialize the review queue in a stable JSON form."""
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


def format_markdown(payload: dict[str, Any]) -> str:
    """Serialize the review queue as Markdown for manual review."""
    lines = [
        f"# Claim Review Queue (last {payload['days']} days)",
        "",
        f"Published content included: {'yes' if payload['include_published'] else 'no'}",
    ]
    items = payload["items"]
    if not items:
        lines.extend(["", "No generated content with unsupported claims found."])
        return "\n".join(lines)

    for item in items:
        status = "published" if item["published"] else "unpublished"
        lines.extend(
            [
                "",
                f"## Content {item['content_id']}",
                "",
                f"- Type: {item['content_type']}",
                f"- Created: {item['created_at']}",
                f"- Published: {status}",
                f"- Unsupported claims: {item['unsupported_count']}",
                f"- Supported claims: {item['supported_count']}",
                f"- Annotation summary: {item['annotation_summary'] or '-'}",
            ]
        )
        if item.get("published_url"):
            lines.append(f"- Published URL: {item['published_url']}")
        lines.extend(["", "```text", item["annotation_text"] or "", "```"])
    return "\n".join(lines)
