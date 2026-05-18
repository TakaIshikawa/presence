"""Build a prioritized queue of blog posts with unresolved revision follow-ups."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
from typing import Any


DEFAULT_LIMIT = 50
SEVERITY_WEIGHTS = {"critical": 400, "high": 300, "medium": 200, "low": 100, "info": 50, "unknown": 0}


def build_blog_revision_followup_report(
    blog_rows: list[dict[str, Any]],
    note_rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    posts = {_post_id(row): _post(row) for row in blog_rows}
    unresolved = [_note(row, generated_at) for row in note_rows if _is_unresolved(row)]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for note in unresolved:
        grouped[note["post_id"]].append(note)

    followups = []
    for post_id, notes in grouped.items():
        notes.sort(key=lambda note: (note["created_at_sort"], note["note_id"]), reverse=True)
        post = posts.get(post_id, _missing_post(post_id))
        latest_note = notes[0]
        updated_at = post["updated_at_sort"]
        age_since_update = _age_days(updated_at, generated_at)
        severity = _max_severity(note["severity"] for note in notes)
        followups.append(
            {
                "post_id": post_id,
                "title": post["title"],
                "status": post["status"],
                "last_blog_update_at": post["updated_at"],
                "latest_note_at": latest_note["created_at"],
                "age_since_latest_note_days": latest_note["age_days"],
                "age_since_last_blog_update_days": age_since_update,
                "severity": severity,
                "unresolved_note_count": len(notes),
                "priority_score": _priority_score(severity, latest_note["age_days"], age_since_update, len(notes)),
                "notes": [{key: value for key, value in note.items() if key != "created_at_sort"} for note in notes],
            }
        )

    followups.sort(key=lambda item: (-item["priority_score"], -item["age_since_latest_note_days"], item["post_id"]))
    return {
        "artifact_type": "blog_revision_followup",
        "generated_at": generated_at.isoformat(),
        "summary": {
            "blog_posts": len(blog_rows),
            "note_rows": len(note_rows),
            "unresolved_notes": len(unresolved),
            "followup_items": len(followups),
        },
        "followup_queue": followups[:limit],
    }


def format_blog_revision_followup_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_blog_revision_followup_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Blog Revision Follow-up",
        f"Generated: {report['generated_at']}",
        f"Totals: posts={summary['blog_posts']} notes={summary['note_rows']} "
        f"unresolved_notes={summary['unresolved_notes']} followups={summary['followup_items']}",
    ]
    if report["followup_queue"]:
        lines.extend(["", "Follow-up queue:"])
        for item in report["followup_queue"]:
            lines.append(
                f"  - post_id={item['post_id']} severity={item['severity']} "
                f"note_age_days={item['age_since_latest_note_days']} update_age_days={item['age_since_last_blog_update_days']} "
                f"title={item['title']}"
            )
    return "\n".join(lines)


def _post(row: dict[str, Any]) -> dict[str, Any]:
    updated_at = _parse_dt(_first(row, "updated_at", "last_updated_at", "modified_at", "published_at", "created_at"))
    return {
        "post_id": _post_id(row),
        "title": _text(_first(row, "title", "headline", "slug")) or "Untitled",
        "status": _text(_first(row, "status", "state")) or "unknown",
        "updated_at": updated_at.isoformat() if updated_at else None,
        "updated_at_sort": updated_at,
    }


def _missing_post(post_id: str) -> dict[str, Any]:
    return {"post_id": post_id, "title": "Unknown post", "status": "unknown", "updated_at": None, "updated_at_sort": None}


def _note(row: dict[str, Any], now: datetime) -> dict[str, Any]:
    created_at = _parse_dt(_first(row, "created_at", "noted_at", "reviewed_at", "updated_at"))
    return {
        "note_id": _text(_first(row, "note_id", "revision_id", "id")) or "unknown",
        "post_id": _text(_first(row, "post_id", "blog_id", "content_id", "article_id")) or "unknown",
        "note_type": _text(_first(row, "note_type", "type", "marker")) or "revision",
        "severity": _severity(_first(row, "severity", "priority")),
        "text": _text(_first(row, "note", "reviewer_note", "comment", "text", "body")),
        "created_at": created_at.isoformat() if created_at else None,
        "created_at_sort": created_at or datetime.min.replace(tzinfo=timezone.utc),
        "age_days": _age_days(created_at, now),
    }


def _is_unresolved(row: dict[str, Any]) -> bool:
    status = _text(_first(row, "status", "state", "resolution_status")).lower()
    if status in {"resolved", "closed", "done", "superseded", "dismissed"}:
        return False
    if _first(row, "resolved_at", "closed_at", "superseded_at", "superseded_by"):
        return False
    if _first(row, "resolved", "is_resolved") in {True, 1, "1", "true", "True", "yes"}:
        return False
    return True


def _max_severity(values: Any) -> str:
    severities = list(values)
    if not severities:
        return "unknown"
    return max(severities, key=lambda severity: SEVERITY_WEIGHTS.get(severity, 0))


def _priority_score(severity: str, note_age_days: int, update_age_days: int, note_count: int) -> int:
    return SEVERITY_WEIGHTS.get(severity, 0) + note_age_days * 2 + update_age_days + note_count * 10


def _severity(value: Any) -> str:
    text = _text(value).lower()
    return text if text in SEVERITY_WEIGHTS else "unknown"


def _age_days(then: datetime | None, now: datetime) -> int:
    if then is None:
        return 0
    return max(0, int((now - then).total_seconds() // 86400))


def _post_id(row: dict[str, Any]) -> str:
    return _text(_first(row, "post_id", "blog_id", "content_id", "article_id", "id")) or "unknown"


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""
