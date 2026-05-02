"""Seed content ideas from recent GitHub activity with high-signal labels."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


SOURCE_NAME = "github_label_idea"
DEFAULT_DAYS = 14
DEFAULT_LIMIT = 10
DEFAULT_LABELS = ("bug", "performance", "security", "incident", "design")
DEFAULT_ACTIVITY_TYPES = ("issue", "pull_request", "discussion", "github_discussion")
DEFAULT_STATES = ("open",)

_WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class GitHubLabelIdeaCandidate:
    repo_name: str
    activity_type: str
    number: str
    title: str
    state: str
    url: str
    updated_at: str
    labels: list[str]
    matched_labels: list[str]
    source_activity_id: str
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GitHubLabelIdeaSeedResult:
    status: str
    repo_name: str
    activity_type: str
    number: str
    title: str
    matched_labels: list[str]
    source_activity_id: str
    idea_id: int | None
    reason: str
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def seed_github_label_ideas(
    db_or_conn: Any,
    *,
    labels: list[str] | tuple[str, ...] | None = None,
    days: int = DEFAULT_DAYS,
    limit: int | None = DEFAULT_LIMIT,
    dry_run: bool = False,
    activity_types: tuple[str, ...] = DEFAULT_ACTIVITY_TYPES,
    states: tuple[str, ...] = DEFAULT_STATES,
    now: datetime | None = None,
) -> list[GitHubLabelIdeaSeedResult]:
    """Create or preview content ideas from recent labeled GitHub activity."""

    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")
    label_filters = _normalize_filters(labels or DEFAULT_LABELS)
    if not label_filters:
        raise ValueError("labels must include at least one non-blank label")
    type_filters = _normalize_filters(activity_types)
    if not type_filters:
        raise ValueError("activity_types must include at least one non-blank type")
    state_filters = _normalize_filters(states)
    if not state_filters:
        raise ValueError("states must include at least one non-blank state")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    rows = _load_activity_rows(
        db_or_conn,
        cutoff=cutoff,
        activity_types=type_filters,
    )
    candidates = [
        candidate
        for row in rows
        if (
            candidate := _row_to_candidate(
                row,
                label_filters=label_filters,
                state_filters=state_filters,
            )
        )
        is not None
    ]
    candidates.sort(
        key=lambda candidate: (
            -_label_signal(candidate.matched_labels),
            _reverse_sort_text(candidate.updated_at),
            candidate.repo_name,
            candidate.activity_type,
            candidate.number,
        )
    )
    if limit is not None:
        candidates = candidates[:limit]

    results: list[GitHubLabelIdeaSeedResult] = []
    for candidate in candidates:
        existing = _find_duplicate_idea(db_or_conn, candidate)
        if existing is not None:
            results.append(_result(candidate, "skipped", int(existing["id"]), f"{existing['status']} duplicate"))
            continue
        if dry_run:
            results.append(_result(candidate, "proposed", None, "dry run"))
            continue
        idea_id = _insert_content_idea(db_or_conn, candidate)
        results.append(_result(candidate, "created", idea_id, "created"))
    return results


def format_github_label_idea_results_json(results: list[GitHubLabelIdeaSeedResult]) -> str:
    return json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True)


def format_github_label_idea_results_text(results: list[GitHubLabelIdeaSeedResult]) -> str:
    created = sum(1 for result in results if result.status == "created")
    proposed = sum(1 for result in results if result.status == "proposed")
    skipped = sum(1 for result in results if result.status == "skipped")
    lines = [f"created={created} proposed={proposed} skipped={skipped}"]
    lines.append(f"{'Status':9s}  {'ID':>4s}  {'Labels':22s}  Activity / reason")
    lines.append(f"{'-' * 9:9s}  {'-' * 4:>4s}  {'-' * 22:22s}  {'-' * 54}")
    if not results:
        lines.append("none       ----  ----------------------  no eligible labeled GitHub activity")
        return "\n".join(lines)

    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        labels = ", ".join(result.matched_labels) or "-"
        detail = f"{result.repo_name}#{result.number} {result.activity_type}: {result.title}"
        lines.append(
            f"{result.status:9s}  {idea_id:>4s}  {_shorten(labels, 22):22s}  "
            f"{_shorten(detail, 54)} ({result.reason})"
        )
    return "\n".join(lines)


def _load_activity_rows(
    db_or_conn: Any,
    *,
    cutoff: datetime,
    activity_types: tuple[str, ...],
) -> list[dict[str, Any]]:
    conn = _connection(db_or_conn)
    placeholders = ", ".join("?" for _ in activity_types)
    cursor = conn.execute(
        f"""SELECT *
           FROM github_activity
           WHERE updated_at >= ?
             AND activity_type IN ({placeholders})
           ORDER BY updated_at DESC, id DESC""",
        (cutoff.isoformat(), *activity_types),
    )
    mapper = getattr(db_or_conn, "_github_activity_from_row", None)
    if callable(mapper):
        return [mapper(row) for row in cursor.fetchall()]
    return [_row_to_dict(row) for row in cursor.fetchall()]


def _row_to_candidate(
    row: dict[str, Any],
    *,
    label_filters: tuple[str, ...],
    state_filters: tuple[str, ...],
) -> GitHubLabelIdeaCandidate | None:
    metadata = _metadata(row.get("metadata"))
    labels = _labels(row.get("labels"))
    normalized_labels = {_normalize_label(label): label for label in labels}
    matched_keys = [label for label in label_filters if label in normalized_labels]
    if not matched_keys:
        return None
    state = _normalize_text(metadata.get("state") or row.get("state")).lower()
    if state not in state_filters:
        return None

    repo_name = _normalize_text(row.get("repo_name"))
    activity_type = _normalize_text(row.get("activity_type"))
    number = _normalize_text(row.get("number"))
    title = _normalize_text(row.get("title") or f"GitHub {activity_type} #{number}")
    url = _normalize_text(row.get("url") or metadata.get("html_url") or metadata.get("url"))
    updated_at = _normalize_text(row.get("updated_at") or row.get("created_at_github"))
    source_activity_id = _normalize_text(
        metadata.get("activity_id")
        or metadata.get("source_activity_id")
        or row.get("activity_id")
        or f"{repo_name}#{number}:{activity_type}"
    )
    matched_labels = sorted(normalized_labels[label] for label in matched_keys)
    topic = _topic_for_labels(matched_keys)
    activity_name = activity_type.replace("_", " ")
    evidence = url or "stored GitHub activity only"
    body_excerpt = _excerpt(row.get("body") or metadata.get("body") or title)
    note = (
        f"Turn labeled GitHub {activity_name} activity into a content idea: "
        f"{repo_name} #{number} ({', '.join(matched_labels)}), {title}. "
        f"Use the repo context, current state, and evidence from {evidence}. "
        f"Relevant detail: {body_excerpt}"
    )
    source_metadata = {
        "source": SOURCE_NAME,
        "activity_id": source_activity_id,
        "source_activity_id": source_activity_id,
        "github_activity_id": row.get("id"),
        "repo_name": repo_name,
        "activity_type": activity_type,
        "number": number,
        "state": state,
        "title": title,
        "url": url,
        "labels": labels,
        "matched_labels": matched_labels,
        "updated_at": updated_at,
    }
    return GitHubLabelIdeaCandidate(
        repo_name=repo_name,
        activity_type=activity_type,
        number=number,
        title=title,
        state=state,
        url=url,
        updated_at=updated_at,
        labels=labels,
        matched_labels=matched_labels,
        source_activity_id=source_activity_id,
        topic=topic,
        note=note,
        priority=_priority_for_labels(matched_keys),
        source_metadata=source_metadata,
    )


def _find_duplicate_idea(
    db_or_conn: Any,
    candidate: GitHubLabelIdeaCandidate,
) -> dict[str, Any] | None:
    conn = _connection(db_or_conn)
    cursor = conn.execute(
        """SELECT *
           FROM content_ideas
           WHERE status IN ('open', 'promoted')
           ORDER BY created_at ASC, id ASC"""
    )
    candidate_ids = {
        "activity_id": _normalize_text(candidate.source_activity_id),
        "source_activity_id": _normalize_text(candidate.source_activity_id),
        "github_activity_id": _normalize_text(candidate.source_metadata.get("github_activity_id")),
    }
    for row in cursor.fetchall():
        item = dict(row)
        metadata = _metadata(item.get("source_metadata"))
        existing_ids = {
            "activity_id": _normalize_text(metadata.get("activity_id")),
            "source_activity_id": _normalize_text(metadata.get("source_activity_id")),
            "github_activity_id": _normalize_text(metadata.get("github_activity_id")),
        }
        if any(value and value == existing_ids.get(key) for key, value in candidate_ids.items()):
            return item
    return None


def _insert_content_idea(db_or_conn: Any, candidate: GitHubLabelIdeaCandidate) -> int:
    add_idea = getattr(db_or_conn, "add_content_idea", None) or getattr(db_or_conn, "insert_content_idea", None)
    if callable(add_idea):
        return int(
            add_idea(
                note=candidate.note,
                topic=candidate.topic,
                priority=candidate.priority,
                source=SOURCE_NAME,
                source_metadata=candidate.source_metadata,
            )
        )

    conn = _connection(db_or_conn)
    cursor = conn.execute(
        """INSERT INTO content_ideas
           (note, topic, priority, status, source, source_metadata)
           VALUES (?, ?, ?, 'open', ?, ?)""",
        (
            candidate.note,
            candidate.topic,
            candidate.priority,
            SOURCE_NAME,
            json.dumps(candidate.source_metadata, sort_keys=True),
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def _result(
    candidate: GitHubLabelIdeaCandidate,
    status: str,
    idea_id: int | None,
    reason: str,
) -> GitHubLabelIdeaSeedResult:
    return GitHubLabelIdeaSeedResult(
        status=status,
        repo_name=candidate.repo_name,
        activity_type=candidate.activity_type,
        number=candidate.number,
        title=candidate.title,
        matched_labels=candidate.matched_labels,
        source_activity_id=candidate.source_activity_id,
        idea_id=idea_id,
        reason=reason,
        topic=candidate.topic,
        note=candidate.note,
        priority=candidate.priority,
        source_metadata=candidate.source_metadata,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        item = dict(row)
    elif hasattr(row, "keys"):
        item = {key: row[key] for key in row.keys()}
    else:
        item = dict(row)
    item["labels"] = _labels(item.get("labels"))
    item["metadata"] = _metadata(item.get("metadata"))
    return item


def _labels(value: Any) -> list[str]:
    if isinstance(value, list):
        return [_label_name(item) for item in value if _label_name(item)]
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return [value.strip()]
    if isinstance(parsed, list):
        return [_label_name(item) for item in parsed if _label_name(item)]
    return []


def _label_name(value: Any) -> str:
    if isinstance(value, dict):
        value = value.get("name") or value.get("label")
    return _normalize_text(value)


def _metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _normalize_filters(values: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    normalized = []
    seen = set()
    for value in values:
        label = _normalize_label(value)
        if label and label not in seen:
            seen.add(label)
            normalized.append(label)
    return tuple(normalized)


def _topic_for_labels(labels: list[str]) -> str:
    if "security" in labels:
        return "security"
    if "incident" in labels:
        return "reliability"
    if "performance" in labels:
        return "performance"
    if "design" in labels:
        return "design"
    if "bug" in labels:
        return "debugging"
    return "engineering"


def _priority_for_labels(labels: list[str]) -> str:
    if any(label in {"security", "incident"} for label in labels):
        return "high"
    return "normal"


def _label_signal(labels: list[str]) -> int:
    weights = {"security": 5, "incident": 4, "performance": 3, "bug": 2, "design": 1}
    return max((weights.get(_normalize_label(label), 0) for label in labels), default=0)


def _excerpt(value: Any, width: int = 220) -> str:
    text = _normalize_text(value)
    if len(text) > width:
        return text[: width - 3].rstrip() + "..."
    return text


def _shorten(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    return value[: width - 3].rstrip() + "..."


def _normalize_label(value: Any) -> str:
    return _normalize_text(value).lower()


def _normalize_text(value: Any) -> str:
    return _WHITESPACE_RE.sub(" ", str(value or "").strip())


def _reverse_sort_text(value: str) -> tuple[int, ...]:
    return tuple(-ord(char) for char in value)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
