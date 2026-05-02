"""Detect content ideas that should wait on unresolved dependencies."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
import sqlite3
from typing import Any, Iterable, Mapping


DEFAULT_LIMIT = 100
DEFAULT_STATUS = "open"
STATUSES = ("open", "promoted", "dismissed")

_DEPENDENCY_CUE_RE = re.compile(
    r"\b(?P<cue>blocked by|depends on|dependency:|after|once|when|waiting for|wait for)\b"
    r"(?P<ref>[^.\n;]{0,180})",
    re.IGNORECASE,
)
_GITHUB_URL_RE = re.compile(
    r"https?://github\.com/(?P<repo>[^/\s]+/[^/\s]+)/(?P<kind>issues|pull|pulls)/(?P<number>\d+)",
    re.IGNORECASE,
)
_GITHUB_REPO_NUMBER_RE = re.compile(
    r"\b(?P<repo>[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)#(?P<number>\d+)\b"
)
_GITHUB_KIND_NUMBER_RE = re.compile(
    r"\b(?P<kind>issue|issues|pr|prs|pull request|pull requests)\s*#?(?P<number>\d+)\b",
    re.IGNORECASE,
)
_PLANNED_TOPIC_ID_RE = re.compile(
    r"\bplanned[-_\s]+topic\s*#?(?P<id>\d+)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ContentIdeaDependency:
    """One dependency cue found on a content idea."""

    idea_id: int
    topic: str | None
    status: str | None
    priority: str | None
    cue: str
    dependency_type: str
    reference_text: str
    confidence: float
    local_entity_id: int | None
    local_entity_status: str | None
    local_resolved: bool | None
    wait_reason: str
    guidance: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "confidence": self.confidence,
            "cue": self.cue,
            "dependency_type": self.dependency_type,
            "guidance": self.guidance,
            "idea_id": self.idea_id,
            "local_entity_id": self.local_entity_id,
            "local_entity_status": self.local_entity_status,
            "local_resolved": self.local_resolved,
            "priority": self.priority,
            "reference_text": self.reference_text,
            "status": self.status,
            "topic": self.topic,
            "wait_reason": self.wait_reason,
        }


def build_content_idea_dependency_report(
    db_or_conn: Any,
    *,
    status: str | None = DEFAULT_STATUS,
    limit: int | None = DEFAULT_LIMIT,
) -> list[ContentIdeaDependency]:
    """Return dependency cues from content ideas, without modifying the database."""
    if status is not None and status not in STATUSES:
        raise ValueError(f"status must be one of: {', '.join(STATUSES)}")
    if limit is not None and limit < 0:
        raise ValueError("limit must be non-negative")
    if limit == 0:
        return []

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "content_ideas" not in schema:
        return []

    ideas = _fetch_content_ideas(conn, schema["content_ideas"], status=status, limit=limit)
    planned = _load_planned_topics(conn, schema)
    github = _load_github_activity(conn, schema)

    records: list[ContentIdeaDependency] = []
    seen: set[tuple[int, str, str, str]] = set()
    for idea in ideas:
        for candidate in _dependency_candidates(idea):
            record = _resolve_candidate(candidate, idea, planned=planned, github=github)
            identity = (
                f"id:{record.local_entity_id}"
                if record.local_entity_id is not None
                else record.reference_text.casefold()
            )
            key = (record.idea_id, record.dependency_type, identity)
            if key in seen:
                continue
            seen.add(key)
            records.append(record)
    return records


def format_content_idea_dependencies_json(rows: list[ContentIdeaDependency]) -> str:
    """Format dependency rows as deterministic JSON."""
    return json.dumps([row.to_dict() for row in rows], indent=2, sort_keys=True)


def format_content_idea_dependencies_text(rows: list[ContentIdeaDependency]) -> str:
    """Format dependency rows for terminal review."""
    unresolved = sum(1 for row in rows if row.local_resolved is False)
    resolved = sum(1 for row in rows if row.local_resolved is True)
    unknown = sum(1 for row in rows if row.local_resolved is None)
    lines = [
        "Content Idea Dependency Report",
        f"flagged={len(rows)} unresolved={unresolved} resolved={resolved} unknown={unknown}",
        f"{'Idea':>5s}  {'Conf':>4s}  {'Type':14s}  {'Resolved':8s}  Reference",
        f"{'-' * 5:>5s}  {'-' * 4:>4s}  {'-' * 14:14s}  {'-' * 8:8s}  {'-' * 48}",
    ]
    if not rows:
        lines.append("    -     -  -               -         no dependency cues found")
        return "\n".join(lines)

    for row in rows:
        resolved_text = (
            "yes" if row.local_resolved is True else "no" if row.local_resolved is False else "unknown"
        )
        lines.append(
            f"{row.idea_id:5d}  {row.confidence:4.2f}  "
            f"{row.dependency_type[:14]:14s}  {resolved_text[:8]:8s}  "
            f"{_shorten(row.reference_text, 72)}"
        )
        lines.append(f"{'':5s}  {'':4s}  {'':14s}  {'':8s}  {row.guidance}")
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("db_or_conn must be a sqlite3.Connection or expose .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
        ).fetchall()
    }
    return {table: _table_columns(conn, table) for table in tables}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")}


def _fetch_content_ideas(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    status: str | None,
    limit: int | None,
) -> list[dict[str, Any]]:
    selected = [
        _column_expr(columns, "id"),
        _column_expr(columns, "note"),
        _column_expr(columns, "topic"),
        _column_expr(columns, "priority"),
        _column_expr(columns, "status"),
        _column_expr(columns, "source"),
        _column_expr(columns, "source_metadata"),
        _column_expr(columns, "created_at"),
    ]
    filters: list[str] = []
    params: list[Any] = []
    if status is not None and "status" in columns:
        filters.append("status = ?")
        params.append(status)
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    order = "created_at ASC, id ASC" if {"created_at", "id"}.issubset(columns) else "rowid ASC"
    limit_clause = ""
    if limit is not None:
        limit_clause = " LIMIT ?"
        params.append(limit)
    rows = conn.execute(
        f"SELECT {', '.join(selected)} FROM content_ideas {where} ORDER BY {order}{limit_clause}",
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def _load_planned_topics(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[str, dict[Any, dict[str, Any]]]:
    by_id: dict[Any, dict[str, Any]] = {}
    by_topic: dict[Any, dict[str, Any]] = {}
    if "planned_topics" not in schema:
        return {"id": by_id, "topic": by_topic}
    columns = schema["planned_topics"]
    selected = [
        _column_expr(columns, "id"),
        _column_expr(columns, "topic"),
        _column_expr(columns, "angle"),
        _column_expr(columns, "status"),
        _column_expr(columns, "content_id"),
    ]
    for row in conn.execute(f"SELECT {', '.join(selected)} FROM planned_topics ORDER BY id ASC"):
        item = dict(row)
        planned_id = _int_or_none(item.get("id"))
        if planned_id is not None:
            by_id[planned_id] = item
        normalized_topic = _normalize_text(item.get("topic"))
        if normalized_topic:
            by_topic.setdefault(normalized_topic, item)
    return {"id": by_id, "topic": by_topic}


def _load_github_activity(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[str, dict[Any, dict[str, Any]]]:
    by_id: dict[Any, dict[str, Any]] = {}
    by_ref: dict[Any, dict[str, Any]] = {}
    by_number: dict[Any, dict[str, Any] | None] = {}
    if "github_activity" not in schema:
        return {"id": by_id, "ref": by_ref, "number": by_number}
    columns = schema["github_activity"]
    selected = [
        _column_expr(columns, "id"),
        _column_expr(columns, "repo_name"),
        _column_expr(columns, "activity_type"),
        _column_expr(columns, "number"),
        _column_expr(columns, "title"),
        _column_expr(columns, "state"),
        _column_expr(columns, "closed_at"),
        _column_expr(columns, "merged_at"),
        _column_expr(columns, "url"),
    ]
    for row in conn.execute(f"SELECT {', '.join(selected)} FROM github_activity ORDER BY id ASC"):
        item = dict(row)
        activity_id = _int_or_none(item.get("id"))
        if activity_id is not None:
            by_id[activity_id] = item
        repo = _normalize_repo(item.get("repo_name"))
        number = str(item.get("number") or "").strip()
        kind = _github_kind(item.get("activity_type"))
        if repo and number:
            by_ref[(repo, kind, number)] = item
            by_ref[(repo, "any", number)] = item
        if number:
            key = (kind, number)
            by_number[key] = item if key not in by_number else None
            any_key = ("any", number)
            by_number[any_key] = item if any_key not in by_number else None
    return {"id": by_id, "ref": by_ref, "number": by_number}


def _dependency_candidates(idea: Mapping[str, Any]) -> list[dict[str, Any]]:
    text = "\n".join(
        part
        for part in (
            str(idea.get("topic") or "").strip(),
            str(idea.get("note") or "").strip(),
            str(idea.get("source") or "").strip(),
        )
        if part
    )
    metadata = _decode_json_object(idea.get("source_metadata")) or {}
    candidates: list[dict[str, Any]] = []

    for match in _DEPENDENCY_CUE_RE.finditer(text):
        reference = _clean_reference(match.group("ref"))
        if not reference:
            continue
        candidates.append(
            {
                "cue": match.group("cue").lower(),
                "reference_text": reference,
                "dependency_type": _classify_reference(reference),
                "confidence": 0.78,
            }
        )

    for match in _github_matches(text):
        candidates.append({**match, "cue": "github_reference", "confidence": 0.7})
    for match in _planned_topic_matches(text):
        candidates.append({**match, "cue": "planned_topic_reference", "confidence": 0.72})

    for key, value in _walk_metadata(metadata):
        key_text = ".".join(str(part) for part in key)
        if _metadata_key_is_dependency(key_text):
            for item in _metadata_values(value):
                dependency_type = _metadata_dependency_type(key_text, item)
                candidates.append(
                    {
                        "cue": key_text,
                        "reference_text": str(item).strip(),
                        "dependency_type": dependency_type,
                        "confidence": 0.9,
                    }
                )
        elif _metadata_key_is_reference(key_text):
            for item in _metadata_values(value):
                candidates.append(
                    {
                        "cue": key_text,
                        "reference_text": str(item).strip(),
                        "dependency_type": _metadata_dependency_type(key_text, item),
                        "confidence": 0.68,
                    }
                )
    return [candidate for candidate in candidates if candidate.get("reference_text")]


def _resolve_candidate(
    candidate: Mapping[str, Any],
    idea: Mapping[str, Any],
    *,
    planned: dict[str, dict[Any, dict[str, Any]]],
    github: dict[str, dict[Any, dict[str, Any]]],
) -> ContentIdeaDependency:
    dependency_type = str(candidate.get("dependency_type") or "dependency")
    reference_text = str(candidate.get("reference_text") or "").strip()
    entity: dict[str, Any] | None = None
    local_resolved: bool | None = None
    local_entity_id: int | None = None
    local_entity_status: str | None = None

    if dependency_type == "planned_topic":
        entity = _resolve_planned_topic(reference_text, planned)
        if entity is not None:
            local_entity_id = _int_or_none(entity.get("id"))
            local_entity_status = str(entity.get("status") or "")
            local_resolved = _planned_topic_resolved(entity)
    elif dependency_type == "github_activity":
        entity = _resolve_github_activity(reference_text, github)
        if entity is not None:
            local_entity_id = _int_or_none(entity.get("id"))
            local_entity_status = str(entity.get("state") or "")
            local_resolved = _github_activity_resolved(entity)
    else:
        entity = _resolve_planned_topic(reference_text, planned)
        if entity is not None:
            dependency_type = "planned_topic"
            local_entity_id = _int_or_none(entity.get("id"))
            local_entity_status = str(entity.get("status") or "")
            local_resolved = _planned_topic_resolved(entity)

    wait_reason = _wait_reason(dependency_type, reference_text, local_resolved, local_entity_status)
    return ContentIdeaDependency(
        idea_id=int(idea.get("id") or 0),
        topic=_none_if_blank(idea.get("topic")),
        status=_none_if_blank(idea.get("status")),
        priority=_none_if_blank(idea.get("priority")),
        cue=str(candidate.get("cue") or "dependency"),
        dependency_type=dependency_type,
        reference_text=reference_text,
        confidence=round(float(candidate.get("confidence") or 0.5), 2),
        local_entity_id=local_entity_id,
        local_entity_status=local_entity_status or None,
        local_resolved=local_resolved,
        wait_reason=wait_reason,
        guidance=_guidance(local_resolved, wait_reason),
    )


def _github_matches(text: str) -> Iterable[dict[str, Any]]:
    for match in _GITHUB_URL_RE.finditer(text):
        kind = "pr" if match.group("kind").startswith("pull") else "issue"
        yield {
            "dependency_type": "github_activity",
            "reference_text": f"{match.group('repo')}#{match.group('number')}",
            "github_repo": match.group("repo"),
            "github_kind": kind,
            "github_number": match.group("number"),
        }
    for match in _GITHUB_REPO_NUMBER_RE.finditer(text):
        yield {
            "dependency_type": "github_activity",
            "reference_text": match.group(0),
            "github_repo": match.group("repo"),
            "github_kind": "any",
            "github_number": match.group("number"),
        }
    for match in _GITHUB_KIND_NUMBER_RE.finditer(text):
        yield {
            "dependency_type": "github_activity",
            "reference_text": match.group(0),
            "github_kind": _github_kind(match.group("kind")),
            "github_number": match.group("number"),
        }


def _planned_topic_matches(text: str) -> Iterable[dict[str, Any]]:
    for match in _PLANNED_TOPIC_ID_RE.finditer(text):
        yield {
            "dependency_type": "planned_topic",
            "reference_text": f"planned_topic:{match.group('id')}",
            "planned_topic_id": match.group("id"),
        }


def _resolve_planned_topic(
    reference_text: str,
    planned: dict[str, dict[Any, dict[str, Any]]],
) -> dict[str, Any] | None:
    planned_id = _int_or_none(reference_text.removeprefix("planned_topic:"))
    if planned_id is None:
        match = _PLANNED_TOPIC_ID_RE.search(reference_text)
        planned_id = _int_or_none(match.group("id")) if match else None
    if planned_id is None:
        planned_id = _int_or_none(reference_text)
    if planned_id is not None and planned_id in planned["id"]:
        return planned["id"][planned_id]
    normalized = _normalize_text(reference_text)
    return planned["topic"].get(normalized)


def _resolve_github_activity(
    reference_text: str,
    github: dict[str, dict[Any, dict[str, Any]]],
) -> dict[str, Any] | None:
    activity_id = _int_or_none(reference_text.removeprefix("github_activity:"))
    if activity_id is not None and activity_id in github["id"]:
        return github["id"][activity_id]

    for match in _github_matches(reference_text):
        repo = _normalize_repo(match.get("github_repo"))
        kind = _github_kind(match.get("github_kind"))
        number = str(match.get("github_number") or "")
        if repo and (entity := github["ref"].get((repo, kind, number))):
            return entity
        if repo and (entity := github["ref"].get((repo, "any", number))):
            return entity
        entity = github["number"].get((kind, number)) or github["number"].get(("any", number))
        if entity:
            return entity

    match = re.search(r"#?(?P<number>\d+)\b", reference_text)
    if match:
        return github["number"].get(("any", match.group("number")))
    return None


def _planned_topic_resolved(row: Mapping[str, Any]) -> bool:
    return str(row.get("status") or "").casefold() == "generated" or _int_or_none(row.get("content_id")) is not None


def _github_activity_resolved(row: Mapping[str, Any]) -> bool:
    state = str(row.get("state") or "").casefold()
    return state in {"closed", "merged", "fixed", "resolved"} or bool(row.get("closed_at") or row.get("merged_at"))


def _wait_reason(
    dependency_type: str,
    reference_text: str,
    local_resolved: bool | None,
    status: str | None,
) -> str:
    if local_resolved is True:
        return f"{reference_text} appears resolved locally"
    if local_resolved is False:
        detail = f" ({status})" if status else ""
        return f"{reference_text} is still unresolved locally{detail}"
    if dependency_type in {"github_activity", "planned_topic"}:
        return f"{reference_text} could not be resolved locally"
    return f"dependency cue references {reference_text}"


def _guidance(local_resolved: bool | None, wait_reason: str) -> str:
    if local_resolved is True:
        return f"Promote is probably safe after confirming freshness: {wait_reason}."
    return f"Wait before promotion: {wait_reason}."


def _classify_reference(reference: str) -> str:
    if _GITHUB_URL_RE.search(reference) or _GITHUB_REPO_NUMBER_RE.search(reference) or _GITHUB_KIND_NUMBER_RE.search(reference):
        return "github_activity"
    if _PLANNED_TOPIC_ID_RE.search(reference) or reference.lower().startswith("planned topic"):
        return "planned_topic"
    return "dependency"


def _metadata_dependency_type(key: str, value: Any) -> str:
    key_lower = key.casefold()
    value_text = str(value or "")
    if "github" in key_lower or _classify_reference(value_text) == "github_activity":
        return "github_activity"
    if "planned_topic" in key_lower or "planned topic" in key_lower:
        return "planned_topic"
    return _classify_reference(value_text)


def _metadata_key_is_dependency(key: str) -> bool:
    key_lower = key.casefold()
    return any(
        token in key_lower
        for token in (
            "blocked_by",
            "blocked by",
            "depends_on",
            "depends on",
            "dependency",
            "dependencies",
            "prerequisite",
            "wait_for",
            "waiting_for",
        )
    )


def _metadata_key_is_reference(key: str) -> bool:
    key_lower = key.casefold()
    return any(
        token in key_lower
        for token in (
            "github_activity_id",
            "github_issue",
            "github_pr",
            "planned_topic_id",
            "planned_topic_ids",
        )
    )


def _walk_metadata(value: Any, prefix: tuple[Any, ...] = ()) -> Iterable[tuple[tuple[Any, ...], Any]]:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            path = (*prefix, key)
            yield path, nested
            yield from _walk_metadata(nested, path)
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            yield from _walk_metadata(nested, (*prefix, index))


def _metadata_values(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list | tuple | set):
        return [item for item in value if item is not None and str(item).strip()]
    if isinstance(value, Mapping):
        for key in ("id", "number", "url", "ref", "reference", "topic"):
            if value.get(key):
                return [value[key]]
        return [json.dumps(value, sort_keys=True)]
    text = str(value).strip()
    return [text] if text else []


def _decode_json_object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return None
    try:
        decoded = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return None
    return decoded if isinstance(decoded, dict) else None


def _column_expr(columns: set[str], column: str, fallback: str = "NULL") -> str:
    if column in columns:
        return _quote_identifier(column)
    return f"{fallback} AS {_quote_identifier(column)}"


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _github_kind(value: Any) -> str:
    text = str(value or "").casefold().replace("-", "_").replace(" ", "_")
    if text in {"pr", "prs", "pull", "pulls", "pull_request", "pull_requests"}:
        return "pr"
    if text in {"issue", "issues"}:
        return "issue"
    return text or "any"


def _normalize_repo(value: Any) -> str:
    return str(value or "").strip().rstrip("/").casefold()


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").casefold()).strip()


def _none_if_blank(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _clean_reference(value: str) -> str:
    return value.strip(" \t:-,")


def _shorten(value: str, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return f"{text[: max(0, limit - 1)].rstrip()}..."
