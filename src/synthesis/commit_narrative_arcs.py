"""Plan narrative arcs from recent commits and Claude session evidence."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from pathlib import Path
from typing import Any


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_MIN_ITEMS_PER_ARC = 2

_STOP_WORDS = {
    "add",
    "added",
    "and",
    "build",
    "change",
    "changes",
    "claude",
    "code",
    "commit",
    "fix",
    "for",
    "from",
    "implement",
    "into",
    "make",
    "planner",
    "refactor",
    "session",
    "support",
    "test",
    "tests",
    "the",
    "this",
    "update",
    "with",
}


@dataclass(frozen=True)
class NarrativeArc:
    """One read-only publishable storyline candidate."""

    title: str
    primary_repo: str
    time_window: dict[str, str | None]
    source_ids: tuple[str, ...]
    evidence_snippets: tuple[str, ...]
    suggested_formats: tuple[str, ...]
    confidence: float

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["source_ids"] = list(self.source_ids)
        data["evidence_snippets"] = list(self.evidence_snippets)
        data["suggested_formats"] = list(self.suggested_formats)
        return data


@dataclass(frozen=True)
class NarrativeArcPlan:
    """Read-only report of commit-to-narrative arc candidates."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    arcs: tuple[NarrativeArc, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "totals": dict(self.totals),
            "arcs": [arc.to_dict() for arc in self.arcs],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
        }


def build_commit_narrative_arcs(
    db_or_conn: Any,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_items_per_arc: int = DEFAULT_MIN_ITEMS_PER_ARC,
    now: datetime | None = None,
) -> NarrativeArcPlan:
    """Return deterministic narrative arcs without mutating the database."""
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if min_items_per_arc <= 1:
        raise ValueError("min_items_per_arc must be greater than 1")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    missing_tables = tuple(
        table
        for table in ("github_commits", "claude_messages")
        if table not in schema
    )
    missing_columns = _missing_required_columns(schema)

    commits = _load_commits(conn, schema, cutoff)
    sessions = _load_sessions(conn, schema, cutoff)
    events = commits + sessions
    if not events:
        return _plan(
            generated_at,
            lookback_days,
            min_items_per_arc,
            cutoff,
            (),
            missing_tables,
            missing_columns,
            commits,
            sessions,
            excluded=0,
        )

    parent = {event["source_id"]: event["source_id"] for event in events}
    event_by_id = {event["source_id"]: event for event in events}
    cluster_keys: dict[str, set[str]] = defaultdict(set)
    for event in sorted(events, key=_event_sort_key):
        repo = event["repo_key"] or "unknown"
        day = event["timestamp"].date().isoformat()
        cluster_keys[f"repo-day:{repo}:{day}"].add(event["source_id"])
        for keyword in event["keywords"][:4]:
            cluster_keys[f"repo-keyword:{repo}:{keyword}"].add(event["source_id"])

    for commit_id, session_id in _load_prompt_links(conn, schema, event_by_id):
        cluster_keys[f"link:{commit_id}:{session_id}"].update((commit_id, session_id))

    for ids in cluster_keys.values():
        ordered = sorted(ids)
        for source_id in ordered[1:]:
            _union(parent, ordered[0], source_id)

    components: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in sorted(events, key=_event_sort_key):
        components[_find(parent, event["source_id"])].append(event)

    arcs = []
    excluded = 0
    for component in components.values():
        if len(component) < min_items_per_arc:
            excluded += 1
            continue
        arcs.append(_build_arc(component))

    ranked = tuple(
        sorted(
            arcs,
            key=lambda arc: (
                -arc.confidence,
                arc.primary_repo,
                arc.time_window["start"] or "",
                arc.title,
                arc.source_ids,
            ),
        )
    )
    return _plan(
        generated_at,
        lookback_days,
        min_items_per_arc,
        cutoff,
        ranked,
        missing_tables,
        missing_columns,
        commits,
        sessions,
        excluded=excluded,
    )


def format_commit_narrative_arcs_json(plan: NarrativeArcPlan, *, limit: int | None = None) -> str:
    """Serialize a narrative arc plan as deterministic JSON."""
    data = plan.to_dict()
    if limit is not None:
        data["arcs"] = data["arcs"][: max(limit, 0)]
    return json.dumps(data, indent=2, sort_keys=True)


def format_commit_narrative_arcs_text(plan: NarrativeArcPlan, *, limit: int | None = None) -> str:
    """Format narrative arcs for terminal review."""
    arcs = plan.arcs[: max(limit, 0)] if limit is not None else plan.arcs
    lines = [
        "Commit Narrative Arc Planner",
        f"Generated: {plan.generated_at}",
        (
            f"Activity since: {plan.filters['activity_since']} "
            f"(days={plan.filters['lookback_days']}, "
            f"min_items={plan.filters['min_items_per_arc']})"
        ),
        (
            "Summary: "
            f"arcs={plan.totals['arc_count']} "
            f"commits={plan.totals['commit_count']} "
            f"sessions={plan.totals['session_count']} "
            f"weak={plan.totals['excluded_weak_arcs']}"
        ),
    ]
    if plan.missing_tables:
        lines.append("Missing tables: " + ", ".join(plan.missing_tables))
    if plan.missing_columns:
        columns = [
            f"{table}({', '.join(cols)})"
            for table, cols in sorted(plan.missing_columns.items())
        ]
        lines.append("Missing columns: " + "; ".join(columns))
    if not arcs:
        lines.append("No narrative arcs found.")
        return "\n".join(lines)

    lines.append("Arcs:")
    for arc in arcs:
        lines.append(
            f"- {arc.title} "
            f"repo={arc.primary_repo} confidence={arc.confidence:.2f}"
        )
        lines.append(
            f"  window: {arc.time_window['start']} to {arc.time_window['end']}"
        )
        lines.append("  sources: " + ", ".join(arc.source_ids))
        lines.append("  formats: " + ", ".join(arc.suggested_formats))
        for snippet in arc.evidence_snippets[:3]:
            lines.append(f"  evidence: {snippet}")
    return "\n".join(lines)


def _plan(
    generated_at: datetime,
    lookback_days: int,
    min_items_per_arc: int,
    cutoff: datetime,
    arcs: tuple[NarrativeArc, ...],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
    commits: list[dict[str, Any]],
    sessions: list[dict[str, Any]],
    *,
    excluded: int,
) -> NarrativeArcPlan:
    return NarrativeArcPlan(
        generated_at=generated_at.isoformat(),
        filters={
            "lookback_days": lookback_days,
            "min_items_per_arc": min_items_per_arc,
            "activity_since": cutoff.isoformat(),
        },
        totals={
            "commit_count": len(commits),
            "session_count": len(sessions),
            "source_item_count": len(commits) + len(sessions),
            "arc_count": len(arcs),
            "excluded_weak_arcs": excluded,
            "missing_tables": len(missing_tables),
            "missing_column_tables": len(missing_columns),
        },
        arcs=arcs,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _build_arc(events: list[dict[str, Any]]) -> NarrativeArc:
    ordered = sorted(events, key=_event_sort_key)
    repos = Counter(event["repo_label"] for event in ordered if event["repo_label"])
    primary_repo = repos.most_common(1)[0][0] if repos else "unknown"
    keywords = Counter(
        keyword
        for event in ordered
        for keyword in event["keywords"][:4]
    )
    common_keywords = [word for word, _count in keywords.most_common(3)]
    title_terms = [term.title() for term in common_keywords[:2]]
    title = (
        f"{' '.join(title_terms)} Narrative Arc"
        if title_terms
        else f"{primary_repo.title()} Narrative Arc"
    )
    source_ids = tuple(event["source_id"] for event in ordered)
    evidence = tuple(_snippet(event) for event in ordered[:5])
    confidence = _confidence(ordered, keywords)
    return NarrativeArc(
        title=title,
        primary_repo=primary_repo,
        time_window={
            "start": ordered[0]["timestamp"].isoformat(),
            "end": ordered[-1]["timestamp"].isoformat(),
        },
        source_ids=source_ids,
        evidence_snippets=evidence,
        suggested_formats=_suggested_formats(ordered, confidence),
        confidence=confidence,
    )


def _confidence(events: list[dict[str, Any]], keywords: Counter[str]) -> float:
    source_types = {event["source_type"] for event in events}
    repos = {event["repo_key"] for event in events if event["repo_key"]}
    score = 0.35
    score += min(len(events), 6) * 0.05
    if len(source_types) > 1:
        score += 0.18
    if any(event.get("linked") for event in events):
        score += 0.17
    if len(repos) <= 1:
        score += 0.1
    if keywords and keywords.most_common(1)[0][1] >= 2:
        score += 0.1
    return round(min(score, 0.95), 2)


def _suggested_formats(events: list[dict[str, Any]], confidence: float) -> tuple[str, ...]:
    source_types = {event["source_type"] for event in events}
    if confidence >= 0.8 or len(events) >= 4:
        return ("newsletter_section", "x_thread")
    if len(source_types) > 1:
        return ("x_thread", "newsletter_section")
    return ("x_thread",)


def _load_commits(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
) -> list[dict[str, Any]]:
    required = {"id", "repo_name", "commit_sha", "commit_message", "timestamp"}
    if "github_commits" not in schema or not required.issubset(schema["github_commits"]):
        return []
    rows = conn.execute(
        """SELECT id, repo_name, commit_sha, commit_message, timestamp
           FROM github_commits
           WHERE timestamp >= ?
           ORDER BY timestamp ASC, id ASC""",
        (cutoff.isoformat(),),
    ).fetchall()
    events = []
    for row in rows:
        item = _row_dict(row)
        timestamp = _parse_time(item.get("timestamp"))
        if timestamp is None:
            continue
        repo = str(item.get("repo_name") or "").strip()
        text = str(item.get("commit_message") or "")
        events.append(
            {
                "db_id": int(item["id"]),
                "source_type": "github_commit",
                "source_id": f"commit:{item.get('commit_sha')}",
                "repo_label": repo or "unknown",
                "repo_key": _repo_key(repo),
                "timestamp": timestamp,
                "text": text,
                "keywords": _keywords(text),
                "linked": False,
            }
        )
    return events


def _load_sessions(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
) -> list[dict[str, Any]]:
    required = {"id", "session_id", "message_uuid", "timestamp", "prompt_text"}
    if "claude_messages" not in schema or not required.issubset(schema["claude_messages"]):
        return []
    project_expr = "project_path" if "project_path" in schema["claude_messages"] else "NULL AS project_path"
    rows = conn.execute(
        f"""SELECT id, session_id, message_uuid, {project_expr}, timestamp, prompt_text
            FROM claude_messages
            WHERE timestamp >= ?
            ORDER BY timestamp ASC, id ASC""",
        (cutoff.isoformat(),),
    ).fetchall()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        item = _row_dict(row)
        if _parse_time(item.get("timestamp")) is not None:
            grouped[str(item.get("session_id") or "unknown")].append(item)

    events = []
    for session_id, messages in sorted(grouped.items()):
        ordered = sorted(messages, key=lambda item: (_parse_time(item["timestamp"]), item["id"]))
        timestamps = [_parse_time(item["timestamp"]) for item in ordered]
        text = " ".join(str(item.get("prompt_text") or "") for item in ordered[:4])
        project = str(ordered[0].get("project_path") or "").strip()
        repo_label = Path(project).name if project else "unknown"
        events.append(
            {
                "db_ids": tuple(int(item["id"]) for item in ordered),
                "message_uuids": tuple(str(item["message_uuid"]) for item in ordered),
                "source_type": "claude_session",
                "source_id": f"session:{session_id}",
                "repo_label": repo_label,
                "repo_key": _repo_key(repo_label),
                "timestamp": timestamps[0],
                "end_timestamp": timestamps[-1],
                "text": text,
                "keywords": _keywords(text),
                "linked": False,
            }
        )
    return events


def _load_prompt_links(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    event_by_id: dict[str, dict[str, Any]],
) -> list[tuple[str, str]]:
    required = {
        "commit_prompt_links": {"commit_id", "message_id"},
        "github_commits": {"id", "commit_sha"},
        "claude_messages": {"id", "session_id"},
    }
    if any(table not in schema or not cols.issubset(schema[table]) for table, cols in required.items()):
        return []
    rows = conn.execute(
        """SELECT gc.commit_sha, cm.session_id
           FROM commit_prompt_links cpl
           JOIN github_commits gc ON gc.id = cpl.commit_id
           JOIN claude_messages cm ON cm.id = cpl.message_id
           ORDER BY gc.timestamp ASC, gc.id ASC, cm.timestamp ASC, cm.id ASC"""
    ).fetchall()
    links = []
    for row in rows:
        item = _row_dict(row)
        commit_id = f"commit:{item.get('commit_sha')}"
        session_id = f"session:{item.get('session_id')}"
        if commit_id in event_by_id and session_id in event_by_id:
            event_by_id[commit_id]["linked"] = True
            event_by_id[session_id]["linked"] = True
            links.append((commit_id, session_id))
    return links


def _missing_required_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    required = {
        "github_commits": ("id", "repo_name", "commit_sha", "commit_message", "timestamp"),
        "claude_messages": ("id", "session_id", "message_uuid", "timestamp", "prompt_text"),
    }
    missing = {}
    for table, columns in required.items():
        if table in schema:
            absent = tuple(column for column in columns if column not in schema[table])
            if absent:
                missing[table] = absent
    return missing


def _keywords(text: str) -> tuple[str, ...]:
    words = re.findall(r"[a-z][a-z0-9_-]{2,}", text.lower())
    prefixes = {"chore", "docs", "feat", "fix", "perf", "style"}
    counts = Counter(
        word
        for word in words
        if word not in _STOP_WORDS
        and word not in prefixes
        and not word.startswith("http")
    )
    cleaned = Counter({word.strip("-_:"): count for word, count in counts.items() if word.strip("-_:")})
    return tuple(word for word, _count in sorted(cleaned.items(), key=lambda item: (-item[1], item[0]))[:8])


def _snippet(event: dict[str, Any]) -> str:
    text = re.sub(r"\s+", " ", str(event.get("text") or "").strip())
    if len(text) > 140:
        text = text[:137].rstrip() + "..."
    return f"{event['source_id']} {text}"


def _repo_key(repo: str) -> str:
    value = str(repo or "").strip().lower()
    if "/" in value:
        return value.rsplit("/", 1)[-1]
    return value


def _event_sort_key(event: dict[str, Any]) -> tuple[Any, ...]:
    return (event["timestamp"], event["source_type"], event["source_id"])


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
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _row_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "keys"):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def _value(row: Any, key: str, index: int) -> Any:
    if hasattr(row, "keys") and key in row.keys():
        return row[key]
    return row[index]


def _parse_time(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _find(parent: dict[str, str], source_id: str) -> str:
    while parent[source_id] != source_id:
        parent[source_id] = parent[parent[source_id]]
        source_id = parent[source_id]
    return source_id


def _union(parent: dict[str, str], left: str, right: str) -> None:
    left_root = _find(parent, left)
    right_root = _find(parent, right)
    if left_root == right_root:
        return
    first, second = sorted((left_root, right_root))
    parent[second] = first
