"""Deterministically rank synthesis source bundles before generation."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 10

SOURCE_COMMIT = "commit"
SOURCE_MESSAGE = "message"
SOURCE_GITHUB_ACTIVITY = "github_activity"

_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_-]*")
_DETAIL_RE = re.compile(r"\b(?:[a-f0-9]{7,40}|[A-Z][A-Z0-9_]{2,}|v?\d+(?:\.\d+)+|#\d+|https?://\S+)\b")
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "into",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "this",
    "to",
    "with",
}


@dataclass(frozen=True)
class NormalizedSource:
    """A source row normalized for bundle scoring."""

    source_type: str
    source_id: str
    title: str
    text: str
    timestamp: str | None
    repo_name: str | None
    url: str | None
    tokens: tuple[str, ...]
    row: dict[str, Any]


@dataclass(frozen=True)
class SourceBundle:
    """A ranked set of related synthesis sources."""

    bundle_id: str
    score: int
    source_ids: dict[str, list[str]]
    title: str
    source_count: int
    source_types: list[str]
    rationale: list[str]
    freshness_signals: dict[str, Any]
    evidence_density_signals: dict[str, Any]
    dedup_penalties: list[dict[str, Any]]
    sources: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def rank_source_bundles(
    *,
    commits: list[dict[str, Any]] | None = None,
    messages: list[dict[str, Any]] | None = None,
    github_activity: list[dict[str, Any]] | None = None,
    now: datetime | None = None,
    limit: int | None = DEFAULT_LIMIT,
) -> list[dict[str, Any]]:
    """Return ranked source bundles from commit, Claude message, and GitHub activity rows."""
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    reference_time = _as_utc(now or datetime.now(timezone.utc))
    sources = _normalize_sources(commits or [], messages or [], github_activity or [])
    groups = _group_sources(sources)
    bundles = [_score_group(group, reference_time) for group in groups]
    bundles.sort(
        key=lambda bundle: (
            -bundle.score,
            -bundle.source_count,
            bundle.freshness_signals.get("newest_at") or "",
            bundle.bundle_id,
        )
    )
    items = [bundle.to_dict() for bundle in bundles]
    return items[:limit] if limit is not None else items


def build_source_bundle_rank_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Read recent storage rows and return ranked source bundles."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    reference_time = _as_utc(now or datetime.now(timezone.utc))
    start = reference_time - timedelta(days=days)
    commits, messages, github_activity = load_recent_sources(db_or_conn, start=start, end=reference_time)
    bundles = rank_source_bundles(
        commits=commits,
        messages=messages,
        github_activity=github_activity,
        now=reference_time,
        limit=limit,
    )
    return {
        "artifact_type": "source_bundle_rank",
        "generated_at": reference_time.isoformat(),
        "filters": {"days": days, "limit": limit},
        "counts": {
            "commits": len(commits),
            "messages": len(messages),
            "github_activity": len(github_activity),
            "bundles": len(bundles),
        },
        "bundles": bundles,
    }


def load_recent_sources(
    db_or_conn: Any,
    *,
    start: datetime,
    end: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Load recent commit, Claude message, and GitHub activity rows without mutating state."""
    db = db_or_conn
    if hasattr(db, "get_commits_in_range") and hasattr(db, "get_messages_in_range"):
        commits = db.get_commits_in_range(start, end)
        messages = db.get_messages_in_range(start, end)
        if hasattr(db, "get_github_activity_in_range"):
            activity = db.get_github_activity_in_range(start, end)
        else:
            activity = _load_github_activity_from_conn(db.conn, start, end)
        return commits, messages, activity

    conn = _connection(db_or_conn)
    return (
        _load_rows(conn, "github_commits", "timestamp", start, end),
        _load_rows(conn, "claude_messages", "timestamp", start, end),
        _load_github_activity_from_conn(conn, start, end),
    )


def format_source_bundle_rank_json(report: dict[str, Any]) -> str:
    """Format a source bundle rank report as JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_source_bundle_rank_text(report: dict[str, Any]) -> str:
    """Format a source bundle rank report for operator review."""
    if not report["bundles"]:
        return "No source bundles matched the filters."

    lines = [
        "Source Bundle Rank",
        (
            f"Counts: commits={report['counts']['commits']} "
            f"messages={report['counts']['messages']} "
            f"github_activity={report['counts']['github_activity']} "
            f"bundles={report['counts']['bundles']}"
        ),
        "",
        "Bundles:",
    ]
    for index, bundle in enumerate(report["bundles"], start=1):
        freshness = bundle["freshness_signals"]
        density = bundle["evidence_density_signals"]
        lines.append(
            f"  {index}. score={bundle['score']} sources={bundle['source_count']} "
            f"types={','.join(bundle['source_types'])} newest={freshness.get('newest_at') or '-'} "
            f"title={bundle['title']}"
        )
        lines.append(
            f"     density: tokens={density['unique_token_count']} "
            f"specific_terms={density['specific_term_count']} avg_words={density['average_word_count']}"
        )
        lines.append(f"     ids: {_format_source_ids(bundle['source_ids'])}")
        lines.append(f"     rationale: {', '.join(bundle['rationale'])}")
        if bundle["dedup_penalties"]:
            reasons = ", ".join(item["reason"] for item in bundle["dedup_penalties"])
            lines.append(f"     dedup: {reasons}")
    return "\n".join(lines)


def _normalize_sources(
    commits: list[dict[str, Any]],
    messages: list[dict[str, Any]],
    github_activity: list[dict[str, Any]],
) -> list[NormalizedSource]:
    sources: list[NormalizedSource] = []
    for row in commits:
        item = dict(row)
        text = _clean(item.get("commit_message") or item.get("message"))
        source_id = _clean(item.get("commit_sha") or item.get("sha") or item.get("id"))
        sources.append(
            _normalized_source(
                SOURCE_COMMIT,
                source_id or f"commit:{len(sources) + 1}",
                text,
                text,
                item.get("timestamp"),
                item.get("repo_name"),
                item.get("url"),
                item,
            )
        )
    for row in messages:
        item = dict(row)
        text = _clean(item.get("prompt_text") or item.get("message") or item.get("content"))
        source_id = _clean(item.get("message_uuid") or item.get("uuid") or item.get("id"))
        title = _first_line(text)
        sources.append(
            _normalized_source(
                SOURCE_MESSAGE,
                source_id or f"message:{len(sources) + 1}",
                title,
                text,
                item.get("timestamp"),
                item.get("project_path"),
                item.get("url"),
                item,
            )
        )
    for row in github_activity:
        item = dict(row)
        title = _clean(item.get("title"))
        body = _clean(item.get("body"))
        source_id = _clean(item.get("activity_id")) or _activity_id(item)
        sources.append(
            _normalized_source(
                SOURCE_GITHUB_ACTIVITY,
                source_id or f"github_activity:{len(sources) + 1}",
                title or _first_line(body),
                f"{title}\n{body}".strip(),
                item.get("updated_at") or item.get("timestamp"),
                item.get("repo_name"),
                item.get("url"),
                item,
            )
        )
    return [source for source in sources if source.text or source.title]


def _normalized_source(
    source_type: str,
    source_id: str,
    title: str,
    text: str,
    timestamp: Any,
    repo_name: Any,
    url: Any,
    row: dict[str, Any],
) -> NormalizedSource:
    tokens = tuple(_tokens(f"{title} {text}"))
    return NormalizedSource(
        source_type=source_type,
        source_id=str(source_id),
        title=_clean(title) or _first_line(text) or str(source_id),
        text=_clean(text),
        timestamp=_timestamp_text(timestamp),
        repo_name=_clean(repo_name) or None,
        url=_clean(url) or None,
        tokens=tokens,
        row=row,
    )


def _group_sources(sources: list[NormalizedSource]) -> list[list[NormalizedSource]]:
    groups: list[list[NormalizedSource]] = []
    for source in sorted(sources, key=lambda item: (item.timestamp or "", item.source_type, item.source_id), reverse=True):
        best_index: int | None = None
        best_similarity = 0.0
        for index, group in enumerate(groups):
            similarity = max(_similarity(source, other) for other in group)
            if similarity > best_similarity:
                best_similarity = similarity
                best_index = index
        if best_index is not None and best_similarity >= 0.15:
            groups[best_index].append(source)
        else:
            groups.append([source])
    return groups


def _score_group(group: list[NormalizedSource], now: datetime) -> SourceBundle:
    ordered = sorted(group, key=lambda item: (item.timestamp or "", item.source_type, item.source_id), reverse=True)
    newest = max((_parse_datetime(item.timestamp) for item in ordered if item.timestamp), default=None)
    oldest = min((_parse_datetime(item.timestamp) for item in ordered if item.timestamp), default=None)
    age_hours = ((now - newest).total_seconds() / 3600) if newest else None
    tokens = sorted({token for source in ordered for token in source.tokens})
    source_types = sorted({source.source_type for source in ordered})
    word_counts = [_word_count(source.text) for source in ordered]
    specific_terms = sorted({term.lower() for source in ordered for term in _DETAIL_RE.findall(source.text)})
    dedup_penalties = _dedup_penalties(ordered)

    freshness_score = _freshness_score(age_hours)
    density_score = min(34, len(tokens) * 2 + len(specific_terms) * 4 + sum(1 for count in word_counts if count >= 8) * 4)
    source_score = min(26, len(ordered) * 6 + len(source_types) * 5)
    score = freshness_score + density_score + source_score - sum(int(item["penalty"]) for item in dedup_penalties)
    score = max(0, min(100, score))

    rationale = _rationale(
        source_count=len(ordered),
        source_types=source_types,
        age_hours=age_hours,
        density_score=density_score,
        dedup_penalties=dedup_penalties,
    )
    source_ids = {
        SOURCE_COMMIT: [item.source_id for item in ordered if item.source_type == SOURCE_COMMIT],
        SOURCE_MESSAGE: [item.source_id for item in ordered if item.source_type == SOURCE_MESSAGE],
        SOURCE_GITHUB_ACTIVITY: [item.source_id for item in ordered if item.source_type == SOURCE_GITHUB_ACTIVITY],
    }
    source_ids = {key: value for key, value in source_ids.items() if value}
    return SourceBundle(
        bundle_id=_bundle_id(ordered),
        score=score,
        source_ids=source_ids,
        title=_bundle_title(ordered),
        source_count=len(ordered),
        source_types=source_types,
        rationale=rationale,
        freshness_signals={
            "newest_at": newest.isoformat() if newest else None,
            "oldest_at": oldest.isoformat() if oldest else None,
            "age_hours": round(age_hours, 1) if age_hours is not None else None,
            "freshness_score": freshness_score,
        },
        evidence_density_signals={
            "unique_token_count": len(tokens),
            "specific_term_count": len(specific_terms),
            "specific_terms": specific_terms[:10],
            "average_word_count": round(sum(word_counts) / len(word_counts), 1) if word_counts else 0,
            "density_score": density_score,
        },
        dedup_penalties=dedup_penalties,
        sources=[_source_payload(item) for item in ordered],
    )


def _dedup_penalties(group: list[NormalizedSource]) -> list[dict[str, Any]]:
    penalties: list[dict[str, Any]] = []
    for index, left in enumerate(group):
        for right in group[index + 1 :]:
            similarity = _similarity(left, right)
            normalized_left = " ".join(left.tokens)
            normalized_right = " ".join(right.tokens)
            if similarity >= 0.82 or (normalized_left and normalized_left == normalized_right):
                penalties.append(
                    {
                        "source_ids": [left.source_id, right.source_id],
                        "penalty": 10,
                        "similarity": round(similarity, 2),
                        "reason": "near-duplicate source text grouped and penalized",
                    }
                )
    return penalties


def _rationale(
    *,
    source_count: int,
    source_types: list[str],
    age_hours: float | None,
    density_score: int,
    dedup_penalties: list[dict[str, Any]],
) -> list[str]:
    reasons: list[str] = []
    if age_hours is None:
        reasons.append("missing timestamp limits freshness confidence")
    elif age_hours <= 48:
        reasons.append("fresh source activity")
    elif age_hours >= 24 * 14:
        reasons.append("stale source activity")
    if len(source_types) >= 2:
        reasons.append("multi-source corroboration")
    if source_count >= 3:
        reasons.append("multiple related sources")
    if density_score >= 24:
        reasons.append("specific evidence-dense text")
    elif density_score <= 10:
        reasons.append("low-detail evidence")
    if dedup_penalties:
        reasons.append("near-duplicate evidence reduced score")
    return reasons or ["ranked by freshness, source diversity, and evidence density"]


def _freshness_score(age_hours: float | None) -> int:
    if age_hours is None:
        return 12
    if age_hours <= 24:
        return 40
    if age_hours <= 72:
        return 34
    if age_hours <= 24 * 7:
        return 26
    if age_hours <= 24 * 14:
        return 18
    if age_hours <= 24 * 30:
        return 10
    return 4


def _similarity(left: NormalizedSource, right: NormalizedSource) -> float:
    left_tokens = set(left.tokens)
    right_tokens = set(right.tokens)
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens)
    if overlap < 2:
        return 0.0
    union = len(left_tokens | right_tokens)
    return overlap / union if union else 0.0


def _tokens(text: str) -> list[str]:
    tokens = []
    for token in _TOKEN_RE.findall(text.lower()):
        if token in _STOPWORDS or len(token) <= 1:
            continue
        tokens.append(_normalize_token(token))
    return tokens


def _normalize_token(token: str) -> str:
    if len(token) > 4 and token.endswith("ies"):
        return f"{token[:-3]}y"
    if len(token) > 4 and token.endswith("s"):
        return token[:-1]
    return token


def _bundle_id(group: list[NormalizedSource]) -> str:
    return "|".join(f"{item.source_type}:{item.source_id}" for item in group)


def _bundle_title(group: list[NormalizedSource]) -> str:
    return max(group, key=lambda item: (_word_count(item.title), item.timestamp or "", item.source_id)).title


def _source_payload(source: NormalizedSource) -> dict[str, Any]:
    return {
        "source_type": source.source_type,
        "source_id": source.source_id,
        "title": source.title,
        "timestamp": source.timestamp,
        "repo_name": source.repo_name,
        "url": source.url,
        "text_excerpt": _shorten(source.text, 220),
    }


def _load_rows(
    conn: sqlite3.Connection,
    table: str,
    timestamp_column: str,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    if not _table_exists(conn, table):
        return []
    rows = conn.execute(
        f"""SELECT *
            FROM {table}
            WHERE {timestamp_column} >= ? AND {timestamp_column} < ?
            ORDER BY {timestamp_column} DESC, id DESC""",
        (start.isoformat(), end.isoformat()),
    ).fetchall()
    return [dict(row) for row in rows]


def _load_github_activity_from_conn(conn: sqlite3.Connection, start: datetime, end: datetime) -> list[dict[str, Any]]:
    rows = _load_rows(conn, "github_activity", "updated_at", start, end)
    for row in rows:
        row["labels"] = _parse_json_list(row.get("labels"))
        row["metadata"] = _parse_json_object(row.get("metadata"))
        row["activity_id"] = _activity_id(row)
    return rows


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _activity_id(row: dict[str, Any]) -> str:
    if row.get("activity_id"):
        return str(row["activity_id"])
    if row.get("repo_name") is not None and row.get("number") is not None and row.get("activity_type") is not None:
        return f"{row.get('repo_name')}#{row.get('number')}:{row.get('activity_type')}"
    return ""


def _format_source_ids(source_ids: dict[str, list[str]]) -> str:
    return "; ".join(f"{key}={','.join(value)}" for key, value in source_ids.items())


def _parse_json_list(value: Any) -> list[Any]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _parse_json_object(value: Any) -> dict[str, Any]:
    if value is None or value == "":
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return _as_utc(value)
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _timestamp_text(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else (_clean(value) or None)


def _word_count(text: str) -> int:
    return len(_TOKEN_RE.findall(text or ""))


def _first_line(text: str) -> str:
    return _shorten((text or "").strip().splitlines()[0] if (text or "").strip() else "", 120)


def _shorten(value: Any, limit: int) -> str:
    text = _clean(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _clean(value: Any) -> str:
    return str(value or "").strip()
