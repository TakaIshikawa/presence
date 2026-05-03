"""Report opening-hook diversity across generated or published X posts."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 20
DEFAULT_THRESHOLD = 0.82
MIN_CLUSTER_SIZE = 2

_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_TOKEN_RE = re.compile(r"[a-z0-9]+(?:'[a-z0-9]+)?")
_BOUNDARY_RE = re.compile(r"[.!?;:,\n]|\s[-\u2013\u2014]{1,2}\s")
_REQUIRED_COLUMNS = {"generated_content": ("id", "content")}


@dataclass(frozen=True)
class HookDiversityPost:
    """One X post contributing to an opening-hook cluster."""

    post_id: int | str
    hook: str
    normalized_hook: str
    content_preview: str
    created_at: str | None = None
    published_at: str | None = None
    platform_post_id: str | None = None
    platform_url: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class HookDiversityCluster:
    """A group of X posts with similar normalized opening hooks."""

    cluster_size: int
    representative_hook: str
    normalized_representative_hook: str
    affected_post_ids: tuple[int | str, ...]
    max_similarity_threshold: float
    latest_seen_at: str | None
    examples: tuple[HookDiversityPost, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "affected_post_ids": list(self.affected_post_ids),
            "cluster_size": self.cluster_size,
            "examples": [example.to_dict() for example in self.examples],
            "latest_seen_at": self.latest_seen_at,
            "max_similarity_threshold": self.max_similarity_threshold,
            "normalized_representative_hook": self.normalized_representative_hook,
            "representative_hook": self.representative_hook,
        }


@dataclass(frozen=True)
class HookDiversityReport:
    """Read-only report of overused X post opening hooks."""

    ok: bool
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    clusters: tuple[HookDiversityCluster, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocking_issue_count(self) -> int:
        return len(self.clusters)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "hook_diversity",
            "blocking_issue_count": self.blocking_issue_count,
            "clusters": [cluster.to_dict() for cluster in self.clusters],
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "ok": self.ok,
            "totals": dict(sorted(self.totals.items())),
        }


def build_hook_diversity_report(
    source: Any | None = None,
    *,
    post_records: Iterable[Mapping[str, Any]] | None = None,
    days: int = DEFAULT_DAYS,
    threshold: float = DEFAULT_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> HookDiversityReport:
    """Build an opening-hook diversity report for recent X posts."""

    if source is not None and post_records is not None:
        raise ValueError("provide either source or post_records, not both")
    if days <= 0:
        raise ValueError("days must be positive")
    if not 0 < threshold <= 1:
        raise ValueError("threshold must be greater than 0 and at most 1")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": cutoff.isoformat(),
        "platform": "x",
        "threshold": threshold,
    }

    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] = {}
    if post_records is not None or _is_records(source):
        records = post_records if post_records is not None else source
        rows = [dict(row) for row in records]
    elif source is not None:
        conn = _connection(source)
        schema = _schema(conn)
        missing_tables, missing_columns = _schema_gaps(schema)
        rows = [] if missing_tables or missing_columns else _load_x_post_rows(conn, schema)
    else:
        raise ValueError("source or post_records is required")

    posts = _posts_from_rows(rows, cutoff=cutoff, now=generated_at)
    clusters = tuple(sorted(_clusters(posts, threshold=threshold), key=_cluster_sort_key)[:limit])
    totals = {
        "cluster_count": len(clusters),
        "posts_scanned": len(rows),
        "posts_with_hook": len(posts),
        "repeated_posts": sum(cluster.cluster_size for cluster in clusters),
    }
    return HookDiversityReport(
        ok=not clusters,
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=totals,
        clusters=clusters,
        missing_tables=missing_tables,
        missing_columns=missing_columns or None,
    )


def extract_opening_hook(content: Any) -> str:
    """Extract the first sentence fragment, falling back to the first words."""

    text = _collapse_spaces(_URL_RE.sub(" ", str(content or "")))
    if not text:
        return ""
    match = _BOUNDARY_RE.search(text)
    fragment = text[: match.start()] if match and match.start() > 0 else text
    words = fragment.split()
    if len(words) < 4:
        words = text.split()
    return " ".join(words[:8])


def normalize_opening_hook(hook: Any) -> str:
    """Normalize an opening hook for deterministic similarity grouping."""

    normalized = _URL_RE.sub(" ", str(hook or "").lower().replace("\u2019", "'"))
    tokens = _TOKEN_RE.findall(normalized)
    return " ".join(tokens[:8])


def hook_similarity(left: str, right: str) -> float:
    """Return a deterministic similarity score for normalized hooks."""

    left_tokens = set(left.split())
    right_tokens = set(right.split())
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens) / len(left_tokens | right_tokens)
    sequence = SequenceMatcher(None, left, right).ratio()
    return max(overlap, sequence)


def format_hook_diversity_json(report: HookDiversityReport) -> str:
    """Render deterministic JSON for automation."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_hook_diversity_text(report: HookDiversityReport) -> str:
    """Render a compact human-readable hook diversity report."""

    totals = report.totals
    lines = [
        "Hook Diversity Report",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"platform=x days={report.filters['days']} "
            f"threshold={report.filters['threshold']:g} limit={report.filters['limit']}"
        ),
        (
            "Totals: "
            f"posts={totals['posts_scanned']} with_hook={totals['posts_with_hook']} "
            f"clusters={totals['cluster_count']} repeated_posts={totals['repeated_posts']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        if missing:
            lines.append("Missing columns: " + missing)
    if not report.clusters:
        lines.append("No overused X post hook clusters found.")
        return "\n".join(lines)

    lines.append("Overused hook clusters:")
    for cluster in report.clusters:
        ids = ",".join(str(post_id) for post_id in cluster.affected_post_ids)
        lines.append(
            f"- size={cluster.cluster_size} hook={cluster.representative_hook!r} "
            f"latest={cluster.latest_seen_at or '-'} post_ids={ids}"
        )
    return "\n".join(lines)


def _posts_from_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    cutoff: datetime,
    now: datetime,
) -> list[HookDiversityPost]:
    posts: list[HookDiversityPost] = []
    for row in rows:
        seen_at = _parse_timestamp(
            row.get("seen_at") or row.get("published_at") or row.get("created_at")
        )
        if seen_at is not None and not cutoff <= seen_at <= now:
            continue
        content = _optional_text(row.get("content") or row.get("text") or row.get("post_text"))
        if not content:
            continue
        hook = extract_opening_hook(content)
        normalized = normalize_opening_hook(hook)
        if not normalized:
            continue
        posts.append(
            HookDiversityPost(
                post_id=_post_id(row),
                hook=hook,
                normalized_hook=normalized,
                content_preview=_preview(content),
                created_at=_optional_text(row.get("created_at")),
                published_at=_optional_text(row.get("published_at")),
                platform_post_id=_optional_text(row.get("platform_post_id")),
                platform_url=_optional_text(row.get("platform_url")),
            )
        )
    return sorted(posts, key=_post_sort_key, reverse=True)


def _clusters(
    posts: list[HookDiversityPost],
    *,
    threshold: float,
) -> list[HookDiversityCluster]:
    groups: list[list[HookDiversityPost]] = []
    representatives: list[str] = []
    for post in posts:
        match_index = _matching_group_index(post.normalized_hook, representatives, threshold)
        if match_index is None:
            groups.append([post])
            representatives.append(post.normalized_hook)
        else:
            groups[match_index].append(post)

    clusters: list[HookDiversityCluster] = []
    for group in groups:
        if len(group) < MIN_CLUSTER_SIZE:
            continue
        ordered = sorted(group, key=_post_sort_key, reverse=True)
        representative = _representative_post(ordered)
        clusters.append(
            HookDiversityCluster(
                cluster_size=len(ordered),
                representative_hook=representative.hook,
                normalized_representative_hook=representative.normalized_hook,
                affected_post_ids=tuple(post.post_id for post in ordered),
                max_similarity_threshold=threshold,
                latest_seen_at=_seen_at_text(ordered[0]),
                examples=tuple(ordered[:5]),
            )
        )
    return clusters


def _matching_group_index(
    hook: str,
    representatives: list[str],
    threshold: float,
) -> int | None:
    for index, representative in enumerate(representatives):
        if hook == representative or hook_similarity(hook, representative) >= threshold:
            return index
    return None


def _representative_post(posts: list[HookDiversityPost]) -> HookDiversityPost:
    counts: dict[str, int] = {}
    first_by_hook: dict[str, HookDiversityPost] = {}
    for post in posts:
        counts[post.normalized_hook] = counts.get(post.normalized_hook, 0) + 1
        first_by_hook.setdefault(post.normalized_hook, post)
    normalized = sorted(counts, key=lambda value: (-counts[value], value))[0]
    return first_by_hook[normalized]


def _load_x_post_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[dict[str, Any]]:
    gc = schema["generated_content"]
    cp = schema.get("content_publications", set())
    has_publications = "content_publications" in schema and {"content_id", "platform"}.issubset(cp)
    joins = ""
    filters: list[str] = []
    if "content_type" in gc:
        filters.append("gc.content_type = 'x_post'")
    if has_publications:
        joins = (
            "LEFT JOIN content_publications cp ON cp.content_id = gc.id "
            "AND LOWER(cp.platform) IN ('x', 'twitter')"
        )
        filters.append(
            "(cp.content_id IS NOT NULL OR gc.content_type = 'x_post')"
            if "content_type" in gc
            else "cp.content_id IS NOT NULL"
        )

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = conn.execute(
        f"""SELECT
               gc.id AS post_id,
               gc.content AS content,
               {_column_expr(gc, "created_at", "NULL", alias="gc")} AS created_at,
               {_column_expr(gc, "published_at", "NULL", alias="gc")} AS legacy_published_at,
               {_column_expr(cp, "published_at", "NULL", alias="cp")} AS publication_published_at,
               {_column_expr(cp, "platform_post_id", "NULL", alias="cp")} AS platform_post_id,
               {_column_expr(cp, "platform_url", "NULL", alias="cp")} AS platform_url
           FROM generated_content gc
           {joins}
           {where}
           ORDER BY COALESCE(publication_published_at, legacy_published_at, created_at, '') DESC,
                    gc.id DESC"""
    ).fetchall()
    output: list[dict[str, Any]] = []
    for row in rows:
        data = dict(row)
        data["published_at"] = data.pop("publication_published_at") or data.pop(
            "legacy_published_at"
        )
        data["seen_at"] = data.get("published_at") or data.get("created_at")
        output.append(data)
    return output


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    missing_tables = tuple(table for table in _REQUIRED_COLUMNS if table not in schema)
    missing_columns = {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in _REQUIRED_COLUMNS.items()
        if table in schema and any(column not in schema[table] for column in columns)
    }
    return missing_tables, missing_columns


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in rows}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _column_expr(columns: set[str], column: str, fallback: str, *, alias: str) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _is_records(value: Any) -> bool:
    if value is None or isinstance(value, (sqlite3.Connection, str, bytes)):
        return False
    if hasattr(value, "conn"):
        return False
    return isinstance(value, Iterable)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _as_utc(value)
    try:
        return _as_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _post_id(row: Mapping[str, Any]) -> int | str:
    value = row.get("post_id") or row.get("content_id") or row.get("id")
    try:
        return int(value)
    except (TypeError, ValueError):
        return str(value or "")


def _post_sort_key(post: HookDiversityPost) -> tuple[float, str]:
    return (_timestamp_sort_value(_seen_at_text(post)), str(post.post_id))


def _cluster_sort_key(cluster: HookDiversityCluster) -> tuple[int, float, str]:
    return (
        -cluster.cluster_size,
        -_timestamp_sort_value(cluster.latest_seen_at),
        cluster.normalized_representative_hook,
    )


def _timestamp_sort_value(value: str | None) -> float:
    parsed = _parse_timestamp(value)
    return parsed.timestamp() if parsed else 0.0


def _seen_at_text(post: HookDiversityPost) -> str | None:
    return post.published_at or post.created_at


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _preview(content: str, max_chars: int = 120) -> str:
    text = _collapse_spaces(content)
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def _collapse_spaces(value: str) -> str:
    return " ".join(str(value).split())
