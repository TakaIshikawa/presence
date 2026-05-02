"""Report repeated opening clauses in recently published X posts."""

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
MIN_CLUSTER_SIZE = 2

_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_TOKEN_RE = re.compile(r"[a-z0-9]+(?:'[a-z0-9]+)?")
_BOUNDARY_RE = re.compile(r"[.!?;:,\n]|\s[-\u2013\u2014]{1,2}\s")
_REQUIRED_COLUMNS = {
    "content_publications": ("content_id", "platform", "published_at"),
    "generated_content": ("id", "content"),
}


@dataclass(frozen=True)
class XPostOpeningFatiguePost:
    """One published X post contributing to an opening-clause cluster."""

    post_id: int
    published_at: str
    normalized_opening: str
    opening_text: str
    content_preview: str
    platform_post_id: str | None = None
    platform_url: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class XPostOpeningFatigueCluster:
    """Repeated or near-repeated opening clause across published posts."""

    normalized_opening: str
    count: int
    latest_published_at: str
    example_post_ids: tuple[int, ...]
    examples: tuple[XPostOpeningFatiguePost, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "example_post_ids": list(self.example_post_ids),
            "examples": [example.to_dict() for example in self.examples],
            "latest_published_at": self.latest_published_at,
            "normalized_opening": self.normalized_opening,
        }


@dataclass(frozen=True)
class XPostOpeningFatigueReport:
    """Opening-fatigue report with filters, totals, and schema gaps."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    clusters: tuple[XPostOpeningFatigueCluster, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "clusters": [cluster.to_dict() for cluster in self.clusters],
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def analyze_x_post_opening_fatigue(
    posts: Iterable[Mapping[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> XPostOpeningFatigueReport:
    """Analyze recent published X post rows for repeated opening clauses."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_start": cutoff.isoformat(),
        "lookback_end": generated_at.isoformat(),
        "platform": "x",
    }

    examples: list[XPostOpeningFatiguePost] = []
    rows_scanned = 0
    for row in posts:
        rows_scanned += 1
        published_at = _optional_text(row.get("published_at"))
        published_ts = _parse_timestamp(published_at)
        if published_ts is not None and not cutoff <= published_ts <= generated_at:
            continue
        content = _optional_text(row.get("content"))
        if not content:
            continue
        opening_text = extract_x_post_opening_clause(content)
        normalized = normalize_x_post_opening(opening_text)
        if not normalized:
            continue
        examples.append(
            XPostOpeningFatiguePost(
                post_id=_coerce_int(
                    row.get("post_id") or row.get("content_id") or row.get("id")
                ),
                published_at=published_at or "",
                normalized_opening=normalized,
                opening_text=opening_text,
                content_preview=_preview(content),
                platform_post_id=_optional_text(row.get("platform_post_id")),
                platform_url=_optional_text(row.get("platform_url")),
            )
        )

    clusters = _clusters(examples)
    clusters = tuple(sorted(clusters, key=_cluster_sort_key)[:limit])
    return XPostOpeningFatigueReport(
        artifact_type="x_post_opening_fatigue",
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "clusters": len(clusters),
            "posts_scanned": rows_scanned,
            "posts_with_opening": len(examples),
            "repeated_posts": sum(cluster.count for cluster in clusters),
        },
        clusters=clusters,
        missing_tables=missing_tables,
        missing_columns=missing_columns or {},
    )


def build_x_post_opening_fatigue_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> XPostOpeningFatigueReport:
    """Load recent published X posts from SQLite and report opening fatigue."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return analyze_x_post_opening_fatigue(
            (),
            days=days,
            limit=limit,
            now=now,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    rows = _load_published_x_posts(conn, schema, cutoff=cutoff, now=generated_at)
    return analyze_x_post_opening_fatigue(
        rows,
        days=days,
        limit=limit,
        now=generated_at,
    )


def format_x_post_opening_fatigue_json(report: XPostOpeningFatigueReport) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_x_post_opening_fatigue_markdown(report: XPostOpeningFatigueReport) -> str:
    """Render the report as Markdown for operator review."""
    totals = report.totals
    lines = [
        "# X Post Opening Fatigue",
        "",
        f"Generated: {report.generated_at}",
        (
            f"Filters: platform=x days={report.filters['days']} "
            f"limit={report.filters['limit']}"
        ),
        (
            "Totals: "
            f"posts={totals['posts_scanned']} "
            f"with_opening={totals['posts_with_opening']} "
            f"clusters={totals['clusters']} "
            f"repeated_posts={totals['repeated_posts']}"
        ),
    ]
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in report.missing_columns.items()
            if columns
        ]
        if missing:
            lines.append(f"Missing columns: {'; '.join(missing)}")

    lines.extend(["", "## Repeated Opening Clusters"])
    if not report.clusters:
        lines.append("")
        lines.append("No repeated X post openings found.")
        return "\n".join(lines)

    for cluster in report.clusters:
        lines.extend(
            [
                "",
                (
                    f"- `{cluster.normalized_opening}` "
                    f"(count={cluster.count}, latest={cluster.latest_published_at or '-'})"
                ),
            ]
        )
        for example in cluster.examples:
            lines.append(
                f"  - post #{example.post_id} at {example.published_at or '-'}: "
                f"`{example.normalized_opening}`"
            )
    return "\n".join(lines)


def format_x_post_opening_fatigue_text(report: XPostOpeningFatigueReport) -> str:
    """Compatibility alias for Markdown output."""
    return format_x_post_opening_fatigue_markdown(report)


def extract_x_post_opening_clause(content: str) -> str:
    """Extract the first 4-8 words or first sentence fragment from a post."""
    text = _collapse_spaces(_URL_RE.sub(" ", str(content)))
    if not text:
        return ""
    match = _BOUNDARY_RE.search(text)
    fragment = text[: match.start()] if match and match.start() > 0 else text
    words = fragment.split()
    if len(words) < 4:
        words = text.split()
    return " ".join(words[:8])


def normalize_x_post_opening(opening: str) -> str:
    """Normalize an opening clause for deterministic grouping."""
    normalized = _URL_RE.sub(" ", str(opening).lower().replace("\u2019", "'"))
    tokens = _TOKEN_RE.findall(normalized)
    return " ".join(tokens[:8])


def _load_published_x_posts(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    cp = schema["content_publications"]
    gc = schema["generated_content"]
    filters = [
        "LOWER(cp.platform) IN ('x', 'twitter')",
        "cp.published_at IS NOT NULL",
        "cp.published_at >= ?",
        "cp.published_at <= ?",
    ]
    params: list[Any] = [cutoff.isoformat(), now.isoformat()]
    if "status" in cp:
        filters.append("LOWER(cp.status) = 'published'")
    if "content_type" in gc:
        filters.append("gc.content_type = 'x_post'")

    rows = conn.execute(
        f"""SELECT
               gc.id AS post_id,
               gc.content AS content,
               cp.published_at AS published_at,
               {_column_expr(cp, "platform_post_id", "NULL", alias="cp")} AS platform_post_id,
               {_column_expr(cp, "platform_url", "NULL", alias="cp")} AS platform_url
           FROM content_publications cp
           INNER JOIN generated_content gc ON gc.id = cp.content_id
           WHERE {' AND '.join(filters)}
           ORDER BY cp.published_at DESC, gc.id DESC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows if _parse_timestamp(row["published_at"]) is not None]


def _clusters(
    examples: list[XPostOpeningFatiguePost],
) -> list[XPostOpeningFatigueCluster]:
    groups: list[list[XPostOpeningFatiguePost]] = []
    representatives: list[str] = []
    for example in sorted(
        examples,
        key=lambda item: (item.published_at, item.post_id),
        reverse=True,
    ):
        match_index = _matching_group_index(example.normalized_opening, representatives)
        if match_index is None:
            groups.append([example])
            representatives.append(example.normalized_opening)
        else:
            groups[match_index].append(example)

    clusters: list[XPostOpeningFatigueCluster] = []
    for group in groups:
        if len(group) < MIN_CLUSTER_SIZE:
            continue
        ordered = sorted(
            group,
            key=lambda item: (item.published_at, item.post_id),
            reverse=True,
        )
        normalized_opening = _representative_opening(ordered)
        clusters.append(
            XPostOpeningFatigueCluster(
                normalized_opening=normalized_opening,
                count=len(ordered),
                latest_published_at=ordered[0].published_at,
                example_post_ids=tuple(example.post_id for example in ordered[:5]),
                examples=tuple(ordered[:5]),
            )
        )
    return clusters


def _matching_group_index(opening: str, representatives: list[str]) -> int | None:
    for index, representative in enumerate(representatives):
        if opening == representative or _opening_similarity(opening, representative) >= 0.82:
            return index
    return None


def _opening_similarity(left: str, right: str) -> float:
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens) / len(left_tokens | right_tokens)
    sequence = SequenceMatcher(None, left, right).ratio()
    return max(overlap, sequence)


def _representative_opening(examples: list[XPostOpeningFatiguePost]) -> str:
    counts: dict[str, int] = {}
    for example in examples:
        counts[example.normalized_opening] = counts.get(example.normalized_opening, 0) + 1
    return sorted(counts, key=lambda value: (-counts[value], value))[0]


def _cluster_sort_key(cluster: XPostOpeningFatigueCluster) -> tuple[float, int, str]:
    return (
        -_timestamp_sort_value(cluster.latest_published_at),
        -cluster.count,
        cluster.normalized_opening,
    )


def _timestamp_sort_value(value: str) -> float:
    parsed = _parse_timestamp(value)
    return parsed.timestamp() if parsed else 0.0


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


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    try:
        return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _preview(content: str, max_chars: int = 120) -> str:
    text = _collapse_spaces(content)
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def _collapse_spaces(value: str) -> str:
    return " ".join(str(value).split())
