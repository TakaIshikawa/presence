"""X post hashtag density and style drift reporting."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_RECENT_DAYS = 14
DEFAULT_BASELINE_DAYS = 45
DEFAULT_LIMIT = 100
DEFAULT_MAX_HASHTAGS = 3
DEFAULT_MAX_HASHTAG_CHAR_SHARE = 0.2
DEFAULT_REPEATED_SET_THRESHOLD = 3
DEFAULT_COUNT_DRIFT_DELTA = 1.0
DEFAULT_SHARE_DRIFT_DELTA = 0.08
X_CONTENT_TYPES = ("x_post", "x_thread", "x_visual", "x_long_post")

HASHTAG_RE = re.compile(r"(?<![\w&])#([A-Za-z][A-Za-z0-9_]*)")
URL_RE = re.compile(r"https?://[^\s<>)]+|www\.[^\s<>)]+", re.IGNORECASE)


@dataclass(frozen=True)
class XHashtagMetrics:
    """Hashtag metrics for one X content row."""

    post_id: int
    source: str
    status: str
    timestamp: str
    content: str
    hashtags: tuple[str, ...]
    canonical_hashtags: tuple[str, ...]
    hashtag_count: int
    hashtag_character_count: int
    character_count: int
    hashtag_character_share: float
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["hashtags"] = list(self.hashtags)
        data["canonical_hashtags"] = list(self.canonical_hashtags)
        data["warnings"] = list(self.warnings)
        return data


@dataclass(frozen=True)
class XHashtagCluster:
    """Repeated hashtag set across multiple posts."""

    canonical_hashtags: tuple[str, ...]
    post_ids: tuple[int, ...]
    count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "canonical_hashtags": list(self.canonical_hashtags),
            "count": self.count,
            "post_ids": list(self.post_ids),
        }


@dataclass(frozen=True)
class XHashtagBaseline:
    """Aggregate hashtag style metrics for a window."""

    post_count: int
    average_hashtag_count: float
    average_hashtag_character_share: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class XHashtagDensityReport:
    """X hashtag density and drift report."""

    generated_at: str
    recent_days: int
    baseline_days: int
    limit: int
    max_hashtags: int
    max_hashtag_char_share: float
    repeated_set_threshold: int
    recent_baseline: XHashtagBaseline
    historical_baseline: XHashtagBaseline
    drift_warnings: tuple[str, ...]
    repeated_clusters: tuple[XHashtagCluster, ...]
    posts: tuple[XHashtagMetrics, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] = field(default_factory=dict)

    @property
    def flagged_posts(self) -> int:
        return sum(1 for post in self.posts if post.warnings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "x_hashtag_density",
            "baseline_days": self.baseline_days,
            "drift_warnings": list(self.drift_warnings),
            "flagged_posts": self.flagged_posts,
            "generated_at": self.generated_at,
            "historical_baseline": self.historical_baseline.to_dict(),
            "limit": self.limit,
            "max_hashtag_char_share": self.max_hashtag_char_share,
            "max_hashtags": self.max_hashtags,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "posts": [post.to_dict() for post in self.posts],
            "recent_baseline": self.recent_baseline.to_dict(),
            "recent_days": self.recent_days,
            "repeated_clusters": [cluster.to_dict() for cluster in self.repeated_clusters],
            "repeated_set_threshold": self.repeated_set_threshold,
            "total_posts": len(self.posts),
        }


def build_x_hashtag_density_report(
    db_or_conn: Any,
    *,
    recent_days: int = DEFAULT_RECENT_DAYS,
    baseline_days: int = DEFAULT_BASELINE_DAYS,
    limit: int = DEFAULT_LIMIT,
    max_hashtags: int = DEFAULT_MAX_HASHTAGS,
    max_hashtag_char_share: float = DEFAULT_MAX_HASHTAG_CHAR_SHARE,
    repeated_set_threshold: int = DEFAULT_REPEATED_SET_THRESHOLD,
) -> XHashtagDensityReport:
    """Load generated/published X posts and return density warnings."""
    recent = _positive_int(recent_days, "recent_days")
    baseline = _positive_int(baseline_days, "baseline_days")
    row_limit = _positive_int(limit, "limit")
    max_tags = _positive_int(max_hashtags, "max_hashtags")
    max_share = _share(max_hashtag_char_share, "max_hashtag_char_share")
    repeat_threshold = _positive_int(repeated_set_threshold, "repeated_set_threshold")
    generated_at = datetime.now(timezone.utc).isoformat()

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or _missing_required_columns(missing_columns):
        return _empty_report(
            generated_at,
            recent,
            baseline,
            row_limit,
            max_tags,
            max_share,
            repeat_threshold,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_x_post_rows(conn, schema, recent_days=recent, baseline_days=baseline, limit=row_limit)
    return analyze_x_hashtag_density(
        rows,
        recent_days=recent,
        baseline_days=baseline,
        limit=row_limit,
        max_hashtags=max_tags,
        max_hashtag_char_share=max_share,
        repeated_set_threshold=repeat_threshold,
        generated_at=generated_at,
    )


def analyze_x_hashtag_density(
    rows: Sequence[Mapping[str, Any]],
    *,
    recent_days: int = DEFAULT_RECENT_DAYS,
    baseline_days: int = DEFAULT_BASELINE_DAYS,
    limit: int = DEFAULT_LIMIT,
    max_hashtags: int = DEFAULT_MAX_HASHTAGS,
    max_hashtag_char_share: float = DEFAULT_MAX_HASHTAG_CHAR_SHARE,
    repeated_set_threshold: int = DEFAULT_REPEATED_SET_THRESHOLD,
    generated_at: str | None = None,
) -> XHashtagDensityReport:
    """Analyze X post-like mappings without querying storage."""
    recent = _positive_int(recent_days, "recent_days")
    baseline = _positive_int(baseline_days, "baseline_days")
    row_limit = _positive_int(limit, "limit")
    max_tags = _positive_int(max_hashtags, "max_hashtags")
    max_share = _share(max_hashtag_char_share, "max_hashtag_char_share")
    repeat_threshold = _positive_int(repeated_set_threshold, "repeated_set_threshold")
    generated = generated_at or datetime.now(timezone.utc).isoformat()

    parsed = [_metrics(row) for row in rows]
    sorted_posts = sorted(parsed, key=lambda post: (post.timestamp, post.post_id), reverse=True)
    recent_posts = tuple(post for post in sorted_posts if _window(row_by_id(rows, post.post_id)) == "recent")
    if not recent_posts:
        recent_posts = tuple(post for post in sorted_posts[:row_limit])
    baseline_posts = tuple(post for post in sorted_posts if _window(row_by_id(rows, post.post_id)) == "baseline")

    repeated_clusters = _repeated_clusters(recent_posts, threshold=repeat_threshold)
    repeated_ids = {post_id for cluster in repeated_clusters for post_id in cluster.post_ids}
    posts = tuple(
        _with_warnings(
            post,
            max_hashtags=max_tags,
            max_hashtag_char_share=max_share,
            repeated_ids=repeated_ids,
        )
        for post in recent_posts
    )
    recent_stats = _baseline(posts)
    historical_stats = _baseline(baseline_posts)
    drift = _drift_warnings(recent_stats, historical_stats)

    return XHashtagDensityReport(
        generated_at=generated,
        recent_days=recent,
        baseline_days=baseline,
        limit=row_limit,
        max_hashtags=max_tags,
        max_hashtag_char_share=max_share,
        repeated_set_threshold=repeat_threshold,
        recent_baseline=recent_stats,
        historical_baseline=historical_stats,
        drift_warnings=drift,
        repeated_clusters=repeated_clusters,
        posts=posts,
    )


def extract_hashtags(content: str) -> tuple[str, ...]:
    """Extract hashtags while ignoring URL fragments and duplicate casings."""
    masked = _mask_urls(content or "")
    seen: set[str] = set()
    hashtags: list[str] = []
    for match in HASHTAG_RE.finditer(masked):
        original = "#" + match.group(1)
        canonical = canonicalize_hashtag(original)
        if canonical in seen:
            continue
        seen.add(canonical)
        hashtags.append(original)
    return tuple(hashtags)


def canonicalize_hashtag(hashtag: str) -> str:
    """Return the case-insensitive canonical hashtag."""
    text = str(hashtag or "").strip()
    return "#" + text.lstrip("#").casefold() if text else ""


def format_x_hashtag_density_json(report: XHashtagDensityReport) -> str:
    """Render deterministic JSON for monitoring."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_x_hashtag_density_text(report: XHashtagDensityReport) -> str:
    """Render a compact human-readable report."""
    lines = [
        "X Hashtag Density",
        f"Recent window: {report.recent_days} days",
        f"Baseline window: {report.baseline_days} days",
        f"Limits: max_hashtags={report.max_hashtags}, max_share={report.max_hashtag_char_share:.2f}",
        (
            "Summary: "
            f"{len(report.posts)} recent posts, {report.flagged_posts} flagged, "
            f"{len(report.drift_warnings)} drift warnings"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = [
            f"{table}.{column}"
            for table, columns in sorted(report.missing_columns.items())
            for column in columns
        ]
        lines.append("Missing columns: " + ", ".join(missing))
    if report.drift_warnings:
        lines.append("Drift: " + ", ".join(report.drift_warnings))
    if report.repeated_clusters:
        lines.append("Repeated clusters:")
        for cluster in report.repeated_clusters:
            lines.append(
                f"- {' '.join(cluster.canonical_hashtags)} count={cluster.count} "
                f"posts={','.join(str(post_id) for post_id in cluster.post_ids)}"
            )
    if not report.posts:
        lines.append("No recent X posts found.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Posts:")
    for post in report.posts:
        warning_text = ", ".join(post.warnings) if post.warnings else "clean"
        tags = " ".join(post.hashtags) or "-"
        lines.append(
            f"- #{post.post_id} [{post.status}] hashtags={post.hashtag_count} "
            f"share={post.hashtag_character_share:.2f} tags={tags}; warnings={warning_text}"
        )
    return "\n".join(lines)


def _metrics(row: Mapping[str, Any]) -> XHashtagMetrics:
    content = str(row.get("content") or "")
    hashtags = extract_hashtags(content)
    canonical = tuple(canonicalize_hashtag(hashtag) for hashtag in hashtags)
    hashtag_chars = sum(len(hashtag) for hashtag in hashtags)
    characters = len("".join((content or "").split()))
    share = round(hashtag_chars / characters, 4) if characters else 0.0
    return XHashtagMetrics(
        post_id=int(row.get("id") or row.get("post_id") or row.get("content_id") or 0),
        source=str(row.get("source") or "generated_content"),
        status=str(row.get("status") or ""),
        timestamp=str(row.get("timestamp") or row.get("created_at") or row.get("published_at") or ""),
        content=content,
        hashtags=hashtags,
        canonical_hashtags=canonical,
        hashtag_count=len(hashtags),
        hashtag_character_count=hashtag_chars,
        character_count=characters,
        hashtag_character_share=share,
    )


def _with_warnings(
    post: XHashtagMetrics,
    *,
    max_hashtags: int,
    max_hashtag_char_share: float,
    repeated_ids: set[int],
) -> XHashtagMetrics:
    warnings: list[str] = []
    if post.hashtag_count > max_hashtags:
        warnings.append("excessive_hashtag_count")
    if post.hashtag_character_share > max_hashtag_char_share:
        warnings.append("high_hashtag_character_share")
    if post.post_id in repeated_ids:
        warnings.append("repeated_hashtag_set")
    return XHashtagMetrics(**{**asdict(post), "warnings": tuple(warnings)})


def _repeated_clusters(
    posts: Sequence[XHashtagMetrics],
    *,
    threshold: int,
) -> tuple[XHashtagCluster, ...]:
    buckets: dict[tuple[str, ...], list[int]] = {}
    for post in posts:
        if not post.canonical_hashtags:
            continue
        key = tuple(sorted(post.canonical_hashtags))
        buckets.setdefault(key, []).append(post.post_id)
    clusters = [
        XHashtagCluster(canonical_hashtags=key, post_ids=tuple(ids), count=len(ids))
        for key, ids in buckets.items()
        if len(ids) >= threshold
    ]
    return tuple(sorted(clusters, key=lambda cluster: (-cluster.count, cluster.canonical_hashtags)))


def _baseline(posts: Sequence[XHashtagMetrics]) -> XHashtagBaseline:
    if not posts:
        return XHashtagBaseline(
            post_count=0,
            average_hashtag_count=0.0,
            average_hashtag_character_share=0.0,
        )
    return XHashtagBaseline(
        post_count=len(posts),
        average_hashtag_count=round(sum(post.hashtag_count for post in posts) / len(posts), 4),
        average_hashtag_character_share=round(
            sum(post.hashtag_character_share for post in posts) / len(posts),
            4,
        ),
    )


def _drift_warnings(
    recent: XHashtagBaseline,
    baseline: XHashtagBaseline,
) -> tuple[str, ...]:
    if recent.post_count == 0 or baseline.post_count == 0:
        return ()
    warnings: list[str] = []
    if recent.average_hashtag_count - baseline.average_hashtag_count >= DEFAULT_COUNT_DRIFT_DELTA:
        warnings.append("hashtag_count_drift")
    if (
        recent.average_hashtag_character_share - baseline.average_hashtag_character_share
        >= DEFAULT_SHARE_DRIFT_DELTA
    ):
        warnings.append("hashtag_share_drift")
    return tuple(warnings)


def _load_x_post_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    recent_days: int,
    baseline_days: int,
    limit: int,
) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    publication_columns = schema.get("content_publications", set())
    can_join_publications = {"content_id", "platform", "status"} <= publication_columns
    content_alias = "gc" if can_join_publications else ""
    selected = [
        _column_expr(columns, "id", alias=content_alias),
        _column_expr(columns, "content", alias=content_alias),
        _column_expr(columns, "content_type", "''", alias=content_alias),
        _column_expr(columns, "published", "0", alias=content_alias),
        _column_expr(columns, "published_url", alias=content_alias),
        _column_expr(columns, "tweet_id", alias=content_alias),
        _column_expr(columns, "published_at", alias=content_alias),
        _column_expr(columns, "created_at", alias=content_alias),
    ]
    if can_join_publications:
        selected.extend(
            [
                "cp.status AS publication_status",
                _column_expr(
                    publication_columns,
                    "published_at",
                    alias="cp",
                    output="publication_published_at",
                ),
            ]
        )
    timestamp_expr = _generated_timestamp_expr(
        columns,
        include_publications=can_join_publications and "published_at" in publication_columns,
    )
    cutoff_days = recent_days + baseline_days
    from_expr = "generated_content"
    if can_join_publications:
        from_expr = (
            "generated_content gc "
            "LEFT JOIN content_publications cp "
            "ON cp.content_id = gc.id AND LOWER(cp.platform) = 'x'"
        )
    content_prefix = f"{content_alias}." if content_alias else ""
    cursor = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM {from_expr}
            WHERE {content_prefix}content_type IN ({', '.join('?' for _ in X_CONTENT_TYPES)})
              AND datetime({timestamp_expr}) >= datetime('now', ?)
            ORDER BY datetime({timestamp_expr}) DESC, {content_prefix}id DESC
            LIMIT ?""",
        (*X_CONTENT_TYPES, f"-{cutoff_days} days", limit),
    )
    rows = _cursor_dicts(cursor)

    queue_by_content = _queue_statuses(conn, schema)
    output: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        content_id = int(item.get("id") or 0)
        queue = queue_by_content.get(content_id)
        timestamp = (
            item.get("publication_published_at")
            or item.get("published_at")
            or (queue or {}).get("scheduled_at")
            or item.get("created_at")
            or ""
        )
        item["timestamp"] = timestamp
        item["status"] = _status(item, queue)
        item["source"] = "content_publications" if item.get("publication_status") else "generated_content"
        item["window"] = _classify_window(timestamp, recent_days=recent_days, baseline_days=baseline_days)
        output.append(item)
    return output


def _status(
    row: Mapping[str, Any],
    queue: Mapping[str, Any] | None,
) -> str:
    if row.get("publication_status"):
        return str(row["publication_status"])
    if queue and queue.get("status"):
        return str(queue["status"])
    if int(row.get("published") or 0) == 1:
        return "published"
    if int(row.get("published") or 0) < 0:
        return "abandoned"
    return "generated"


def _queue_statuses(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, dict[str, Any]]:
    if "publish_queue" not in schema:
        return {}
    columns = schema["publish_queue"]
    if not {"content_id", "status"} <= columns:
        return {}
    selected = [
        "content_id",
        "status",
        _column_expr(columns, "scheduled_at"),
        _column_expr(columns, "published_at"),
    ]
    order_expr = _coalesce_expr(columns, ("published_at", "scheduled_at", "created_at"))
    platform_filter = (
        "WHERE LOWER(COALESCE(platform, 'all')) IN ('x', 'all')"
        if "platform" in columns
        else ""
    )
    id_order = ", id DESC" if "id" in columns else ""
    cursor = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM publish_queue
            {platform_filter}
            ORDER BY datetime({order_expr}) DESC{id_order}"""
    )
    statuses: dict[int, dict[str, Any]] = {}
    for item in _cursor_dicts(cursor):
        statuses.setdefault(int(item["content_id"]), item)
    return statuses


def _classify_window(timestamp: Any, *, recent_days: int, baseline_days: int) -> str:
    parsed = _parse_timestamp(timestamp)
    if parsed is None:
        return "recent"
    now = datetime.now(timezone.utc)
    age_days = (now - parsed).total_seconds() / 86400
    if age_days <= recent_days:
        return "recent"
    if age_days <= recent_days + baseline_days:
        return "baseline"
    return "older"


def _window(row: Mapping[str, Any]) -> str:
    return str(row.get("window") or "recent")


def row_by_id(rows: Sequence[Mapping[str, Any]], post_id: int) -> Mapping[str, Any]:
    for row in rows:
        row_id = int(row.get("id") or row.get("post_id") or row.get("content_id") or 0)
        if row_id == post_id:
            return row
    return {}


def _mask_urls(text: str) -> str:
    return URL_RE.sub(lambda match: " " * (match.end() - match.start()), text)


def _generated_timestamp_expr(columns: set[str], *, include_publications: bool = False) -> str:
    timestamps = [column for column in ("published_at", "created_at") if column in columns]
    if include_publications:
        timestamps.insert(0, "cp.published_at")
    timestamps = [
        timestamp if "." in timestamp else f"gc.{timestamp}"
        for timestamp in timestamps
    ] if include_publications else timestamps
    if not timestamps:
        return "CURRENT_TIMESTAMP"
    if len(timestamps) == 1:
        return timestamps[0]
    return "COALESCE(" + ", ".join(timestamps) + ")"


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    if "generated_content" not in schema:
        return ("generated_content",), {}
    missing = tuple(sorted({"id", "content", "content_type"} - schema["generated_content"]))
    return (), {"generated_content": missing} if missing else {}


def _missing_required_columns(missing_columns: dict[str, tuple[str, ...]]) -> bool:
    return any(columns for columns in missing_columns.values())


def _empty_report(
    generated_at: str,
    recent_days: int,
    baseline_days: int,
    limit: int,
    max_hashtags: int,
    max_hashtag_char_share: float,
    repeated_set_threshold: int,
    *,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> XHashtagDensityReport:
    empty = XHashtagBaseline(0, 0.0, 0.0)
    return XHashtagDensityReport(
        generated_at=generated_at,
        recent_days=recent_days,
        baseline_days=baseline_days,
        limit=limit,
        max_hashtags=max_hashtags,
        max_hashtag_char_share=max_hashtag_char_share,
        repeated_set_threshold=repeated_set_threshold,
        recent_baseline=empty,
        historical_baseline=empty,
        drift_warnings=(),
        repeated_clusters=(),
        posts=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns or {},
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[str(table)] = {
            column[1] for column in conn.execute(f"PRAGMA table_info({_quote_identifier(str(table))})")
        }
    return schema


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str = "",
    output: str | None = None,
) -> str:
    name = output or column
    prefix = f"{alias}." if alias else ""
    return f"{prefix}{column} AS {name}" if column in columns else f"{fallback} AS {name}"


def _cursor_dicts(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    names = [description[0] for description in cursor.description or ()]
    return [
        {
            names[index]: value
            for index, value in enumerate(row)
        }
        for row in cursor.fetchall()
    ]


def _coalesce_expr(columns: set[str], candidates: Sequence[str]) -> str:
    available = [column for column in candidates if column in columns]
    if not available:
        return "CURRENT_TIMESTAMP"
    if len(available) == 1:
        return available[0]
    return "COALESCE(" + ", ".join(available) + ")"


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _positive_int(value: Any, name: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


def _share(value: Any, name: str) -> float:
    parsed = float(value)
    if parsed <= 0 or parsed > 1:
        raise ValueError(f"{name} must be greater than 0 and at most 1")
    return parsed


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'
