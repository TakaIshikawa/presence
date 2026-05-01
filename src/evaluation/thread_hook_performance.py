"""Thread opening style performance reporting."""

from __future__ import annotations

import json
import re
import sqlite3
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from output.x_client import parse_thread_content


DEFAULT_DAYS = 30
DEFAULT_MIN_COUNT = 1
HOOK_STYLES = (
    "question",
    "confession",
    "build-log",
    "contrarian",
    "metric-led",
    "plain-summary",
)

_NUMBER_RE = re.compile(r"^\s*(?:\d+[.)/:-]\s*)+")
_WHITESPACE_RE = re.compile(r"\s+")
_METRIC_RE = re.compile(
    r"(?ix)"
    r"^\s*(?:\d+(?:[,.]\d+)*(?:\.\d+)?\s*(?:%|x|k|m|ms|s|min|hours?|days?)\b|"
    r"(?:after|before|in|over)\s+\d+(?:[,.]\d+)*(?:\.\d+)?\b)"
)
_BUILD_LOG_RE = re.compile(
    r"(?i)\b(i|we)\s+(built|shipped|launched|implemented|added|fixed|debugged|rewrote|migrated)\b|"
    r"\b(building|shipping|launching|debugging|rewriting)\b|"
    r"\bbuild log\b"
)
_CONFESSION_RE = re.compile(
    r"(?i)\b(i|we)\s+(was|were|am|are|have been|had been)\s+wrong\b|"
    r"\bconfession\b|"
    r"\bi\s+(used to|thought|assumed|missed|ignored|learned)\b"
)
_CONTRARIAN_RE = re.compile(
    r"(?i)\b(everyone|most people|teams|developers)\b.*\b(wrong|miss|overlook|think)\b|"
    r"\b(actually|instead|but)\b|"
    r"\b(unpopular opinion|counterintuitive|myth)\b"
)


@dataclass(frozen=True)
class ThreadHookExample:
    """One representative thread opening for a hook style."""

    content_id: int
    opening: str
    engagement_score: float
    published_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ThreadHookPerformanceRow:
    """Aggregated performance metrics for one thread hook style."""

    style: str
    count: int
    average_engagement: float
    resonance_rate: float
    resonated_count: int
    latest_published_at: str | None
    examples: tuple[ThreadHookExample, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class _ThreadSample:
    content_id: int
    opening: str
    style: str
    engagement_score: float
    resonated: bool
    published_at: str | None


def build_thread_hook_performance_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_count: int = DEFAULT_MIN_COUNT,
    examples: int = 0,
    now: datetime | None = None,
) -> list[ThreadHookPerformanceRow]:
    """Return ranked hook style performance for published X threads."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_count <= 0:
        raise ValueError("min_count must be positive")
    if examples < 0:
        raise ValueError("examples must be non-negative")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "generated_content" not in schema:
        return []

    observed_now = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = observed_now - timedelta(days=days)
    samples = [
        sample
        for sample in _thread_samples(conn, schema, cutoff=cutoff, now=observed_now)
        if sample.opening
    ]

    grouped: dict[str, list[_ThreadSample]] = defaultdict(list)
    for sample in samples:
        grouped[sample.style].append(sample)

    rows = [
        _performance_row(style, style_samples, examples)
        for style, style_samples in grouped.items()
        if len(style_samples) >= min_count
    ]
    rows.sort(
        key=lambda row: (
            -row.average_engagement,
            -row.resonance_rate,
            -row.count,
            row.style,
        )
    )
    return rows


def format_thread_hook_performance_json(
    rows: list[ThreadHookPerformanceRow],
) -> str:
    """Render hook performance rows as deterministic JSON."""
    return json.dumps([row.to_dict() for row in rows], indent=2, sort_keys=True)


def format_thread_hook_performance_table(
    rows: list[ThreadHookPerformanceRow],
    *,
    days: int,
    min_count: int,
) -> str:
    """Render hook performance rows as a compact table."""
    lines = [
        f"Thread Hook Performance (last {days} days)",
        f"styles={len(rows)} min_count={min_count}",
        "",
    ]
    if not rows:
        lines.append("No hook styles met the sample threshold.")
        return "\n".join(lines)

    lines.extend(
        [
            f"{'Style':15s}  {'Count':>5s}  {'Avg Eng':>7s}  {'Resonance':>9s}  {'Latest':19s}",
            f"{'-' * 15:15s}  {'-' * 5:>5s}  {'-' * 7:>7s}  {'-' * 9:>9s}  {'-' * 19:19s}",
        ]
    )
    for row in rows:
        lines.append(
            f"{row.style:15s}  "
            f"{row.count:5d}  "
            f"{row.average_engagement:7.2f}  "
            f"{row.resonance_rate:8.1%}  "
            f"{_clip(row.latest_published_at or '-', 19):19s}"
        )
        for example in row.examples:
            lines.append(
                f"{'':15s}  example #{example.content_id} "
                f"({example.engagement_score:.2f}): {_clip(example.opening, 90)}"
            )
    return "\n".join(lines)


def extract_thread_opening(content: str) -> str:
    """Extract the first post or opening line from stored thread content."""
    text = str(content or "").strip()
    if not text:
        return ""

    decoded = _decode_json(text)
    if decoded is not None:
        opening = _opening_from_json(decoded)
        if opening:
            return _normalize_opening(opening)

    parts = parse_thread_content(text)
    if parts:
        return _normalize_opening(parts[0])
    return _normalize_opening(text)


def classify_hook_style(opening: str) -> str:
    """Classify a thread opening with deterministic text heuristics."""
    normalized = _normalize_opening(opening)
    lowered = normalized.lower()
    if "?" in normalized:
        return "question"
    if _CONFESSION_RE.search(normalized):
        return "confession"
    if _BUILD_LOG_RE.search(normalized):
        return "build-log"
    if _CONTRARIAN_RE.search(normalized):
        return "contrarian"
    if _METRIC_RE.search(normalized) or re.search(r"\b\d+(?:[,.]\d+)*(?:\.\d+)?\b", lowered):
        return "metric-led"
    return "plain-summary"


def _thread_samples(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    now: datetime,
) -> list[_ThreadSample]:
    rows = _published_thread_rows(conn, schema, cutoff=cutoff, now=now)
    content_ids = {int(row["id"]) for row in rows}
    scores = _latest_x_engagement_scores(conn, schema, content_ids)
    samples: list[_ThreadSample] = []
    for row in rows:
        content_id = int(row["id"])
        opening = extract_thread_opening(str(row["content"] or ""))
        if not opening:
            continue
        samples.append(
            _ThreadSample(
                content_id=content_id,
                opening=opening,
                style=classify_hook_style(opening),
                engagement_score=round(scores.get(content_id, 0.0), 2),
                resonated=str(row["auto_quality"] or "") == "resonated",
                published_at=row["published_at"],
            )
        )
    return samples


def _published_thread_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    published_expr = (
        "COALESCE(MAX(cp.published_at), "
        + _value_expr(columns, "published_at", "gc")
        + ", "
        + _value_expr(columns, "created_at", "gc")
        + ")"
        if "content_publications" in schema
        else "COALESCE("
        + _value_expr(columns, "published_at", "gc")
        + ", "
        + _value_expr(columns, "created_at", "gc")
        + ")"
    )
    select_columns = [
        "gc.id",
        "gc.content",
        _value_expr(columns, "auto_quality", "gc") + " AS auto_quality",
        f"{published_expr} AS effective_published_at",
    ]
    filters = ["gc.content_type = 'x_thread'"]
    having = ["effective_published_at >= ?", "effective_published_at < ?"]
    params: list[Any] = [cutoff.isoformat(), now.isoformat()]

    if "content_publications" in schema:
        cp_join = (
            "LEFT JOIN content_publications cp "
            "ON cp.content_id = gc.id AND cp.platform = 'x' AND cp.status = 'published'"
        )
        legacy_published = (
            " OR COALESCE(gc.published, 0) = 1" if "published" in columns else ""
        )
        filters.append(f"(cp.id IS NOT NULL{legacy_published})")
    else:
        cp_join = ""
        if "published" in columns:
            filters.append("COALESCE(gc.published, 0) = 1")

    rows = conn.execute(
        f"""SELECT {", ".join(select_columns)}
            FROM generated_content gc
            {cp_join}
            WHERE {' AND '.join(filters)}
            GROUP BY gc.id
            HAVING {' AND '.join(having)}
            ORDER BY effective_published_at DESC, gc.id DESC""",
        tuple(params),
    ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["published_at"] = item.pop("effective_published_at")
        result.append(item)
    return result


def _latest_x_engagement_scores(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: set[int],
) -> dict[int, float]:
    if not content_ids or "post_engagement" not in schema:
        return {}
    columns = schema["post_engagement"]
    if not {"content_id", "engagement_score"}.issubset(columns):
        return {}
    order_column = "fetched_at" if "fetched_at" in columns else "created_at"
    id_order = ", id DESC" if "id" in columns else ""
    placeholders = ",".join("?" for _ in content_ids)
    rows = conn.execute(
        f"""SELECT content_id, engagement_score
            FROM (
                SELECT content_id, engagement_score,
                       ROW_NUMBER() OVER (
                           PARTITION BY content_id ORDER BY {order_column} DESC{id_order}
                       ) AS rn
                FROM post_engagement
                WHERE engagement_score IS NOT NULL
                  AND content_id IN ({placeholders})
            )
            WHERE rn = 1""",
        tuple(sorted(content_ids)),
    ).fetchall()
    return {
        int(row["content_id"]): float(row["engagement_score"] or 0.0)
        for row in rows
    }


def _performance_row(
    style: str,
    samples: list[_ThreadSample],
    example_count: int,
) -> ThreadHookPerformanceRow:
    count = len(samples)
    resonated_count = sum(1 for sample in samples if sample.resonated)
    total_engagement = sum(sample.engagement_score for sample in samples)
    ranked_examples = sorted(
        samples,
        key=lambda sample: (
            -sample.engagement_score,
            sample.published_at or "",
            sample.content_id,
        ),
    )
    return ThreadHookPerformanceRow(
        style=style,
        count=count,
        average_engagement=round(total_engagement / count if count else 0.0, 2),
        resonance_rate=round(resonated_count / count if count else 0.0, 4),
        resonated_count=resonated_count,
        latest_published_at=max(
            (sample.published_at for sample in samples if sample.published_at),
            default=None,
        ),
        examples=tuple(
            ThreadHookExample(
                content_id=sample.content_id,
                opening=sample.opening,
                engagement_score=sample.engagement_score,
                published_at=sample.published_at,
            )
            for sample in ranked_examples[:example_count]
        ),
    )


def _opening_from_json(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        for item in value:
            opening = _opening_from_json(item)
            if opening:
                return opening
        return ""
    if isinstance(value, dict):
        for key in (
            "first_post",
            "opening",
            "hook",
            "text",
            "content",
            "body",
        ):
            if key in value:
                opening = _opening_from_json(value[key])
                if opening:
                    return opening
        for key in ("thread", "tweets", "posts", "items", "parts"):
            if key in value:
                opening = _opening_from_json(value[key])
                if opening:
                    return opening
    return ""


def _decode_json(text: str) -> Any | None:
    stripped = text.strip()
    if not stripped or stripped[0] not in "[{":
        return None
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return None


def _normalize_opening(value: str) -> str:
    lines = [line.strip() for line in str(value or "").splitlines() if line.strip()]
    if not lines:
        return ""
    first = re.sub(r"(?i)^(?:tweet|post|thread)\s*\d*\s*[:.)-]\s*", "", lines[0])
    first = _NUMBER_RE.sub("", first)
    return _WHITESPACE_RE.sub(" ", first).strip()


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    return {
        row["name"] if isinstance(row, sqlite3.Row) else row[0]: {
            column[1] for column in conn.execute(
                f"PRAGMA table_info({row['name'] if isinstance(row, sqlite3.Row) else row[0]})"
            )
        }
        for row in tables
    }


def _value_expr(columns: set[str], column: str, alias: str) -> str:
    return f"{alias}.{column}" if column in columns else "NULL"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clip(value: str, width: int) -> str:
    text = str(value)
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)].rstrip() + "..."
