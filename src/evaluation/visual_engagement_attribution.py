"""Visual asset engagement attribution reporting."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_MIN_SAMPLE = 3
ENGAGEMENT_TABLES = {
    "x": "post_engagement",
    "twitter": "post_engagement",
    "linkedin": "linkedin_engagement",
    "bluesky": "bluesky_engagement",
    "mastodon": "mastodon_engagement",
}
VISUAL_CONTENT_TYPES = {"x_visual", "visual"}


@dataclass(frozen=True)
class VisualEngagementCohort:
    """One visual/non-visual comparison row."""

    group_by: str
    platform: str | None
    content_type: str | None
    template: str | None
    image_prompt_group: str | None
    age_bucket: str | None
    visual_sample_count: int
    non_visual_sample_count: int
    visual_normalized_engagement_rate: float
    non_visual_normalized_engagement_rate: float
    engagement_delta: float
    status: str
    visual_content_ids: tuple[int, ...]
    non_visual_content_ids: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["visual_content_ids"] = list(self.visual_content_ids)
        data["non_visual_content_ids"] = list(self.non_visual_content_ids)
        return data


@dataclass(frozen=True)
class VisualEngagementAttributionReport:
    """Visual engagement attribution report plus applied filters."""

    generated_at: str
    days: int
    platform: str | None
    min_sample: int
    missing_optional_tables: tuple[str, ...]
    missing_metadata_columns: tuple[str, ...]
    totals: dict[str, Any]
    cohorts: tuple[VisualEngagementCohort, ...]
    recommendations: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "days": self.days,
            "platform": self.platform,
            "min_sample": self.min_sample,
            "missing_optional_tables": list(self.missing_optional_tables),
            "missing_metadata_columns": list(self.missing_metadata_columns),
            "totals": self.totals,
            "cohorts": [cohort.to_dict() for cohort in self.cohorts],
            "recommendations": list(self.recommendations),
        }


def build_visual_engagement_attribution_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str | None = None,
    min_sample: int = DEFAULT_MIN_SAMPLE,
    now: datetime | None = None,
) -> VisualEngagementAttributionReport:
    """Build a read-only report comparing visual and non-visual engagement."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_sample <= 0:
        raise ValueError("min_sample must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    now = _ensure_aware(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days)
    platform_filter = _platform_label(platform) if _clean_label(platform) else None

    entries = _load_entries(conn, schema, cutoff=cutoff, now=now, platform=platform_filter)
    cohorts = _build_cohorts(entries, min_sample=min_sample)
    totals = _build_totals(entries)
    recommendations = _recommendations(cohorts, totals)

    optional_tables = ("content_publications", *sorted(set(ENGAGEMENT_TABLES.values())))
    metadata_columns = ("image_path", "image_prompt", "template")
    gc_columns = schema.get("generated_content", set())

    return VisualEngagementAttributionReport(
        generated_at=now.isoformat(),
        days=days,
        platform=platform_filter,
        min_sample=min_sample,
        missing_optional_tables=tuple(table for table in optional_tables if table not in schema),
        missing_metadata_columns=tuple(column for column in metadata_columns if column not in gc_columns),
        totals=totals,
        cohorts=tuple(cohorts),
        recommendations=tuple(recommendations),
    )


def format_visual_engagement_attribution_json(
    report: VisualEngagementAttributionReport,
) -> str:
    """Render the attribution report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_visual_engagement_attribution_text(
    report: VisualEngagementAttributionReport,
) -> str:
    """Render a stable operator-facing text report."""
    lines = [
        "Visual Engagement Attribution",
        f"Generated: {report.generated_at}",
        f"Window: {report.days} days",
        f"Minimum sample: {report.min_sample}",
    ]
    if report.platform:
        lines.append(f"Platform: {report.platform}")
    if report.missing_optional_tables:
        lines.append(f"Missing optional tables: {', '.join(report.missing_optional_tables)}")
    if report.missing_metadata_columns:
        lines.append(f"Missing metadata columns: {', '.join(report.missing_metadata_columns)}")
    lines.append("")

    totals = report.totals
    lines.append(
        "Totals: "
        f"visual={totals['visual_sample_count']} "
        f"non_visual={totals['non_visual_sample_count']} "
        f"delta={totals['engagement_delta']:+.2f}"
    )
    lines.append("")

    if not report.cohorts:
        lines.append("No published content with engagement context found.")
        return "\n".join(lines)

    header = (
        f"{'Group':<30} {'Visual':>6} {'Text':>6} "
        f"{'V rate':>8} {'T rate':>8} {'Delta':>8} {'Status':<20}"
    )
    lines.extend([header, "-" * len(header)])
    for cohort in report.cohorts:
        lines.append(
            f"{_cohort_label(cohort):<30} "
            f"{cohort.visual_sample_count:>6} "
            f"{cohort.non_visual_sample_count:>6} "
            f"{cohort.visual_normalized_engagement_rate:>8.2f} "
            f"{cohort.non_visual_normalized_engagement_rate:>8.2f} "
            f"{cohort.engagement_delta:>+8.2f} "
            f"{cohort.status:<20}"
        )
    if report.recommendations:
        lines.extend(["", "Recommendations:"])
        lines.extend(f"- {item}" for item in report.recommendations)
    return "\n".join(lines)


def _load_entries(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    now: datetime,
    platform: str | None,
) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []

    content_rows = _content_rows(conn, schema)
    publications = _publication_rows(conn, schema, content_rows)
    scores = _engagement_scores(conn, schema)
    entries: list[dict[str, Any]] = []
    for content in content_rows:
        content_id = int(content["id"])
        for publication in publications.get(content_id, []):
            entry_platform = _platform_label(publication["platform"])
            if platform and entry_platform != platform:
                continue
            published_at = (
                _parse_timestamp(publication.get("published_at"))
                or _parse_timestamp(content.get("published_at"))
                or _parse_timestamp(content.get("created_at"))
                or now
            )
            if published_at < cutoff or published_at > now:
                continue
            score = scores.get((entry_platform, content_id), 0.0)
            age_days = max((now - published_at).total_seconds() / 86400, 0.0)
            entries.append(
                {
                    "content_id": content_id,
                    "platform": entry_platform,
                    "content_type": _value_label(content.get("content_type"), "unknown"),
                    "template": _template_value(content),
                    "image_prompt_group": _image_prompt_group(content.get("image_prompt")),
                    "age_bucket": _age_bucket(age_days),
                    "is_visual": _is_visual(content),
                    "engagement_score": score,
                    "published_at": published_at,
                }
            )
    entries.sort(key=lambda item: (item["platform"], item["content_type"], item["content_id"]))
    return entries


def _content_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    select_columns = [
        _column_expr(columns, "id"),
        _column_expr(columns, "content_type"),
        _column_expr(columns, "published"),
        _column_expr(columns, "published_at"),
        _column_expr(columns, "created_at"),
        _column_expr(columns, "image_path"),
        _column_expr(columns, "image_prompt"),
        _column_expr(columns, "template"),
        _column_expr(columns, "prompt_type"),
        _column_expr(columns, "content_format"),
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
              FROM generated_content
             ORDER BY id ASC"""
    ).fetchall()
    return [dict(row) for row in rows if _int_or_none(row["id"]) is not None]


def _publication_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_rows: list[dict[str, Any]],
) -> dict[int, list[dict[str, Any]]]:
    publications: dict[int, list[dict[str, Any]]] = {}
    if "content_publications" in schema and "content_id" in schema["content_publications"]:
        columns = schema["content_publications"]
        select_columns = [
            _column_expr(columns, "content_id"),
            _column_expr(columns, "platform", "'x'"),
            _column_expr(columns, "status"),
            _column_expr(columns, "published_at"),
        ]
        rows = conn.execute(
            f"""SELECT {', '.join(select_columns)}
                  FROM content_publications
                 ORDER BY content_id ASC, platform ASC"""
        ).fetchall()
        for row in rows:
            content_id = _int_or_none(row["content_id"])
            if content_id is None or _status(row["status"]) != "published":
                continue
            publications.setdefault(content_id, []).append(dict(row))

    for content in content_rows:
        content_id = int(content["id"])
        if publications.get(content_id):
            continue
        if content.get("published") == 1 or _has_text(content.get("published_at")):
            publications.setdefault(content_id, []).append(
                {
                    "content_id": content_id,
                    "platform": "x",
                    "status": "published",
                    "published_at": content.get("published_at"),
                }
            )
    return publications


def _engagement_scores(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[tuple[str, int], float]:
    scores: dict[tuple[str, int], float] = {}
    for platform, table in ENGAGEMENT_TABLES.items():
        if table not in schema or "content_id" not in schema[table] or "engagement_score" not in schema[table]:
            continue
        for content_id, score in _latest_scores(conn, schema, table).items():
            key = (_platform_label(platform), content_id)
            scores[key] = max(scores.get(key, 0.0), score)
    return scores


def _latest_scores(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    table: str,
) -> dict[int, float]:
    order_column = "fetched_at" if "fetched_at" in schema[table] else "created_at"
    if order_column not in schema[table]:
        order_column = "id"
    rows = conn.execute(
        f"""SELECT content_id, engagement_score
              FROM (
                    SELECT content_id, engagement_score,
                           ROW_NUMBER() OVER (
                               PARTITION BY content_id ORDER BY {order_column} DESC, id DESC
                           ) AS rn
                      FROM {table}
                     WHERE engagement_score IS NOT NULL
                   )
             WHERE rn = 1"""
    ).fetchall()
    return {int(row["content_id"]): float(row["engagement_score"] or 0.0) for row in rows}


def _build_cohorts(
    entries: list[dict[str, Any]],
    *,
    min_sample: int,
) -> list[VisualEngagementCohort]:
    cohorts: list[VisualEngagementCohort] = []
    groupers = [
        ("overall", lambda item: (None, None, None, None, None)),
        ("platform", lambda item: (item["platform"], None, None, None, None)),
        ("content_type", lambda item: (None, item["content_type"], None, None, None)),
        (
            "platform-content_type",
            lambda item: (item["platform"], item["content_type"], None, None, None),
        ),
        (
            "platform-content_type-age_bucket",
            lambda item: (
                item["platform"],
                item["content_type"],
                None,
                None,
                item["age_bucket"],
            ),
        ),
    ]
    if any(item.get("template") for item in entries):
        groupers.append(
            (
                "template",
                lambda item: (
                    item["platform"],
                    item["content_type"],
                    item.get("template"),
                    None,
                    item["age_bucket"],
                ),
            )
        )

    for group_by, key_func in groupers:
        grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for entry in entries:
            key = key_func(entry)
            if group_by == "template" and key[2] is None:
                continue
            grouped.setdefault(key, []).append(entry)
        for key, rows in grouped.items():
            cohorts.append(_cohort_from_rows(group_by, key, rows, min_sample=min_sample))

    if any(item.get("image_prompt_group") for item in entries):
        cohorts.extend(_image_prompt_cohorts(entries, min_sample=min_sample))

    cohorts.sort(
        key=lambda cohort: (
            cohort.group_by,
            cohort.platform or "",
            cohort.content_type or "",
            cohort.template or "",
            cohort.image_prompt_group or "",
            cohort.age_bucket or "",
        )
    )
    return cohorts


def _image_prompt_cohorts(
    entries: list[dict[str, Any]],
    *,
    min_sample: int,
) -> list[VisualEngagementCohort]:
    visual_groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    baselines: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for entry in entries:
        base_key = (entry["platform"], entry["content_type"], entry["age_bucket"])
        if entry["is_visual"] and entry.get("image_prompt_group"):
            visual_groups.setdefault((*base_key, entry["image_prompt_group"]), []).append(entry)
        if not entry["is_visual"]:
            baselines.setdefault(base_key, []).append(entry)

    cohorts: list[VisualEngagementCohort] = []
    for key, visual_rows in visual_groups.items():
        platform, content_type, age_bucket, prompt_group = key
        rows = [*visual_rows, *baselines.get((platform, content_type, age_bucket), [])]
        cohorts.append(
            _cohort_from_rows(
                "image_prompt",
                (platform, content_type, None, prompt_group, age_bucket),
                rows,
                min_sample=min_sample,
            )
        )
    return cohorts


def _cohort_from_rows(
    group_by: str,
    key: tuple[Any, ...],
    rows: list[dict[str, Any]],
    *,
    min_sample: int,
) -> VisualEngagementCohort:
    visual = [row for row in rows if row["is_visual"]]
    non_visual = [row for row in rows if not row["is_visual"]]
    visual_rate = _normalized_rate(visual)
    non_visual_rate = _normalized_rate(non_visual)
    status = (
        "sufficient_sample"
        if len(visual) >= min_sample and len(non_visual) >= min_sample
        else "insufficient_sample"
    )
    return VisualEngagementCohort(
        group_by=group_by,
        platform=key[0],
        content_type=key[1],
        template=key[2],
        image_prompt_group=key[3],
        age_bucket=key[4],
        visual_sample_count=len(visual),
        non_visual_sample_count=len(non_visual),
        visual_normalized_engagement_rate=visual_rate,
        non_visual_normalized_engagement_rate=non_visual_rate,
        engagement_delta=round(visual_rate - non_visual_rate, 3),
        status=status,
        visual_content_ids=tuple(sorted(int(row["content_id"]) for row in visual)),
        non_visual_content_ids=tuple(sorted(int(row["content_id"]) for row in non_visual)),
    )


def _build_totals(entries: list[dict[str, Any]]) -> dict[str, Any]:
    visual = [entry for entry in entries if entry["is_visual"]]
    non_visual = [entry for entry in entries if not entry["is_visual"]]
    visual_rate = _normalized_rate(visual)
    non_visual_rate = _normalized_rate(non_visual)
    return {
        "sample_count": len(entries),
        "visual_sample_count": len(visual),
        "non_visual_sample_count": len(non_visual),
        "visual_normalized_engagement_rate": visual_rate,
        "non_visual_normalized_engagement_rate": non_visual_rate,
        "engagement_delta": round(visual_rate - non_visual_rate, 3),
        "platforms": sorted({entry["platform"] for entry in entries}),
        "content_types": sorted({entry["content_type"] for entry in entries}),
    }


def _recommendations(
    cohorts: list[VisualEngagementCohort],
    totals: dict[str, Any],
) -> list[str]:
    sufficient = [cohort for cohort in cohorts if cohort.status == "sufficient_sample"]
    if not totals["sample_count"]:
        return ["No published content matched the selected window."]
    if not sufficient:
        return ["Collect more paired visual and non-visual samples before changing visual strategy."]

    best = max(sufficient, key=lambda item: (item.engagement_delta, item.visual_sample_count, _cohort_label(item)))
    worst = min(sufficient, key=lambda item: (item.engagement_delta, -item.visual_sample_count, _cohort_label(item)))
    recommendations = []
    if best.engagement_delta > 0:
        recommendations.append(
            f"Lean into visuals for {_cohort_label(best)}; normalized engagement is "
            f"{best.engagement_delta:.2f} higher than the non-visual cohort."
        )
    if worst.engagement_delta < 0:
        recommendations.append(
            f"Review visual usage for {_cohort_label(worst)}; normalized engagement is "
            f"{abs(worst.engagement_delta):.2f} lower than the non-visual cohort."
        )
    return recommendations or ["Visual and non-visual cohorts are roughly even in sufficient samples."]


def _normalized_rate(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    return round(sum(float(row["engagement_score"] or 0.0) for row in rows) / len(rows), 3)


def _is_visual(row: dict[str, Any]) -> bool:
    content_type = _status(row.get("content_type"))
    return content_type in VISUAL_CONTENT_TYPES or _has_text(row.get("image_path")) or _has_text(row.get("image_prompt"))


def _template_value(row: dict[str, Any]) -> str | None:
    for key in ("template", "prompt_type", "content_format"):
        value = _clean_label(row.get(key))
        if value:
            return value
    return None


def _image_prompt_group(value: Any) -> str | None:
    text = _clean_label(value)
    if not text:
        return None
    if "|" in text:
        text = text.split("|", 1)[0].strip()
    elif ":" in text:
        text = text.split(":", 1)[0].strip()
    words = text.split()
    if len(words) > 6:
        text = " ".join(words[:6])
    return text.lower() or None


def _age_bucket(age_days: float) -> str:
    if age_days < 3:
        return "0-2d"
    if age_days < 8:
        return "3-7d"
    if age_days < 31:
        return "8-30d"
    return "31d+"


def _cohort_label(cohort: VisualEngagementCohort) -> str:
    parts = [cohort.group_by]
    if cohort.platform:
        parts.append(cohort.platform)
    if cohort.content_type:
        parts.append(cohort.content_type)
    if cohort.template:
        parts.append(f"template={cohort.template}")
    if cohort.image_prompt_group:
        parts.append(f"prompt={cohort.image_prompt_group}")
    if cohort.age_bucket:
        parts.append(cohort.age_bucket)
    return _shorten(" / ".join(parts), 30)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if conn is None:
        raise ValueError("database connection is not available")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }
    return {
        table: {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
    }


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    return f"{column} AS {column}" if column in columns else f"{default} AS {column}"


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_aware(parsed)


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _status(value: Any) -> str:
    return str(value or "").strip().lower()


def _platform_label(value: Any) -> str:
    text = _status(value)
    return "x" if text in {"twitter", ""} else text


def _value_label(value: Any, default: str) -> str:
    text = _clean_label(value)
    return text or default


def _clean_label(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _has_text(value: Any) -> bool:
    return bool(_clean_label(value))


def _shorten(value: str, max_length: int) -> str:
    if len(value) <= max_length:
        return value
    return value[: max_length - 3] + "..."
