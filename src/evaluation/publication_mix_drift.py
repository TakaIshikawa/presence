"""Published content mix drift reporting."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any


DIMENSIONS = ("content_type", "content_format", "platform")


def build_publication_mix_drift_report(
    db_or_conn: Any,
    *,
    recent_days: int = 7,
    baseline_days: int = 7,
    drift_warning_points: float = 20.0,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only drift report for successfully published content."""
    if recent_days <= 0:
        raise ValueError("recent_days must be positive")
    if baseline_days <= 0:
        raise ValueError("baseline_days must be positive")
    if drift_warning_points < 0:
        raise ValueError("drift_warning_points must be non-negative")

    conn = _connection(db_or_conn)
    now = _aware(now or datetime.now(timezone.utc))
    recent_start = now - timedelta(days=recent_days)
    baseline_start = recent_start - timedelta(days=baseline_days)
    schema = _schema(conn)

    rows = _published_rows(conn, schema, baseline_start, now)
    recent_rows = [
        row for row in rows if recent_start <= _parse_timestamp(row["published_at"]) < now
    ]
    baseline_rows = [
        row
        for row in rows
        if baseline_start <= _parse_timestamp(row["published_at"]) < recent_start
    ]

    dimensions = {
        dimension: _dimension_entries(
            dimension,
            recent_rows,
            baseline_rows,
            drift_warning_points,
        )
        for dimension in DIMENSIONS
    }
    warnings = [
        warning
        for entries in dimensions.values()
        for entry in entries
        for warning in entry["warnings"]
    ]
    warnings.sort(
        key=lambda warning: (
            warning["dimension"],
            warning["label"],
            -abs(warning["drift_points"]),
            warning["value"],
        )
    )

    return {
        "generated_at": now.isoformat(),
        "recent_days": recent_days,
        "baseline_days": baseline_days,
        "windows": {
            "recent": {
                "start": recent_start.isoformat(),
                "end": now.isoformat(),
            },
            "baseline": {
                "start": baseline_start.isoformat(),
                "end": recent_start.isoformat(),
            },
        },
        "thresholds": {
            "drift_warning_points": drift_warning_points,
        },
        "totals": {
            "recent": len(recent_rows),
            "baseline": len(baseline_rows),
        },
        "dimensions": dimensions,
        "warnings": warnings,
    }


def format_publication_mix_drift_json(report: dict[str, Any]) -> str:
    """Render a publication mix drift report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_mix_drift_text(report: dict[str, Any]) -> str:
    """Render a stable human-readable publication mix drift report."""
    lines = [
        "Publication content mix drift report",
        f"Generated: {report['generated_at']}",
        (
            "Windows: recent="
            f"{report['recent_days']} days, baseline={report['baseline_days']} days"
        ),
        f"Drift warning threshold: {report['thresholds']['drift_warning_points']} points",
        f"Totals: recent={report['totals']['recent']} baseline={report['totals']['baseline']}",
        "",
    ]

    if report["totals"]["recent"] == 0 and report["totals"]["baseline"] == 0:
        lines.append("No published content found in either window.")
        return "\n".join(lines)

    for dimension in DIMENSIONS:
        lines.append(dimension)
        columns = [
            ("value", "VALUE", 18),
            ("recent_count", "RECENT", 6),
            ("recent_share", "R_SHARE", 8),
            ("baseline_count", "BASE", 6),
            ("baseline_share", "B_SHARE", 8),
            ("drift_points", "DRIFT", 7),
            ("warning_labels", "WARNINGS", 30),
        ]
        lines.append("  ".join(label.ljust(width) for _, label, width in columns))
        lines.append("  ".join("-" * width for _, _, width in columns))
        entries = report["dimensions"][dimension]
        if not entries:
            lines.append("No rows.")
        for entry in entries:
            rendered = {
                **entry,
                "recent_share": _format_percent(entry["recent_share"]),
                "baseline_share": _format_percent(entry["baseline_share"]),
                "drift_points": _format_points(entry["drift_points"]),
                "warning_labels": ",".join(
                    warning["label"] for warning in entry["warnings"]
                ),
            }
            lines.append(
                "  ".join(
                    _format_cell(rendered.get(key), width).ljust(width)
                    for key, _, width in columns
                )
            )
        lines.append("")

    lines.append("Warnings:")
    if not report["warnings"]:
        lines.append("No content mix drift warnings.")
    else:
        for warning in report["warnings"]:
            lines.append(
                "- "
                f"{warning['label']} {warning['dimension']}={warning['value']} "
                f"drift={_format_points(warning['drift_points'])} "
                f"recent={warning['recent_count']} baseline={warning['baseline_count']}"
            )
    return "\n".join(lines).rstrip()


def _dimension_entries(
    dimension: str,
    recent_rows: list[dict[str, Any]],
    baseline_rows: list[dict[str, Any]],
    drift_warning_points: float,
) -> list[dict[str, Any]]:
    recent_counts = _counts(recent_rows, dimension)
    baseline_counts = _counts(baseline_rows, dimension)
    recent_total = len(recent_rows)
    baseline_total = len(baseline_rows)
    values = sorted(set(recent_counts) | set(baseline_counts))

    entries: list[dict[str, Any]] = []
    for value in values:
        recent_count = recent_counts.get(value, 0)
        baseline_count = baseline_counts.get(value, 0)
        recent_share = recent_count / recent_total if recent_total else 0.0
        baseline_share = baseline_count / baseline_total if baseline_total else 0.0
        drift_points = round((recent_share - baseline_share) * 100, 2)
        entry = {
            "value": value,
            "recent_count": recent_count,
            "recent_share": round(recent_share, 4),
            "baseline_count": baseline_count,
            "baseline_share": round(baseline_share, 4),
            "drift_points": drift_points,
            "warnings": [],
        }
        entry["warnings"] = _warnings_for_entry(
            dimension,
            value,
            entry,
            drift_warning_points,
        )
        entries.append(entry)

    entries.sort(
        key=lambda entry: (
            not entry["warnings"],
            -abs(entry["drift_points"]),
            -entry["recent_count"],
            entry["value"],
        )
    )
    return entries


def _warnings_for_entry(
    dimension: str,
    value: str,
    entry: dict[str, Any],
    drift_warning_points: float,
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    if entry["drift_points"] >= drift_warning_points and entry["recent_count"] > 0:
        warnings.append(_warning("high_positive_drift", dimension, value, entry))
    if (
        dimension == "content_type"
        and entry["recent_count"] == 0
        and entry["baseline_count"] > 0
        and entry["baseline_share"] * 100 >= drift_warning_points
    ):
        warnings.append(_warning("missing_recent_type", dimension, value, entry))
    return warnings


def _warning(
    label: str,
    dimension: str,
    value: str,
    entry: dict[str, Any],
) -> dict[str, Any]:
    return {
        "label": label,
        "dimension": dimension,
        "value": value,
        "recent_count": entry["recent_count"],
        "recent_share": entry["recent_share"],
        "baseline_count": entry["baseline_count"],
        "baseline_share": entry["baseline_share"],
        "drift_points": entry["drift_points"],
    }


def _published_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    baseline_start: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    if not _required_schema(schema):
        return []

    filters = ["cp.published_at >= ?", "cp.published_at < ?"]
    params: list[Any] = [baseline_start.isoformat(), now.isoformat()]
    if "status" in schema["content_publications"]:
        filters.append("cp.status = 'published'")

    rows = conn.execute(
        f"""SELECT
                  gc.content_type AS content_type,
                  gc.content_format AS content_format,
                  cp.platform AS platform,
                  cp.published_at AS published_at
              FROM content_publications cp
              INNER JOIN generated_content gc ON gc.id = cp.content_id
              WHERE {' AND '.join(filters)}
              ORDER BY cp.published_at ASC, cp.id ASC""",
        params,
    ).fetchall()
    return [
        {
            "content_type": _bucket(row["content_type"]),
            "content_format": _bucket(row["content_format"]),
            "platform": _bucket(row["platform"]),
            "published_at": row["published_at"],
        }
        for row in rows
        if _parse_timestamp(row["published_at"]) is not None
    ]


def _required_schema(schema: dict[str, set[str]]) -> bool:
    return (
        {"id", "content_type", "content_format"}.issubset(
            schema.get("generated_content", set())
        )
        and {"content_id", "platform", "published_at"}.issubset(
            schema.get("content_publications", set())
        )
    )


def _counts(rows: list[dict[str, Any]], dimension: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = row[dimension]
        counts[value] = counts.get(value, 0) + 1
    return counts


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _bucket(value: Any) -> str:
    text = str(value).strip() if value is not None else ""
    return text or "unknown"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _format_cell(value: Any, width: int) -> str:
    text = "" if value is None else str(value)
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"


def _format_points(value: float) -> str:
    return f"{value:+.1f}pt"
