"""Report content variant selection gaps by generated content and platform."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30

BUCKET_MISSING_SELECTION = "missing_selection"
BUCKET_MULTIPLE_SELECTED = "multiple_selected"
BUCKET_STALE_UNSELECTED = "stale_unselected"
BUCKETS = (
    BUCKET_MISSING_SELECTION,
    BUCKET_MULTIPLE_SELECTED,
    BUCKET_STALE_UNSELECTED,
)

ACTION_SELECT_VARIANT = "select one variant before publishing"
ACTION_KEEP_ONE_SELECTED = "keep exactly one selected variant"
ACTION_REFRESH_OR_ARCHIVE = "refresh or archive stale unselected variants"


@dataclass(frozen=True)
class ContentVariantSelectionGap:
    """One content/platform variant selection issue."""

    bucket: str
    content_id: int
    platform: str
    variant_count: int
    selected_count: int
    recommended_action: str
    variant_ids: tuple[int, ...]
    selected_variant_ids: tuple[int, ...]
    stale_variant_ids: tuple[int, ...] = ()
    oldest_unselected_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket,
            "content_id": self.content_id,
            "oldest_unselected_at": self.oldest_unselected_at,
            "platform": self.platform,
            "recommended_action": self.recommended_action,
            "selected_count": self.selected_count,
            "selected_variant_ids": list(self.selected_variant_ids),
            "stale_variant_ids": list(self.stale_variant_ids),
            "variant_count": self.variant_count,
            "variant_ids": list(self.variant_ids),
        }


def build_content_variant_selection_gap_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a deterministic read-only report of variant selection gaps."""
    if days <= 0:
        raise ValueError("days must be positive")
    if platform is not None and not platform.strip():
        raise ValueError("platform must not be blank")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_aware(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)

    findings: list[ContentVariantSelectionGap] = []
    if {"generated_content", "content_variants"}.issubset(schema):
        findings = _selection_gap_findings(
            conn,
            schema,
            cutoff=cutoff,
            platform=platform,
        )

    return {
        "artifact_type": "content_variant_selection_gaps",
        "findings": [finding.to_dict() for finding in findings],
        "generated_at": generated_at.isoformat(),
        "platform": platform,
        "totals": _totals(findings),
        "window_days": days,
    }


def format_content_variant_selection_gaps_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_variant_selection_gaps_text(report: dict[str, Any]) -> str:
    """Render the report as stable human-readable text."""
    findings = report["findings"]
    if not findings:
        return (
            "Content Variant Selection Gaps\n"
            f"Window: {report['window_days']} days\n"
            "No content variant selection gaps found."
        )

    totals = report["totals"]["all"]
    lines = [
        "Content Variant Selection Gaps",
        (
            f"Window: {report['window_days']} days "
            f"missing_selection={totals[BUCKET_MISSING_SELECTION]} "
            f"multiple_selected={totals[BUCKET_MULTIPLE_SELECTED]} "
            f"stale_unselected={totals[BUCKET_STALE_UNSELECTED]}"
        ),
        "",
        "Findings",
    ]
    for item in findings:
        lines.append(
            "  - "
            f"content_id={item['content_id']} platform={item['platform']} "
            f"bucket={item['bucket']} variant_count={item['variant_count']} "
            f"selected_count={item['selected_count']} "
            f"action={item['recommended_action']}"
        )
        if item["stale_variant_ids"]:
            lines.append(
                "    "
                f"stale_variant_ids={','.join(str(value) for value in item['stale_variant_ids'])} "
                f"oldest_unselected_at={item['oldest_unselected_at']}"
            )
    return "\n".join(lines)


def _selection_gap_findings(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    platform: str | None,
) -> list[ContentVariantSelectionGap]:
    rows = _variant_rows(conn, schema, platform=platform)
    grouped: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((int(row["content_id"]), str(row["platform"])), []).append(row)

    findings: list[ContentVariantSelectionGap] = []
    for (content_id, platform_name), variants in grouped.items():
        selected = [row for row in variants if int(row["selected"] or 0) == 1]
        base = {
            "content_id": content_id,
            "platform": platform_name,
            "variant_count": len(variants),
            "selected_count": len(selected),
            "variant_ids": tuple(int(row["variant_id"]) for row in variants),
            "selected_variant_ids": tuple(int(row["variant_id"]) for row in selected),
        }
        if not selected:
            findings.append(
                ContentVariantSelectionGap(
                    bucket=BUCKET_MISSING_SELECTION,
                    recommended_action=ACTION_SELECT_VARIANT,
                    **base,
                )
            )
        elif len(selected) > 1:
            findings.append(
                ContentVariantSelectionGap(
                    bucket=BUCKET_MULTIPLE_SELECTED,
                    recommended_action=ACTION_KEEP_ONE_SELECTED,
                    **base,
                )
            )

        stale_rows = [
            row
            for row in variants
            if int(row["selected"] or 0) != 1
            and _is_stale(row.get("created_at"), cutoff)
        ]
        if stale_rows:
            oldest = min(
                (_parse_timestamp(row.get("created_at")) for row in stale_rows),
                default=None,
            )
            findings.append(
                ContentVariantSelectionGap(
                    bucket=BUCKET_STALE_UNSELECTED,
                    recommended_action=ACTION_REFRESH_OR_ARCHIVE,
                    stale_variant_ids=tuple(int(row["variant_id"]) for row in stale_rows),
                    oldest_unselected_at=oldest.isoformat() if oldest else None,
                    **base,
                )
            )

    return sorted(
        findings,
        key=lambda item: (
            item.platform,
            item.content_id,
            BUCKETS.index(item.bucket),
            item.variant_ids,
        ),
    )


def _variant_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    platform: str | None,
) -> list[dict[str, Any]]:
    cv = schema["content_variants"]
    required = {"id", "content_id", "platform", "variant_type"}
    if not required.issubset(cv):
        return []
    selected_expr = "cv.selected" if "selected" in cv else "0"
    created_expr = "cv.created_at" if "created_at" in cv else "NULL"
    filters = []
    params: list[Any] = []
    if platform is not None:
        filters.append("cv.platform = ?")
        params.append(platform)
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = conn.execute(
        f"""SELECT cv.id AS variant_id,
                  cv.content_id,
                  cv.platform,
                  cv.variant_type,
                  {selected_expr} AS selected,
                  {created_expr} AS created_at
           FROM content_variants cv
           INNER JOIN generated_content gc ON gc.id = cv.content_id
           {where}
           ORDER BY cv.platform ASC, cv.content_id ASC, cv.variant_type ASC, cv.id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _totals(findings: list[ContentVariantSelectionGap]) -> dict[str, Any]:
    by_platform: dict[str, dict[str, int]] = {}
    all_counts = _empty_bucket_counts()
    for finding in findings:
        platform_counts = by_platform.setdefault(finding.platform, _empty_bucket_counts())
        platform_counts[finding.bucket] += 1
        all_counts[finding.bucket] += 1
    return {
        "all": all_counts,
        "by_platform": {
            platform: by_platform[platform]
            for platform in sorted(by_platform)
        },
    }


def _empty_bucket_counts() -> dict[str, int]:
    return {bucket: 0 for bucket in BUCKETS}


def _is_stale(value: Any, cutoff: datetime) -> bool:
    parsed = _parse_timestamp(value)
    return parsed is not None and parsed < cutoff


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_aware(value)
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _ensure_aware(parsed)


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or Database-like object with conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    return {
        str(row["name"]): {
            str(column["name"])
            for column in conn.execute(f"PRAGMA table_info({row['name']})").fetchall()
        }
        for row in tables
    }
