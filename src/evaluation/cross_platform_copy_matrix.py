"""Cross-platform copy matrix reporting."""

from __future__ import annotations

import json
import re
import sqlite3
from typing import Any, Iterable

from output.platform_adapter import count_graphemes


DEFAULT_PLATFORMS = ("x", "bluesky", "linkedin", "newsletter", "blog")

_HASHTAG_RE = re.compile(r"(?<!\w)#[A-Za-z][A-Za-z0-9_]*")
_URL_RE = re.compile(r"https?://[^\s<>()]+")


def build_cross_platform_copy_matrix(
    content_rows: Iterable[dict[str, Any]],
    variant_rows: Iterable[dict[str, Any]],
    *,
    platforms: Iterable[str] = DEFAULT_PLATFORMS,
    campaign: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a read-only selected-variant matrix from content and variant rows."""
    selected_platforms = tuple(_normalize_platforms(platforms))
    variants_by_content_platform = _variants_by_content_platform(variant_rows)
    rows = [
        _matrix_row(dict(content), variants_by_content_platform, selected_platforms)
        for content in sorted(
            (dict(row) for row in content_rows),
            key=lambda row: (str(row.get("created_at") or ""), int(row.get("id") or 0)),
        )
    ]
    gaps = [
        {
            "content_id": row["content_id"],
            "platform": platform,
            "reason": entry["gap_reason"],
        }
        for row in rows
        for platform, entry in row["platforms"].items()
        if entry["gap"]
    ]

    return {
        "campaign": campaign,
        "platforms": list(selected_platforms),
        "totals": {
            "content_items": len(rows),
            "platforms": len(selected_platforms),
            "gaps": len(gaps),
        },
        "rows": rows,
        "gaps": gaps,
    }


def build_cross_platform_copy_matrix_report(
    db_or_conn: Any,
    *,
    campaign: str | int | None = None,
    content_ids: Iterable[int] | None = None,
    platforms: Iterable[str] = DEFAULT_PLATFORMS,
) -> dict[str, Any]:
    """Load campaign or content rows and build the cross-platform copy matrix."""
    selected_platforms = tuple(_normalize_platforms(platforms))
    normalized_content_ids = _normalize_content_ids(content_ids)
    if campaign is None and not normalized_content_ids:
        raise ValueError("either --campaign or --content-id is required")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "generated_content" not in schema:
        return build_cross_platform_copy_matrix(
            [],
            [],
            platforms=selected_platforms,
            campaign=None,
        )

    campaign_row = _resolve_campaign(conn, schema, campaign)
    content_rows = _content_rows(
        conn,
        schema,
        campaign_id=campaign_row["id"] if campaign_row else None,
        content_ids=normalized_content_ids,
    )
    variant_rows = _variant_rows(
        conn,
        schema,
        content_ids=[int(row["id"]) for row in content_rows],
        platforms=selected_platforms,
    )
    return build_cross_platform_copy_matrix(
        content_rows,
        variant_rows,
        platforms=selected_platforms,
        campaign=campaign_row,
    )


def format_cross_platform_copy_matrix_json(report: dict[str, Any]) -> str:
    """Render a cross-platform copy matrix as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_cross_platform_copy_matrix_markdown(report: dict[str, Any]) -> str:
    """Render a compact Markdown matrix for campaign copy review."""
    title = "Cross-platform copy matrix"
    campaign = report.get("campaign")
    lines = [f"# {title}"]
    if campaign:
        lines.append(f"Campaign: {campaign.get('name')} (ID {campaign.get('id')})")
    lines.append(
        "Totals: "
        f"content_items={report['totals']['content_items']} "
        f"platforms={report['totals']['platforms']} "
        f"gaps={report['totals']['gaps']}"
    )
    lines.append("")

    platforms = list(report["platforms"])
    headers = [
        "Content ID",
        "Type",
        "Source",
        *[platform.upper() for platform in platforms],
        "Gaps",
    ]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for row in report["rows"]:
        cells = [
            str(row["content_id"]),
            _md(row.get("content_type") or ""),
            _variant_summary(row["source"]),
        ]
        cells.extend(
            _variant_summary(row["platforms"][platform]) for platform in platforms
        )
        gaps = [
            f"{platform}:{row['platforms'][platform]['gap_reason']}"
            for platform in platforms
            if row["platforms"][platform]["gap"]
        ]
        cells.append(_md(", ".join(gaps) if gaps else "-"))
        lines.append("| " + " | ".join(cells) + " |")

    if not report["rows"]:
        lines.extend(["", "No generated content matched the selected campaign or IDs."])
    return "\n".join(lines)


def _matrix_row(
    content: dict[str, Any],
    variants_by_content_platform: dict[tuple[int, str], list[dict[str, Any]]],
    platforms: tuple[str, ...],
) -> dict[str, Any]:
    content_id = int(content["id"])
    platform_entries = {}
    for platform in platforms:
        candidates = variants_by_content_platform.get((content_id, platform), [])
        platform_entries[platform] = _platform_entry(platform, candidates)

    return {
        "content_id": content_id,
        "content_type": content.get("content_type"),
        "content_format": content.get("content_format"),
        "created_at": content.get("created_at"),
        "planned_topic_id": content.get("planned_topic_id"),
        "campaign_id": content.get("campaign_id"),
        "source": _copy_entry(content.get("content") or "", source="generated_content"),
        "platforms": platform_entries,
        "missing_platforms": [
            platform
            for platform, entry in platform_entries.items()
            if entry["gap"]
        ],
    }


def _platform_entry(platform: str, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    if not candidates:
        return {
            "platform": platform,
            "available": False,
            "gap": True,
            "gap_reason": "missing_variant",
            "variant": None,
            "available_variants": [],
            "counts": None,
        }

    selected = next((row for row in candidates if _truthy(row.get("selected"))), None)
    chosen = selected or candidates[-1]
    entry = _copy_entry(chosen.get("content") or "", source="content_variant")
    entry.update(
        {
            "platform": platform,
            "available": True,
            "gap": selected is None,
            "gap_reason": None if selected is not None else "missing_selected_variant",
            "variant": {
                "id": chosen.get("id"),
                "content_id": chosen.get("content_id"),
                "platform": chosen.get("platform"),
                "variant_type": chosen.get("variant_type"),
                "selected": bool(_truthy(chosen.get("selected"))),
                "metadata": _parse_metadata(chosen.get("metadata")),
                "created_at": chosen.get("created_at"),
            },
            "available_variants": [_variant_copy_entry(row) for row in candidates],
        }
    )
    return entry


def _copy_entry(text: str, *, source: str) -> dict[str, Any]:
    urls = _URL_RE.findall(text)
    return {
        "source": source,
        "text": text,
        "counts": {
            "characters": len(text),
            "graphemes": count_graphemes(text),
            "urls": len(urls),
            "has_links": bool(urls),
            "hashtags": len(_HASHTAG_RE.findall(text)),
        },
    }


def _variant_copy_entry(row: dict[str, Any]) -> dict[str, Any]:
    entry = _copy_entry(row.get("content") or "", source="content_variant")
    entry["variant"] = {
        "id": row.get("id"),
        "content_id": row.get("content_id"),
        "platform": row.get("platform"),
        "variant_type": row.get("variant_type"),
        "selected": bool(_truthy(row.get("selected"))),
        "metadata": _parse_metadata(row.get("metadata")),
        "created_at": row.get("created_at"),
    }
    return entry


def _variants_by_content_platform(
    variant_rows: Iterable[dict[str, Any]],
) -> dict[tuple[int, str], list[dict[str, Any]]]:
    grouped: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for row in variant_rows:
        variant = dict(row)
        key = (int(variant["content_id"]), str(variant["platform"]).lower())
        grouped.setdefault(key, []).append(variant)
    for rows in grouped.values():
        rows.sort(
            key=lambda row: (
                _truthy(row.get("selected")),
                str(row.get("created_at") or ""),
                int(row.get("id") or 0),
            )
        )
    return grouped


def _content_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    campaign_id: int | None,
    content_ids: tuple[int, ...],
) -> list[dict[str, Any]]:
    columns = schema.get("generated_content", set())
    select = [
        "gc.id",
        _column_expr(columns, "content_type", "NULL", "gc"),
        _column_expr(columns, "content_format", "NULL", "gc"),
        _column_expr(columns, "content", "''", "gc"),
        _column_expr(columns, "created_at", "NULL", "gc"),
    ]
    joins = ""
    filters: list[str] = []
    params: list[Any] = []

    if campaign_id is not None:
        if not {"planned_topics", "content_campaigns"}.issubset(schema):
            return []
        joins = " INNER JOIN planned_topics pt ON pt.content_id = gc.id"
        select.extend(["pt.id AS planned_topic_id", "pt.campaign_id AS campaign_id"])
        filters.append("pt.campaign_id = ?")
        params.append(campaign_id)
    elif "planned_topics" in schema:
        joins = " LEFT JOIN planned_topics pt ON pt.content_id = gc.id"
        select.extend(["pt.id AS planned_topic_id", "pt.campaign_id AS campaign_id"])
    else:
        select.extend(["NULL AS planned_topic_id", "NULL AS campaign_id"])

    if content_ids:
        filters.append(f"gc.id IN ({', '.join('?' for _ in content_ids)})")
        params.extend(content_ids)
    where = f" WHERE {' AND '.join(filters)}" if filters else ""
    rows = conn.execute(
        f"""SELECT {', '.join(select)}
            FROM generated_content gc
            {joins}
            {where}
            ORDER BY gc.created_at ASC, gc.id ASC""",
        params,
    ).fetchall()
    by_content_id: dict[int, dict[str, Any]] = {}
    for row in rows:
        item = dict(row)
        by_content_id.setdefault(int(item["id"]), item)
    return list(by_content_id.values())


def _variant_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    content_ids: list[int],
    platforms: tuple[str, ...],
) -> list[dict[str, Any]]:
    if not content_ids or "content_variants" not in schema:
        return []
    columns = schema["content_variants"]
    required = {"content_id", "platform", "variant_type", "content"}
    if not required.issubset(columns):
        return []
    selected = [
        _column_expr(columns, "id", "NULL"),
        "content_id",
        "platform",
        "variant_type",
        "content",
        _column_expr(columns, "metadata", "NULL"),
        _column_expr(columns, "selected", "0"),
        _column_expr(columns, "created_at", "NULL"),
    ]
    params: list[Any] = [*content_ids, *platforms]
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM content_variants
            WHERE content_id IN ({', '.join('?' for _ in content_ids)})
              AND lower(platform) IN ({', '.join('?' for _ in platforms)})
            ORDER BY content_id ASC, platform ASC, selected ASC, created_at ASC, id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _resolve_campaign(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    campaign: str | int | None,
) -> dict[str, Any] | None:
    if campaign is None:
        return None
    if "content_campaigns" not in schema:
        raise ValueError("content_campaigns table is not available")
    if isinstance(campaign, int) or str(campaign).isdigit():
        row = conn.execute(
            "SELECT * FROM content_campaigns WHERE id = ?",
            (int(campaign),),
        ).fetchone()
    else:
        row = conn.execute(
            """SELECT * FROM content_campaigns
               WHERE name = ?
               ORDER BY created_at ASC, id ASC
               LIMIT 1""",
            (str(campaign),),
        ).fetchone()
    if not row:
        raise ValueError(f"campaign {campaign} does not exist")
    return dict(row)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    ).fetchall()
    return {
        row["name"]: {
            column[1]
            for column in conn.execute(f"PRAGMA table_info({row['name']})")
        }
        for row in rows
    }


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str,
    table_alias: str | None = None,
) -> str:
    prefix = f"{table_alias}." if table_alias else ""
    return f"{prefix}{column}" if column in columns else f"{fallback} AS {column}"


def _normalize_content_ids(content_ids: Iterable[int] | None) -> tuple[int, ...]:
    if not content_ids:
        return ()
    normalized = []
    for content_id in content_ids:
        value = int(content_id)
        if value <= 0:
            raise ValueError("content IDs must be positive")
        normalized.append(value)
    return tuple(dict.fromkeys(normalized))


def _normalize_platforms(platforms: Iterable[str]) -> list[str]:
    normalized = []
    for platform in platforms:
        value = str(platform).strip().lower()
        if not value:
            continue
        if value == "all":
            normalized.extend(DEFAULT_PLATFORMS)
        else:
            normalized.append(value)
    result = list(dict.fromkeys(normalized))
    if not result:
        raise ValueError("at least one platform is required")
    return result


def _parse_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _truthy(value: Any) -> bool:
    return value in {True, 1, "1", "true", "True", "yes", "YES"}


def _variant_summary(entry: dict[str, Any]) -> str:
    if not entry.get("available", True):
        return "**GAP** missing_variant"
    counts = entry["counts"]
    marker = ""
    variant = entry.get("variant")
    if variant and not variant.get("selected"):
        marker = " GAP missing_selected_variant"
    text = _truncate(entry.get("text") or "", 54)
    return _md(
        f"{text} ({counts['graphemes']}g, {counts['urls']} url, "
        f"{counts['hashtags']} #){marker}"
    )


def _truncate(value: str, width: int) -> str:
    text = " ".join(str(value).split())
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _md(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", "<br>")
