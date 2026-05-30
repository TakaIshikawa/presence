"""Audit blog draft images for missing or undersized dimensions."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_MIN_WIDTH = 320
DEFAULT_MIN_HEIGHT = 180
_MD_IMAGE_RE = re.compile(r"!\[([^\]]*)\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)(?:\{([^}]*)\})?")
_HTML_IMAGE_RE = re.compile(r"<img\b[^>]*>", re.I)
_ATTR_RE = re.compile(r"([\w:-]+)\s*=\s*['\"]([^'\"]*)['\"]")
_BRACE_ATTR_RE = re.compile(r"([\w:-]+)\s*=\s*['\"]?([^'\"\s}]+)")


def build_blog_draft_image_dimension_gaps_report(
    rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    min_width: int = DEFAULT_MIN_WIDTH,
    min_height: int = DEFAULT_MIN_HEIGHT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    if min_width <= 0:
        raise ValueError("min_width must be positive")
    if min_height <= 0:
        raise ValueError("min_height must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    issues = []
    scanned = 0
    for row in rows:
        draft_id = _text(row.get("draft_id") or row.get("id") or row.get("slug"))
        draft_path = _text(row.get("path") or row.get("file_path") or row.get("source_path"))
        for image in _extract_images(row):
            scanned += 1
            for reason in _reasons(image, min_width=min_width, min_height=min_height):
                issues.append(
                    {
                        "draft_id": draft_id,
                        "draft_path": draft_path,
                        "image_src": image["src"],
                        "width": image.get("width"),
                        "height": image.get("height"),
                        "reason": reason,
                        "recommendation": _recommendation(reason, min_width, min_height),
                    }
                )
    issues.sort(key=lambda item: (item["draft_id"], item["draft_path"], item["image_src"], item["reason"]))
    shown = issues[:limit]
    return {
        "artifact_type": "blog_draft_image_dimension_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit, "min_width": min_width, "min_height": min_height},
        "summary": {
            "drafts": len(rows),
            "images_scanned": scanned,
            "issue_count": len(issues),
            "shown": len(shown),
            "reason_counts": dict(sorted(Counter(item["reason"] for item in issues).items())),
        },
        "issues": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items())},
        "empty_state": {
            "is_empty": not issues,
            "reason": "missing_schema" if missing_tables or missing_columns else ("no_findings" if not issues else None),
            "message": "No blog draft image dimension gaps found." if not issues else None,
        },
    }


def build_blog_draft_image_dimension_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    sch = _schema(conn)
    table = "blog_drafts" if "blog_drafts" in sch else "generated_content" if "generated_content" in sch else None
    if not table:
        return build_blog_draft_image_dimension_gaps_report([], missing_tables=["blog_drafts|generated_content"], **kwargs)
    cols = sch[table]
    body_candidates = {"body", "content", "markdown", "html", "draft"}
    if not (body_candidates & cols):
        return build_blog_draft_image_dimension_gaps_report([], missing_columns={table: ["body|content|markdown|html|draft"]}, **kwargs)
    select = [
        _pick(cols, ("id", "draft_id", "slug"), "draft_id"),
        _pick(cols, ("path", "file_path", "source_path"), "path"),
        _pick(cols, ("body", "content", "markdown", "html", "draft"), "body"),
        _pick(cols, ("metadata", "image_metadata"), "metadata"),
    ]
    where = []
    if table == "generated_content" and "content_type" in cols:
        where.append("lower(content_type) LIKE '%blog%'")
    if "status" in cols:
        where.append("lower(status)='draft'")
    elif "state" in cols:
        where.append("lower(state)='draft'")
    sql = f"SELECT {', '.join(select)} FROM {table}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    return build_blog_draft_image_dimension_gaps_report([dict(row) for row in conn.execute(sql)], **kwargs)


def format_blog_draft_image_dimension_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_blog_draft_image_dimension_gaps_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Blog Draft Image Dimension Gaps",
        f"Generated: {report['generated_at']}",
        f"Totals: drafts={summary['drafts']} images={summary['images_scanned']} issues={summary['issue_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + "; ".join(f"{t}({', '.join(c)})" for t, c in report["missing_columns"].items()))
    if not report["issues"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines += ["", "draft_id | draft_path | image_src | reason | width | height | recommendation"]
    for item in report["issues"]:
        lines.append(
            f"{item['draft_id']} | {item['draft_path'] or '-'} | {item['image_src']} | {item['reason']} | "
            f"{item['width'] or '-'} | {item['height'] or '-'} | {item['recommendation']}"
        )
    return "\n".join(lines)


def _extract_images(row: dict[str, Any]) -> list[dict[str, Any]]:
    body = _text(row.get("body"))
    images = []
    for _alt, src, attrs_text in _MD_IMAGE_RE.findall(body):
        attrs = {k.lower(): v for k, v in _BRACE_ATTR_RE.findall(attrs_text or "")}
        images.append({"src": src.strip(), "width": _dim(attrs.get("width")), "height": _dim(attrs.get("height"))})
    for tag in _HTML_IMAGE_RE.findall(body):
        attrs = {key.lower(): value for key, value in _ATTR_RE.findall(tag)}
        if attrs.get("src"):
            images.append({"src": attrs["src"].strip(), "width": _dim(attrs.get("width")), "height": _dim(attrs.get("height"))})
    for image in _metadata_images(_json_obj(row.get("metadata"))):
        src = _text(image.get("src") or image.get("url") or image.get("path"))
        if src:
            images.append({"src": src, "width": _dim(image.get("width")), "height": _dim(image.get("height"))})
    return images


def _reasons(image: dict[str, Any], *, min_width: int, min_height: int) -> list[str]:
    width, height = image.get("width"), image.get("height")
    if width is None and height is None:
        return ["missing_dimensions"]
    reasons = []
    if width is None or height is None:
        reasons.append("partial_dimensions")
    if width is not None and width < min_width:
        reasons.append("width_below_minimum")
    if height is not None and height < min_height:
        reasons.append("height_below_minimum")
    return reasons


def _recommendation(reason: str, min_width: int, min_height: int) -> str:
    if reason == "missing_dimensions":
        return "Add explicit width and height metadata to reserve image layout space."
    if reason == "partial_dimensions":
        return "Add the missing width or height so both dimensions are available."
    if reason == "width_below_minimum":
        return f"Use an image at least {min_width}px wide or lower the configured width threshold."
    return f"Use an image at least {min_height}px tall or lower the configured height threshold."


def _metadata_images(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        found = [value] if any(key in value for key in ("src", "url", "path")) else []
        for key in ("images", "image_assets", "media"):
            found.extend(_metadata_images(value.get(key)))
        return found
    if isinstance(value, list):
        return [item for entry in value for item in _metadata_images(entry)]
    return []


def _dim(value: Any) -> int | None:
    if value in (None, ""):
        return None
    match = re.search(r"\d+", str(value))
    return int(match.group(0)) if match else None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _pick(cols: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for candidate in candidates:
        if candidate in cols:
            return candidate if candidate == alias else f"{candidate} AS {alias}"
    return f"NULL AS {alias}"


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
