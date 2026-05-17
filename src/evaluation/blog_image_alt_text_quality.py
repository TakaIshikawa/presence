"""Audit blog image alt text quality."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
DEFAULT_MIN_CHARS = 12
_MD_IMAGE_RE = re.compile(r"!\[([^\]]*)\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
_HTML_IMAGE_RE = re.compile(r"<img\b[^>]*>", re.I)
_ATTR_RE = re.compile(r"(\w[\w:-]*)\s*=\s*['\"]([^'\"]*)['\"]")
_GENERIC = {"image", "photo", "picture", "screenshot", "graphic", "diagram", "untitled"}


def build_blog_image_alt_text_quality_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    min_chars: int = DEFAULT_MIN_CHARS,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    findings = []
    scanned = 0
    for row in rows:
        created_at = _parse_dt(row.get("created_at") or row.get("published_at"))
        if created_at and created_at < cutoff:
            continue
        images = _extract_images(row)
        scanned += len(images)
        seen_alt: Counter[str] = Counter(_norm_alt(image["alt_text"]) for image in images if _norm_alt(image["alt_text"]))
        for image in images:
            reasons = _reasons(image, seen_alt, min_chars=min_chars)
            for reason in reasons:
                findings.append(
                    {
                        "content_id": _text(row.get("id")),
                        "content_type": _text(row.get("content_type") or row.get("type") or "blog"),
                        "created_at": created_at.isoformat() if created_at else None,
                        "image_src": image["src"],
                        "alt_text": image["alt_text"],
                        "caption": image["caption"],
                        "reason_code": reason,
                        "severity": "error" if reason == "missing" else "warning",
                    }
                )
    findings.sort(key=lambda item: (item["content_id"], item["image_src"], item["reason_code"]))
    return {
        "artifact_type": "blog_image_alt_text_quality",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit, "min_chars": min_chars, "lookback_start": cutoff.isoformat()},
        "summary": {
            "images_scanned": scanned,
            "finding_count": len(findings),
            "reason_counts": dict(sorted(Counter(item["reason_code"] for item in findings).items())),
        },
        "findings": findings[:limit],
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
    }


def build_blog_image_alt_text_quality_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    rows = _load_rows(conn, schema) if not gaps["missing_tables"] else []
    return build_blog_image_alt_text_quality_report(rows, schema_gaps=gaps, **kwargs)


def format_blog_image_alt_text_quality_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_blog_image_alt_text_quality_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Blog Image Alt Text Quality",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['days']} days limit={report['filters']['limit']}",
        f"Totals: images={summary['images_scanned']} findings={summary['finding_count']}",
    ]
    if not report["findings"]:
        lines.extend(["", "No blog image alt text findings."])
        return "\n".join(lines)
    lines.extend(["", "Findings:"])
    for item in report["findings"]:
        lines.append(
            f"  - content={item['content_id']} src={item['image_src']} reason={item['reason_code']} "
            f"severity={item['severity']} alt={item['alt_text'] or '-'}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = "blog_posts" if "blog_posts" in schema else "generated_content" if "generated_content" in schema else ""
    if not table:
        return []
    columns = schema[table]
    select = [
        _select(columns, ("id", "post_id", "slug"), "id"),
        _select(columns, ("content_type", "type"), "content_type"),
        _select(columns, ("body", "content", "markdown", "html"), "body"),
        _select(columns, ("metadata", "image_metadata"), "metadata"),
        _select(columns, ("created_at", "published_at", "updated_at"), "created_at"),
    ]
    where = ""
    if table == "generated_content" and "content_type" in columns:
        where = "WHERE lower(content_type) LIKE '%blog%'"
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table} {where}").fetchall()]


def _extract_images(row: dict[str, Any]) -> list[dict[str, str]]:
    body = _text(row.get("body"))
    images = [{"src": src.strip(), "alt_text": alt.strip(), "caption": ""} for alt, src in _MD_IMAGE_RE.findall(body)]
    for tag in _HTML_IMAGE_RE.findall(body):
        attrs = {key.lower(): value for key, value in _ATTR_RE.findall(tag)}
        if attrs.get("src"):
            images.append({"src": attrs["src"].strip(), "alt_text": attrs.get("alt", "").strip(), "caption": attrs.get("title", "").strip()})
    metadata = _json_obj(row.get("metadata"))
    for entry in _metadata_images(metadata):
        src = _text(entry.get("src") or entry.get("url") or entry.get("path"))
        if src:
            images.append(
                {
                    "src": src,
                    "alt_text": _text(entry.get("alt") or entry.get("alt_text")),
                    "caption": _text(entry.get("caption")),
                }
            )
    return images


def _metadata_images(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        found = []
        if any(key in value for key in ("src", "url", "path")):
            found.append(value)
        for key in ("images", "image_assets", "media"):
            found.extend(_metadata_images(value.get(key)))
        return found
    if isinstance(value, list):
        return [item for entry in value for item in _metadata_images(entry)]
    return []


def _reasons(image: dict[str, str], seen_alt: Counter[str], *, min_chars: int) -> list[str]:
    alt = image["alt_text"].strip()
    norm = _norm_alt(alt)
    reasons = []
    if not alt:
        reasons.append("missing")
    elif len(alt) < min_chars:
        reasons.append("too_short")
    if norm and seen_alt[norm] > 1:
        reasons.append("duplicated")
    if norm in _GENERIC:
        reasons.append("generic")
    src_stem = re.sub(r"[-_]+", " ", image["src"].rsplit("/", 1)[-1].rsplit(".", 1)[0]).lower().strip()
    if norm and norm == src_stem:
        reasons.append("filename_like")
    return reasons


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "blog_posts" not in schema and "generated_content" not in schema:
        return {"missing_tables": ["blog_posts|generated_content"], "missing_columns": {}}
    return {"missing_tables": [], "missing_columns": {}}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for candidate in candidates:
        if candidate in columns:
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


def _norm_alt(value: str) -> str:
    return re.sub(r"\s+", " ", value.lower().strip())


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
