"""Audit blog draft image alt text quality."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_MIN_CHARS = 12
_MD_IMAGE_RE = re.compile(r"!\[([^\]]*)\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
_HTML_IMAGE_RE = re.compile(r"<img\b[^>]*>", re.I)
_ATTR_RE = re.compile(r"([\w:-]+)\s*=\s*['\"]([^'\"]*)['\"]")


def build_blog_draft_alt_text_quality_report(
    rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    min_chars: int = DEFAULT_MIN_CHARS,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    if min_chars <= 0:
        raise ValueError("min_chars must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    findings = []
    scanned = 0
    for row in rows:
        images = _extract_images(row)
        scanned += len(images)
        counts = Counter(_norm(image["alt_text"]) for image in images if _norm(image["alt_text"]))
        for image in images:
            for issue_type in _issue_types(image, counts, min_chars=min_chars):
                findings.append(
                    {
                        "draft_id": _text(row.get("draft_id") or row.get("id") or row.get("slug")),
                        "image_src": image["src"],
                        "alt_text": image["alt_text"],
                        "issue_type": issue_type,
                        "recommendation": _recommendation(issue_type, image["src"]),
                    }
                )
    findings.sort(key=lambda item: (item["draft_id"], item["image_src"], item["issue_type"]))
    return {
        "artifact_type": "blog_draft_alt_text_quality",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit, "min_chars": min_chars},
        "summary": {
            "drafts": len(rows),
            "images_scanned": scanned,
            "finding_count": len(findings),
            "issue_counts": dict(sorted(Counter(item["issue_type"] for item in findings).items())),
        },
        "findings": findings[:limit],
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items())},
        "empty_state": {
            "is_empty": not findings,
            "reason": "missing_schema" if missing_tables or missing_columns else ("no_findings" if not findings else None),
            "message": "No blog draft alt text quality findings." if not findings else None,
        },
    }


def build_blog_draft_alt_text_quality_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    sch = _schema(conn)
    table = "blog_drafts" if "blog_drafts" in sch else "generated_content" if "generated_content" in sch else None
    if not table:
        return build_blog_draft_alt_text_quality_report([], missing_tables=["blog_drafts|generated_content"], **kwargs)
    cols = sch[table]
    body_candidates = {"body", "content", "markdown", "html", "draft"}
    if not (body_candidates & cols):
        return build_blog_draft_alt_text_quality_report([], missing_columns={table: ["body|content|markdown|html|draft"]}, **kwargs)
    select = [
        _pick(cols, ("id", "draft_id", "slug"), "draft_id"),
        _pick(cols, ("body", "content", "markdown", "html", "draft"), "body"),
        _pick(cols, ("metadata", "image_metadata"), "metadata"),
        _pick(cols, ("status", "state"), "status"),
        _pick(cols, ("content_type", "type"), "content_type"),
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
    rows = [dict(row) for row in conn.execute(sql)]
    return build_blog_draft_alt_text_quality_report(rows, **kwargs)


def format_blog_draft_alt_text_quality_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_blog_draft_alt_text_quality_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Blog Draft Alt Text Quality",
        f"Generated: {report['generated_at']}",
        f"Totals: drafts={summary['drafts']} images={summary['images_scanned']} findings={summary['finding_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append(
            "Missing columns: "
            + "; ".join(f"{table}({', '.join(cols)})" for table, cols in report["missing_columns"].items())
        )
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines += ["", "draft_id | image_src | issue_type | alt_text | recommendation"]
    for item in report["findings"]:
        lines.append(
            f"{item['draft_id']} | {item['image_src']} | {item['issue_type']} | "
            f"{item['alt_text'] or '-'} | {item['recommendation']}"
        )
    return "\n".join(lines)


def _extract_images(row: dict[str, Any]) -> list[dict[str, str]]:
    body = _text(row.get("body"))
    images = [{"src": src.strip(), "alt_text": alt.strip()} for alt, src in _MD_IMAGE_RE.findall(body)]
    for tag in _HTML_IMAGE_RE.findall(body):
        attrs = {key.lower(): value for key, value in _ATTR_RE.findall(tag)}
        if attrs.get("src"):
            images.append({"src": attrs["src"].strip(), "alt_text": attrs.get("alt", "").strip()})
    metadata = _json_obj(row.get("metadata"))
    for image in _metadata_images(metadata):
        src = _text(image.get("src") or image.get("url") or image.get("path"))
        if src:
            images.append({"src": src, "alt_text": _text(image.get("alt") or image.get("alt_text"))})
    return images


def _issue_types(image: dict[str, str], counts: Counter[str], *, min_chars: int) -> list[str]:
    alt = image["alt_text"].strip()
    norm = _norm(alt)
    issues = []
    if not alt:
        issues.append("missing")
    elif len(alt) < min_chars:
        issues.append("short")
    if norm and counts[norm] > 1:
        issues.append("duplicate")
    stem = re.sub(r"[-_]+", " ", image["src"].rsplit("/", 1)[-1].rsplit(".", 1)[0]).lower().strip()
    if norm and norm == stem:
        issues.append("filename_like")
    return issues


def _recommendation(issue_type: str, src: str) -> str:
    if issue_type == "missing":
        return f"Add concise descriptive alt text for {src}."
    if issue_type == "duplicate":
        return "Make this alt text specific to this image instead of reusing the same phrase."
    if issue_type == "short":
        return "Expand the alt text with the visible subject and relevant context."
    return "Replace filename-like alt text with a human-readable image description."


def _metadata_images(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        found = [value] if any(key in value for key in ("src", "url", "path")) else []
        for key in ("images", "image_assets", "media"):
            found.extend(_metadata_images(value.get(key)))
        return found
    if isinstance(value, list):
        return [item for entry in value for item in _metadata_images(entry)]
    return []


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


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


def _norm(value: str) -> str:
    return re.sub(r"\s+", " ", value.lower().strip())


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
