"""Audit published X media attachment metadata."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import csv
import io
import json
from pathlib import Path
import posixpath
import sqlite3
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


DEFAULT_DAYS = 30
ISSUE_TYPES = (
    "missing_alt_text",
    "malformed_media_url",
    "broken_local_path",
    "orphaned_media_prompt",
    "duplicate_media_url",
    "duplicate_media_prompt",
)
_CSV_FIELDS = (
    "issue_type",
    "content_id",
    "published_at",
    "platform_post_id",
    "platform_url",
    "content_type",
    "media_reference",
    "normalized_media_reference",
    "image_prompt",
    "normalized_prompt_reference",
    "image_alt_text",
    "duplicate_group",
    "detail",
)


@dataclass(frozen=True)
class XMediaAttachmentAuditRow:
    """One published X media attachment metadata finding."""

    issue_type: str
    content_id: int
    published_at: str | None
    platform_post_id: str | None
    platform_url: str | None
    content_type: str | None
    media_reference: str | None
    normalized_media_reference: str | None
    image_prompt: str | None
    normalized_prompt_reference: str | None
    image_alt_text: str | None
    duplicate_group: str | None
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class XMediaAttachmentAuditReport:
    """Published X media attachment audit report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    rows: tuple[XMediaAttachmentAuditRow, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "x_media_attachment_audit",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def build_x_media_attachment_audit_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    now: datetime | None = None,
) -> XMediaAttachmentAuditReport:
    """Return findings for recent published X posts with visual metadata."""
    if days <= 0:
        raise ValueError("days must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"days": days, "cutoff": cutoff.isoformat(), "platform": "x"}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or _missing_required_columns(missing_columns):
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    published_rows = _load_published_x_rows(
        conn,
        schema=schema,
        cutoff=cutoff,
        now=generated_at,
    )
    findings = _metadata_findings(published_rows)
    findings.extend(_duplicate_findings(published_rows))
    rows = tuple(sorted(findings, key=_sort_key))
    counts = {issue_type: 0 for issue_type in ISSUE_TYPES}
    for row in rows:
        counts[row.issue_type] = counts.get(row.issue_type, 0) + 1
    return XMediaAttachmentAuditReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "published_posts_scanned": len(published_rows),
            "finding_count": len(rows),
            "by_issue_type": counts,
        },
        rows=rows,
        missing_tables=(),
        missing_columns=missing_columns,
    )


def format_x_media_attachment_audit_json(report: XMediaAttachmentAuditReport) -> str:
    """Serialize the X media attachment audit as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_x_media_attachment_audit_csv(report: XMediaAttachmentAuditReport) -> str:
    """Render the X media attachment audit as CSV."""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=_CSV_FIELDS, lineterminator="\n")
    writer.writeheader()
    for row in report.rows:
        writer.writerow({field: row.to_dict().get(field) or "" for field in _CSV_FIELDS})
    return output.getvalue().rstrip("\n")


def _metadata_findings(rows: list[dict[str, Any]]) -> list[XMediaAttachmentAuditRow]:
    findings: list[XMediaAttachmentAuditRow] = []
    for row in rows:
        media_reference = _clean(row.get("image_path"))
        image_prompt = _clean(row.get("image_prompt"))
        image_alt_text = _clean(row.get("image_alt_text"))
        normalized_media = normalize_media_reference(media_reference)
        normalized_prompt = normalize_prompt_reference(image_prompt)
        has_media = bool(media_reference)
        has_prompt = bool(image_prompt)

        if has_media and not image_alt_text:
            findings.append(
                _finding(
                    row,
                    issue_type="missing_alt_text",
                    media_reference=media_reference,
                    normalized_media_reference=normalized_media,
                    image_prompt=image_prompt,
                    normalized_prompt_reference=normalized_prompt,
                    image_alt_text=image_alt_text,
                    detail="Published X post has a media reference without image_alt_text.",
                )
            )

        if media_reference and _is_malformed_url(media_reference):
            findings.append(
                _finding(
                    row,
                    issue_type="malformed_media_url",
                    media_reference=media_reference,
                    normalized_media_reference=normalized_media,
                    image_prompt=image_prompt,
                    normalized_prompt_reference=normalized_prompt,
                    image_alt_text=image_alt_text,
                    detail=(
                        "Media reference looks like a URL but is not a valid "
                        "HTTP(S) or file URL."
                    ),
                )
            )
        elif media_reference and _is_local_path(media_reference):
            path = Path(media_reference).expanduser()
            if not path.exists():
                findings.append(
                    _finding(
                        row,
                        issue_type="broken_local_path",
                        media_reference=media_reference,
                        normalized_media_reference=normalized_media,
                        image_prompt=image_prompt,
                        normalized_prompt_reference=normalized_prompt,
                        image_alt_text=image_alt_text,
                        detail=(
                            "Published X post references a local media path "
                            "that does not exist."
                        ),
                    )
                )
            elif not path.is_file():
                findings.append(
                    _finding(
                        row,
                        issue_type="broken_local_path",
                        media_reference=media_reference,
                        normalized_media_reference=normalized_media,
                        image_prompt=image_prompt,
                        normalized_prompt_reference=normalized_prompt,
                        image_alt_text=image_alt_text,
                        detail="Published X post references a local media path that is not a file.",
                    )
                )

        if has_prompt and not has_media:
            findings.append(
                _finding(
                    row,
                    issue_type="orphaned_media_prompt",
                    media_reference=media_reference,
                    normalized_media_reference=normalized_media,
                    image_prompt=image_prompt,
                    normalized_prompt_reference=normalized_prompt,
                    image_alt_text=image_alt_text,
                    detail=(
                        "Published X post has an image_prompt but no media "
                        "attachment reference."
                    ),
                )
            )
    return findings


def _duplicate_findings(rows: list[dict[str, Any]]) -> list[XMediaAttachmentAuditRow]:
    findings: list[XMediaAttachmentAuditRow] = []
    media_groups: dict[str, list[dict[str, Any]]] = {}
    prompt_groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        media_reference = _clean(row.get("image_path"))
        normalized_media = normalize_media_reference(media_reference)
        if normalized_media:
            media_groups.setdefault(normalized_media, []).append(row)
        image_prompt = _clean(row.get("image_prompt"))
        normalized_prompt = normalize_prompt_reference(image_prompt)
        if normalized_prompt:
            prompt_groups.setdefault(normalized_prompt, []).append(row)

    for normalized_media, group in sorted(media_groups.items()):
        if len(group) < 2:
            continue
        duplicate_group = f"media:{normalized_media}"
        for row in group:
            findings.append(
                _finding(
                    row,
                    issue_type="duplicate_media_url",
                    media_reference=_clean(row.get("image_path")),
                    normalized_media_reference=normalized_media,
                    image_prompt=_clean(row.get("image_prompt")),
                    normalized_prompt_reference=normalize_prompt_reference(row.get("image_prompt")),
                    image_alt_text=_clean(row.get("image_alt_text")),
                    duplicate_group=duplicate_group,
                    detail=f"Media reference is reused by {len(group)} recent published X posts.",
                )
            )

    for normalized_prompt, group in sorted(prompt_groups.items()):
        if len(group) < 2:
            continue
        duplicate_group = f"prompt:{normalized_prompt}"
        for row in group:
            findings.append(
                _finding(
                    row,
                    issue_type="duplicate_media_prompt",
                    media_reference=_clean(row.get("image_path")),
                    normalized_media_reference=normalize_media_reference(row.get("image_path")),
                    image_prompt=_clean(row.get("image_prompt")),
                    normalized_prompt_reference=normalized_prompt,
                    image_alt_text=_clean(row.get("image_alt_text")),
                    duplicate_group=duplicate_group,
                    detail=(
                        "Image prompt reference is reused by "
                        f"{len(group)} recent published X posts."
                    ),
                )
            )
    return findings


def normalize_media_reference(value: Any) -> str:
    """Normalize a media URL or local path reference for duplicate detection."""
    text = _clean(value)
    if not text:
        return ""
    parsed = urlparse(text)
    if parsed.scheme:
        scheme = parsed.scheme.casefold()
        netloc = parsed.netloc.casefold()
        path = posixpath.normpath(parsed.path or "/")
        if parsed.path.endswith("/") and not path.endswith("/"):
            path += "/"
        query = urlencode(sorted(parse_qsl(parsed.query, keep_blank_values=True)))
        return urlunparse((scheme, netloc, path, "", query, ""))
    try:
        return str(Path(text).expanduser().resolve(strict=False))
    except (OSError, RuntimeError):
        return text


def normalize_prompt_reference(value: Any) -> str:
    """Normalize image prompt text for exact duplicate detection."""
    return " ".join(_clean(value).casefold().split())


def _load_published_x_rows(
    conn: sqlite3.Connection,
    *,
    schema: dict[str, set[str]],
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    gc = schema["generated_content"]
    has_cp = "content_publications" in schema and {"content_id", "platform"}.issubset(
        schema["content_publications"]
    )
    cp = schema.get("content_publications", set())
    select = [
        "gc.id AS content_id",
        _column_expr(gc, "content_type", "NULL", alias="gc") + " AS content_type",
        _column_expr(gc, "image_path", "NULL", alias="gc") + " AS image_path",
        _column_expr(gc, "image_prompt", "NULL", alias="gc") + " AS image_prompt",
        _column_expr(gc, "image_alt_text", "NULL", alias="gc") + " AS image_alt_text",
        _column_expr(gc, "published", "0", alias="gc") + " AS legacy_published",
        _column_expr(gc, "published_at", "NULL", alias="gc") + " AS legacy_published_at",
        _column_expr(gc, "published_url", "NULL", alias="gc") + " AS legacy_published_url",
        _column_expr(gc, "tweet_id", "NULL", alias="gc") + " AS legacy_tweet_id",
        _column_expr(gc, "created_at", "NULL", alias="gc") + " AS created_at",
    ]
    if has_cp:
        select.extend(
            [
                _column_expr(cp, "status", "NULL", alias="cp") + " AS publication_status",
                _column_expr(cp, "platform_post_id", "NULL", alias="cp") + " AS platform_post_id",
                _column_expr(cp, "platform_url", "NULL", alias="cp") + " AS platform_url",
                _column_expr(cp, "published_at", "NULL", alias="cp")
                + " AS publication_published_at",
            ]
        )
        join = "LEFT JOIN content_publications cp ON cp.content_id = gc.id AND cp.platform = 'x'"
    else:
        select.extend(
            [
                "NULL AS publication_status",
                "NULL AS platform_post_id",
                "NULL AS platform_url",
                "NULL AS publication_published_at",
            ]
        )
        join = ""

    rows = [
        dict(row)
        for row in conn.execute(
            f"""SELECT {", ".join(select)}
                FROM generated_content gc
                {join}
                ORDER BY gc.id ASC"""
        ).fetchall()
    ]

    selected: list[dict[str, Any]] = []
    for row in rows:
        if not _is_published_x_row(row):
            continue
        published_at_text = (
            _clean(row.get("publication_published_at"))
            or _clean(row.get("legacy_published_at"))
            or _clean(row.get("created_at"))
        )
        published_at = _parse_datetime(published_at_text)
        if published_at is None or published_at < cutoff or published_at > now:
            continue
        row["published_at"] = published_at_text
        row["platform_post_id"] = _clean(row.get("platform_post_id")) or _clean(
            row.get("legacy_tweet_id")
        )
        row["platform_url"] = _clean(row.get("platform_url")) or _clean(
            row.get("legacy_published_url")
        )
        selected.append(row)
    return selected


def _is_published_x_row(row: dict[str, Any]) -> bool:
    status = _clean(row.get("publication_status")).casefold()
    if status == "published" or _clean(row.get("publication_published_at")):
        return True
    content_type = _clean(row.get("content_type")).casefold()
    legacy_x = content_type in {"x_post", "x_thread", "x_visual", "visual"}
    legacy_published = row.get("legacy_published") in (1, "1", True)
    return legacy_x and (
        legacy_published
        or bool(_clean(row.get("legacy_tweet_id")))
        or bool(_clean(row.get("legacy_published_url")))
    )


def _finding(
    row: dict[str, Any],
    *,
    issue_type: str,
    media_reference: str | None,
    normalized_media_reference: str | None,
    image_prompt: str | None,
    normalized_prompt_reference: str | None,
    image_alt_text: str | None,
    detail: str,
    duplicate_group: str | None = None,
) -> XMediaAttachmentAuditRow:
    return XMediaAttachmentAuditRow(
        issue_type=issue_type,
        content_id=int(row["content_id"]),
        published_at=_clean(row.get("published_at")) or None,
        platform_post_id=_clean(row.get("platform_post_id")) or None,
        platform_url=_clean(row.get("platform_url")) or None,
        content_type=_clean(row.get("content_type")) or None,
        media_reference=media_reference or None,
        normalized_media_reference=normalized_media_reference or None,
        image_prompt=image_prompt or None,
        normalized_prompt_reference=normalized_prompt_reference or None,
        image_alt_text=image_alt_text or None,
        duplicate_group=duplicate_group,
        detail=detail,
    )


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    missing_tables = tuple(
        table for table in ("generated_content",) if table not in schema
    )
    missing_columns: dict[str, tuple[str, ...]] = {}
    if "generated_content" in schema:
        required = ("id",)
        optional = (
            "content_type",
            "image_path",
            "image_prompt",
            "image_alt_text",
            "published",
            "published_url",
            "tweet_id",
            "published_at",
            "created_at",
        )
        missing = [
            column
            for column in (*required, *optional)
            if column not in schema["generated_content"]
        ]
        if missing:
            missing_columns["generated_content"] = tuple(sorted(missing))
    if "content_publications" in schema:
        expected = (
            "content_id",
            "platform",
            "status",
            "platform_post_id",
            "platform_url",
            "published_at",
        )
        missing = [column for column in expected if column not in schema["content_publications"]]
        if missing:
            missing_columns["content_publications"] = tuple(sorted(missing))
    return missing_tables, missing_columns


def _missing_required_columns(missing_columns: dict[str, tuple[str, ...]]) -> bool:
    return "id" in missing_columns.get("generated_content", ())


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> XMediaAttachmentAuditReport:
    return XMediaAttachmentAuditReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "published_posts_scanned": 0,
            "finding_count": 0,
            "by_issue_type": {issue_type: 0 for issue_type in ISSUE_TYPES},
        },
        rows=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


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


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str | None = None,
) -> str:
    if column not in columns:
        return fallback
    prefix = f"{alias}." if alias else ""
    return f"{prefix}{column}"


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _is_malformed_url(value: str) -> bool:
    parsed = urlparse(value)
    if not parsed.scheme:
        return False
    if parsed.scheme.casefold() == "file":
        return not bool(parsed.path)
    if parsed.scheme.casefold() not in {"http", "https"}:
        return True
    return not bool(parsed.netloc)


def _is_local_path(value: str) -> bool:
    parsed = urlparse(value)
    return not parsed.scheme or parsed.scheme.casefold() == "file"


def _sort_key(row: XMediaAttachmentAuditRow) -> tuple[str, str, int]:
    return (row.issue_type, row.duplicate_group or "", row.content_id)
