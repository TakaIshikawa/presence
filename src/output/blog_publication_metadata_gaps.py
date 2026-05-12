"""Report published blog posts missing static-site publication metadata."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any

from output.blog_frontmatter_validator import parse_markdown_frontmatter


DEFAULT_DAYS = 90

GAP_MISSING_PUBLISHED_URL = "missing_published_url"
GAP_MISSING_PUBLISHED_AT = "missing_published_at"
GAP_MISSING_TITLE = "missing_title"


@dataclass(frozen=True)
class BlogPublicationMetadataGap:
    """One missing publication metadata field for a generated blog post."""

    content_id: int
    gap_type: str
    content_type: str
    published_url: str | None
    published_at: str | None
    title_source: str | None
    content_preview: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "content_id": self.content_id,
            "content_preview": self.content_preview,
            "content_type": self.content_type,
            "gap_type": self.gap_type,
            "published_at": self.published_at,
            "published_url": self.published_url,
            "title_source": self.title_source,
        }


@dataclass(frozen=True)
class BlogPublicationMetadataGapReport:
    """Deterministic report for generated blog publication metadata gaps."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    gaps: tuple[BlogPublicationMetadataGap, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "blog_publication_metadata_gaps",
            "filters": dict(self.filters),
            "gaps": [gap.to_dict() for gap in self.gaps],
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(self.totals),
        }


def build_blog_publication_metadata_gap_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    now: datetime | None = None,
) -> BlogPublicationMetadataGapReport:
    """Return published blog_post rows missing static-site metadata."""
    if days <= 0:
        raise ValueError("days must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_aware(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "content_type": "blog_post",
        "days": days,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": cutoff.isoformat(),
        "published_only": True,
    }
    required = {
        "generated_content": {
            "content",
            "content_type",
            "id",
            "published",
            "published_at",
            "published_url",
        },
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and not columns.issubset(schema[table])
    }
    if missing_tables or missing_columns:
        return _empty_report(
            generated_at.isoformat(),
            filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _blog_rows(conn, schema, cutoff=cutoff)
    gaps: list[BlogPublicationMetadataGap] = []
    for row in rows:
        title_source = _title_source(row["content"])
        base = {
            "content_id": int(row["id"]),
            "content_type": str(row["content_type"]),
            "published_at": _optional_text(row["published_at"]),
            "published_url": _optional_text(row["published_url"]),
            "title_source": title_source,
            "content_preview": _preview(row["content"]),
        }
        if not _optional_text(row["published_url"]):
            gaps.append(
                BlogPublicationMetadataGap(
                    gap_type=GAP_MISSING_PUBLISHED_URL,
                    **base,
                )
            )
        if not _optional_text(row["published_at"]):
            gaps.append(
                BlogPublicationMetadataGap(
                    gap_type=GAP_MISSING_PUBLISHED_AT,
                    **base,
                )
            )
        if title_source is None:
            gaps.append(
                BlogPublicationMetadataGap(
                    gap_type=GAP_MISSING_TITLE,
                    **base,
                )
            )

    gaps.sort(key=lambda gap: (gap.content_id, gap.gap_type))
    return BlogPublicationMetadataGapReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "gaps_found": len(gaps),
            "posts_checked": len(rows),
        },
        gaps=tuple(gaps),
    )


def format_blog_publication_metadata_gaps_json(
    report: BlogPublicationMetadataGapReport,
) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_blog_publication_metadata_gaps_text(
    report: BlogPublicationMetadataGapReport,
) -> str:
    """Render a compact human-readable report."""
    lines = [
        "Blog Publication Metadata Gaps",
        f"Window: {report.filters['days']} days",
        (
            f"Checked: posts={report.totals['posts_checked']} "
            f"gaps={report.totals['gaps_found']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        lines.append(
            "Missing columns: "
            + ", ".join(
                f"{table}.{column}"
                for table, columns in sorted(report.missing_columns.items())
                for column in columns
            )
        )
    if not report.gaps:
        lines.append("No blog publication metadata gaps found.")
        return "\n".join(lines)

    lines.append("Gaps:")
    for gap in report.gaps:
        lines.append(
            "  - "
            f"content_id={gap.content_id} type={gap.gap_type} "
            f"published_url={gap.published_url or '-'} "
            f"published_at={gap.published_at or '-'} "
            f"title_source={gap.title_source or '-'}"
        )
    return "\n".join(lines)


def _blog_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    created_filter = (
        "AND datetime(COALESCE(created_at, ?)) >= datetime(?)"
        if "created_at" in columns
        else ""
    )
    params: list[Any] = ["blog_post", cutoff.isoformat(), cutoff.isoformat()]
    if not created_filter:
        params = ["blog_post"]
    rows = conn.execute(
        f"""SELECT id, content_type, content, published_url, published_at
            FROM generated_content
            WHERE content_type = ?
              AND published = 1
              {created_filter}
            ORDER BY id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _title_source(content: Any) -> str | None:
    text = str(content or "")
    if text.startswith("---\n"):
        frontmatter, _body, _issues = parse_markdown_frontmatter(text)
        title = frontmatter.get("title")
        if isinstance(title, str) and title.strip():
            return "frontmatter"
    if re.search(r"(?m)^#\s+\S.*$", text):
        return "h1"
    return None


def _preview(value: Any, limit: int = 120) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:limit]


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _empty_report(
    generated_at: str,
    filters: dict[str, Any],
    *,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> BlogPublicationMetadataGapReport:
    return BlogPublicationMetadataGapReport(
        generated_at=generated_at,
        filters=filters,
        totals={"gaps_found": 0, "posts_checked": 0},
        gaps=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


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
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        str(row["name"]): {
            str(column["name"])
            for column in conn.execute(f"PRAGMA table_info({row['name']})").fetchall()
        }
        for row in tables
    }
