"""Report source artifact coverage for generated blog drafts."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Sequence


DEFAULT_MIN_SOURCES = 3
DEFAULT_MIN_SOURCE_TYPES = 2
BLOG_CONTENT_TYPES = frozenset({"blog", "blog_post", "long_post"})
PREVIEW_LENGTH = 120


@dataclass(frozen=True)
class BlogDraftRecord:
    """One generated blog draft to check for source coverage."""

    draft_id: int
    title: str | None = None
    content_type: str | None = None
    content_preview: str | None = None
    created_at: str | None = None


@dataclass(frozen=True)
class BlogSourceLinkRecord:
    """One source artifact linked to a draft."""

    draft_id: int
    source_type: str
    source_id: str
    label: str | None = None


@dataclass(frozen=True)
class BlogDraftSourceCoverage:
    """Source coverage status for one blog draft."""

    draft_id: int
    title: str | None
    content_type: str | None
    content_preview: str | None
    created_at: str | None
    total_source_count: int
    source_counts_by_type: dict[str, int]
    source_type_count: int
    ok: bool
    warnings: tuple[str, ...]
    missing_source_hints: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["source_counts_by_type"] = dict(sorted(self.source_counts_by_type.items()))
        data["warnings"] = list(self.warnings)
        data["missing_source_hints"] = list(self.missing_source_hints)
        return data


@dataclass(frozen=True)
class BlogSourceCoverageReport:
    """Read-only source coverage report for blog drafts."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    counts: dict[str, int]
    source_counts_by_type: dict[str, int]
    drafts: tuple[BlogDraftSourceCoverage, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocking_issue_count(self) -> int:
        return self.counts["warning_drafts"]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "blocking_issue_count": self.blocking_issue_count,
            "counts": dict(self.counts),
            "drafts": [draft.to_dict() for draft in self.drafts],
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "source_counts_by_type": dict(sorted(self.source_counts_by_type.items())),
        }


def build_blog_source_coverage_report(
    db_or_conn: Any | None = None,
    *,
    drafts: Sequence[BlogDraftRecord | dict[str, Any]] | None = None,
    source_links: Sequence[BlogSourceLinkRecord | dict[str, Any]] | None = None,
    min_sources: int = DEFAULT_MIN_SOURCES,
    min_source_types: int = DEFAULT_MIN_SOURCE_TYPES,
    now: datetime | None = None,
) -> BlogSourceCoverageReport:
    """Return source artifact coverage for blog drafts."""

    if min_sources < 0:
        raise ValueError("min_sources must be non-negative")
    if min_source_types < 0:
        raise ValueError("min_source_types must be non-negative")
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    filters = {"min_sources": min_sources, "min_source_types": min_source_types}
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] = {}

    if drafts is None:
        if db_or_conn is None:
            raise ValueError("db_or_conn is required when drafts are not provided")
        conn = _connection(db_or_conn)
        schema = _schema(conn)
        missing_tables, missing_columns = _schema_gaps(schema)
        draft_records = _load_blog_drafts(conn, schema)
        link_records = _load_source_links(conn, schema, draft_records)
    else:
        draft_records = [_draft_record(item) for item in drafts]
        link_records = [_source_link_record(item) for item in (source_links or ())]

    rows = _coverage_rows(
        draft_records,
        link_records,
        min_sources=min_sources,
        min_source_types=min_source_types,
    )
    totals = Counter()
    for row in rows:
        totals.update(row.source_counts_by_type)
    return BlogSourceCoverageReport(
        artifact_type="blog_source_coverage",
        generated_at=generated_at.isoformat(),
        filters=filters,
        counts={
            "drafts": len(rows),
            "passing_drafts": sum(1 for row in rows if row.ok),
            "warning_drafts": sum(1 for row in rows if not row.ok),
            "total_sources": sum(totals.values()),
        },
        source_counts_by_type=dict(totals),
        drafts=tuple(rows),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_blog_source_coverage_json(report: BlogSourceCoverageReport) -> str:
    """Serialize a blog source coverage report as deterministic JSON."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_blog_source_coverage_markdown(report: BlogSourceCoverageReport) -> str:
    """Render a deterministic markdown source coverage report."""

    filters = report.filters
    counts = report.counts
    lines = [
        "# Blog Source Coverage",
        "",
        f"- Generated: {report.generated_at}",
        (
            f"- Filters: min_sources={filters['min_sources']} "
            f"min_source_types={filters['min_source_types']}"
        ),
        (
            f"- Drafts: {counts['drafts']} checked, {counts['passing_drafts']} passing, "
            f"{counts['warning_drafts']} with warnings"
        ),
        f"- Sources: {counts['total_sources']} total",
    ]
    if report.source_counts_by_type:
        lines.append("- Source types: " + _format_counts(report.source_counts_by_type))
    if report.missing_tables:
        lines.append("- Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = [
            f"{table}.{column}"
            for table, columns in sorted(report.missing_columns.items())
            for column in columns
        ]
        lines.append("- Missing columns: " + ", ".join(missing))

    if not report.drafts:
        lines.append("")
        lines.append("No blog drafts matched.")
        return "\n".join(lines)

    lines.append("")
    lines.append("## Drafts")
    for draft in report.drafts:
        status = "pass" if draft.ok else "warning"
        title = f" title={draft.title!r}" if draft.title else ""
        lines.append(
            f"- `{draft.draft_id}` {status} sources={draft.total_source_count} "
            f"types={draft.source_type_count}{title}"
        )
        lines.append(f"  counts: {_format_counts(draft.source_counts_by_type) or 'none'}")
        if draft.missing_source_hints:
            lines.append(f"  hints: {'; '.join(draft.missing_source_hints)}")
        if draft.warnings:
            lines.append(f"  warnings: {'; '.join(draft.warnings)}")
    return "\n".join(lines)


def _coverage_rows(
    drafts: Sequence[BlogDraftRecord],
    source_links: Sequence[BlogSourceLinkRecord],
    *,
    min_sources: int,
    min_source_types: int,
) -> list[BlogDraftSourceCoverage]:
    links_by_draft: dict[int, set[tuple[str, str]]] = defaultdict(set)
    for link in source_links:
        source_type = _normalize_source_type(link.source_type)
        source_id = _clean_text(link.source_id)
        if source_type and source_id:
            links_by_draft[int(link.draft_id)].add((source_type, source_id))

    rows = []
    for draft in sorted(drafts, key=lambda item: item.draft_id):
        counts = Counter(source_type for source_type, _source_id in links_by_draft[draft.draft_id])
        total = sum(counts.values())
        type_count = len(counts)
        warnings, hints = _warnings_and_hints(
            total,
            type_count,
            counts,
            min_sources=min_sources,
            min_source_types=min_source_types,
        )
        rows.append(
            BlogDraftSourceCoverage(
                draft_id=draft.draft_id,
                title=draft.title,
                content_type=draft.content_type,
                content_preview=draft.content_preview,
                created_at=draft.created_at,
                total_source_count=total,
                source_counts_by_type=dict(counts),
                source_type_count=type_count,
                ok=not warnings,
                warnings=tuple(warnings),
                missing_source_hints=tuple(hints),
            )
        )
    return rows


def _warnings_and_hints(
    total: int,
    type_count: int,
    counts: Counter[str],
    *,
    min_sources: int,
    min_source_types: int,
) -> tuple[list[str], list[str]]:
    warnings: list[str] = []
    hints: list[str] = []
    if total < min_sources:
        missing = min_sources - total
        warnings.append(f"needs {missing} more source artifact{'s' if missing != 1 else ''}")
        hints.append(
            "Attach source commits, Claude sessions, curated articles, or published posts "
            "before publication."
        )
    if type_count < min_source_types:
        missing_types = min_source_types - type_count
        warnings.append(f"needs {missing_types} more source type{'s' if missing_types != 1 else ''}")
        preferred = [
            label
            for label in ("commit", "claude_session", "curated_article", "published_post")
            if counts.get(label, 0) == 0
        ]
        if preferred:
            hints.append("Add evidence from another source type: " + ", ".join(preferred[:3]) + ".")
    return warnings, hints


def _load_blog_drafts(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[BlogDraftRecord]:
    columns = schema.get("generated_content", set())
    if not columns or "id" not in columns:
        return []
    select_columns = [
        "gc.id",
        _column_expr(columns, "content_type", "gc"),
        _column_expr(columns, "content", "gc"),
        _column_expr(columns, "created_at", "gc"),
    ]
    has_variants = _content_variants_available(schema)
    joins = ""
    blog_filter = "gc.content_type IN ({})".format(
        ", ".join("?" for _ in sorted(BLOG_CONTENT_TYPES))
    )
    params: list[Any] = sorted(BLOG_CONTENT_TYPES)
    if has_variants:
        joins = """LEFT JOIN content_variants bv
                      ON bv.content_id = gc.id
                     AND (bv.platform = 'blog' OR bv.variant_type LIKE '%blog%')"""
        blog_filter = f"({blog_filter} OR bv.content_id IS NOT NULL)"
    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM generated_content gc
            {joins}
            WHERE {blog_filter}
            GROUP BY gc.id
            ORDER BY gc.id ASC""",
        tuple(params),
    ).fetchall()
    return [
        BlogDraftRecord(
            draft_id=int(row["id"]),
            title=_title_from_content(row["content"] if "content" in row.keys() else None),
            content_type=row["content_type"] if "content_type" in row.keys() else None,
            content_preview=_preview(row["content"] if "content" in row.keys() else None),
            created_at=row["created_at"] if "created_at" in row.keys() else None,
        )
        for row in rows
    ]


def _load_source_links(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    drafts: Sequence[BlogDraftRecord],
) -> list[BlogSourceLinkRecord]:
    if not drafts:
        return []
    draft_ids = [draft.draft_id for draft in drafts]
    links: list[BlogSourceLinkRecord] = []
    columns = schema.get("generated_content", set())
    if columns:
        selected = [
            "id",
            _column_expr(columns, "source_commits"),
            _column_expr(columns, "source_messages"),
            _column_expr(columns, "source_activity_ids"),
        ]
        rows = conn.execute(
            f"""SELECT {', '.join(selected)}
                FROM generated_content
                WHERE id IN ({', '.join('?' for _ in draft_ids)})
                ORDER BY id ASC""",
            tuple(draft_ids),
        ).fetchall()
        for row in rows:
            links.extend(_json_source_links(row["id"], "commit", row["source_commits"]))
            links.extend(_json_source_links(row["id"], "claude_session", row["source_messages"]))
            links.extend(_json_source_links(row["id"], "github_activity", row["source_activity_ids"]))

    if _knowledge_links_available(schema):
        rows = conn.execute(
            f"""SELECT ckl.content_id, k.id AS knowledge_id, k.source_type, k.source_id, k.source_url
                FROM content_knowledge_links ckl
                INNER JOIN knowledge k ON k.id = ckl.knowledge_id
                WHERE ckl.content_id IN ({', '.join('?' for _ in draft_ids)})
                ORDER BY ckl.content_id ASC, k.id ASC""",
            tuple(draft_ids),
        ).fetchall()
        for row in rows:
            source_type = _knowledge_source_type(row["source_type"])
            source_id = row["source_id"] or row["source_url"] or row["knowledge_id"]
            links.append(
                BlogSourceLinkRecord(
                    draft_id=int(row["content_id"]),
                    source_type=source_type,
                    source_id=str(source_id),
                )
            )
    return links


def _json_source_links(draft_id: int, source_type: str, value: Any) -> list[BlogSourceLinkRecord]:
    links = []
    for item in _parse_json_list(value):
        text = _clean_text(item)
        if text:
            links.append(BlogSourceLinkRecord(draft_id=int(draft_id), source_type=source_type, source_id=text))
    return links


def _parse_json_list(value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        parsed = value
    else:
        try:
            parsed = json.loads(str(value))
        except (TypeError, json.JSONDecodeError):
            return []
    return parsed if isinstance(parsed, list) else []


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {"generated_content": ("id",)}
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and any(column not in schema.get(table, set()) for column in columns)
    }
    return missing_tables, missing_columns


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        name = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        schema[name] = {
            str(column["name"] if isinstance(column, sqlite3.Row) else column[1])
            for column in conn.execute(f"PRAGMA table_info({_quote_identifier(name)})")
        }
    return schema


def _content_variants_available(schema: dict[str, set[str]]) -> bool:
    return {"content_id", "platform", "variant_type"}.issubset(schema.get("content_variants", set()))


def _knowledge_links_available(schema: dict[str, set[str]]) -> bool:
    return {"content_id", "knowledge_id"}.issubset(schema.get("content_knowledge_links", set())) and {
        "id",
        "source_type",
    }.issubset(schema.get("knowledge", set()))


def _column_expr(columns: set[str], column: str, table_alias: str | None = None) -> str:
    if column in columns:
        prefix = f"{table_alias}." if table_alias else ""
        return f"{prefix}{column} AS {column}"
    return f"NULL AS {column}"


def _draft_record(value: BlogDraftRecord | dict[str, Any]) -> BlogDraftRecord:
    if isinstance(value, BlogDraftRecord):
        return value
    draft_id = value.get("draft_id", value.get("id", value.get("content_id")))
    if draft_id is None:
        raise ValueError("draft record must include draft_id, id, or content_id")
    return BlogDraftRecord(
        draft_id=int(draft_id),
        title=value.get("title"),
        content_type=value.get("content_type"),
        content_preview=value.get("content_preview") or _preview(value.get("content")),
        created_at=value.get("created_at"),
    )


def _source_link_record(value: BlogSourceLinkRecord | dict[str, Any]) -> BlogSourceLinkRecord:
    if isinstance(value, BlogSourceLinkRecord):
        return value
    draft_id = value.get("draft_id", value.get("content_id"))
    if draft_id is None:
        raise ValueError("source link record must include draft_id or content_id")
    return BlogSourceLinkRecord(
        draft_id=int(draft_id),
        source_type=str(value.get("source_type", "")),
        source_id=str(value.get("source_id", value.get("id", ""))),
        label=value.get("label"),
    )


def _normalize_source_type(value: Any) -> str:
    cleaned = _clean_text(value).lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "claude": "claude_session",
        "claude_message": "claude_session",
        "message": "claude_session",
        "messages": "claude_session",
        "commit_sha": "commit",
        "commits": "commit",
        "curated_blog": "curated_article",
        "curated_newsletter": "curated_article",
        "blog": "curated_article",
        "newsletter": "curated_article",
        "own_post": "published_post",
        "post": "published_post",
    }
    return aliases.get(cleaned, cleaned)


def _knowledge_source_type(value: Any) -> str:
    normalized = _normalize_source_type(value)
    if normalized.startswith("curated_"):
        return "curated_article"
    if normalized == "own_conversation":
        return "claude_session"
    return normalized


def _format_counts(counts: dict[str, int]) -> str:
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))


def _title_from_content(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    first = text.splitlines()[0].strip("# ")
    return _preview(first, 80) if first else None


def _preview(value: Any, width: int = PREVIEW_LENGTH) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    if len(text) <= width:
        return text
    return text[: width - 3].rstrip() + "..."


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("db_or_conn must be a sqlite3.Connection or Database-like object")
    conn.row_factory = sqlite3.Row
    return conn


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
