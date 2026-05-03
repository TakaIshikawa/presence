"""Report blog draft CTA intent alignment with draft theme or category."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any, Iterable, Mapping


BLOG_CONTENT_TYPES = frozenset({"blog", "blog_post", "long_post"})
ENDING_SCAN_CHARS = 1400
PREVIEW_LENGTH = 100

CTA_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "subscribe",
        (
            r"\bsubscribe\b",
            r"\bnewsletter\b",
            r"\binbox\b",
            r"\brss feed\b",
            r"\bget future posts\b",
            r"\bjoin (?:the )?(?:mailing list|email list)\b",
        ),
    ),
    (
        "follow",
        (
            r"\bfollow (?:me|us|along|on)\b",
            r"\bbluesky\b",
            r"\bmastodon\b",
            r"\blinkedin\b",
            r"\bgithub\b",
            r"\bx\.com\b",
            r"\btwitter\b",
        ),
    ),
    (
        "reply",
        (
            r"\breply\b",
            r"\bcomment\b",
            r"\btell me\b",
            r"\bshare your\b",
            r"\bwhat do you think\b",
            r"\bi[' ]?d love to hear\b",
            r"\bemail me\b",
        ),
    ),
    (
        "read_related_post",
        (
            r"\bread (?:the )?(?:next|related|companion|previous) post\b",
            r"\brelated post\b",
            r"\bsee also\b",
            r"\bcontinue (?:with|reading)\b",
            r"\bcheck out (?:the|this|my) (?:post|guide|article)\b",
        ),
    ),
    (
        "try_project",
        (
            r"\btry (?:it|this|the project|the demo)\b",
            r"\binstall\b",
            r"\bnpm install\b",
            r"\bpip install\b",
            r"\bclone (?:the )?(?:repo|repository)\b",
            r"\bdownload\b",
            r"\bopen the demo\b",
            r"\buse (?:the )?(?:project|tool|template|starter)\b",
        ),
    ),
)

THEME_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "project",
        (
            r"\b(project|tool|demo|app|library|package|template|starter|github repo)\b",
            r"\b(api|cli|sdk|prototype|implementation)\b",
        ),
    ),
    (
        "discussion",
        (
            r"\b(opinion|reflection|lesson|takeaway|mistake|tradeoff|question|debate)\b",
            r"\bwhat changed\b",
        ),
    ),
    (
        "guide",
        (
            r"\b(guide|how to|tutorial|walkthrough|playbook|checklist|reference)\b",
            r"\bpatterns?\b",
        ),
    ),
    (
        "community",
        (
            r"\b(community|social|thread|updates|build in public|following along)\b",
            r"\bconversation\b",
        ),
    ),
    (
        "newsletter",
        (
            r"\b(newsletter|digest|roundup|weekly|monthly|issue|archive)\b",
            r"\bupdates\b",
        ),
    ),
)

THEME_EXPECTATIONS: dict[str, tuple[str, ...]] = {
    "project": ("try_project", "follow"),
    "discussion": ("reply", "follow"),
    "guide": ("read_related_post", "subscribe"),
    "community": ("follow", "reply"),
    "newsletter": ("subscribe", "read_related_post"),
}


@dataclass(frozen=True)
class BlogDraftCtaRecord:
    """Minimum draft fields needed for CTA alignment checks."""

    draft_id: int
    title: str | None = None
    content: str = ""
    category: str | None = None
    theme: str | None = None
    content_type: str | None = None


@dataclass(frozen=True)
class BlogCtaAlignmentRow:
    """CTA alignment status for one blog draft."""

    draft_id: int
    title: str | None
    detected_theme: str
    cta_intent: str | None
    alignment_status: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BlogCtaAlignmentReport:
    """Read-only CTA alignment report for blog drafts."""

    artifact_type: str
    generated_at: str
    counts: dict[str, int]
    rows: tuple[BlogCtaAlignmentRow, ...]
    missing_tables: tuple[str, ...] = ()

    @property
    def blocking_issue_count(self) -> int:
        return self.counts["missing_cta"] + self.counts["mismatched_cta"]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "blocking_issue_count": self.blocking_issue_count,
            "counts": dict(self.counts),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
        }


def build_blog_cta_alignment_report(
    db_or_conn: Any | None = None,
    *,
    drafts: Iterable[BlogDraftCtaRecord | Mapping[str, Any]] | None = None,
    include_aligned: bool = False,
    now: datetime | None = None,
) -> BlogCtaAlignmentReport:
    """Return CTA alignment rows for blog drafts."""

    generated_at = _as_utc(now or datetime.now(timezone.utc)).isoformat()
    missing_tables: tuple[str, ...] = ()
    if drafts is None:
        if db_or_conn is None:
            raise ValueError("db_or_conn is required when drafts are not provided")
        conn = _connection(db_or_conn)
        schema = _schema(conn)
        if "generated_content" not in schema:
            draft_records = []
            missing_tables = ("generated_content",)
        else:
            draft_records = _load_blog_drafts(conn, schema)
    else:
        draft_records = [_coerce_draft(record) for record in drafts]

    all_rows = tuple(_alignment_row(draft) for draft in draft_records)
    rows = all_rows if include_aligned else tuple(
        row for row in all_rows if row.alignment_status in {"missing_cta", "mismatched_cta"}
    )
    return BlogCtaAlignmentReport(
        artifact_type="blog_cta_alignment",
        generated_at=generated_at,
        counts={
            "drafts": len(all_rows),
            "aligned": sum(1 for row in all_rows if row.alignment_status == "aligned"),
            "missing_cta": sum(1 for row in all_rows if row.alignment_status == "missing_cta"),
            "mismatched_cta": sum(1 for row in all_rows if row.alignment_status == "mismatched_cta"),
            "unknown_theme": sum(1 for row in all_rows if row.detected_theme == "unknown"),
            "reported": len(rows),
        },
        rows=rows,
        missing_tables=missing_tables,
    )


def format_blog_cta_alignment_json(report: BlogCtaAlignmentReport) -> str:
    """Serialize a CTA alignment report as deterministic JSON."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def detect_cta_intent(content: str) -> str | None:
    """Detect the dominant CTA intent from the draft ending."""

    ending = _normalize_markdown(content)[-ENDING_SCAN_CHARS:]
    if not ending:
        return None
    matches: list[tuple[int, int, str]] = []
    for priority, (intent, patterns) in enumerate(CTA_PATTERNS):
        for pattern in patterns:
            match = re.search(pattern, ending, flags=re.IGNORECASE)
            if match:
                matches.append((match.start(), priority, intent))
                break
    if not matches:
        return None
    return max(matches, key=lambda item: (item[0], -item[1]))[2]


def detect_draft_theme(draft: BlogDraftCtaRecord) -> str:
    """Detect a stable draft theme from explicit metadata first, then content."""

    explicit = _clean_text(draft.theme or draft.category)
    if explicit:
        normalized = _theme_from_text(explicit)
        if normalized != "unknown":
            return normalized

    haystack = " ".join(
        value
        for value in (
            draft.title or "",
            draft.content_type or "",
            draft.category or "",
            draft.theme or "",
            draft.content,
        )
        if value
    )
    return _theme_from_text(haystack)


def _alignment_row(draft: BlogDraftCtaRecord) -> BlogCtaAlignmentRow:
    theme = detect_draft_theme(draft)
    intent = detect_cta_intent(draft.content)
    expected = THEME_EXPECTATIONS.get(theme, ())
    if intent is None:
        return BlogCtaAlignmentRow(
            draft_id=draft.draft_id,
            title=draft.title,
            detected_theme=theme,
            cta_intent=None,
            alignment_status="missing_cta",
            reason="draft ending does not contain a recognized CTA",
        )
    if expected and intent not in expected:
        return BlogCtaAlignmentRow(
            draft_id=draft.draft_id,
            title=draft.title,
            detected_theme=theme,
            cta_intent=intent,
            alignment_status="mismatched_cta",
            reason=f"{theme} drafts expect one of: {', '.join(expected)}",
        )
    return BlogCtaAlignmentRow(
        draft_id=draft.draft_id,
        title=draft.title,
        detected_theme=theme,
        cta_intent=intent,
        alignment_status="aligned",
        reason="CTA intent matches detected draft theme",
    )


def _load_blog_drafts(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[BlogDraftCtaRecord]:
    gc_columns = schema["generated_content"]
    if "id" not in gc_columns:
        return []
    select_columns = [
        "gc.id",
        _column_expr(gc_columns, "content_type", "gc"),
        _column_expr(gc_columns, "content", "gc"),
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
        select_columns.extend(
            [
                _column_expr(schema["content_variants"], "content", "bv", "variant_content"),
                _column_expr(schema["content_variants"], "metadata", "bv", "variant_metadata"),
            ]
        )
    else:
        select_columns.extend(["NULL AS variant_content", "NULL AS variant_metadata"])

    if _planned_topics_available(schema):
        joins += " LEFT JOIN planned_topics pt ON pt.content_id = gc.id"
        select_columns.extend(
            [
                _column_expr(schema["planned_topics"], "topic", "pt", "planned_topic"),
                _column_expr(schema["planned_topics"], "angle", "pt", "planned_angle"),
            ]
        )
    else:
        select_columns.extend(["NULL AS planned_topic", "NULL AS planned_angle"])

    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM generated_content gc
            {joins}
            WHERE {blog_filter}
            GROUP BY gc.id
            ORDER BY gc.id ASC""",
        tuple(params),
    ).fetchall()
    return [_draft_from_row(row) for row in rows]


def _draft_from_row(row: sqlite3.Row) -> BlogDraftCtaRecord:
    content = str(row["variant_content"] or row["content"] or "")
    metadata = _json_object(row["variant_metadata"])
    category = _first_text(
        metadata.get("theme"),
        metadata.get("category"),
        metadata.get("topic"),
        metadata.get("tags"),
        row["planned_topic"],
        row["planned_angle"],
    )
    return BlogDraftCtaRecord(
        draft_id=int(row["id"]),
        title=_title_from_content(content),
        content=content,
        category=category,
        content_type=row["content_type"],
    )


def _coerce_draft(record: BlogDraftCtaRecord | Mapping[str, Any]) -> BlogDraftCtaRecord:
    if isinstance(record, BlogDraftCtaRecord):
        return record
    draft_id = record.get("draft_id", record.get("id", record.get("content_id")))
    if draft_id is None:
        raise ValueError("draft record must include draft_id, id, or content_id")
    content = str(record.get("content") or record.get("body") or "")
    return BlogDraftCtaRecord(
        draft_id=int(draft_id),
        title=record.get("title") or _title_from_content(content),
        content=content,
        category=record.get("category") or record.get("topic"),
        theme=record.get("theme"),
        content_type=record.get("content_type"),
    )


def _theme_from_text(text: str) -> str:
    normalized = _normalize_markdown(text)
    aliases = {
        "tutorial": "guide",
        "howto": "guide",
        "how_to": "guide",
        "tooling": "project",
        "product": "project",
        "social": "community",
        "updates": "newsletter",
        "digest": "newsletter",
    }
    compact = normalized.replace("-", "_").replace(" ", "_")
    if compact in THEME_EXPECTATIONS:
        return compact
    if compact in aliases:
        return aliases[compact]
    for theme, patterns in THEME_PATTERNS:
        if any(re.search(pattern, normalized, flags=re.IGNORECASE) for pattern in patterns):
            return theme
    return "unknown"


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _first_text(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, list | tuple):
            text = _clean_text(" ".join(str(item) for item in value))
        else:
            text = _clean_text(value)
        if text:
            return text
    return None


def _normalize_markdown(value: Any) -> str:
    text = str(value or "").casefold()
    text = re.sub(r"\A---\s*\n.*?\n---\s*", " ", text, flags=re.DOTALL)
    text = re.sub(r"`{1,3}[^`]*`{1,3}", " ", text)
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[*_~>#\[\]()`]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _title_from_content(value: Any) -> str | None:
    text = str(value or "").strip()
    frontmatter = re.search(r"\A---\s*\n(.*?)\n---", text, flags=re.DOTALL)
    if frontmatter:
        for line in frontmatter.group(1).splitlines():
            key, sep, val = line.partition(":")
            if sep and key.strip().casefold() == "title":
                return _preview(val.strip().strip("\"'"))
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return _preview(stripped[2:].strip())
    return None


def _preview(value: str | None, width: int = PREVIEW_LENGTH) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    return text if len(text) <= width else text[: width - 3].rstrip() + "..."


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("db_or_conn must be a sqlite3.Connection or Database-like object")
    conn.row_factory = sqlite3.Row
    return conn


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


def _planned_topics_available(schema: dict[str, set[str]]) -> bool:
    return {"content_id"}.issubset(schema.get("planned_topics", set()))


def _column_expr(
    columns: set[str],
    column: str,
    table_alias: str | None = None,
    output_name: str | None = None,
) -> str:
    alias = output_name or column
    if column in columns:
        prefix = f"{table_alias}." if table_alias else ""
        return f"{prefix}{column} AS {alias}"
    return f"NULL AS {alias}"


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
