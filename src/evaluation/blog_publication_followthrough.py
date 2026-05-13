"""Find blog content that did not complete publication followthrough."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 60
DEFAULT_MIN_AGE_DAYS = 3
BLOG_TYPES = {"blog_post", "long_form"}


@dataclass(frozen=True)
class BlogPublicationFollowthroughIssue:
    issue_type: str
    content_id: int | None
    publication_id: int | None
    age_days: float | None
    title: str | None
    content_preview: str | None
    blog_url: str | None
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BlogPublicationFollowthroughReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    issues: tuple[BlogPublicationFollowthroughIssue, ...]
    empty_state: dict[str, Any]
    missing_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "blog_publication_followthrough",
            "empty_state": dict(self.empty_state),
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "issues": [issue.to_dict() for issue in self.issues],
            "missing_tables": list(self.missing_tables),
            "totals": dict(self.totals),
        }


def build_blog_publication_followthrough_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_age_days: int = DEFAULT_MIN_AGE_DAYS,
    issue_type: str | None = None,
    now: datetime | None = None,
) -> BlogPublicationFollowthroughReport:
    if days <= 0:
        raise ValueError("days must be positive")
    if min_age_days < 0:
        raise ValueError("min_age_days must be non-negative")
    valid = {"unpublished_blog_draft", "missing_blog_url", "orphan_blog_publication"}
    if issue_type and issue_type not in valid:
        raise ValueError(f"invalid issue_type: {issue_type}")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"days": days, "min_age_days": min_age_days, "issue_type": issue_type}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "generated_content" not in schema:
        return _report(generated_at, filters, (), 0, ("generated_content",))
    pubs_table = _publication_table(schema)
    contents = _load_content(conn, schema, cutoff.isoformat())
    publications = _load_publications(conn, pubs_table, schema.get(pubs_table or "", set())) if pubs_table else []
    by_content: dict[int, list[dict[str, Any]]] = {}
    for pub in publications:
        if pub.get("content_id") is not None:
            by_content.setdefault(int(pub["content_id"]), []).append(pub)
    issues: list[BlogPublicationFollowthroughIssue] = []
    for content in contents:
        content_id = int(content["id"])
        age = _age_days(generated_at, content.get("created_at_dt"))
        if age is not None and age < min_age_days:
            continue
        pubs = by_content.get(content_id, [])
        if not pubs:
            issues.append(_issue("unpublished_blog_draft", content, None, age))
        elif any(not _clean(pub.get("blog_url")) for pub in pubs):
            issues.append(_issue("missing_blog_url", content, pubs[0], age))
    known_ids = {int(row["id"]) for row in contents}
    for pub in publications:
        cid = pub.get("content_id")
        if cid is None or int(cid) not in known_ids:
            issues.append(_issue("orphan_blog_publication", None, pub, _age_days(generated_at, pub.get("published_at_dt"))))
    if issue_type:
        issues = [issue for issue in issues if issue.issue_type == issue_type]
    issues.sort(key=lambda item: (item.issue_type, -(item.age_days or 0), item.content_id or 0))
    return _report(generated_at, filters, tuple(issues), len(contents) + len(publications), ())


def format_blog_publication_followthrough_json(report: BlogPublicationFollowthroughReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_blog_publication_followthrough_text(report: BlogPublicationFollowthroughReport) -> str:
    lines = [
        "Blog Publication Followthrough",
        f"Window={report.filters['days']} days; min_age={report.filters['min_age_days']} days; issue_type={report.filters.get('issue_type') or 'all'}",
        f"Rows scanned={report.totals['rows_scanned']}; issues={report.totals['issue_count']}",
        "",
    ]
    if not report.issues:
        lines.append(report.empty_state["message"])
        return "\n".join(lines)
    for issue in report.issues:
        lines.append(f"- {issue.issue_type} content={issue.content_id or '-'} publication={issue.publication_id or '-'} age_days={issue.age_days} url={issue.blog_url or '-'}")
        lines.append(f"  preview={issue.content_preview or issue.title or '-'} action={issue.recommended_action}")
    return "\n".join(lines)


def _load_content(conn: sqlite3.Connection, schema: dict[str, set[str]], cutoff: str) -> list[dict[str, Any]]:
    cols = schema["generated_content"]
    type_col = _first(cols, ("content_type", "type", "format"))
    created_col = _first(cols, ("created_at", "generated_at", "updated_at"))
    text_col = _first(cols, ("content", "body", "text", "generated_text"))
    title_col = _first(cols, ("title", "headline"))
    if "id" not in cols or not type_col:
        return []
    where = [f"{type_col} IN ({','.join('?' for _ in BLOG_TYPES)})"]
    params: list[Any] = sorted(BLOG_TYPES)
    if created_col:
        where.append(f"{created_col} >= ?")
        params.append(cutoff)
    sql = f"""SELECT id, {type_col} AS content_type,
                     {created_col if created_col else 'NULL'} AS created_at,
                     {text_col if text_col else 'NULL'} AS content_text,
                     {title_col if title_col else 'NULL'} AS title
              FROM generated_content WHERE {' AND '.join(where)}"""
    rows = []
    for row in conn.execute(sql, params).fetchall():
        item = dict(row)
        item["created_at_dt"] = _parse_ts(item.get("created_at"))
        rows.append(item)
    return rows


def _load_publications(conn: sqlite3.Connection, table: str | None, cols: set[str]) -> list[dict[str, Any]]:
    if not table or "id" not in cols:
        return []
    content_id_col = _first(cols, ("content_id", "generated_content_id", "source_content_id"))
    platform_col = _first(cols, ("platform", "channel", "publication_type"))
    url_col = _first(cols, ("blog_url", "url", "published_url", "platform_url"))
    published_col = _first(cols, ("published_at", "created_at", "exported_at"))
    where = []
    if platform_col:
        where.append(f"LOWER(COALESCE({platform_col}, '')) LIKE '%blog%'")
    sql = f"""SELECT id,
                     {content_id_col if content_id_col else 'NULL'} AS content_id,
                     {url_col if url_col else 'NULL'} AS blog_url,
                     {published_col if published_col else 'NULL'} AS published_at
              FROM {table}"""
    if where:
        sql += " WHERE " + " AND ".join(where)
    rows = []
    for row in conn.execute(sql).fetchall():
        item = dict(row)
        item["published_at_dt"] = _parse_ts(item.get("published_at"))
        rows.append(item)
    return rows


def _issue(kind: str, content: dict[str, Any] | None, pub: dict[str, Any] | None, age: float | None) -> BlogPublicationFollowthroughIssue:
    actions = {
        "unpublished_blog_draft": "Publish, archive, or deliberately reschedule this stale blog draft.",
        "missing_blog_url": "Backfill the canonical blog URL or rerun the blog export.",
        "orphan_blog_publication": "Attach this publication row to generated_content or remove the orphan export record.",
    }
    return BlogPublicationFollowthroughIssue(
        issue_type=kind,
        content_id=int(content["id"]) if content and content.get("id") is not None else (int(pub["content_id"]) if pub and pub.get("content_id") is not None else None),
        publication_id=int(pub["id"]) if pub and pub.get("id") is not None else None,
        age_days=age,
        title=_clean(content.get("title")) if content else None,
        content_preview=_preview(content.get("content_text")) if content else None,
        blog_url=_clean(pub.get("blog_url")) if pub else None,
        recommended_action=actions[kind],
    )


def _publication_table(schema: dict[str, set[str]]) -> str | None:
    for table in ("content_publications", "blog_publications", "content_exports"):
        if table in schema:
            return table
    return None


def _report(generated_at: datetime, filters: dict[str, Any], issues: tuple[BlogPublicationFollowthroughIssue, ...], scanned: int, missing: tuple[str, ...]) -> BlogPublicationFollowthroughReport:
    return BlogPublicationFollowthroughReport(generated_at.isoformat(), filters, {"rows_scanned": scanned, "issue_count": len(issues)}, issues, {"is_empty": not issues, "message": "No blog publication followthrough issues found." if not missing else "Generated content schema is unavailable."}, missing)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _first(columns: set[str], names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in columns), None)


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _age_days(now: datetime, then: datetime | None) -> float | None:
    return None if then is None else round((now - then).total_seconds() / 86400, 2)


def _preview(value: Any) -> str | None:
    text = _clean(value)
    return text[:120] if text else None


def _clean(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None
