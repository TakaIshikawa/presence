"""Report relationship and conversation context coverage for reply drafts."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from io import StringIO
import json
import sqlite3
from typing import Any, Mapping, Sequence


DEFAULT_STATUS = ("pending",)
RELATIONSHIP_KEYS = (
    "relationship_strength",
    "strength",
    "engagement_stage",
    "stage",
    "dunbar_tier",
    "tier",
    "relationship_notes",
    "relationship_summary",
    "notes",
    "display_name",
    "bio",
    "recent_interactions",
)
CONVERSATION_METADATA_KEYS = (
    "parent_post_text",
    "parent_post_id",
    "parent_post_uri",
    "parent_tweet_id",
    "conversation_id",
    "quoted_tweet_id",
    "quoted_text",
    "sibling_replies",
    "reply_root",
    "reply_parent",
)


@dataclass(frozen=True)
class ReplyContextCoverageDraft:
    """One drafted reply's context coverage classification."""

    reply_queue_id: int | None
    mention_id: str | None
    platform: str | None
    author_handle: str | None
    author_id: str | None
    detected_at: str | None
    has_relationship_context: bool
    has_conversation_context: bool

    @property
    def has_full_context(self) -> bool:
        return self.has_relationship_context and self.has_conversation_context

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["has_full_context"] = self.has_full_context
        return data


@dataclass(frozen=True)
class ReplyContextCoverageReport:
    """Read-only coverage report for inbound mentions and reply drafts."""

    ok: bool
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    drafts: tuple[ReplyContextCoverageDraft, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None
    missing_cultivate_tables: tuple[str, ...] = ()

    @property
    def blocking_issue_count(self) -> int:
        return self.totals["context_missing_drafts"]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_context_coverage",
            "ok": self.ok,
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "totals": dict(self.totals),
            "blocking_issue_count": self.blocking_issue_count,
            "drafts": [draft.to_dict() for draft in self.drafts],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_cultivate_tables": list(self.missing_cultivate_tables),
        }


def build_reply_context_coverage_report(
    db_or_conn: Any,
    *,
    start: str | None = None,
    end: str | None = None,
    status: str | Sequence[str] | None = DEFAULT_STATUS,
    platform: str | Sequence[str] | None = None,
    account: str | None = None,
    author: str | None = None,
    now: datetime | None = None,
) -> ReplyContextCoverageReport:
    """Return coverage counts for inbound mentions and drafted replies."""
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    statuses = _normalise_filter(status)
    platforms = _normalise_filter(platform)
    filters = {
        "start": start,
        "end": end,
        "status": list(statuses),
        "platform": list(platforms),
        "account": account,
        "author": author,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "reply_queue" not in schema:
        return _empty_report(generated_at, filters, missing_tables=("reply_queue",))

    required = {"id", "inbound_tweet_id", "draft_text"}
    missing_required = tuple(sorted(required - schema["reply_queue"]))
    if missing_required:
        return _empty_report(
            generated_at,
            filters,
            missing_columns={"reply_queue": missing_required},
        )

    people_rows, missing_cultivate_tables = _cultivate_people(conn, schema)
    rows = _reply_rows(
        conn,
        schema["reply_queue"],
        start=start,
        end=end,
        statuses=statuses,
        platforms=platforms,
        account=account,
        author=author,
    )
    drafts = tuple(
        _draft_coverage(row, people_rows)
        for row in rows
        if _has_draft(row.get("draft_text"))
    )
    totals = {
        "total_mentions": len(rows),
        "drafted_replies": len(drafts),
        "relationship_context_drafts": sum(
            1 for draft in drafts if draft.has_relationship_context
        ),
        "conversation_context_drafts": sum(
            1 for draft in drafts if draft.has_conversation_context
        ),
        "context_covered_drafts": sum(1 for draft in drafts if draft.has_full_context),
        "context_missing_drafts": sum(1 for draft in drafts if not draft.has_full_context),
    }
    return ReplyContextCoverageReport(
        ok=totals["context_missing_drafts"] == 0,
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=totals,
        drafts=tuple(sorted(drafts, key=_draft_sort_key)),
        missing_cultivate_tables=missing_cultivate_tables,
    )


def format_reply_context_coverage_json(report: ReplyContextCoverageReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_context_coverage_csv(report: ReplyContextCoverageReport) -> str:
    """Render one-row CSV for spreadsheets and shell automation."""
    output = StringIO()
    fieldnames = [
        "generated_at",
        "total_mentions",
        "drafted_replies",
        "relationship_context_drafts",
        "conversation_context_drafts",
        "context_covered_drafts",
        "context_missing_drafts",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow({"generated_at": report.generated_at, **report.totals})
    return output.getvalue().rstrip("\r\n")


def _draft_coverage(
    row: Mapping[str, Any],
    people_rows: Sequence[Mapping[str, Any]],
) -> ReplyContextCoverageDraft:
    return ReplyContextCoverageDraft(
        reply_queue_id=_int_or_none(row.get("id")),
        mention_id=_clean(row.get("inbound_tweet_id") or row.get("mention_id")),
        platform=_clean(row.get("platform")),
        author_handle=_clean(row.get("inbound_author_handle") or row.get("author_handle")),
        author_id=_clean(row.get("inbound_author_id") or row.get("author_id")),
        detected_at=_clean(row.get("detected_at") or row.get("created_at")),
        has_relationship_context=_has_relationship_context(row)
        or _has_cultivate_person_context(row, people_rows),
        has_conversation_context=_has_conversation_context(row),
    )


def _has_relationship_context(row: Mapping[str, Any]) -> bool:
    context = _parse_json_object(row.get("relationship_context"))
    if not context:
        return False
    return any(_present(_first_value(context, key)) for key in RELATIONSHIP_KEYS)


def _has_conversation_context(row: Mapping[str, Any]) -> bool:
    metadata = _parse_json_object(row.get("platform_metadata"))
    if any(_present(_first_value(metadata, key)) for key in CONVERSATION_METADATA_KEYS):
        return True
    if _present(row.get("our_post_text")) and _present(row.get("our_tweet_id")):
        return True
    if _present(row.get("our_post_text")) and _present(row.get("our_platform_id")):
        return True
    return False


def _has_cultivate_person_context(
    row: Mapping[str, Any],
    people_rows: Sequence[Mapping[str, Any]],
) -> bool:
    handle = _normalise_handle(row.get("inbound_author_handle") or row.get("author_handle"))
    author_id = _clean(row.get("inbound_author_id") or row.get("author_id"))
    for person in people_rows:
        person_handle = _normalise_handle(
            person.get("x_handle") or person.get("handle") or person.get("username")
        )
        person_id = _clean(person.get("x_user_id") or person.get("user_id") or person.get("id"))
        if handle and person_handle and handle == person_handle:
            return _person_has_context(person)
        if author_id and person_id and author_id == person_id:
            return _person_has_context(person)
    return False


def _person_has_context(person: Mapping[str, Any]) -> bool:
    return any(
        _present(person.get(key))
        for key in (
            "display_name",
            "bio",
            "relationship_strength",
            "engagement_stage",
            "dunbar_tier",
            "notes",
            "relationship_notes",
        )
    )


def _reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    start: str | None,
    end: str | None,
    statuses: tuple[str, ...],
    platforms: tuple[str, ...],
    account: str | None,
    author: str | None,
) -> list[dict[str, Any]]:
    where: list[str] = []
    params: list[Any] = []
    if "detected_at" in columns and start:
        where.append("(detected_at IS NULL OR datetime(detected_at) >= datetime(?))")
        params.append(start)
    if "detected_at" in columns and end:
        where.append("(detected_at IS NULL OR datetime(detected_at) <= datetime(?))")
        params.append(end)
    if "status" in columns and statuses:
        where.append(f"LOWER(COALESCE(status, 'pending')) IN ({_placeholders(statuses)})")
        params.extend(statuses)
    if "platform" in columns and platforms:
        where.append(f"LOWER(COALESCE(platform, '')) IN ({_placeholders(platforms)})")
        params.extend(platforms)
    account_columns = [column for column in ("account", "account_id", "our_platform_id", "our_tweet_id") if column in columns]
    if account and account_columns:
        where.append("(" + " OR ".join(f"{column} = ?" for column in account_columns) + ")")
        params.extend([account] * len(account_columns))
    author_columns = [column for column in ("inbound_author_handle", "inbound_author_id", "author_handle", "author_id") if column in columns]
    if author and author_columns:
        normalised_author = _normalise_handle(author)
        clauses = []
        for column in author_columns:
            if "handle" in column:
                clauses.append(f"LOWER(REPLACE(COALESCE({column}, ''), '@', '')) = ?")
                params.append(normalised_author)
            else:
                clauses.append(f"COALESCE({column}, '') = ?")
                params.append(author)
        where.append("(" + " OR ".join(clauses) + ")")

    query = "SELECT * FROM reply_queue"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY " + _order_clause(columns)
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _cultivate_people(
    conn: sqlite3.Connection,
    schema: Mapping[str, set[str]],
) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
    if "people" not in schema:
        return [], ("people",)
    columns = schema["people"]
    query = "SELECT * FROM people"
    rows = [dict(row) for row in conn.execute(query).fetchall()]
    if not rows:
        return [], ()
    usable_columns = {"x_handle", "handle", "username", "x_user_id", "user_id", "id"}
    if not columns.intersection(usable_columns):
        return [], ()
    return rows, ()


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in tables}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row[1])
        for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})").fetchall()
    }


def _empty_report(
    generated_at: datetime,
    filters: Mapping[str, Any],
    *,
    missing_tables: Sequence[str] = (),
    missing_columns: Mapping[str, Sequence[str]] | None = None,
) -> ReplyContextCoverageReport:
    return ReplyContextCoverageReport(
        ok=True,
        generated_at=generated_at.isoformat(),
        filters=dict(filters),
        totals={
            "total_mentions": 0,
            "drafted_replies": 0,
            "relationship_context_drafts": 0,
            "conversation_context_drafts": 0,
            "context_covered_drafts": 0,
            "context_missing_drafts": 0,
        },
        drafts=(),
        missing_tables=tuple(missing_tables),
        missing_columns={
            table: tuple(columns) for table, columns in (missing_columns or {}).items()
        }
        or None,
    )


def _normalise_filter(value: str | Sequence[str] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        values = (value,)
    else:
        values = tuple(value)
    return tuple(sorted({item.strip().casefold() for item in values if item and item.strip()}))


def _parse_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value is None or str(value).strip() == "":
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _first_value(mapping: Mapping[str, Any], key: str) -> Any:
    return mapping.get(key)


def _has_draft(value: Any) -> bool:
    return bool(str(value or "").strip())


def _present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return bool(value)
    return True


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalise_handle(value: Any) -> str | None:
    text = _clean(value)
    if not text:
        return None
    return text.lstrip("@").casefold()


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _draft_sort_key(draft: ReplyContextCoverageDraft) -> tuple[str, int]:
    return (draft.detected_at or "", draft.reply_queue_id or 0)


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "detected_at" in columns:
        parts.append("datetime(detected_at) ASC")
    if "id" in columns:
        parts.append("id ASC")
    else:
        parts.append("rowid ASC")
    return ", ".join(parts)


def _placeholders(values: Sequence[Any]) -> str:
    return ",".join("?" for _ in values)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
