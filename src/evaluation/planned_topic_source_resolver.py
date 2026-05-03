"""Resolve planned topic source_material references to ingested artifacts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 50
RESOLUTION_STATUSES = ("resolved", "partial", "missing", "unparseable")
SOURCE_TABLES = ("github_commits", "claude_messages", "github_activity")


@dataclass(frozen=True)
class SourceReferenceLookup:
    """Lookup result for one parsed source_material reference."""

    reference: str
    reference_type: str
    lookup_status: str
    artifact_table: str | None
    artifact_id: int | str | None
    diagnostic: str
    suggested_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PlannedTopicSourceResolution:
    """Resolution status for one planned_topics row."""

    planned_topic_id: int
    campaign_id: int | None
    topic: str | None
    status: str | None
    target_date: str | None
    source_material: str | None
    resolution_status: str
    parse_status: str
    parse_diagnostic: str | None
    reference_count: int
    resolved_reference_count: int
    missing_reference_count: int
    lookups: tuple[SourceReferenceLookup, ...]
    suggested_action: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["lookups"] = [lookup.to_dict() for lookup in self.lookups]
        return payload


@dataclass(frozen=True)
class PlannedTopicSourceResolverReport:
    """Report for planned topic source reference resolution."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    planned_topics: tuple[PlannedTopicSourceResolution, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def has_issues(self) -> bool:
        return any(row.resolution_status != "resolved" for row in self.planned_topics)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "planned_topic_source_resolution",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "has_issues": self.has_issues,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "planned_topics": [row.to_dict() for row in self.planned_topics],
            "totals": dict(sorted(self.totals.items())),
        }


def build_planned_topic_source_resolver_report(
    db_or_conn: Any,
    *,
    campaign_id: int | None = None,
    status: str | None = None,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> PlannedTopicSourceResolverReport:
    """Return a read-only report resolving planned_topics.source_material."""
    if campaign_id is not None and campaign_id <= 0:
        raise ValueError("campaign_id must be positive")
    if status is not None and not str(status).strip():
        raise ValueError("status must not be blank")
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    current_time = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = current_time - timedelta(days=days)
    filters = {
        "campaign_id": campaign_id,
        "days": days,
        "limit": limit,
        "lookback_end": current_time.isoformat(),
        "lookback_start": cutoff.isoformat(),
        "status": status,
    }

    conn = _connection(db_or_conn)
    conn.row_factory = sqlite3.Row
    schema = _schema(conn)
    missing_tables = tuple(
        table for table in ("planned_topics", *SOURCE_TABLES) if table not in schema
    )
    missing_columns = _missing_columns(schema)
    if "planned_topics" not in schema:
        return PlannedTopicSourceResolverReport(
            generated_at=current_time.isoformat(),
            filters=filters,
            totals=_totals(()),
            planned_topics=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    indexes = _source_indexes(conn, schema)
    rows = _load_planned_topics(
        conn,
        schema,
        campaign_id=campaign_id,
        status=status,
        cutoff=cutoff,
        limit=limit,
    )
    resolutions = tuple(_resolve_topic(row, indexes=indexes, schema=schema) for row in rows)

    return PlannedTopicSourceResolverReport(
        generated_at=current_time.isoformat(),
        filters=filters,
        totals=_totals(resolutions),
        planned_topics=resolutions,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_planned_topic_source_resolver_json(
    report: PlannedTopicSourceResolverReport,
) -> str:
    """Render the resolver report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_planned_topic_source_resolver_text(
    report: PlannedTopicSourceResolverReport,
) -> str:
    """Render the resolver report as stable terminal text."""
    totals = report.totals
    filters = report.filters
    lines = [
        "Planned Topic Source Resolution",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"campaign_id={filters['campaign_id'] or '-'} "
            f"status={filters['status'] or '-'} "
            f"days={filters['days']} limit={filters['limit']}"
        ),
        (
            "Totals: "
            f"topics={totals['topics_scanned']} resolved={totals['resolved']} "
            f"partial={totals['partial']} missing={totals['missing']} "
            f"unparseable={totals['unparseable']} references={totals['references']} "
            f"resolved_references={totals['resolved_references']} "
            f"missing_references={totals['missing_references']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
            if columns
        )
        if missing:
            lines.append("Missing columns: " + missing)

    if not report.planned_topics:
        lines.extend(["", "No planned topics matched the filters."])
        return "\n".join(lines)

    lines.extend(["", "Planned topics:"])
    for topic in report.planned_topics:
        lines.append(
            f"- planned_topic_id={topic.planned_topic_id} "
            f"status={topic.status or '-'} resolution={topic.resolution_status} "
            f"references={topic.resolved_reference_count}/{topic.reference_count} "
            f"topic={topic.topic or '-'}"
        )
        if topic.parse_diagnostic:
            lines.append(f"  parse={topic.parse_diagnostic}")
        for lookup in topic.lookups:
            artifact = (
                f"{lookup.artifact_table}:{lookup.artifact_id}"
                if lookup.artifact_table and lookup.artifact_id is not None
                else "-"
            )
            lines.append(
                f"  - ref={lookup.reference} type={lookup.reference_type} "
                f"status={lookup.lookup_status} artifact={artifact}"
            )
            lines.append(f"    diagnostic={lookup.diagnostic}")
        lines.append(f"  suggested_action={topic.suggested_action}")
    return "\n".join(lines)


def _resolve_topic(
    row: dict[str, Any],
    *,
    indexes: dict[str, Any],
    schema: dict[str, set[str]],
) -> PlannedTopicSourceResolution:
    parse_status, parse_diagnostic, refs = _parse_source_material(row.get("source_material"))
    if parse_status == "unparseable":
        lookups: tuple[SourceReferenceLookup, ...] = ()
        resolution_status = "unparseable"
    else:
        lookups = tuple(_lookup_reference(ref, indexes=indexes, schema=schema) for ref in refs)
        resolution_status = _resolution_status(lookups)

    resolved_count = sum(1 for lookup in lookups if lookup.lookup_status == "resolved")
    missing_count = len(lookups) - resolved_count
    return PlannedTopicSourceResolution(
        planned_topic_id=int(row["id"]),
        campaign_id=_optional_int(row.get("campaign_id")),
        topic=_optional_text(row.get("topic")),
        status=_optional_text(row.get("status")),
        target_date=_optional_text(row.get("target_date")),
        source_material=_optional_text(row.get("source_material")),
        resolution_status=resolution_status,
        parse_status=parse_status,
        parse_diagnostic=parse_diagnostic,
        reference_count=len(lookups),
        resolved_reference_count=resolved_count,
        missing_reference_count=missing_count,
        lookups=lookups,
        suggested_action=_suggested_topic_action(resolution_status, parse_status),
    )


def _parse_source_material(value: Any) -> tuple[str, str | None, tuple[str, ...]]:
    if value is None:
        return "unparseable", "source_material is empty", ()
    if isinstance(value, list):
        refs = _refs_from_iterable(value)
        return _parse_result(refs)
    text = str(value).strip()
    if not text:
        return "unparseable", "source_material is empty", ()

    if text[:1] in {"[", "{"}:
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            return "unparseable", f"source_material is malformed JSON: {exc.msg}", ()
        refs = _refs_from_json(parsed)
        return _parse_result(refs)

    refs = tuple(token for token in re.split(r"[\s,]+", text) if token)
    return _parse_result(refs)


def _parse_result(refs: tuple[str, ...]) -> tuple[str, str | None, tuple[str, ...]]:
    deduped = tuple(dict.fromkeys(ref.strip() for ref in refs if ref and ref.strip()))
    if not deduped:
        return "unparseable", "source_material did not contain source references", ()
    return "parsed", None, deduped


def _refs_from_json(value: Any) -> tuple[str, ...]:
    if isinstance(value, list):
        return _refs_from_iterable(value)
    if not isinstance(value, dict):
        return ()

    refs: list[str] = []
    for key in (
        "commit_sha",
        "commit_shas",
        "commits",
        "source_commits",
        "message_uuid",
        "message_uuids",
        "session_id",
        "session_ids",
        "messages",
        "source_messages",
        "github_activity_id",
        "github_activity_ids",
        "source_activity_ids",
        "activity_ids",
    ):
        if key in value:
            refs.extend(_refs_from_iterable([value[key]]))
    return tuple(refs)


def _refs_from_iterable(values: list[Any] | tuple[Any, ...]) -> tuple[str, ...]:
    refs: list[str] = []
    for item in values:
        if item is None:
            continue
        if isinstance(item, (list, tuple)):
            refs.extend(_refs_from_iterable(item))
        elif isinstance(item, dict):
            refs.extend(_refs_from_json(item))
        else:
            text = str(item).strip()
            if text:
                refs.append(text)
    return tuple(refs)


def _lookup_reference(
    reference: str,
    *,
    indexes: dict[str, Any],
    schema: dict[str, set[str]],
) -> SourceReferenceLookup:
    explicit_type, lookup_value = _explicit_reference_type(reference)
    candidates = [explicit_type] if explicit_type else [
        "commit",
        "claude_message",
        "claude_session",
        "github_activity",
    ]

    table_missing = False
    for candidate in candidates:
        lookup = _lookup_typed_reference(
            reference,
            lookup_value,
            candidate,
            indexes=indexes,
            schema=schema,
        )
        if lookup.lookup_status == "resolved":
            return lookup
        if lookup.diagnostic.endswith("table is missing"):
            table_missing = True

    if explicit_type and table_missing:
        return _missing_lookup(
            reference,
            explicit_type,
            None,
            f"{_table_for_type(explicit_type)} table is missing",
            "ingest_or_restore_source_table_before_generation",
        )
    return _missing_lookup(
        reference,
        explicit_type or "free_form",
        None,
        "no ingested artifact matched this source reference",
        "replace_reference_with_ingested_commit_message_or_activity_id",
    )


def _lookup_typed_reference(
    reference: str,
    lookup_value: str,
    reference_type: str,
    *,
    indexes: dict[str, Any],
    schema: dict[str, set[str]],
) -> SourceReferenceLookup:
    table = _table_for_type(reference_type)
    if table not in schema:
        return _missing_lookup(
            reference,
            reference_type,
            table,
            f"{table} table is missing",
            "ingest_or_restore_source_table_before_generation",
        )

    match = indexes.get(reference_type, {}).get(lookup_value)
    if match is None and reference_type == "commit":
        match = _commit_prefix_match(lookup_value, indexes.get(reference_type, {}))
    if match is None:
        return _missing_lookup(
            reference,
            reference_type,
            table,
            f"no {table} row matched this reference",
            "refresh_ingestion_or_update_source_material_reference",
        )
    return SourceReferenceLookup(
        reference=reference,
        reference_type=reference_type,
        lookup_status="resolved",
        artifact_table=table,
        artifact_id=match["artifact_id"],
        diagnostic=f"matched {table}",
        suggested_action="source_reference_ready",
    )


def _source_indexes(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[str, dict[str, dict[str, Any]]]:
    return {
        "commit": _commit_index(conn, schema),
        "claude_message": _message_index(conn, schema),
        "claude_session": _session_index(conn, schema),
        "github_activity": _activity_index(conn, schema),
    }


def _commit_index(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[str, dict[str, Any]]:
    columns = schema.get("github_commits", set())
    if not {"id", "commit_sha"}.issubset(columns):
        return {}
    rows = conn.execute(
        "SELECT id, commit_sha FROM github_commits ORDER BY commit_sha ASC"
    ).fetchall()
    return {
        str(row["commit_sha"]): {"artifact_id": int(row["id"]), "commit_sha": str(row["commit_sha"])}
        for row in rows
        if row["commit_sha"] is not None
    }


def _message_index(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[str, dict[str, Any]]:
    columns = schema.get("claude_messages", set())
    if not {"id", "message_uuid"}.issubset(columns):
        return {}
    rows = conn.execute(
        "SELECT id, message_uuid FROM claude_messages ORDER BY message_uuid ASC"
    ).fetchall()
    return {
        str(row["message_uuid"]): {"artifact_id": int(row["id"])}
        for row in rows
        if row["message_uuid"] is not None
    }


def _session_index(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[str, dict[str, Any]]:
    columns = schema.get("claude_messages", set())
    if not {"id", "session_id"}.issubset(columns):
        return {}
    rows = conn.execute(
        """SELECT session_id, MIN(id) AS first_message_id
           FROM claude_messages
           WHERE session_id IS NOT NULL
           GROUP BY session_id
           ORDER BY session_id ASC"""
    ).fetchall()
    return {
        str(row["session_id"]): {"artifact_id": int(row["first_message_id"])}
        for row in rows
        if row["session_id"] is not None
    }


def _activity_index(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[str, dict[str, Any]]:
    columns = schema.get("github_activity", set())
    required = {"id", "repo_name", "number", "activity_type"}
    if not required.issubset(columns):
        return {}
    rows = conn.execute(
        """SELECT id, repo_name, number, activity_type
           FROM github_activity
           ORDER BY id ASC"""
    ).fetchall()
    index: dict[str, dict[str, Any]] = {}
    for row in rows:
        match = {"artifact_id": int(row["id"])}
        index[str(row["id"])] = match
        index[_activity_id(row["repo_name"], row["number"], row["activity_type"])] = match
    return index


def _load_planned_topics(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    campaign_id: int | None,
    status: str | None,
    cutoff: datetime,
    limit: int,
) -> list[dict[str, Any]]:
    columns = schema["planned_topics"]
    if "id" not in columns:
        return []
    select_columns = [
        _column_expr(columns, "id", "pt", "id"),
        _column_expr(columns, "campaign_id", "pt", "campaign_id"),
        _column_expr(columns, "topic", "pt", "topic"),
        _column_expr(columns, "status", "pt", "status"),
        _column_expr(columns, "target_date", "pt", "target_date"),
        _column_expr(columns, "source_material", "pt", "source_material"),
        _column_expr(columns, "created_at", "pt", "created_at"),
    ]
    where: list[str] = []
    params: list[Any] = []
    if campaign_id is not None:
        if "campaign_id" not in columns:
            return []
        where.append("pt.campaign_id = ?")
        params.append(campaign_id)
    if status is not None:
        if "status" not in columns:
            return []
        where.append("pt.status = ?")
        params.append(status)
    window_filter, window_params = _window_filter(columns, cutoff)
    where.append(window_filter)
    params.extend(window_params)

    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
           FROM planned_topics pt
           WHERE {' AND '.join(where)}
           ORDER BY {_order_by(columns)}
           LIMIT ?""",
        (*params, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def _window_filter(columns: set[str], cutoff: datetime) -> tuple[str, list[Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if "created_at" in columns:
        filters.append("datetime(pt.created_at) >= datetime(?)")
        params.append(cutoff.isoformat())
    if "target_date" in columns:
        filters.append("(pt.target_date IS NULL OR date(pt.target_date) >= date(?))")
        params.append(cutoff.date().isoformat())
    if not filters:
        return "1", []
    return "(" + " OR ".join(filters) + ")", params


def _order_by(columns: set[str]) -> str:
    order = []
    if "target_date" in columns:
        order.append("pt.target_date ASC NULLS LAST")
    if "created_at" in columns:
        order.append("pt.created_at ASC")
    order.append("pt.id ASC")
    return ", ".join(order)


def _resolution_status(lookups: tuple[SourceReferenceLookup, ...]) -> str:
    if not lookups:
        return "unparseable"
    resolved = sum(1 for lookup in lookups if lookup.lookup_status == "resolved")
    if resolved == len(lookups):
        return "resolved"
    if resolved:
        return "partial"
    return "missing"


def _suggested_topic_action(resolution_status: str, parse_status: str) -> str:
    if resolution_status == "resolved":
        return "ready_for_generation"
    if resolution_status == "partial":
        return "refresh_or_replace_missing_source_references_before_generation"
    if resolution_status == "missing":
        return "attach_ingested_source_references_before_generation"
    if parse_status == "unparseable":
        return "rewrite_source_material_as_json_array_or_delimited_reference_list"
    return "review_source_material_before_generation"


def _totals(rows: tuple[PlannedTopicSourceResolution, ...]) -> dict[str, int]:
    totals = {status: 0 for status in RESOLUTION_STATUSES}
    for row in rows:
        totals[row.resolution_status] = totals.get(row.resolution_status, 0) + 1
    return {
        **totals,
        "missing_references": sum(row.missing_reference_count for row in rows),
        "references": sum(row.reference_count for row in rows),
        "resolved_references": sum(row.resolved_reference_count for row in rows),
        "topics_scanned": len(rows),
    }


def _explicit_reference_type(reference: str) -> tuple[str | None, str]:
    text = reference.strip().strip("'\"")
    if ":" not in text:
        return None, _clean_reference(text)
    prefix, value = text.split(":", 1)
    prefix = prefix.strip().casefold().replace("-", "_")
    reference_type = {
        "commit": "commit",
        "commit_sha": "commit",
        "github_commit": "commit",
        "sha": "commit",
        "message": "claude_message",
        "msg": "claude_message",
        "message_uuid": "claude_message",
        "claude_message": "claude_message",
        "session": "claude_session",
        "session_id": "claude_session",
        "claude_session": "claude_session",
        "activity": "github_activity",
        "activity_id": "github_activity",
        "github_activity": "github_activity",
        "github_activity_id": "github_activity",
    }.get(prefix)
    if reference_type is None:
        return None, _clean_reference(text)
    return reference_type, _clean_reference(value)


def _commit_prefix_match(
    value: str,
    index: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    if len(value) < 7 or not re.fullmatch(r"[0-9a-fA-F]+", value):
        return None
    matches = [match for sha, match in index.items() if sha.startswith(value)]
    if len(matches) == 1:
        return matches[0]
    return None


def _missing_lookup(
    reference: str,
    reference_type: str,
    artifact_table: str | None,
    diagnostic: str,
    suggested_action: str,
) -> SourceReferenceLookup:
    return SourceReferenceLookup(
        reference=reference,
        reference_type=reference_type,
        lookup_status="missing",
        artifact_table=artifact_table,
        artifact_id=None,
        diagnostic=diagnostic,
        suggested_action=suggested_action,
    )


def _table_for_type(reference_type: str) -> str:
    if reference_type == "commit":
        return "github_commits"
    if reference_type in {"claude_message", "claude_session"}:
        return "claude_messages"
    if reference_type == "github_activity":
        return "github_activity"
    return ""


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    required = {
        "planned_topics": ("id", "source_material"),
        "github_commits": ("id", "commit_sha"),
        "claude_messages": ("id", "message_uuid", "session_id"),
        "github_activity": ("id", "repo_name", "number", "activity_type"),
    }
    optional = {
        "planned_topics": ("campaign_id", "topic", "status", "target_date", "created_at"),
    }
    missing: dict[str, tuple[str, ...]] = {}
    for table, required_columns in required.items():
        if table not in schema:
            continue
        expected = tuple(dict.fromkeys((*required_columns, *optional.get(table, ()))))
        absent = tuple(column for column in expected if column not in schema[table])
        if absent:
            missing[table] = absent
    return missing


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        schema[table] = {
            str(info["name"] if isinstance(info, sqlite3.Row) else info[1])
            for info in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")
        }
    return schema


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3 connection or database wrapper with .conn")
    return conn


def _column_expr(columns: set[str], column: str, alias: str, output: str) -> str:
    if column in columns:
        return f"{alias}.{column} AS {output}"
    return f"NULL AS {output}"


def _activity_id(repo_name: Any, number: Any, activity_type: Any) -> str:
    return f"{repo_name}#{number}:{activity_type}"


def _clean_reference(value: str) -> str:
    return value.strip().strip("'\"").rstrip(".,;")


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
