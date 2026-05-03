"""Audit proactive engagement actions for target cooldown violations."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import urlparse


DEFAULT_DAYS = 7
DEFAULT_MAX_ACTIONS = 2
PROACTIVE_ACTION_TYPES = frozenset(("like", "quote_tweet", "reply", "retweet", "follow"))

PROACTIVE_COLUMNS = (
    "id",
    "action_type",
    "target_tweet_id",
    "target_author_handle",
    "target_author_id",
    "status",
    "platform_metadata",
    "created_at",
    "reviewed_at",
    "posted_at",
)
ACTION_COLUMNS = (
    "id",
    "action_type",
    "target_person_id",
    "status",
    "created_at",
    "completed_at",
    "payload",
)

_HANDLE_RE = re.compile(r"@([A-Za-z0-9_]{1,30})")


@dataclass(frozen=True)
class ProactiveCooldownViolation:
    """One target whose proactive engagement action count exceeded policy."""

    target_id: str
    action_count: int
    max_actions: int
    most_recent_action_at: str | None
    action_ids: tuple[Any, ...]
    aliases: tuple[str, ...] = ()
    sources: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["action_ids"] = list(self.action_ids)
        data["aliases"] = list(self.aliases)
        data["sources"] = list(self.sources)
        return data


@dataclass(frozen=True)
class ProactiveCooldownAuditReport:
    """Read-only proactive engagement cooldown audit report."""

    ok: bool
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    violations: tuple[ProactiveCooldownViolation, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def violation_count(self) -> int:
        return len(self.violations)

    @property
    def blocking_issue_count(self) -> int:
        return self.violation_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "proactive_cooldown_audit",
            "ok": self.ok,
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "totals": dict(self.totals),
            "violation_count": self.violation_count,
            "blocking_issue_count": self.blocking_issue_count,
            "violations": [violation.to_dict() for violation in self.violations],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_proactive_cooldown_audit(
    source: Any | None = None,
    *,
    action_records: Iterable[Mapping[str, Any]] | None = None,
    days: int = DEFAULT_DAYS,
    max_actions: int = DEFAULT_MAX_ACTIONS,
    now: datetime | None = None,
) -> ProactiveCooldownAuditReport:
    """Build a deterministic audit of proactive target contact frequency."""

    if source is not None and action_records is not None:
        raise ValueError("provide either source or action_records, not both")
    if days <= 0:
        raise ValueError("days must be positive")
    if max_actions <= 0:
        raise ValueError("max_actions must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"days": days, "max_actions": max_actions}

    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] = {}
    if action_records is not None or _is_records(source):
        raw_rows = [dict(row) for row in (action_records if action_records is not None else source)]
    elif source is not None:
        conn = _connection(source)
        schema = _schema(conn)
        if "proactive_actions" not in schema and "actions" not in schema:
            missing_tables = ("actions", "proactive_actions")
            raw_rows = []
        else:
            raw_rows = []
            if "proactive_actions" in schema:
                missing = tuple(
                    column for column in PROACTIVE_COLUMNS if column not in schema["proactive_actions"]
                )
                if missing:
                    missing_columns["proactive_actions"] = missing
                else:
                    raw_rows.extend(_proactive_rows(conn, cutoff=cutoff, now=generated_at))
            if "actions" in schema:
                missing = tuple(column for column in ACTION_COLUMNS if column not in schema["actions"])
                if missing:
                    missing_columns["actions"] = missing
                else:
                    raw_rows.extend(_resolved_action_rows(conn, schema=schema))
    else:
        raise ValueError("source or action_records is required")

    actions = [
        action
        for row in raw_rows
        for action in [_normalize_action_row(row)]
        if action is not None and _within_window(action["seen_at"], cutoff, generated_at)
    ]
    violations = _violations(actions, max_actions=max_actions)
    totals = {
        "audited_actions": len(actions),
        "target_count": _target_count(actions),
        "violation_count": len(violations),
        "missing_target_count": sum(1 for action in actions if not action["aliases"]),
    }
    return ProactiveCooldownAuditReport(
        ok=not violations,
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=totals,
        violations=violations,
        missing_tables=missing_tables,
        missing_columns=missing_columns or None,
    )


def format_proactive_cooldown_audit_json(report: ProactiveCooldownAuditReport) -> str:
    """Render deterministic JSON for automation."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_proactive_cooldown_audit_text(report: ProactiveCooldownAuditReport) -> str:
    """Render a compact human-readable proactive cooldown audit."""

    totals = report.totals
    lines = [
        "Proactive Cooldown Audit",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={report.filters['days']} max_actions={report.filters['max_actions']}"
        ),
        (
            "Totals: "
            f"audited={totals['audited_actions']} targets={totals['target_count']} "
            f"violations={totals['violation_count']} missing_target={totals['missing_target_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = ", ".join(
            f"{table}.{column}"
            for table, columns in sorted(report.missing_columns.items())
            for column in columns
        )
        if missing:
            lines.append("Missing columns: " + missing)
    if not report.violations:
        lines.append("No proactive cooldown violations found.")
        return "\n".join(lines)

    lines.append("Violations:")
    for violation in report.violations:
        lines.append(
            f"- target={violation.target_id} actions={violation.action_count}/{violation.max_actions} "
            f"latest={violation.most_recent_action_at or 'unknown'} "
            f"ids={','.join(str(action_id) for action_id in violation.action_ids)}"
        )
        if violation.aliases:
            lines.append("  aliases=" + ", ".join(violation.aliases))
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _proactive_rows(
    conn: sqlite3.Connection,
    *,
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    timestamp_expr = "COALESCE(posted_at, reviewed_at, created_at)"
    cursor = conn.execute(
        f"""SELECT {', '.join(PROACTIVE_COLUMNS)}
            FROM proactive_actions
            WHERE LOWER(action_type) IN ({','.join('?' for _ in PROACTIVE_ACTION_TYPES)})
              AND datetime({timestamp_expr}) >= datetime(?)
              AND datetime({timestamp_expr}) <= datetime(?)
            ORDER BY datetime({timestamp_expr}) DESC, id ASC""",
        [*sorted(PROACTIVE_ACTION_TYPES), cutoff.isoformat(), now.isoformat()],
    )
    return [{**dict(row), "_source": "proactive_actions"} for row in cursor.fetchall()]


def _resolved_action_rows(
    conn: sqlite3.Connection,
    *,
    schema: dict[str, set[str]],
) -> list[dict[str, Any]]:
    people_columns = schema.get("people", set())
    select_people = ""
    join_people = ""
    if people_columns:
        x_handle = _column_expr("p", people_columns, "x_handle")
        x_user_id = _column_expr("p", people_columns, "x_user_id")
        select_people = f", {x_handle} AS person_x_handle, {x_user_id} AS person_x_user_id"
        join_people = "LEFT JOIN people p ON p.id = a.target_person_id"

    cursor = conn.execute(
        f"""SELECT a.{', a.'.join(ACTION_COLUMNS)}{select_people}
            FROM actions a
            {join_people}
            ORDER BY datetime(COALESCE(a.completed_at, a.created_at)) DESC, a.id ASC""",
    )
    return [{**dict(row), "_source": "actions"} for row in cursor.fetchall()]


def _normalize_action_row(row: dict[str, Any]) -> dict[str, Any] | None:
    source = _text(row.get("_source") or row.get("source") or "proactive_actions")
    if source == "actions":
        payload = _parse_json_object(row.get("payload"))
        action_type = _text(payload.get("execution_type") or row.get("action_type")).casefold()
        if action_type not in PROACTIVE_ACTION_TYPES:
            return None
        seen_at = _latest_timestamp(payload.get("resolved_at"), row.get("completed_at"), row.get("created_at"))
        aliases = _aliases_from_values(
            ids=(payload.get("x_user_id"), row.get("person_x_user_id"), row.get("target_person_id")),
            handles=(payload.get("target_handle"), payload.get("x_handle"), row.get("person_x_handle")),
            urls=(payload.get("target_url"), payload.get("url")),
            loose=(row.get("target_person_id"),),
        )
        return {
            "source": "actions",
            "action_id": _text(row.get("id")),
            "action_type": action_type,
            "seen_at": seen_at,
            "aliases": aliases,
        }

    metadata = _parse_json_object(row.get("platform_metadata"))
    action_type = _text(row.get("action_type")).casefold()
    if action_type not in PROACTIVE_ACTION_TYPES:
        return None
    aliases = _aliases_from_values(
        ids=(row.get("target_author_id"), metadata.get("target_author_id"), metadata.get("author_id")),
        handles=(
            row.get("target_author_handle"),
            metadata.get("target_author_handle"),
            metadata.get("author_handle"),
            metadata.get("target_handle"),
            metadata.get("handle"),
        ),
        urls=(
            metadata.get("target_url"),
            metadata.get("url"),
            metadata.get("author_url"),
            row.get("target_tweet_id"),
        ),
        loose=(row.get("target_author_id"),),
    )
    return {
        "source": "proactive_actions",
        "action_id": int(row["id"]) if str(row.get("id", "")).isdigit() else row.get("id"),
        "action_type": action_type,
        "seen_at": _latest_timestamp(row.get("posted_at"), row.get("reviewed_at"), row.get("created_at")),
        "aliases": aliases,
    }


def _violations(
    actions: list[dict[str, Any]],
    *,
    max_actions: int,
) -> tuple[ProactiveCooldownViolation, ...]:
    parent: dict[str, str] = {}

    def find(alias: str) -> str:
        parent.setdefault(alias, alias)
        if parent[alias] != alias:
            parent[alias] = find(parent[alias])
        return parent[alias]

    def union(first: str, second: str) -> None:
        first_root = find(first)
        second_root = find(second)
        if first_root != second_root:
            parent[max(first_root, second_root)] = min(first_root, second_root)

    for action in actions:
        aliases = action["aliases"]
        for alias in aliases:
            find(alias)
        for alias in aliases[1:]:
            union(aliases[0], alias)

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for action in actions:
        if not action["aliases"]:
            continue
        root = find(action["aliases"][0])
        grouped[root].append(action)

    violations: list[ProactiveCooldownViolation] = []
    for grouped_actions in grouped.values():
        if len(grouped_actions) <= max_actions:
            continue
        aliases = tuple(sorted({alias for action in grouped_actions for alias in action["aliases"]}))
        target_id = _preferred_alias(aliases)
        most_recent = _latest_timestamp(*(action["seen_at"] for action in grouped_actions))
        ordered_actions = sorted(
            grouped_actions,
            key=lambda action: (
                _parse_datetime(action["seen_at"]) or datetime.min.replace(tzinfo=timezone.utc),
                str(action["action_id"]),
            ),
            reverse=True,
        )
        violations.append(
            ProactiveCooldownViolation(
                target_id=target_id,
                action_count=len(grouped_actions),
                max_actions=max_actions,
                most_recent_action_at=most_recent,
                action_ids=tuple(action["action_id"] for action in ordered_actions),
                aliases=aliases,
                sources=tuple(sorted(Counter(action["source"] for action in grouped_actions))),
            )
        )
    return tuple(
        sorted(
            violations,
            key=lambda violation: (
                -violation.action_count,
                -(
                    _parse_datetime(violation.most_recent_action_at)
                    or datetime.min.replace(tzinfo=timezone.utc)
                ).timestamp(),
                violation.target_id,
            ),
        )
    )


def _aliases_from_values(
    *,
    ids: tuple[Any, ...] = (),
    handles: tuple[Any, ...] = (),
    urls: tuple[Any, ...] = (),
    loose: tuple[Any, ...] = (),
) -> tuple[str, ...]:
    aliases: set[str] = set()
    for value in ids:
        text = _text(value)
        if text:
            aliases.add("id:" + text.casefold())
    for value in handles:
        handle = _normalize_handle(value)
        if handle:
            aliases.add("handle:" + handle)
    for value in urls:
        aliases.update(_aliases_from_url(value))
    for value in loose:
        text = _text(value)
        if text and not aliases:
            aliases.add("id:" + text.casefold())
    return tuple(sorted(aliases))


def _aliases_from_url(value: Any) -> set[str]:
    text = _text(value)
    if not text:
        return set()
    handle_match = _HANDLE_RE.search(text)
    if handle_match:
        return {"handle:" + handle_match.group(1).casefold()}
    parsed = urlparse(text)
    if not parsed.scheme or not parsed.netloc:
        return set()
    host = parsed.netloc.casefold()
    parts = [part for part in parsed.path.split("/") if part]
    if host in {"x.com", "www.x.com", "twitter.com", "www.twitter.com"} and parts:
        first = parts[0].casefold().lstrip("@")
        if first not in {"i", "intent", "hashtag", "search"}:
            return {"handle:" + first}
    if host.endswith("bsky.app") and len(parts) >= 2 and parts[0] == "profile":
        return {"handle:" + parts[1].casefold().lstrip("@")}
    return {"url:" + host + "/" + "/".join(part.casefold() for part in parts)}


def _preferred_alias(aliases: tuple[str, ...]) -> str:
    for prefix in ("id:", "handle:", "url:"):
        for alias in aliases:
            if alias.startswith(prefix):
                return alias
    return aliases[0] if aliases else "unknown"


def _target_count(actions: list[dict[str, Any]]) -> int:
    parent: dict[str, str] = {}

    def find(alias: str) -> str:
        parent.setdefault(alias, alias)
        if parent[alias] != alias:
            parent[alias] = find(parent[alias])
        return parent[alias]

    def union(first: str, second: str) -> None:
        first_root = find(first)
        second_root = find(second)
        if first_root != second_root:
            parent[max(first_root, second_root)] = min(first_root, second_root)

    for action in actions:
        aliases = action["aliases"]
        if not aliases:
            continue
        for alias in aliases:
            find(alias)
        for alias in aliases[1:]:
            union(aliases[0], alias)
    return len({find(action["aliases"][0]) for action in actions if action["aliases"]})


def _within_window(value: str | None, cutoff: datetime, now: datetime) -> bool:
    parsed = _parse_datetime(value)
    return parsed is not None and cutoff <= parsed <= now


def _latest_timestamp(*values: Any) -> str | None:
    parsed = [_parse_datetime(value) for value in values]
    valid = [value for value in parsed if value is not None]
    if not valid:
        return None
    return max(valid).isoformat()


def _parse_datetime(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _column_expr(alias: str, columns: set[str], column: str) -> str:
    return f"{alias}.{column}" if column in columns else "NULL"


def _normalize_handle(value: Any) -> str | None:
    text = _text(value).lstrip("@").casefold()
    return text or None


def _text(value: Any) -> str:
    return str(value or "").strip()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _is_records(value: Any) -> bool:
    return isinstance(value, Iterable) and not isinstance(value, (str, bytes, sqlite3.Connection))
