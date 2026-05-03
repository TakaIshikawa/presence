"""Audit proactive actions for duplicate or conflicting engagement targets."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
import json
import re
import sqlite3
from typing import Any, Sequence


DEFAULT_DAYS = 30
VALID_ACTION_TYPES = frozenset(("like", "quote_tweet", "reply", "retweet"))
PENDING_STATUS = "pending"

ISSUE_DUPLICATE_TARGET_TWEET_ID = "duplicate_target_tweet_id"
ISSUE_NEAR_DUPLICATE_TARGET_TEXT = "near_duplicate_target_text"
ISSUE_MISSING_TARGET_METADATA = "missing_target_metadata"
ISSUE_POSTED_STATUS_MISMATCH = "posted_status_mismatch"

RECOMMEND_MERGE = "merge"
RECOMMEND_DISMISS_DUPLICATE = "dismiss_duplicate"
RECOMMEND_REPAIR_STATUS = "repair_status"
RECOMMEND_ENRICH_TARGET_METADATA = "enrich_target_metadata"

PROACTIVE_COLUMNS = (
    "id",
    "action_type",
    "target_tweet_id",
    "target_tweet_text",
    "target_author_handle",
    "target_author_id",
    "status",
    "posted_tweet_id",
    "created_at",
    "reviewed_at",
    "posted_at",
)

_SPACE_RE = re.compile(r"\s+")
_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_NON_WORD_RE = re.compile(r"[^a-z0-9\s]")


@dataclass(frozen=True)
class ProactiveActionTargetIssue:
    """One duplicate or conflicting proactive action target issue."""

    issue_type: str
    severity: str
    recommendation: str
    action_ids: tuple[int, ...]
    statuses: tuple[str, ...]
    action_types: tuple[str, ...]
    target_tweet_id: str | None = None
    target_author_handle: str | None = None
    target_tweet_text_preview: str | None = None
    posted_tweet_id: str | None = None
    details: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "issue_type": self.issue_type,
            "severity": self.severity,
            "recommendation": self.recommendation,
            "action_ids": list(self.action_ids),
            "statuses": list(self.statuses),
            "action_types": list(self.action_types),
            "target_tweet_id": self.target_tweet_id,
            "target_author_handle": self.target_author_handle,
            "target_tweet_text_preview": self.target_tweet_text_preview,
            "posted_tweet_id": self.posted_tweet_id,
            "details": self.details,
        }


@dataclass(frozen=True)
class ProactiveActionTargetAuditReport:
    """Read-only proactive action target audit report."""

    ok: bool
    generated_at: str
    filters: dict[str, Any]
    audited_count: int
    issue_count: int
    summary: dict[str, Any]
    issues: tuple[ProactiveActionTargetIssue, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocking_issue_count(self) -> int:
        return self.issue_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "proactive_action_target_audit",
            "ok": self.ok,
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "audited_count": self.audited_count,
            "issue_count": self.issue_count,
            "blocking_issue_count": self.blocking_issue_count,
            "summary": self.summary,
            "issues": [issue.to_dict() for issue in self.issues],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_proactive_action_target_audit(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    action_types: str | Sequence[str] | None = None,
    now: datetime | None = None,
) -> ProactiveActionTargetAuditReport:
    """Build a deterministic, read-only audit of proactive action target conflicts."""
    if days <= 0:
        raise ValueError("days must be positive")

    wanted_action_types = _normalise_action_types(action_types)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    filters = {
        "days": days,
        "action_type": list(wanted_action_types),
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "proactive_actions" not in schema:
        return _empty_report(generated_at, filters, missing_tables=("proactive_actions",))
    missing = tuple(column for column in PROACTIVE_COLUMNS if column not in schema["proactive_actions"])
    if missing:
        return _empty_report(
            generated_at,
            filters,
            missing_columns={"proactive_actions": missing},
        )

    rows = _action_rows(conn, days=days, action_types=wanted_action_types, now=generated_at)
    issues = _find_issues(rows)
    return ProactiveActionTargetAuditReport(
        ok=not issues,
        generated_at=generated_at.isoformat(),
        filters=filters,
        audited_count=len(rows),
        issue_count=len(issues),
        summary=_summary(issues),
        issues=issues,
    )


def format_proactive_action_target_audit_json(report: ProactiveActionTargetAuditReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_proactive_action_target_audit_text(report: ProactiveActionTargetAuditReport) -> str:
    """Render a compact human-readable proactive action target audit."""
    lines = [
        "Proactive Action Target Audit",
        (
            "Filters: "
            f"days={report.filters.get('days')} "
            f"action_type={_display_filter(report.filters.get('action_type'))}"
        ),
        f"Audited actions: {report.audited_count}",
        f"Issues: {report.issue_count}",
    ]
    by_recommendation = report.summary.get("by_recommendation", {})
    if by_recommendation:
        lines.append("Recommendations:")
        for recommendation, count in sorted(by_recommendation.items()):
            lines.append(f"  - {recommendation}: {count}")
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
    if not report.issues:
        lines.append("No duplicate or conflicting proactive action targets found.")
        return "\n".join(lines)

    lines.append("Findings:")
    for issue in report.issues:
        lines.append(
            f"  - {issue.issue_type} ids={','.join(str(action_id) for action_id in issue.action_ids)} "
            f"severity={issue.severity} recommendation={issue.recommendation}"
        )
        lines.append(
            "    "
            f"target={issue.target_tweet_id or 'n/a'} "
            f"author={issue.target_author_handle or 'n/a'} "
            f"types={','.join(issue.action_types) or 'n/a'} "
            f"statuses={','.join(issue.statuses) or 'n/a'}"
        )
        if issue.details:
            lines.append(f"    details={issue.details}")
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ProactiveActionTargetAuditReport:
    return ProactiveActionTargetAuditReport(
        ok=True,
        generated_at=generated_at.isoformat(),
        filters=filters,
        audited_count=0,
        issue_count=0,
        summary=_summary(()),
        issues=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _action_rows(
    conn: sqlite3.Connection,
    *,
    days: int,
    action_types: Sequence[str],
    now: datetime,
) -> list[dict[str, Any]]:
    cutoff = now - timedelta(days=days)
    where = ["datetime(COALESCE(created_at, reviewed_at, posted_at)) >= datetime(?)"]
    params: list[Any] = [cutoff.isoformat()]
    if action_types:
        where.append("LOWER(action_type) IN (" + ",".join("?" for _ in action_types) + ")")
        params.extend(action_types)
    cursor = conn.execute(
        f"""SELECT {', '.join(PROACTIVE_COLUMNS)}
            FROM proactive_actions
            WHERE {' AND '.join(where)}
            ORDER BY datetime(COALESCE(created_at, reviewed_at, posted_at)) DESC, id ASC""",
        params,
    )
    return [dict(row) for row in cursor.fetchall()]


def _find_issues(rows: Sequence[dict[str, Any]]) -> tuple[ProactiveActionTargetIssue, ...]:
    issues: list[ProactiveActionTargetIssue] = []
    pending = [row for row in rows if _status(row) == PENDING_STATUS]

    by_target: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in pending:
        target_id = _text(row.get("target_tweet_id"))
        if target_id:
            by_target[target_id].append(row)
    for target_id, grouped in sorted(by_target.items()):
        if len(grouped) > 1:
            issues.append(
                _issue(
                    ISSUE_DUPLICATE_TARGET_TWEET_ID,
                    grouped,
                    severity="high",
                    recommendation=RECOMMEND_DISMISS_DUPLICATE,
                    target_tweet_id=target_id,
                    details="multiple pending proactive actions share target_tweet_id",
                )
            )

    for grouped in _near_duplicate_text_groups(pending):
        issues.append(
            _issue(
                ISSUE_NEAR_DUPLICATE_TARGET_TEXT,
                grouped,
                severity="medium",
                recommendation=RECOMMEND_MERGE,
                target_author_handle=_normalise_handle(grouped[0].get("target_author_handle")),
                target_tweet_text_preview=_preview(grouped[0].get("target_tweet_text")),
                details="same target_author_handle has near-identical target_tweet_text",
            )
        )

    for row in rows:
        action_type = _action_type(row)
        if action_type in {"reply", "quote_tweet"} and _missing_target_metadata(row):
            missing = _missing_target_fields(row)
            issues.append(
                _issue(
                    ISSUE_MISSING_TARGET_METADATA,
                    (row,),
                    severity="high",
                    recommendation=RECOMMEND_ENRICH_TARGET_METADATA,
                    target_tweet_id=_text(row.get("target_tweet_id")),
                    target_author_handle=_text(row.get("target_author_handle")),
                    target_tweet_text_preview=_preview(row.get("target_tweet_text")),
                    details="missing fields: " + ", ".join(missing),
                )
            )
        posted_tweet_id = _text(row.get("posted_tweet_id"))
        if posted_tweet_id and _status(row) != "posted":
            issues.append(
                _issue(
                    ISSUE_POSTED_STATUS_MISMATCH,
                    (row,),
                    severity="high",
                    recommendation=RECOMMEND_REPAIR_STATUS,
                    target_tweet_id=_text(row.get("target_tweet_id")),
                    target_author_handle=_text(row.get("target_author_handle")),
                    posted_tweet_id=posted_tweet_id,
                    details="posted_tweet_id is set but status is not posted",
                )
            )

    return tuple(sorted(issues, key=lambda issue: (issue.issue_type, issue.action_ids)))


def _near_duplicate_text_groups(rows: Sequence[dict[str, Any]]) -> tuple[tuple[dict[str, Any], ...], ...]:
    by_author: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        author = _normalise_handle(row.get("target_author_handle"))
        text = _normalise_text(row.get("target_tweet_text"))
        if author and text:
            by_author[author].append(row)

    groups: list[tuple[dict[str, Any], ...]] = []
    emitted: set[frozenset[int]] = set()
    for author_rows in by_author.values():
        for index, row in enumerate(author_rows):
            row_text = _normalise_text(row.get("target_tweet_text"))
            matches = [row]
            for other in author_rows[index + 1 :]:
                other_text = _normalise_text(other.get("target_tweet_text"))
                if row_text == other_text or SequenceMatcher(None, row_text, other_text).ratio() >= 0.92:
                    matches.append(other)
            if len(matches) > 1:
                key = frozenset(int(match["id"]) for match in matches)
                if key not in emitted:
                    emitted.add(key)
                    groups.append(tuple(sorted(matches, key=lambda item: int(item["id"]))))
    return tuple(groups)


def _issue(
    issue_type: str,
    rows: Sequence[dict[str, Any]],
    *,
    severity: str,
    recommendation: str,
    target_tweet_id: str | None = None,
    target_author_handle: str | None = None,
    target_tweet_text_preview: str | None = None,
    posted_tweet_id: str | None = None,
    details: str | None = None,
) -> ProactiveActionTargetIssue:
    return ProactiveActionTargetIssue(
        issue_type=issue_type,
        severity=severity,
        recommendation=recommendation,
        action_ids=tuple(int(row["id"]) for row in rows),
        statuses=tuple(sorted({_status(row) for row in rows})),
        action_types=tuple(sorted({_action_type(row) for row in rows})),
        target_tweet_id=target_tweet_id,
        target_author_handle=target_author_handle,
        target_tweet_text_preview=target_tweet_text_preview,
        posted_tweet_id=posted_tweet_id,
        details=details,
    )


def _missing_target_metadata(row: dict[str, Any]) -> bool:
    return bool(_missing_target_fields(row))


def _missing_target_fields(row: dict[str, Any]) -> tuple[str, ...]:
    fields = ("target_tweet_id", "target_tweet_text", "target_author_handle")
    return tuple(field for field in fields if not _text(row.get(field)))


def _summary(issues: Sequence[ProactiveActionTargetIssue]) -> dict[str, Any]:
    return {
        "by_issue_type": dict(sorted(Counter(issue.issue_type for issue in issues).items())),
        "by_recommendation": dict(sorted(Counter(issue.recommendation for issue in issues).items())),
        "by_severity": dict(sorted(Counter(issue.severity for issue in issues).items())),
    }


def _normalise_action_types(values: str | Sequence[str] | None) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, str):
        values = (values,)
    parsed = tuple(sorted({value.strip().casefold() for value in values if value and value.strip()}))
    invalid = tuple(value for value in parsed if value not in VALID_ACTION_TYPES)
    if invalid:
        raise ValueError(f"invalid action_type: {', '.join(invalid)}")
    return parsed


def _status(row: dict[str, Any]) -> str:
    return _text(row.get("status")).casefold() or PENDING_STATUS


def _action_type(row: dict[str, Any]) -> str:
    return _text(row.get("action_type")).casefold() or "unknown"


def _normalise_handle(value: Any) -> str:
    return _text(value).lstrip("@").casefold()


def _normalise_text(value: Any) -> str:
    text = _URL_RE.sub("", _text(value).casefold())
    text = _NON_WORD_RE.sub(" ", text)
    return _SPACE_RE.sub(" ", text).strip()


def _text(value: Any) -> str:
    return str(value or "").strip()


def _preview(value: Any, limit: int = 120) -> str:
    text = _SPACE_RE.sub(" ", _text(value))
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "..."


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _display_filter(value: Any) -> str:
    if not value:
        return "all"
    if isinstance(value, (list, tuple)):
        return ",".join(str(item) for item in value) or "all"
    return str(value)
