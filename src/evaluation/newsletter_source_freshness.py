"""Classify newsletter issues by source freshness and reuse."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_MAX_SOURCE_AGE_DAYS = 30
DEFAULT_REUSE_THRESHOLD = 2


@dataclass(frozen=True)
class NewsletterSourceFreshnessIssue:
    """Freshness classification for one newsletter issue."""

    issue_id: str
    newsletter_send_id: int
    source_count: int
    newest_source_age_days: int | None
    oldest_source_age_days: int | None
    reused_source_ids: tuple[str, ...]
    freshness_status: str
    reason_codes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reused_source_ids"] = list(self.reused_source_ids)
        payload["reason_codes"] = list(self.reason_codes)
        return payload


@dataclass(frozen=True)
class NewsletterSourceFreshnessReport:
    """Newsletter source freshness report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    issues: tuple[NewsletterSourceFreshnessIssue, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_source_freshness",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "issue_count": len(self.issues),
            "issues": [issue.to_dict() for issue in self.issues],
            "totals": dict(self.totals),
        }


def build_newsletter_source_freshness_report(
    db_or_conn: Any,
    *,
    max_source_age_days: int = DEFAULT_MAX_SOURCE_AGE_DAYS,
    reuse_threshold: int = DEFAULT_REUSE_THRESHOLD,
    now: datetime | None = None,
) -> NewsletterSourceFreshnessReport:
    """Inspect newsletter source payloads for stale or reused material."""
    if max_source_age_days <= 0:
        raise ValueError("max_source_age_days must be positive")
    if reuse_threshold <= 0:
        raise ValueError("reuse_threshold must be positive")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    sends = _load_sends(conn)
    source_lookup = _generated_content_dates(conn)
    parsed = [(_send, _source_refs(_send, source_lookup, generated_at)) for _send in sends]
    counts = Counter(ref["canonical_id"] for _send, refs in parsed for ref in refs)
    issues = tuple(
        _issue(send, refs, counts, generated_at, max_source_age_days, reuse_threshold)
        for send, refs in parsed
    )
    return NewsletterSourceFreshnessReport(
        generated_at=generated_at.isoformat(),
        filters={
            "max_source_age_days": max_source_age_days,
            "reuse_threshold": reuse_threshold,
        },
        totals={
            "newsletter_send_count": len(sends),
            "source_reference_count": sum(issue.source_count for issue in issues),
            "source_light_count": sum(1 for issue in issues if issue.freshness_status == "source-light"),
            "stale_count": sum(1 for issue in issues if issue.freshness_status == "stale"),
        },
        issues=issues,
    )


def format_newsletter_source_freshness_json(report: NewsletterSourceFreshnessReport) -> str:
    """Serialize as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_source_freshness_text(report: NewsletterSourceFreshnessReport) -> str:
    """Render newsletter source freshness for review."""
    lines = [
        "Newsletter Source Freshness",
        f"Generated: {report.generated_at}",
        (
            f"Filters: max_source_age_days={report.filters['max_source_age_days']} "
            f"reuse_threshold={report.filters['reuse_threshold']}"
        ),
        (
            f"Totals: sends={report.totals['newsletter_send_count']} "
            f"sources={report.totals['source_reference_count']} stale={report.totals['stale_count']}"
        ),
    ]
    if not report.issues:
        lines.extend(["", "No newsletter issues found."])
        return "\n".join(lines)
    lines.extend(["", "Issues:"])
    for issue in report.issues:
        lines.append(
            f"- issue_id={issue.issue_id} send_id={issue.newsletter_send_id} "
            f"status={issue.freshness_status} sources={issue.source_count} "
            f"newest_age={issue.newest_source_age_days} oldest_age={issue.oldest_source_age_days} "
            f"reused={','.join(issue.reused_source_ids) or '-'}"
        )
    return "\n".join(lines)


def _load_sends(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    tables = {row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "newsletter_sends" not in tables:
        return []
    return conn.execute(
        """SELECT id, issue_id, source_content_ids, metadata, sent_at
             FROM newsletter_sends
             ORDER BY datetime(sent_at) DESC, id DESC"""
    ).fetchall()


def _generated_content_dates(conn: sqlite3.Connection) -> dict[str, datetime]:
    tables = {row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "generated_content" not in tables:
        return {}
    lookup: dict[str, datetime] = {}
    for row in conn.execute("SELECT id, created_at FROM generated_content").fetchall():
        parsed = _parse_dt(row["created_at"])
        if parsed is not None:
            lookup[str(row["id"])] = parsed
    return lookup


def _source_refs(send: sqlite3.Row, source_lookup: dict[str, datetime], now: datetime) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    for value in _json_list(send["source_content_ids"]):
        canonical = _canonical_id(value)
        refs.append({"canonical_id": canonical, "date": source_lookup.get(str(value))})
    metadata = _json_obj(send["metadata"])
    for source in metadata.get("sources") or metadata.get("source_payloads") or []:
        if not isinstance(source, dict):
            continue
        canonical = _canonical_id(source.get("canonical_id") or source.get("source_id") or source.get("id") or source.get("url"))
        refs.append(
            {
                "canonical_id": canonical,
                "date": _parse_dt(source.get("published_at") or source.get("created_at") or source.get("date")),
            }
        )
    return [ref for ref in refs if ref["canonical_id"]]


def _issue(
    send: sqlite3.Row,
    refs: list[dict[str, Any]],
    counts: Counter[str],
    now: datetime,
    max_source_age_days: int,
    reuse_threshold: int,
) -> NewsletterSourceFreshnessIssue:
    ages = [int((now - ref["date"]).days) for ref in refs if ref.get("date") is not None]
    reused = tuple(sorted({ref["canonical_id"] for ref in refs if counts[ref["canonical_id"]] >= reuse_threshold}))
    reasons: list[str] = []
    if len(refs) < 2:
        reasons.append("source_light")
        status = "source-light"
    elif not ages:
        reasons.append("missing_source_dates")
        status = "source-light"
    elif min(ages) > max_source_age_days:
        reasons.append("all_sources_stale")
        status = "stale"
    elif max(ages) > max_source_age_days:
        reasons.append("mixed_fresh_stale_sources")
        status = "aging"
    else:
        status = "fresh"
    if reused:
        reasons.append("reused_sources")
        if status == "fresh":
            status = "aging"
    return NewsletterSourceFreshnessIssue(
        issue_id=str(send["issue_id"] or send["id"]),
        newsletter_send_id=int(send["id"]),
        source_count=len(refs),
        newest_source_age_days=min(ages) if ages else None,
        oldest_source_age_days=max(ages) if ages else None,
        reused_source_ids=reused,
        freshness_status=status,
        reason_codes=tuple(reasons),
    )


def _canonical_id(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text.removeprefix("http://").removeprefix("https://").removesuffix("/")


def _json_list(value: Any) -> list[Any]:
    parsed = _json(value)
    return parsed if isinstance(parsed, list) else []


def _json_obj(value: Any) -> dict[str, Any]:
    parsed = _json(value)
    return parsed if isinstance(parsed, dict) else {}


def _json(value: Any) -> Any:
    if not value:
        return None
    try:
        return json.loads(value) if isinstance(value, str) else value
    except json.JSONDecodeError:
        return None


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
