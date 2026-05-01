"""Plan safe retry order for paused or quarantined curated sources."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


BUCKETS = ("retry", "wait", "manual_review")


@dataclass(frozen=True)
class SourceRecoveryCandidate:
    id: int
    source_type: str
    identifier: str
    name: str | None
    bucket: str
    reason_codes: list[str]
    status: str
    active: bool
    last_fetch_status: str | None
    consecutive_failures: int
    last_failure_at: str | None
    last_success_at: str | None
    last_error: str | None
    failure_age_days: float | None
    success_age_days: float | None


@dataclass(frozen=True)
class SourceRecoveryPlan:
    artifact_type: str
    stale_days: int
    max_failures: int
    source_type: str | None
    limit: int
    generated_at: str
    considered_count: int
    bucket_counts: dict[str, int]
    retry: list[SourceRecoveryCandidate]
    wait: list[SourceRecoveryCandidate]
    manual_review: list[SourceRecoveryCandidate]
    missing_required_tables: list[str]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_source_recovery_plan(
    db,
    stale_days: int = 7,
    max_failures: int = 5,
    source_type: str | None = None,
    limit: int = 50,
    now: datetime | None = None,
) -> SourceRecoveryPlan:
    """Return a read-only recovery plan for paused or quarantined curated sources."""

    if stale_days < 0:
        raise ValueError("stale_days must be >= 0")
    if max_failures < 0:
        raise ValueError("max_failures must be >= 0")
    if limit <= 0:
        raise ValueError("limit must be positive")

    now = _normalize_datetime(now or datetime.now(timezone.utc))
    conn = getattr(db, "conn", db)
    if not _table_exists(conn, "curated_sources"):
        return _empty_plan(
            stale_days=stale_days,
            max_failures=max_failures,
            source_type=source_type,
            limit=limit,
            now=now,
            missing=["curated_sources"],
        )

    rows = _load_recovery_rows(conn, source_type=source_type, limit=limit)
    candidates = [
        _classify_row(
            row,
            stale_days=stale_days,
            max_failures=max_failures,
            now=now,
        )
        for row in rows
    ]
    retry = sorted(
        [candidate for candidate in candidates if candidate.bucket == "retry"],
        key=_retry_sort_key,
    )
    wait = sorted(
        [candidate for candidate in candidates if candidate.bucket == "wait"],
        key=_non_retry_sort_key,
    )
    manual_review = sorted(
        [candidate for candidate in candidates if candidate.bucket == "manual_review"],
        key=_non_retry_sort_key,
    )
    return SourceRecoveryPlan(
        artifact_type="source_recovery_plan",
        stale_days=stale_days,
        max_failures=max_failures,
        source_type=source_type,
        limit=limit,
        generated_at=now.isoformat(),
        considered_count=len(candidates),
        bucket_counts={
            "retry": len(retry),
            "wait": len(wait),
            "manual_review": len(manual_review),
        },
        retry=retry,
        wait=wait,
        manual_review=manual_review,
        missing_required_tables=[],
    )


def export_to_json(plan: SourceRecoveryPlan) -> str:
    """Serialize a source recovery plan as stable JSON."""

    return json.dumps(plan.as_dict(), indent=2, sort_keys=True)


def format_text_report(plan: SourceRecoveryPlan) -> str:
    """Render a source recovery plan for terminal review."""

    lines = [
        "Source Recovery Plan",
        (
            "Filters: "
            f"source_type={plan.source_type or 'all'} "
            f"stale_days={plan.stale_days} "
            f"max_failures={plan.max_failures} "
            f"limit={plan.limit}"
        ),
        (
            "Counts: "
            f"considered={plan.considered_count} "
            f"retry={plan.bucket_counts['retry']} "
            f"wait={plan.bucket_counts['wait']} "
            f"manual_review={plan.bucket_counts['manual_review']}"
        ),
    ]
    if plan.missing_required_tables:
        lines.append("Missing required tables: " + ", ".join(plan.missing_required_tables))
    if not any((plan.retry, plan.wait, plan.manual_review)):
        lines.append("")
        lines.append("No paused or quarantined curated sources found.")
        return "\n".join(lines)

    for bucket in BUCKETS:
        candidates = getattr(plan, bucket)
        lines.append("")
        lines.append(f"{bucket}: {len(candidates)}")
        if not candidates:
            lines.append("  -")
            continue
        for candidate in candidates:
            lines.append(
                f"  - #{candidate.id} {candidate.source_type} "
                f"{_source_label(candidate)} failures={candidate.consecutive_failures} "
                f"last_failure={candidate.last_failure_at or '-'} "
                f"reasons={','.join(candidate.reason_codes)}"
            )
    return "\n".join(lines)


def _load_recovery_rows(
    conn: sqlite3.Connection,
    *,
    source_type: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    filters = [
        "(COALESCE(status, 'active') = 'paused' OR last_fetch_status = 'quarantined')"
    ]
    params: list[Any] = []
    if source_type:
        filters.append("source_type = ?")
        params.append(source_type)
    params.append(limit)
    rows = conn.execute(
        f"""SELECT id, source_type, identifier, name, status, active,
                  last_fetch_status, consecutive_failures, last_success_at,
                  last_failure_at, last_error
           FROM curated_sources
           WHERE {' AND '.join(filters)}
           ORDER BY source_type ASC, identifier ASC, id ASC
           LIMIT ?""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _classify_row(
    row: dict[str, Any],
    *,
    stale_days: int,
    max_failures: int,
    now: datetime,
) -> SourceRecoveryCandidate:
    failures = int(row.get("consecutive_failures") or 0)
    last_failure_at = row.get("last_failure_at")
    last_success_at = row.get("last_success_at")
    last_failure = _parse_datetime(last_failure_at)
    last_success = _parse_datetime(last_success_at)
    failure_age_days = _age_days(now, last_failure)
    success_age_days = _age_days(now, last_success)

    if failures <= 0:
        bucket = "manual_review"
        reasons = ["no_failures_recorded"]
    elif last_failure is None:
        bucket = "manual_review"
        reasons = ["missing_failure_at"]
    elif failures > max_failures:
        bucket = "manual_review"
        reasons = ["too_many_failures"]
    elif last_success is None:
        bucket = "manual_review"
        reasons = ["no_success_history"]
    elif failure_age_days is not None and failure_age_days < stale_days:
        bucket = "wait"
        reasons = ["failure_too_recent"]
    else:
        bucket = "retry"
        reasons = ["stale_failure", "within_failure_budget", "success_history"]

    return SourceRecoveryCandidate(
        id=int(row["id"]),
        source_type=row.get("source_type") or "",
        identifier=row.get("identifier") or "",
        name=row.get("name"),
        bucket=bucket,
        reason_codes=reasons,
        status=row.get("status") or "active",
        active=bool(row.get("active", 1)),
        last_fetch_status=row.get("last_fetch_status"),
        consecutive_failures=failures,
        last_failure_at=last_failure_at,
        last_success_at=last_success_at,
        last_error=row.get("last_error"),
        failure_age_days=failure_age_days,
        success_age_days=success_age_days,
    )


def _empty_plan(
    *,
    stale_days: int,
    max_failures: int,
    source_type: str | None,
    limit: int,
    now: datetime,
    missing: list[str],
) -> SourceRecoveryPlan:
    return SourceRecoveryPlan(
        artifact_type="source_recovery_plan",
        stale_days=stale_days,
        max_failures=max_failures,
        source_type=source_type,
        limit=limit,
        generated_at=now.isoformat(),
        considered_count=0,
        bucket_counts={"retry": 0, "wait": 0, "manual_review": 0},
        retry=[],
        wait=[],
        manual_review=[],
        missing_required_tables=missing,
    )


def _retry_sort_key(candidate: SourceRecoveryCandidate) -> tuple[str, str, int, int]:
    return (
        candidate.source_type,
        candidate.last_failure_at or "",
        candidate.consecutive_failures,
        candidate.id,
    )


def _non_retry_sort_key(
    candidate: SourceRecoveryCandidate,
) -> tuple[str, int, str, int]:
    return (
        candidate.source_type,
        candidate.consecutive_failures,
        candidate.last_failure_at or "",
        candidate.id,
    )


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _normalize_datetime(value)
    if not isinstance(value, str):
        return None
    try:
        return _normalize_datetime(datetime.fromisoformat(value))
    except ValueError:
        return None


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _age_days(now: datetime, then: datetime | None) -> float | None:
    if then is None:
        return None
    return round(max((now - then).total_seconds(), 0) / 86400, 3)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _source_label(candidate: SourceRecoveryCandidate) -> str:
    if candidate.source_type == "x_account":
        return f"@{candidate.identifier.lstrip('@')}"
    return candidate.identifier
