"""Detect likely flaky GitHub Actions workflow trends."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from typing import Any, Mapping


DEFAULT_DAYS = 14
DEFAULT_MIN_RUNS = 2
SUCCESS_CONCLUSIONS = frozenset({"success"})
FAILURE_CONCLUSIONS = frozenset({"failure", "timed_out", "startup_failure", "action_required"})
TERMINAL_CONCLUSIONS = SUCCESS_CONCLUSIONS | FAILURE_CONCLUSIONS | frozenset(
    {"cancelled", "skipped", "neutral"}
)


@dataclass(frozen=True)
class WorkflowFlakeTrend:
    """A grouped workflow trend with likely flakiness signals."""

    repo_name: str
    workflow_name: str
    branch: str
    source_activity_id: str | None
    run_count: int
    failure_count: int
    success_count: int
    rerun_count: int
    latest_url: str
    latest_updated_at: str
    conclusions: tuple[str, ...]
    run_numbers: tuple[int, ...]
    activity_ids: tuple[str, ...]
    reasons: tuple[str, ...]
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "repo_name": self.repo_name,
            "workflow_name": self.workflow_name,
            "branch": self.branch,
            "source_activity_id": self.source_activity_id,
            "run_count": self.run_count,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "rerun_count": self.rerun_count,
            "latest_url": self.latest_url,
            "latest_updated_at": self.latest_updated_at,
            "conclusions": list(self.conclusions),
            "run_numbers": list(self.run_numbers),
            "activity_ids": list(self.activity_ids),
            "reasons": list(self.reasons),
            "recommended_action": self.recommended_action,
        }


@dataclass(frozen=True)
class WorkflowFlakeTrendReport:
    """Read-only workflow flake trend report."""

    generated_at: str
    window_start: str
    window_end: str
    days: int
    min_runs: int
    repo: str | None
    trends: tuple[WorkflowFlakeTrend, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "workflow_flake_trends",
            "generated_at": self.generated_at,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "days": self.days,
            "min_runs": self.min_runs,
            "repo": self.repo,
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns) for table, columns in (self.missing_columns or {}).items()
            },
            "trend_count": len(self.trends),
            "trends": [trend.to_dict() for trend in self.trends],
        }


@dataclass(frozen=True)
class _WorkflowRun:
    repo_name: str
    workflow_name: str
    branch: str
    source_activity_id: str | None
    conclusion: str
    run_number: int
    run_attempt: int
    url: str
    updated_at: datetime
    updated_at_raw: str
    activity_id: str


def build_workflow_flake_trends_report(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_runs: int = DEFAULT_MIN_RUNS,
    repo: str | None = None,
    now: datetime | None = None,
) -> WorkflowFlakeTrendReport:
    """Return likely flaky workflow groups from recent github_activity rows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_runs <= 0:
        raise ValueError("min_runs must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    window_start = generated_at - timedelta(days=days)
    window_end = generated_at
    schema = _schema(db)
    if "github_activity" not in schema:
        return _empty_report(
            generated_at=generated_at,
            window_start=window_start,
            window_end=window_end,
            days=days,
            min_runs=min_runs,
            repo=repo,
            missing_tables=("github_activity",),
        )

    required = ("activity_type", "repo_name", "number")
    missing = tuple(column for column in required if column not in schema["github_activity"])
    if missing:
        return _empty_report(
            generated_at=generated_at,
            window_start=window_start,
            window_end=window_end,
            days=days,
            min_runs=min_runs,
            repo=repo,
            missing_columns={"github_activity": missing},
        )

    rows = _load_workflow_rows(
        db,
        columns=schema["github_activity"],
        window_start=window_start,
        repo=repo,
    )
    runs = [
        run
        for row in rows
        if (run := _normalise_workflow_run(row)) is not None
        and window_start <= run.updated_at <= window_end
    ]

    groups: dict[tuple[str, str, str, str | None], list[_WorkflowRun]] = {}
    for run in runs:
        key = (run.repo_name, run.workflow_name, run.branch, run.source_activity_id)
        groups.setdefault(key, []).append(run)

    trends = tuple(
        sorted(
            (
                trend
                for group_runs in groups.values()
                if (trend := _build_trend(group_runs, min_runs=min_runs)) is not None
            ),
            key=lambda item: (
                -item.failure_count,
                -item.success_count,
                -item.run_count,
                item.repo_name,
                item.workflow_name,
                item.branch,
                item.source_activity_id or "",
            ),
        )
    )
    return WorkflowFlakeTrendReport(
        generated_at=generated_at.isoformat(),
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
        days=days,
        min_runs=min_runs,
        repo=repo,
        trends=trends,
    )


def format_workflow_flake_trends_json(report: WorkflowFlakeTrendReport) -> str:
    """Format a workflow flake trend report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_workflow_flake_trends_text(report: WorkflowFlakeTrendReport) -> str:
    """Format a workflow flake trend report for compact terminal review."""
    lines = [
        "Workflow Flake Trends",
        f"Window: {report.window_start} to {report.window_end}",
    ]
    if report.repo:
        lines.append(f"Repo: {report.repo}")
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
        return "\n".join(lines)
    if report.missing_columns:
        parts = [
            f"{table}: {', '.join(columns)}"
            for table, columns in sorted(report.missing_columns.items())
        ]
        lines.append(f"Missing columns: {'; '.join(parts)}")
        return "\n".join(lines)
    if not report.trends:
        lines.append("No likely flaky workflow groups found.")
        return "\n".join(lines)

    lines.append(f"Trends: {len(report.trends)}")
    for trend in report.trends:
        source = f" source={trend.source_activity_id}" if trend.source_activity_id else ""
        reasons = ", ".join(trend.reasons)
        lines.append(
            f"- {trend.repo_name} | {trend.workflow_name} | {trend.branch}{source}: "
            f"{trend.run_count} runs, {trend.failure_count} failures, "
            f"{trend.success_count} successes, {trend.rerun_count} reruns; "
            f"latest={trend.latest_url or 'none'}; {reasons}; "
            f"{trend.recommended_action}"
        )
    return "\n".join(lines)


def _empty_report(
    *,
    generated_at: datetime,
    window_start: datetime,
    window_end: datetime,
    days: int,
    min_runs: int,
    repo: str | None,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> WorkflowFlakeTrendReport:
    return WorkflowFlakeTrendReport(
        generated_at=generated_at.isoformat(),
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
        days=days,
        min_runs=min_runs,
        repo=repo,
        trends=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns or {},
    )


def _build_trend(
    runs: list[_WorkflowRun],
    *,
    min_runs: int,
) -> WorkflowFlakeTrend | None:
    ordered = sorted(runs, key=lambda run: (run.updated_at, run.activity_id))
    if len(ordered) < min_runs:
        return None

    failure_count = sum(1 for run in ordered if run.conclusion in FAILURE_CONCLUSIONS)
    success_count = sum(1 for run in ordered if run.conclusion in SUCCESS_CONCLUSIONS)
    rerun_count = _rerun_count(ordered)
    reasons = _flake_reasons(ordered, failure_count, success_count, rerun_count)
    if not reasons:
        return None

    latest = max(ordered, key=lambda run: (run.updated_at, run.activity_id))
    return WorkflowFlakeTrend(
        repo_name=latest.repo_name,
        workflow_name=latest.workflow_name,
        branch=latest.branch,
        source_activity_id=latest.source_activity_id,
        run_count=len(ordered),
        failure_count=failure_count,
        success_count=success_count,
        rerun_count=rerun_count,
        latest_url=latest.url,
        latest_updated_at=latest.updated_at_raw,
        conclusions=tuple(sorted({run.conclusion for run in ordered if run.conclusion})),
        run_numbers=tuple(sorted({run.run_number for run in ordered if run.run_number > 0})),
        activity_ids=tuple(sorted(run.activity_id for run in ordered)),
        reasons=tuple(reasons),
        recommended_action=_recommended_action(reasons),
    )


def _rerun_count(runs: list[_WorkflowRun]) -> int:
    attempt_count = sum(1 for run in runs if run.run_attempt > 1)
    run_numbers = [run.run_number for run in runs if run.run_number > 0]
    duplicate_count = len(run_numbers) - len(set(run_numbers))
    return max(attempt_count, duplicate_count)


def _flake_reasons(
    runs: list[_WorkflowRun],
    failure_count: int,
    success_count: int,
    rerun_count: int,
) -> list[str]:
    reasons: list[str] = []
    if failure_count and success_count:
        reasons.append("mixed success/failure conclusions")
    if rerun_count:
        reasons.append("repeated reruns")
    if _has_failure_then_success(runs):
        reasons.append("failure followed by success")
    return reasons


def _has_failure_then_success(runs: list[_WorkflowRun]) -> bool:
    seen_failure = False
    for run in runs:
        if run.conclusion in FAILURE_CONCLUSIONS:
            seen_failure = True
        elif seen_failure and run.conclusion in SUCCESS_CONCLUSIONS:
            return True
    return False


def _recommended_action(reasons: list[str]) -> str:
    if "failure followed by success" in reasons:
        return "Review logs around the failing attempt and capture the stabilization fix."
    if "repeated reruns" in reasons:
        return "Inspect rerun attempts for non-deterministic test or dependency behavior."
    return "Compare failed and successful runs before the workflow becomes noisy."


def _load_workflow_rows(
    db: Any,
    *,
    columns: set[str],
    window_start: datetime,
    repo: str | None,
) -> list[dict[str, Any]]:
    conn = getattr(db, "conn", db)
    selected = ", ".join(_quote_identifier(column) for column in sorted(columns))
    where = ["activity_type = ?"]
    params: list[Any] = ["workflow_run"]
    if "updated_at" in columns:
        where.append("updated_at >= ?")
        params.append(window_start.isoformat())
    if repo and "repo_name" in columns:
        where.append("repo_name = ?")
        params.append(repo)
    order = "updated_at ASC, id ASC" if "updated_at" in columns and "id" in columns else "rowid ASC"
    cursor = conn.execute(
        f"SELECT {selected} FROM github_activity WHERE {' AND '.join(where)} ORDER BY {order}",
        tuple(params),
    )
    return [_row_to_dict(row, cursor.description) for row in cursor.fetchall()]


def _normalise_workflow_run(row: dict[str, Any]) -> _WorkflowRun | None:
    metadata = _json_object(row.get("metadata")) or {}
    updated_raw = _first_value(
        row,
        metadata,
        "updated_at",
        "run_started_at",
        "created_at_github",
        "created_at",
        "created_at_github",
    )
    updated_at = _parse_datetime(updated_raw)
    if updated_at is None:
        return None

    conclusion = str(
        _first_value(row, metadata, "conclusion", "state", "status") or ""
    ).strip().lower()
    if conclusion not in TERMINAL_CONCLUSIONS:
        return None

    repo_name = str(_first_value(row, metadata, "repo_name", "repo", "repository") or "")
    workflow_name = str(
        _first_value(row, metadata, "workflow_name", "name", "workflow") or row.get("title") or "workflow"
    )
    branch = str(_first_value(row, metadata, "branch", "head_branch", "ref") or "unknown")
    number = _int_value(_first_value(row, metadata, "run_number", "run_id", "number"))
    run_attempt = _int_value(_first_value(row, metadata, "run_attempt", "attempt"), default=1)
    url = str(_first_value(row, metadata, "run_url", "html_url", "url") or row.get("url") or "")
    activity_id = str(
        _first_value(row, metadata, "activity_id")
        or f"{repo_name}#{row.get('number', '')}:workflow_run"
    )
    source_activity_id = _optional_str(
        _first_value(
            row,
            metadata,
            "source_activity_id",
            "linked_activity_id",
            "source_github_activity_id",
            "source_activity",
        )
    )

    return _WorkflowRun(
        repo_name=repo_name,
        workflow_name=workflow_name,
        branch=branch,
        source_activity_id=source_activity_id,
        conclusion=conclusion,
        run_number=number,
        run_attempt=max(1, run_attempt),
        url=url,
        updated_at=updated_at,
        updated_at_raw=str(updated_raw),
        activity_id=activity_id,
    )


def _first_value(row: dict[str, Any], metadata: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
        value = metadata.get(key)
        if value not in (None, ""):
            return value
    return None


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _int_value(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _json_object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if not value:
        return None
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError):
        return None
    return decoded if isinstance(decoded, dict) else None


def _row_to_dict(row: Any, description: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    names = [column[0] for column in description]
    return dict(zip(names, row))


def _schema(db: Any) -> dict[str, set[str]]:
    conn = getattr(db, "conn", db)
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    ).fetchall()
    names = [row["name"] if isinstance(row, Mapping) else row[0] for row in rows]
    return {
        name: {column[1] for column in conn.execute(f"PRAGMA table_info({_quote_identifier(name)})")}
        for name in names
    }


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
