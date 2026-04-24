"""Seed content ideas from failed GitHub workflow runs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


SOURCE_NAME = "github_workflow_failure_seed"
DEFAULT_MIN_SCORE = 60.0


@dataclass(frozen=True)
class WorkflowFailureCandidate:
    activity_id: str
    repo_name: str
    workflow_name: str
    run_number: int
    conclusion: str
    head_sha: str | None
    branch: str | None
    url: str
    updated_at: str
    topic: str
    note: str
    priority: str
    score: float
    score_reasons: list[str]
    source_metadata: dict[str, Any]


@dataclass(frozen=True)
class WorkflowFailureSeedResult:
    status: str
    repo_name: str
    workflow_name: str
    run_number: int
    topic: str
    score: float
    idea_id: int | None
    reason: str
    note: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _metadata_value(row: dict, *keys: str) -> Any:
    metadata = row.get("metadata") or {}
    for key in keys:
        if row.get(key) not in (None, ""):
            return row.get(key)
        if isinstance(metadata, dict) and metadata.get(key) not in (None, ""):
            return metadata.get(key)
    return None


def _workflow_name(row: dict) -> str:
    return str(_metadata_value(row, "workflow_name", "name") or row.get("title") or "workflow")


def _run_number(row: dict) -> int:
    value = _metadata_value(row, "run_number", "run_id") or row.get("number") or 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _conclusion(row: dict) -> str:
    value = _metadata_value(row, "conclusion") or row.get("state") or ""
    return str(value).strip().lower()


def _head_sha(row: dict) -> str | None:
    value = _metadata_value(row, "head_sha", "sha")
    return str(value) if value not in (None, "") else None


def _branch(row: dict) -> str | None:
    value = _metadata_value(row, "branch", "head_branch", "ref")
    return str(value) if value not in (None, "") else None


def _url(row: dict) -> str:
    return str(_metadata_value(row, "url", "html_url") or row.get("url") or "")


def _source_activity_id(row: dict) -> str | None:
    value = _metadata_value(
        row,
        "source_activity_id",
        "linked_activity_id",
        "github_activity_id",
        "source_github_activity_id",
    )
    return str(value) if value not in (None, "") else None


def _repeated_failure_counts(rows: list[dict], cutoff: datetime) -> dict[tuple[str, str], int]:
    counts: dict[tuple[str, str], int] = {}
    for row in rows:
        updated_at = _parse_datetime(row.get("updated_at"))
        if updated_at is None or updated_at < cutoff:
            continue
        if _conclusion(row) != "failure":
            continue
        key = (str(row.get("repo_name") or ""), _workflow_name(row))
        counts[key] = counts.get(key, 0) + 1
    return counts


def _score_candidate(
    row: dict,
    *,
    now: datetime,
    repeated_count: int,
) -> tuple[float, list[str]]:
    score = 45.0
    reasons = ["conclusion=failure+45"]

    updated_at = _parse_datetime(row.get("updated_at"))
    if updated_at is not None:
        age_days = max(0.0, (now - updated_at).total_seconds() / 86400)
        if age_days <= 1:
            score += 20
            reasons.append("updated<=1d+20")
        elif age_days <= 3:
            score += 14
            reasons.append("updated<=3d+14")
        elif age_days <= 7:
            score += 8
            reasons.append("updated<=7d+8")
        else:
            score += 4
            reasons.append("updated>7d+4")

    if repeated_count >= 3:
        score += 20
        reasons.append("repeated-failures>=3+20")
    elif repeated_count == 2:
        score += 12
        reasons.append("repeated-failures=2+12")

    if _head_sha(row):
        score += 8
        reasons.append("head-sha+8")
    if _source_activity_id(row):
        score += 7
        reasons.append("linked-source-activity+7")

    return round(score, 2), reasons


def workflow_run_to_candidate(
    row: dict,
    *,
    now: datetime | None = None,
    repeated_count: int = 1,
) -> WorkflowFailureCandidate:
    now = now or datetime.now(timezone.utc)
    repo_name = str(row.get("repo_name") or "")
    workflow_name = _workflow_name(row)
    run_number = _run_number(row)
    conclusion = _conclusion(row)
    head_sha = _head_sha(row)
    branch = _branch(row)
    url = _url(row)
    source_activity_id = _source_activity_id(row)
    score, score_reasons = _score_candidate(row, now=now, repeated_count=repeated_count)
    topic = f"{repo_name}: {workflow_name} workflow failure"
    note_parts = [
        f"Failed workflow run {workflow_name} #{run_number} in {repo_name}.",
        f"Conclusion: {conclusion}.",
        f"Branch: {branch or 'unknown'}.",
        f"Head SHA: {head_sha or 'unknown'}.",
        f"URL: {url or 'none'}.",
        "Review the failure for a concrete debugging lesson before publishing.",
    ]
    note = " ".join(note_parts)
    priority = "high" if score >= 80 else "normal"
    metadata = {
        "source": SOURCE_NAME,
        "github_activity_id": row.get("activity_id"),
        "workflow_name": workflow_name,
        "repo_name": repo_name,
        "run_number": run_number,
        "head_sha": head_sha,
        "branch": branch,
        "url": url,
        "conclusion": conclusion,
        "score": score,
        "score_reasons": score_reasons,
        "updated_at": row.get("updated_at"),
    }
    if source_activity_id:
        metadata["source_activity_id"] = source_activity_id
    return WorkflowFailureCandidate(
        activity_id=str(row.get("activity_id") or ""),
        repo_name=repo_name,
        workflow_name=workflow_name,
        run_number=run_number,
        conclusion=conclusion,
        head_sha=head_sha,
        branch=branch,
        url=url,
        updated_at=str(row.get("updated_at") or ""),
        topic=topic,
        note=note,
        priority=priority,
        score=score,
        score_reasons=score_reasons,
        source_metadata=metadata,
    )


def seed_workflow_failure_ideas(
    db,
    *,
    days: int = 7,
    min_score: float = DEFAULT_MIN_SCORE,
    limit: int | None = 25,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[WorkflowFailureSeedResult]:
    """Create content ideas from failed workflow_run GitHub activity."""
    if days <= 0 or (limit is not None and limit <= 0):
        return []
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)
    rows = db.get_github_workflow_runs(limit=limit)
    repeated_counts = _repeated_failure_counts(rows, cutoff)

    results: list[WorkflowFailureSeedResult] = []
    for row in rows:
        repo_name = str(row.get("repo_name") or "")
        workflow_name = _workflow_name(row)
        run_number = _run_number(row)
        updated_at = _parse_datetime(row.get("updated_at"))
        base_result = {
            "repo_name": repo_name,
            "workflow_name": workflow_name,
            "run_number": run_number,
            "topic": f"{repo_name}: {workflow_name} workflow failure",
            "idea_id": None,
            "note": "",
        }
        if updated_at is None or updated_at < cutoff:
            results.append(
                WorkflowFailureSeedResult(
                    status="skipped",
                    score=0.0,
                    reason="stale",
                    **base_result,
                )
            )
            continue

        conclusion = _conclusion(row)
        if conclusion != "failure":
            reason = f"conclusion {conclusion or 'unknown'}"
            results.append(
                WorkflowFailureSeedResult(
                    status="skipped",
                    score=0.0,
                    reason=reason,
                    **base_result,
                )
            )
            continue

        candidate = workflow_run_to_candidate(
            row,
            now=now,
            repeated_count=repeated_counts.get((repo_name, workflow_name), 1),
        )
        if candidate.score < min_score:
            results.append(
                WorkflowFailureSeedResult(
                    status="skipped",
                    repo_name=candidate.repo_name,
                    workflow_name=candidate.workflow_name,
                    run_number=candidate.run_number,
                    topic=candidate.topic,
                    score=candidate.score,
                    idea_id=None,
                    reason=f"score below {min_score:g}",
                    note=candidate.note,
                )
            )
            continue

        existing = db.find_active_content_idea_for_source_metadata(
            source=SOURCE_NAME,
            source_metadata={"github_activity_id": candidate.activity_id},
        )
        if existing:
            results.append(
                WorkflowFailureSeedResult(
                    status="skipped",
                    repo_name=candidate.repo_name,
                    workflow_name=candidate.workflow_name,
                    run_number=candidate.run_number,
                    topic=candidate.topic,
                    score=candidate.score,
                    idea_id=existing["id"],
                    reason=f"{existing['status']} duplicate",
                    note=candidate.note,
                )
            )
            continue

        if dry_run:
            results.append(
                WorkflowFailureSeedResult(
                    status="proposed",
                    repo_name=candidate.repo_name,
                    workflow_name=candidate.workflow_name,
                    run_number=candidate.run_number,
                    topic=candidate.topic,
                    score=candidate.score,
                    idea_id=None,
                    reason="dry run",
                    note=candidate.note,
                )
            )
            continue

        idea_id = db.insert_content_idea(
            note=candidate.note,
            topic=candidate.topic,
            priority=candidate.priority,
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        results.append(
            WorkflowFailureSeedResult(
                status="created",
                repo_name=candidate.repo_name,
                workflow_name=candidate.workflow_name,
                run_number=candidate.run_number,
                topic=candidate.topic,
                score=candidate.score,
                idea_id=idea_id,
                reason="created",
                note=candidate.note,
            )
        )

    return results
