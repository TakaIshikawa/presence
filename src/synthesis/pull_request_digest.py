"""Rank GitHub pull request activity into reviewable content ideas."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


SOURCE_NAME = "github_pull_request_digest"
DEFAULT_DAYS = 14
DEFAULT_MIN_SCORE = 42.0
BODY_EXCERPT_CHARS = 360
MAX_FILE_HINTS = 6

HIGH_SIGNAL_LABELS = {
    "bug": 8,
    "customer": 14,
    "docs": 8,
    "documentation": 8,
    "enhancement": 8,
    "feature": 8,
    "incident": 16,
    "performance": 12,
    "priority": 12,
    "regression": 14,
    "release": 8,
    "security": 16,
    "ux": 10,
}

LOW_SIGNAL_LABELS = {
    "chore",
    "dependencies",
    "dependency",
    "renovate",
}


@dataclass(frozen=True)
class PullRequestDigestItem:
    repo_name: str
    number: int
    activity_id: str
    title: str
    url: str
    author: str
    state: str
    labels: list[str]
    merged_at: str
    updated_at: str
    changed_file_hints: list[str]
    changed_files_count: int | None
    additions: int | None
    deletions: int | None
    commits: int | None
    topic: str
    angle: str
    note: str
    score: float
    score_reasons: list[str]
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PullRequestSeedResult:
    status: str
    repo_name: str
    number: int
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
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _shorten(text: str | None, width: int = 70) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _body_excerpt(text: str | None) -> str:
    return _shorten(text, BODY_EXCERPT_CHARS) or "No pull request body provided."


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _metadata(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("metadata") or {}
    return value if isinstance(value, dict) else {}


def _is_merged(row: dict[str, Any]) -> bool:
    metadata = _metadata(row)
    return bool(row.get("merged_at") or metadata.get("merged") is True)


def _merged_at(row: dict[str, Any]) -> str:
    metadata = _metadata(row)
    return str(
        row.get("merged_at")
        or metadata.get("merged_at")
        or row.get("closed_at")
        or row.get("updated_at")
        or ""
    )


def _labels(row: dict[str, Any]) -> list[str]:
    return sorted(
        {str(label).strip() for label in (row.get("labels") or []) if str(label).strip()},
        key=str.lower,
    )


def _file_path_from_entry(entry: Any) -> str | None:
    if isinstance(entry, str):
        return entry.strip() or None
    if isinstance(entry, dict):
        for key in ("filename", "path", "name"):
            value = entry.get(key)
            if value:
                return str(value).strip()
    return None


def _changed_file_hints(metadata: dict[str, Any]) -> list[str]:
    candidates: list[Any] = []
    for key in ("files", "changed_files_detail", "changed_file_paths", "file_paths", "paths"):
        value = metadata.get(key)
        if isinstance(value, list):
            candidates.extend(value)

    paths: list[str] = []
    seen: set[str] = set()
    for entry in candidates:
        path = _file_path_from_entry(entry)
        if not path or path in seen:
            continue
        seen.add(path)
        paths.append(path)
        if len(paths) >= MAX_FILE_HINTS:
            return paths

    changed_files = _int_or_none(metadata.get("changed_files"))
    if changed_files is not None:
        paths.append(f"{changed_files} changed files")
    return paths[:MAX_FILE_HINTS]


def _score(row: dict[str, Any], *, now: datetime) -> tuple[float, list[str]]:
    metadata = _metadata(row)
    labels = [label.lower() for label in _labels(row)]
    title = str(row.get("title") or "")
    body = str(row.get("body") or "")
    text = f"{title} {body}".lower()
    score = 0.0
    reasons: list[str] = []

    if _is_merged(row):
        score += 24
        reasons.append("merged+24")
    elif str(row.get("state") or "").lower() == "closed":
        score += 8
        reasons.append("closed+8")

    for label in labels:
        points = HIGH_SIGNAL_LABELS.get(label, 0)
        if points:
            score += points
            reasons.append(f"label:{label}+{points}")

    changed_files = _int_or_none(metadata.get("changed_files"))
    additions = _int_or_none(metadata.get("additions"))
    deletions = _int_or_none(metadata.get("deletions"))
    commits = _int_or_none(metadata.get("commits"))
    total_delta = (additions or 0) + (deletions or 0)

    if changed_files is not None:
        if changed_files >= 8:
            score += 14
            reasons.append("changed_files>=8+14")
        elif changed_files >= 3:
            score += 8
            reasons.append("changed_files>=3+8")
    if total_delta >= 500:
        score += 10
        reasons.append("delta>=500+10")
    elif total_delta >= 100:
        score += 6
        reasons.append("delta>=100+6")
    if commits and commits >= 3:
        score += 6
        reasons.append("commits>=3+6")
    if len(title.split()) >= 5:
        score += 6
        reasons.append("specific-title+6")
    if len(body.split()) >= 20:
        score += 8
        reasons.append("body-detail+8")
    if re.search(
        r"\b(api|cli|customer|database|incident|pipeline|regression|workflow|release)\b",
        text,
    ):
        score += 8
        reasons.append("product-context+8")

    merged_at = _parse_datetime(_merged_at(row))
    if merged_at is not None:
        age_days = max(0.0, (now - merged_at).total_seconds() / 86400)
        if age_days <= 2:
            score += 8
            reasons.append("merged<=2d+8")
        elif age_days <= 7:
            score += 4
            reasons.append("merged<=7d+4")

    if labels and set(labels) <= LOW_SIGNAL_LABELS and score < 50:
        score -= 10
        reasons.append("low-signal-labels-10")

    return round(score, 2), reasons


def pull_request_to_digest_item(
    row: dict[str, Any],
    *,
    now: datetime | None = None,
) -> PullRequestDigestItem:
    now = now or datetime.now(timezone.utc)
    metadata = _metadata(row)
    repo_name = str(row.get("repo_name") or "")
    number = int(row.get("number"))
    title = str(row.get("title") or f"Pull request #{number}")
    url = str(row.get("url") or "")
    labels = _labels(row)
    file_hints = _changed_file_hints(metadata)
    score, reasons = _score(row, now=now)
    changed_files = _int_or_none(metadata.get("changed_files"))
    additions = _int_or_none(metadata.get("additions"))
    deletions = _int_or_none(metadata.get("deletions"))
    commits = _int_or_none(metadata.get("commits"))
    body_excerpt = _body_excerpt(row.get("body"))
    merged_at = _merged_at(row)
    topic = f"{repo_name}: shipped {title}".strip(": ")
    hint_text = ", ".join(file_hints) if file_hints else "none"
    angle = (
        f"Turn merged PR #{number} into a shipping note: what changed, why it mattered, "
        "and what readers can learn from the implementation."
    )
    note = (
        f"GitHub pull request #{number} in {repo_name}: {title}. "
        f"Merged: {merged_at or 'unknown'}. "
        f"Labels: {', '.join(labels) if labels else 'none'}. "
        f"Changed files: {hint_text}. "
        f"Additions/deletions: {additions if additions is not None else 'unknown'}/"
        f"{deletions if deletions is not None else 'unknown'}. "
        f"URL: {url or 'none'}. "
        f"Body excerpt: {body_excerpt} "
        f"Suggested angle: {angle}"
    )
    source_metadata = {
        "source": SOURCE_NAME,
        "activity_id": row.get("activity_id"),
        "github_activity_id": row.get("id"),
        "repo_name": repo_name,
        "number": number,
        "pull_request_number": number,
        "title": title,
        "url": url,
        "labels": labels,
        "merged_at": merged_at,
        "updated_at": row.get("updated_at"),
        "changed_file_hints": file_hints,
        "changed_files": changed_files,
        "additions": additions,
        "deletions": deletions,
        "commits": commits,
        "score": score,
        "score_reasons": reasons,
        "body_excerpt": body_excerpt,
    }
    return PullRequestDigestItem(
        repo_name=repo_name,
        number=number,
        activity_id=str(row.get("activity_id") or ""),
        title=title,
        url=url,
        author=str(row.get("author") or ""),
        state=str(row.get("state") or ""),
        labels=labels,
        merged_at=merged_at,
        updated_at=str(row.get("updated_at") or ""),
        changed_file_hints=file_hints,
        changed_files_count=changed_files,
        additions=additions,
        deletions=deletions,
        commits=commits,
        topic=topic,
        angle=angle,
        note=note,
        score=score,
        score_reasons=reasons,
        source_metadata=source_metadata,
    )


def _recent_pull_request_rows(
    db,
    *,
    days: int,
    repo: str | None,
    now: datetime,
) -> list[dict[str, Any]]:
    cutoff = (now - timedelta(days=days)).isoformat()
    if hasattr(db, "conn") and hasattr(db, "_github_activity_from_row"):
        params: list[object] = [cutoff]
        repo_filter = ""
        if repo:
            repo_filter = " AND repo_name = ?"
            params.append(repo)
        rows = db.conn.execute(
            f"""SELECT * FROM github_activity
                WHERE activity_type = 'pull_request'
                  AND COALESCE(merged_at, updated_at) >= ?{repo_filter}
                ORDER BY COALESCE(merged_at, updated_at) DESC, id DESC""",
            tuple(params),
        ).fetchall()
        return [db._github_activity_from_row(row) for row in rows]

    getter = getattr(db, "get_recent_merged_github_pull_requests", None)
    if callable(getter):
        rows = getter(days=days, limit=None, now=now)
    else:
        rows = db.get_recent_github_activity(
            days=days,
            limit=100,
            now=now,
            activity_type="pull_request",
        )
    if repo:
        rows = [row for row in rows if row.get("repo_name") == repo]
    return rows


def build_pull_request_digest(
    db,
    *,
    days: int = DEFAULT_DAYS,
    repo: str | None = None,
    min_score: float = DEFAULT_MIN_SCORE,
    limit: int | None = None,
    now: datetime | None = None,
) -> list[PullRequestDigestItem]:
    """Return scored, high-signal pull request candidates from stored GitHub activity."""
    if days <= 0 or (limit is not None and limit <= 0):
        return []
    now = now or datetime.now(timezone.utc)
    rows = _recent_pull_request_rows(db, days=days, repo=repo, now=now)
    candidates: list[PullRequestDigestItem] = []
    for row in rows:
        item = pull_request_to_digest_item(row, now=now)
        if not _is_merged(row) and item.score < min_score + 12:
            continue
        if item.score >= min_score:
            candidates.append(item)

    candidates.sort(key=lambda item: (-item.score, item.repo_name, item.number))
    return candidates[:limit] if limit is not None else candidates


def seed_pull_request_ideas(
    db,
    *,
    days: int = DEFAULT_DAYS,
    repo: str | None = None,
    min_score: float = DEFAULT_MIN_SCORE,
    limit: int | None = 10,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[PullRequestSeedResult]:
    """Create content ideas from scored pull request digest items."""
    items = build_pull_request_digest(
        db,
        days=days,
        repo=repo,
        min_score=min_score,
        limit=limit,
        now=now,
    )
    results: list[PullRequestSeedResult] = []
    find_existing = getattr(db, "find_active_content_idea_for_source_metadata", None)
    add_idea = getattr(db, "add_content_idea", None)
    if not callable(add_idea):
        return results

    for item in items:
        existing = None
        if callable(find_existing):
            existing = find_existing(
                note=item.note,
                topic=item.topic,
                source=SOURCE_NAME,
                source_metadata=item.source_metadata,
            )
        if existing:
            results.append(
                PullRequestSeedResult(
                    status="skipped",
                    repo_name=item.repo_name,
                    number=item.number,
                    topic=item.topic,
                    score=item.score,
                    idea_id=existing["id"],
                    reason=f"{existing['status']} duplicate",
                    note=item.note,
                )
            )
            continue

        if dry_run:
            results.append(
                PullRequestSeedResult(
                    status="proposed",
                    repo_name=item.repo_name,
                    number=item.number,
                    topic=item.topic,
                    score=item.score,
                    idea_id=None,
                    reason="dry run",
                    note=item.note,
                )
            )
            continue

        idea_id = add_idea(
            note=item.note,
            topic=item.topic,
            priority="normal",
            source=SOURCE_NAME,
            source_metadata=item.source_metadata,
        )
        results.append(
            PullRequestSeedResult(
                status="created",
                repo_name=item.repo_name,
                number=item.number,
                topic=item.topic,
                score=item.score,
                idea_id=idea_id,
                reason="created",
                note=item.note,
            )
        )

    return results


def format_pull_request_digest_json(
    items: list[PullRequestDigestItem],
    seed_results: list[PullRequestSeedResult] | None = None,
) -> str:
    payload: dict[str, Any] = {"pull_requests": [item.to_dict() for item in items]}
    if seed_results is not None:
        payload["seed_results"] = [result.to_dict() for result in seed_results]
    return json.dumps(payload, indent=2, sort_keys=True)
