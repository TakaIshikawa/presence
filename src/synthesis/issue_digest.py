"""Summarize recent GitHub issue activity for review and idea seeding."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


SOURCE_NAME = "github_issue_digest"
BODY_EXCERPT_CHARS = 320
HIGH_SIGNAL_THRESHOLD = 38.0

HIGH_SIGNAL_LABELS = {
    "bug": 10,
    "customer": 16,
    "docs": 8,
    "enhancement": 10,
    "feature": 10,
    "incident": 18,
    "performance": 14,
    "priority": 14,
    "security": 18,
    "ux": 10,
}


@dataclass(frozen=True)
class IssueDigestItem:
    id: int | None
    activity_id: str
    repo_name: str
    number: int
    title: str
    state: str
    author: str
    url: str
    labels: list[str]
    updated_at: str
    created_at: str | None
    closed_at: str | None
    event_type: str
    score: float
    score_reasons: list[str]
    body_excerpt: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class IssueDigestGroup:
    repo_name: str
    label: str
    issues: list[IssueDigestItem]
    opened_count: int
    closed_count: int
    high_signal_count: int
    summary: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["issues"] = [issue.to_dict() for issue in self.issues]
        return payload


@dataclass(frozen=True)
class IssueDigest:
    generated_at: str
    days: int
    repo: str | None
    label: str | None
    groups: list[IssueDigestGroup]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["groups"] = [group.to_dict() for group in self.groups]
        return payload


@dataclass(frozen=True)
class IssueSeedResult:
    status: str
    repo_name: str
    number: int
    idea_id: int | None
    reason: str
    topic: str
    note: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _shorten(text: str | None, width: int = 70) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _body_excerpt(text: str | None) -> str:
    return _shorten(text, BODY_EXCERPT_CHARS)


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


def _fingerprint(parts: dict[str, Any]) -> str:
    material = json.dumps(parts, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def _event_type(row: dict) -> str:
    metadata = row.get("metadata") or {}
    value = str(metadata.get("issue_event_type") or "").strip().lower()
    if value:
        return value
    if str(row.get("state") or "").lower() == "closed" or row.get("closed_at"):
        return "closed"
    created_at = _parse_datetime(row.get("created_at_github"))
    updated_at = _parse_datetime(row.get("updated_at"))
    if created_at and updated_at and abs((updated_at - created_at).total_seconds()) < 60:
        return "opened"
    return "updated"


def _score_issue(row: dict) -> tuple[float, list[str]]:
    labels = [str(label).strip().lower() for label in row.get("labels") or []]
    metadata = row.get("metadata") or {}
    score = 0.0
    reasons: list[str] = []

    for label in labels:
        points = HIGH_SIGNAL_LABELS.get(label, 0)
        if points:
            score += points
            reasons.append(f"label:{label}+{points}")

    comment_count = int(metadata.get("comments_count") or metadata.get("comments") or 0)
    if comment_count >= 5:
        score += 16
        reasons.append("comments>=5+16")
    elif comment_count >= 2:
        score += 8
        reasons.append("comments>=2+8")

    event_type = _event_type(row)
    if event_type in {"commented", "reopened"}:
        score += 12
        reasons.append(f"event:{event_type}+12")
    elif event_type == "closed":
        score += 8
        reasons.append("event:closed+8")

    title = str(row.get("title") or "")
    body = str(row.get("body") or "")
    text = f"{title} {body}".lower()
    if len(title.split()) >= 5:
        score += 6
        reasons.append("specific-title+6")
    if len(body.split()) >= 20:
        score += 8
        reasons.append("body-detail+8")
    if re.search(r"\b(api|cli|customer|database|incident|pipeline|regression|workflow)\b", text):
        score += 8
        reasons.append("product-context+8")

    return round(score, 2), reasons


def issue_to_item(row: dict) -> IssueDigestItem:
    score, reasons = _score_issue(row)
    return IssueDigestItem(
        id=row.get("id"),
        activity_id=str(row.get("activity_id") or ""),
        repo_name=str(row.get("repo_name") or ""),
        number=int(row.get("number")),
        title=str(row.get("title") or ""),
        state=str(row.get("state") or ""),
        author=str(row.get("author") or ""),
        url=str(row.get("url") or ""),
        labels=sorted({str(label) for label in row.get("labels") or []}, key=str.lower),
        updated_at=str(row.get("updated_at") or ""),
        created_at=row.get("created_at_github"),
        closed_at=row.get("closed_at"),
        event_type=_event_type(row),
        score=score,
        score_reasons=reasons,
        body_excerpt=_body_excerpt(row.get("body")),
    )


def _group_summary(repo_name: str, label: str, issues: list[IssueDigestItem]) -> str:
    opened = sum(1 for issue in issues if issue.event_type == "opened")
    closed = sum(1 for issue in issues if issue.event_type == "closed" or issue.state.lower() == "closed")
    high = [issue for issue in issues if issue.score >= HIGH_SIGNAL_THRESHOLD]
    lead = high[0] if high else issues[0]
    return (
        f"{repo_name} / {label}: {len(issues)} recent issues, "
        f"{opened} opened, {closed} closed. Highest signal: #{lead.number} {lead.title}."
    )


def build_issue_digest(
    db,
    *,
    days: int = 7,
    repo: str | None = None,
    label: str | None = None,
    now: datetime | None = None,
    limit: int | None = None,
) -> IssueDigest:
    """Build a deterministic digest of recent GitHub issue activity."""
    if days <= 0:
        return IssueDigest(
            generated_at=(now or datetime.now(timezone.utc)).isoformat(),
            days=days,
            repo=repo,
            label=label,
            groups=[],
        )

    now = now or datetime.now(timezone.utc)
    rows = db.get_recent_github_issues(days=days, repo_name=repo, label=label, limit=limit, now=now)
    items = [issue_to_item(row) for row in rows]
    buckets: dict[tuple[str, str], list[IssueDigestItem]] = {}
    for item in items:
        labels = item.labels or ["unlabeled"]
        for item_label in labels:
            normalized_label = item_label or "unlabeled"
            buckets.setdefault((item.repo_name, normalized_label), []).append(item)

    groups: list[IssueDigestGroup] = []
    for (repo_name, group_label), group_items in sorted(
        buckets.items(),
        key=lambda entry: (entry[0][0].lower(), entry[0][1].lower()),
    ):
        ordered = sorted(
            group_items,
            key=lambda issue: (issue.updated_at, issue.repo_name, issue.number),
            reverse=True,
        )
        groups.append(
            IssueDigestGroup(
                repo_name=repo_name,
                label=group_label,
                issues=ordered,
                opened_count=sum(1 for issue in ordered if issue.event_type == "opened"),
                closed_count=sum(
                    1
                    for issue in ordered
                    if issue.event_type == "closed" or issue.state.lower() == "closed"
                ),
                high_signal_count=sum(1 for issue in ordered if issue.score >= HIGH_SIGNAL_THRESHOLD),
                summary=_group_summary(repo_name, group_label, ordered),
            )
        )

    return IssueDigest(
        generated_at=now.isoformat(),
        days=days,
        repo=repo,
        label=label,
        groups=groups,
    )


def _topic_for_issue(issue: IssueDigestItem) -> str:
    return f"{issue.repo_name}: {issue.title}".strip()


def _note_for_issue(issue: IssueDigestItem, group: IssueDigestGroup) -> str:
    labels = ", ".join(issue.labels) if issue.labels else "none"
    excerpt = issue.body_excerpt or "No issue body provided."
    return (
        f"GitHub issue #{issue.number} in {issue.repo_name}: {issue.title}. "
        f"State: {issue.state or 'unknown'}; event: {issue.event_type}; labels: {labels}. "
        f"URL: {issue.url or 'none'}. "
        f"Context: {excerpt} "
        f"Digest group: {group.label}. Suggested angle: explain the product or engineering "
        "tension behind this issue and the practical takeaway."
    )


def seed_issue_ideas(db, digest: IssueDigest, *, dry_run: bool = False) -> list[IssueSeedResult]:
    """Create content ideas for high-signal digest issues, skipping active duplicates."""
    results: list[IssueSeedResult] = []
    seen_activity_ids: set[str] = set()
    for group in digest.groups:
        for issue in group.issues:
            if issue.score < HIGH_SIGNAL_THRESHOLD or issue.activity_id in seen_activity_ids:
                continue
            seen_activity_ids.add(issue.activity_id)
            topic = _topic_for_issue(issue)
            note = _note_for_issue(issue, group)
            source_metadata = {
                "source": SOURCE_NAME,
                "activity_id": issue.activity_id,
                "github_activity_id": issue.id,
                "repo_name": issue.repo_name,
                "number": issue.number,
                "title": issue.title,
                "url": issue.url,
                "labels": issue.labels,
                "state": issue.state,
                "event_type": issue.event_type,
                "score": issue.score,
                "score_reasons": issue.score_reasons,
                "digest_label": group.label,
                "digest_fingerprint": _fingerprint(
                    {
                        "source": SOURCE_NAME,
                        "activity_id": issue.activity_id,
                        "score": issue.score,
                        "label": group.label,
                    }
                ),
            }

            existing = db.find_active_content_idea_for_source_metadata(
                note=note,
                topic=topic,
                source=SOURCE_NAME,
                source_metadata=source_metadata,
            )
            if existing:
                results.append(
                    IssueSeedResult(
                        status="skipped",
                        repo_name=issue.repo_name,
                        number=issue.number,
                        idea_id=existing["id"],
                        reason=f"{existing['status']} duplicate",
                        topic=topic,
                        note=note,
                    )
                )
                continue

            if dry_run:
                results.append(
                    IssueSeedResult(
                        status="proposed",
                        repo_name=issue.repo_name,
                        number=issue.number,
                        idea_id=None,
                        reason="dry run",
                        topic=topic,
                        note=note,
                    )
                )
                continue

            idea_id = db.add_content_idea(
                note=note,
                topic=topic,
                priority="normal",
                source=SOURCE_NAME,
                source_metadata=source_metadata,
            )
            results.append(
                IssueSeedResult(
                    status="created",
                    repo_name=issue.repo_name,
                    number=issue.number,
                    idea_id=idea_id,
                    reason="created",
                    topic=topic,
                    note=note,
                )
            )

    return results
