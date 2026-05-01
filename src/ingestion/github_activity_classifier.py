"""Deterministically classify GitHub activity impact for synthesis source selection."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_IMPACT = 0

CATEGORY_USER_FACING_FEATURE = "user-facing feature"
CATEGORY_BUG_FIX = "bug fix"
CATEGORY_MAINTENANCE = "maintenance"
CATEGORY_DOCUMENTATION = "documentation"
CATEGORY_RELEASE = "release"
CATEGORY_QUESTION = "question"
CATEGORY_LOW_SIGNAL = "low-signal"

CATEGORIES = {
    CATEGORY_USER_FACING_FEATURE,
    CATEGORY_BUG_FIX,
    CATEGORY_MAINTENANCE,
    CATEGORY_DOCUMENTATION,
    CATEGORY_RELEASE,
    CATEGORY_QUESTION,
    CATEGORY_LOW_SIGNAL,
}

SYNTHESIS_WORTHY_CATEGORIES = {
    CATEGORY_USER_FACING_FEATURE,
    CATEGORY_BUG_FIX,
    CATEGORY_DOCUMENTATION,
    CATEGORY_RELEASE,
    CATEGORY_QUESTION,
}

_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_-]*")
_QUESTION_RE = re.compile(r"\?|(?:\b(?:how|why|what|where|when|can|could|should|would|is|are)\b.{0,80}\?)", re.I)

_LABEL_SIGNALS: dict[str, tuple[str, int, str]] = {
    "enhancement": (CATEGORY_USER_FACING_FEATURE, 34, "label:enhancement"),
    "feature": (CATEGORY_USER_FACING_FEATURE, 34, "label:feature"),
    "new feature": (CATEGORY_USER_FACING_FEATURE, 34, "label:new feature"),
    "bug": (CATEGORY_BUG_FIX, 36, "label:bug"),
    "fix": (CATEGORY_BUG_FIX, 28, "label:fix"),
    "regression": (CATEGORY_BUG_FIX, 34, "label:regression"),
    "docs": (CATEGORY_DOCUMENTATION, 36, "label:docs"),
    "documentation": (CATEGORY_DOCUMENTATION, 36, "label:documentation"),
    "release": (CATEGORY_RELEASE, 38, "label:release"),
    "question": (CATEGORY_QUESTION, 38, "label:question"),
    "q&a": (CATEGORY_QUESTION, 38, "label:q&a"),
    "support": (CATEGORY_QUESTION, 26, "label:support"),
    "help wanted": (CATEGORY_QUESTION, 22, "label:help wanted"),
    "maintenance": (CATEGORY_MAINTENANCE, 30, "label:maintenance"),
    "chore": (CATEGORY_MAINTENANCE, 28, "label:chore"),
    "refactor": (CATEGORY_MAINTENANCE, 28, "label:refactor"),
    "dependencies": (CATEGORY_MAINTENANCE, 26, "label:dependencies"),
    "ci": (CATEGORY_MAINTENANCE, 24, "label:ci"),
}

_TERM_SIGNALS: tuple[tuple[str, str, int, str], ...] = (
    (CATEGORY_USER_FACING_FEATURE, r"\b(?:add|adds|added|new|support|enable|launch|implement|introduce|allow)\b", 18, "feature terms"),
    (CATEGORY_USER_FACING_FEATURE, r"\b(?:ui|ux|user|customer|onboarding|dashboard|checkout|settings|export|import)\b", 12, "user-facing terms"),
    (CATEGORY_BUG_FIX, r"\b(?:fix|fixed|fixes|bug|resolve|resolved|regression|crash|broken|error|failure|race condition)\b", 22, "bug-fix terms"),
    (CATEGORY_DOCUMENTATION, r"\b(?:docs|documentation|readme|guide|tutorial|example|changelog|copy|typo)\b", 24, "documentation terms"),
    (CATEGORY_RELEASE, r"\b(?:release|ship|shipped|version|v\d+\.\d+|tag|publish|published)\b", 24, "release terms"),
    (CATEGORY_QUESTION, r"\b(?:question|help|support|how to|what is|why does|should we|can we|discussion)\b", 20, "question terms"),
    (CATEGORY_MAINTENANCE, r"\b(?:chore|refactor|cleanup|ci|test|tests|deps|dependency|dependencies|build|lint|format|rename|migration)\b", 18, "maintenance terms"),
)

_ADMIN_TERMS = {
    "assigned",
    "unassigned",
    "labeled",
    "unlabeled",
    "milestoned",
    "demilestoned",
    "locked",
    "unlocked",
    "transferred",
    "subscribed",
    "unsubscribed",
    "mentioned",
}

_LOW_SIGNAL_LABELS = {
    "duplicate",
    "invalid",
    "wontfix",
    "stale",
    "no-op",
    "skip-changelog",
}


@dataclass(frozen=True)
class GitHubActivityClassification:
    """One classified GitHub activity row."""

    id: int | None
    activity_id: str
    repo_name: str | None
    activity_type: str | None
    number: int | str | None
    title: str
    state: str | None
    url: str | None
    updated_at: str | None
    category: str
    confidence: float
    impact_score: int
    synthesis_worthy: bool
    rationale: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def classify_github_activity_row(row: Any) -> GitHubActivityClassification:
    """Classify one github_activity-like row using deterministic weighted signals."""
    item = _row_dict(row)
    labels = _parse_json_list(item.get("labels"))
    metadata = _parse_json_object(item.get("metadata"))
    activity_type = _clean(item.get("activity_type")).lower()
    state = _clean(item.get("state")).lower()
    title = _clean(item.get("title"))
    body = _clean(item.get("body"))
    text = f"{title} {body}".strip()

    scores = {category: 0 for category in CATEGORIES if category != CATEGORY_LOW_SIGNAL}
    rationale: list[str] = []

    _apply_activity_type_signal(activity_type, scores, rationale)
    _apply_label_signals(labels, scores, rationale)
    _apply_text_signals(text, scores, rationale)
    _apply_state_metadata_signals(activity_type, state, item, metadata, scores, rationale)

    low_signal_reasons = _low_signal_reasons(activity_type, state, title, body, labels, metadata)
    if _should_force_low_signal(scores, low_signal_reasons):
        category = CATEGORY_LOW_SIGNAL
    else:
        category = max(scores, key=lambda key: (scores[key], _category_rank(key)))

    impact_score = _impact_score(category, scores, activity_type, state, labels, metadata, item, low_signal_reasons)
    confidence = _confidence(category, scores, low_signal_reasons)
    if category == CATEGORY_LOW_SIGNAL:
        rationale.extend(low_signal_reasons or ["low-signal activity without synthesis-worthy evidence"])
    else:
        rationale.extend(low_signal_reasons)

    rationale = _unique(rationale)[:8]
    activity_id = _activity_id(item)
    return GitHubActivityClassification(
        id=_int_or_none(item.get("id")),
        activity_id=activity_id,
        repo_name=item.get("repo_name"),
        activity_type=item.get("activity_type"),
        number=item.get("number"),
        title=title,
        state=item.get("state"),
        url=item.get("url"),
        updated_at=item.get("updated_at"),
        category=category,
        confidence=confidence,
        impact_score=impact_score,
        synthesis_worthy=category in SYNTHESIS_WORTHY_CATEGORIES and impact_score >= 50,
        rationale=rationale,
    )


def classify_recent_github_activity(
    db_or_conn: Any,
    *,
    days: int | None = DEFAULT_DAYS,
    repo: str | None = None,
    min_impact: int = DEFAULT_MIN_IMPACT,
    now: datetime | None = None,
) -> list[GitHubActivityClassification]:
    """Return classified recent GitHub activity rows."""
    if days is not None and days <= 0:
        raise ValueError("days must be positive")
    if repo is not None and not repo.strip():
        raise ValueError("repo must not be blank")
    if min_impact < 0 or min_impact > 100:
        raise ValueError("min_impact must be between 0 and 100")

    conn = _connection(db_or_conn)
    if not _table_exists(conn, "github_activity"):
        return []

    where: list[str] = []
    params: list[Any] = []
    if days is not None:
        reference_time = _as_utc(now or datetime.now(timezone.utc))
        cutoff = (reference_time - timedelta(days=days)).isoformat()
        where.append("updated_at >= ?")
        params.append(cutoff)
    if repo:
        where.append("repo_name = ?")
        params.append(repo)

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""SELECT *
            FROM github_activity
            {where_sql}
            ORDER BY updated_at DESC, id DESC""",
        tuple(params),
    ).fetchall()
    classifications = [classify_github_activity_row(row) for row in rows]
    return [item for item in classifications if item.impact_score >= min_impact]


def build_github_activity_classification_report(
    db_or_conn: Any,
    *,
    days: int | None = DEFAULT_DAYS,
    repo: str | None = None,
    min_impact: int = DEFAULT_MIN_IMPACT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a structured report of recent classified GitHub activity."""
    generated_at = _as_utc(now or datetime.now(timezone.utc)).isoformat()
    classifications = classify_recent_github_activity(
        db_or_conn,
        days=days,
        repo=repo,
        min_impact=min_impact,
        now=now,
    )
    items = [item.to_dict() for item in classifications]
    high_impact = [item for item in items if item["synthesis_worthy"] and item["impact_score"] >= max(70, min_impact)]
    return {
        "artifact_type": "github_activity_classification",
        "generated_at": generated_at,
        "filters": {
            "days": days,
            "repo": repo,
            "min_impact": min_impact,
        },
        "counts": {
            "items": len(items),
            "high_impact": len(high_impact),
            "by_category": _count_by_category(items),
        },
        "items": items,
        "high_impact": high_impact,
    }


def format_github_activity_classification_json(report: dict[str, Any]) -> str:
    """Format a GitHub activity classification report as JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_github_activity_classification_text(report: dict[str, Any]) -> str:
    """Format a GitHub activity classification report for humans."""
    if not report["items"]:
        return "No GitHub activity matched the filters."

    lines = [
        "GitHub Activity Classification",
        (
            f"Counts: items={report['counts']['items']} "
            f"high_impact={report['counts']['high_impact']}"
        ),
        "",
        "By category:",
    ]
    for category, count in report["counts"]["by_category"].items():
        lines.append(f"  {category}: {count}")

    if report["high_impact"]:
        lines.extend(["", "High-impact synthesis candidates:"])
        for item in report["high_impact"]:
            lines.append(_format_item(item))

    lines.extend(["", "Items:"])
    for item in report["items"]:
        lines.append(_format_item(item))
        lines.append(f"    rationale: {', '.join(item['rationale'])}")
    return "\n".join(lines)


def _apply_activity_type_signal(activity_type: str, scores: dict[str, int], rationale: list[str]) -> None:
    if activity_type == "release":
        scores[CATEGORY_RELEASE] += 50
        rationale.append("activity_type:release")
    elif activity_type == "discussion":
        scores[CATEGORY_QUESTION] += 24
        rationale.append("activity_type:discussion")
    elif activity_type == "pull_request":
        scores[CATEGORY_MAINTENANCE] += 8
        rationale.append("activity_type:pull_request")
    elif activity_type == "issue":
        scores[CATEGORY_QUESTION] += 8
        rationale.append("activity_type:issue")
    elif activity_type == "workflow_run":
        scores[CATEGORY_MAINTENANCE] += 18
        rationale.append("activity_type:workflow_run")
    elif activity_type in {"issue_comment", "review_comment", "discussion_comment"}:
        scores[CATEGORY_QUESTION] += 10
        rationale.append(f"activity_type:{activity_type}")


def _apply_label_signals(labels: list[Any], scores: dict[str, int], rationale: list[str]) -> None:
    for label in labels:
        normalized = _normalize_label(label)
        if normalized in _LABEL_SIGNALS:
            category, points, reason = _LABEL_SIGNALS[normalized]
            scores[category] += points
            rationale.append(reason)


def _apply_text_signals(text: str, scores: dict[str, int], rationale: list[str]) -> None:
    if not text:
        return
    for category, pattern, points, reason in _TERM_SIGNALS:
        if re.search(pattern, text, re.I):
            scores[category] += points
            rationale.append(reason)
    if _QUESTION_RE.search(text):
        scores[CATEGORY_QUESTION] += 16
        rationale.append("question punctuation")


def _apply_state_metadata_signals(
    activity_type: str,
    state: str,
    row: dict[str, Any],
    metadata: dict[str, Any],
    scores: dict[str, int],
    rationale: list[str],
) -> None:
    if activity_type == "pull_request":
        if row.get("merged_at") or metadata.get("merged") is True:
            scores[CATEGORY_USER_FACING_FEATURE] += 8
            scores[CATEGORY_BUG_FIX] += 8
            rationale.append("merged pull request")
        if metadata.get("draft") is True:
            scores[CATEGORY_MAINTENANCE] += 10
            rationale.append("draft pull request")
    if activity_type == "issue" and state == "closed":
        scores[CATEGORY_BUG_FIX] += 8
        rationale.append("closed issue")
    if activity_type == "discussion":
        category = metadata.get("category") if isinstance(metadata.get("category"), dict) else {}
        if _clean(category.get("name")).lower() in {"q&a", "questions", "question"}:
            scores[CATEGORY_QUESTION] += 16
            rationale.append("discussion category:q&a")
        if metadata.get("answer_state") == "answered":
            scores[CATEGORY_QUESTION] += 8
            rationale.append("answered discussion")
    if activity_type == "workflow_run":
        conclusion = _clean(metadata.get("conclusion") or state).lower()
        if conclusion in {"failure", "cancelled", "timed_out", "action_required"}:
            scores[CATEGORY_MAINTENANCE] += 18
            rationale.append(f"workflow conclusion:{conclusion}")
    if metadata.get("prerelease") is True:
        scores[CATEGORY_RELEASE] += 8
        rationale.append("metadata:prerelease")
    if _numeric(metadata.get("changed_files")) >= 5:
        scores[CATEGORY_MAINTENANCE] += 6
        rationale.append("metadata:changed_files")
    if _numeric(metadata.get("additions")) + _numeric(metadata.get("deletions")) >= 100:
        scores[CATEGORY_MAINTENANCE] += 6
        rationale.append("metadata:large_change")
    if _numeric(metadata.get("comments_count")) >= 3:
        scores[CATEGORY_QUESTION] += 6
        rationale.append("metadata:discussion")


def _low_signal_reasons(
    activity_type: str,
    state: str,
    title: str,
    body: str,
    labels: list[Any],
    metadata: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    normalized_labels = {_normalize_label(label) for label in labels}
    if normalized_labels & _LOW_SIGNAL_LABELS:
        reasons.append("low-signal label")
    issue_event_type = _clean(metadata.get("issue_event_type")).lower()
    if issue_event_type in _ADMIN_TERMS:
        reasons.append(f"administrative issue event:{issue_event_type}")
    if activity_type == "workflow_run" and _clean(metadata.get("conclusion") or state).lower() == "success":
        reasons.append("successful workflow run")
    text = f"{title} {body}".lower()
    if any(term in text for term in ("bump ", "dependabot", "merge branch", "merge remote-tracking", "no-op", "typo only")):
        reasons.append("administrative title/body")
    if activity_type in {"issue_comment", "review_comment", "discussion_comment"} and not _QUESTION_RE.search(text):
        reasons.append("comment without direct question")
    return reasons


def _should_force_low_signal(scores: dict[str, int], low_signal_reasons: list[str]) -> bool:
    best = max(scores.values()) if scores else 0
    if best < 24:
        return True
    if low_signal_reasons and best < 42:
        return True
    return False


def _impact_score(
    category: str,
    scores: dict[str, int],
    activity_type: str,
    state: str,
    labels: list[Any],
    metadata: dict[str, Any],
    row: dict[str, Any],
    low_signal_reasons: list[str],
) -> int:
    if category == CATEGORY_LOW_SIGNAL:
        return min(35, max(5, max(scores.values(), default=0)))

    score = 25 + min(45, scores.get(category, 0))
    if activity_type == "release":
        score += 18
    if activity_type == "pull_request" and (row.get("merged_at") or metadata.get("merged") is True):
        score += 14
    if activity_type == "issue" and state == "closed":
        score += 8
    if metadata.get("answer_state") == "answered":
        score += 6
    if _numeric(metadata.get("changed_files")) >= 5:
        score += 5
    if _numeric(metadata.get("additions")) + _numeric(metadata.get("deletions")) >= 100:
        score += 5
    if any(_normalize_label(label) in {"breaking-change", "security", "customer-impact"} for label in labels):
        score += 10
    if low_signal_reasons:
        score -= 18
    return max(0, min(100, score))


def _confidence(category: str, scores: dict[str, int], low_signal_reasons: list[str]) -> float:
    if category == CATEGORY_LOW_SIGNAL:
        return 0.72 if low_signal_reasons else 0.58
    ordered = sorted(scores.values(), reverse=True)
    best = ordered[0] if ordered else 0
    second = ordered[1] if len(ordered) > 1 else 0
    value = 0.48 + min(0.34, best / 160) + min(0.14, max(0, best - second) / 140)
    if low_signal_reasons:
        value -= 0.08
    return round(max(0.35, min(0.96, value)), 2)


def _format_item(item: dict[str, Any]) -> str:
    return (
        f"  - impact={item['impact_score']} confidence={item['confidence']:.2f} "
        f"category={item['category']} {item['repo_name']} "
        f"{item['activity_type']} #{item['number']} [{item['state'] or '-'}] "
        f"{item['updated_at']}: {item['title']} ({item['url'] or '-'})"
    )


def _row_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    return dict(row)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _parse_json_list(value: Any) -> list[Any]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _parse_json_object(value: Any) -> dict[str, Any]:
    if value is None or value == "":
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _activity_id(row: dict[str, Any]) -> str:
    if row.get("activity_id"):
        return str(row["activity_id"])
    return f"{row.get('repo_name')}#{row.get('number')}:{row.get('activity_type')}"


def _normalize_label(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().replace("_", " ").split())


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _numeric(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _count_by_category(items: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        category = str(item["category"])
        counts[category] = counts.get(category, 0) + 1
    return dict(sorted(counts.items()))


def _category_rank(category: str) -> int:
    order = [
        CATEGORY_RELEASE,
        CATEGORY_BUG_FIX,
        CATEGORY_USER_FACING_FEATURE,
        CATEGORY_DOCUMENTATION,
        CATEGORY_QUESTION,
        CATEGORY_MAINTENANCE,
    ]
    return len(order) - order.index(category) if category in order else 0


def _unique(values: list[str]) -> list[str]:
    seen = set()
    result = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result
