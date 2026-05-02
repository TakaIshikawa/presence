"""Seed content ideas from unresolved GitHub Dependabot alert activity."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


SOURCE_NAME = "github_dependabot_alert"
ACTIVITY_TYPE = "dependabot_alert"
DEFAULT_DAYS = 14
DEFAULT_LIMIT = 10
SEVERITY_ORDER = {
    "low": 0,
    "medium": 1,
    "high": 2,
    "critical": 3,
}


@dataclass(frozen=True)
class DependabotAlertIdeaCandidate:
    repo_name: str
    package: str
    ecosystem: str
    severity: str
    advisory: str
    alert_count: int
    activity_ids: list[str]
    alert_identifiers: list[str]
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DependabotAlertIdeaSeedResult:
    status: str
    repo_name: str
    package: str
    ecosystem: str
    severity: str
    advisory: str
    alert_count: int
    idea_id: int | None
    reason: str
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def seed_dependabot_alert_ideas(
    db,
    *,
    days: int = DEFAULT_DAYS,
    min_severity: str = "medium",
    limit: int | None = DEFAULT_LIMIT,
    dry_run: bool = False,
    include_resolved: bool = False,
    now: datetime | None = None,
) -> list[DependabotAlertIdeaSeedResult]:
    """Create content ideas from recent Dependabot alert clusters."""

    if days <= 0 or (limit is not None and limit <= 0):
        return []
    min_rank = _severity_rank(min_severity)
    now = now or datetime.now(timezone.utc)

    rows = _recent_dependabot_alert_rows(db, days=days, now=now)
    candidates: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    results: list[DependabotAlertIdeaSeedResult] = []

    for row in rows:
        skip_reason = _skip_reason(row, min_rank=min_rank, include_resolved=include_resolved)
        if skip_reason:
            results.append(_skipped_row(row, skip_reason))
            continue
        candidates.setdefault(_cluster_key(row), []).append(row)

    for grouped_rows in _ordered_groups(candidates.values())[: limit or None]:
        candidate = dependabot_alert_rows_to_candidate(grouped_rows)
        existing = db.find_active_content_idea_for_source_metadata(
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        if existing:
            results.append(_result(candidate, "skipped", existing["id"], f"{existing['status']} duplicate"))
            continue

        if dry_run:
            results.append(_result(candidate, "proposed", None, "dry run"))
            continue

        idea_id = db.add_content_idea(
            note=candidate.note,
            topic=candidate.topic,
            priority=candidate.priority,
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        results.append(_result(candidate, "created", idea_id, "created"))

    return _ordered_results(results)


def dependabot_alert_rows_to_candidate(rows: list[dict[str, Any]]) -> DependabotAlertIdeaCandidate:
    if not rows:
        raise ValueError("at least one alert row is required")

    rows = sorted(
        rows,
        key=lambda row: (
            -_severity_rank(_metadata(row).get("severity"), default=0),
            _reverse_sort_text(str(row.get("updated_at") or "")),
            _reverse_sort_text(str(row.get("id") or "")),
        ),
    )
    first = rows[0]
    metadata = _metadata(first)
    repo_name = str(first.get("repo_name") or "")
    package = _normalize_text(metadata.get("package")) or "dependency"
    ecosystem = _normalize_text(metadata.get("ecosystem"))
    severity = _normalize_severity(metadata.get("severity") or _label_severity(first.get("labels")))
    advisory = _advisory_identifier(metadata, first)
    cluster_id = _cluster_id(_cluster_key(first))
    activity_ids = sorted({_activity_id(row) for row in rows if _activity_id(row)})
    alert_identifiers = sorted({_alert_identifier(row) for row in rows if _alert_identifier(row)})
    manifest_paths = sorted(
        {
            _normalize_text(_metadata(row).get("manifest_path"))
            for row in rows
            if _normalize_text(_metadata(row).get("manifest_path"))
        }
    )
    urls = sorted(
        {
            _normalize_text(row.get("url") or _metadata(row).get("html_url"))
            for row in rows
            if _normalize_text(row.get("url") or _metadata(row).get("html_url"))
        }
    )
    summaries = [_normalize_text(_metadata(row).get("advisory_summary") or row.get("body")) for row in rows]
    summary = next((item for item in summaries if item), "")
    patched_versions = sorted(
        {
            _normalize_text(_metadata(row).get("patched_versions"))
            for row in rows
            if _normalize_text(_metadata(row).get("patched_versions"))
        }
    )
    priority = _priority_for_severity(severity)
    topic = "security"
    note = (
        f"{repo_name} has {len(rows)} unresolved Dependabot alert"
        f"{'' if len(rows) == 1 else 's'} for {package}"
        f"{f' ({ecosystem})' if ecosystem else ''} at {severity or 'unknown'} severity. "
        f"Advisory: {advisory or 'not captured'}. "
        f"Manifest paths: {', '.join(manifest_paths) if manifest_paths else 'not captured'}. "
        f"Patched versions: {', '.join(patched_versions) if patched_versions else 'not captured'}. "
        f"Alert: {urls[0] if urls else 'stored GitHub activity only'}. "
        f"Summary: {summary or 'No advisory summary captured.'} "
        "Suggested angle: write a practical security-maintenance review of the fix, "
        "the upgrade risk, and the verification checklist."
    )
    source_metadata = {
        "source": SOURCE_NAME,
        "alert_cluster_id": cluster_id,
        "repo_name": repo_name,
        "package": package,
        "ecosystem": ecosystem,
        "severity": severity,
        "advisory": advisory,
        "activity_ids": activity_ids,
        "github_activity_ids": sorted(
            str(row.get("id")) for row in rows if row.get("id") not in (None, "")
        ),
        "alert_identifiers": alert_identifiers,
        "manifest_paths": manifest_paths,
        "urls": urls,
        "patched_versions": patched_versions,
        "alert_count": len(rows),
    }
    return DependabotAlertIdeaCandidate(
        repo_name=repo_name,
        package=package,
        ecosystem=ecosystem,
        severity=severity,
        advisory=advisory,
        alert_count=len(rows),
        activity_ids=activity_ids,
        alert_identifiers=alert_identifiers,
        topic=topic,
        note=note,
        priority=priority,
        source_metadata={key: value for key, value in source_metadata.items() if value not in (None, "", [])},
    )


def format_dependabot_alert_idea_results_json(results: list[DependabotAlertIdeaSeedResult]) -> str:
    return json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True)


def format_dependabot_alert_idea_results_text(results: list[DependabotAlertIdeaSeedResult]) -> str:
    created = sum(1 for result in results if result.status == "created")
    proposed = sum(1 for result in results if result.status == "proposed")
    skipped = sum(1 for result in results if result.status == "skipped")
    lines = [f"created={created} proposed={proposed} skipped={skipped}"]
    lines.append(f"{'Status':9s}  {'ID':>4s}  {'Severity':8s}  {'Alerts':>6s}  Package / reason")
    lines.append(f"{'-' * 9:9s}  {'-' * 4:>4s}  {'-' * 8:8s}  {'-' * 6:>6s}  {'-' * 44}")
    if not results:
        lines.append("none       ----  --------       0  no eligible Dependabot alerts")
        return "\n".join(lines)

    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        package = result.package or "dependency"
        detail = f"{_shorten(result.repo_name, 16)} {package} {result.advisory}".strip()
        lines.append(
            f"{result.status:9s}  {idea_id:>4s}  {result.severity or '-':8s}  "
            f"{result.alert_count:6d}  {_shorten(detail, 44)} ({result.reason})"
        )
    return "\n".join(lines)


def _recent_dependabot_alert_rows(db, *, days: int, now: datetime) -> list[dict[str, Any]]:
    cutoff = (now - timedelta(days=days)).isoformat()
    cursor = db.conn.execute(
        """SELECT * FROM github_activity
           WHERE updated_at >= ?
             AND activity_type = ?
           ORDER BY updated_at DESC, id DESC""",
        (cutoff, ACTIVITY_TYPE),
    )
    return [db._github_activity_from_row(row) for row in cursor.fetchall()]


def _result(
    candidate: DependabotAlertIdeaCandidate,
    status: str,
    idea_id: int | None,
    reason: str,
) -> DependabotAlertIdeaSeedResult:
    return DependabotAlertIdeaSeedResult(
        status=status,
        repo_name=candidate.repo_name,
        package=candidate.package,
        ecosystem=candidate.ecosystem,
        severity=candidate.severity,
        advisory=candidate.advisory,
        alert_count=candidate.alert_count,
        idea_id=idea_id,
        reason=reason,
        topic=candidate.topic,
        note=candidate.note,
        priority=candidate.priority,
        source_metadata=candidate.source_metadata,
    )


def _skipped_row(row: dict[str, Any], reason: str) -> DependabotAlertIdeaSeedResult:
    metadata = _metadata(row)
    repo_name = str(row.get("repo_name") or "")
    severity = _normalize_severity(metadata.get("severity") or _label_severity(row.get("labels")))
    source_metadata = {
        "source": SOURCE_NAME,
        "activity_id": _activity_id(row),
        "repo_name": repo_name,
        "package": _normalize_text(metadata.get("package")),
        "ecosystem": _normalize_text(metadata.get("ecosystem")),
        "severity": severity,
        "advisory": _advisory_identifier(metadata, row),
        "alert_identifier": _alert_identifier(row),
    }
    return DependabotAlertIdeaSeedResult(
        status="skipped",
        repo_name=repo_name,
        package=_normalize_text(metadata.get("package")),
        ecosystem=_normalize_text(metadata.get("ecosystem")),
        severity=severity,
        advisory=_advisory_identifier(metadata, row),
        alert_count=1,
        idea_id=None,
        reason=reason,
        topic="security",
        note="",
        priority=_priority_for_severity(severity),
        source_metadata={key: value for key, value in source_metadata.items() if value not in (None, "")},
    )


def _skip_reason(row: dict[str, Any], *, min_rank: int, include_resolved: bool) -> str | None:
    metadata = _metadata(row)
    severity = _normalize_severity(metadata.get("severity") or _label_severity(row.get("labels")))
    if _severity_rank(severity, default=-1) < min_rank:
        return f"below {next(key for key, value in SEVERITY_ORDER.items() if value == min_rank)} severity"
    if include_resolved:
        return None
    state = _normalize_text(metadata.get("state") or row.get("state")).lower()
    if state in {"dismissed", "fixed", "closed", "resolved"}:
        return f"{state} alert"
    if metadata.get("dismissed_at") or (row.get("closed_at") and state == "dismissed"):
        return "dismissed alert"
    if metadata.get("fixed_at") or (row.get("closed_at") and state == "fixed"):
        return "fixed alert"
    return None


def _ordered_groups(groups: Any) -> list[list[dict[str, Any]]]:
    return sorted(
        (list(group) for group in groups),
        key=lambda rows: (
            -max(_severity_rank(_normalize_severity(_metadata(row).get("severity")), default=0) for row in rows),
            -len(rows),
            _reverse_sort_text(max(str(row.get("updated_at") or "") for row in rows)),
            _cluster_key(rows[0]),
        ),
    )


def _ordered_results(results: list[DependabotAlertIdeaSeedResult]) -> list[DependabotAlertIdeaSeedResult]:
    return sorted(
        results,
        key=lambda result: (
            -_severity_rank(result.severity, default=0),
            -result.alert_count,
            result.repo_name,
            result.package,
            result.advisory,
            result.status,
        ),
    )


def _reverse_sort_text(value: str) -> tuple[int, ...]:
    return tuple(-ord(char) for char in value)


def _cluster_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    metadata = _metadata(row)
    return (
        str(row.get("repo_name") or "").strip().lower(),
        _normalize_text(metadata.get("ecosystem")).lower(),
        _normalize_text(metadata.get("package")).lower(),
        _advisory_identifier(metadata, row).lower(),
    )


def _cluster_id(key: tuple[str, str, str, str]) -> str:
    return hashlib.sha256("|".join(key).encode("utf-8")).hexdigest()[:16]


def _metadata(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("metadata")
    return value if isinstance(value, dict) else {}


def _activity_id(row: dict[str, Any]) -> str:
    metadata = _metadata(row)
    return str(
        row.get("activity_id")
        or metadata.get("activity_id")
        or f"{row.get('repo_name', '')}#{row.get('number', '')}:{row.get('activity_type', '')}"
    )


def _alert_identifier(row: dict[str, Any]) -> str:
    metadata = _metadata(row)
    return str(
        metadata.get("external_id")
        or f"dependabot_alert:{row.get('repo_name', '')}:{metadata.get('alert_number') or row.get('number')}"
    )


def _advisory_identifier(metadata: dict[str, Any], row: dict[str, Any]) -> str:
    return _normalize_text(
        metadata.get("ghsa_id")
        or metadata.get("cve_id")
        or metadata.get("advisory_url")
        or metadata.get("advisory_summary")
        or row.get("title")
    )


def _normalize_text(value: object | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _shorten(text: str | None, width: int = 70) -> str:
    value = _normalize_text(text)
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _normalize_severity(value: object | None) -> str:
    severity = str(value or "").strip().lower()
    return severity if severity in SEVERITY_ORDER else ""


def _severity_rank(value: object | None, *, default: int | None = None) -> int:
    severity = _normalize_severity(value)
    if severity in SEVERITY_ORDER:
        return SEVERITY_ORDER[severity]
    if default is not None:
        return default
    allowed = ", ".join(SEVERITY_ORDER)
    raise ValueError(f"min_severity must be one of: {allowed}")


def _label_severity(labels: object | None) -> str:
    if not isinstance(labels, list):
        return ""
    for label in labels:
        severity = _normalize_severity(label)
        if severity:
            return severity
    return ""


def _priority_for_severity(severity: str) -> str:
    if severity in {"critical", "high"}:
        return "high"
    if severity == "low":
        return "low"
    return "normal"
