"""Export read-only remediation seeds from Dependabot alert activity."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import re
from typing import Any


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
class DependabotRemediationSeed:
    repo_name: str
    package: str
    ecosystem: str
    severity: str
    alert_count: int
    score: float | None
    fixed_version: str
    risk_summary: str
    suggested_angle: str
    alert_urls: tuple[str, ...]
    source_urls: tuple[str, ...]
    activity_ids: tuple[str, ...]
    alert_identifiers: tuple[str, ...]
    advisories: tuple[str, ...]
    latest_updated_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "repo_name": self.repo_name,
            "package": self.package,
            "ecosystem": self.ecosystem,
            "severity": self.severity,
            "alert_count": self.alert_count,
            "score": self.score,
            "fixed_version": self.fixed_version,
            "risk_summary": self.risk_summary,
            "suggested_angle": self.suggested_angle,
            "alert_urls": list(self.alert_urls),
            "source_urls": list(self.source_urls),
            "activity_ids": list(self.activity_ids),
            "alert_identifiers": list(self.alert_identifiers),
            "advisories": list(self.advisories),
            "latest_updated_at": self.latest_updated_at,
        }


@dataclass(frozen=True)
class DependabotRemediationSeedReport:
    generated_at: str
    window_start: str
    window_end: str
    days: int
    severity: str
    repo: str | None
    limit: int | None
    seeds: tuple[DependabotRemediationSeed, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "dependabot_remediation_seeds",
            "generated_at": self.generated_at,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "days": self.days,
            "severity": self.severity,
            "repo": self.repo,
            "limit": self.limit,
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns) for table, columns in (self.missing_columns or {}).items()
            },
            "seed_count": len(self.seeds),
            "seeds": [seed.to_dict() for seed in self.seeds],
        }


def build_dependabot_remediation_seed_report(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    severity: str = "medium",
    repo: str | None = None,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> DependabotRemediationSeedReport:
    """Return grouped remediation seed material from recent Dependabot alerts."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")
    min_rank = _severity_rank(severity)
    repo = _normalize_text(repo) or None
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
            severity=severity,
            repo=repo,
            limit=limit,
            missing_tables=("github_activity",),
        )

    required = ("activity_type", "repo_name", "updated_at", "metadata")
    missing = tuple(column for column in required if column not in schema["github_activity"])
    if missing:
        return _empty_report(
            generated_at=generated_at,
            window_start=window_start,
            window_end=window_end,
            days=days,
            severity=severity,
            repo=repo,
            limit=limit,
            missing_columns={"github_activity": missing},
        )

    rows = [
        row
        for row in _load_dependabot_rows(db, window_start=window_start, repo=repo)
        if _row_updated_at(row) is not None
        and window_start <= _row_updated_at(row) <= window_end
        and _severity_rank(_row_severity(row), default=-1) >= min_rank
    ]
    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(_group_key(row), []).append(row)

    seeds = tuple(_ordered_seeds(_rows_to_seed(group) for group in groups.values()))
    if limit is not None:
        seeds = seeds[:limit]

    return DependabotRemediationSeedReport(
        generated_at=generated_at.isoformat(),
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
        days=days,
        severity=severity,
        repo=repo,
        limit=limit,
        seeds=seeds,
    )


def format_dependabot_remediation_seed_json(report: DependabotRemediationSeedReport) -> str:
    """Format a remediation seed report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_dependabot_remediation_seed_markdown(report: DependabotRemediationSeedReport) -> str:
    """Format a remediation seed report as stable Markdown."""
    lines = [
        "# Dependabot Remediation Seeds",
        "",
        f"Window: {report.window_start} to {report.window_end}",
        f"Minimum severity: {report.severity}",
    ]
    if report.repo:
        lines.append(f"Repo: {report.repo}")
    if report.missing_tables:
        lines.extend(["", f"Missing tables: {', '.join(report.missing_tables)}"])
        return "\n".join(lines)
    if report.missing_columns:
        parts = [
            f"{table}: {', '.join(columns)}"
            for table, columns in sorted(report.missing_columns.items())
        ]
        lines.extend(["", f"Missing columns: {'; '.join(parts)}"])
        return "\n".join(lines)
    if not report.seeds:
        lines.extend(["", "No Dependabot remediation seeds found."])
        return "\n".join(lines)

    lines.extend(["", f"Seeds: {len(report.seeds)}"])
    for index, seed in enumerate(report.seeds, start=1):
        fixed = seed.fixed_version or "not captured"
        score = f"{seed.score:g}" if seed.score is not None else "not captured"
        alerts = ", ".join(seed.alert_urls) if seed.alert_urls else "stored GitHub activity only"
        advisories = ", ".join(seed.advisories) if seed.advisories else "not captured"
        lines.extend(
            [
                "",
                f"## {index}. {seed.repo_name}: {seed.package} ({seed.ecosystem or 'unknown'})",
                "",
                f"- Severity: {seed.severity or 'unknown'}",
                f"- Alerts: {seed.alert_count}",
                f"- Score: {score}",
                f"- Fixed version: {fixed}",
                f"- Advisories: {advisories}",
                f"- Risk summary: {seed.risk_summary}",
                f"- Suggested angle: {seed.suggested_angle}",
                f"- Alert URLs: {alerts}",
            ]
        )
    return "\n".join(lines)


def _empty_report(
    *,
    generated_at: datetime,
    window_start: datetime,
    window_end: datetime,
    days: int,
    severity: str,
    repo: str | None,
    limit: int | None,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> DependabotRemediationSeedReport:
    return DependabotRemediationSeedReport(
        generated_at=generated_at.isoformat(),
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
        days=days,
        severity=severity,
        repo=repo,
        limit=limit,
        seeds=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns or {},
    )


def _rows_to_seed(rows: list[dict[str, Any]]) -> DependabotRemediationSeed:
    rows = sorted(
        rows,
        key=lambda row: (
            _reverse_sort_text(str(row.get("updated_at") or "")),
            _reverse_sort_text(str(row.get("id") or "")),
        ),
    )
    first = rows[0]
    metadata = _metadata(first)
    repo_name = _normalize_text(first.get("repo_name"))
    package = _normalize_text(metadata.get("package")) or "dependency"
    ecosystem = _normalize_text(metadata.get("ecosystem"))
    severity = _row_severity(first)
    summaries = [
        _normalize_text(_metadata(row).get("advisory_summary") or _metadata(row).get("summary") or row.get("body"))
        for row in rows
    ]
    summary = next((item for item in summaries if item), "")
    advisories = tuple(sorted({_advisory_identifier(_metadata(row), row) for row in rows if _advisory_identifier(_metadata(row), row)}))
    fixed_versions = tuple(
        sorted(
            {
                _fixed_version_from_metadata(_metadata(row))
                for row in rows
                if _fixed_version_from_metadata(_metadata(row))
            }
        )
    )
    alert_urls = tuple(
        sorted(
            {
                _normalize_text(row.get("url") or _metadata(row).get("html_url") or _metadata(row).get("url"))
                for row in rows
                if _normalize_text(row.get("url") or _metadata(row).get("html_url") or _metadata(row).get("url"))
            }
        )
    )
    source_urls = tuple(
        sorted(
            {
                _normalize_text(_metadata(row).get("advisory_url"))
                for row in rows
                if _normalize_text(_metadata(row).get("advisory_url"))
            }
        )
    )
    scores = [_cvss_score(_metadata(row)) for row in rows]
    score = max((value for value in scores if value is not None), default=None)
    latest_updated_at = max(str(row.get("updated_at") or "") for row in rows)

    return DependabotRemediationSeed(
        repo_name=repo_name,
        package=package,
        ecosystem=ecosystem,
        severity=severity,
        alert_count=len(rows),
        score=score,
        fixed_version=", ".join(fixed_versions),
        risk_summary=_risk_summary(package, ecosystem, severity, summary, advisories, score),
        suggested_angle=_suggested_angle(package, ecosystem, severity, bool(fixed_versions), len(rows)),
        alert_urls=alert_urls,
        source_urls=source_urls,
        activity_ids=tuple(sorted({_activity_id(row) for row in rows if _activity_id(row)})),
        alert_identifiers=tuple(sorted({_alert_identifier(row) for row in rows if _alert_identifier(row)})),
        advisories=advisories,
        latest_updated_at=latest_updated_at,
    )


def _risk_summary(
    package: str,
    ecosystem: str,
    severity: str,
    summary: str,
    advisories: tuple[str, ...],
    score: float | None,
) -> str:
    subject = package or "dependency"
    if ecosystem:
        subject = f"{subject} ({ecosystem})"
    advisory = advisories[0] if advisories else "a Dependabot advisory"
    score_text = f" CVSS score {score:g}." if score is not None else ""
    detail = summary or f"{advisory} affects {subject}."
    return f"{severity or 'unknown'} severity alert for {subject}: {detail}{score_text}"


def _suggested_angle(
    package: str,
    ecosystem: str,
    severity: str,
    has_fixed_version: bool,
    alert_count: int,
) -> str:
    subject = package or "the affected dependency"
    if ecosystem:
        subject = f"{subject} in {ecosystem}"
    fix_clause = "fixed-version upgrade" if has_fixed_version else "remediation path"
    return (
        f"Show the {fix_clause} for {subject}, why the {severity or 'unknown'} risk matters, "
        f"and the verification checklist after updating {alert_count} alert"
        f"{'' if alert_count == 1 else 's'}."
    )


def _load_dependabot_rows(
    db: Any,
    *,
    window_start: datetime,
    repo: str | None,
) -> list[dict[str, Any]]:
    conn = getattr(db, "conn", db)
    params: list[Any] = [window_start.isoformat(), ACTIVITY_TYPE]
    where = ["updated_at >= ?", "activity_type = ?"]
    if repo:
        where.append("repo_name = ?")
        params.append(repo)
    cursor = conn.execute(
        f"""SELECT * FROM github_activity
            WHERE {' AND '.join(where)}
            ORDER BY updated_at DESC, id DESC""",
        params,
    )
    rows = cursor.fetchall()
    hydrate = getattr(db, "_github_activity_from_row", None)
    if callable(hydrate):
        return [hydrate(row) for row in rows]
    return [_row_to_dict(row) for row in rows]


def _schema(db: Any) -> dict[str, set[str]]:
    conn = getattr(db, "conn", db)
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    tables = [_row_value(row, "name", 0) for row in rows]
    schema: dict[str, set[str]] = {}
    for table in tables:
        schema[str(table)] = {
            str(_row_value(column, "name", 1))
            for column in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
    return schema


def _row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        data = dict(row)
    elif hasattr(row, "keys"):
        data = {key: row[key] for key in row.keys()}
    else:
        data = dict(row)
    data["metadata"] = _parse_metadata(data.get("metadata"))
    data["labels"] = _parse_labels(data.get("labels"))
    return data


def _parse_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_labels(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def _metadata(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("metadata")
    return value if isinstance(value, dict) else {}


def _row_updated_at(row: dict[str, Any]) -> datetime | None:
    value = str(row.get("updated_at") or "")
    if not value:
        return None
    try:
        return _ensure_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _group_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    metadata = _metadata(row)
    return (
        _normalize_text(row.get("repo_name")).lower(),
        _normalize_text(metadata.get("package")).lower(),
        _normalize_text(metadata.get("ecosystem")).lower(),
        _row_severity(row),
    )


def _row_severity(row: dict[str, Any]) -> str:
    metadata = _metadata(row)
    return _normalize_severity(metadata.get("severity") or _label_severity(row.get("labels")))


def _fixed_version_from_metadata(metadata: dict[str, Any]) -> str:
    fixed = _normalize_text(metadata.get("fixed_version") or metadata.get("fixed_versions"))
    if fixed:
        return fixed
    patched = _normalize_text(metadata.get("patched_versions"))
    return patched


def _cvss_score(metadata: dict[str, Any]) -> float | None:
    cvss = metadata.get("cvss")
    score: Any = None
    if isinstance(cvss, dict):
        score = cvss.get("score")
    elif cvss not in (None, ""):
        score = cvss
    if score in (None, ""):
        return None
    try:
        return float(score)
    except (TypeError, ValueError):
        return None


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


def _ordered_seeds(seeds: Any) -> list[DependabotRemediationSeed]:
    return sorted(
        seeds,
        key=lambda seed: (
            -_severity_rank(seed.severity, default=0),
            -seed.alert_count,
            _reverse_sort_text(seed.latest_updated_at),
            seed.repo_name,
            seed.package,
            seed.ecosystem,
        ),
    )


def _severity_rank(value: object | None, *, default: int | None = None) -> int:
    severity = _normalize_severity(value)
    if severity in SEVERITY_ORDER:
        return SEVERITY_ORDER[severity]
    if default is not None:
        return default
    allowed = ", ".join(SEVERITY_ORDER)
    raise ValueError(f"severity must be one of: {allowed}")


def _normalize_severity(value: object | None) -> str:
    severity = str(value or "").strip().lower()
    return severity if severity in SEVERITY_ORDER else ""


def _label_severity(labels: object | None) -> str:
    if not isinstance(labels, list):
        return ""
    for label in labels:
        severity = _normalize_severity(label)
        if severity:
            return severity
    return ""


def _normalize_text(value: object | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _reverse_sort_text(value: str) -> tuple[int, ...]:
    return tuple(-ord(char) for char in value)


def _row_value(row: Any, key: str, index: int) -> Any:
    if isinstance(row, dict):
        return row[key]
    if hasattr(row, "keys") and key in row.keys():
        return row[key]
    return row[index]


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
