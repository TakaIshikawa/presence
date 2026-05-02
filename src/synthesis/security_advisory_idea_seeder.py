"""Seed content ideas from GitHub security advisory activity."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
import sqlite3
from typing import Any


SOURCE_NAME = "github_security_advisory_seed"
ACTIVITY_TYPE = "security_advisory"
DEFAULT_DAYS = 30
DEFAULT_LIMIT = 10
SEVERITY_ORDER = {
    "low": 0,
    "medium": 1,
    "moderate": 1,
    "high": 2,
    "critical": 3,
}


@dataclass(frozen=True)
class SecurityAdvisoryIdeaCandidate:
    activity_id: str
    repo_name: str
    advisory_id: str
    title: str
    severity: str
    state: str
    updated_at: str
    advisory_url: str
    affected_packages: tuple[dict[str, Any], ...]
    cves: tuple[str, ...]
    ghsa_ids: tuple[str, ...]
    topic: str
    note: str
    priority: str
    advisory_fingerprint: str
    source_metadata: dict[str, Any]
    skip_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["affected_packages"] = [dict(item) for item in self.affected_packages]
        data["cves"] = list(self.cves)
        data["ghsa_ids"] = list(self.ghsa_ids)
        return data


@dataclass(frozen=True)
class SecurityAdvisoryIdeaSeedResult:
    status: str
    activity_id: str
    repo_name: str
    advisory_id: str
    severity: str
    state: str
    package_count: int
    idea_id: int | None
    reason: str
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SecurityAdvisoryIdeaSeedReport:
    generated_at: str
    filters: dict[str, Any]
    results: tuple[SecurityAdvisoryIdeaSeedResult, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def summary(self) -> dict[str, int]:
        return {
            "created": sum(1 for result in self.results if result.status == "created"),
            "dry_run": sum(1 for result in self.results if result.status == "dry-run"),
            "skipped": sum(1 for result in self.results if result.status == "skipped"),
            "total": len(self.results),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "security_advisory_idea_seed",
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "summary": self.summary,
            "results": [result.to_dict() for result in self.results],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_security_advisory_idea_candidates(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    repo: str | None = None,
    limit: int | None = DEFAULT_LIMIT,
    include_withdrawn: bool = False,
    now: datetime | None = None,
) -> tuple[SecurityAdvisoryIdeaCandidate, ...]:
    """Return ranked security advisory idea candidates without writing."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or _has_missing_required_columns(missing_columns):
        return ()

    rows = _load_security_advisory_rows(conn, cutoff=cutoff, now=generated_at, repo=repo)
    candidates = [
        _row_to_candidate(row, include_withdrawn=include_withdrawn)
        for row in rows
    ]
    candidates.sort(key=_candidate_sort_key)
    if limit is not None:
        candidates = candidates[:limit]
    return tuple(candidates)


def seed_security_advisory_ideas(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    repo: str | None = None,
    limit: int | None = DEFAULT_LIMIT,
    include_withdrawn: bool = False,
    dry_run: bool = True,
    now: datetime | None = None,
) -> SecurityAdvisoryIdeaSeedReport:
    """Preview or insert deduplicated content ideas for security advisories."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "cutoff": cutoff.isoformat(),
        "repo": _normalize_text(repo) or None,
        "limit": limit,
        "include_withdrawn": include_withdrawn,
        "dry_run": dry_run,
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or _has_missing_required_columns(missing_columns):
        return SecurityAdvisoryIdeaSeedReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            results=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    candidates = build_security_advisory_idea_candidates(
        db_or_conn,
        days=days,
        repo=repo,
        limit=limit,
        include_withdrawn=include_withdrawn,
        now=generated_at,
    )
    results: list[SecurityAdvisoryIdeaSeedResult] = []
    for candidate in candidates:
        if candidate.skip_reason:
            results.append(_result("skipped", candidate, None, candidate.skip_reason))
            continue

        existing = _existing_idea(conn, candidate)
        if existing:
            results.append(_result("skipped", candidate, existing.get("id"), "active duplicate"))
            continue

        if dry_run:
            results.append(_result("dry-run", candidate, None, "dry run"))
            continue

        idea_id = _insert_content_idea(db_or_conn, candidate)
        results.append(_result("created", candidate, idea_id, "created"))

    return SecurityAdvisoryIdeaSeedReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        results=tuple(results),
        missing_columns=missing_columns,
    )


def format_security_advisory_ideas_json(report: SecurityAdvisoryIdeaSeedReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_security_advisory_ideas_text(report: SecurityAdvisoryIdeaSeedReport) -> str:
    summary = report.summary
    lines = [
        (
            f"created={summary['created']} dry_run={summary['dry_run']} "
            f"skipped={summary['skipped']}"
        ),
        f"{'Status':9s}  {'ID':>4s}  {'Severity':8s}  {'Packages':>8s}  Advisory / reason",
        f"{'-' * 9:9s}  {'-' * 4:>4s}  {'-' * 8:8s}  {'-' * 8:>8s}  {'-' * 48}",
    ]
    if not report.results:
        lines.append("none       ----  --------         0  no eligible security advisories")
        return "\n".join(lines)
    for result in report.results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        detail = f"{result.repo_name} {result.advisory_id}".strip()
        lines.append(
            f"{result.status:9s}  {idea_id:>4s}  {result.severity or '-':8s}  "
            f"{result.package_count:8d}  {_shorten(detail, 48)} ({result.reason})"
        )
    return "\n".join(lines)


def _load_security_advisory_rows(
    conn: sqlite3.Connection,
    *,
    cutoff: datetime,
    now: datetime,
    repo: str | None,
) -> list[dict[str, Any]]:
    where = [
        "activity_type = ?",
        "datetime(updated_at) >= datetime(?)",
        "datetime(updated_at) <= datetime(?)",
    ]
    params: list[Any] = [ACTIVITY_TYPE, cutoff.isoformat(), now.isoformat()]
    normalized_repo = _normalize_text(repo)
    if normalized_repo:
        where.append("repo_name = ?")
        params.append(normalized_repo)

    cursor = conn.execute(
        f"""SELECT *
            FROM github_activity
            WHERE {' AND '.join(where)}
            ORDER BY updated_at DESC, id DESC""",
        params,
    )
    return [_row_dict(row) for row in cursor.fetchall()]


def _row_to_candidate(
    row: dict[str, Any],
    *,
    include_withdrawn: bool,
) -> SecurityAdvisoryIdeaCandidate:
    metadata = _metadata(row)
    repo_name = _normalize_text(row.get("repo_name"))
    severity = _normalize_severity(metadata.get("severity") or _label_severity(row.get("labels")))
    state = _normalize_text(metadata.get("state") or row.get("state")).lower()
    withdrawn_at = _normalize_text(metadata.get("withdrawn_at") or row.get("closed_at"))
    if withdrawn_at and state not in {"withdrawn", "closed"}:
        state = "withdrawn"
    advisory_id = _advisory_identifier(metadata, row)
    affected_packages = tuple(_affected_packages(metadata))
    cves = tuple(_string_list(metadata.get("cves") or metadata.get("cve_ids") or metadata.get("cve_id")))
    ghsa_ids = tuple(
        _string_list(metadata.get("ghsa_ids") or metadata.get("ghsa_id") or advisory_id)
    )
    advisory_url = _normalize_text(metadata.get("advisory_url") or metadata.get("html_url") or row.get("url"))
    activity_id = _activity_id(row)
    fingerprint = _advisory_fingerprint(
        repo_name=repo_name,
        advisory_id=advisory_id,
        cves=cves,
        ghsa_ids=ghsa_ids,
    )
    package_names = _package_names(affected_packages, metadata)
    topic = "security remediation"
    priority = _priority_for_severity(severity)
    note = _note(
        repo_name=repo_name,
        severity=severity,
        advisory_id=advisory_id,
        title=_normalize_text(row.get("title")),
        package_names=package_names,
        cves=cves,
        ghsa_ids=ghsa_ids,
        advisory_url=advisory_url,
        state=state,
    )
    source_metadata = {
        "source": SOURCE_NAME,
        "source_id": fingerprint,
        "advisory_fingerprint": fingerprint,
        "advisory_fingerprint_id": fingerprint,
        "activity_id": activity_id,
        "github_activity_id": row.get("id"),
        "repo_name": repo_name,
        "advisory_id": advisory_id,
        "severity": severity,
        "state": state,
        "cves": list(cves),
        "ghsa_ids": list(ghsa_ids),
        "affected_packages": list(affected_packages),
        "advisory_url": advisory_url,
        "updated_at": _normalize_text(row.get("updated_at")),
    }
    skip_reason = None
    if not include_withdrawn and _is_withdrawn(state=state, withdrawn_at=withdrawn_at):
        skip_reason = "withdrawn advisory"

    return SecurityAdvisoryIdeaCandidate(
        activity_id=activity_id,
        repo_name=repo_name,
        advisory_id=advisory_id,
        title=_normalize_text(row.get("title")),
        severity=severity,
        state=state,
        updated_at=_normalize_text(row.get("updated_at")),
        advisory_url=advisory_url,
        affected_packages=affected_packages,
        cves=cves,
        ghsa_ids=ghsa_ids,
        topic=topic,
        note=note,
        priority=priority,
        advisory_fingerprint=fingerprint,
        source_metadata={key: value for key, value in source_metadata.items() if value not in (None, "", [])},
        skip_reason=skip_reason,
    )


def _result(
    status: str,
    candidate: SecurityAdvisoryIdeaCandidate,
    idea_id: int | None,
    reason: str,
) -> SecurityAdvisoryIdeaSeedResult:
    return SecurityAdvisoryIdeaSeedResult(
        status=status,
        activity_id=candidate.activity_id,
        repo_name=candidate.repo_name,
        advisory_id=candidate.advisory_id,
        severity=candidate.severity,
        state=candidate.state,
        package_count=len(candidate.affected_packages),
        idea_id=idea_id,
        reason=reason,
        topic=candidate.topic,
        note=candidate.note,
        priority=candidate.priority,
        source_metadata=candidate.source_metadata,
    )


def _insert_content_idea(db_or_conn: Any, candidate: SecurityAdvisoryIdeaCandidate) -> int:
    add = getattr(db_or_conn, "add_content_idea", None)
    if callable(add):
        return int(
            add(
                note=candidate.note,
                topic=candidate.topic,
                priority=candidate.priority,
                source=SOURCE_NAME,
                source_metadata=candidate.source_metadata,
            )
        )
    conn = _connection(db_or_conn)
    cursor = conn.execute(
        """INSERT INTO content_ideas
           (note, topic, priority, status, source, source_metadata)
           VALUES (?, ?, ?, 'open', ?, ?)""",
        (
            candidate.note,
            candidate.topic,
            candidate.priority,
            SOURCE_NAME,
            json.dumps(candidate.source_metadata, sort_keys=True),
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def _existing_idea(conn: sqlite3.Connection, candidate: SecurityAdvisoryIdeaCandidate) -> dict[str, Any] | None:
    if "content_ideas" not in _schema(conn):
        return None
    cursor = conn.execute(
        """SELECT *
           FROM content_ideas
           WHERE status IN ('open', 'promoted')
             AND source_metadata IS NOT NULL
           ORDER BY created_at ASC, id ASC"""
    )
    expected = candidate.advisory_fingerprint
    for row in cursor.fetchall():
        item = _row_dict(row)
        metadata = _decode_json_object(item.get("source_metadata"))
        if metadata.get("source") != SOURCE_NAME:
            continue
        if (
            metadata.get("advisory_fingerprint") == expected
            or metadata.get("advisory_fingerprint_id") == expected
            or metadata.get("source_id") == expected
        ):
            return item
    return None


def _candidate_sort_key(candidate: SecurityAdvisoryIdeaCandidate) -> tuple[Any, ...]:
    return (
        -_severity_rank(candidate.severity, default=-1),
        -_package_specificity(candidate),
        _reverse_sort_text(candidate.updated_at),
        candidate.repo_name.lower(),
        candidate.advisory_id.lower(),
    )


def _package_specificity(candidate: SecurityAdvisoryIdeaCandidate) -> int:
    score = 0
    for package in candidate.affected_packages:
        if _normalize_text(package.get("name")):
            score += 3
        if _normalize_text(package.get("ecosystem")):
            score += 2
        if _normalize_text(package.get("patched_versions")):
            score += 1
        if _normalize_text(package.get("vulnerable_version_range")):
            score += 1
    return score


def _note(
    *,
    repo_name: str,
    severity: str,
    advisory_id: str,
    title: str,
    package_names: tuple[str, ...],
    cves: tuple[str, ...],
    ghsa_ids: tuple[str, ...],
    advisory_url: str,
    state: str,
) -> str:
    packages = ", ".join(package_names) if package_names else "affected package not captured"
    identifiers = ", ".join((*ghsa_ids, *cves)) or advisory_id
    return (
        f"{repo_name} has a {severity or 'unknown'} severity security advisory for {packages}. "
        f"Advisory: {identifiers}. State: {state or 'unknown'}. "
        f"Title: {title or advisory_id}. "
        f"URL: {advisory_url or 'stored GitHub activity only'}. "
        "Suggested angle: turn the remediation into a practical write-up covering impact, "
        "upgrade or patch path, verification steps, and lessons learned."
    )


def _advisory_fingerprint(
    *,
    repo_name: str,
    advisory_id: str,
    cves: tuple[str, ...],
    ghsa_ids: tuple[str, ...],
) -> str:
    identifiers = sorted({*_lower_nonempty(cves), *_lower_nonempty(ghsa_ids), advisory_id.lower()})
    raw = "|".join([repo_name.lower(), *identifiers])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _affected_packages(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    raw = metadata.get("affected_packages") or metadata.get("vulnerabilities") or []
    packages: list[dict[str, Any]] = []
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            package = item.get("package") if isinstance(item.get("package"), dict) else {}
            normalized = {
                "ecosystem": item.get("ecosystem") or package.get("ecosystem"),
                "name": item.get("name") or package.get("name"),
                "vulnerable_version_range": item.get("vulnerable_version_range"),
                "patched_versions": item.get("patched_versions"),
                "vulnerable_functions": item.get("vulnerable_functions") or [],
            }
            packages.append({key: value for key, value in normalized.items() if value not in (None, "", [])})
    if not packages:
        names = _string_list(metadata.get("package_names") or metadata.get("package"))
        ecosystem = _normalize_text(metadata.get("ecosystem"))
        packages = [
            {key: value for key, value in {"name": name, "ecosystem": ecosystem}.items() if value}
            for name in names
        ]
    return sorted(packages, key=lambda item: (_normalize_text(item.get("ecosystem")), _normalize_text(item.get("name"))))


def _package_names(packages: tuple[dict[str, Any], ...], metadata: dict[str, Any]) -> tuple[str, ...]:
    names = [_normalize_text(package.get("name")) for package in packages]
    names.extend(_string_list(metadata.get("package_names")))
    return tuple(sorted(dict.fromkeys(name for name in names if name)))


def _activity_id(row: dict[str, Any]) -> str:
    metadata = _metadata(row)
    return _normalize_text(
        row.get("activity_id")
        or metadata.get("activity_id")
        or f"{row.get('repo_name', '')}#{row.get('number', '')}:{row.get('activity_type', '')}"
    )


def _advisory_identifier(metadata: dict[str, Any], row: dict[str, Any]) -> str:
    values = _string_list(metadata.get("ghsa_ids") or metadata.get("ghsa_id"))
    if values:
        return values[0]
    values = _string_list(metadata.get("cves") or metadata.get("cve_id"))
    if values:
        return values[0]
    return _normalize_text(row.get("number") or metadata.get("id") or row.get("title"))


def _is_withdrawn(*, state: str, withdrawn_at: str) -> bool:
    return state in {"withdrawn", "retracted"} or bool(withdrawn_at)


def _metadata(row: dict[str, Any]) -> dict[str, Any]:
    return _decode_json_object(row.get("metadata"))


def _decode_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _string_list(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, (list, tuple, set)):
        return sorted(dict.fromkeys(_normalize_text(item) for item in value if _normalize_text(item)))
    return [_normalize_text(value)] if _normalize_text(value) else []


def _lower_nonempty(values: tuple[str, ...]) -> list[str]:
    return [value.lower() for value in values if value]


def _normalize_text(value: object | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _normalize_severity(value: object | None) -> str:
    severity = _normalize_text(value).lower()
    return "medium" if severity == "moderate" else severity if severity in SEVERITY_ORDER else ""


def _label_severity(labels: object | None) -> str:
    if isinstance(labels, str):
        try:
            labels = json.loads(labels)
        except (TypeError, ValueError):
            labels = [labels]
    if not isinstance(labels, list):
        return ""
    for label in labels:
        severity = _normalize_severity(label)
        if severity:
            return severity
    return ""


def _severity_rank(value: object | None, *, default: int | None = None) -> int:
    severity = _normalize_severity(value)
    if severity in SEVERITY_ORDER:
        return SEVERITY_ORDER[severity]
    if default is not None:
        return default
    raise ValueError("severity must be one of: low, medium, high, critical")


def _priority_for_severity(severity: str) -> str:
    if severity in {"critical", "high"}:
        return "high"
    if severity == "low":
        return "low"
    return "normal"


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _reverse_sort_text(value: str) -> tuple[int, ...]:
    return tuple(-ord(char) for char in value)


def _shorten(text: str | None, width: int = 70) -> str:
    value = _normalize_text(text)
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        row["name"] if isinstance(row, sqlite3.Row) else row[0]: {
            column["name"] if isinstance(column, sqlite3.Row) else column[1]
            for column in conn.execute(
                f"PRAGMA table_info({row['name'] if isinstance(row, sqlite3.Row) else row[0]})"
            )
        }
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "github_activity": {"id", "repo_name", "activity_type", "number", "title", "state", "url", "updated_at", "metadata", "labels"},
        "content_ideas": {"id", "note", "topic", "priority", "status", "source", "source_metadata"},
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _has_missing_required_columns(missing_columns: dict[str, tuple[str, ...]]) -> bool:
    return any(columns for columns in missing_columns.values())


def _row_dict(row: Any) -> dict[str, Any]:
    item = dict(row)
    if isinstance(item.get("metadata"), str):
        item["metadata"] = _decode_json_object(item.get("metadata"))
    if isinstance(item.get("labels"), str):
        try:
            labels = json.loads(item["labels"])
        except (TypeError, ValueError):
            labels = []
        item["labels"] = labels if isinstance(labels, list) else []
    return item
