"""Read-only validation for exported publication replay bundles."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
import json
import sqlite3
from typing import Any


VALID_REPLAY_PLATFORMS = ("x", "bluesky")
VALID_FILTER_PLATFORMS = ("all", *VALID_REPLAY_PLATFORMS)
ERROR_SEVERITY = "error"
WARNING_SEVERITY = "warning"


@dataclass(frozen=True)
class PublicationReplayValidationIssue:
    """One deterministic validation issue for a replay target."""

    code: str
    severity: str
    content_id: int | None
    platform: str | None
    detail: str
    expected: Any = None
    actual: Any = None


@dataclass(frozen=True)
class PublicationReplayValidationTarget:
    """Validation status for one content/platform replay target."""

    content_id: int
    platform: str
    status: str
    attempt_count: int
    platform_state_count: int
    selected_variant_count: int
    issues: list[PublicationReplayValidationIssue]


@dataclass(frozen=True)
class PublicationReplayValidationReport:
    """Stable report for publication replay dry-run validation."""

    artifact_type: str
    strict: bool
    bundle_version: int | None
    checked_content_count: int
    checked_target_count: int
    error_count: int
    warning_count: int
    blocked_count: int
    passed_count: int
    targets: list[PublicationReplayValidationTarget]
    bundle_issues: list[PublicationReplayValidationIssue]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_publication_replay_bundle(
    bundle: dict[str, Any],
    db: Any = None,
    strict: bool = False,
) -> PublicationReplayValidationReport:
    """Validate a replay bundle without publishing or mutating storage."""

    conn = _connection(db)
    schema = _schema(conn) if conn is not None else {}
    contents = bundle.get("contents")
    if not isinstance(contents, list):
        contents = []
        bundle_issues = [
            _issue(
                "invalid_bundle_contents",
                ERROR_SEVERITY,
                None,
                None,
                "bundle.contents must be a list",
                expected="list",
                actual=type(bundle.get("contents")).__name__,
            )
        ]
    else:
        bundle_issues = []

    bundle_version = bundle.get("bundle_version")
    if bundle_version != 1:
        bundle_issues.append(
            _issue(
                "unsupported_bundle_version",
                ERROR_SEVERITY,
                None,
                None,
                "bundle_version must be 1",
                expected=1,
                actual=bundle_version,
            )
        )

    filter_platform = (bundle.get("filters") or {}).get("platform")
    if filter_platform is not None and filter_platform not in VALID_FILTER_PLATFORMS:
        bundle_issues.append(
            _issue(
                "invalid_filter_platform",
                ERROR_SEVERITY,
                None,
                str(filter_platform),
                "filters.platform is not supported",
                expected=list(VALID_FILTER_PLATFORMS),
                actual=filter_platform,
            )
        )

    targets = _bundle_targets(contents)
    target_issues: dict[
        tuple[int, str],
        list[PublicationReplayValidationIssue],
    ] = defaultdict(list)
    bundle_issues.extend(_duplicate_content_issues(contents))
    for issue in _duplicate_target_issues(contents):
        if issue.content_id is not None and issue.platform is not None:
            target_issues[(issue.content_id, issue.platform)].append(issue)
        else:
            bundle_issues.append(issue)

    for entry in contents:
        content = entry.get("content") if isinstance(entry, dict) else None
        content_id = _int_or_none(
            (content or {}).get("id") if isinstance(content, dict) else None
        )
        if content_id is None:
            bundle_issues.append(
                _issue(
                    "missing_content_id",
                    ERROR_SEVERITY,
                    None,
                    None,
                    "content entry is missing content.id",
                )
            )
            continue

        target_platforms = {
            platform for cid, platform in targets if cid == content_id
        }
        for platform in _entry_platforms(entry):
            if platform not in VALID_REPLAY_PLATFORMS:
                target_issues[(content_id, platform)].append(
                    _issue(
                        "invalid_platform",
                        ERROR_SEVERITY,
                        content_id,
                        platform,
                        "replay target platform is not supported",
                        expected=list(VALID_REPLAY_PLATFORMS),
                        actual=platform,
                    )
                )

        if conn is None:
            continue

        current_content = _fetch_generated_content(conn, schema, content_id)
        if current_content is None:
            for platform in sorted(target_platforms):
                target_issues[(content_id, platform)].append(
                    _issue(
                        "missing_content",
                        ERROR_SEVERITY,
                        content_id,
                        platform,
                        "generated_content row no longer exists",
                    )
                )
            continue

        bundled_text = content.get("content") if isinstance(content, dict) else None
        if bundled_text is not None and current_content.get("content") != bundled_text:
            for platform in sorted(target_platforms):
                target_issues[(content_id, platform)].append(
                    _issue(
                        "content_mismatch",
                        WARNING_SEVERITY,
                        content_id,
                        platform,
                        "generated_content.content differs from the bundle snapshot",
                        expected=bundled_text,
                        actual=current_content.get("content"),
                    )
                )

        for issue in _variant_issues(conn, schema, content_id, entry):
            target_issues[(content_id, issue.platform or "")].append(issue)
        for issue in _url_issues(conn, schema, content_id, entry):
            target_issues[(content_id, issue.platform or "")].append(issue)

    target_rows: list[PublicationReplayValidationTarget] = []
    for content_id, platform in sorted(targets | set(target_issues)):
        issues = sorted(
            target_issues.get((content_id, platform), []),
            key=lambda item: (item.severity, item.code, item.detail),
        )
        blocked = _has_blocking_issue(issues, strict=strict)
        target_rows.append(
            PublicationReplayValidationTarget(
                content_id=content_id,
                platform=platform,
                status="blocked" if blocked else "passed",
                attempt_count=_count_section(contents, content_id, platform, "attempts"),
                platform_state_count=_count_section(
                    contents,
                    content_id,
                    platform,
                    "platform_states",
                ),
                selected_variant_count=_count_section(
                    contents,
                    content_id,
                    platform,
                    "selected_variants",
                ),
                issues=issues,
            )
        )

    all_issues = [*bundle_issues, *[issue for row in target_rows for issue in row.issues]]
    blocked_count = sum(1 for row in target_rows if row.status == "blocked")
    return PublicationReplayValidationReport(
        artifact_type="publication_replay_validation",
        strict=strict,
        bundle_version=bundle_version if isinstance(bundle_version, int) else None,
        checked_content_count=len(
            {
                _int_or_none((entry.get("content") or {}).get("id"))
                for entry in contents
                if isinstance(entry, dict) and isinstance(entry.get("content"), dict)
            }
            - {None}
        ),
        checked_target_count=len(target_rows),
        error_count=sum(1 for issue in all_issues if issue.severity == ERROR_SEVERITY),
        warning_count=sum(1 for issue in all_issues if issue.severity == WARNING_SEVERITY),
        blocked_count=blocked_count,
        passed_count=sum(1 for row in target_rows if row.status == "passed"),
        targets=target_rows,
        bundle_issues=sorted(bundle_issues, key=lambda item: (item.code, item.detail)),
    )


def export_to_json(report: PublicationReplayValidationReport) -> str:
    """Serialize the validation report as stable JSON."""

    return json.dumps(report.as_dict(), indent=2, sort_keys=True)


def format_text_report(report: PublicationReplayValidationReport) -> str:
    """Render a deterministic operator-facing replay validation report."""

    lines = [
        "Publication Replay Validation",
        (
            "Mode: "
            f"strict={str(report.strict).lower()} "
            f"bundle_version={report.bundle_version if report.bundle_version is not None else '-'}"
        ),
        (
            "Checked: "
            f"contents={report.checked_content_count} "
            f"targets={report.checked_target_count} "
            f"errors={report.error_count} "
            f"warnings={report.warning_count} "
            f"blocked={report.blocked_count} "
            f"passed={report.passed_count}"
        ),
    ]
    if report.bundle_issues:
        lines.append("")
        lines.append("Bundle issues:")
        for issue in report.bundle_issues:
            lines.append(f"- {issue.severity} {issue.code}: {issue.detail}")

    if not report.targets:
        lines.append("")
        lines.append("No replay targets found in bundle.")
        return "\n".join(lines)

    lines.append("")
    for target in report.targets:
        lines.append(
            f"content #{target.content_id} {target.platform}: {target.status} "
            f"attempts={target.attempt_count} "
            f"states={target.platform_state_count} "
            f"variants={target.selected_variant_count}"
        )
        for issue in target.issues:
            lines.append(f"  - {issue.severity} {issue.code}: {issue.detail}")
    return "\n".join(lines)


def _bundle_targets(contents: list[Any]) -> set[tuple[int, str]]:
    targets: set[tuple[int, str]] = set()
    for entry in contents:
        if not isinstance(entry, dict):
            continue
        content = entry.get("content") if isinstance(entry.get("content"), dict) else {}
        content_id = _int_or_none(content.get("id"))
        if content_id is None:
            continue
        for platform in _entry_platforms(entry):
            targets.add((content_id, platform))
    return targets


def _entry_platforms(entry: dict[str, Any]) -> set[str]:
    platforms: set[str] = set()
    for section in ("attempts", "platform_states", "selected_variants"):
        for item in _list(entry.get(section)):
            platform = item.get("platform") if isinstance(item, dict) else None
            if platform is not None:
                platforms.add(str(platform))
    return platforms


def _duplicate_content_issues(contents: list[Any]) -> list[PublicationReplayValidationIssue]:
    ids = [
        _int_or_none(entry.get("content", {}).get("id"))
        for entry in contents
        if isinstance(entry, dict) and isinstance(entry.get("content"), dict)
    ]
    counts = Counter(content_id for content_id in ids if content_id is not None)
    return [
        _issue(
            "duplicate_content_entry",
            ERROR_SEVERITY,
            content_id,
            None,
            "bundle contains multiple content entries for the same generated_content id",
            expected=1,
            actual=count,
        )
        for content_id, count in sorted(counts.items())
        if count > 1
    ]


def _duplicate_target_issues(contents: list[Any]) -> list[PublicationReplayValidationIssue]:
    counts: Counter[tuple[int, str]] = Counter()
    for entry in contents:
        if not isinstance(entry, dict) or not isinstance(entry.get("content"), dict):
            continue
        content_id = _int_or_none(entry["content"].get("id"))
        if content_id is None:
            continue
        for attempt in _list(entry.get("attempts")):
            platform = attempt.get("platform") if isinstance(attempt, dict) else None
            if platform is not None:
                counts[(content_id, str(platform))] += 1

    return [
        _issue(
            "duplicate_replay_target",
            ERROR_SEVERITY,
            content_id,
            platform,
            "bundle contains multiple replay attempts for the same content/platform target",
            expected=1,
            actual=count,
        )
        for (content_id, platform), count in sorted(counts.items())
        if count > 1
    ]


def _variant_issues(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
    entry: dict[str, Any],
) -> list[PublicationReplayValidationIssue]:
    if "content_variants" not in schema:
        return []
    issues: list[PublicationReplayValidationIssue] = []
    for variant in _list(entry.get("selected_variants")):
        if not isinstance(variant, dict):
            continue
        platform = str(variant.get("platform") or "")
        if platform not in VALID_REPLAY_PLATFORMS:
            continue
        current = _fetch_selected_variant(conn, schema, content_id, platform)
        if current is None:
            issues.append(
                _issue(
                    "stale_variant_missing",
                    ERROR_SEVERITY,
                    content_id,
                    platform,
                    "selected variant in bundle is no longer selected",
                )
            )
            continue
        expected_type = variant.get("variant_type")
        expected_content = variant.get("content")
        if (
            current.get("variant_type") != expected_type
            or current.get("content") != expected_content
        ):
            issues.append(
                _issue(
                    "stale_variant_content",
                    ERROR_SEVERITY,
                    content_id,
                    platform,
                    "selected variant content differs from the bundle snapshot",
                    expected={
                        "variant_type": expected_type,
                        "content": expected_content,
                    },
                    actual={
                        "variant_type": current.get("variant_type"),
                        "content": current.get("content"),
                    },
                )
            )
    return issues


def _url_issues(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
    entry: dict[str, Any],
) -> list[PublicationReplayValidationIssue]:
    if "content_publications" not in schema:
        return []
    issues: list[PublicationReplayValidationIssue] = []
    expected_urls: dict[str, set[str | None]] = defaultdict(set)
    for section in ("platform_states", "attempts"):
        for item in _list(entry.get(section)):
            if not isinstance(item, dict):
                continue
            platform = str(item.get("platform") or "")
            if platform in VALID_REPLAY_PLATFORMS and "platform_url" in item:
                expected_urls[platform].add(_blank_to_none(item.get("platform_url")))

    for platform, urls in sorted(expected_urls.items()):
        if platform not in VALID_REPLAY_PLATFORMS:
            continue
        current = _fetch_publication_state(conn, schema, content_id, platform)
        actual_url = _blank_to_none((current or {}).get("platform_url"))
        for expected_url in sorted(urls, key=lambda value: value or ""):
            if expected_url == actual_url:
                continue
            issues.append(
                _issue(
                    "publication_url_mismatch",
                    ERROR_SEVERITY,
                    content_id,
                    platform,
                    "current publication URL differs from the bundle snapshot",
                    expected=expected_url,
                    actual=actual_url,
                )
            )
    return issues


def _fetch_generated_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
) -> dict[str, Any] | None:
    if "generated_content" not in schema:
        return None
    row = conn.execute(
        "SELECT * FROM generated_content WHERE id = ?",
        (content_id,),
    ).fetchone()
    return dict(row) if row else None


def _fetch_selected_variant(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
    platform: str,
) -> dict[str, Any] | None:
    columns = schema.get("content_variants", set())
    if not {"content_id", "platform", "selected"}.issubset(columns):
        return None
    row = conn.execute(
        """SELECT *
           FROM content_variants
           WHERE content_id = ? AND platform = ? AND selected = 1
           ORDER BY id ASC
           LIMIT 1""",
        (content_id, platform),
    ).fetchone()
    return dict(row) if row else None


def _fetch_publication_state(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
    platform: str,
) -> dict[str, Any] | None:
    columns = schema.get("content_publications", set())
    if not {"content_id", "platform"}.issubset(columns):
        return None
    row = conn.execute(
        """SELECT *
           FROM content_publications
           WHERE content_id = ? AND platform = ?
           ORDER BY id ASC
           LIMIT 1""",
        (content_id, platform),
    ).fetchone()
    return dict(row) if row else None


def _count_section(
    contents: list[Any],
    content_id: int,
    platform: str,
    section: str,
) -> int:
    count = 0
    for entry in contents:
        if not isinstance(entry, dict) or not isinstance(entry.get("content"), dict):
            continue
        if _int_or_none(entry["content"].get("id")) != content_id:
            continue
        for item in _list(entry.get(section)):
            if isinstance(item, dict) and str(item.get("platform")) == platform:
                count += 1
    return count


def _has_blocking_issue(
    issues: list[PublicationReplayValidationIssue],
    *,
    strict: bool,
) -> bool:
    return any(
        issue.severity == ERROR_SEVERITY
        or (strict and issue.severity == WARNING_SEVERITY)
        for issue in issues
    )


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _connection(db: Any) -> sqlite3.Connection | None:
    if db is None:
        return None
    return getattr(db, "conn", db)


def _issue(
    code: str,
    severity: str,
    content_id: int | None,
    platform: str | None,
    detail: str,
    *,
    expected: Any = None,
    actual: Any = None,
) -> PublicationReplayValidationIssue:
    return PublicationReplayValidationIssue(
        code=code,
        severity=severity,
        content_id=content_id,
        platform=platform,
        detail=detail,
        expected=expected,
        actual=actual,
    )


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _blank_to_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
