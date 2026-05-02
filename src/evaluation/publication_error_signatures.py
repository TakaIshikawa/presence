"""Digest recurring publication attempt failures by normalized error signature."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any

from output.publish_errors import classify_publish_error, normalize_error_category


DEFAULT_DAYS = 30
DEFAULT_MIN_COUNT = 2
MAX_SIGNATURE_LENGTH = 180
MAX_SAMPLE_ERRORS = 3
PLATFORMS = ("x", "bluesky")
VALID_PLATFORMS = {"all", *PLATFORMS}

_URL_RE = re.compile(r"\b(?:[a-z][a-z0-9+.-]*://|www\.)\S+", re.IGNORECASE)
_EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w.-]+\.[a-z]{2,}\b", re.IGNORECASE)
_ISO_TIMESTAMP_RE = re.compile(
    r"\b\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}"
    r"(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b",
    re.IGNORECASE,
)
_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
_TIME_RE = re.compile(r"\b\d{1,2}:\d{2}(?::\d{2})?(?:\.\d+)?\b")
_UUID_RE = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)
_LONG_HEX_RE = re.compile(r"\b[0-9a-f]{12,}\b", re.IGNORECASE)
_LONG_TOKEN_RE = re.compile(r"\b[A-Za-z0-9_-]{20,}\b")
_KEYED_ID_RE = re.compile(
    r"\b("
    r"id|ids|tweet|content|queue|publication|attempt|request|trace|"
    r"uri|cid|did|record"
    r")([ #:=/-]+)[A-Za-z0-9_.:-]{3,}\b",
    re.IGNORECASE,
)
_NUMBER_RE = re.compile(r"\b\d+\b")
_WHITESPACE_RE = re.compile(r"\s+")
_STATUS_CODES = ("400", "401", "402", "403", "404", "409", "422", "429", "500", "502", "503", "504")


@dataclass(frozen=True)
class PublicationErrorSignature:
    """One recurring unsuccessful publication attempt signature."""

    platform: str
    error_category: str
    signature: str
    count: int
    first_attempted_at: str | None
    last_attempted_at: str | None
    affected_content_ids: tuple[int, ...]
    attempt_ids: tuple[int, ...]
    sample_errors: tuple[str, ...]
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["affected_content_ids"] = list(self.affected_content_ids)
        payload["attempt_ids"] = list(self.attempt_ids)
        payload["sample_errors"] = list(self.sample_errors)
        return payload


@dataclass(frozen=True)
class PublicationErrorSignatureReport:
    """Read-only report for recurring unsuccessful publication attempts."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    signatures: tuple[PublicationErrorSignature, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "publication_error_signatures",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "has_issues": bool(self.signatures),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "signature_count": len(self.signatures),
            "signatures": [signature.to_dict() for signature in self.signatures],
            "totals": dict(sorted(self.totals.items())),
        }


@dataclass(frozen=True)
class _AttemptFailure:
    attempt_id: int | None
    content_id: int | None
    platform: str
    error_category: str
    error: str
    attempted_at: str | None


def build_publication_error_signature_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str = "all",
    min_count: int = DEFAULT_MIN_COUNT,
    now: datetime | None = None,
) -> PublicationErrorSignatureReport:
    """Group unsuccessful publication attempts by normalized error signature."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_count <= 0:
        raise ValueError("min_count must be positive")
    if platform not in VALID_PLATFORMS:
        raise ValueError(f"invalid platform: {platform}")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "min_count": min_count,
        "platform": platform,
        "window_end": generated_at.isoformat(),
        "window_start": cutoff.isoformat(),
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return PublicationErrorSignatureReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals=_empty_totals(),
            signatures=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    failures = _load_failures(conn, cutoff=cutoff.isoformat(), platform=platform)
    signatures = tuple(_build_signatures(failures, min_count=min_count))
    totals = _build_totals(failures, signatures)
    return PublicationErrorSignatureReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=totals,
        signatures=signatures,
    )


def format_publication_error_signature_json(
    report: PublicationErrorSignatureReport,
) -> str:
    """Serialize a publication error signature report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publication_error_signature_text(
    report: PublicationErrorSignatureReport,
) -> str:
    """Render publication error signatures for terminal output."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Publication Error Signatures",
        f"Generated: {report.generated_at}",
        (
            f"Window: {filters['window_start']} to {filters['window_end']} "
            f"({filters['days']} days)"
        ),
        f"Platform: {filters['platform']}",
        f"Min count: {filters['min_count']}",
    ]
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    missing = [
        f"{table}({', '.join(columns)})"
        for table, columns in (report.missing_columns or {}).items()
        if columns
    ]
    if missing:
        lines.append(f"Missing columns: {'; '.join(missing)}")
    lines.append(
        "Totals: "
        f"failed_attempts={totals['failed_attempts']} "
        f"signature_groups_scanned={totals['signature_groups_scanned']} "
        f"findings={totals['finding_count']}"
    )
    lines.append("")

    if not report.signatures:
        lines.append("No recurring publication error signatures found.")
        return "\n".join(lines)

    lines.append("Findings:")
    for signature in report.signatures:
        lines.append(
            "  - "
            f"{signature.platform} / {signature.error_category}: "
            f"count={signature.count} "
            f"first_attempted_at={signature.first_attempted_at or '-'} "
            f"last_attempted_at={signature.last_attempted_at or '-'} "
            f"action={signature.recommended_action}"
        )
        lines.append(f"    signature: {signature.signature}")
        lines.append(
            "    "
            f"content_ids={_join_ids(signature.affected_content_ids)}; "
            f"sample={signature.sample_errors[0] if signature.sample_errors else '-'}"
        )
    return "\n".join(lines)


def normalize_publication_error_signature(error: Any) -> str:
    """Collapse volatile IDs, URLs, timestamps, and long tails out of error text."""
    text = str(error or "").strip().lower()
    if not text:
        return "(empty error)"

    text = _URL_RE.sub("<url>", text)
    text = _EMAIL_RE.sub("<email>", text)
    text = _ISO_TIMESTAMP_RE.sub("<timestamp>", text)
    text = _DATE_RE.sub("<date>", text)
    text = _TIME_RE.sub("<time>", text)
    text = _UUID_RE.sub("<id>", text)
    text = _KEYED_ID_RE.sub(
        lambda match: f"{match.group(1).lower()}{match.group(2)}<id>",
        text,
    )
    text = _LONG_HEX_RE.sub("<id>", text)
    text = _LONG_TOKEN_RE.sub("<id>", text)
    placeholders: dict[str, str] = {}
    for index, code in enumerate(_STATUS_CODES):
        token = f"__status_{index}__"
        placeholders[token] = code
        text = re.sub(rf"\b{re.escape(code)}\b", token, text)
    text = _NUMBER_RE.sub("<id>", text)
    for token, code in placeholders.items():
        text = text.replace(token, code)
    text = _WHITESPACE_RE.sub(" ", text)
    text = text.replace("( ", "(").replace(" )", ")").strip(" .")
    if len(text) > MAX_SIGNATURE_LENGTH:
        text = text[:MAX_SIGNATURE_LENGTH].rstrip()
    return text or "(empty error)"


def _load_failures(
    conn: sqlite3.Connection,
    *,
    cutoff: str,
    platform: str,
) -> list[_AttemptFailure]:
    filters = ["attempted_at >= ?", "COALESCE(success, 0) = 0"]
    params: list[Any] = [cutoff]
    if platform != "all":
        filters.append("platform = ?")
        params.append(platform)
    rows = conn.execute(
        f"""SELECT id, content_id, platform, attempted_at, error, error_category
            FROM publication_attempts
            WHERE {' AND '.join(filters)}
            ORDER BY attempted_at ASC, id ASC""",
        params,
    ).fetchall()
    return [_failure_from_row(dict(row)) for row in rows]


def _failure_from_row(row: dict[str, Any]) -> _AttemptFailure:
    error = _clean(row.get("error")) or ""
    platform = _clean(row.get("platform")) or "unknown"
    category = normalize_error_category(row.get("error_category"))
    if category == "unknown":
        category = classify_publish_error(error, platform=platform)
    return _AttemptFailure(
        attempt_id=_optional_int(row.get("id")),
        content_id=_optional_int(row.get("content_id")),
        platform=platform,
        error_category=category,
        error=error,
        attempted_at=_clean(row.get("attempted_at")),
    )


def _build_signatures(
    failures: list[_AttemptFailure],
    *,
    min_count: int,
) -> list[PublicationErrorSignature]:
    grouped: dict[tuple[str, str, str], list[_AttemptFailure]] = {}
    for failure in failures:
        signature = normalize_publication_error_signature(failure.error)
        key = (failure.platform, failure.error_category, signature)
        grouped.setdefault(key, []).append(failure)

    findings: list[PublicationErrorSignature] = []
    for (platform, category, signature), rows in grouped.items():
        if len(rows) < min_count:
            continue
        ordered = sorted(rows, key=_failure_sort_key)
        attempted = sorted(row.attempted_at for row in rows if row.attempted_at)
        samples: list[str] = []
        for row in ordered:
            if row.error and row.error not in samples:
                samples.append(row.error)
        findings.append(
            PublicationErrorSignature(
                platform=platform,
                error_category=category,
                signature=signature,
                count=len(rows),
                first_attempted_at=attempted[0] if attempted else None,
                last_attempted_at=attempted[-1] if attempted else None,
                affected_content_ids=_ids(row.content_id for row in rows),
                attempt_ids=_ids(row.attempt_id for row in rows),
                sample_errors=tuple(samples[:MAX_SAMPLE_ERRORS]),
                recommended_action=_recommended_action(category),
            )
        )
    findings.sort(
        key=lambda item: (
            item.platform,
            item.error_category,
            item.signature,
        )
    )
    return findings


def _build_totals(
    failures: list[_AttemptFailure],
    signatures: tuple[PublicationErrorSignature, ...],
) -> dict[str, Any]:
    grouped_keys = {
        (
            failure.platform,
            failure.error_category,
            normalize_publication_error_signature(failure.error),
        )
        for failure in failures
    }
    by_platform: dict[str, int] = {}
    by_category: dict[str, int] = {}
    for failure in failures:
        by_platform[failure.platform] = by_platform.get(failure.platform, 0) + 1
        by_category[failure.error_category] = by_category.get(failure.error_category, 0) + 1
    return {
        "by_error_category": dict(sorted(by_category.items())),
        "by_platform": dict(sorted(by_platform.items())),
        "failed_attempts": len(failures),
        "finding_count": len(signatures),
        "signature_groups_scanned": len(grouped_keys),
    }


def _empty_totals() -> dict[str, Any]:
    return {
        "by_error_category": {},
        "by_platform": {},
        "failed_attempts": 0,
        "finding_count": 0,
        "signature_groups_scanned": 0,
    }


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "publication_attempts": {
            "id",
            "content_id",
            "platform",
            "attempted_at",
            "success",
            "error",
            "error_category",
        },
    }
    missing_tables = tuple(table for table in sorted(required) if table not in schema)
    missing_columns = {
        table: tuple(column for column in sorted(columns) if column not in schema.get(table, set()))
        for table, columns in required.items()
        if table in schema
        and any(column not in schema.get(table, set()) for column in columns)
    }
    return missing_tables, missing_columns


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ids(values: Any) -> tuple[int, ...]:
    return tuple(sorted({value for value in values if isinstance(value, int)}))


def _failure_sort_key(failure: _AttemptFailure) -> tuple[str, int]:
    return (failure.attempted_at or "", failure.attempt_id or 0)


def _join_ids(values: tuple[int, ...]) -> str:
    return ",".join(str(value) for value in values) if values else "-"


def _recommended_action(category: str) -> str:
    if category in {"network", "rate_limit"}:
        return "retry_later"
    if category == "auth":
        return "fix_credentials"
    if category == "media":
        return "fix_media"
    if category == "duplicate":
        return "cancel_duplicate"
    if category == "validation":
        return "fix_content"
    return "inspect_error"
