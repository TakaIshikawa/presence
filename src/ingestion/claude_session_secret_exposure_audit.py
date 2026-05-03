"""Audit Claude sessions for likely secret exposure."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
from pathlib import Path
from typing import Any, Iterable, Mapping

from ingestion.claude_session_approval_decision_audit import (
    COMMAND_COLUMNS,
    METADATA_COLUMNS,
    SESSION_COLUMNS,
    TEXT_COLUMNS,
    TIMESTAMP_COLUMNS,
    _ensure_utc,
    _first_text,
    _metadata,
    _nested_text,
    _tool_name,
)


DEFAULT_LIMIT = 50
DEFAULT_SNIPPET_CHARS = 180

SCAN_COLUMNS = COMMAND_COLUMNS + TEXT_COLUMNS + ("tool_input", "input", "message")
ENV_ASSIGNMENT_RE = re.compile(
    r"\b([A-Z][A-Z0-9_]*(?:API_KEY|TOKEN|SECRET|PASSWORD)[A-Z0-9_]*)\s*=\s*([^\s'\"`]+)"
)
BEARER_RE = re.compile(r"\bBearer\s+([A-Za-z0-9._~+/=-]{20,})\b", re.I)
API_KEY_RE = re.compile(
    r"\b("
    r"sk-[A-Za-z0-9_-]{20,}|"
    r"AKIA[0-9A-Z]{16}|"
    r"AIza[0-9A-Za-z_-]{20,}|"
    r"[a-z0-9]{16,}_[A-Za-z0-9_-]{16,}"
    r")\b"
)
PRIVATE_KEY_RE = re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----")


@dataclass(frozen=True)
class ClaudeSessionSecretExposureFinding:
    """One redacted likely secret exposure."""

    session_id: str
    timestamp: str | None
    project_path: str | None
    tool_name: str
    secret_family: str
    severity: str
    source_field: str
    evidence: str
    source_table: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeSessionSecretExposureAuditReport:
    """Claude session secret exposure audit report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    findings: tuple[ClaudeSessionSecretExposureFinding, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_secret_exposure_audit",
            "filters": dict(self.filters),
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "totals": dict(sorted(self.totals.items())),
        }


def build_claude_session_secret_exposure_audit_report(
    rows: Iterable[Mapping[str, Any]],
    *,
    max_snippet_chars: int = DEFAULT_SNIPPET_CHARS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeSessionSecretExposureAuditReport:
    """Build a deterministic, redacted audit of likely secret exposure."""
    if max_snippet_chars <= 0:
        raise ValueError("max_snippet_chars must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    raw_rows = [_mapping(row) for row in rows]
    findings, malformed_metadata_count = detect_secret_exposures(
        raw_rows,
        max_snippet_chars=max_snippet_chars,
    )
    reported = tuple(findings[:limit])
    return ClaudeSessionSecretExposureAuditReport(
        generated_at=generated_at.isoformat(),
        filters={"limit": limit, "max_snippet_chars": max_snippet_chars},
        totals={
            "critical_severity_count": sum(1 for finding in findings if finding.severity == "critical"),
            "finding_count": len(findings),
            "high_severity_count": sum(1 for finding in findings if finding.severity == "high"),
            "malformed_metadata_count": malformed_metadata_count,
            "reported_count": len(reported),
            "rows_scanned": len(raw_rows),
            "session_count": len({finding.session_id for finding in findings}),
        },
        findings=reported,
    )


def detect_secret_exposures(
    rows: Iterable[Mapping[str, Any]],
    *,
    max_snippet_chars: int = DEFAULT_SNIPPET_CHARS,
) -> tuple[list[ClaudeSessionSecretExposureFinding], int]:
    """Detect likely secret exposure patterns in parsed Claude rows."""
    findings: list[ClaudeSessionSecretExposureFinding] = []
    malformed_metadata_count = 0
    for ordinal, row in enumerate(rows):
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        context = _row_context(row, metadata)
        seen: set[tuple[str, str, str]] = set()
        for source_field, text in _scan_texts(row, metadata):
            for family, severity, secret, start, end in _matches(text):
                key = (source_field, family, secret)
                if key in seen:
                    continue
                seen.add(key)
                findings.append(
                    ClaudeSessionSecretExposureFinding(
                        session_id=context["session_id"],
                        timestamp=context["timestamp"],
                        project_path=context["project_path"],
                        tool_name=context["tool_name"],
                        secret_family=family,
                        severity=severity,
                        source_field=source_field,
                        evidence=_redacted_snippet(
                            text,
                            secret=secret,
                            start=start,
                            end=end,
                            max_chars=max_snippet_chars,
                        ),
                        source_table=str(row.get("_source_table") or "rows"),
                    )
                )
        if findings:
            findings = sorted(findings, key=_finding_sort_key)
    return findings, malformed_metadata_count


def read_claude_session_rows(path: str | Path) -> list[dict[str, Any]]:
    """Read parsed Claude session rows from a JSON or JSONL file."""
    raw = Path(path).read_text(encoding="utf-8")
    if not raw.strip():
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        rows: list[dict[str, Any]] = []
        for line_number, line in enumerate(raw.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at line {line_number}: {exc}") from exc
            if not isinstance(item, Mapping):
                raise ValueError(f"expected object at JSONL line {line_number}")
            rows.append(dict(item))
        return rows
    if isinstance(parsed, Mapping):
        if isinstance(parsed.get("rows"), list):
            return [dict(item) for item in parsed["rows"] if isinstance(item, Mapping)]
        return [dict(parsed)]
    if isinstance(parsed, list):
        return [dict(item) for item in parsed if isinstance(item, Mapping)]
    raise ValueError("expected a JSON object, JSON array, or JSONL objects")


def format_claude_session_secret_exposure_audit_json(
    report: ClaudeSessionSecretExposureAuditReport,
) -> str:
    """Serialize a secret exposure audit report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _matches(text: str) -> list[tuple[str, str, str, int, int]]:
    matches: list[tuple[str, str, str, int, int]] = []
    env_spans: list[tuple[int, int]] = []
    for match in PRIVATE_KEY_RE.finditer(text):
        matches.append(("private_key", "critical", match.group(0), match.start(), match.end()))
    for match in BEARER_RE.finditer(text):
        matches.append(("bearer_token", "high", match.group(1), match.start(1), match.end(1)))
    for match in ENV_ASSIGNMENT_RE.finditer(text):
        key, value = match.group(1), match.group(2).rstrip(",;")
        if _looks_secret_value(value):
            start = match.start(2)
            end = match.start(2) + len(value)
            env_spans.append((start, end))
            matches.append(("env_secret", "high", value, start, end))
    for match in API_KEY_RE.finditer(text):
        secret = match.group(1)
        if any(_spans_overlap(match.start(1), match.end(1), start, end) for start, end in env_spans):
            continue
        if _looks_secret_value(secret):
            matches.append(("api_key", "high", secret, match.start(1), match.end(1)))
    return matches


def _spans_overlap(left_start: int, left_end: int, right_start: int, right_end: int) -> bool:
    return left_start < right_end and right_start < left_end


def _looks_secret_value(value: str) -> bool:
    stripped = value.strip().strip("'\"")
    if len(stripped) < 16:
        return False
    if stripped.lower() in {"example", "placeholder", "notasecret", "changeme"}:
        return False
    return bool(re.search(r"[A-Za-z]", stripped) and re.search(r"[0-9_-]", stripped))


def _scan_texts(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for column in SCAN_COLUMNS:
        _append_value(texts, column, row.get(column))
    for column in METADATA_COLUMNS:
        if column in row:
            continue
        _append_value(texts, column, row.get(column))
    _append_value(texts, "metadata", metadata)
    for path in (
        ("tool_input", "command"),
        ("input", "command"),
        ("tool", "input", "command"),
        ("tool_use", "input", "command"),
        ("message", "content"),
        ("tool_result", "content"),
        ("result", "output"),
    ):
        text = _nested_text(metadata, path)
        if text:
            texts.append((".".join(path), text))
    return [(field, text) for field, text in texts if text.strip()]


def _append_value(texts: list[tuple[str, str]], field: str, value: Any) -> None:
    if isinstance(value, str):
        texts.append((field, value))
        return
    if isinstance(value, Mapping):
        for key, nested in _flatten_mapping(value):
            texts.append((f"{field}.{key}", nested))


def _flatten_mapping(value: Mapping[str, Any], prefix: str = "") -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = []
    for key, nested in value.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(nested, str):
            items.append((path, nested))
        elif isinstance(nested, Mapping):
            items.extend(_flatten_mapping(nested, path))
        elif isinstance(nested, list):
            for index, item in enumerate(nested):
                if isinstance(item, str):
                    items.append((f"{path}.{index}", item))
                elif isinstance(item, Mapping):
                    items.extend(_flatten_mapping(item, f"{path}.{index}"))
    return items


def _row_context(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> dict[str, str | None]:
    return {
        "session_id": (
            _first_text(row, SESSION_COLUMNS)
            or _first_text(metadata, SESSION_COLUMNS)
            or "unknown-session"
        ),
        "timestamp": _first_text(row, TIMESTAMP_COLUMNS)
        or _first_text(metadata, TIMESTAMP_COLUMNS),
        "project_path": _first_text(row, ("project_path", "cwd", "working_directory"))
        or _first_text(metadata, ("project_path", "cwd", "working_directory")),
        "tool_name": _tool_name(row, metadata),
    }


def _redacted_snippet(
    text: str,
    *,
    secret: str,
    start: int,
    end: int,
    max_chars: int,
) -> str:
    redacted = text[:start] + _redact_secret(secret) + text[end:]
    redacted = _redact_known_secrets(redacted)
    compact = " ".join(redacted.split())
    if len(compact) <= max_chars:
        return compact
    marker = _redact_secret(secret)
    marker_index = compact.find(marker)
    if marker_index < 0:
        return compact[: max(0, max_chars - 3)].rstrip() + "..."
    half = max(0, (max_chars - len(marker) - 6) // 2)
    snippet = compact[max(0, marker_index - half) : marker_index + len(marker) + half]
    if marker_index > half:
        snippet = "..." + snippet.lstrip()
    if marker_index + len(marker) + half < len(compact):
        snippet = snippet.rstrip() + "..."
    return snippet[:max_chars]


def _redact_known_secrets(text: str) -> str:
    redacted = PRIVATE_KEY_RE.sub(lambda match: _redact_secret(match.group(0)), text)
    redacted = BEARER_RE.sub(
        lambda match: "Bearer " + _redact_secret(match.group(1)),
        redacted,
    )
    redacted = ENV_ASSIGNMENT_RE.sub(
        lambda match: (
            f"{match.group(1)}={_redact_secret(match.group(2).rstrip(',;'))}"
            if _looks_secret_value(match.group(2).rstrip(",;"))
            else match.group(0)
        ),
        redacted,
    )
    return API_KEY_RE.sub(lambda match: _redact_secret(match.group(1)), redacted)


def _redact_secret(secret: str) -> str:
    value = secret.strip()
    if len(value) <= 8:
        return "[REDACTED]"
    return f"{value[:4]}...[REDACTED]...{value[-4:]}"


def _finding_sort_key(finding: ClaudeSessionSecretExposureFinding) -> tuple[int, str, str, str]:
    severity_rank = {"critical": 0, "high": 1, "medium": 2}.get(finding.severity, 3)
    return (
        severity_rank,
        _timestamp_sort(finding.timestamp),
        finding.session_id,
        finding.secret_family,
    )


def _timestamp_sort(value: Any) -> str:
    from ingestion.claude_session_approval_decision_audit import _parse_datetime

    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value or "")


def _mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row)
