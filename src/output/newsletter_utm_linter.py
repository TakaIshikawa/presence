"""Pre-send newsletter UTM coverage linting."""

from __future__ import annotations

from dataclasses import dataclass
import json
import sqlite3
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qs, urlparse

from output.newsletter_link_health import dedupe_links, extract_newsletter_links
from output.link_tracking import LOCAL_HOSTS


REQUIRED_UTM_PARAMETERS = ("utm_source", "utm_medium", "utm_campaign")
DEFAULT_EXPECTED_UTM_VALUES = {
    "utm_source": "newsletter",
    "utm_medium": "email",
}
METADATA_TEXT_KEYS = (
    "body",
    "html",
    "text",
    "content",
    "markdown",
    "preview",
)
IGNORE_URL_MARKERS = (
    "unsubscribe",
    "manage-preferences",
    "manage_preferences",
    "preferences",
)


@dataclass(frozen=True)
class NewsletterUtmIssue:
    """One missing or inconsistent UTM parameter."""

    code: str
    url: str
    parameter: str
    expected: str
    actual: str
    message: str
    occurrences: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "url": self.url,
            "parameter": self.parameter,
            "expected": self.expected,
            "actual": self.actual,
            "message": self.message,
            "occurrences": [dict(occurrence) for occurrence in self.occurrences],
        }


@dataclass(frozen=True)
class NewsletterUtmLink:
    """One unique newsletter link considered by the UTM linter."""

    url: str
    trackable: bool
    ignored: bool
    ignore_reason: str
    domain: str
    utm_values: dict[str, list[str]]
    issues: tuple[NewsletterUtmIssue, ...]
    occurrences: tuple[dict[str, Any], ...]

    @property
    def ok(self) -> bool:
        return not self.issues

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "trackable": self.trackable,
            "ignored": self.ignored,
            "ignore_reason": self.ignore_reason,
            "domain": self.domain,
            "utm_values": {key: list(values) for key, values in sorted(self.utm_values.items())},
            "ok": self.ok,
            "issues": [issue.to_dict() for issue in self.issues],
            "occurrences": [dict(occurrence) for occurrence in self.occurrences],
        }


@dataclass(frozen=True)
class NewsletterUtmLintReport:
    """Aggregated UTM lint result for one newsletter draft or issue."""

    ok: bool
    source: str
    issue_id: str
    expected_utm: dict[str, str]
    link_count: int
    checked_count: int
    ignored_count: int
    issue_count: int
    links: tuple[NewsletterUtmLink, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocking_issue_count(self) -> int:
        return self.issue_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_utm_lint",
            "ok": self.ok,
            "source": self.source,
            "issue_id": self.issue_id,
            "expected_utm": dict(sorted(self.expected_utm.items())),
            "link_count": self.link_count,
            "checked_count": self.checked_count,
            "ignored_count": self.ignored_count,
            "issue_count": self.issue_count,
            "blocking_issue_count": self.blocking_issue_count,
            "links": [link.to_dict() for link in self.links],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def lint_newsletter_utm_text(
    text: str,
    *,
    metadata: Mapping[str, Any] | None = None,
    issue_id: str = "",
    expected_utm: Mapping[str, str] | None = None,
    source: str = "text",
) -> NewsletterUtmLintReport:
    """Lint rendered newsletter HTML/Markdown text for required UTM parameters."""
    expected = _expected_utm(metadata=metadata, issue_id=issue_id, overrides=expected_utm)
    occurrences = extract_newsletter_links(body=text, html=text)
    links = tuple(
        _lint_link(url, grouped_occurrences, expected)
        for url, grouped_occurrences in dedupe_links(occurrences)
    )
    issue_count = sum(len(link.issues) for link in links)
    ignored_count = sum(1 for link in links if link.ignored)
    checked_count = sum(1 for link in links if link.trackable and not link.ignored)
    return NewsletterUtmLintReport(
        ok=issue_count == 0,
        source=source,
        issue_id=issue_id,
        expected_utm=expected,
        link_count=len(links),
        checked_count=checked_count,
        ignored_count=ignored_count,
        issue_count=issue_count,
        links=tuple(sorted(links, key=lambda item: item.url)),
    )


def build_newsletter_utm_lint_report_for_issue(
    db_or_conn: Any,
    issue_id: str,
    *,
    expected_utm: Mapping[str, str] | None = None,
) -> NewsletterUtmLintReport:
    """Look up a newsletter send by issue id and lint its stored body/metadata."""
    if not issue_id:
        raise ValueError("issue_id is required")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "newsletter_sends" not in schema:
        return _empty_report(
            source=f"issue:{issue_id}",
            issue_id=issue_id,
            expected_utm=_expected_utm(metadata=None, issue_id=issue_id, overrides=expected_utm),
            missing_tables=("newsletter_sends",),
            missing_columns={},
        )

    required = {"id", "issue_id"}
    missing = tuple(sorted(required - schema["newsletter_sends"]))
    if missing:
        return _empty_report(
            source=f"issue:{issue_id}",
            issue_id=issue_id,
            expected_utm=_expected_utm(metadata=None, issue_id=issue_id, overrides=expected_utm),
            missing_tables=(),
            missing_columns={"newsletter_sends": missing},
        )

    row = _load_issue_send(conn, schema["newsletter_sends"], issue_id)
    if row is None:
        return _empty_report(
            source=f"issue:{issue_id}",
            issue_id=issue_id,
            expected_utm=_expected_utm(metadata=None, issue_id=issue_id, overrides=expected_utm),
            missing_tables=(),
            missing_columns={},
        )

    metadata = _parse_json(row.get("metadata"))
    metadata = metadata if isinstance(metadata, dict) else {}
    texts = _send_texts(row, metadata)
    return lint_newsletter_utm_text(
        "\n".join(text for _source, text in texts),
        metadata=metadata,
        issue_id=str(row.get("issue_id") or issue_id),
        expected_utm=expected_utm,
        source=f"issue:{issue_id}",
    )


def format_newsletter_utm_lint_json(report: NewsletterUtmLintReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_utm_lint_text(report: NewsletterUtmLintReport) -> str:
    """Render a compact human-readable UTM lint report."""
    lines = [
        "Newsletter UTM Lint",
        f"Source: {report.source}",
        f"Issue: {report.issue_id or '-'}",
        "Expected: "
        + ", ".join(
            f"{parameter}={value or '<present>'}"
            for parameter, value in sorted(report.expected_utm.items())
        ),
        (
            "Links: "
            f"{report.link_count} ({report.checked_count} checked, {report.ignored_count} ignored)"
        ),
        f"Issues: {report.issue_count}",
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        details = ", ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + details)
    if not report.links:
        lines.append("No links found.")
        return "\n".join(lines)
    if not report.issue_count:
        lines.append("No newsletter UTM issues found.")
        return "\n".join(lines)

    lines.append("")
    for link in report.links:
        if link.ignored or not link.issues:
            continue
        lines.append(f"{link.url}")
        for issue in link.issues:
            expectation = issue.expected or "<present>"
            actual = issue.actual or "<missing>"
            lines.append(
                f"  {issue.code}: {issue.parameter} expected={expectation} actual={actual}"
            )
    return "\n".join(lines)


def _lint_link(
    url: str,
    occurrences: Iterable[Any],
    expected_utm: Mapping[str, str],
) -> NewsletterUtmLink:
    parsed = urlparse(url)
    domain = (parsed.hostname or parsed.netloc or "").casefold()
    ignored, reason = _ignore_reason(url)
    occurrence_dicts = tuple(
        occurrence.to_dict() if hasattr(occurrence, "to_dict") else dict(occurrence)
        for occurrence in occurrences
    )
    if ignored:
        return NewsletterUtmLink(
            url=url,
            trackable=False,
            ignored=True,
            ignore_reason=reason,
            domain=domain,
            utm_values={},
            issues=(),
            occurrences=occurrence_dicts,
        )

    query = parse_qs(parsed.query, keep_blank_values=True)
    utm_values = {
        parameter: [value for value in query.get(parameter, [])]
        for parameter in REQUIRED_UTM_PARAMETERS
        if parameter in query
    }
    issues: list[NewsletterUtmIssue] = []
    for parameter in REQUIRED_UTM_PARAMETERS:
        values = [value.strip() for value in query.get(parameter, ()) if value.strip()]
        actual = values[0] if values else ""
        expected = expected_utm.get(parameter, "")
        if not actual:
            issues.append(
                NewsletterUtmIssue(
                    code=f"missing_{parameter}",
                    url=url,
                    parameter=parameter,
                    expected=expected,
                    actual="",
                    message=f"{parameter} is missing.",
                    occurrences=occurrence_dicts,
                )
            )
        elif expected and actual != expected:
            issues.append(
                NewsletterUtmIssue(
                    code=f"inconsistent_{parameter}",
                    url=url,
                    parameter=parameter,
                    expected=expected,
                    actual=actual,
                    message=f"{parameter} does not match expected value.",
                    occurrences=occurrence_dicts,
                )
            )

    return NewsletterUtmLink(
        url=url,
        trackable=True,
        ignored=False,
        ignore_reason="",
        domain=domain,
        utm_values=utm_values,
        issues=tuple(issues),
        occurrences=occurrence_dicts,
    )


def _ignore_reason(url: str) -> tuple[bool, str]:
    if not url:
        return True, "empty"
    if url.startswith("#"):
        return True, "internal_anchor"

    parsed = urlparse(url)
    scheme = parsed.scheme.casefold()
    if scheme == "mailto":
        return True, "mailto"
    if scheme not in {"http", "https"}:
        return True, "unsupported_scheme"
    domain = (parsed.hostname or parsed.netloc or "").casefold()
    if not domain:
        return True, "missing_domain"
    if domain in LOCAL_HOSTS or domain.endswith(".local"):
        return True, "local"

    haystack = " ".join(
        part.casefold()
        for part in (domain, parsed.path, parsed.query, parsed.fragment)
        if part
    )
    if any(marker in haystack for marker in IGNORE_URL_MARKERS):
        return True, "subscriber_management"
    return False, ""


def _expected_utm(
    *,
    metadata: Mapping[str, Any] | None,
    issue_id: str,
    overrides: Mapping[str, str] | None,
) -> dict[str, str]:
    expected = dict(DEFAULT_EXPECTED_UTM_VALUES)
    metadata_expected = _metadata_expected_utm(metadata or {})
    expected.update(metadata_expected)
    if issue_id and "utm_campaign" not in expected:
        expected["utm_campaign"] = issue_id
    if overrides:
        expected.update(
            {
                key: str(value)
                for key, value in overrides.items()
                if key in REQUIRED_UTM_PARAMETERS and value is not None
            }
        )
    for parameter in REQUIRED_UTM_PARAMETERS:
        expected.setdefault(parameter, "")
    return expected


def _metadata_expected_utm(metadata: Mapping[str, Any]) -> dict[str, str]:
    candidates: dict[str, Any] = {}
    for parameter in REQUIRED_UTM_PARAMETERS:
        if parameter in metadata:
            candidates[parameter] = metadata[parameter]
    utm = metadata.get("utm")
    if isinstance(utm, Mapping):
        for short_name, parameter in (
            ("source", "utm_source"),
            ("medium", "utm_medium"),
            ("campaign", "utm_campaign"),
        ):
            if parameter not in candidates and short_name in utm:
                candidates[parameter] = utm[short_name]
            if parameter not in candidates and parameter in utm:
                candidates[parameter] = utm[parameter]
    campaign = metadata.get("campaign") or metadata.get("campaign_id")
    if campaign and "utm_campaign" not in candidates:
        candidates["utm_campaign"] = campaign
    return {
        parameter: str(value).strip()
        for parameter, value in candidates.items()
        if parameter in REQUIRED_UTM_PARAMETERS and str(value).strip()
    }


def _load_issue_send(
    conn: sqlite3.Connection,
    columns: set[str],
    issue_id: str,
) -> dict[str, Any] | None:
    selected = [
        column
        for column in ("id", "issue_id", "subject", "body", "content", "html", "text", "metadata", "sent_at")
        if column in columns
    ]
    cursor = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM newsletter_sends
            WHERE issue_id = ?
            ORDER BY datetime(sent_at) DESC, id DESC
            LIMIT 1""",
        (issue_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return _row_dict(cursor, row)


def _send_texts(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for key in ("body", "content", "html", "text"):
        value = row.get(key)
        if value:
            texts.append((f"newsletter_sends.{key}", str(value)))
    texts.extend(_metadata_texts(metadata, prefix="newsletter_sends.metadata"))
    return texts


def _metadata_texts(value: Any, *, prefix: str) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            child_prefix = f"{prefix}.{key}"
            key_lower = key.casefold()
            if isinstance(item, str) and (
                any(marker in key_lower for marker in METADATA_TEXT_KEYS)
                or "url" in key_lower
            ):
                texts.append((child_prefix, item))
            elif isinstance(item, (Mapping, list, tuple)):
                texts.extend(_metadata_texts(item, prefix=child_prefix))
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            child_prefix = f"{prefix}[{index}]"
            if isinstance(item, str):
                texts.append((child_prefix, item))
            elif isinstance(item, (Mapping, list, tuple)):
                texts.extend(_metadata_texts(item, prefix=child_prefix))
    return texts


def _empty_report(
    *,
    source: str,
    issue_id: str,
    expected_utm: dict[str, str],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> NewsletterUtmLintReport:
    return NewsletterUtmLintReport(
        ok=not missing_tables and not missing_columns,
        source=source,
        issue_id=issue_id,
        expected_utm=expected_utm,
        link_count=0,
        checked_count=0,
        ignored_count=0,
        issue_count=0,
        links=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _parse_json(raw_value: Any) -> Any:
    if raw_value is None or raw_value == "":
        return None
    if isinstance(raw_value, (dict, list)):
        return raw_value
    try:
        return json.loads(str(raw_value))
    except (TypeError, json.JSONDecodeError):
        return None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or Database-like object")
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        columns = conn.execute(f"PRAGMA table_info({table})").fetchall()
        schema[str(table)] = {
            column["name"] if isinstance(column, sqlite3.Row) else column[1]
            for column in columns
        }
    return schema


def _row_dict(cursor: sqlite3.Cursor, row: Any) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return dict(row)
    names = [description[0] for description in cursor.description or ()]
    return dict(zip(names, row))
