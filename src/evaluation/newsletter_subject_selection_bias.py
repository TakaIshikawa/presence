"""Analyze newsletter subject candidate selection bias."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from statistics import median
from typing import Any, Mapping


DEFAULT_DAYS = 60
DEFAULT_MIN_CANDIDATES_PER_ISSUE = 2

SELECTED_NOT_RANK_1 = "selected_not_rank_1"
SELECTED_BELOW_MEDIAN_SCORE = "selected_below_median_score"

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_PUNCT_RE = re.compile(r"[!?]+|:|;")


@dataclass(frozen=True)
class NewsletterSubjectSelectionBiasIssue:
    """One candidate pool where the final selected subject merits review."""

    newsletter_send_id: int | None
    issue_id: str | None
    candidate_count: int
    selected_candidate_id: int
    selected_subject: str
    selected_source: str
    selected_rank: int | None
    selected_score: float
    best_score: float
    median_score: float
    score_delta_vs_best: float
    issue_codes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["issue_codes"] = list(self.issue_codes)
        return payload


@dataclass(frozen=True)
class NewsletterSubjectSelectionBiasReport:
    """Selection-bias report for newsletter subject candidate pools."""

    generated_at: str
    window_days: int
    filters: dict[str, Any]
    totals: dict[str, Any]
    source_bias: dict[str, Any]
    rank_distribution: dict[str, Any]
    score_bands: dict[str, int]
    wording_patterns: dict[str, Any]
    flagged_issues: tuple[NewsletterSubjectSelectionBiasIssue, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    @property
    def has_issues(self) -> bool:
        return bool(self.flagged_issues)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_subject_selection_bias",
            "filters": dict(self.filters),
            "flagged_issues": [issue.to_dict() for issue in self.flagged_issues],
            "generated_at": self.generated_at,
            "has_issues": self.has_issues,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "rank_distribution": self.rank_distribution,
            "score_bands": dict(sorted(self.score_bands.items())),
            "source_bias": self.source_bias,
            "totals": dict(sorted(self.totals.items())),
            "window_days": self.window_days,
            "wording_patterns": self.wording_patterns,
        }


def build_newsletter_subject_selection_bias_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_candidates_per_issue: int = DEFAULT_MIN_CANDIDATES_PER_ISSUE,
    now: datetime | None = None,
) -> NewsletterSubjectSelectionBiasReport:
    """Return a read-only report on selected subject candidate patterns."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_candidates_per_issue <= 0:
        raise ValueError("min_candidates_per_issue must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "cutoff": cutoff.isoformat(),
        "days": days,
        "min_candidates_per_issue": min_candidates_per_issue,
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_report(
            generated_at=generated_at,
            window_days=days,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_candidate_rows(conn, cutoff=cutoff)
    groups = [
        group
        for group in _group_candidate_rows(rows).values()
        if len(group) >= min_candidates_per_issue and _selected_candidate(group)
    ]
    selections = [_selection_summary(group) for group in groups]
    flagged = [selection for selection in selections if selection.issue_codes]
    flagged.sort(
        key=lambda issue: (
            issue.newsletter_send_id is None,
            issue.newsletter_send_id or 0,
            issue.issue_id or "",
            issue.selected_candidate_id,
        )
    )

    return NewsletterSubjectSelectionBiasReport(
        generated_at=generated_at.isoformat(),
        window_days=days,
        filters=filters,
        totals=_totals(rows, groups, selections, flagged),
        source_bias=_source_bias(selections),
        rank_distribution=_rank_distribution(selections),
        score_bands=_score_bands(selections),
        wording_patterns=_wording_patterns(selections),
        flagged_issues=tuple(flagged),
        missing_tables=(),
        missing_columns={},
    )


def format_newsletter_subject_selection_bias_json(
    report: NewsletterSubjectSelectionBiasReport,
) -> str:
    """Format a selection-bias report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_subject_selection_bias_text(
    report: NewsletterSubjectSelectionBiasReport,
) -> str:
    """Format a concise text report for command-line review."""
    totals = report.totals
    filters = report.filters
    lines = [
        "Newsletter Subject Selection Bias Report",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.window_days} days cutoff={filters['cutoff']} "
            f"min_candidates_per_issue={filters['min_candidates_per_issue']}"
        ),
        (
            f"Totals: pools={totals['candidate_pool_count']} "
            f"selected_pools={totals['selected_pool_count']} "
            f"candidates={totals['candidate_count']} flagged={totals['flagged_issue_count']}"
        ),
        (
            "Selected source counts: "
            + _format_counts(report.source_bias["selected_source_counts"])
        ),
        (
            "Selected rank distribution: "
            + _format_counts(report.rank_distribution["selected_rank_distribution"])
        ),
        (
            "Average selected score delta vs best: "
            f"{report.totals['average_selected_score_delta_vs_best']:.4f}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        ]
        lines.append("Missing columns: " + "; ".join(missing))
    lines.append("")

    if not report.flagged_issues:
        lines.append("No newsletter subject selection bias issues found.")
        return "\n".join(lines)

    lines.append("Flagged issues:")
    for issue in report.flagged_issues:
        lines.append(
            f"- send={issue.newsletter_send_id or '-'} issue={issue.issue_id or '-'} "
            f"candidate={issue.selected_candidate_id} rank={issue.selected_rank or '-'} "
            f"source={issue.selected_source} score={issue.selected_score:.2f} "
            f"delta_vs_best={issue.score_delta_vs_best:.2f} "
            f"issues={', '.join(issue.issue_codes)}"
        )
        lines.append(f"  subject={issue.selected_subject}")
    return "\n".join(lines)


def _load_candidate_rows(
    conn: sqlite3.Connection,
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    cursor = conn.execute(
        """SELECT id AS candidate_id,
                  newsletter_send_id,
                  issue_id,
                  subject,
                  score,
                  source,
                  rank,
                  selected,
                  created_at
           FROM newsletter_subject_candidates
           WHERE datetime(created_at) >= datetime(?)
           ORDER BY newsletter_send_id ASC, issue_id ASC, rank ASC, id ASC""",
        (cutoff.isoformat(),),
    )
    return [dict(row) for row in cursor.fetchall()]


def _group_candidate_rows(
    rows: list[Mapping[str, Any]],
) -> dict[tuple[str, int | str], list[Mapping[str, Any]]]:
    groups: dict[tuple[str, int | str], list[Mapping[str, Any]]] = {}
    for row in rows:
        send_id = _optional_int(row.get("newsletter_send_id"))
        issue_id = str(row.get("issue_id") or "")
        key: tuple[str, int | str]
        if send_id is not None:
            key = ("send", send_id)
        else:
            key = ("issue", issue_id)
        groups.setdefault(key, []).append(row)
    return groups


def _selection_summary(
    rows: list[Mapping[str, Any]],
) -> NewsletterSubjectSelectionBiasIssue:
    selected = _selected_candidate(rows)
    if selected is None:
        raise ValueError("selection summary requires a selected candidate")
    scores = [_score(row) for row in rows]
    best_score = max(scores)
    median_score = float(median(scores))
    selected_score = _score(selected)
    selected_rank = _optional_int(selected.get("rank"))
    issue_codes = []
    if selected_rank != 1:
        issue_codes.append(SELECTED_NOT_RANK_1)
    if selected_score < median_score:
        issue_codes.append(SELECTED_BELOW_MEDIAN_SCORE)
    return NewsletterSubjectSelectionBiasIssue(
        newsletter_send_id=_optional_int(selected.get("newsletter_send_id")),
        issue_id=_text_or_none(selected.get("issue_id")),
        candidate_count=len(rows),
        selected_candidate_id=int(selected["candidate_id"]),
        selected_subject=str(selected.get("subject") or ""),
        selected_source=_source(selected),
        selected_rank=selected_rank,
        selected_score=round(selected_score, 4),
        best_score=round(best_score, 4),
        median_score=round(median_score, 4),
        score_delta_vs_best=round(selected_score - best_score, 4),
        issue_codes=tuple(issue_codes),
    )


def _selected_candidate(rows: list[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    selected = [row for row in rows if _truthy(row.get("selected"))]
    if not selected:
        return None
    return sorted(selected, key=lambda row: int(row["candidate_id"]))[0]


def _totals(
    rows: list[Mapping[str, Any]],
    groups: list[list[Mapping[str, Any]]],
    selections: list[NewsletterSubjectSelectionBiasIssue],
    flagged: list[NewsletterSubjectSelectionBiasIssue],
) -> dict[str, Any]:
    deltas = [selection.score_delta_vs_best for selection in selections]
    return {
        "average_selected_score_delta_vs_best": round(
            sum(deltas) / len(deltas), 4
        )
        if deltas
        else 0.0,
        "candidate_count": len(rows),
        "candidate_pool_count": len(groups),
        "flagged_issue_count": len(flagged),
        "selected_below_median_count": sum(
            SELECTED_BELOW_MEDIAN_SCORE in issue.issue_codes for issue in flagged
        ),
        "selected_not_rank_1_count": sum(
            SELECTED_NOT_RANK_1 in issue.issue_codes for issue in flagged
        ),
        "selected_pool_count": len(selections),
    }


def _source_bias(
    selections: list[NewsletterSubjectSelectionBiasIssue],
) -> dict[str, Any]:
    counts = Counter(selection.selected_source for selection in selections)
    total = sum(counts.values())
    dominant_source = None
    dominant_share = 0.0
    if counts:
        dominant_source, dominant_count = sorted(
            counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[0]
        dominant_share = round(dominant_count / total, 4)
    return {
        "dominant_selected_source": dominant_source,
        "dominant_selected_source_share": dominant_share,
        "selected_source_counts": dict(sorted(counts.items())),
    }


def _rank_distribution(
    selections: list[NewsletterSubjectSelectionBiasIssue],
) -> dict[str, Any]:
    counts = Counter(_rank_label(selection.selected_rank) for selection in selections)
    total = sum(counts.values())
    return {
        "non_rank_1_selected_count": sum(
            1 for selection in selections if selection.selected_rank != 1
        ),
        "rank_1_selected_share": round(counts.get("1", 0) / total, 4)
        if total
        else 0.0,
        "selected_rank_distribution": dict(sorted(counts.items())),
    }


def _score_bands(
    selections: list[NewsletterSubjectSelectionBiasIssue],
) -> dict[str, int]:
    counts = Counter(_score_band(selection.selected_score) for selection in selections)
    return dict(sorted(counts.items()))


def _wording_patterns(
    selections: list[NewsletterSubjectSelectionBiasIssue],
) -> dict[str, Any]:
    openings = Counter(_opening_token(selection.selected_subject) for selection in selections)
    openings.pop(None, None)
    punctuation = Counter(
        _punctuation_pattern(selection.selected_subject) for selection in selections
    )
    return {
        "selected_opening_token_counts": dict(sorted(openings.items())),
        "selected_punctuation_pattern_counts": dict(sorted(punctuation.items())),
    }


def _score(row: Mapping[str, Any]) -> float:
    try:
        return float(row.get("score") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _source(row: Mapping[str, Any]) -> str:
    return str(row.get("source") or "unknown")


def _rank_label(value: int | None) -> str:
    return str(value) if value is not None else "unknown"


def _score_band(score: float) -> str:
    if score >= 8:
        return "high"
    if score >= 5:
        return "medium"
    return "low"


def _opening_token(subject: str) -> str | None:
    tokens = _TOKEN_RE.findall(subject.lower())
    return tokens[0] if tokens else None


def _punctuation_pattern(subject: str) -> str:
    pieces = _PUNCT_RE.findall(subject)
    return "".join(pieces) or "plain"


def _format_counts(counts: Mapping[str, int]) -> str:
    if not counts:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None


def _empty_report(
    *,
    generated_at: datetime,
    window_days: int,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> NewsletterSubjectSelectionBiasReport:
    return NewsletterSubjectSelectionBiasReport(
        generated_at=generated_at.isoformat(),
        window_days=window_days,
        filters=filters,
        totals={
            "average_selected_score_delta_vs_best": 0.0,
            "candidate_count": 0,
            "candidate_pool_count": 0,
            "flagged_issue_count": 0,
            "selected_below_median_count": 0,
            "selected_not_rank_1_count": 0,
            "selected_pool_count": 0,
        },
        source_bias={
            "dominant_selected_source": None,
            "dominant_selected_source_share": 0.0,
            "selected_source_counts": {},
        },
        rank_distribution={
            "non_rank_1_selected_count": 0,
            "rank_1_selected_share": 0.0,
            "selected_rank_distribution": {},
        },
        score_bands={},
        wording_patterns={
            "selected_opening_token_counts": {},
            "selected_punctuation_pattern_counts": {},
        },
        flagged_issues=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns or {},
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        row[0]: {
            column[1]
            for column in conn.execute(f"PRAGMA table_info({row[0]})").fetchall()
        }
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }


def _schema_gaps(
    schema: Mapping[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "newsletter_subject_candidates": {
            "id",
            "newsletter_send_id",
            "issue_id",
            "subject",
            "score",
            "source",
            "rank",
            "selected",
            "created_at",
        },
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
