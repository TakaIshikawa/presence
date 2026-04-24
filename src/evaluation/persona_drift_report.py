"""Persona guard drift reporting."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from storage.db import Database


@dataclass(frozen=True)
class PersonaFailure:
    """Representative failed persona guard outcome."""

    content_id: int
    content_type: str
    status: str
    score: float
    reasons: list[str]
    created_at: str


@dataclass(frozen=True)
class PersonaDriftReport:
    """Aggregated persona guard outcomes for a date range."""

    days: int
    total: int
    passed: int
    failed: int
    pass_rate: float
    average_score: float
    reason_counts: dict[str, int]
    recent_failures: list[PersonaFailure] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable representation."""
        return {
            "days": self.days,
            "totals": {
                "total": self.total,
                "passed": self.passed,
                "failed": self.failed,
            },
            "pass_rate": self.pass_rate,
            "average_score": self.average_score,
            "reason_counts": self.reason_counts,
            "recent_failures": [
                {
                    "content_id": failure.content_id,
                    "content_type": failure.content_type,
                    "status": failure.status,
                    "score": failure.score,
                    "reasons": failure.reasons,
                    "created_at": failure.created_at,
                }
                for failure in self.recent_failures
            ],
        }


class PersonaDriftReporter:
    """Summarize persona guard outcomes joined to generated content."""

    def __init__(self, db: Database):
        self.db = db

    def build_report(self, days: int = 7, limit_failures: int = 5) -> PersonaDriftReport:
        """Build a persona drift report for guard rows created in the last N days."""
        rows = self._fetch_rows(days)
        if not rows:
            return PersonaDriftReport(
                days=days,
                total=0,
                passed=0,
                failed=0,
                pass_rate=0.0,
                average_score=0.0,
                reason_counts={},
                recent_failures=[],
            )

        total = len(rows)
        passed = sum(1 for row in rows if bool(row["passed"]))
        failed = total - passed
        average_score = sum(float(row["score"] or 0.0) for row in rows) / total

        reason_counter: Counter[str] = Counter()
        failures: list[PersonaFailure] = []
        for row in rows:
            reasons = _parse_reasons(row["reasons"])
            if not bool(row["passed"]):
                reason_counter.update(reasons or ["unspecified"])
                failures.append(
                    PersonaFailure(
                        content_id=int(row["content_id"]),
                        content_type=row["content_type"] or "unknown",
                        status=row["status"] or "unknown",
                        score=round(float(row["score"] or 0.0), 3),
                        reasons=reasons,
                        created_at=row["created_at"] or "",
                    )
                )

        failures.sort(key=lambda failure: (failure.created_at, failure.content_id), reverse=True)
        reason_counts = dict(
            sorted(reason_counter.items(), key=lambda item: (-item[1], item[0]))
        )

        return PersonaDriftReport(
            days=days,
            total=total,
            passed=passed,
            failed=failed,
            pass_rate=round(passed / total, 3),
            average_score=round(average_score, 3),
            reason_counts=reason_counts,
            recent_failures=failures[: max(0, limit_failures)],
        )

    def _fetch_rows(self, days: int) -> list[dict[str, Any]]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=max(0, days))
        cutoff_text = cutoff.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
        cursor = self.db.conn.execute(
            """SELECT
                   cpg.content_id,
                   gc.content_type,
                   cpg.passed,
                   cpg.status,
                   cpg.score,
                   cpg.reasons,
                   COALESCE(cpg.created_at, gc.created_at) AS created_at
               FROM content_persona_guard cpg
               INNER JOIN generated_content gc ON gc.id = cpg.content_id
               WHERE COALESCE(cpg.created_at, gc.created_at) >= ?
               ORDER BY COALESCE(cpg.created_at, gc.created_at) DESC, cpg.content_id DESC""",
            (cutoff_text,),
        )
        return [dict(row) for row in cursor.fetchall()]


def _parse_reasons(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return [str(value)]
    if isinstance(parsed, list):
        return [str(item) for item in parsed if str(item)]
    if isinstance(parsed, str) and parsed:
        return [parsed]
    return []


def format_text_report(report: PersonaDriftReport) -> str:
    """Format a concise human-readable persona drift report."""
    lines = [
        f"Persona Drift Report (last {report.days} days)",
        f"Total: {report.total} | Passed: {report.passed} | Failed: {report.failed}",
        f"Pass rate: {report.pass_rate * 100:.1f}% | Average score: {report.average_score:.3f}",
    ]

    if report.total == 0:
        lines.append("No persona guard rows found.")
        return "\n".join(lines)

    if report.reason_counts:
        lines.append("Recurring reasons:")
        for reason, count in report.reason_counts.items():
            lines.append(f"- {reason}: {count}")

    if report.recent_failures:
        lines.append("Recent failures:")
        for failure in report.recent_failures:
            reason_text = "; ".join(failure.reasons) if failure.reasons else "unspecified"
            lines.append(
                f"- content_id={failure.content_id} "
                f"({failure.content_type}, score {failure.score:.3f}): {reason_text}"
            )

    return "\n".join(lines)


def format_json_report(report: PersonaDriftReport) -> str:
    """Format a persona drift report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)
