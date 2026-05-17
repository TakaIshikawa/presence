"""Compare generated content review decisions with scores and final gates."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOW_SCORE_THRESHOLD = 5.0
DEFAULT_HIGH_SCORE_THRESHOLD = 8.0


@dataclass(frozen=True)
class ReviewDecisionConsistencyRow:
    item_id: int
    review_decision: str
    evaluator_score: float | None
    final_gate_status: str
    inconsistency_codes: tuple[str, ...]
    consistency_status: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["inconsistency_codes"] = list(self.inconsistency_codes)
        return payload


@dataclass(frozen=True)
class ReviewDecisionConsistencyReport:
    generated_at: str
    filters: dict[str, Any]
    rows: tuple[ReviewDecisionConsistencyRow, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "review_decision_consistency",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "rows": [row.to_dict() for row in self.rows],
        }


def build_review_decision_consistency_report(
    db_or_conn: Any,
    *,
    low_score_threshold: float = DEFAULT_LOW_SCORE_THRESHOLD,
    high_score_threshold: float = DEFAULT_HIGH_SCORE_THRESHOLD,
    now: datetime | None = None,
) -> ReviewDecisionConsistencyReport:
    if low_score_threshold < 0:
        raise ValueError("low_score_threshold must be non-negative")
    if high_score_threshold < low_score_threshold:
        raise ValueError("high_score_threshold must be greater than or equal to low_score_threshold")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    rows = [
        _row(raw, low_score_threshold=low_score_threshold, high_score_threshold=high_score_threshold)
        for raw in _load_review_rows(conn)
    ]
    rows.sort(key=lambda row: (_severity_rank(row.consistency_status), row.item_id))
    return ReviewDecisionConsistencyReport(
        generated_at=generated_at.isoformat(),
        filters={
            "low_score_threshold": low_score_threshold,
            "high_score_threshold": high_score_threshold,
        },
        rows=tuple(rows),
    )


def format_review_decision_consistency_json(report: ReviewDecisionConsistencyReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_review_decision_consistency_table(report: ReviewDecisionConsistencyReport) -> str:
    lines = [
        "Review Decision Consistency",
        f"Generated: {report.generated_at}",
        "",
        "item_id | review_decision | evaluator_score | final_gate_status | inconsistency_codes | consistency_status",
    ]
    if not report.rows:
        lines.append("No generated content review decisions found.")
        return "\n".join(lines)
    for row in report.rows:
        lines.append(
            " | ".join(
                [
                    str(row.item_id),
                    row.review_decision,
                    _fmt(row.evaluator_score),
                    row.final_gate_status,
                    ",".join(row.inconsistency_codes) or "-",
                    row.consistency_status,
                ]
            )
        )
    return "\n".join(lines)


def _load_review_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    if not _has_table(conn, "generated_content"):
        return []
    gc = _columns(conn, "generated_content")
    if {"id", "eval_score"} - gc:
        return []
    decision_expr = "gc.curation_quality" if "curation_quality" in gc else "NULL"
    guard_join = ""
    final_status = "NULL AS final_gate_status"
    final_passed = "NULL AS final_gate_passed"
    if _has_table(conn, "content_persona_guard"):
        cpg = _columns(conn, "content_persona_guard")
        if "content_id" in cpg:
            guard_join = "LEFT JOIN content_persona_guard cpg ON cpg.content_id = gc.id"
            final_status = "cpg.status AS final_gate_status" if "status" in cpg else "NULL AS final_gate_status"
            final_passed = "cpg.passed AS final_gate_passed" if "passed" in cpg else "NULL AS final_gate_passed"
    return conn.execute(
        f"""SELECT gc.id,
                  {decision_expr} AS review_decision,
                  gc.eval_score AS evaluator_score,
                  {final_status},
                  {final_passed}
           FROM generated_content gc
           {guard_join}
           WHERE {decision_expr} IS NOT NULL OR gc.eval_score IS NOT NULL
           ORDER BY gc.id ASC"""
    ).fetchall()


def _row(raw: sqlite3.Row, *, low_score_threshold: float, high_score_threshold: float) -> ReviewDecisionConsistencyRow:
    decision = _normalize_decision(raw["review_decision"])
    score = _float(raw["evaluator_score"])
    gate = _final_gate_status(raw["final_gate_status"], raw["final_gate_passed"])
    codes: list[str] = []
    if decision == "approved" and score is not None and score < low_score_threshold:
        codes.append("approved_low_score")
    if decision == "rejected" and score is not None and score >= high_score_threshold:
        codes.append("rejected_high_score")
    if (decision == "approved" and gate == "failed") or (decision == "rejected" and gate == "passed"):
        codes.append("final_gate_mismatch")
    return ReviewDecisionConsistencyRow(
        item_id=int(raw["id"]),
        review_decision=decision,
        evaluator_score=score,
        final_gate_status=gate,
        inconsistency_codes=tuple(codes),
        consistency_status="inconsistent" if codes else "consistent",
    )


def _normalize_decision(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"good", "approved", "approve", "prefer", "accepted"}:
        return "approved"
    if text in {"too_specific", "rejected", "reject", "revise", "blocked", "failed"}:
        return "rejected"
    return text or "unknown"


def _final_gate_status(status: Any, passed: Any) -> str:
    text = str(status or "").strip().lower()
    if text in {"passed", "pass", "ok", "approved"}:
        return "passed"
    if text in {"failed", "fail", "blocked", "rejected"}:
        return "failed"
    if passed is not None:
        return "passed" if bool(passed) else "failed"
    return "unknown"


def _float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (table,)).fetchone() is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _severity_rank(status: str) -> int:
    return {"inconsistent": 0, "consistent": 1}.get(status, 9)


def _fmt(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}"
