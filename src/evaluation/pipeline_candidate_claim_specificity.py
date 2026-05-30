"""Score pipeline candidate claims for specificity."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


ARTIFACT_TYPE = "pipeline_candidate_claim_specificity"
VAGUE_RE = re.compile(r"\b(many|most|several|various|significant|major|best|leading|world-class|everyone|always|never|huge|massive)\b", re.I)
NUMBER_RE = re.compile(r"\b\d+(?:\.\d+)?%?\b")
CONCRETE_NOUN_RE = re.compile(r"\b(api|release|customer|subscriber|metric|repository|campaign|post|newsletter|source|claim|test|day|week|month|dollar|url|domain)\b", re.I)


def build_pipeline_candidate_claim_specificity_report(candidates: list[dict[str, Any]], *, min_score: int = 60, now: datetime | None = None) -> dict[str, Any]:
    if not 0 <= min_score <= 100:
        raise ValueError("min_score must be between 0 and 100")
    generated_at = _utc(now or datetime.now(timezone.utc))
    rows = []
    for candidate in candidates:
        text = _text(candidate)
        vague = sorted(set(match.group(0).lower() for match in VAGUE_RE.finditer(text)))
        concrete = sorted(set(match.group(0).lower() for match in CONCRETE_NOUN_RE.finditer(text)))
        concrete += NUMBER_RE.findall(text)
        score = max(0, min(100, 55 + len(concrete) * 10 - len(vague) * 18 - (15 if len(text.split()) < 5 else 0)))
        if score >= min_score and not vague:
            continue
        rows.append(
            {
                "pipeline_run_id": candidate.get("pipeline_run_id") or candidate.get("run_id"),
                "candidate_id": candidate.get("candidate_id") or candidate.get("id"),
                "format": candidate.get("format") or "unknown",
                "vague_markers": vague,
                "concrete_markers": concrete,
                "specificity_score": score,
                "recommendation": "revise_with_numbers_or_source" if score < min_score else "review_vague_language",
            }
        )
    rows.sort(key=lambda row: (row["specificity_score"], str(row["pipeline_run_id"]), str(row["candidate_id"])))
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": generated_at.isoformat(), "filters": {"min_score": min_score}, "totals": {"candidate_count": len(candidates), "row_count": len(rows)}, "rows": rows}


def build_pipeline_candidate_claim_specificity_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    table = "pipeline_candidates" if "pipeline_candidates" in schema else "generated_candidates" if "generated_candidates" in schema else None
    if table is None:
        return build_pipeline_candidate_claim_specificity_report([], **kwargs) | {"missing_tables": ["pipeline_candidates"]}
    cols = schema[table]
    select = [_expr(cols, ("pipeline_run_id", "run_id"), "NULL") + " AS pipeline_run_id", _expr(cols, ("id", "candidate_id"), "rowid") + " AS candidate_id", _expr(cols, ("format", "kind"), "NULL") + " AS format", _expr(cols, ("text", "body", "content", "claim"), "NULL") + " AS text"]
    rows = [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY rowid ASC")]
    return build_pipeline_candidate_claim_specificity_report(rows, **kwargs) | {"missing_tables": []}


def format_pipeline_candidate_claim_specificity_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_pipeline_candidate_claim_specificity_text(report: dict[str, Any]) -> str:
    lines = ["Pipeline Candidate Claim Specificity", f"Generated: {report['generated_at']}", f"Rows: {report['totals']['row_count']}"]
    lines.extend(f"{row['pipeline_run_id']} | {row['candidate_id']} | {row['format']} | {row['specificity_score']} | {row['recommendation']}" for row in report["rows"])
    return "\n".join(lines)


def _text(candidate: dict[str, Any]) -> str:
    return str(candidate.get("text") or candidate.get("body") or candidate.get("content") or candidate.get("claim") or "")


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    if isinstance(db_or_conn, sqlite3.Connection):
        db_or_conn.row_factory = sqlite3.Row
        return db_or_conn
    conn = sqlite3.connect(db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _expr(columns: set[str], names: tuple[str, ...], fallback: str) -> str:
    return next((name for name in names if name in columns), fallback)
