"""Find generated content candidates with material judge disagreement."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_SCORE_SPREAD_THRESHOLD = 2.0


@dataclass(frozen=True)
class JudgeDisagreementRow:
    """One candidate group with score spread or pass/fail disagreement."""

    content_id: str
    content_type: str
    prompt_version: str
    model_pair: tuple[str, ...]
    evaluator_models: tuple[str, ...]
    score_min: float | None
    score_max: float | None
    score_spread: float | None
    has_pass_fail_conflict: bool
    has_high_score_spread: bool
    reason_snippets: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["evaluator_models"] = list(self.evaluator_models)
        payload["model_pair"] = list(self.model_pair)
        payload["reason_snippets"] = list(self.reason_snippets)
        return payload


@dataclass(frozen=True)
class JudgeDisagreementReport:
    """Read-only report of material evaluator disagreement."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    rows: tuple[JudgeDisagreementRow, ...]
    pass_fail_conflicts: tuple[JudgeDisagreementRow, ...]
    score_spread_conflicts: tuple[JudgeDisagreementRow, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "pass_fail_conflicts": [row.to_dict() for row in self.pass_fail_conflicts],
            "rows": [row.to_dict() for row in self.rows],
            "score_spread_conflicts": [row.to_dict() for row in self.score_spread_conflicts],
            "totals": dict(sorted(self.totals.items())),
        }


def build_judge_disagreement_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    score_spread_threshold: float = DEFAULT_SCORE_SPREAD_THRESHOLD,
    now: datetime | None = None,
) -> JudgeDisagreementReport:
    """Build a disagreement report from recent eval result rows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if score_spread_threshold < 0:
        raise ValueError("score_spread_threshold must be non-negative")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = (generated_at - timedelta(days=days)).isoformat()
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    rows = (
        _load_rows(conn, schema, cutoff)
        if not missing_tables and not missing_columns
        else []
    )
    disagreements = _disagreement_rows(
        rows,
        score_spread_threshold=score_spread_threshold,
    )
    pass_fail = tuple(row for row in disagreements if row.has_pass_fail_conflict)
    spread = tuple(row for row in disagreements if row.has_high_score_spread)
    return JudgeDisagreementReport(
        artifact_type="judge_disagreement_report",
        generated_at=generated_at.isoformat(),
        filters={"days": days, "score_spread_threshold": score_spread_threshold},
        totals={
            "candidate_group_count": len(_group_rows(rows)),
            "disagreement_count": len(disagreements),
            "pass_fail_conflict_count": len(pass_fail),
            "score_spread_conflict_count": len(spread),
        },
        rows=tuple(disagreements),
        pass_fail_conflicts=pass_fail,
        score_spread_conflicts=spread,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_judge_disagreement_json(report: JudgeDisagreementReport) -> str:
    """Serialize judge disagreements as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_judge_disagreement_text(report: JudgeDisagreementReport) -> str:
    """Render a concise terminal report."""
    lines = [
        "Judge Disagreement Report",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['days']} days",
        (
            f"Disagreements: {report.totals['disagreement_count']} "
            f"pass_fail={report.totals['pass_fail_conflict_count']} "
            f"score_spread={report.totals['score_spread_conflict_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        lines.append(
            "Missing columns: "
            + "; ".join(
                f"{table}({', '.join(columns)})"
                for table, columns in sorted(report.missing_columns.items())
            )
        )
    if not report.rows:
        lines.append("No material judge disagreements found.")
        return "\n".join(lines)
    for row in report.rows:
        lines.append(
            f"- content={row.content_id} type={row.content_type} prompt={row.prompt_version} "
            f"models={','.join(row.evaluator_models)} range={_fmt(row.score_min)}..{_fmt(row.score_max)} "
            f"spread={_fmt(row.score_spread)} pass_fail={row.has_pass_fail_conflict}"
        )
        if row.reason_snippets:
            lines.append("  reasons: " + " | ".join(row.reason_snippets))
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: str,
) -> list[dict[str, Any]]:
    er = schema["eval_results"]
    eb = schema["eval_batches"]
    content_id_expr = "er.content_id" if "content_id" in er else "NULL"
    prompt_expr = (
        "er.prompt_version"
        if "prompt_version" in er
        else ("eb.label" if "label" in eb else "''")
    )
    passed_expr = "er.passed" if "passed" in er else "NULL"
    created_expr = "er.created_at" if "created_at" in er else "eb.created_at"
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT er.id,
                      {content_id_expr} AS content_id,
                      er.content_type,
                      er.generator_model,
                      er.evaluator_model,
                      er.threshold,
                      er.final_score,
                      er.rejection_reason,
                      er.filter_stats,
                      er.final_content,
                      {prompt_expr} AS prompt_version,
                      {passed_expr} AS passed,
                      {created_expr} AS created_at
               FROM eval_results er
               JOIN eval_batches eb ON eb.id = er.batch_id
               WHERE datetime({created_expr}) >= datetime(?)
               ORDER BY er.content_type, prompt_version, er.id""",
            (cutoff,),
        ).fetchall()
    ]


def _disagreement_rows(
    rows: list[dict[str, Any]],
    *,
    score_spread_threshold: float,
) -> list[JudgeDisagreementRow]:
    disagreements: list[JudgeDisagreementRow] = []
    for group in _group_rows(rows).values():
        if len(group) < 2:
            continue
        scores = [float(row["final_score"]) for row in group if row.get("final_score") is not None]
        score_min = min(scores) if scores else None
        score_max = max(scores) if scores else None
        spread = (score_max - score_min) if score_min is not None and score_max is not None else None
        pass_values = {_passed(row) for row in group if _passed(row) is not None}
        high_spread = spread is not None and spread >= score_spread_threshold
        pass_fail = pass_values == {False, True}
        if not high_spread and not pass_fail:
            continue
        first = group[0]
        evaluator_models = tuple(sorted({_clean(row.get("evaluator_model")) for row in group}))
        model_pair = tuple(sorted({_clean(row.get("generator_model")) for row in group} | set(evaluator_models)))
        disagreements.append(
            JudgeDisagreementRow(
                content_id=_content_key(first),
                content_type=_clean(first.get("content_type")),
                prompt_version=_prompt_version(first),
                model_pair=model_pair,
                evaluator_models=evaluator_models,
                score_min=score_min,
                score_max=score_max,
                score_spread=spread,
                has_pass_fail_conflict=pass_fail,
                has_high_score_spread=high_spread,
                reason_snippets=_reason_snippets(group),
            )
        )
    disagreements.sort(
        key=lambda row: (
            not row.has_pass_fail_conflict,
            -(row.score_spread or 0),
            row.content_type,
            row.prompt_version,
            row.content_id,
        )
    )
    return disagreements


def _group_rows(rows: list[dict[str, Any]]) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(_content_key(row), _clean(row.get("content_type")), _prompt_version(row))].append(row)
    return grouped


def _passed(row: dict[str, Any]) -> bool | None:
    if row.get("passed") is not None:
        return bool(row.get("passed"))
    if row.get("final_score") is None:
        return None
    return float(row["final_score"]) >= float(row.get("threshold") or 0)


def _prompt_version(row: dict[str, Any]) -> str:
    explicit = _clean(row.get("prompt_version"))
    metadata = _json_dict(row.get("filter_stats"))
    return _clean(metadata.get("prompt_version") or metadata.get("prompt") or explicit or "unknown")


def _content_key(row: dict[str, Any]) -> str:
    if row.get("content_id") not in (None, ""):
        return str(row["content_id"])
    content = str(row.get("final_content") or "")
    if content:
        return hashlib.sha1(content.encode("utf-8")).hexdigest()[:12]
    return str(row.get("id"))


def _reason_snippets(rows: list[dict[str, Any]]) -> tuple[str, ...]:
    snippets = []
    for row in rows:
        text = str(row.get("rejection_reason") or "").strip()
        if text and text not in snippets:
            snippets.append(text[:120])
    return tuple(sorted(snippets)[:4])


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "eval_batches": {"id"},
        "eval_results": {
            "batch_id",
            "content_type",
            "evaluator_model",
            "final_score",
            "generator_model",
            "id",
            "threshold",
        },
    }
    missing_tables = tuple(sorted(table for table in required if table not in schema))
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _clean(value: Any) -> str:
    return str(value or "").strip() or "unknown"


def _fmt(value: float | None) -> str:
    return "-" if value is None else f"{value:g}"


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
