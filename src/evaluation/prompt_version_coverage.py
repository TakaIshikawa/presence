"""Prompt template coverage reporting."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "synthesis" / "prompts"
WEAK_ACTUAL_THRESHOLD = 5.0
WEAK_MAE_THRESHOLD = 3.0

_VERSION_RE = re.compile(r"^(?P<type>.+)_v(?P<version>\d+)$")


@dataclass(frozen=True)
class PromptVersionCoverageRow:
    """Coverage signals for one prompt template file."""

    prompt_file: str
    prompt_type: str
    inferred_version: int
    prompt_hash: str
    registered_version: int | None
    recent_usage_count: int
    total_usage_count: int
    latest_usage_at: str | None
    recent_prediction_count: int
    avg_predicted_score: float | None
    avg_actual_engagement_score: float | None
    mean_absolute_prediction_error: float | None
    statuses: tuple[str, ...]
    rationale: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["statuses"] = list(self.statuses)
        data["rationale"] = list(self.rationale)
        return data


@dataclass(frozen=True)
class PromptVersionCoverageReport:
    """Read-only prompt template coverage report."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    counts: dict[str, int]
    missing_required_tables: tuple[str, ...]
    missing_optional_tables: tuple[str, ...]
    prompts: tuple[PromptVersionCoverageRow, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "counts": self.counts,
            "filters": self.filters,
            "generated_at": self.generated_at,
            "missing_optional_tables": list(self.missing_optional_tables),
            "missing_required_tables": list(self.missing_required_tables),
            "prompts": [row.to_dict() for row in self.prompts],
        }


def build_prompt_version_coverage_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    prompts_dir: str | Path = DEFAULT_PROMPTS_DIR,
    now: datetime | None = None,
) -> PromptVersionCoverageReport:
    """Compare prompt files with recent generated content and prediction metadata."""
    if days <= 0:
        raise ValueError("days must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    prompt_path = Path(prompts_dir)
    prompt_files = _load_prompt_files(prompt_path)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = ("generated_content",)
    optional = ("engagement_predictions", "prompt_versions")
    missing_required = tuple(table for table in required if table not in schema)
    missing_optional = tuple(table for table in optional if table not in schema)

    rows = tuple(
        _coverage_row(conn, schema, prompt, cutoff=cutoff, now=generated_at)
        for prompt in prompt_files
    )
    counts = {
        "prompt_files": len(rows),
        "recently_used": sum(1 for row in rows if row.recent_usage_count > 0),
        "unvalidated": sum(1 for row in rows if "unvalidated" in row.statuses),
        "stale": sum(1 for row in rows if "stale" in row.statuses),
        "weak_outcomes": sum(1 for row in rows if "weak_outcomes" in row.statuses),
        "missing_required_tables": len(missing_required),
        "missing_optional_tables": len(missing_optional),
    }
    return PromptVersionCoverageReport(
        artifact_type="prompt_version_coverage",
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "cutoff": cutoff.isoformat(),
            "prompts_dir": str(prompt_path),
        },
        counts=counts,
        missing_required_tables=missing_required,
        missing_optional_tables=missing_optional,
        prompts=tuple(sorted(rows, key=lambda row: (row.prompt_type, row.inferred_version, row.prompt_file))),
    )


def format_prompt_version_coverage_json(report: PromptVersionCoverageReport) -> str:
    """Serialize a prompt coverage report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_prompt_version_coverage_text(report: PromptVersionCoverageReport) -> str:
    """Format a prompt coverage report for operator review."""
    lines = [
        "Prompt Version Coverage",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['days']} days",
        (
            f"Counts: prompt_files={report.counts['prompt_files']} "
            f"recently_used={report.counts['recently_used']} "
            f"unvalidated={report.counts['unvalidated']} stale={report.counts['stale']} "
            f"weak_outcomes={report.counts['weak_outcomes']}"
        ),
    ]
    if report.missing_required_tables:
        lines.append("Missing required tables: " + ", ".join(report.missing_required_tables))
    if report.missing_optional_tables:
        lines.append("Missing optional tables: " + ", ".join(report.missing_optional_tables))
    lines.append("")

    if not report.prompts:
        lines.append("No prompt template files were found.")
        return "\n".join(lines)

    for row in report.prompts:
        statuses = ",".join(row.statuses) if row.statuses else "ok"
        registered = f"v{row.registered_version}" if row.registered_version else "-"
        lines.append(
            f"- {row.prompt_file} type={row.prompt_type} inferred=v{row.inferred_version} "
            f"registered={registered} recent_usage={row.recent_usage_count} "
            f"latest={row.latest_usage_at or '-'} status={statuses}"
        )
        outcome = (
            f"  predictions={row.recent_prediction_count} "
            f"avg_predicted={_fmt(row.avg_predicted_score)} "
            f"avg_actual={_fmt(row.avg_actual_engagement_score)} "
            f"mae={_fmt(row.mean_absolute_prediction_error)}"
        )
        lines.append(outcome)
        if row.rationale:
            lines.append("  rationale: " + "; ".join(row.rationale))
    return "\n".join(lines)


def _load_prompt_files(prompts_dir: Path) -> list[dict[str, Any]]:
    if not prompts_dir.exists():
        return []
    prompts = []
    for path in sorted(prompts_dir.glob("*.txt")):
        text = path.read_text(encoding="utf-8")
        stem = path.stem
        match = _VERSION_RE.match(stem)
        if match:
            prompt_type = match.group("type")
            inferred_version = int(match.group("version"))
        else:
            prompt_type = stem
            inferred_version = 1
        prompts.append(
            {
                "path": path,
                "file": path.name,
                "stem": stem,
                "prompt_type": prompt_type,
                "inferred_version": inferred_version,
                "prompt_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            }
        )
    return prompts


def _coverage_row(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    prompt: dict[str, Any],
    *,
    cutoff: datetime,
    now: datetime,
) -> PromptVersionCoverageRow:
    registered = _registered_prompt(conn, schema, prompt)
    usage = _usage_stats(conn, schema, prompt, cutoff=cutoff, now=now)
    predictions = _prediction_stats(conn, schema, prompt, cutoff=cutoff, now=now)
    statuses, rationale = _statuses(usage, predictions)
    return PromptVersionCoverageRow(
        prompt_file=prompt["file"],
        prompt_type=prompt["prompt_type"],
        inferred_version=prompt["inferred_version"],
        prompt_hash=prompt["prompt_hash"],
        registered_version=registered.get("version"),
        recent_usage_count=usage["recent_count"],
        total_usage_count=usage["total_count"],
        latest_usage_at=usage["latest_at"],
        recent_prediction_count=predictions["count"],
        avg_predicted_score=_round_or_none(predictions["avg_predicted"]),
        avg_actual_engagement_score=_round_or_none(predictions["avg_actual"]),
        mean_absolute_prediction_error=_round_or_none(predictions["mae"]),
        statuses=tuple(statuses),
        rationale=tuple(rationale),
    )


def _registered_prompt(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    prompt: dict[str, Any],
) -> dict[str, Any]:
    if "prompt_versions" not in schema:
        return {}
    columns = schema["prompt_versions"]
    if not {"prompt_type", "version", "prompt_hash"}.issubset(columns):
        return {}
    row = conn.execute(
        """SELECT version, prompt_hash, created_at
           FROM prompt_versions
           WHERE prompt_hash = ?
              OR (prompt_type = ? AND version = ?)
              OR (prompt_type = ? AND version = ?)
           ORDER BY CASE WHEN prompt_hash = ? THEN 0 ELSE 1 END, version DESC
           LIMIT 1""",
        (
            prompt["prompt_hash"],
            prompt["stem"],
            prompt["inferred_version"],
            prompt["prompt_type"],
            prompt["inferred_version"],
            prompt["prompt_hash"],
        ),
    ).fetchone()
    return dict(row) if row else {}


def _usage_stats(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    prompt: dict[str, Any],
    *,
    cutoff: datetime,
    now: datetime,
) -> dict[str, Any]:
    if "generated_content" not in schema:
        return {"recent_count": 0, "total_count": 0, "latest_at": None}
    gc = schema["generated_content"]
    if not {"id", "content_type"}.issubset(gc):
        return {"recent_count": 0, "total_count": 0, "latest_at": None}
    created_expr = "gc.created_at" if "created_at" in gc else "NULL"
    joins = ""
    match_clauses = ["gc.content_type = ?"]
    params: list[Any] = [prompt["stem"]]
    if prompt["stem"] == prompt["prompt_type"]:
        match_clauses.append("gc.content_type = ?")
        params.append(prompt["prompt_type"])
    if "engagement_predictions" in schema:
        ep = schema["engagement_predictions"]
        if {"content_id", "prompt_type"}.issubset(ep):
            joins = """
               LEFT JOIN engagement_predictions ep
                 ON ep.content_id = gc.id"""
            match_clauses.append("ep.prompt_type = ?")
            params.append(prompt["stem"])
            if "prompt_hash" in ep:
                match_clauses.append("ep.prompt_hash = ?")
                params.append(prompt["prompt_hash"])
            if "prompt_version" in ep:
                match_clauses.append(
                    "(ep.prompt_type IN (?, ?) AND CAST(ep.prompt_version AS TEXT) IN (?, ?))"
                )
                params.extend(
                    [
                        prompt["stem"],
                        prompt["prompt_type"],
                        str(prompt["inferred_version"]),
                        f"v{prompt['inferred_version']}",
                    ]
                )
    match_sql = " OR ".join(match_clauses)
    recent_filter = ""
    if "created_at" in gc:
        recent_filter = f"AND {created_expr} >= ? AND {created_expr} <= ?"
        recent_params = params + [cutoff.isoformat(), now.isoformat()]
    else:
        recent_params = params

    recent = conn.execute(
        f"""SELECT COUNT(DISTINCT gc.id) AS count, MAX({created_expr}) AS latest_at
            FROM generated_content gc
            {joins}
            WHERE ({match_sql}) {recent_filter}""",
        recent_params,
    ).fetchone()
    total = conn.execute(
        f"""SELECT COUNT(DISTINCT gc.id) AS count, MAX({created_expr}) AS latest_at
            FROM generated_content gc
            {joins}
            WHERE ({match_sql})""",
        params,
    ).fetchone()
    return {
        "recent_count": recent["count"] or 0,
        "total_count": total["count"] or 0,
        "latest_at": recent["latest_at"] or total["latest_at"],
    }


def _prediction_stats(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    prompt: dict[str, Any],
    *,
    cutoff: datetime,
    now: datetime,
) -> dict[str, Any]:
    if "engagement_predictions" not in schema:
        return {"count": 0, "avg_predicted": None, "avg_actual": None, "mae": None}
    ep = schema["engagement_predictions"]
    if "prompt_type" not in ep:
        return {"count": 0, "avg_predicted": None, "avg_actual": None, "mae": None}
    clauses = ["(prompt_type = ?"]
    params: list[Any] = [prompt["stem"]]
    if prompt["stem"] == prompt["prompt_type"]:
        clauses[0] += " OR prompt_type = ?"
        params.append(prompt["prompt_type"])
    if "prompt_hash" in ep:
        clauses[0] += " OR prompt_hash = ?"
        params.append(prompt["prompt_hash"])
    if "prompt_version" in ep:
        clauses[0] += " OR (prompt_type IN (?, ?) AND CAST(prompt_version AS TEXT) IN (?, ?))"
        params.extend(
            [
                prompt["stem"],
                prompt["prompt_type"],
                str(prompt["inferred_version"]),
                f"v{prompt['inferred_version']}",
            ]
        )
    clauses[0] += ")"
    if "created_at" in ep:
        clauses.append("created_at >= ?")
        clauses.append("created_at <= ?")
        params.extend([cutoff.isoformat(), now.isoformat()])
    predicted = "predicted_score" if "predicted_score" in ep else "NULL"
    actual = "actual_engagement_score" if "actual_engagement_score" in ep else "NULL"
    error = "prediction_error" if "prediction_error" in ep else "NULL"
    row = conn.execute(
        f"""SELECT COUNT(*) AS count,
                   AVG({predicted}) AS avg_predicted,
                   AVG({actual}) AS avg_actual,
                   AVG(ABS({error})) AS mae
            FROM engagement_predictions
            WHERE {' AND '.join(clauses)}""",
        params,
    ).fetchone()
    return {
        "count": row["count"] or 0,
        "avg_predicted": row["avg_predicted"],
        "avg_actual": row["avg_actual"],
        "mae": row["mae"],
    }


def _statuses(
    usage: dict[str, Any],
    predictions: dict[str, Any],
) -> tuple[list[str], list[str]]:
    statuses: list[str] = []
    rationale: list[str] = []
    if usage["recent_count"] == 0 and usage["total_count"] == 0:
        statuses.append("unvalidated")
        rationale.append("no generated_content rows matched this prompt file")
    elif usage["recent_count"] == 0:
        statuses.append("stale")
        rationale.append("matched generated_content exists, but none in the lookback window")
    else:
        statuses.append("recent_usage")
        rationale.append(f"{usage['recent_count']} generated_content rows matched recently")

    weak_reasons = []
    avg_actual = predictions.get("avg_actual")
    mae = predictions.get("mae")
    if avg_actual is not None and avg_actual < WEAK_ACTUAL_THRESHOLD:
        weak_reasons.append(f"avg actual engagement {avg_actual:.2f} below {WEAK_ACTUAL_THRESHOLD:g}")
    if mae is not None and mae >= WEAK_MAE_THRESHOLD:
        weak_reasons.append(f"prediction MAE {mae:.2f} at or above {WEAK_MAE_THRESHOLD:g}")
    if weak_reasons:
        statuses.append("weak_outcomes")
        rationale.extend(weak_reasons)
    elif predictions.get("count"):
        rationale.append(f"{predictions['count']} engagement_predictions rows joined")
    return statuses, rationale


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _round_or_none(value: float | None, digits: int = 2) -> float | None:
    return round(value, digits) if value is not None else None


def _fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f}"
