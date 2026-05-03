"""Report model usage grouped by provider, model, and pipeline stage."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
from typing import Any


DEFAULT_DAYS = 30


@dataclass(frozen=True)
class ModelUsageProviderMixRow:
    """One provider/model/stage usage group."""

    provider: str
    model: str
    stage: str
    call_count: int
    input_tokens: int
    output_tokens: int
    total_tokens: int
    percentage_of_calls: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ModelUsageProviderMixReport:
    """Model usage provider mix report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    rows: tuple[ModelUsageProviderMixRow, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "model_usage_provider_mix",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def build_model_usage_provider_mix_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    now: datetime | None = None,
) -> ModelUsageProviderMixReport:
    """Build a deterministic report of model usage by provider, model, and stage."""
    if days <= 0:
        raise ValueError("days must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = () if "model_usage" in schema else ("model_usage",)

    if missing_tables:
        return ModelUsageProviderMixReport(
            generated_at=generated_at.isoformat(),
            filters={
                "days": days,
                "lookback_end": generated_at.isoformat(),
                "lookback_start": cutoff.isoformat(),
            },
            totals={
                "call_count": 0,
                "rows_scanned": 0,
            },
            rows=(),
            missing_tables=missing_tables,
        )

    rows_scanned, usage_rows = _load_usage_rows(conn, cutoff=cutoff)
    aggregated = _aggregate_by_provider_model_stage(usage_rows)
    total_calls = sum(row.call_count for row in aggregated)

    return ModelUsageProviderMixReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
        },
        totals={
            "call_count": total_calls,
            "rows_scanned": rows_scanned,
        },
        rows=aggregated,
        missing_tables=missing_tables,
    )


def format_model_usage_provider_mix_json(report: ModelUsageProviderMixReport) -> str:
    """Serialize a model usage provider mix report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_model_usage_provider_mix_text(report: ModelUsageProviderMixReport) -> str:
    """Render a concise human-readable model usage provider mix report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Model Usage Provider Mix",
        f"Generated: {report.generated_at}",
        (
            f"Filters: days={filters['days']} "
            f"lookback_start={filters['lookback_start']} "
            f"lookback_end={filters['lookback_end']}"
        ),
        f"Totals: rows={totals['rows_scanned']} calls={totals['call_count']}",
    ]

    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))

    if not report.rows:
        lines.extend(["", "No model usage records found."])
        return "\n".join(lines)

    lines.extend(["", "Usage by Provider/Model/Stage:"])
    for row in report.rows:
        tokens_str = f"in={row.input_tokens} out={row.output_tokens} total={row.total_tokens}"
        lines.append(
            f"- provider={row.provider} model={row.model} stage={row.stage} "
            f"calls={row.call_count} ({row.percentage_of_calls:.1f}%) {tokens_str}"
        )
    return "\n".join(lines)


def _aggregate_by_provider_model_stage(
    usage_rows: list[dict[str, Any]],
) -> tuple[ModelUsageProviderMixRow, ...]:
    """Group usage rows by provider, model, and stage."""
    groups: dict[tuple[str, str, str], dict[str, int]] = {}

    for row in usage_rows:
        model = row["model_name"] or "unknown"
        provider = _extract_provider(model)
        stage = _extract_stage(row["operation_name"] or "unknown")
        key = (provider, model, stage)

        if key not in groups:
            groups[key] = {
                "call_count": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
            }

        groups[key]["call_count"] += 1
        groups[key]["input_tokens"] += int(row["input_tokens"] or 0)
        groups[key]["output_tokens"] += int(row["output_tokens"] or 0)
        groups[key]["total_tokens"] += int(row["total_tokens"] or 0)

    total_calls = sum(group["call_count"] for group in groups.values())

    rows = [
        ModelUsageProviderMixRow(
            provider=provider,
            model=model,
            stage=stage,
            call_count=data["call_count"],
            input_tokens=data["input_tokens"],
            output_tokens=data["output_tokens"],
            total_tokens=data["total_tokens"],
            percentage_of_calls=(
                (data["call_count"] / total_calls * 100) if total_calls > 0 else 0.0
            ),
        )
        for (provider, model, stage), data in groups.items()
    ]

    # Sort by call count descending, then by provider, model, stage
    return tuple(
        sorted(
            rows,
            key=lambda r: (-r.call_count, r.provider, r.model, r.stage),
        )
    )


def _extract_provider(model_name: str) -> str:
    """Extract provider from model name."""
    model_lower = model_name.lower()

    if model_lower.startswith("claude"):
        return "anthropic"
    if model_lower.startswith("gpt") or model_lower.startswith("o1"):
        return "openai"
    if model_lower.startswith("gemini"):
        return "google"
    if model_lower.startswith("llama"):
        return "meta"
    if model_lower.startswith("mistral"):
        return "mistral"

    return "unknown"


def _extract_stage(operation_name: str) -> str:
    """Extract pipeline stage from operation name."""
    if not operation_name or operation_name == "unknown":
        return "unknown"

    # Extract first segment before first dot
    parts = operation_name.split(".")
    if parts:
        stage = parts[0].strip()
        if stage:
            return stage

    return "unknown"


def _load_usage_rows(
    conn: Any,
    *,
    cutoff: datetime,
) -> tuple[int, list[dict[str, Any]]]:
    """Load model_usage rows within the lookback window."""
    cursor = conn.execute(
        """
        SELECT
            model_name,
            operation_name,
            input_tokens,
            output_tokens,
            total_tokens
        FROM model_usage
        WHERE created_at IS NULL OR created_at >= ?
        ORDER BY created_at ASC, id ASC
        """,
        (cutoff.isoformat(),),
    )

    rows = []
    for row in cursor.fetchall():
        rows.append({
            "model_name": row[0],
            "operation_name": row[1],
            "input_tokens": row[2],
            "output_tokens": row[3],
            "total_tokens": row[4],
        })

    return len(rows), rows


def _ensure_utc(dt: datetime) -> datetime:
    """Ensure datetime has UTC timezone."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> Any:
    """Extract connection from db wrapper or return raw connection."""
    if hasattr(db_or_conn, "conn"):
        return db_or_conn.conn
    return db_or_conn


def _schema(conn: Any) -> dict[str, set[str]]:
    """Return mapping of table name to column names."""
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    )
    schema: dict[str, set[str]] = {}
    for (table_name,) in cursor.fetchall():
        column_cursor = conn.execute(f"PRAGMA table_info({table_name})")
        schema[table_name] = {row[1] for row in column_cursor.fetchall()}
    return schema
