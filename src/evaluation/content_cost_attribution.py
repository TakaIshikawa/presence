"""Attribute model usage cost to generated content and outcomes."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 20
DEFAULT_MIN_COST = 0.0


class ContentCostAttributionError(ValueError):
    """Raised when content cost attribution inputs are invalid."""


@dataclass(frozen=True)
class OperationCostBreakdown:
    """Model usage totals for one operation on one content item."""

    operation_name: str
    call_count: int
    input_tokens: int
    output_tokens: int
    total_tokens: int
    estimated_cost: float
    models: list[str]


@dataclass(frozen=True)
class PlatformPublicationStatus:
    """Publication state for a content item on one platform."""

    platform: str
    status: str
    platform_url: str | None
    published_at: str | None
    attempt_count: int


@dataclass(frozen=True)
class ContentCostAttributionItem:
    """Cost attribution summary for one generated content item."""

    content_id: int
    content_type: str | None
    created_at: str | None
    estimated_cost: float
    call_count: int
    input_tokens: int
    output_tokens: int
    total_tokens: int
    operations: list[OperationCostBreakdown]
    outcome: str | None
    rejection_reason: str | None
    published: bool | None
    published_url: str | None
    published_at: str | None
    platform_statuses: list[PlatformPublicationStatus]
    engagement_score: float | None
    engagement_fetched_at: str | None
    cost_per_engagement: float | None


@dataclass(frozen=True)
class ContentCostAttributionReport:
    """Stable report container for content-level model spend attribution."""

    artifact_type: str
    days: int
    content_type: str | None
    published: str
    min_cost: float
    limit: int
    total_content: int
    total_estimated_cost: float
    total_tokens: int
    items: list[ContentCostAttributionItem]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class ContentCostAttribution:
    """Build content-level cost attribution reports from model_usage."""

    def __init__(self, db: Any) -> None:
        self.db = db

    def build_report(
        self,
        *,
        days: int = DEFAULT_DAYS,
        content_type: str | None = None,
        published: str = "all",
        min_cost: float = DEFAULT_MIN_COST,
        limit: int = DEFAULT_LIMIT,
    ) -> ContentCostAttributionReport:
        """Return a deterministic content-level model spend report."""

        days = _positive_int(days, "days")
        limit = max(0, int(limit or 0))
        min_cost = max(0.0, float(min_cost or 0.0))
        published = _normalize_published_filter(published)
        content_type = _clean_string(content_type)

        rows = self._select_content_rows(
            days=days,
            content_type=content_type,
            published=published,
            min_cost=min_cost,
            limit=limit,
        )
        if not rows:
            items: list[ContentCostAttributionItem] = []
        else:
            content_ids = [int(row["content_id"]) for row in rows]
            operations = self._operation_breakdowns(days=days, content_ids=content_ids)
            publications = self._platform_statuses(content_ids)
            items = [
                _item_from_row(
                    row,
                    operations=operations.get(int(row["content_id"]), []),
                    platform_statuses=publications.get(int(row["content_id"]), []),
                )
                for row in rows
            ]

        return ContentCostAttributionReport(
            artifact_type="content_cost_attribution",
            days=days,
            content_type=content_type,
            published=published,
            min_cost=min_cost,
            limit=limit,
            total_content=len(items),
            total_estimated_cost=round(
                sum(item.estimated_cost for item in items),
                8,
            ),
            total_tokens=sum(item.total_tokens for item in items),
            items=items,
        )

    def _select_content_rows(
        self,
        *,
        days: int,
        content_type: str | None,
        published: str,
        min_cost: float,
        limit: int,
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            return []

        clauses = [
            "attributed.created_at >= datetime('now', ?)",
            "attributed.content_id IS NOT NULL",
        ]
        params: list[Any] = [f"-{days} days"]
        if content_type:
            clauses.append("gc.content_type = ?")
            params.append(content_type)
        if published == "published":
            clauses.append("COALESCE(gc.published, 0) = 1")
        elif published == "unpublished":
            clauses.append("COALESCE(gc.published, 0) = 0")

        where_sql = " AND ".join(clauses)
        rows = self.db.conn.execute(
            f"""WITH attributed AS (
                   SELECT mu.id,
                          mu.model_name,
                          mu.operation_name,
                          mu.input_tokens,
                          mu.output_tokens,
                          mu.total_tokens,
                          mu.estimated_cost,
                          mu.pipeline_run_id,
                          mu.created_at,
                          COALESCE(mu.content_id, pr.content_id) AS content_id,
                          pr.outcome AS usage_pipeline_outcome,
                          pr.rejection_reason AS usage_rejection_reason
                   FROM model_usage mu
                   LEFT JOIN pipeline_runs pr ON pr.id = mu.pipeline_run_id
               ),
               latest_pipeline AS (
                   SELECT content_id, outcome, rejection_reason
                   FROM (
                       SELECT content_id, outcome, rejection_reason, created_at, id,
                              ROW_NUMBER() OVER (
                                  PARTITION BY content_id
                                  ORDER BY created_at DESC, id DESC
                              ) AS rn
                       FROM pipeline_runs
                       WHERE content_id IS NOT NULL
                   )
                   WHERE rn = 1
               ),
               latest_engagement AS (
                   SELECT content_id, engagement_score, fetched_at
                   FROM (
                       SELECT content_id, engagement_score, fetched_at, id,
                              ROW_NUMBER() OVER (
                                  PARTITION BY content_id
                                  ORDER BY fetched_at DESC, id DESC
                              ) AS rn
                       FROM post_engagement
                   )
                   WHERE rn = 1
               )
               SELECT attributed.content_id,
                      gc.content_type,
                      gc.created_at,
                      gc.published,
                      gc.published_url,
                      gc.published_at,
                      COALESCE(
                          latest_pipeline.outcome,
                          MAX(attributed.usage_pipeline_outcome)
                      ) AS outcome,
                      COALESCE(
                          latest_pipeline.rejection_reason,
                          MAX(attributed.usage_rejection_reason)
                      ) AS rejection_reason,
                      COUNT(*) AS call_count,
                      COALESCE(SUM(attributed.input_tokens), 0) AS input_tokens,
                      COALESCE(SUM(attributed.output_tokens), 0) AS output_tokens,
                      COALESCE(SUM(attributed.total_tokens), 0) AS total_tokens,
                      COALESCE(SUM(attributed.estimated_cost), 0) AS estimated_cost,
                      latest_engagement.engagement_score,
                      latest_engagement.fetched_at AS engagement_fetched_at
               FROM attributed
               LEFT JOIN generated_content gc ON gc.id = attributed.content_id
               LEFT JOIN latest_pipeline
                 ON latest_pipeline.content_id = attributed.content_id
               LEFT JOIN latest_engagement
                 ON latest_engagement.content_id = attributed.content_id
               WHERE {where_sql}
               GROUP BY attributed.content_id
               HAVING estimated_cost >= ?
               ORDER BY estimated_cost DESC, attributed.content_id DESC
               LIMIT ?""",
            (*params, min_cost, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def _operation_breakdowns(
        self,
        *,
        days: int,
        content_ids: list[int],
    ) -> dict[int, list[OperationCostBreakdown]]:
        if not content_ids:
            return {}
        placeholders = ", ".join("?" for _ in content_ids)
        rows = self.db.conn.execute(
            f"""WITH attributed AS (
                   SELECT mu.id,
                          mu.model_name,
                          mu.operation_name,
                          mu.input_tokens,
                          mu.output_tokens,
                          mu.total_tokens,
                          mu.estimated_cost,
                          mu.created_at,
                          COALESCE(mu.content_id, pr.content_id) AS content_id
                   FROM model_usage mu
                   LEFT JOIN pipeline_runs pr ON pr.id = mu.pipeline_run_id
               )
               SELECT content_id,
                      operation_name,
                      COUNT(*) AS call_count,
                      COALESCE(SUM(input_tokens), 0) AS input_tokens,
                      COALESCE(SUM(output_tokens), 0) AS output_tokens,
                      COALESCE(SUM(total_tokens), 0) AS total_tokens,
                      COALESCE(SUM(estimated_cost), 0) AS estimated_cost,
                      json_group_array(DISTINCT model_name) AS models
               FROM attributed
               WHERE created_at >= datetime('now', ?)
                 AND content_id IN ({placeholders})
               GROUP BY content_id, operation_name
               ORDER BY estimated_cost DESC, operation_name ASC""",
            (f"-{days} days", *content_ids),
        ).fetchall()

        grouped: dict[int, list[OperationCostBreakdown]] = {}
        for row in rows:
            item = OperationCostBreakdown(
                operation_name=str(row["operation_name"]),
                call_count=int(row["call_count"] or 0),
                input_tokens=int(row["input_tokens"] or 0),
                output_tokens=int(row["output_tokens"] or 0),
                total_tokens=int(row["total_tokens"] or 0),
                estimated_cost=round(float(row["estimated_cost"] or 0.0), 8),
                models=sorted(_parse_json_list(row["models"])),
            )
            grouped.setdefault(int(row["content_id"]), []).append(item)
        return grouped

    def _platform_statuses(
        self,
        content_ids: list[int],
    ) -> dict[int, list[PlatformPublicationStatus]]:
        if not content_ids:
            return {}
        placeholders = ", ".join("?" for _ in content_ids)
        rows = self.db.conn.execute(
            f"""SELECT content_id, platform, status, platform_url,
                      published_at, attempt_count
               FROM content_publications
               WHERE content_id IN ({placeholders})
               ORDER BY content_id ASC, platform ASC""",
            tuple(content_ids),
        ).fetchall()
        grouped: dict[int, list[PlatformPublicationStatus]] = {}
        for row in rows:
            status = PlatformPublicationStatus(
                platform=str(row["platform"]),
                status=str(row["status"]),
                platform_url=_clean_string(row["platform_url"]),
                published_at=_clean_string(row["published_at"]),
                attempt_count=int(row["attempt_count"] or 0),
            )
            grouped.setdefault(int(row["content_id"]), []).append(status)
        return grouped


def export_to_json(report: ContentCostAttributionReport) -> str:
    """Serialize a content cost attribution report as stable JSON."""

    return json.dumps(report.as_dict(), indent=2, sort_keys=True)


def format_text_report(report: ContentCostAttributionReport) -> str:
    """Render a concise text report for operator review."""

    filters = [f"last {report.days} days", f"min cost ${report.min_cost:.4f}"]
    if report.content_type:
        filters.append(f"type {report.content_type}")
    if report.published != "all":
        filters.append(report.published)

    lines = [
        "Content Cost Attribution",
        f"Filters: {', '.join(filters)}",
        (
            f"Total: {report.total_content} content items, "
            f"${report.total_estimated_cost:.4f}, "
            f"{report.total_tokens} tokens"
        ),
        "",
    ]
    if not report.items:
        lines.append("No content cost attribution rows matched.")
        return "\n".join(lines).rstrip()

    for index, item in enumerate(report.items, start=1):
        published = "published" if item.published else "unpublished"
        cpe = (
            f"${item.cost_per_engagement:.4f}/engagement"
            if item.cost_per_engagement is not None
            else "n/a"
        )
        lines.append(
            f"{index}. content #{item.content_id} "
            f"({item.content_type or 'unknown'}, {published}, "
            f"outcome={item.outcome or 'unknown'})"
        )
        lines.append(
            f"   Cost ${item.estimated_cost:.4f}; "
            f"tokens {item.total_tokens}; calls {item.call_count}; "
            f"engagement {item.engagement_score if item.engagement_score is not None else 'n/a'}; "
            f"CPE {cpe}"
        )
        if item.operations:
            parts = [
                (
                    f"{operation.operation_name} "
                    f"${operation.estimated_cost:.4f}/"
                    f"{operation.total_tokens} tok"
                )
                for operation in item.operations
            ]
            lines.append(f"   Operations: {'; '.join(parts)}")
        if item.platform_statuses:
            statuses = [
                f"{status.platform}:{status.status}"
                for status in item.platform_statuses
            ]
            lines.append(f"   Platforms: {', '.join(statuses)}")
        lines.append("")

    return "\n".join(lines).rstrip()


def _item_from_row(
    row: dict[str, Any],
    *,
    operations: list[OperationCostBreakdown],
    platform_statuses: list[PlatformPublicationStatus],
) -> ContentCostAttributionItem:
    estimated_cost = round(float(row["estimated_cost"] or 0.0), 8)
    engagement_score = _optional_float(row.get("engagement_score"))
    return ContentCostAttributionItem(
        content_id=int(row["content_id"]),
        content_type=_clean_string(row.get("content_type")),
        created_at=_clean_string(row.get("created_at")),
        estimated_cost=estimated_cost,
        call_count=int(row["call_count"] or 0),
        input_tokens=int(row["input_tokens"] or 0),
        output_tokens=int(row["output_tokens"] or 0),
        total_tokens=int(row["total_tokens"] or 0),
        operations=operations,
        outcome=_clean_string(row.get("outcome")),
        rejection_reason=_clean_string(row.get("rejection_reason")),
        published=_optional_bool(row.get("published")),
        published_url=_clean_string(row.get("published_url")),
        published_at=_clean_string(row.get("published_at")),
        platform_statuses=platform_statuses,
        engagement_score=engagement_score,
        engagement_fetched_at=_clean_string(row.get("engagement_fetched_at")),
        cost_per_engagement=_cost_per_engagement(estimated_cost, engagement_score),
    )


def _cost_per_engagement(
    estimated_cost: float,
    engagement_score: float | None,
) -> float | None:
    if engagement_score is None or engagement_score <= 0:
        return None
    return round(estimated_cost / engagement_score, 8)


def _normalize_published_filter(value: str | None) -> str:
    normalized = str(value or "all").strip().lower()
    aliases = {
        "true": "published",
        "yes": "published",
        "1": "published",
        "false": "unpublished",
        "no": "unpublished",
        "0": "unpublished",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in {"all", "published", "unpublished"}:
        raise ContentCostAttributionError(
            "published must be one of: all, published, unpublished"
        )
    return normalized


def _positive_int(value: Any, name: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ContentCostAttributionError(f"{name} must be positive")
    return parsed


def _clean_string(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _parse_json_list(value: Any) -> list[str]:
    if value is None:
        return []
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if item is not None]
