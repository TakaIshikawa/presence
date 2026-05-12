"""Cluster publish queue hold and rejection reasons for operational review."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_MIN_CLUSTER_SIZE = 2
DEFAULT_REPRESENTATIVE_LIMIT = 5
ACTIVE_STATUSES = ("queued", "held")
REASON_COLUMNS = ("hold_reason", "rejection_reason", "error", "error_category")
VOLATILE_RE = re.compile(
    r"https?://\S+|"
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b|"
    r"\b(?:id|uuid|trace|job|content|queue)[_-]?id[:=# ]+[a-z0-9._:-]+\b|"
    r"\b(?:request|trace|job|queue)[ _-]id[:=# ]+[a-z0-9._:-]+\b|"
    r"\b[0-9a-f]{12,}\b",
    re.I,
)
PUNCT_RE = re.compile(r"[\s,;:|/()\\[\\]{}<>]+")


@dataclass(frozen=True)
class PublishQueueHoldReasonCluster:
    """One actionable repeated hold/rejection reason cluster."""

    normalized_reason: str
    example_reason: str
    count: int
    platforms: tuple[str, ...]
    content_types: tuple[str, ...]
    representative_ids: tuple[int, ...]
    oldest_item_id: int | None
    oldest_item_at: str | None
    newest_item_id: int | None
    newest_item_at: str | None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["platforms"] = list(self.platforms)
        payload["content_types"] = list(self.content_types)
        payload["representative_ids"] = list(self.representative_ids)
        return payload


@dataclass(frozen=True)
class PublishQueueHoldReasonClustersReport:
    """Read-only clusters for currently queued or held publish items."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    clusters: tuple[PublishQueueHoldReasonCluster, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "clusters": [cluster.to_dict() for cluster in self.clusters],
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_publish_queue_hold_reason_clusters_report(
    db_or_conn: Any,
    *,
    min_cluster_size: int = DEFAULT_MIN_CLUSTER_SIZE,
    representative_limit: int = DEFAULT_REPRESENTATIVE_LIMIT,
    now: datetime | None = None,
) -> PublishQueueHoldReasonClustersReport:
    """Build a deterministic report of repeated queue hold/rejection reasons."""
    if min_cluster_size <= 0:
        raise ValueError("min_cluster_size must be positive")
    if representative_limit <= 0:
        raise ValueError("representative_limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: set[str] = set()
    missing_columns: dict[str, tuple[str, ...]] = {}
    rows = _queue_rows(conn, schema, missing_tables, missing_columns)
    clusters = _clusters(
        rows,
        min_cluster_size=min_cluster_size,
        representative_limit=representative_limit,
    )

    return PublishQueueHoldReasonClustersReport(
        artifact_type="publish_queue_hold_reason_clusters",
        generated_at=generated_at.isoformat(),
        filters={
            "active_statuses": list(ACTIVE_STATUSES),
            "min_cluster_size": min_cluster_size,
            "representative_limit": representative_limit,
            "reason_columns": [
                column
                for column in REASON_COLUMNS
                if column in schema.get("publish_queue", set())
            ],
        },
        totals={
            "active_reason_item_count": len(rows),
            "cluster_count": len(clusters),
            "clustered_item_count": sum(cluster.count for cluster in clusters),
            "filtered_item_count": len(rows) - sum(cluster.count for cluster in clusters),
        },
        clusters=tuple(clusters),
        missing_tables=tuple(sorted(missing_tables)),
        missing_columns=missing_columns,
    )


def format_publish_queue_hold_reason_clusters_json(
    report: PublishQueueHoldReasonClustersReport,
) -> str:
    """Serialize hold reason clusters as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publish_queue_hold_reason_clusters_text(
    report: PublishQueueHoldReasonClustersReport,
) -> str:
    """Render a concise terminal report."""
    lines = [
        "Publish Queue Hold Reason Clusters",
        f"Generated: {report.generated_at}",
        (
            f"Minimum cluster size: {report.filters['min_cluster_size']}; "
            f"statuses={', '.join(report.filters['active_statuses'])}"
        ),
        (
            f"Clusters: {report.totals['cluster_count']} "
            f"covering {report.totals['clustered_item_count']} items"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + missing)
    if not report.clusters:
        lines.append("No repeated publish queue hold reasons found.")
        return "\n".join(lines)

    lines.append("Clusters:")
    for cluster in report.clusters:
        lines.append(
            f"- {cluster.normalized_reason}: count={cluster.count} "
            f"platforms={','.join(cluster.platforms)} "
            f"content_types={','.join(cluster.content_types)} "
            f"oldest={cluster.oldest_item_id or '-'}@{cluster.oldest_item_at or '-'} "
            f"newest={cluster.newest_item_id or '-'}@{cluster.newest_item_at or '-'}"
        )
        lines.append(
            "  ids: "
            + ", ".join(str(value) for value in cluster.representative_ids)
        )
    return "\n".join(lines)


def normalize_hold_reason(value: Any) -> str | None:
    """Normalize reason text by removing volatile URLs and identifiers."""
    text = str(value or "").strip()
    if not text:
        return None
    text = VOLATILE_RE.sub(" ", text)
    text = PUNCT_RE.sub(" ", text.casefold())
    normalized = " ".join(text.split())
    return normalized or None


def _queue_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> list[dict[str, Any]]:
    if "publish_queue" not in schema:
        missing_tables.add("publish_queue")
        return []
    columns = schema["publish_queue"]
    required = ("id", "status")
    missing_required = tuple(column for column in required if column not in columns)
    reason_columns = tuple(column for column in REASON_COLUMNS if column in columns)
    if missing_required:
        missing_columns["publish_queue"] = missing_required
        return []
    if not reason_columns:
        missing_columns["publish_queue"] = REASON_COLUMNS
        return []

    joins = ""
    content_type_expr = "'unknown'"
    if "generated_content" in schema and "content_type" in schema["generated_content"]:
        joins = "LEFT JOIN generated_content gc ON gc.id = pq.content_id"
        content_type_expr = "gc.content_type"
    elif "generated_content" not in schema:
        missing_tables.add("generated_content")
    else:
        missing_columns["generated_content"] = ("content_type",)

    reason_expr = "COALESCE(" + ", ".join(f"pq.{column}" for column in reason_columns) + ")"
    cursor = conn.execute(
        f"""SELECT
                  pq.id AS queue_id,
                  {_column_expr(columns, "platform", "'all'", alias="pq")} AS platform,
                  pq.status AS status,
                  {_column_expr(columns, "created_at", "NULL", alias="pq")} AS created_at,
                  {_column_expr(columns, "scheduled_at", "NULL", alias="pq")} AS scheduled_at,
                  {reason_expr} AS reason,
                  {content_type_expr} AS content_type
           FROM publish_queue pq
           {joins}
           WHERE LOWER(pq.status) IN ({", ".join("?" for _ in ACTIVE_STATUSES)})
             AND TRIM(COALESCE({", ".join(f"pq.{column}" for column in reason_columns)}, '')) != ''
           ORDER BY pq.id ASC""",
        ACTIVE_STATUSES,
    )
    return [dict(row) for row in cursor.fetchall()]


def _clusters(
    rows: list[dict[str, Any]],
    *,
    min_cluster_size: int,
    representative_limit: int,
) -> list[PublishQueueHoldReasonCluster]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        normalized_reason = normalize_hold_reason(row.get("reason"))
        if normalized_reason is None:
            continue
        platform = _normalize_label(row.get("platform"))
        content_type = _normalize_label(row.get("content_type"))
        grouped[(normalized_reason, platform, content_type)].append(row)

    clusters: list[PublishQueueHoldReasonCluster] = []
    for (normalized_reason, platform, content_type), cluster_rows in grouped.items():
        if len(cluster_rows) < min_cluster_size:
            continue
        ordered = sorted(cluster_rows, key=_row_sort_key)
        oldest = ordered[0]
        newest = ordered[-1]
        clusters.append(
            PublishQueueHoldReasonCluster(
                normalized_reason=normalized_reason,
                example_reason=_example_reason(cluster_rows),
                count=len(cluster_rows),
                platforms=(platform,),
                content_types=(content_type,),
                representative_ids=tuple(
                    int(row["queue_id"]) for row in ordered[:representative_limit]
                ),
                oldest_item_id=int(oldest["queue_id"]),
                oldest_item_at=_item_timestamp(oldest),
                newest_item_id=int(newest["queue_id"]),
                newest_item_at=_item_timestamp(newest),
            )
        )
    clusters.sort(
        key=lambda cluster: (
            -cluster.count,
            cluster.normalized_reason,
            cluster.platforms,
            cluster.content_types,
        )
    )
    return clusters


def _row_sort_key(row: dict[str, Any]) -> tuple[str, int]:
    return (_item_timestamp(row) or "", int(row["queue_id"]))


def _item_timestamp(row: dict[str, Any]) -> str | None:
    value = row.get("created_at") or row.get("scheduled_at")
    return str(value).strip() if str(value or "").strip() else None


def _example_reason(rows: list[dict[str, Any]]) -> str:
    values = sorted({str(row.get("reason") or "").strip() for row in rows if row.get("reason")})
    return values[0] if values else ""


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    names = [row[0] for row in rows]
    return {
        name: {column[1] for column in conn.execute(f"PRAGMA table_info({name})")}
        for name in names
    }


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str | None = None,
) -> str:
    if column not in columns:
        return fallback
    return f"{alias}.{column}" if alias else column


def _normalize_label(value: Any) -> str:
    return str(value or "unknown").strip().casefold() or "unknown"


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
