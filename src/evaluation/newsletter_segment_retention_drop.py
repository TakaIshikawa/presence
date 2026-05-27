"""Flag newsletter segments with unusually high recent subscriber loss."""
from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from typing import Any

from ._batch_report_common import *


ARTIFACT_TYPE = "newsletter_segment_retention_drop"
DEFAULT_WINDOW_DAYS = 7
DEFAULT_BASELINE_DAYS = 28
DEFAULT_LIMIT = 50
DEFAULT_MIN_LOSS_RATE = 0.02
DEFAULT_RATIO = 1.5


def build_newsletter_segment_retention_drop_report(
    rows: list[dict[str, Any]],
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    baseline_days: int = DEFAULT_BASELINE_DAYS,
    limit: int = DEFAULT_LIMIT,
    min_loss_rate: float = DEFAULT_MIN_LOSS_RATE,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    now=None,
) -> dict[str, Any]:
    positive("window_days", window_days)
    positive("baseline_days", baseline_days)
    positive("limit", limit)
    non_negative("min_loss_rate", min_loss_rate)
    gen = now_value(now)
    recent_cut = gen - timedelta(days=window_days)
    base_cut = recent_cut - timedelta(days=baseline_days)
    buckets: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {"subscribers": set(), "subscriber_count": 0, "recent_loss": 0, "baseline_loss": 0}
    )
    for row in rows:
        sid = clean(row.get("segment_id") or row.get("segment") or row.get("tag") or row.get("status"), "unknown")
        name = clean(row.get("segment_name") or row.get("segment") or row.get("tag") or row.get("status"), sid)
        bucket = buckets[(sid, name)]
        key = clean(row.get("subscriber_id") or row.get("email") or row.get("subscriber_key"))
        if key:
            bucket["subscribers"].add(key)
        bucket["subscriber_count"] += _first_int(row, ("subscriber_count", "active_count", "audience_count", "segment_size"))
        loss_count = _first_int(row, ("loss_count", "unsubscribe_count", "unsubscribes", "churn_count", "churned_count"))
        loss_at = dt(row.get("unsubscribed_at") or row.get("churned_at") or row.get("canceled_at") or row.get("cancelled_at") or row.get("lost_at"))
        status = lower(row.get("status") or row.get("subscriber_status"))
        if loss_at:
            if recent_cut <= loss_at <= gen:
                bucket["recent_loss"] += max(1, loss_count)
            elif base_cut <= loss_at < recent_cut:
                bucket["baseline_loss"] += max(1, loss_count)
        elif loss_count:
            bucket["recent_loss"] += loss_count
        elif status in {"unsubscribed", "churned", "cancelled", "canceled"}:
            bucket["recent_loss"] += 1
    drops = []
    for (sid, name), values in buckets.items():
        subscriber_count = values["subscriber_count"] or len(values["subscribers"])
        if subscriber_count <= 0:
            continue
        recent_loss = int(values["recent_loss"])
        baseline_loss = int(values["baseline_loss"])
        loss_rate = round(recent_loss / subscriber_count, 4)
        expected = baseline_loss * (window_days / baseline_days) if baseline_days else 0
        expected_rate = round(expected / subscriber_count, 4) if subscriber_count else 0.0
        ratio = round(loss_rate / expected_rate, 4) if expected_rate else (float("inf") if recent_loss else None)
        if recent_loss and (loss_rate >= min_loss_rate or ratio == float("inf") or (ratio is not None and ratio >= DEFAULT_RATIO)):
            drops.append(
                {
                    "segment_id": sid,
                    "segment_name": name,
                    "subscriber_count": subscriber_count,
                    "recent_loss_count": recent_loss,
                    "baseline_loss_count": baseline_loss,
                    "loss_rate": loss_rate,
                    "expected_loss_rate": expected_rate,
                    "loss_rate_ratio": ratio,
                    "recommended_action": "review recent sends, acquisition source, and opt-out reasons for this segment",
                }
            )
    drops.sort(key=lambda item: (-item["loss_rate"], str(item["segment_id"])))
    shown = drops[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": gen.isoformat(),
        "filters": {
            "window_days": window_days,
            "baseline_days": baseline_days,
            "limit": limit,
            "min_loss_rate": min_loss_rate,
        },
        "summary": {
            "segments": len(buckets),
            "drop_count": len(drops),
            "shown": len(shown),
            "subscriber_total": sum((v["subscriber_count"] or len(v["subscribers"])) for v in buckets.values()),
            "recent_loss_total": sum(v["recent_loss"] for v in buckets.values()),
            "baseline_loss_total": sum(v["baseline_loss"] for v in buckets.values()),
        },
        "drops": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())},
        "empty_state": empty_state(drops, "No newsletter segment retention drops found.", schema_gap=bool(missing_tables or missing_columns)),
    }


def build_newsletter_segment_retention_drop_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    sch = schema(conn)
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    rows: list[dict[str, Any]] = []
    if "newsletter_subscribers" not in sch:
        return build_newsletter_segment_retention_drop_report([], missing_tables=["newsletter_subscribers"], **kwargs)
    cols = sch["newsletter_subscribers"]
    if not ({"id", "subscriber_id", "email"} & cols):
        missing_columns["newsletter_subscribers"] = ["id|subscriber_id|email"]
    if not ({"unsubscribed_at", "churned_at", "canceled_at", "cancelled_at", "status"} & cols):
        missing_columns.setdefault("newsletter_subscribers", []).append("unsubscribed_at|churned_at|status")
    if "newsletter_subscriber_segments" in sch:
        join_cols = sch["newsletter_subscriber_segments"]
        seg_cols = sch.get("newsletter_segments", set())
        if not ({"subscriber_id", "email"} & join_cols):
            missing_columns["newsletter_subscriber_segments"] = ["subscriber_id|email"]
        if not ({"segment_id", "segment", "tag"} & join_cols):
            missing_columns.setdefault("newsletter_subscriber_segments", []).append("segment_id|segment|tag")
        if not missing_columns:
            sub_key = "email" if "email" in join_cols else "id" if "id" in cols else "subscriber_id"
            join_key = "email" if "email" in join_cols else "subscriber_id"
            seg_expr = "ss.segment_id" if "segment_id" in join_cols else "ss.segment" if "segment" in join_cols else "ss.tag"
            loss_expr = _coalesce(cols, "unsubscribed_at", "churned_at", "canceled_at", "cancelled_at", default="NULL")
            status_expr = "n.status" if "status" in cols else "NULL"
            name_col = next((c for c in ("name", "segment_name", "label") if c in seg_cols), None)
            name_expr = f"COALESCE(ns.{name_col}, {seg_expr})" if name_col else seg_expr
            join_seg = ""
            if "newsletter_segments" in sch and ({"id", "segment_id"} & seg_cols) and "segment_id" in join_cols:
                key = "id" if "id" in seg_cols else "segment_id"
                join_seg = f" LEFT JOIN newsletter_segments ns ON ns.{key}=ss.segment_id"
            rows = [
                dict(row)
                for row in conn.execute(
                    f"SELECT n.{sub_key} AS subscriber_id, {seg_expr} AS segment_id, {name_expr} AS segment_name, "
                    f"{loss_expr} AS unsubscribed_at, {status_expr} AS status FROM newsletter_subscribers n "
                    f"JOIN newsletter_subscriber_segments ss ON ss.{join_key}=n.{sub_key}{join_seg}"
                )
            ]
    else:
        missing_tables.append("newsletter_subscriber_segments|newsletter_segments")
        if {"segment_id", "segment", "tag", "status"} & cols and not missing_columns:
            rows = load_table(
                conn,
                "newsletter_subscribers",
                cols,
                {
                    "subscriber_id": ("id", "subscriber_id", "email"),
                    "segment_id": ("segment_id", "segment", "tag", "status"),
                    "segment_name": ("segment_name", "segment", "tag", "status"),
                    "unsubscribed_at": ("unsubscribed_at", "churned_at", "canceled_at", "cancelled_at"),
                    "status": ("status",),
                },
            )
        elif not missing_columns:
            missing_columns["newsletter_subscribers"] = ["segment_id|segment|tag|status"]
    return build_newsletter_segment_retention_drop_report(
        rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs
    )


def format_newsletter_segment_retention_drop_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_newsletter_segment_retention_drop_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Newsletter Segment Retention Drop",
        f"Generated: {report['generated_at']}",
        f"Totals: segments={summary['segments']} drops={summary['drop_count']} subscribers={summary['subscriber_total']} recent_loss={summary['recent_loss_total']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["drops"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines += ["", "segment_id | segment_name | subscribers | recent_loss | baseline_loss | loss_rate | expected_rate"]
    for item in report["drops"]:
        lines.append(
            f"{item['segment_id']} | {item['segment_name']} | {item['subscriber_count']} | "
            f"{item['recent_loss_count']} | {item['baseline_loss_count']} | {item['loss_rate']} | {item['expected_loss_rate']}"
        )
    return "\n".join(lines)


def _first_int(row: dict[str, Any], names: tuple[str, ...]) -> int:
    for name in names:
        if row.get(name) not in (None, ""):
            return to_int(row.get(name), 0)
    return 0


def _coalesce(cols: set[str], *names: str, default: str = "NULL") -> str:
    found = [f"n.{name}" for name in names if name in cols]
    return f"COALESCE({', '.join(found)})" if len(found) > 1 else found[0] if found else default
