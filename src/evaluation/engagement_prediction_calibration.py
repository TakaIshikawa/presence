"""Compare predicted engagement scores against actual engagement outcomes.

Queries published content and engagement data to assess prediction calibration,
detect drift, and compute correlation between predictions and outcomes.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any


BUCKET_RANGES = [
    ("0.0-0.2", 0.0, 0.2),
    ("0.2-0.4", 0.2, 0.4),
    ("0.4-0.6", 0.4, 0.6),
    ("0.6-0.8", 0.6, 0.8),
    ("0.8-1.0", 0.8, 1.0),
]


def _pearson(x_list: list[float], y_list: list[float]) -> float | None:
    n = len(x_list)
    if n < 3:
        return None
    mean_x = sum(x_list) / n
    mean_y = sum(y_list) / n
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(x_list, y_list))
    std_x = math.sqrt(sum((x - mean_x) ** 2 for x in x_list))
    std_y = math.sqrt(sum((y - mean_y) ** 2 for y in y_list))
    if std_x == 0 or std_y == 0:
        return None
    return round(cov / (std_x * std_y), 4)


def _query_matched_posts(db, days: int) -> list[tuple[float, int, int, int, str]]:
    """Return (eval_score, likes, retweets, replies, created_at) for matched posts."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    rows = db.execute(
        """
        SELECT gc.eval_score, pe.likes, pe.retweets, pe.replies, gc.created_at
        FROM generated_content gc
        JOIN post_engagement pe ON gc.id = pe.content_id
        WHERE gc.published = 1 AND gc.created_at >= ?
        """,
        (cutoff,),
    ).fetchall()
    return rows


def _count_published(db, days: int) -> int:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    row = db.execute(
        "SELECT COUNT(*) FROM generated_content WHERE published = 1 AND created_at >= ?",
        (cutoff,),
    ).fetchone()
    return row[0] if row else 0


def _build_buckets(
    rows: list[tuple[float, int, int, int, str]],
) -> dict[str, dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for label, lo, hi in BUCKET_RANGES:
        buckets[label] = {"count": 0, "avg_predicted": 0.0, "avg_actual_engagement": 0.0}

    bucket_data: dict[str, list[tuple[float, float]]] = {label: [] for label, _, _ in BUCKET_RANGES}

    for eval_score, likes, retweets, replies, _created_at in rows:
        total_engagement = float(likes + retweets + replies)
        for label, lo, hi in BUCKET_RANGES:
            if lo <= eval_score < hi or (hi == 1.0 and eval_score == 1.0):
                bucket_data[label].append((eval_score, total_engagement))
                break

    for label, _lo, _hi in BUCKET_RANGES:
        items = bucket_data[label]
        count = len(items)
        buckets[label]["count"] = count
        if count > 0:
            buckets[label]["avg_predicted"] = round(
                sum(s for s, _ in items) / count, 4
            )
            buckets[label]["avg_actual_engagement"] = round(
                sum(e for _, e in items) / count, 4
            )

    return buckets


def _detect_drift(
    full_rows: list[tuple[float, int, int, int, str]],
    days: int,
) -> tuple[bool, list[str]]:
    """Compare recent 7-day bucket averages against full-period averages."""
    recent_cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    recent_rows = [r for r in full_rows if r[4] >= recent_cutoff]

    full_buckets = _build_buckets(full_rows)
    recent_buckets = _build_buckets(recent_rows)

    drift_detected = False
    drift_details: list[str] = []

    for label, _, _ in BUCKET_RANGES:
        full_avg = full_buckets[label]["avg_actual_engagement"]
        recent_avg = recent_buckets[label]["avg_actual_engagement"]
        full_count = full_buckets[label]["count"]
        recent_count = recent_buckets[label]["count"]

        if full_count == 0 or recent_count == 0:
            continue

        if full_avg == 0:
            if recent_avg > 0:
                drift_detected = True
                drift_details.append(
                    f"Bucket {label}: full-period avg 0.0 vs recent avg {recent_avg}"
                )
            continue

        pct_diff = abs(recent_avg - full_avg) / full_avg
        if pct_diff > 0.20:
            drift_detected = True
            drift_details.append(
                f"Bucket {label}: full-period avg {full_avg} vs recent avg {recent_avg} ({pct_diff:.0%} difference)"
            )

    return drift_detected, drift_details


def _calibration_quality(pearson: float | None, drift_detected: bool) -> str:
    if pearson is not None and abs(pearson) >= 0.6 and not drift_detected:
        return "good"
    if pearson is not None and abs(pearson) >= 0.3:
        return "moderate"
    if pearson is None and not drift_detected:
        return "moderate"
    return "poor"


def generate_prediction_calibration_report(
    db, days: int = 30
) -> dict[str, Any]:
    """Generate a report comparing predicted engagement scores to actuals.

    Args:
        db: A sqlite3 connection object.
        days: Number of days of history to analyze.

    Returns:
        Dict with calibration buckets, correlation, drift info, and quality.
    """
    total_published = _count_published(db, days)
    matched_rows = _query_matched_posts(db, days)
    matched_count = len(matched_rows)

    buckets = _build_buckets(matched_rows)

    # Pearson correlation
    if matched_rows:
        x_list = [r[0] for r in matched_rows]
        y_list = [float(r[1] + r[2] + r[3]) for r in matched_rows]
        pearson = _pearson(x_list, y_list)
    else:
        pearson = None

    drift_detected, drift_details = _detect_drift(matched_rows, days)

    quality = _calibration_quality(pearson, drift_detected)

    return {
        "total_published": total_published,
        "matched_with_engagement": matched_count,
        "calibration_buckets": buckets,
        "pearson_correlation": pearson,
        "drift_detected": drift_detected,
        "drift_details": drift_details,
        "calibration_quality": quality,
    }
