"""GitHub issue and pull request label drift reporting."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any


ACTIVITY_TYPES = ("issue", "pull_request")


def build_github_label_drift_report(
    db_or_conn: Any,
    *,
    days: int = 14,
    compare_days: int = 14,
    repo: str | None = None,
    now: datetime | None = None,
    top_n: int = 10,
) -> dict[str, Any]:
    """Build a read-only label drift report for GitHub issues and pull requests."""
    if days <= 0:
        raise ValueError("days must be positive")
    if compare_days <= 0:
        raise ValueError("compare_days must be positive")
    if top_n <= 0:
        raise ValueError("top_n must be positive")

    conn = _connection(db_or_conn)
    now = _aware(now or datetime.now(timezone.utc))
    recent_start = now - timedelta(days=days)
    comparison_start = recent_start - timedelta(days=compare_days)

    rows = _activity_rows(conn, comparison_start, now, repo)
    recent_rows = [
        row for row in rows if recent_start <= _parse_timestamp(row["updated_at"]) < now
    ]
    comparison_rows = [
        row
        for row in rows
        if comparison_start <= _parse_timestamp(row["updated_at"]) < recent_start
    ]

    recent_metrics = _window_metrics(recent_rows, top_n=top_n)
    comparison_metrics = _window_metrics(comparison_rows, top_n=top_n)
    recent_counts = recent_metrics["label_counts"]
    comparison_counts = comparison_metrics["label_counts"]

    disappeared = [
        {
            "label": label,
            "comparison_count": comparison_counts[label],
        }
        for label in sorted(set(comparison_counts) - set(recent_counts))
    ]
    dominant = [
        {
            "label": label,
            "recent_count": recent_counts[label],
            "comparison_count": comparison_counts.get(label, 0),
            "delta": recent_counts[label] - comparison_counts.get(label, 0),
            "recent_share": _share(recent_counts[label], recent_metrics["label_total"]),
            "comparison_share": _share(
                comparison_counts.get(label, 0),
                comparison_metrics["label_total"],
            ),
        }
        for label in sorted(recent_counts)
    ]
    dominant.sort(
        key=lambda item: (
            -item["recent_share"],
            -item["delta"],
            item["label"],
        )
    )

    return {
        "artifact_type": "github_label_drift",
        "generated_at": now.isoformat(),
        "days": days,
        "compare_days": compare_days,
        "filters": {"repo": repo},
        "windows": {
            "recent": {"start": recent_start.isoformat(), "end": now.isoformat()},
            "comparison": {
                "start": comparison_start.isoformat(),
                "end": recent_start.isoformat(),
            },
        },
        "totals": {
            "recent_items": len(recent_rows),
            "comparison_items": len(comparison_rows),
            "label_total": recent_metrics["label_total"],
            "unlabeled_open_items": recent_metrics["unlabeled_open_items"],
            "malformed_label_rows": recent_metrics["malformed_label_rows"],
        },
        "top_labels": _top_labels(recent_counts, recent_metrics["label_total"], top_n),
        "newly_dominant_labels": dominant[:top_n],
        "disappeared_labels": disappeared,
        "by_repo": _group_entries(recent_rows, "repo_name", top_n=top_n),
        "by_activity_type": _group_entries(recent_rows, "activity_type", top_n=top_n),
        "malformed_items": recent_metrics["malformed_items"],
        "unlabeled_open_items": recent_metrics["unlabeled_open_item_refs"],
    }


def format_github_label_drift_json(report: dict[str, Any]) -> str:
    """Render a GitHub label drift report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_github_label_drift_text(report: dict[str, Any]) -> str:
    """Render a stable human-readable GitHub label drift report."""
    lines = [
        "GitHub label drift report",
        f"Generated: {report['generated_at']}",
        (
            "Windows: recent="
            f"{report['days']} days, comparison={report['compare_days']} days"
        ),
        f"Repo: {report['filters']['repo'] or 'all'}",
        (
            "Totals: "
            f"recent_items={report['totals']['recent_items']} "
            f"comparison_items={report['totals']['comparison_items']} "
            f"labels={report['totals']['label_total']} "
            f"unlabeled_open={report['totals']['unlabeled_open_items']} "
            f"malformed={report['totals']['malformed_label_rows']}"
        ),
        "",
        "Top labels:",
    ]
    if report["top_labels"]:
        for label in report["top_labels"]:
            lines.append(
                f"- {label['label']}: {label['count']} ({_format_percent(label['share'])})"
            )
    else:
        lines.append("No labels found in the recent window.")

    lines.extend(["", "Disappeared labels:"])
    if report["disappeared_labels"]:
        for item in report["disappeared_labels"]:
            lines.append(f"- {item['label']}: comparison_count={item['comparison_count']}")
    else:
        lines.append("No labels disappeared from the comparison window.")

    lines.extend(["", "Newly dominant labels:"])
    if report["newly_dominant_labels"]:
        for item in report["newly_dominant_labels"]:
            lines.append(
                "- "
                f"{item['label']}: recent={item['recent_count']} "
                f"comparison={item['comparison_count']} delta={item['delta']} "
                f"recent_share={_format_percent(item['recent_share'])}"
            )
    else:
        lines.append("No recent labels found.")

    lines.extend(["", "By repo:"])
    lines.extend(_format_group_lines(report["by_repo"], "repo"))

    lines.extend(["", "By activity type:"])
    lines.extend(_format_group_lines(report["by_activity_type"], "activity_type"))

    if report["unlabeled_open_items"]:
        lines.extend(["", "Unlabeled open items:"])
        for item in report["unlabeled_open_items"]:
            lines.append(
                f"- {item['repo']} {item['activity_type']} #{item['number']} "
                f"{item['updated_at']}: {item['title']}"
            )

    if report["malformed_items"]:
        lines.extend(["", "Malformed label rows:"])
        for item in report["malformed_items"]:
            lines.append(
                f"- {item['repo']} {item['activity_type']} #{item['number']} "
                f"{item['updated_at']}: {item['reason']}"
            )

    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _activity_rows(
    conn: sqlite3.Connection,
    start: datetime,
    end: datetime,
    repo: str | None,
) -> list[dict[str, Any]]:
    where = [
        "activity_type IN ('issue', 'pull_request')",
        "updated_at >= ?",
        "updated_at < ?",
    ]
    params: list[Any] = [start.isoformat(), end.isoformat()]
    if repo:
        where.append("repo_name = ?")
        params.append(repo)
    cursor = conn.execute(
        f"""SELECT id, repo_name, activity_type, number, title, state, url, updated_at, labels
            FROM github_activity
            WHERE {' AND '.join(where)}
            ORDER BY updated_at ASC, id ASC""",
        tuple(params),
    )
    return [dict(row) for row in cursor.fetchall()]


def _window_metrics(rows: list[dict[str, Any]], *, top_n: int) -> dict[str, Any]:
    label_counts: Counter[str] = Counter()
    malformed_items: list[dict[str, Any]] = []
    unlabeled_open_items: list[dict[str, Any]] = []

    for row in rows:
        labels, malformed_reason = _parse_labels(row.get("labels"))
        if malformed_reason:
            malformed_items.append(_row_ref(row, reason=malformed_reason))
        else:
            label_counts.update(labels)
            if not labels and _state(row) == "open":
                unlabeled_open_items.append(_row_ref(row))

    label_total = sum(label_counts.values())
    return {
        "label_counts": label_counts,
        "label_total": label_total,
        "unlabeled_open_items": len(unlabeled_open_items),
        "unlabeled_open_item_refs": unlabeled_open_items,
        "malformed_label_rows": len(malformed_items),
        "malformed_items": malformed_items[:top_n],
    }


def _group_entries(rows: list[dict[str, Any]], key: str, *, top_n: int) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(key) or "unknown")].append(row)

    entries = []
    for value in sorted(grouped):
        metrics = _window_metrics(grouped[value], top_n=top_n)
        label_counts = metrics["label_counts"]
        label_total = metrics["label_total"]
        top_labels = _top_labels(label_counts, label_total, top_n)
        entries.append(
            {
                key if key != "repo_name" else "repo": value,
                "items": len(grouped[value]),
                "label_total": label_total,
                "unlabeled_open_items": metrics["unlabeled_open_items"],
                "malformed_label_rows": metrics["malformed_label_rows"],
                "top_labels": top_labels,
                "concentration": {
                    "top_label": top_labels[0]["label"] if top_labels else None,
                    "top_label_share": top_labels[0]["share"] if top_labels else 0.0,
                },
            }
        )
    return entries


def _parse_labels(value: Any) -> tuple[list[str], str | None]:
    if value is None or value == "":
        return [], None
    if isinstance(value, list):
        parsed = value
    else:
        try:
            parsed = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return [], "invalid_json"
    if not isinstance(parsed, list):
        return [], "not_a_list"

    labels: list[str] = []
    for item in parsed:
        if not isinstance(item, str):
            return [], "non_string_label"
        label = item.strip()
        if label:
            labels.append(label)
    return sorted(set(labels), key=str.lower), None


def _top_labels(
    label_counts: Counter[str],
    label_total: int,
    top_n: int,
) -> list[dict[str, Any]]:
    rows = [
        {
            "label": label,
            "count": count,
            "share": _share(count, label_total),
        }
        for label, count in label_counts.items()
    ]
    rows.sort(key=lambda row: (-row["count"], row["label"].lower(), row["label"]))
    return rows[:top_n]


def _row_ref(row: dict[str, Any], *, reason: str | None = None) -> dict[str, Any]:
    item = {
        "id": row["id"],
        "repo": row["repo_name"],
        "activity_type": row["activity_type"],
        "number": row["number"],
        "title": row["title"],
        "state": row["state"],
        "url": row["url"],
        "updated_at": row["updated_at"],
    }
    if reason is not None:
        item["reason"] = reason
    return item


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _state(row: dict[str, Any]) -> str:
    return str(row.get("state") or "").strip().lower()


def _share(count: int, total: int) -> float:
    return round(count / total, 4) if total else 0.0


def _format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"


def _format_group_lines(entries: list[dict[str, Any]], label_key: str) -> list[str]:
    if not entries:
        return ["No rows."]
    lines = []
    for entry in entries:
        top = entry["concentration"]["top_label"] or "-"
        share = _format_percent(entry["concentration"]["top_label_share"])
        lines.append(
            f"- {entry[label_key]}: items={entry['items']} labels={entry['label_total']} "
            f"unlabeled_open={entry['unlabeled_open_items']} "
            f"malformed={entry['malformed_label_rows']} top={top} ({share})"
        )
    return lines
