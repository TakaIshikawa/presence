"""Report near-duplicate generated candidates within generation groups."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_SIMILARITY_THRESHOLD = 0.82
DEFAULT_LIMIT = 100
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOPWORDS = {"about", "after", "again", "because", "from", "have", "into", "that", "the", "this", "with", "your", "you", "and", "for"}


def build_generation_candidate_novelty_report(
    candidate_rows: list[dict[str, Any]],
    *,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if not 0 <= similarity_threshold <= 1:
        raise ValueError("similarity_threshold must be between 0 and 1")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    candidates = [_normalize_candidate(row) for row in candidate_rows]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        grouped[candidate["group_key"]].append(candidate)

    groups = []
    duplicate_pairs = []
    for group_key, rows in grouped.items():
        pairs = []
        for index, left in enumerate(rows):
            for right in rows[index + 1 :]:
                similarity = _similarity(left["tokens"], right["tokens"])
                if similarity >= similarity_threshold:
                    pair = {
                        "group_key": group_key,
                        "left_candidate_id": left["id"],
                        "right_candidate_id": right["id"],
                        "similarity": round(similarity, 4),
                        "left_preview": left["text"][:120],
                        "right_preview": right["text"][:120],
                    }
                    pairs.append(pair)
                    duplicate_pairs.append(pair)
        groups.append(
            {
                "group_key": group_key,
                "candidate_count": len(rows),
                "near_duplicate_pair_count": len(pairs),
                "novelty_status": "single_candidate" if len(rows) == 1 else "near_duplicates" if pairs else "diverse",
                "candidate_ids": [row["id"] for row in rows],
            }
        )
    groups.sort(key=lambda item: (-item["near_duplicate_pair_count"], -item["candidate_count"], item["group_key"]))
    duplicate_pairs.sort(key=lambda item: (-item["similarity"], item["group_key"], item["left_candidate_id"]))
    return {
        "artifact_type": "generation_candidate_novelty",
        "generated_at": generated_at.isoformat(),
        "filters": {"similarity_threshold": similarity_threshold, "limit": limit},
        "totals": {
            "group_count": len(groups),
            "candidate_count": len(candidates),
            "near_duplicate_pair_count": len(duplicate_pairs),
            "near_duplicate_group_count": sum(1 for group in groups if group["novelty_status"] == "near_duplicates"),
            "single_candidate_group_count": sum(1 for group in groups if group["novelty_status"] == "single_candidate"),
            "diverse_group_count": sum(1 for group in groups if group["novelty_status"] == "diverse"),
        },
        "groups": groups[:limit],
        "near_duplicate_pairs": duplicate_pairs[:limit],
        "empty_state": {
            "is_empty": not candidates,
            "message": "No generation candidates found." if not candidates else None,
        },
    }


def build_generation_candidate_novelty_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    return build_generation_candidate_novelty_report(_load_candidates(conn, _schema(conn)), **kwargs)


def format_generation_candidate_novelty_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_generation_candidate_novelty_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Generation Candidate Novelty",
        f"Generated: {report['generated_at']}",
        f"Filters: similarity_threshold={report['filters']['similarity_threshold']:g} limit={report['filters']['limit']}",
        (
            "Totals: "
            f"groups={totals['group_count']} candidates={totals['candidate_count']} "
            f"near_pairs={totals['near_duplicate_pair_count']} duplicate_groups={totals['near_duplicate_group_count']}"
        ),
    ]
    if not report["groups"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "Groups:", "status            candidates  pairs  group"])
    for group in report["groups"]:
        lines.append(
            f"{group['novelty_status']:<16}  {group['candidate_count']:>10}  "
            f"{group['near_duplicate_pair_count']:>5}  {group['group_key']}"
        )
    return "\n".join(lines)


def _load_candidates(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    for table in ("generation_candidates", "generated_candidates"):
        columns = schema.get(table, set())
        if {"id", "text"}.issubset(columns) or {"id", "content"}.issubset(columns):
            selected = [
                "id",
                _select(columns, ("generation_run_id", "run_id", "batch_id"), "generation_run_id"),
                _select(columns, ("source_content_id", "source_item_id", "source_id"), "source_item_id"),
                _select(columns, ("text", "content", "candidate_text"), "text"),
            ]
            return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]
    columns = schema.get("generated_content", set())
    if not {"id", "content"}.issubset(columns):
        return []
    selected = [
        "id",
        _select(columns, ("generation_run_id", "run_id", "batch_id"), "generation_run_id"),
        _select(columns, ("source_content_id", "source_item_id", "source_activity_ids", "source_commits"), "source_item_id"),
        "content AS text",
        "published" if "published" in columns else "0 AS published",
    ]
    where = "WHERE COALESCE(published, 0) = 0" if "published" in columns else ""
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content {where}").fetchall()]


def _normalize_candidate(row: dict[str, Any]) -> dict[str, Any]:
    text = _text(row.get("text") or row.get("content") or row.get("candidate_text"))
    group_key = _text(row.get("generation_run_id") or row.get("run_id") or row.get("batch_id"))
    if not group_key:
        group_key = "source:" + _text(row.get("source_item_id") or row.get("source_content_id") or row.get("source_id") or "ungrouped")
    return {
        "id": _text(row.get("id")),
        "group_key": group_key,
        "text": text,
        "tokens": _tokens(text),
    }


def _similarity(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    union = left | right
    return len(left & right) / len(union) if union else 0.0


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate if candidate == alias else f"{candidate} AS {alias}"
    return f"NULL AS {alias}"


def _tokens(value: str) -> set[str]:
    return {token for token in _TOKEN_RE.findall(value.lower()) if len(token) > 2 and token not in _STOPWORDS}


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()
