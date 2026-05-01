"""Audit repeated image generation prompts in generated content."""

from __future__ import annotations

import json
import re
import sqlite3
import string
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_SIMILARITY_THRESHOLD = 0.8
TOKEN_RE = re.compile(r"[a-z0-9]+")


def build_image_prompt_reuse_report(
    db_or_conn: Any,
    *,
    days: int = 30,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    limit: int = 20,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only audit report for repeated image prompts."""
    if days <= 0:
        raise ValueError("days must be positive")
    if not 0 <= similarity_threshold <= 1:
        raise ValueError("similarity_threshold must be between 0 and 1")
    if limit <= 0:
        raise ValueError("limit must be positive")

    conn = _connection(db_or_conn)
    now = _aware(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days)
    schema = _schema(conn)
    rows = _image_prompt_rows(conn, schema, cutoff, now)

    exact_findings = _exact_duplicate_findings(rows)
    exact_keys = {finding["normalized_prompt"] for finding in exact_findings}
    near_findings = _near_duplicate_findings(
        [row for row in rows if row["normalized_prompt"] not in exact_keys],
        similarity_threshold,
    )
    findings = sorted(
        [*exact_findings, *near_findings],
        key=lambda item: (
            item["finding_type"] != "exact_duplicate",
            -item["count"],
            -item["max_similarity"],
            item["prompt_preview"],
        ),
    )[:limit]

    return {
        "generated_at": now.isoformat(),
        "window_days": days,
        "window": {
            "start": cutoff.isoformat(),
            "end": now.isoformat(),
        },
        "similarity_threshold": similarity_threshold,
        "limit": limit,
        "totals": {
            "scanned_prompts": len(rows),
            "findings": len(findings),
            "exact_duplicate_groups": sum(
                1 for item in findings if item["finding_type"] == "exact_duplicate"
            ),
            "near_duplicate_groups": sum(
                1 for item in findings if item["finding_type"] == "near_duplicate"
            ),
        },
        "findings": findings,
        "empty_state": {
            "is_empty": not rows,
            "schema_present": "generated_content" in schema,
            "message": (
                "No generated content rows with image_prompt found for the selected window."
                if not rows
                else None
            ),
        },
    }


def format_image_prompt_reuse_json(report: dict[str, Any]) -> str:
    """Render an image prompt reuse report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_image_prompt_reuse_text(report: dict[str, Any]) -> str:
    """Render a stable human-readable image prompt reuse report."""
    lines = [
        "Image prompt reuse audit",
        f"Generated: {report['generated_at']}",
        f"Window: {report['window_days']} days",
        f"Similarity threshold: {report['similarity_threshold']:.2f}",
        (
            "Totals: "
            f"scanned={report['totals']['scanned_prompts']} "
            f"findings={report['totals']['findings']} "
            f"exact={report['totals']['exact_duplicate_groups']} "
            f"near={report['totals']['near_duplicate_groups']}"
        ),
        "",
    ]

    if report["empty_state"]["is_empty"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    if not report["findings"]:
        lines.append("No repeated or near-repeated image prompts found.")
        return "\n".join(lines)

    lines.append("Findings:")
    for index, finding in enumerate(report["findings"], start=1):
        lines.append(
            f"{index}. {finding['finding_type']} count={finding['count']} "
            f"similarity={finding['min_similarity']:.2f}-{finding['max_similarity']:.2f}"
        )
        lines.append(f"   prompt={finding['prompt_preview']}")
        for item in finding["items"]:
            lines.append(
                "   - "
                f"id={item['content_id']} "
                f"type={item['content_type']} "
                f"published={item['published']} "
                f"created_at={item['created_at']} "
                f"image_path={item['image_path'] or '-'}"
            )
    return "\n".join(lines)


def normalize_image_prompt(prompt: str | None) -> str:
    """Normalize an image prompt for exact duplicate detection."""
    if prompt is None:
        return ""
    text = prompt.casefold()
    text = text.translate(str.maketrans({char: " " for char in string.punctuation}))
    return " ".join(text.split())


def token_overlap_similarity(left: str | None, right: str | None) -> float:
    """Return Jaccard token-overlap similarity for two prompts."""
    left_tokens = _tokens(left)
    right_tokens = _tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _image_prompt_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    content_columns = schema.get("generated_content", set())
    if not {"id", "image_prompt"}.issubset(content_columns):
        return []

    content_type_expr = (
        "gc.content_type" if "content_type" in content_columns else "'unknown'"
    )
    image_path_expr = "gc.image_path" if "image_path" in content_columns else "NULL"
    created_at_expr = "gc.created_at" if "created_at" in content_columns else "NULL"
    published_expr = "gc.published" if "published" in content_columns else "0"
    filters = ["gc.image_prompt IS NOT NULL", "TRIM(gc.image_prompt) != ''"]
    params: list[Any] = []
    if "created_at" in content_columns:
        filters.append("gc.created_at >= ?")
        filters.append("gc.created_at < ?")
        params.extend([cutoff.isoformat(), now.isoformat()])

    raw_rows = conn.execute(
        f"""SELECT gc.id AS content_id,
                  {content_type_expr} AS content_type,
                  {image_path_expr} AS image_path,
                  gc.image_prompt AS image_prompt,
                  {created_at_expr} AS created_at,
                  {published_expr} AS published
           FROM generated_content gc
           WHERE {' AND '.join(filters)}
           ORDER BY created_at ASC, gc.id ASC""",
        params,
    ).fetchall()

    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        created_at = _parse_timestamp(row["created_at"]) or now
        if created_at < cutoff or created_at > now:
            continue
        normalized = normalize_image_prompt(row["image_prompt"])
        if not normalized:
            continue
        rows.append(
            {
                "content_id": int(row["content_id"]),
                "content_type": row["content_type"] or "unknown",
                "image_path": row["image_path"],
                "image_prompt": row["image_prompt"],
                "normalized_prompt": normalized,
                "tokens": _tokens(normalized),
                "created_at": created_at.isoformat(),
                "published": _published_status(row["published"]),
            }
        )
    return rows


def _exact_duplicate_findings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(row["normalized_prompt"], []).append(row)

    findings = []
    for normalized_prompt, items in groups.items():
        if len(items) < 2:
            continue
        findings.append(
            {
                "finding_type": "exact_duplicate",
                "normalized_prompt": normalized_prompt,
                "prompt_preview": _preview(items[0]["image_prompt"]),
                "count": len(items),
                "min_similarity": 1.0,
                "max_similarity": 1.0,
                "items": [_finding_item(item) for item in items],
            }
        )
    return findings


def _near_duplicate_findings(
    rows: list[dict[str, Any]],
    threshold: float,
) -> list[dict[str, Any]]:
    if len(rows) < 2:
        return []

    parent = list(range(len(rows)))
    similarities: dict[tuple[int, int], float] = {}
    for left_index, left in enumerate(rows):
        for right_index in range(left_index + 1, len(rows)):
            right = rows[right_index]
            similarity = _token_set_similarity(left["tokens"], right["tokens"])
            if similarity >= threshold:
                _union(parent, left_index, right_index)
                similarities[(left_index, right_index)] = similarity

    components: dict[int, list[int]] = {}
    for index in range(len(rows)):
        components.setdefault(_find(parent, index), []).append(index)

    findings = []
    for indexes in components.values():
        if len(indexes) < 2:
            continue
        pair_scores = [
            score
            for (left_index, right_index), score in similarities.items()
            if left_index in indexes and right_index in indexes
        ]
        if not pair_scores:
            continue
        items = [rows[index] for index in indexes]
        items.sort(key=lambda item: (item["created_at"], item["content_id"]))
        findings.append(
            {
                "finding_type": "near_duplicate",
                "normalized_prompt": None,
                "prompt_preview": _preview(items[0]["image_prompt"]),
                "count": len(items),
                "min_similarity": round(min(pair_scores), 4),
                "max_similarity": round(max(pair_scores), 4),
                "items": [_finding_item(item) for item in items],
            }
        )
    return findings


def _finding_item(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "content_id": row["content_id"],
        "content_type": row["content_type"],
        "created_at": row["created_at"],
        "published": row["published"],
        "image_path": row["image_path"],
        "image_prompt": row["image_prompt"],
    }


def _tokens(prompt: str | None) -> set[str]:
    return set(TOKEN_RE.findall(normalize_image_prompt(prompt)))


def _token_set_similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _published_status(value: Any) -> str:
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        numeric = 0
    if numeric == 1:
        return "published"
    if numeric == -1:
        return "abandoned"
    return "unpublished"


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _preview(value: str, limit: int = 100) -> str:
    text = " ".join(str(value).split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _find(parent: list[int], index: int) -> int:
    while parent[index] != index:
        parent[index] = parent[parent[index]]
        index = parent[index]
    return index


def _union(parent: list[int], left: int, right: int) -> None:
    left_root = _find(parent, left)
    right_root = _find(parent, right)
    if left_root != right_root:
        parent[right_root] = left_root
