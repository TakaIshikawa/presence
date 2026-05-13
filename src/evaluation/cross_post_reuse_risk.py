"""Detect near-identical generated content reused across platforms."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import difflib
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_MIN_SIMILARITY = 0.9


@dataclass(frozen=True)
class CrossPostReuseRisk:
    left_content_id: int
    right_content_id: int
    left_platform: str | None
    right_platform: str | None
    left_content_type: str | None
    right_content_type: str | None
    similarity_score: float
    age_delta_hours: float | None
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CrossPostReuseRiskReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    risky_pairs: tuple[CrossPostReuseRisk, ...]
    empty_state: dict[str, Any]
    missing_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "cross_post_reuse_risk",
            "empty_state": dict(self.empty_state),
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "risky_pairs": [pair.to_dict() for pair in self.risky_pairs],
            "totals": dict(self.totals),
        }


def build_cross_post_reuse_risk_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_similarity: float = DEFAULT_MIN_SIMILARITY,
    platform: str | None = None,
    now: datetime | None = None,
) -> CrossPostReuseRiskReport:
    if days <= 0:
        raise ValueError("days must be positive")
    if min_similarity < 0 or min_similarity > 1:
        raise ValueError("min_similarity must be between 0 and 1")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "lookback_start": cutoff.isoformat(),
        "min_similarity": min_similarity,
        "platform": platform,
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "generated_content" not in schema:
        return _report(generated_at, filters, (), 0, ("generated_content",))

    rows = _load_generated_content(conn, schema["generated_content"], cutoff.isoformat(), platform)
    risks: list[CrossPostReuseRisk] = []
    for idx, left in enumerate(rows):
        for right in rows[idx + 1 :]:
            if left["id"] == right["id"]:
                continue
            if left.get("platform") == right.get("platform") and left.get("content_type") == right.get("content_type"):
                continue
            score = _similarity(left["normalized"], right["normalized"])
            if score < min_similarity:
                continue
            risks.append(
                CrossPostReuseRisk(
                    left_content_id=int(left["id"]),
                    right_content_id=int(right["id"]),
                    left_platform=left.get("platform"),
                    right_platform=right.get("platform"),
                    left_content_type=left.get("content_type"),
                    right_content_type=right.get("content_type"),
                    similarity_score=round(score, 4),
                    age_delta_hours=_age_delta_hours(left.get("created_at_dt"), right.get("created_at_dt")),
                    recommended_action="Rewrite reused copy for platform norms, length, CTA, and audience context.",
                )
            )
    risks.sort(key=lambda item: (-item.similarity_score, item.left_content_id, item.right_content_id))
    return _report(generated_at, filters, tuple(risks), len(rows), ())


def format_cross_post_reuse_risk_json(report: CrossPostReuseRiskReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_cross_post_reuse_risk_text(report: CrossPostReuseRiskReport) -> str:
    lines = [
        "Cross-Post Reuse Risk",
        f"Window: {report.filters['days']} days; platform={report.filters.get('platform') or 'all'}; min_similarity={report.filters['min_similarity']}",
        f"Scanned: {report.totals['content_scanned']}; risky_pairs={report.totals['risky_pair_count']}",
        "",
    ]
    if not report.risky_pairs:
        lines.append(report.empty_state["message"])
        return "\n".join(lines)
    for pair in report.risky_pairs:
        lines.append(
            f"- content={pair.left_content_id}->{pair.right_content_id} "
            f"{pair.left_platform or '-'}:{pair.left_content_type or '-'} -> "
            f"{pair.right_platform or '-'}:{pair.right_content_type or '-'} "
            f"similarity={pair.similarity_score:.3f} age_delta_hours={pair.age_delta_hours}"
        )
        lines.append(f"  action={pair.recommended_action}")
    return "\n".join(lines)


def _load_generated_content(
    conn: sqlite3.Connection,
    columns: set[str],
    cutoff: str,
    platform: str | None,
) -> list[dict[str, Any]]:
    if "id" not in columns:
        return []
    text_col = _first(columns, ("content", "text", "body", "generated_text", "output"))
    if not text_col:
        return []
    created_col = _first(columns, ("created_at", "generated_at", "updated_at"))
    platform_col = _first(columns, ("platform", "target_platform", "channel"))
    type_col = _first(columns, ("content_type", "type", "format"))
    title_col = _first(columns, ("title", "headline", "subject"))
    select = [
        "id",
        f"{text_col} AS content_text",
        f"{created_col} AS created_at" if created_col else "NULL AS created_at",
        f"{platform_col} AS platform" if platform_col else "NULL AS platform",
        f"{type_col} AS content_type" if type_col else "NULL AS content_type",
        f"{title_col} AS title" if title_col else "NULL AS title",
    ]
    where: list[str] = []
    params: list[Any] = []
    if created_col:
        where.append(f"{created_col} >= ?")
        params.append(cutoff)
    if platform and platform_col:
        where.append(f"{platform_col} = ?")
        params.append(platform)
    sql = f"SELECT {', '.join(select)} FROM generated_content"
    if where:
        sql += " WHERE " + " AND ".join(where)
    rows = []
    for row in conn.execute(sql, params).fetchall():
        item = dict(row)
        normalized = _normalize(" ".join(str(item.get(key) or "") for key in ("title", "content_text")))
        if not normalized:
            continue
        item["normalized"] = normalized
        item["created_at_dt"] = _parse_ts(item.get("created_at"))
        rows.append(item)
    return rows


def _report(
    generated_at: datetime,
    filters: dict[str, Any],
    risks: tuple[CrossPostReuseRisk, ...],
    scanned: int,
    missing_tables: tuple[str, ...],
) -> CrossPostReuseRiskReport:
    return CrossPostReuseRiskReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={"content_scanned": scanned, "risky_pair_count": len(risks)},
        risky_pairs=risks,
        empty_state={
            "is_empty": not risks,
            "message": "No cross-post reuse risks found." if not missing_tables else "Generated content schema is unavailable.",
        },
        missing_tables=missing_tables,
    )


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _first(columns: set[str], names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in columns), None)


def _normalize(text: str) -> str:
    text = re.sub(r"https?://\S+", " ", text.casefold())
    text = re.sub(r"[@#]\w+", " ", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _similarity(left: str, right: str) -> float:
    return difflib.SequenceMatcher(None, left, right).ratio()


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _age_delta_hours(left: datetime | None, right: datetime | None) -> float | None:
    if left is None or right is None:
        return None
    return round(abs((right - left).total_seconds()) / 3600, 2)
