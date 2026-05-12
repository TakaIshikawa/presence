"""Watch generated content for drift from the configured author voice."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_SEVERITY = 1
DEFAULT_STALE_PATTERNS = (
    "game changer",
    "unlock",
    "delve",
    "in today's fast-paced",
    "not just",
)


@dataclass(frozen=True)
class PersonaDriftWatchlistItem:
    """One generated or published item with persona drift signals."""

    content_id: int
    content_type: str
    created_at: str | None
    published: bool
    severity: int
    overlap_score: float
    repeated_phrases: tuple[str, ...]
    stale_pattern_hits: tuple[str, ...]
    reason_codes: tuple[str, ...]
    example: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["repeated_phrases"] = list(self.repeated_phrases)
        payload["stale_pattern_hits"] = list(self.stale_pattern_hits)
        payload["reason_codes"] = list(self.reason_codes)
        return payload


@dataclass(frozen=True)
class PersonaDriftWatchlistReport:
    """Persona drift watchlist report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    items: tuple[PersonaDriftWatchlistItem, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "persona_drift_watchlist",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "item_count": len(self.items),
            "items": [item.to_dict() for item in self.items],
            "totals": dict(self.totals),
        }


def build_persona_drift_watchlist_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_severity: int = DEFAULT_MIN_SEVERITY,
    overlap_threshold: float = 0.34,
    stale_patterns: tuple[str, ...] = DEFAULT_STALE_PATTERNS,
    voice_terms: tuple[str, ...] = (),
    now: datetime | None = None,
) -> PersonaDriftWatchlistReport:
    """Build a read-only watchlist of generated content with persona drift signals."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_severity < 0:
        raise ValueError("min_severity must be non-negative")
    if overlap_threshold < 0 or overlap_threshold > 1:
        raise ValueError("overlap_threshold must be between 0 and 1")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    rows = _load_rows(_connection(db_or_conn), cutoff)
    items = [
        item
        for item in (
            _score_row(
                row,
                overlap_threshold=overlap_threshold,
                stale_patterns=stale_patterns,
                voice_terms=voice_terms,
            )
            for row in rows
        )
        if item.severity >= min_severity
    ]
    items.sort(key=lambda item: (-item.severity, item.created_at or "", item.content_id))
    return PersonaDriftWatchlistReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "cutoff": cutoff.isoformat(),
            "min_severity": min_severity,
            "overlap_threshold": overlap_threshold,
        },
        totals={
            "candidates_scanned": len(rows),
            "watchlist_count": len(items),
            "published_count": sum(1 for item in items if item.published),
        },
        items=tuple(items),
    )


def format_persona_drift_watchlist_json(report: PersonaDriftWatchlistReport) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_persona_drift_watchlist_text(report: PersonaDriftWatchlistReport) -> str:
    """Render the watchlist for review."""
    lines = [
        "Persona Drift Watchlist",
        f"Generated: {report.generated_at}",
        (
            f"Window: days={report.filters['days']} "
            f"min_severity={report.filters['min_severity']} "
            f"overlap_threshold={report.filters['overlap_threshold']}"
        ),
        (
            f"Totals: scanned={report.totals['candidates_scanned']} "
            f"watchlist={report.totals['watchlist_count']}"
        ),
    ]
    if not report.items:
        lines.extend(["", "No persona drift candidates found."])
        return "\n".join(lines)
    lines.extend(["", "Candidates:"])
    for item in report.items:
        lines.append(
            f"- content_id={item.content_id} type={item.content_type} "
            f"severity={item.severity} overlap={item.overlap_score:.3f} "
            f"reasons={','.join(item.reason_codes)}"
        )
        if item.stale_pattern_hits:
            lines.append(f"  stale_patterns={', '.join(item.stale_pattern_hits)}")
        if item.repeated_phrases:
            lines.append(f"  repeated_phrases={', '.join(item.repeated_phrases)}")
        lines.append(f"  example={item.example}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, cutoff: datetime) -> list[sqlite3.Row]:
    tables = {row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "generated_content" not in tables:
        return []
    guard_join = ""
    guard_cols = "NULL AS guard_reasons, NULL AS guard_metrics, NULL AS guard_score"
    if "content_persona_guard" in tables:
        guard_join = "LEFT JOIN content_persona_guard cpg ON cpg.content_id = gc.id"
        guard_cols = "cpg.reasons AS guard_reasons, cpg.metrics AS guard_metrics, cpg.score AS guard_score"
    return conn.execute(
        f"""SELECT gc.id, gc.content_type, gc.content, gc.eval_feedback, gc.published,
                  gc.created_at, {guard_cols}
             FROM generated_content gc
             {guard_join}
             WHERE datetime(gc.created_at) >= datetime(?)
             ORDER BY datetime(gc.created_at) DESC, gc.id DESC""",
        (cutoff.isoformat(),),
    ).fetchall()


def _score_row(
    row: sqlite3.Row,
    *,
    overlap_threshold: float,
    stale_patterns: tuple[str, ...],
    voice_terms: tuple[str, ...],
) -> PersonaDriftWatchlistItem:
    content = str(row["content"] or "")
    tokens = _tokens(content)
    repeated = _repeated_phrases(tokens)
    stale_hits = tuple(pattern for pattern in stale_patterns if pattern.lower() in content.lower())
    metrics = _json_obj(row["guard_metrics"])
    overlap = _overlap_score(tokens, voice_terms, metrics)
    reason_codes: list[str] = []
    severity = 0
    if overlap >= overlap_threshold:
        reason_codes.append("high_voice_overlap")
        severity += 35 + round((overlap - overlap_threshold) * 40)
    if repeated:
        reason_codes.append("repeated_phrasing")
        severity += 18 + min(12, len(repeated) * 3)
    if stale_hits:
        reason_codes.append("stale_pattern")
        severity += 22 + min(16, len(stale_hits) * 4)
    guard_reasons = _reason_codes(row["guard_reasons"])
    if guard_reasons:
        reason_codes.extend(f"guard_{reason}" for reason in guard_reasons)
        severity += 18
    score = row["guard_score"]
    if score is not None and float(score) < 0.45:
        reason_codes.append("low_persona_guard_score")
        severity += round((0.45 - float(score)) * 40)
    return PersonaDriftWatchlistItem(
        content_id=int(row["id"]),
        content_type=str(row["content_type"] or "unknown"),
        created_at=row["created_at"],
        published=bool(row["published"]),
        severity=min(100, severity),
        overlap_score=round(overlap, 3),
        repeated_phrases=repeated,
        stale_pattern_hits=tuple(sorted(stale_hits)),
        reason_codes=tuple(dict.fromkeys(reason_codes)),
        example=_shorten(content),
    )


def _overlap_score(tokens: list[str], voice_terms: tuple[str, ...], metrics: dict[str, Any]) -> float:
    for key in ("overlap_score", "voice_overlap", "persona_overlap"):
        if key in metrics:
            try:
                return max(0.0, min(1.0, float(metrics[key])))
            except (TypeError, ValueError):
                pass
    if not tokens or not voice_terms:
        return 0.0
    voice = set(_tokens(" ".join(voice_terms)))
    return len(set(tokens) & voice) / max(1, len(voice))


def _repeated_phrases(tokens: list[str]) -> tuple[str, ...]:
    phrases: dict[str, int] = {}
    for size in (2, 3):
        for idx in range(0, max(0, len(tokens) - size + 1)):
            phrase = " ".join(tokens[idx : idx + size])
            phrases[phrase] = phrases.get(phrase, 0) + 1
    return tuple(sorted(phrase for phrase, count in phrases.items() if count > 1)[:5])


def _reason_codes(value: Any) -> list[str]:
    parsed = _json(value)
    if isinstance(parsed, list):
        return [str(item) for item in parsed if str(item)]
    if isinstance(parsed, dict):
        code = parsed.get("code") or parsed.get("reason")
        return [str(code)] if code else []
    if value:
        return [str(value)]
    return []


def _json_obj(value: Any) -> dict[str, Any]:
    parsed = _json(value)
    return parsed if isinstance(parsed, dict) else {}


def _json(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return value


def _tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def _shorten(text: str, limit: int = 120) -> str:
    one_line = " ".join(text.split())
    return one_line if len(one_line) <= limit else one_line[: limit - 3] + "..."


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
