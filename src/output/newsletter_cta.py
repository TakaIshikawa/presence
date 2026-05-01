"""Deterministic newsletter CTA rotation planning."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class CtaCandidate:
    """One newsletter call-to-action candidate."""

    id: str
    label: str
    text: str = ""
    url: str = ""
    campaign_tags: tuple[str, ...] = field(default_factory=tuple)
    cooldown_count: int = 1
    priority_weight: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> "CtaCandidate":
        """Build a validated CTA candidate from JSON/YAML data."""
        if not isinstance(raw, dict):
            raise ValueError("CTA candidate entries must be objects")
        candidate_id = str(raw.get("id") or "").strip()
        if not candidate_id:
            raise ValueError("CTA candidate id is required")
        label = str(raw.get("label") or raw.get("title") or candidate_id).strip()
        tags = _normalize_tags(
            raw.get("campaign_tags", raw.get("campaigns", raw.get("tags", [])))
        )
        cooldown_count = _non_negative_int(
            raw.get("cooldown_count", raw.get("cooldown", 1)),
            field_name=f"{candidate_id}.cooldown_count",
        )
        try:
            priority_weight = float(
                raw.get("priority_weight", raw.get("priority", 0.0)) or 0.0
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{candidate_id}.priority_weight must be numeric") from exc

        metadata = raw.get("metadata") or {}
        if not isinstance(metadata, dict):
            raise ValueError(f"{candidate_id}.metadata must be an object")

        return cls(
            id=candidate_id,
            label=label or candidate_id,
            text=str(raw.get("text") or raw.get("copy") or ""),
            url=str(raw.get("url") or ""),
            campaign_tags=tuple(tags),
            cooldown_count=cooldown_count,
            priority_weight=priority_weight,
            metadata=dict(metadata),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable representation."""
        return {
            "campaign_tags": list(self.campaign_tags),
            "cooldown_count": self.cooldown_count,
            "id": self.id,
            "label": self.label,
            "metadata": self.metadata,
            "priority_weight": self.priority_weight,
            "text": self.text,
            "url": self.url,
        }


@dataclass(frozen=True)
class NewsletterCtaSelection:
    """Selected CTA plus the deterministic rationale used to pick it."""

    selected: CtaCandidate
    requested_campaign_tags: tuple[str, ...]
    recent_cta_ids: tuple[str, ...]
    eligible_candidate_ids: tuple[str, ...]
    blocked_candidate_ids: tuple[str, ...]
    rationale: str
    scores: dict[str, dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable selection payload."""
        return {
            "blocked_candidate_ids": list(self.blocked_candidate_ids),
            "eligible_candidate_ids": list(self.eligible_candidate_ids),
            "rationale": self.rationale,
            "recent_cta_ids": list(self.recent_cta_ids),
            "requested_campaign_tags": list(self.requested_campaign_tags),
            "scores": self.scores,
            "selected": self.selected.to_dict(),
        }


def plan_newsletter_cta(
    candidates: list[CtaCandidate],
    *,
    recent_sends: list[dict[str, Any]] | None = None,
    campaign_tags: list[str] | tuple[str, ...] | None = None,
) -> NewsletterCtaSelection:
    """Select the best eligible CTA with deterministic rotation rules."""
    if not candidates:
        raise ValueError("at least one CTA candidate is required")

    by_id: dict[str, CtaCandidate] = {}
    for candidate in candidates:
        if candidate.id in by_id:
            raise ValueError(f"duplicate CTA candidate id: {candidate.id}")
        by_id[candidate.id] = candidate

    requested_tags = tuple(_normalize_tags(campaign_tags or []))
    recent_ids = tuple(
        cta_id
        for cta_id in (
            extract_cta_id_from_send(send) for send in (recent_sends or [])
        )
        if cta_id
    )

    scores: dict[str, dict[str, Any]] = {}
    eligible: list[CtaCandidate] = []
    blocked: list[str] = []
    for candidate in sorted(candidates, key=lambda item: item.id):
        cooldown_hits = [
            cta_id
            for cta_id in recent_ids[: candidate.cooldown_count]
            if cta_id == candidate.id
        ]
        tag_matches = sorted(set(candidate.campaign_tags).intersection(requested_tags))
        is_blocked = bool(cooldown_hits)
        scores[candidate.id] = {
            "blocked_by_cooldown": is_blocked,
            "campaign_match_count": len(tag_matches),
            "campaign_matches": tag_matches,
            "cooldown_count": candidate.cooldown_count,
            "priority_weight": candidate.priority_weight,
        }
        if is_blocked:
            blocked.append(candidate.id)
        else:
            eligible.append(candidate)

    if not eligible:
        raise ValueError("no CTA candidates are eligible after cooldown filtering")

    selected = sorted(
        eligible,
        key=lambda candidate: (
            -scores[candidate.id]["campaign_match_count"],
            -candidate.priority_weight,
            candidate.id,
        ),
    )[0]
    selected_score = scores[selected.id]
    rationale = (
        f"Selected {selected.id}: "
        f"{selected_score['campaign_match_count']} campaign tag match(es), "
        f"priority weight {selected.priority_weight:g}, "
        f"cooldown window {selected.cooldown_count}."
    )
    if blocked:
        rationale += f" Blocked by cooldown: {', '.join(blocked)}."

    return NewsletterCtaSelection(
        selected=selected,
        requested_campaign_tags=requested_tags,
        recent_cta_ids=recent_ids,
        eligible_candidate_ids=tuple(candidate.id for candidate in eligible),
        blocked_candidate_ids=tuple(blocked),
        rationale=rationale,
        scores=scores,
    )


def fetch_recent_newsletter_sends(
    db_or_conn: Any, limit: int = 10
) -> list[dict[str, Any]]:
    """Read recent newsletter send metadata newest-first."""
    if limit <= 0:
        return []
    conn = _connection(db_or_conn)
    if "newsletter_sends" not in _tables(conn):
        return []
    columns = _columns(conn, "newsletter_sends")
    selected = [
        column
        for column in ("id", "issue_id", "subject", "metadata", "sent_at")
        if column in columns
    ]
    if not selected:
        return []
    order_parts = [
        column
        for column in ("sent_at", "id")
        if column in columns
    ]
    order_clause = ", ".join(f"{column} DESC" for column in order_parts)
    sql = f"SELECT {', '.join(selected)} FROM newsletter_sends"
    if order_clause:
        sql += f" ORDER BY {order_clause}"
    sql += " LIMIT ?"
    return [
        _parse_send_metadata(_row_to_dict(row, selected))
        for row in conn.execute(sql, (limit,)).fetchall()
    ]


def extract_cta_id_from_send(send: dict[str, Any]) -> str | None:
    """Extract a CTA identifier from newsletter send metadata."""
    metadata = send.get("metadata")
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata or "{}")
        except json.JSONDecodeError:
            metadata = {}
    if not isinstance(metadata, dict):
        metadata = {}

    for key in ("cta_id", "newsletter_cta_id", "selected_cta_id"):
        value = metadata.get(key)
        if value:
            return str(value)

    cta = metadata.get("cta") or metadata.get("newsletter_cta")
    if isinstance(cta, dict):
        for key in ("id", "cta_id", "name"):
            value = cta.get(key)
            if value:
                return str(value)
    elif cta:
        return str(cta)

    return None


def selection_to_json(selection: NewsletterCtaSelection) -> str:
    """Render a CTA selection as stable JSON."""
    return json.dumps(selection.to_dict(), indent=2, sort_keys=True)


def selection_to_text(selection: NewsletterCtaSelection) -> str:
    """Render a CTA selection for terminal review."""
    candidate = selection.selected
    lines = [
        f"Selected CTA: {candidate.label} ({candidate.id})",
        f"Rationale: {selection.rationale}",
    ]
    if candidate.text:
        lines.append(f"Text: {candidate.text}")
    if candidate.url:
        lines.append(f"URL: {candidate.url}")
    if selection.requested_campaign_tags:
        lines.append(
            "Campaign tags: " + ", ".join(selection.requested_campaign_tags)
        )
    if selection.recent_cta_ids:
        lines.append("Recent CTA IDs: " + ", ".join(selection.recent_cta_ids))
    return "\n".join(lines)


def _parse_send_metadata(send: dict[str, Any]) -> dict[str, Any]:
    metadata = send.get("metadata")
    if isinstance(metadata, str):
        try:
            send["metadata"] = json.loads(metadata or "{}")
        except json.JSONDecodeError:
            send["metadata"] = {}
    elif metadata is None:
        send["metadata"] = {}
    return send


def _row_to_dict(row: Any, columns: list[str]) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return dict(row)
    return dict(zip(columns, row))


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3 connection or database wrapper with .conn")
    return conn


def _tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        for row in rows
    }


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
        for row in conn.execute(f"PRAGMA table_info({table})")
    }


def _normalize_tags(raw_tags: Any) -> list[str]:
    if raw_tags in (None, ""):
        return []
    if isinstance(raw_tags, str):
        parts = raw_tags.split(",")
    elif isinstance(raw_tags, (list, tuple, set)):
        parts = []
        for tag in raw_tags:
            parts.extend(str(tag).split(","))
    else:
        raise ValueError("campaign tags must be a string or list")
    return sorted({str(tag).strip().lower() for tag in parts if str(tag).strip()})


def _non_negative_int(value: Any, *, field_name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if parsed < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return parsed
