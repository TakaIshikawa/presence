"""Select and apply evaluated newsletter subject candidates."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Literal


SelectionMode = Literal["best", "explicit"]

SENT_NEWSLETTER_STATUSES = {"sent", "resonated", "low_resonance"}


def list_candidates_for_send(db_or_conn: Any, send_id: int) -> list[dict[str, Any]]:
    """Return subject candidates attached to one newsletter send."""
    conn = _connection(db_or_conn)
    cursor = conn.execute(
        """SELECT id, newsletter_send_id, issue_id, subject, score, rationale,
                  source, rank, selected, source_content_ids, week_start, week_end,
                  metadata, created_at
           FROM newsletter_subject_candidates
           WHERE newsletter_send_id = ?
           ORDER BY score DESC, rank ASC, subject COLLATE NOCASE ASC, id ASC""",
        (send_id,),
    )
    return [_candidate_dict(row) for row in cursor.fetchall()]


def select_candidate_for_send(
    db_or_conn: Any,
    *,
    send_id: int,
    candidate_id: int | None = None,
    best: bool = False,
) -> dict[str, Any]:
    """Select a candidate by explicit id or deterministic best score."""
    if candidate_id is None and not best:
        raise ValueError("either candidate_id or best must be provided")
    if candidate_id is not None and best:
        raise ValueError("candidate_id and best are mutually exclusive")

    candidates = list_candidates_for_send(db_or_conn, send_id)
    if candidate_id is not None:
        for candidate in candidates:
            if candidate["id"] == candidate_id:
                return candidate
        raise ValueError(f"candidate {candidate_id} does not belong to send {send_id}")

    eligible = [candidate for candidate in candidates if not candidate["rejected"]]
    if not eligible:
        raise ValueError(f"send {send_id} has no non-rejected subject candidates")
    return sorted(eligible, key=_best_sort_key)[0]


def apply_newsletter_subject_selection(
    db_or_conn: Any,
    *,
    send_id: int,
    candidate_id: int | None = None,
    best: bool = False,
    dry_run: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return an audit report and optionally apply the chosen subject."""
    if send_id <= 0:
        raise ValueError("send_id must be positive")

    conn = _connection(db_or_conn)
    generated_at = _timestamp(now)
    send = _get_send(conn, send_id)
    if send is None:
        raise ValueError(f"newsletter send not found: {send_id}")

    mode: SelectionMode = "explicit" if candidate_id is not None else "best"
    candidates = list_candidates_for_send(conn, send_id)
    report = {
        "generated_at": generated_at,
        "send_id": send_id,
        "mode": mode,
        "dry_run": dry_run,
        "applied": False,
        "status": "pending",
        "reason": None,
        "send": send,
        "selected_candidate": None,
        "proposed_subject": None,
        "candidate_count": len(candidates),
        "candidates": candidates,
    }

    if _send_is_sent(send):
        report.update(
            {
                "status": "blocked",
                "reason": "newsletter_already_sent",
            }
        )
        return report

    try:
        selected = select_candidate_for_send(
            conn,
            send_id=send_id,
            candidate_id=candidate_id,
            best=best,
        )
    except ValueError as exc:
        report.update({"status": "blocked", "reason": str(exc)})
        return report

    if selected["rejected"]:
        report.update(
            {
                "status": "blocked",
                "reason": "candidate_rejected",
                "selected_candidate": selected,
                "proposed_subject": selected["subject"],
            }
        )
        return report

    report.update(
        {
            "selected_candidate": selected,
            "proposed_subject": selected["subject"],
            "status": "dry_run" if dry_run else "applied",
        }
    )

    if dry_run:
        return report

    conn.execute(
        "UPDATE newsletter_sends SET subject = ? WHERE id = ?",
        (selected["subject"], send_id),
    )
    conn.execute(
        "UPDATE newsletter_subject_candidates SET selected = CASE WHEN id = ? THEN 1 ELSE 0 END WHERE newsletter_send_id = ?",
        (selected["id"], send_id),
    )
    conn.commit()
    report["applied"] = True
    report["send"] = _get_send(conn, send_id)
    return report


def format_newsletter_subject_selection_json(report: dict[str, Any]) -> str:
    """Format a selection report as stable JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_subject_selection_text(report: dict[str, Any]) -> str:
    """Render a newsletter subject selection report for terminal output."""
    lines = [
        "Newsletter subject selection",
        f"Generated: {report['generated_at']}",
        f"Send: {report['send_id']}",
        f"Mode: {report['mode']}",
        f"Status: {report['status']}",
    ]
    if report.get("reason"):
        lines.append(f"Reason: {report['reason']}")

    selected = report.get("selected_candidate")
    if selected:
        lines.extend(
            [
                f"Current subject: {report['send'].get('subject') or '-'}",
                f"Proposed subject: {report.get('proposed_subject') or '-'}",
                (
                    "Candidate: "
                    f"{selected['id']} score={selected['score']:.2f} "
                    f"rank={selected.get('rank') or '-'}"
                ),
            ]
        )
    else:
        lines.append(f"Candidates: {report['candidate_count']}")

    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _get_send(conn: sqlite3.Connection, send_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        """SELECT id, issue_id, subject, status, sent_at, subscriber_count, metadata
           FROM newsletter_sends
           WHERE id = ?""",
        (send_id,),
    ).fetchone()
    if row is None:
        return None
    item = dict(row)
    try:
        item["metadata"] = json.loads(item.get("metadata") or "{}")
    except (TypeError, json.JSONDecodeError):
        item["metadata"] = {}
    return item


def _candidate_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    try:
        item["source_content_ids"] = json.loads(item.get("source_content_ids") or "[]")
    except (TypeError, json.JSONDecodeError):
        item["source_content_ids"] = []
    try:
        item["metadata"] = json.loads(item.get("metadata") or "{}")
    except (TypeError, json.JSONDecodeError):
        item["metadata"] = {}
    item["selected"] = bool(item.get("selected"))
    item["score"] = float(item.get("score") or 0.0)
    item["rejected"] = _candidate_rejected(item)
    return item


def _candidate_rejected(candidate: dict[str, Any]) -> bool:
    metadata = candidate.get("metadata") or {}
    status = str(metadata.get("status") or metadata.get("decision") or "").lower()
    return bool(
        metadata.get("rejected")
        or metadata.get("reject_reason")
        or status == "rejected"
    )


def _best_sort_key(candidate: dict[str, Any]) -> tuple[float, int, str, int]:
    rank = candidate.get("rank")
    return (
        -float(candidate.get("score") or 0.0),
        int(rank) if rank is not None else 10**9,
        str(candidate.get("subject") or "").lower(),
        int(candidate["id"]),
    )


def _send_is_sent(send: dict[str, Any]) -> bool:
    status = str(send.get("status") or "").lower()
    return status in SENT_NEWSLETTER_STATUSES or bool(send.get("sent_at"))


def _timestamp(now: datetime | None) -> str:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()
