"""Report newsletter subject candidate rank and selection gaps."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any

from ._report_helpers import base_report, clean, col, connection, json_format, missing, schema, text_format, to_float, to_int, truthy, utc


ARTIFACT_TYPE = "newsletter_subject_candidate_rank_gaps"
DEFAULT_SCORE_GAP_THRESHOLD = 0.1
DEFAULT_LIMIT = 100
REASONS = ("duplicate_rank", "missing_selected_candidate", "selected_low_rank", "send_subject_mismatch")


def build_newsletter_subject_candidate_rank_gaps_report(rows: list[dict[str, Any]], *, score_gap_threshold: float = DEFAULT_SCORE_GAP_THRESHOLD, limit: int = DEFAULT_LIMIT, now: datetime | None = None, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None) -> dict[str, Any]:
    if score_gap_threshold < 0:
        raise ValueError("score_gap_threshold must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = utc(now)
    findings: list[dict[str, Any]] = []
    groups: defaultdict[tuple[Any, Any], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[(row.get("issue_id"), row.get("newsletter_send_id"))].append(row)
    for key, items in groups.items():
        ranks: defaultdict[int, list[dict[str, Any]]] = defaultdict(list)
        for item in items:
            rank = to_int(item.get("rank"))
            if rank is not None:
                ranks[rank].append(item)
        for rank, ranked in ranks.items():
            if len(ranked) > 1:
                findings.append({"reason": "duplicate_rank", "issue_id": key[0], "newsletter_send_id": key[1], "rank": rank, "detail": "multiple candidates share rank"})
        selected = [item for item in items if truthy(item.get("selected"))]
        if not selected:
            findings.append({"reason": "missing_selected_candidate", "issue_id": key[0], "newsletter_send_id": key[1], "detail": "candidate group has no selected row"})
            continue
        best_score = max(to_float(item.get("score")) or 0 for item in items)
        for item in selected:
            rank = to_int(item.get("rank")) or 0
            score = to_float(item.get("score")) or 0
            if rank > 1 and best_score - score > score_gap_threshold:
                findings.append({"reason": "selected_low_rank", "id": item.get("id"), "issue_id": key[0], "newsletter_send_id": key[1], "detail": "selected candidate is low rank with material score gap"})
            send_subject = clean(item.get("send_subject"))
            if send_subject and clean(item.get("subject")) != send_subject:
                findings.append({"reason": "send_subject_mismatch", "id": item.get("id"), "issue_id": key[0], "newsletter_send_id": key[1], "detail": "selected candidate subject differs from newsletter_sends.subject"})
    findings.sort(key=lambda f: (REASONS.index(f["reason"]), str(f.get("newsletter_send_id")), str(f.get("id"))))
    return base_report(artifact_type=ARTIFACT_TYPE, generated_at=generated_at, filters={"score_gap_threshold": score_gap_threshold, "limit": limit}, rows_count=len(rows), findings=findings, reasons=REASONS, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def build_newsletter_subject_candidate_rank_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    db_schema = schema(conn)
    required = {"newsletter_subject_candidates": {"id", "subject"}}
    missing_tables, missing_columns = missing(db_schema, required)
    if missing_tables or missing_columns:
        return build_newsletter_subject_candidate_rank_gaps_report([], missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)
    nsc = db_schema["newsletter_subject_candidates"]
    join = ""
    send_subject = "NULL AS send_subject"
    if "newsletter_sends" in db_schema and "id" in db_schema["newsletter_sends"]:
        join = "LEFT JOIN newsletter_sends ns ON ns.id = nsc.newsletter_send_id"
        send_subject = col(db_schema["newsletter_sends"], "subject", "ns") + " AS send_subject"
    select = ["nsc.id", col(nsc, "newsletter_send_id", "nsc") + " AS newsletter_send_id", col(nsc, "issue_id", "nsc") + " AS issue_id", "nsc.subject", col(nsc, "score", "nsc") + " AS score", col(nsc, "rank", "nsc") + " AS rank", col(nsc, "selected", "nsc", "0") + " AS selected", send_subject]
    rows = [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM newsletter_subject_candidates nsc {join} ORDER BY nsc.rowid").fetchall()]
    return build_newsletter_subject_candidate_rank_gaps_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_newsletter_subject_candidate_rank_gaps_json(report: dict[str, Any]) -> str:
    return json_format(report)


def format_newsletter_subject_candidate_rank_gaps_text(report: dict[str, Any]) -> str:
    return text_format("Newsletter Subject Candidate Rank Gaps", report)
