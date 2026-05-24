"""Export content claim evidence packets for review handoff."""

from __future__ import annotations

import json
from typing import Any

from evaluation._batch_report_utils import connection, dump_json, first_table, pick, redact, schema, text

DEFAULT_LIMIT = 100


def build_content_claim_evidence_packet_export_from_db(db_or_conn: Any, *, verdict: str | None = None, include_pending: bool = False, limit: int = DEFAULT_LIMIT) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    conn = connection(db_or_conn)
    sch = schema(conn)
    table = first_table(sch, ("content_claim_checks", "content_claims"))
    missing_tables = [] if table else ["content_claim_checks|content_claims"]
    claims = _claims(conn, table, sch[table]) if table else []
    evidence = _evidence(conn, sch)
    packets = []
    for claim in claims:
        claim_verdict = text(claim["verdict"]).lower() or "pending"
        if verdict and claim_verdict != verdict.lower():
            continue
        if not include_pending and claim_verdict == "pending":
            continue
        packets.append({"claim_id": claim["claim_id"], "content_id": claim["content_id"], "claim_text": claim["claim_text"], "evidence_items": evidence.get(str(claim["claim_id"]), []), "reviewer_notes": redact(claim["reviewer_notes"]), "verdict": claim_verdict, "packet_warnings": [] if evidence.get(str(claim["claim_id"])) else ["missing_evidence_items"]})
    packets.sort(key=lambda p: str(p["claim_id"]))
    return {"artifact_type": "content_claim_evidence_packet_export", "filters": {"verdict": verdict, "include_pending": include_pending, "limit": limit}, "packets": packets[:limit], "missing_tables": missing_tables, "empty_state": {"is_empty": not packets, "message": "No content claim evidence packets found." if not packets and not missing_tables else None}}


def build_content_claim_evidence_packet_export(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return build_content_claim_evidence_packet_export_from_db(*args, **kwargs)


def format_content_claim_evidence_packet_export_json(export: dict[str, Any]) -> str:
    return dump_json(export)


def format_content_claim_evidence_packet_export_jsonl(export: dict[str, Any]) -> str:
    return "\n".join(json.dumps(packet, sort_keys=True) for packet in export["packets"]) + ("\n" if export["packets"] else "")


def _claims(conn, table: str, cols: set[str]) -> list[dict[str, Any]]:
    verdict_expr = pick(cols, "verdict", "status", default="'pending'")
    select = [f"{pick(cols, 'id', 'claim_id', default='rowid')} AS claim_id", f"{pick(cols, 'content_id', default='NULL')} AS content_id", f"{pick(cols, 'claim_text', 'text', default='NULL')} AS claim_text", f"{verdict_expr} AS verdict", f"{pick(cols, 'reviewer_notes', 'notes', default='NULL')} AS reviewer_notes"]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY claim_id ASC")]


def _evidence(conn, sch: dict[str, set[str]]) -> dict[str, list[dict[str, Any]]]:
    table = first_table(sch, ("content_claim_evidence", "claim_evidence"))
    if not table:
        return {}
    cols = sch[table]
    if not {"claim_id"} <= cols:
        return {}
    select = [f"{pick(cols, 'claim_id')} AS claim_id", f"{pick(cols, 'url', 'evidence_url', default='NULL')} AS url", f"{pick(cols, 'snippet', 'extracted_snippet', default='NULL')} AS snippet", f"{pick(cols, 'label', 'support_label', default='NULL')} AS label"]
    out: dict[str, list[dict[str, Any]]] = {}
    for row in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY claim_id ASC"):
        out.setdefault(str(row["claim_id"]), []).append({"url": row["url"], "snippet": redact(row["snippet"]), "label": row["label"]})
    return out
