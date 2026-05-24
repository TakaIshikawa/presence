"""Export QA packets for reply drafts awaiting review."""

from __future__ import annotations

import json
from typing import Any

from evaluation._batch_report_utils import connection, dump_json, first_table, json_load, pick, schema, text

DEFAULT_LIMIT = 100


def build_reply_draft_qa_packet_export_from_db(db_or_conn: Any, *, platform: str | None = None, status: str = "pending", limit: int = DEFAULT_LIMIT) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    conn = connection(db_or_conn)
    sch = schema(conn)
    table = first_table(sch, ("reply_drafts", "reply_queue"))
    missing_tables = [] if table else ["reply_drafts|reply_queue"]
    diagnostics = []
    rows = _load(conn, table, sch[table]) if table else []
    citations = _citations(conn, sch, diagnostics)
    packets = []
    for row in rows:
        if platform and text(row["platform"]).lower() != platform.lower():
            continue
        if status and text(row["status"]).lower() != status.lower():
            continue
        meta = json_load(row.get("metadata"))
        packets.append({"draft_id": row["draft_id"], "platform": row["platform"], "text": row["text"], "source_context": row["source_context"], "citations": citations.get(str(row["draft_id"]), []), "persona": row["persona"] or (meta.get("persona") if isinstance(meta, dict) else None), "risk_flags": meta.get("risk_flags", []) if isinstance(meta, dict) else [], "checklist": {"tone_reviewed": False, "claims_checked": False, "privacy_checked": False}, "packet_diagnostics": diagnostics})
    packets.sort(key=lambda p: str(p["draft_id"]))
    return {"artifact_type": "reply_draft_qa_packet_export", "filters": {"platform": platform, "status": status, "limit": limit}, "packets": packets[:limit], "diagnostics": diagnostics, "missing_tables": missing_tables, "empty_state": {"is_empty": not packets, "message": "No reply draft QA packets found." if not packets and not missing_tables else None}}


def build_reply_draft_qa_packet_export(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return build_reply_draft_qa_packet_export_from_db(*args, **kwargs)


def format_reply_draft_qa_packet_export_json(export: dict[str, Any]) -> str:
    return dump_json(export)


def format_reply_draft_qa_packet_export_jsonl(export: dict[str, Any]) -> str:
    return "\n".join(json.dumps(p, sort_keys=True) for p in export["packets"]) + ("\n" if export["packets"] else "")


def _load(conn, table: str, cols: set[str]) -> list[dict[str, Any]]:
    text_expr = pick(cols, "text", "draft_text", "body", default="NULL")
    status_expr = pick(cols, "status", "review_status", default="'pending'")
    select = [f"{pick(cols, 'id', default='rowid')} AS draft_id", f"{pick(cols, 'platform', default='NULL')} AS platform", f"{status_expr} AS status", f"{text_expr} AS text", f"{pick(cols, 'source_context', 'context', default='NULL')} AS source_context", f"{pick(cols, 'persona', 'persona_id', default='NULL')} AS persona", f"{pick(cols, 'metadata', default='NULL')} AS metadata"]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY draft_id ASC")]


def _citations(conn, sch: dict[str, set[str]], diagnostics: list[str]) -> dict[str, list[dict[str, Any]]]:
    table = first_table(sch, ("reply_draft_citations", "reply_knowledge_links"))
    if not table:
        diagnostics.append("missing optional citation table")
        return {}
    cols = sch[table]
    draft_col = pick(cols, "draft_id", "reply_draft_id", "reply_id", default="")
    if not draft_col:
        diagnostics.append("citation table missing draft id")
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    for row in conn.execute(f"SELECT {draft_col} AS draft_id, {pick(cols, 'url', 'source_url', default='NULL')} AS url, {pick(cols, 'title', default='NULL')} AS title FROM {table}"):
        out.setdefault(str(row["draft_id"]), []).append({"url": row["url"], "title": row["title"]})
    return out
