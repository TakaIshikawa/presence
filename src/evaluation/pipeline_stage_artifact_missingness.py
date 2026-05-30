"""Detect missing or malformed pipeline stage artifacts."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from ._report_utils import clean, connection, expr, json_dumps, now_iso, schema, positive, to_int


ARTIFACT_TYPE = "pipeline_stage_artifact_missingness"
DEFAULT_LIMIT = 50


def build_pipeline_stage_artifact_missingness_report(stage_rows: list[dict[str, Any]], artifact_rows: list[dict[str, Any]], *, limit: int = DEFAULT_LIMIT, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None, now: Any = None) -> dict[str, Any]:
    positive("limit", limit)
    artifacts_by_stage: dict[str, list[dict[str, Any]]] = defaultdict(list); findings = []
    stage_ids = {clean(r.get("stage_id")) for r in stage_rows}
    for a in artifact_rows:
        sid = clean(a.get("stage_id")); artifacts_by_stage[sid].append(a)
        if sid and sid not in stage_ids: findings.append(_finding(None, a, "orphan_artifact"))
    for st in stage_rows:
        sid = clean(st.get("stage_id")); expected = [x.strip() for x in clean(st.get("expected_artifact_types")).replace(";", ",").split(",") if x.strip()]
        by_type = Counter(clean(a.get("artifact_type")) for a in artifacts_by_stage.get(sid, []))
        for typ in expected:
            if by_type[typ] == 0: findings.append(_finding(st, None, "missing_expected_artifact", typ))
        for a in artifacts_by_stage.get(sid, []):
            if (to_int(a.get("byte_size")) or 0) <= 0: findings.append(_finding(st, a, "zero_byte_payload"))
        for typ, count in by_type.items():
            if typ and count > 1: findings.append(_finding(st, None, "duplicate_artifact_type", typ, count))
    findings.sort(key=lambda r: (r["issue_type"], r["run_id"] or "", r["stage_id"] or "", r["artifact_type"] or ""))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_iso(now), "thresholds": {"limit": limit}, "summary": {"stage_count": len(stage_rows), "artifact_count": len(artifact_rows), "finding_count": len(findings), "shown_count": len(shown)}, "findings": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}}


def build_pipeline_stage_artifact_missingness_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); s = schema(conn); missing_tables = [t for t in ("pipeline_stages", "pipeline_artifacts") if t not in s]; missing_columns = {}; stages = []; artifacts = []
    if "pipeline_stages" in s:
        c = s["pipeline_stages"]; req = {"id", "run_id"}
        if not req.issubset(c): missing_columns["pipeline_stages"] = sorted(req - c)
        else: stages = [dict(r) for r in conn.execute(f"SELECT id AS stage_id, run_id, {expr(c,'stage_name','name',default='NULL',out='stage_name')}, {expr(c,'expected_artifact_types','expected_artifacts',default='NULL',out='expected_artifact_types')} FROM pipeline_stages ORDER BY rowid")]
    if "pipeline_artifacts" in s:
        c = s["pipeline_artifacts"]; req = {"stage_id"}
        if not req.issubset(c): missing_columns["pipeline_artifacts"] = sorted(req - c)
        else: artifacts = [dict(r) for r in conn.execute(f"SELECT {expr(c,'id','artifact_id',default='rowid',out='artifact_id')}, stage_id, {expr(c,'run_id',default='NULL',out='run_id')}, {expr(c,'artifact_type','type',default='NULL',out='artifact_type')}, {expr(c,'byte_size','size_bytes','payload_size',default='NULL',out='byte_size')} FROM pipeline_artifacts ORDER BY rowid")]
    return build_pipeline_stage_artifact_missingness_report(stages, artifacts, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_pipeline_stage_artifact_missingness_json(report: dict[str, Any]) -> str: return json_dumps(report)


def format_pipeline_stage_artifact_missingness_text(report: dict[str, Any]) -> str:
    lines = ["Pipeline Stage Artifact Missingness", f"Generated: {report['generated_at']}", f"Totals: stages={report['summary']['stage_count']} artifacts={report['summary']['artifact_count']} findings={report['summary']['finding_count']}"]
    if report["missing_tables"]: lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]: lines.append("No pipeline stage artifact missingness found."); return "\n".join(lines)
    for r in report["findings"]: lines.append(f"- run={r['run_id'] or '-'} stage={r['stage_id'] or '-'} type={r['issue_type']} artifact={r['artifact_type'] or '-'}")
    return "\n".join(lines)


def _finding(stage: dict[str, Any] | None, artifact: dict[str, Any] | None, issue: str, artifact_type: str | None = None, count: int | None = None) -> dict[str, Any]:
    return {"run_id": clean((stage or artifact or {}).get("run_id")) or None, "stage_id": clean((stage or artifact or {}).get("stage_id")) or None, "artifact_id": clean((artifact or {}).get("artifact_id")) or None, "artifact_type": artifact_type or clean((artifact or {}).get("artifact_type")) or None, "issue_type": issue, "count": count}
