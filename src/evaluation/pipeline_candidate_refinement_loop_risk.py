"""Find pipeline candidates stuck in repeated refinement loops."""
from __future__ import annotations
from collections import defaultdict
from typing import Any
from ._batch_report_common import *

ARTIFACT_TYPE = "pipeline_candidate_refinement_loop_risk"
DEFAULT_LIMIT = 50
DEFAULT_MIN_ATTEMPTS = 3


def build_pipeline_candidate_refinement_loop_risk_report(rows: list[dict[str, Any]], *, min_attempts: int = DEFAULT_MIN_ATTEMPTS, limit: int = DEFAULT_LIMIT, missing_tables=None, missing_columns=None, now=None):
    positive("min_attempts", min_attempts); positive("limit", limit)
    by_candidate = defaultdict(list)
    for r in rows:
        by_candidate[clean(r.get("candidate_id") or r.get("content_id") or r.get("run_id") or r.get("id"))].append(r)
    findings = []
    for cid, items in by_candidate.items():
        items.sort(key=lambda r: clean(r.get("created_at") or r.get("evaluated_at") or r.get("attempted_at")))
        attempts = sum(1 for r in items if lower(r.get("event_type") or r.get("stage") or r.get("kind")) in {"refinement", "refine", "evaluation", "gate", "final_gate"} or r.get("score") is not None)
        scores = [to_float(r.get("score"), None) for r in items if r.get("score") not in (None, "")]
        scores = [s for s in scores if s is not None]
        failures = sum(1 for r in items if "fail" in lower(r.get("status") or r.get("gate_result") or r.get("result")))
        reasons = []
        if attempts >= min_attempts: reasons.append("repeated_refinement_attempts")
        score_delta = round((scores[-1] - scores[0]), 4) if len(scores) >= 2 else 0.0
        if len(scores) >= 2 and score_delta <= 0: reasons.append("non_improving_scores")
        if failures >= 2: reasons.append("repeated_final_gate_failures")
        for reason in reasons:
            findings.append({"candidate_id": cid, "run_id": clean(items[-1].get("run_id")), "attempt_count": attempts, "score_delta": score_delta, "loop_reason": reason, "recommended_action": "stop automatic refinement and route candidate for manual review"})
    order = {"repeated_final_gate_failures": 0, "non_improving_scores": 1, "repeated_refinement_attempts": 2}
    findings.sort(key=lambda f: (order.get(f["loop_reason"], 9), -f["attempt_count"], f["candidate_id"]))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_iso(now), "filters": {"min_attempts": min_attempts, "limit": limit}, "summary": {"candidate_count": len(by_candidate), "finding_count": len(findings), "shown": len(shown)}, "refinement_loop_risks": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}, "empty_state": empty_state(findings, "No pipeline candidate refinement loop risks found.", schema_gap=bool(missing_tables or missing_columns))}


def build_pipeline_candidate_refinement_loop_risk_report_from_db(db_or_conn: Any, **kw):
    conn = connection(db_or_conn); s = schema(conn); mt = []; mc = {}; rows = []
    tables = [t for t in ("candidate_refinement_attempts", "candidate_evaluations", "pipeline_gate_results", "generated_content", "pipeline_runs") if t in s]
    if not tables: mt.append("pipeline_runs|generated_content|candidate_evaluations|candidate_refinement_attempts|pipeline_gate_results")
    for t in tables:
        c = s[t]
        if not ({"candidate_id", "content_id", "run_id", "id"} & c): mc[t] = ["candidate_id|content_id|run_id|id"]
        rows += load_table(conn, t, c, {"id": ("id",), "candidate_id": ("candidate_id", "content_id", "id"), "content_id": ("content_id",), "run_id": ("run_id", "pipeline_run_id"), "event_type": ("event_type", "kind", "stage"), "stage": ("stage",), "status": ("status",), "gate_result": ("gate_result", "result"), "score": ("score", "quality_score", "evaluation_score"), "created_at": ("created_at", "started_at"), "evaluated_at": ("evaluated_at",), "attempted_at": ("attempted_at",)})
    return build_pipeline_candidate_refinement_loop_risk_report(rows, missing_tables=mt, missing_columns=mc, **kw)


def format_pipeline_candidate_refinement_loop_risk_json(r): return json_dumps(r)
def format_pipeline_candidate_refinement_loop_risk_text(r):
    s = r["summary"]; lines = ["Pipeline Candidate Refinement Loop Risk", f"Generated: {r['generated_at']}", f"Totals: candidates={s['candidate_count']} findings={s['finding_count']} shown={s['shown']}"]
    if r["missing_tables"]: lines.append("Missing tables: " + ", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: " + flatten_missing(r["missing_columns"]))
    if not r["refinement_loop_risks"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines += ["", "candidate_id | attempt_count | score_delta | loop_reason"]
    for f in r["refinement_loop_risks"]: lines.append(f"{f['candidate_id']} | {f['attempt_count']} | {f['score_delta']} | {f['loop_reason']}")
    return "\n".join(lines)
