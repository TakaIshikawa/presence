"""Find repeated model calls that could benefit from prompt caching."""
from __future__ import annotations
import hashlib
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any
from ._batch_report_common import *

ARTIFACT_TYPE = "model_usage_cache_savings_opportunities"
DEFAULT_LIMIT = 50
DEFAULT_LOOKBACK_DAYS = 30


def _norm_prompt(r: dict[str, Any]) -> str:
    raw = clean(r.get("prompt_hash") or r.get("template") or r.get("template_id") or r.get("prompt") or r.get("input"))
    return " ".join(raw.lower().split())


def build_model_usage_cache_savings_opportunities_report(rows: list[dict[str, Any]], *, now: datetime | None = None, lookback_days: int = DEFAULT_LOOKBACK_DAYS, limit: int = DEFAULT_LIMIT, missing_tables=None, missing_columns=None):
    positive("lookback_days", lookback_days); positive("limit", limit); gen = now_value(now); cutoff = gen - timedelta(days=lookback_days)
    groups = defaultdict(list)
    for r in rows:
        created = dt(r.get("created_at") or r.get("started_at"))
        if created and created < cutoff: continue
        norm = _norm_prompt(r)
        if not norm: continue
        key = (hashlib.sha256(norm.encode()).hexdigest()[:16], clean(r.get("model")), clean(r.get("provider")), clean(r.get("task_type") or r.get("task")))
        groups[key].append(r)
    repeated = []
    for (ph, model, provider, task), items in groups.items():
        if len(items) < 2: continue
        input_tokens = sum(to_int(i.get("input_tokens") or i.get("prompt_tokens")) for i in items)
        output_tokens = sum(to_int(i.get("output_tokens") or i.get("completion_tokens")) for i in items)
        cost = round(sum(to_float(i.get("cost") or i.get("total_cost")) for i in items), 6)
        duplicate = len(items) - 1
        repeated.append({"prompt_hash": ph, "model": model, "provider": provider, "task_type": task, "total_calls": len(items), "duplicate_calls": duplicate, "input_tokens": input_tokens, "output_tokens": output_tokens, "cost": cost, "cacheable_cost": round(cost * duplicate / len(items) * 0.5, 6)})
    repeated.sort(key=lambda r: (-r["cacheable_cost"], -r["duplicate_calls"], r["prompt_hash"]))
    shown = repeated[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": gen.isoformat(), "filters": {"lookback_days": lookback_days, "limit": limit}, "summary": {"call_count": len(rows), "repeated_group_count": len(repeated), "shown": len(shown)}, "repeated_call_groups": shown, "estimated_savings": {"cacheable_cost": round(sum(r["cacheable_cost"] for r in repeated), 6), "shown_cacheable_cost": round(sum(r["cacheable_cost"] for r in shown), 6)}, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}, "empty_state": empty_state(repeated, "No model usage cache savings opportunities found.", schema_gap=bool(missing_tables or missing_columns))}


def build_model_usage_cache_savings_opportunities_report_from_db(db_or_conn: Any, **kw):
    conn = connection(db_or_conn); s = schema(conn); mt = []; mc = {}; rows = []
    table = next((t for t in ("model_usage", "model_calls", "llm_usage") if t in s), None)
    if not table: mt.append("model_usage|model_calls|llm_usage")
    else:
        c = s[table]
        if not ({"prompt", "prompt_hash", "template", "template_id", "input"} & c): mc[table] = ["prompt|prompt_hash|template|input"]
        rows = load_table(conn, table, c, {"prompt": ("prompt", "input"), "prompt_hash": ("prompt_hash",), "template": ("template", "template_id"), "model": ("model", "model_name"), "provider": ("provider",), "task_type": ("task_type", "task"), "input_tokens": ("input_tokens", "prompt_tokens"), "output_tokens": ("output_tokens", "completion_tokens"), "cost": ("cost", "total_cost"), "created_at": ("created_at", "started_at")})
    return build_model_usage_cache_savings_opportunities_report(rows, missing_tables=mt, missing_columns=mc, **kw)


def format_model_usage_cache_savings_opportunities_json(r): return json_dumps(r)
def format_model_usage_cache_savings_opportunities_text(r):
    s = r["summary"]; lines = ["Model Usage Cache Savings Opportunities", f"Generated: {r['generated_at']}", f"Totals: calls={s['call_count']} repeated_groups={s['repeated_group_count']} shown={s['shown']} cacheable_cost={r['estimated_savings']['cacheable_cost']}"]
    if r["missing_tables"]: lines.append("Missing tables: " + ", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: " + flatten_missing(r["missing_columns"]))
    if not r["repeated_call_groups"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines += ["", "prompt_hash | model | provider | duplicate_calls | cacheable_cost"]
    for f in r["repeated_call_groups"]: lines.append(f"{f['prompt_hash']} | {f['model']} | {f['provider']} | {f['duplicate_calls']} | {f['cacheable_cost']}")
    return "\n".join(lines)
