"""Report near-identical content variants reused across channels."""
from __future__ import annotations
from itertools import combinations
from typing import Any
from ._batch_report_common import clean, connection, empty_state, flatten_missing, jaccard, json_dumps, pick, positive, schema, tokens, bounded_share, now_value

ARTIFACT_TYPE="content_variant_channel_collision"; DEFAULT_SIMILARITY_THRESHOLD=0.9; DEFAULT_LIMIT=100

def build_content_variant_channel_collision_report(rows:list[dict[str,Any]],*,similarity_threshold:float=DEFAULT_SIMILARITY_THRESHOLD,limit:int=DEFAULT_LIMIT,now:Any=None,missing_tables=None,missing_columns=None)->dict[str,Any]:
    bounded_share("similarity_threshold",similarity_threshold); positive("limit",limit)
    by_content={}
    for row in rows: by_content.setdefault(clean(row.get("content_id")),[]).append(row)
    findings=[]
    for content_id,items in by_content.items():
        for left,right in combinations(items,2):
            lp=clean(left.get("platform") or left.get("channel"),"unknown").lower(); rp=clean(right.get("platform") or right.get("channel"),"unknown").lower()
            if lp==rp: continue
            lt=clean(left.get("variant_text")); rt=clean(right.get("variant_text"))
            sim=_similarity(lt,rt)
            if sim<similarity_threshold: continue
            findings.append({"content_id":content_id,"left_variant_id":left.get("variant_id") or left.get("id"),"right_variant_id":right.get("variant_id") or right.get("id"),"left_platform":lp,"right_platform":rp,"similarity":sim,"collision_type":"exact" if _norm(lt)==_norm(rt) else "near_identical","recommendation":"Rewrite one variant with platform-specific framing, length, or call to action."})
    findings.sort(key=lambda f:(-f["similarity"],f["content_id"],f["left_platform"],f["right_platform"],clean(f["left_variant_id"])))
    shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_value(now).isoformat(),"filters":{"similarity_threshold":similarity_threshold,"limit":limit},"summary":{"variant_count":len(rows),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items()) if v},"empty_state":empty_state(findings,"No content variant channel collisions found.",schema_gap=bool(missing_tables or missing_columns))}

def build_content_variant_channel_collision_report_from_db(db_or_conn:Any,**kw)->dict[str,Any]:
    conn=connection(db_or_conn); s=schema(conn)
    if "content_variants" not in s: return build_content_variant_channel_collision_report([],missing_tables=["content_variants"],**kw)
    c=s["content_variants"]; missing=[col for col in ("id","content_id") if col not in c]
    if not ({"platform","channel"}&c): missing.append("platform|channel")
    if not ({"variant_text","content","body","text"}&c): missing.append("variant_text|content|body|text")
    if missing: return build_content_variant_channel_collision_report([],missing_columns={"content_variants":missing},**kw)
    return build_content_variant_channel_collision_report(_load_rows(conn,c),**kw)

def format_content_variant_channel_collision_json(r): return json_dumps(r)
def format_content_variant_channel_collision_text(r):
    s=r["summary"]; lines=["Content Variant Channel Collision",f"Generated: {r['generated_at']}",f"Totals: variants={s['variant_count']} findings={s['finding_count']} shown={s['shown_count']}"]
    if r["missing_tables"]: lines.append("Missing tables: "+", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: "+flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines.extend(["","content_id | left | right | platforms | similarity | type"])
    for f in r["findings"]: lines.append(f"{f['content_id']} | {f['left_variant_id']} | {f['right_variant_id']} | {f['left_platform']}/{f['right_platform']} | {f['similarity']:.4f} | {f['collision_type']}")
    return "\n".join(lines)

def _load_rows(conn,cols):
    select=[pick(cols,"id",out="variant_id"),"content_id",pick(cols,"platform","channel",out="platform"),pick(cols,"variant_text","content","body","text",out="variant_text")]
    return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM content_variants ORDER BY content_id ASC, id ASC")]
def _norm(text): return " ".join(clean(text).lower().split())
def _similarity(a,b):
    if not clean(a) or not clean(b): return 0.0
    if _norm(a)==_norm(b): return 1.0
    return jaccard(tokens(a),tokens(b))
