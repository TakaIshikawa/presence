"""Check visual assets for required channel renditions."""
from __future__ import annotations
from collections import Counter, defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="visual_asset_rendition_completeness"; DEFAULT_LIMIT=50
REQUIRED={"social":["social_card","thumbnail"],"newsletter":["newsletter_inline","thumbnail"],"blog":["hero","thumbnail"],"default":["thumbnail"]}
def build_visual_asset_rendition_completeness_report(assets:list[dict[str,Any]],renditions:list[dict[str,Any]]|None=None,*,channel:str|None=None,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    positive("limit",limit); by_asset=defaultdict(set)
    for r in renditions or []: by_asset[clean(r.get("asset_id"))].add(clean(r.get("rendition") or r.get("rendition_type") or r.get("type")))
    findings=[]
    for a in assets:
        ch=clean(a.get("channel") or a.get("target_channel"),"default")
        if channel and ch!=channel: continue
        aid=clean(a.get("asset_id") or a.get("id")); present=set(by_asset.get(aid,set()))|_metadata_renditions(a.get("metadata"))
        req=REQUIRED.get(ch,REQUIRED["default"]); alt=bool(clean(a.get("alt_text") or a.get("alt")))
        missing=[r for r in req if r not in present]
        if missing or not alt:
            findings.append({"asset_id":aid,"content_id":a.get("content_id"),"channel":ch,"required_renditions":req,"present_renditions":sorted(present),"missing_renditions":missing,"alt_text_present":alt,"recommended_action":"generate missing renditions and alt text" if missing and not alt else ("generate missing channel renditions" if missing else "add alt text")})
    findings.sort(key=lambda f:(-len(f["missing_renditions"]),f["alt_text_present"],str(f["asset_id"])))
    shown=findings[:limit]; by=Counter(f["channel"] for f in findings)
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"channel":channel,"limit":limit},"summary":{"asset_count":len(assets),"finding_count":len(findings),"shown":len(shown),"missing_by_channel":dict(sorted(by.items()))},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"All visual assets have required renditions.",schema_gap=bool(missing_tables or missing_columns))}
def _metadata_renditions(value):
    text=clean(value)
    if not text: return set()
    try:
        obj=json.loads(text); vals=obj.get("renditions",obj if isinstance(obj,list) else [])
        return {clean(v.get("type") if isinstance(v,dict) else v) for v in vals}
    except (TypeError,ValueError,AttributeError): return set()
def build_visual_asset_rendition_completeness_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; mc={}; assets=[]; rends=[]
    if "visual_assets" not in s: mt.append("visual_assets")
    else:
        c=s["visual_assets"]
        if not ({"id","asset_id"} & c): mc.setdefault("visual_assets",[]).append("id|asset_id")
        if "visual_assets" not in mc: assets=load_table(conn,"visual_assets",c,{"asset_id":("id","asset_id"),"content_id":("content_id",),"channel":("channel","target_channel"),"alt_text":("alt_text","alt"),"metadata":("metadata","renditions_json")})
    if "visual_asset_usage" in s:
        usage=load_table(conn,"visual_asset_usage",s["visual_asset_usage"],{"asset_id":("asset_id","visual_asset_id"),"content_id":("content_id",),"channel":("channel","target_channel")})
        by={a["asset_id"]:a for a in assets}
        for u in usage:
            if u["asset_id"] in by:
                by[u["asset_id"]]["content_id"]=by[u["asset_id"]].get("content_id") or u.get("content_id"); by[u["asset_id"]]["channel"]=u.get("channel") or by[u["asset_id"]].get("channel")
    for t in ("visual_asset_renditions","asset_renditions"):
        if t in s: rends+=load_table(conn,t,s[t],{"asset_id":("asset_id","visual_asset_id"),"rendition":("rendition","rendition_type","type")})
    return build_visual_asset_rendition_completeness_report(assets,rends,missing_tables=mt,missing_columns=mc,**kw)
def format_visual_asset_rendition_completeness_json(r): return json_dumps(r)
def format_visual_asset_rendition_completeness_text(r):
    s=r["summary"]; lines=["Visual Asset Rendition Completeness",f"Generated: {r['generated_at']}",f"Totals: assets={s['asset_count']} findings={s['finding_count']} shown={s['shown']}"]
    if r["missing_tables"]: lines.append("Missing tables: "+", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: "+flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","asset_id | channel | missing | alt_text"]
    for f in r["findings"]: lines.append(f"{f['asset_id']} | {f['channel']} | {', '.join(f['missing_renditions'])} | {f['alt_text_present']}")
    return "\n".join(lines)
