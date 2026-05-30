"""Find duplicated and stale topic coverage across knowledge sources."""
from __future__ import annotations
from collections import defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="knowledge_source_topic_overlap"; DEFAULT_DAYS=365; DEFAULT_LIMIT=50; DEFAULT_MIN_SOURCES=2
def _topics(r):
 vals=[]; meta=clean(r.get("metadata"))
 for k in ("topic","topics","tags","category"):
  v=r.get(k)
  if v: vals.append(v)
 if meta:
  try:
   obj=json.loads(meta); vals += [obj.get(k) for k in ("topic","topics","tags","category") if obj.get(k)]
  except Exception: pass
 out=[]
 for v in vals:
  if isinstance(v,list): out += [clean(x).lower() for x in v if clean(x)]
  else: out += [p.strip().lower() for p in re.split(r"[,|;]",clean(v)) if p.strip()]
 return sorted(set(out))
def build_knowledge_source_topic_overlap_report(rows:list[dict[str,Any]],*,days:int=DEFAULT_DAYS,limit:int=DEFAULT_LIMIT,topic:str|None=None,min_sources:int=DEFAULT_MIN_SOURCES,missing_tables=None,missing_columns=None,now=None):
 positive("days",days); positive("limit",limit); positive("min_sources",min_sources); buckets=defaultdict(list); wanted=lower(topic) if topic else None
 for r in rows:
  for t in _topics(r):
   if wanted and t!=wanted: continue
   buckets[t].append(r)
 findings=[]
 for t,items in buckets.items():
  if len(items)<min_sources: continue
  dates=sorted([d.isoformat() for d in (dt(x.get("updated_at") or x.get("last_seen_at") or x.get("created_at")) for x in items) if d]); urls=[clean(x.get("url") or x.get("source_url")) for x in items if clean(x.get("url") or x.get("source_url"))][:5]
  findings.append({"topic":t,"source_count":len(items),"oldest_timestamp":dates[0] if dates else None,"newest_timestamp":dates[-1] if dates else None,"representative_urls":urls})
 findings.sort(key=lambda f:(-f["source_count"],f["oldest_timestamp"] or "",f["topic"]))
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"days":days,"limit":limit,"topic":topic,"min_sources":min_sources},"summary":{"topic_count":len(buckets),"finding_count":len(findings)},"findings":findings[:limit],"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No knowledge source topic overlap found.",schema_gap=bool(missing_tables or missing_columns))}
def build_knowledge_source_topic_overlap_report_from_db(db_or_conn:Any,**kw):
 conn=connection(db_or_conn); s=schema(conn); table="knowledge_sources" if "knowledge_sources" in s else "knowledge" if "knowledge" in s else None
 if not table: return build_knowledge_source_topic_overlap_report([],missing_tables=["knowledge_sources|knowledge"],**kw)
 c=s[table]
 if not ({"topic","topics","tags","category","metadata"}&c): return build_knowledge_source_topic_overlap_report([],missing_columns={table:["topic|topics|tags|category|metadata"]},**kw)
 where=[]; params=[]; days=kw.get("days",DEFAULT_DAYS)
 if "updated_at" in c: where.append("(updated_at IS NULL OR updated_at >= ?)"); params.append((now_value(kw.get("now"))-timedelta(days=days)).isoformat())
 q=f"SELECT {pick(c,'id',out='id')}, {pick(c,'url','source_url',out='url')}, {pick(c,'topic',out='topic')}, {pick(c,'topics',out='topics')}, {pick(c,'tags',out='tags')}, {pick(c,'category',out='category')}, {pick(c,'metadata','meta_json',out='metadata')}, {pick(c,'updated_at','last_seen_at','created_at',out='updated_at')} FROM {table}"+((" WHERE "+" AND ".join(where)) if where else "")+" ORDER BY rowid"
 return build_knowledge_source_topic_overlap_report([dict(r) for r in conn.execute(q,params)],**kw)
def format_knowledge_source_topic_overlap_json(r): return json_dumps(r)
def format_knowledge_source_topic_overlap_text(r):
 lines=["Knowledge Source Topic Overlap",f"Artifact: {r['artifact_type']}",f"Generated: {r['generated_at']}",f"Totals: topics={r['summary']['topic_count']} findings={r['summary']['finding_count']}"]
 for f in r["findings"]: lines.append(f"- {f['topic']}: sources={f['source_count']} oldest={f['oldest_timestamp']}")
 return "\n".join(lines)
