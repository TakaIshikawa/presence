"""Deterministically flag claim-heavy content with sentiment imbalance."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="generated_content_claim_sentiment_balance"; DEFAULT_DAYS=30; DEFAULT_LIMIT=50; DEFAULT_POSITIVE_THRESHOLD=3; DEFAULT_NEGATIVE_THRESHOLD=3
POS={"best","excellent","proven","guaranteed","breakthrough","superior","amazing","win","success"}; NEG={"bad","fail","broken","risk","danger","terrible","harm","loss","crisis","problem"}; CLAIM={"always","never","must","will","proves","guarantees","all","none","every","clearly"}; QUAL={"may","might","could","sometimes","often","depends","however","although","likely"}
def build_generated_content_claim_sentiment_balance_report(rows:list[dict[str,Any]],*,days:int=DEFAULT_DAYS,limit:int=DEFAULT_LIMIT,positive_threshold:int=DEFAULT_POSITIVE_THRESHOLD,negative_threshold:int=DEFAULT_NEGATIVE_THRESHOLD,missing_tables=None,missing_columns=None,now=None):
 positive("days",days); positive("limit",limit); positive("positive_threshold",positive_threshold); positive("negative_threshold",negative_threshold); findings=[]
 for r in rows:
  text=clean(r.get("body") or r.get("content") or r.get("text")); words=re.findall(r"[a-z]+",text.lower()); pos=[w for w in words if w in POS]; neg=[w for w in words if w in NEG]; claims=[w for w in words if w in CLAIM]; quals=[w for w in words if w in QUAL]; reasons=[]
  if len(claims)<2: continue
  if len(pos)>=positive_threshold and len(pos)>len(neg): reasons.append("overly_positive")
  if len(neg)>=negative_threshold and len(neg)>len(pos): reasons.append("overly_negative")
  if not quals: reasons.append("missing_qualifiers")
  if reasons: findings.append({"content_id":r.get("content_id") or r.get("id"),"content_type":clean(r.get("content_type")) or None,"reasons":reasons,"positive_terms":dict(Counter(pos)),"negative_terms":dict(Counter(neg)),"claim_terms":dict(Counter(claims)),"excerpt":text[:160]})
 findings.sort(key=lambda f:(-len(f["reasons"]),str(f["content_id"])))
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"days":days,"limit":limit,"positive_threshold":positive_threshold,"negative_threshold":negative_threshold},"summary":{"content_count":len(rows),"finding_count":len(findings)},"findings":findings[:limit],"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No generated content claim sentiment imbalance found.",schema_gap=bool(missing_tables or missing_columns))}
def build_generated_content_claim_sentiment_balance_report_from_db(db_or_conn:Any,**kw):
 conn=connection(db_or_conn); s=schema(conn)
 if "generated_content" not in s: return build_generated_content_claim_sentiment_balance_report([],missing_tables=["generated_content"],**kw)
 c=s["generated_content"]
 if not ({"body","content","text"}&c): return build_generated_content_claim_sentiment_balance_report([],missing_columns={"generated_content":["body|content|text"]},**kw)
 where=[]; params=[]; days=kw.get("days",DEFAULT_DAYS)
 if "created_at" in c: where.append("(created_at IS NULL OR created_at >= ?)"); params.append((now_value(kw.get("now"))-timedelta(days=days)).isoformat())
 q=f"SELECT {pick(c,'id',out='content_id')}, {pick(c,'content_type',out='content_type')}, {pick(c,'body','content','text',out='body')} FROM generated_content"+((" WHERE "+" AND ".join(where)) if where else "")+" ORDER BY rowid"
 return build_generated_content_claim_sentiment_balance_report([dict(r) for r in conn.execute(q,params)],**kw)
def format_generated_content_claim_sentiment_balance_json(r): return json_dumps(r)
def format_generated_content_claim_sentiment_balance_text(r):
 lines=["Generated Content Claim Sentiment Balance",f"Artifact: {r['artifact_type']}",f"Generated: {r['generated_at']}",f"Totals: content={r['summary']['content_count']} findings={r['summary']['finding_count']}"]
 for f in r["findings"]: lines.append(f"- {f['content_id']}: {','.join(f['reasons'])}")
 return "\n".join(lines)
