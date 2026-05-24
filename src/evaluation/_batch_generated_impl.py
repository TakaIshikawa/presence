"""Compact implementations for isolated batch evaluation reports."""
from __future__ import annotations
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
import json, sqlite3, statistics
from typing import Any
from urllib.parse import urlparse
from ._report_utils import clean, connection, dt, domain, json_dumps, now_iso, positive, nonnegative, median, schema, to_int, to_float

def _empty(artifact, filters, totals, findings, missing_tables=None, missing_columns=None, **extra):
    return {"artifact_type":artifact,"generated_at":now_iso(filters.pop('_now', None)),"filters":filters,"totals":totals,"findings":findings,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":{"is_empty":not findings,"message":f"No {artifact.replace('_',' ')} findings found." if not findings else None}, **extra}

def _rows(conn, table):
    conn.row_factory=sqlite3.Row
    return [dict(r) for r in conn.execute(f"SELECT * FROM {table} ORDER BY rowid")]

def _has(conn, table): return table in schema(conn)

def _fmt_json(r): return json_dumps(r)
def _fmt_text(title, r, cols):
    lines=[title, f"Generated: {r['generated_at']}", "Totals: "+", ".join(f"{k}={v}" for k,v in r.get('totals',{}).items() if not isinstance(v,dict))]
    if r.get('missing_tables'): lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if r.get('missing_columns'): lines.append('Missing columns: '+ '; '.join(f"{t}({', '.join(c)})" for t,c in r['missing_columns'].items()))
    if not r.get('findings'):
        lines.append(r.get('empty_state',{}).get('message') or 'No findings.'); return '\n'.join(lines)
    lines += ['', ' | '.join(cols)]
    for f in r['findings']:
        lines.append(' | '.join(str(f.get(c,'-') if f.get(c) not in (None,'') else '-') for c in cols))
    return '\n'.join(lines)

# Newsletter delivery failure taxonomy
_CATS=("bounce","suppression","provider_reject","timeout","unknown")
def _failure_cat(r):
    t=' '.join(clean(r.get(k)).lower() for k in ('status','error_code','error_message','provider_error','payload'))
    if any(x in t for x in ('bounce','mailbox','invalid recipient')): return 'bounce'
    if any(x in t for x in ('suppress','unsubscribe','blocked')): return 'suppression'
    if any(x in t for x in ('reject','denied','policy','spam')): return 'provider_reject'
    if any(x in t for x in ('timeout','timed out','deadline')): return 'timeout'
    return 'unknown'
def build_newsletter_delivery_failure_taxonomy_report(rows, *, limit=50, now=None, missing_tables=None, missing_columns=None):
    positive('limit',limit); totals=Counter(); prov=defaultdict(Counter); issue=defaultdict(Counter); findings=[]
    for r in rows:
        cat=_failure_cat(r); p=clean(r.get('provider'),'unknown'); i=clean(r.get('issue_id') or r.get('newsletter_issue_id'),'unknown')
        totals[cat]+=1; prov[p][cat]+=1; issue[i][cat]+=1
        findings.append({'category':cat,'provider':p,'issue_id':i,'subscriber_id':r.get('subscriber_id'),'occurred_at':r.get('occurred_at') or r.get('created_at'),'error_snippet':clean(r.get('error_message') or r.get('provider_error'))[:160]})
    findings.sort(key=lambda f:(f['category'],f['provider'],f['issue_id'],clean(f.get('occurred_at'))), reverse=True); shown=findings[:limit]
    return _empty('newsletter_delivery_failure_taxonomy',{'limit':limit,'_now':now},{'failure_count':len(rows),'by_category':{c:totals[c] for c in _CATS},'shown_count':len(shown)},shown,missing_tables,missing_columns,provider_breakdown=[{'provider':k,'total':sum(v.values()),'by_category':dict(sorted(v.items()))} for k,v in sorted(prov.items())],issue_breakdown=[{'issue_id':k,'total':sum(v.values()),'by_category':dict(sorted(v.items()))} for k,v in sorted(issue.items())])
def build_newsletter_delivery_failure_taxonomy_report_from_db(db_or_conn, **kw):
    conn=connection(db_or_conn); return build_newsletter_delivery_failure_taxonomy_report(_rows(conn,'newsletter_delivery_events') if _has(conn,'newsletter_delivery_events') else [], missing_tables=[] if _has(conn,'newsletter_delivery_events') else ['newsletter_delivery_events'], **kw)
def format_newsletter_delivery_failure_taxonomy_json(r): return _fmt_json(r)
def format_newsletter_delivery_failure_taxonomy_text(r): return _fmt_text('Newsletter Delivery Failure Taxonomy',r,['category','provider','issue_id','subscriber_id','occurred_at'])

# Archive metric backfill
_TYPES=('opens','clicks','bounces','unsubscribes')
def build_newsletter_archive_metric_backfill_candidates_report(issue_rows, metric_rows=None, *, min_age_hours=24, limit=50, now=None, missing_tables=None, missing_columns=None):
    positive('min_age_hours',min_age_hours); positive('limit',limit); gen=now or datetime.now(timezone.utc); have=defaultdict(set)
    for m in metric_rows or []:
        iid=clean(m.get('issue_id') or m.get('newsletter_issue_id')); typ=clean(m.get('metric_type') or m.get('type')).lower()
        if iid and typ in _TYPES and m.get('value', m.get('count', 1)) not in (None,''): have[iid].add(typ)
    findings=[]
    for r in issue_rows:
        iid=clean(r.get('issue_id') or r.get('id')); sent=dt(r.get('sent_at') or r.get('published_at')); status=clean(r.get('status') or r.get('state')).lower()
        if not iid or not sent or status not in {'sent','published','archived'}: continue
        age=(gen.astimezone(timezone.utc)-sent).total_seconds()/3600; miss=[t for t in _TYPES if t not in have[iid]]
        if age>=min_age_hours and miss:
            aud=to_int(r.get('audience_size') or r.get('recipient_count')) or 0; score=round(age/24+aud/1000+len(miss)*5,4)
            findings.append({'issue_id':iid,'status':status,'age_hours':round(age,2),'audience_size':aud,'missing_metric_types':miss,'priority_score':score})
    findings.sort(key=lambda f:(-f['priority_score'],f['issue_id'])); shown=findings[:limit]
    return _empty('newsletter_archive_metric_backfill_candidates',{'min_age_hours':min_age_hours,'limit':limit,'_now':gen},{'issue_count':len(issue_rows),'candidate_count':len(findings),'shown_count':len(shown)},shown,missing_tables,missing_columns)
def build_newsletter_archive_metric_backfill_candidates_report_from_db(db_or_conn, **kw):
    conn=connection(db_or_conn); s=schema(conn); mt=next((t for t in ('newsletter_metrics','newsletter_issue_metrics') if t in s),None)
    return build_newsletter_archive_metric_backfill_candidates_report(_rows(conn,'newsletter_issues') if 'newsletter_issues' in s else [], _rows(conn,mt) if mt else [], missing_tables=([t for t in ['newsletter_issues'] if t not in s]+([] if mt else ['newsletter_metrics/newsletter_issue_metrics'])), **kw)
def format_newsletter_archive_metric_backfill_candidates_json(r): return _fmt_json(r)
def format_newsletter_archive_metric_backfill_candidates_text(r): return _fmt_text('Newsletter Archive Metric Backfill Candidates',r,['issue_id','age_hours','audience_size','missing_metric_types','priority_score'])

# Redirect risk
_SUCCESS={'success','succeeded','published','ok','sent',''}; _SHORT={'bit.ly','t.co','tinyurl.com','goo.gl','ow.ly'}
def build_publication_attempt_redirect_chain_risk_report(rows, *, max_hops=3, limit=50, now=None, missing_tables=None, missing_columns=None):
    positive('max_hops',max_hops); positive('limit',limit); findings=[]; plat=Counter(); maxh=defaultdict(int)
    for r in rows:
        if clean(r.get('status') or r.get('outcome')).lower() not in _SUCCESS: continue
        url=clean(r.get('url') or r.get('platform_url')); final=clean(r.get('final_url')); hops=to_int(r.get('redirect_hops')) or 0; reasons=[]; du=domain(url); df=domain(final)
        if hops>max_hops: reasons.append('too_many_hops')
        if not final: reasons.append('missing_final_url')
        if url.startswith('https://') and final.startswith('http://'): reasons.append('http_downgrade')
        if final and du and df and du!=df: reasons.append('final_domain_mismatch')
        if du in _SHORT or df in _SHORT: reasons.append('shortened_link_chain')
        p=clean(r.get('platform'),'unknown'); maxh[p]=max(maxh[p],hops)
        if reasons: plat[p]+=1; findings.append({'platform':p,'content_id':r.get('content_id'),'url':url,'final_url':final or None,'redirect_hops':hops,'risk_reasons':reasons,'checked_at':r.get('checked_at')})
    findings.sort(key=lambda f:(-len(f['risk_reasons']),-f['redirect_hops'],f['platform'],clean(f.get('content_id')))); shown=findings[:limit]
    return _empty('publication_attempt_redirect_chain_risk',{'max_hops':max_hops,'limit':limit,'_now':now},{'attempt_count':len(rows),'risk_count':len(findings),'shown_count':len(shown)},shown,missing_tables,missing_columns,platform_totals=dict(sorted(plat.items())),max_hop_summary=dict(sorted(maxh.items())))
def build_publication_attempt_redirect_chain_risk_report_from_db(db_or_conn, **kw):
    conn=connection(db_or_conn); s=schema(conn); miss=[] if 'publication_attempts' in s else ['publication_attempts']; opt=[] if any(t in s for t in ('url_check_results','link_check_results')) else ['url_check_results/link_check_results']
    return build_publication_attempt_redirect_chain_risk_report(_rows(conn,'publication_attempts') if 'publication_attempts' in s else [], missing_tables=miss+opt, **kw)
def format_publication_attempt_redirect_chain_risk_json(r): return _fmt_json(r)
def format_publication_attempt_redirect_chain_risk_text(r): return _fmt_text('Publication Attempt Redirect Chain Risk',r,['platform','content_id','redirect_hops','risk_reasons','final_url'])

# Retry reason drift
def build_publication_attempt_retry_reason_drift_report(rows, *, baseline_days=30, current_days=7, min_delta=0.2, limit=50, now=None, missing_tables=None, missing_columns=None):
    positive('baseline_days',baseline_days); positive('current_days',current_days); nonnegative('min_delta',min_delta); positive('limit',limit); gen=now or datetime.now(timezone.utc); cur_cut=gen-timedelta(days=current_days); base_cut=cur_cut-timedelta(days=baseline_days); buckets={'baseline':Counter(),'current':Counter()}; groups=defaultdict(lambda:{'baseline':0,'current':0})
    for r in rows:
        at=dt(r.get('attempted_at') or r.get('created_at')); reason=clean(r.get('retry_reason') or r.get('error_category') or r.get('reason'),'unknown'); p=clean(r.get('platform'),'unknown');
        if not at: continue
        b='current' if at>=cur_cut else 'baseline' if at>=base_cut else None
        if b: buckets[b][(p,reason)]+=1; groups[(p,reason)][b]+=1
    bt=sum(buckets['baseline'].values()) or 1; ct=sum(buckets['current'].values()) or 1; findings=[]
    for (p,reason),g in groups.items():
        bs=g['baseline']/bt; cs=g['current']/ct; delta=round(cs-bs,4)
        if abs(delta)>=min_delta: findings.append({'platform':p,'reason':reason,'baseline_count':g['baseline'],'current_count':g['current'],'baseline_share':round(bs,4),'current_share':round(cs,4),'share_delta':delta})
    findings.sort(key=lambda f:(-abs(f['share_delta']),f['platform'],f['reason'])); shown=findings[:limit]
    return _empty('publication_attempt_retry_reason_drift',{'baseline_days':baseline_days,'current_days':current_days,'min_delta':min_delta,'limit':limit,'_now':gen},{'baseline_count':bt if bt!=1 else 0,'current_count':ct if ct!=1 else 0,'drift_count':len(findings),'shown_count':len(shown)},shown,missing_tables,missing_columns)
def build_publication_attempt_retry_reason_drift_report_from_db(db_or_conn, **kw):
    conn=connection(db_or_conn); s=schema(conn); t=next((x for x in ('publication_retries','publication_attempts') if x in s),None)
    return build_publication_attempt_retry_reason_drift_report(_rows(conn,t) if t else [], missing_tables=[] if t else ['publication_attempts/publication_retries'], **kw)
def format_publication_attempt_retry_reason_drift_json(r): return _fmt_json(r)
def format_publication_attempt_retry_reason_drift_text(r): return _fmt_text('Publication Attempt Retry Reason Drift',r,['platform','reason','baseline_count','current_count','share_delta'])

# Schedule window utilization
def build_publish_queue_schedule_window_utilization_report(queue_rows, window_rows=None, *, timezone='UTC', underused_threshold=0.5, overfilled_threshold=1.0, limit=50, now=None, missing_tables=None, missing_columns=None):
    nonnegative('underused_threshold',underused_threshold); positive('overfilled_threshold',overfilled_threshold); positive('limit',limit); windows=window_rows or []; counts=Counter(); outside=[]
    def match(q):
        at=dt(q.get('scheduled_at') or q.get('publish_at')); p=clean(q.get('platform'),'unknown')
        if not at: return None
        hm=at.strftime('%H:%M')
        for w in windows:
            if clean(w.get('platform'),p) in {p,'all',''} and clean(w.get('start_time'),'00:00')<=hm<=clean(w.get('end_time'),'23:59'): return clean(w.get('window_id') or w.get('id') or f"{p}:{w.get('start_time')}-{w.get('end_time')}")
        return None
    for q in queue_rows:
        wid=match(q)
        if wid: counts[(clean(q.get('platform'),'unknown'),wid)]+=1
        else: outside.append({'issue_type':'outside_window','queue_id':q.get('id') or q.get('queue_id'),'platform':clean(q.get('platform'),'unknown'),'scheduled_at':q.get('scheduled_at') or q.get('publish_at')})
    findings=outside[:]
    for w in windows:
        wid=clean(w.get('window_id') or w.get('id') or f"{w.get('platform')}:{w.get('start_time')}-{w.get('end_time')}"); p=clean(w.get('platform'),'unknown'); cap=to_int(w.get('capacity') or w.get('max_items')) or 1; used=counts[(p,wid)]; util=used/cap
        if util<underused_threshold: findings.append({'issue_type':'underused_window','platform':p,'window_id':wid,'scheduled_count':used,'capacity':cap,'utilization':round(util,4)})
        if util>overfilled_threshold: findings.append({'issue_type':'overfilled_window','platform':p,'window_id':wid,'scheduled_count':used,'capacity':cap,'utilization':round(util,4)})
    findings.sort(key=lambda f:(f['issue_type'],clean(f.get('platform')),clean(f.get('window_id') or f.get('queue_id')))); shown=findings[:limit]
    return _empty('publish_queue_schedule_window_utilization',{'timezone':timezone,'underused_threshold':underused_threshold,'overfilled_threshold':overfilled_threshold,'limit':limit,'_now':now},{'queue_count':len(queue_rows),'window_count':len(windows),'finding_count':len(findings),'shown_count':len(shown)},shown,missing_tables,missing_columns,utilization_by_window=[{'platform':p,'window_id':w,'scheduled_count':c} for (p,w),c in sorted(counts.items())])
def build_publish_queue_schedule_window_utilization_report_from_db(db_or_conn, **kw):
    conn=connection(db_or_conn); s=schema(conn); wt=next((t for t in ('posting_windows','publish_windows') if t in s),None)
    return build_publish_queue_schedule_window_utilization_report(_rows(conn,'publish_queue') if 'publish_queue' in s else [], _rows(conn,wt) if wt else [], missing_tables=([t for t in ['publish_queue'] if t not in s]+([] if wt else ['posting_windows/publish_windows'])), **kw)
def format_publish_queue_schedule_window_utilization_json(r): return _fmt_json(r)
def format_publish_queue_schedule_window_utilization_text(r): return _fmt_text('Publish Queue Schedule Window Utilization',r,['issue_type','platform','window_id','queue_id','utilization'])

# Domain concentration
def build_content_claim_evidence_domain_concentration_report(rows, *, min_claims=2, max_domain_share=0.6, limit=50, now=None, missing_tables=None, missing_columns=None):
    positive('min_claims',min_claims); positive('limit',limit); byc=defaultdict(list)
    for r in rows:
        cid=clean(r.get('content_id') or r.get('generated_content_id') or r.get('claim_id')); d=domain(r.get('evidence_url') or r.get('url') or r.get('source_url'))
        if cid and d: byc[cid].append(d)
    findings=[]
    for cid,ds in byc.items():
        if len(ds)<min_claims: continue
        c=Counter(ds); top,topn=c.most_common(1)[0]; share=topn/len(ds)
        if share>max_domain_share or len(c)<2: findings.append({'content_id':cid,'claim_count':len(ds),'unique_domain_count':len(c),'top_domain':top,'top_domain_share':round(share,4),'domains':dict(sorted(c.items()))})
    findings.sort(key=lambda f:(-f['top_domain_share'],-f['claim_count'],f['content_id'])); shown=findings[:limit]
    return _empty('content_claim_evidence_domain_concentration',{'min_claims':min_claims,'max_domain_share':max_domain_share,'limit':limit,'_now':now},{'content_count':len(byc),'finding_count':len(findings),'shown_count':len(shown)},shown,missing_tables,missing_columns)
def build_content_claim_evidence_domain_concentration_report_from_db(db_or_conn, **kw):
    conn=connection(db_or_conn); s=schema(conn)
    return build_content_claim_evidence_domain_concentration_report(_rows(conn,'content_claim_checks') if 'content_claim_checks' in s else [], missing_tables=[] if 'content_claim_checks' in s else ['content_claim_checks'], **kw)
def format_content_claim_evidence_domain_concentration_json(r): return _fmt_json(r)
def format_content_claim_evidence_domain_concentration_text(r): return _fmt_text('Content Claim Evidence Domain Concentration',r,['content_id','claim_count','unique_domain_count','top_domain','top_domain_share'])

# Pipeline artifact retention
def build_pipeline_run_artifact_retention_gaps_report(run_rows, artifact_rows=None, *, max_age_days=30, max_size_bytes=100000000, limit=50, now=None, missing_tables=None, missing_columns=None):
    positive('max_age_days',max_age_days); positive('max_size_bytes',max_size_bytes); positive('limit',limit); gen=now or datetime.now(timezone.utc); arts=defaultdict(list); findings=[]
    for a in artifact_rows or []: arts[clean(a.get('run_id') or a.get('pipeline_run_id'))].append(a)
    runids={clean(r.get('run_id') or r.get('id')) for r in run_rows}
    for r in run_rows:
        rid=clean(r.get('run_id') or r.get('id')); stage=clean(r.get('stage'),'unknown')
        if not arts.get(rid): findings.append({'issue_type':'missing_artifact','run_id':rid,'stage':stage})
    for a in artifact_rows or []:
        rid=clean(a.get('run_id') or a.get('pipeline_run_id')); stage=clean(a.get('stage'),'unknown'); exp=dt(a.get('expires_at')); size=to_int(a.get('size_bytes')) or 0
        if rid not in runids: findings.append({'issue_type':'orphan_artifact','run_id':rid,'artifact_id':a.get('id') or a.get('artifact_id'),'stage':stage})
        if exp and exp<gen: findings.append({'issue_type':'expired_artifact','run_id':rid,'artifact_id':a.get('id') or a.get('artifact_id'),'stage':stage})
        if size>max_size_bytes: findings.append({'issue_type':'oversized_artifact','run_id':rid,'artifact_id':a.get('id') or a.get('artifact_id'),'stage':stage,'size_bytes':size})
    findings.sort(key=lambda f:(f['issue_type'],clean(f.get('stage')),clean(f.get('run_id')))); shown=findings[:limit]
    return _empty('pipeline_run_artifact_retention_gaps',{'max_age_days':max_age_days,'max_size_bytes':max_size_bytes,'limit':limit,'_now':gen},{'run_count':len(run_rows),'artifact_count':len(artifact_rows or []),'finding_count':len(findings),'shown_count':len(shown)},shown,missing_tables,missing_columns,stage_summary=dict(sorted(Counter(f.get('stage','unknown') for f in findings).items())))
def build_pipeline_run_artifact_retention_gaps_report_from_db(db_or_conn, **kw):
    conn=connection(db_or_conn); s=schema(conn); at=next((t for t in ('pipeline_artifacts','publish_artifacts') if t in s),None)
    return build_pipeline_run_artifact_retention_gaps_report(_rows(conn,'pipeline_runs') if 'pipeline_runs' in s else [], _rows(conn,at) if at else [], missing_tables=([t for t in ['pipeline_runs'] if t not in s]+([] if at else ['pipeline_artifacts/publish_artifacts'])), **kw)
def format_pipeline_run_artifact_retention_gaps_json(r): return _fmt_json(r)
def format_pipeline_run_artifact_retention_gaps_text(r): return _fmt_text('Pipeline Run Artifact Retention Gaps',r,['issue_type','run_id','artifact_id','stage','size_bytes'])

# Reply source license exposure
_BAD=('restricted','noncommercial','private','missing','unknown','none')
def build_reply_draft_source_license_exposure_report(rows, *, include_posted=False, limit=50, now=None, missing_tables=None, missing_columns=None):
    positive('limit',limit); findings=[]
    for r in rows:
        status=clean(r.get('status'),'draft').lower(); lic=clean(r.get('license'),'missing').lower()
        if not include_posted and status in {'posted','sent','published'}: continue
        if any(b in lic for b in _BAD): findings.append({'reply_queue_id':r.get('reply_queue_id') or r.get('id'),'status':status,'author':r.get('author'),'source_url':r.get('source_url') or r.get('url'),'license':lic,'reason':'restricted_or_missing_license'})
    findings.sort(key=lambda f:(f['status'],clean(f.get('author')),clean(f.get('reply_queue_id')))); shown=findings[:limit]
    return _empty('reply_draft_source_license_exposure',{'include_posted':include_posted,'limit':limit,'_now':now},{'source_count':len(rows),'exposure_count':len(findings),'shown_count':len(shown)},shown,missing_tables,missing_columns)
def build_reply_draft_source_license_exposure_report_from_db(db_or_conn, **kw):
    conn=connection(db_or_conn); s=schema(conn)
    rows=[]
    if all(t in s for t in ('reply_queue','reply_knowledge_links','knowledge')):
        rows=[dict(r) for r in conn.execute('SELECT rq.id AS reply_queue_id, rq.status, rq.author, k.url AS source_url, k.license FROM reply_queue rq JOIN reply_knowledge_links l ON l.reply_queue_id=rq.id JOIN knowledge k ON k.id=l.knowledge_id ORDER BY rq.id')]
    return build_reply_draft_source_license_exposure_report(rows, missing_tables=[t for t in ('reply_queue','reply_knowledge_links','knowledge') if t not in s], **kw)
def format_reply_draft_source_license_exposure_json(r): return _fmt_json(r)
def format_reply_draft_source_license_exposure_text(r): return _fmt_text('Reply Draft Source License Exposure',r,['reply_queue_id','status','author','license','reason'])

# GitHub activity response time
def build_github_activity_author_response_time_report(activity_rows, response_rows=None, *, sla_hours=24, window_days=30, limit=50, now=None, missing_tables=None, missing_columns=None):
    positive('sla_hours',sla_hours); positive('window_days',window_days); positive('limit',limit); gen=now or datetime.now(timezone.utc); cut=gen-timedelta(days=window_days); responses=defaultdict(list)
    for r in response_rows or []: responses[clean(r.get('activity_id') or r.get('github_activity_id'))].append(r)
    findings=[]; lats=[]; breakdown=defaultdict(list)
    for a in activity_rows:
        aid=clean(a.get('activity_id') or a.get('id')); at=dt(a.get('occurred_at') or a.get('created_at')); author=clean(a.get('author'),'unknown'); repo=clean(a.get('repository') or a.get('repo'),'unknown')
        if not aid or not at or at<cut: continue
        first=min([dt(r.get('responded_at') or r.get('created_at')) for r in responses.get(aid,[]) if dt(r.get('responded_at') or r.get('created_at'))], default=None)
        if first: lat=round((first-at).total_seconds()/3600,4); lats.append(lat); breakdown[(author,repo)].append(lat)
        else: lat=None
        if lat is None or lat>sla_hours: findings.append({'activity_id':aid,'author':author,'repository':repo,'occurred_at':at.isoformat(),'first_response_hours':lat,'reason':'missing_response' if lat is None else 'over_sla'})
    findings.sort(key=lambda f:(f['first_response_hours'] is not None, -(f['first_response_hours'] or 999999), f['author'], f['repository'])); shown=findings[:limit]
    return _empty('github_activity_author_response_time',{'sla_hours':sla_hours,'window_days':window_days,'limit':limit,'_now':gen},{'activity_count':len(activity_rows),'response_count':len(response_rows or []),'median_first_response_hours':median(lats),'overdue_count':len(findings),'shown_count':len(shown)},shown,missing_tables,missing_columns,author_repo_breakdown=[{'author':a,'repository':r,'count':len(v),'median_hours':median(v)} for (a,r),v in sorted(breakdown.items())])
def build_github_activity_author_response_time_report_from_db(db_or_conn, **kw):
    conn=connection(db_or_conn); s=schema(conn); rt=next((t for t in ('reply_queue','proactive_actions') if t in s),None)
    return build_github_activity_author_response_time_report(_rows(conn,'github_activity') if 'github_activity' in s else [], _rows(conn,rt) if rt else [], missing_tables=([t for t in ['github_activity'] if t not in s]+([] if rt else ['reply_queue/proactive_actions'])), **kw)
def format_github_activity_author_response_time_json(r): return _fmt_json(r)
def format_github_activity_author_response_time_text(r): return _fmt_text('GitHub Activity Author Response Time',r,['activity_id','author','repository','first_response_hours','reason'])

# Blog/newsletter topic echo
def _tokens(v): return {x for x in ''.join(ch.lower() if ch.isalnum() else ' ' for ch in clean(v)).split() if len(x)>2}
def build_blog_newsletter_topic_echo_risk_report(blog_rows, issue_rows=None, *, cooldown_days=14, similarity_threshold=0.5, limit=50, now=None, missing_tables=None, missing_columns=None):
    positive('cooldown_days',cooldown_days); positive('limit',limit); findings=[]
    for b in blog_rows:
        bt=_tokens(b.get('topic') or b.get('title') or b.get('headline')); bd=dt(b.get('published_at') or b.get('created_at'))
        for n in issue_rows or []:
            nt=_tokens(n.get('topic') or n.get('title') or n.get('subject')); nd=dt(n.get('sent_at') or n.get('published_at') or n.get('created_at'))
            if not bt or not nt or not bd or not nd: continue
            gap=abs((nd-bd).days); sim=len(bt&nt)/len(bt|nt)
            if gap<=cooldown_days and sim>=similarity_threshold: findings.append({'blog_id':b.get('id') or b.get('blog_id'),'newsletter_issue_id':n.get('id') or n.get('issue_id'),'similarity':round(sim,4),'day_gap':gap,'blog_title':b.get('title'),'newsletter_title':n.get('title') or n.get('subject')})
    findings.sort(key=lambda f:(-f['similarity'],f['day_gap'],clean(f.get('blog_id')))); shown=findings[:limit]
    return _empty('blog_newsletter_topic_echo_risk',{'cooldown_days':cooldown_days,'similarity_threshold':similarity_threshold,'limit':limit,'_now':now},{'blog_count':len(blog_rows),'newsletter_issue_count':len(issue_rows or []),'echo_count':len(findings),'shown_count':len(shown)},shown,missing_tables,missing_columns)
def build_blog_newsletter_topic_echo_risk_report_from_db(db_or_conn, **kw):
    conn=connection(db_or_conn); s=schema(conn); bt=next((t for t in ('blog_posts','generated_content') if t in s),None)
    return build_blog_newsletter_topic_echo_risk_report(_rows(conn,bt) if bt else [], _rows(conn,'newsletter_issues') if 'newsletter_issues' in s else [], missing_tables=([ 'generated_content/blog_posts'] if not bt else [])+([ 'newsletter_issues'] if 'newsletter_issues' not in s else []), **kw)
def format_blog_newsletter_topic_echo_risk_json(r): return _fmt_json(r)
def format_blog_newsletter_topic_echo_risk_text(r): return _fmt_text('Blog Newsletter Topic Echo Risk',r,['blog_id','newsletter_issue_id','similarity','day_gap'])
