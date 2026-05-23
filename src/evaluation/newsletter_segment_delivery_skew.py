"""Detect newsletter segment delivery and engagement skew."""
from ._focused_report_helpers import *
ARTIFACT_TYPE="newsletter_segment_delivery_skew"
DEFAULT_WINDOW_DAYS=30; DEFAULT_MIN_SHARE=0.05; DEFAULT_RATE_DELTA_THRESHOLD=0.2
def build_newsletter_segment_delivery_skew_report(rows, *, window_days=DEFAULT_WINDOW_DAYS, min_share=DEFAULT_MIN_SHARE, rate_delta_threshold=DEFAULT_RATE_DELTA_THRESHOLD, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
    _positive('window_days',window_days); _nonneg('min_share',min_share); _nonneg('rate_delta_threshold',rate_delta_threshold); _positive('limit',limit); gen=_now(now); cutoff=gen-timedelta(days=window_days)
    scoped=[r for r in rows if (_dt(r.get('sent_at') or r.get('created_at')) is None or _dt(r.get('sent_at') or r.get('created_at'))>=cutoff)]
    agg=defaultdict(lambda:{'send_count':0,'subscriber_count':0,'opens':[],'clicks':[]})
    for r in scoped:
        seg=_clean(r.get('segment') or r.get('segment_id') or 'unknown') or 'unknown'; a=agg[seg]; a['send_count']+=1; a['subscriber_count']+=_int(r.get('subscriber_count')) or 0
        if _float(r.get('open_rate')) is not None: a['opens'].append(_float(r.get('open_rate')))
        if _float(r.get('click_rate')) is not None: a['clicks'].append(_float(r.get('click_rate')))
    total=sum(a['send_count'] for a in agg.values()) or 1; subs=sum(a['subscriber_count'] for a in agg.values()) or 0
    open_med=median([median(a['opens']) for a in agg.values() if a['opens']]) if any(a['opens'] for a in agg.values()) else None; click_med=median([median(a['clicks']) for a in agg.values() if a['clicks']]) if any(a['clicks'] for a in agg.values()) else None
    findings=[]
    for seg,a in agg.items():
        actual=a['send_count']/total; expected=(a['subscriber_count']/subs) if subs else 1/max(len(agg),1); orate=median(a['opens']) if a['opens'] else None; crate=median(a['clicks']) if a['clicks'] else None
        base={'segment':seg,'send_count':a['send_count'],'subscriber_count':a['subscriber_count'],'open_rate':orate,'click_rate':crate,'expected_share':expected,'actual_share':actual}
        if actual<min_share: findings.append({**base,'reason':'low_send_share','detail':'segment send share below threshold'})
        if expected and actual<expected*min_share: findings.append({**base,'reason':'low_subscriber_coverage','detail':'segment coverage below expected share'})
        if orate is not None and open_med is not None and abs(orate-open_med)>=rate_delta_threshold: findings.append({**base,'reason':'open_rate_skew','detail':'open rate differs from median'})
        if crate is not None and click_med is not None and abs(crate-click_med)>=rate_delta_threshold: findings.append({**base,'reason':'click_rate_skew','detail':'click rate differs from median'})
    return _finish(ARTIFACT_TYPE,gen,{'window_days':window_days,'min_share':min_share,'rate_delta_threshold':rate_delta_threshold,'limit':limit},len(scoped),findings,limit,missing_tables,missing_columns)
def build_newsletter_segment_delivery_skew_report_from_db(db_or_conn, **kwargs):
    conn=_conn(db_or_conn); s=_schema(conn); send=s.get('newsletter_sends')
    if send is None: return build_newsletter_segment_delivery_skew_report([], missing_tables=['newsletter_sends'], **kwargs)
    metric=s.get('newsletter_engagement_metrics',set()); sel=[_expr(send,'id',alias='send_id'),_expr(send,'segment','segment_id',alias='segment'),_expr(send,'subscriber_count','recipient_count',alias='subscriber_count'),_expr(send,'sent_at','created_at',alias='sent_at')]
    if metric and 'send_id' in metric and 'id' in send:
        sel += [_expr(metric,'open_rate',alias='open_rate',prefix='m.'),_expr(metric,'click_rate',alias='click_rate',prefix='m.')]
        rows=[dict(r) for r in conn.execute(f"SELECT {', '.join(sel)} FROM newsletter_sends ns LEFT JOIN newsletter_engagement_metrics m ON m.send_id=ns.id")]
    else:
        sel += [_expr(send,'open_rate'),_expr(send,'click_rate')]; rows=[dict(r) for r in conn.execute(f"SELECT {', '.join(sel)} FROM newsletter_sends")]
    miss={'newsletter_sends':[c for c in ['segment','segment_id'] if c not in send]} if not ({'segment','segment_id'}&send) else None
    return build_newsletter_segment_delivery_skew_report(rows, missing_columns=miss, **kwargs)
def format_newsletter_segment_delivery_skew_json(report): return _json(report)
def format_newsletter_segment_delivery_skew_text(report): return _text('Newsletter Segment Delivery Skew', report)
