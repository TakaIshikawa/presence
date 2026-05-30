"""Detect click-to-open ratio drift in newsletter engagement metrics."""
from ._focused_report_helpers import *
ARTIFACT_TYPE="newsletter_open_click_ratio_drift"
DEFAULT_MIN_RATIO=0.1
DEFAULT_DROP_THRESHOLD=0.25
REASONS=("low_click_to_open_ratio","ratio_drop","missing_open_rate","missing_click_rate")
def build_newsletter_open_click_ratio_drift_report(rows, *, min_ratio=DEFAULT_MIN_RATIO, drop_threshold=DEFAULT_DROP_THRESHOLD, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
    _nonneg('min_ratio',min_ratio); _nonneg('drop_threshold',drop_threshold); _positive('limit',limit); gen=_now(now)
    norm=[]
    for i,r in enumerate(rows):
        o=_float(r.get('open_rate')); c=_float(r.get('click_rate')); ratio=(c/o) if o and c is not None else None
        norm.append({**r,'metric_id':r.get('metric_id') or r.get('id') or i,'send_id':r.get('send_id'),'issue_id':r.get('issue_id'),'open_rate':o,'click_rate':c,'ratio':ratio,'fetched_at':r.get('fetched_at') or r.get('created_at')})
    findings=[]; by=defaultdict(list)
    for row in norm:
        by[(row.get('send_id'),row.get('issue_id'))].append(row)
        if row['open_rate'] is None: findings.append(_f('missing_open_rate',row,'open_rate is missing'))
        if row['click_rate'] is None: findings.append(_f('missing_click_rate',row,'click_rate is missing'))
        if row['ratio'] is not None and row['ratio']<min_ratio: findings.append(_f('low_click_to_open_ratio',row,f"ratio {row['ratio']:.4g} below {min_ratio}"))
    for group in by.values():
        group.sort(key=lambda x:(_dt(x.get('fetched_at')) or datetime.max.replace(tzinfo=timezone.utc), str(x['metric_id'])))
        for prev,cur in zip(group,group[1:]):
            if prev['ratio'] is not None and cur['ratio'] is not None and prev['ratio']-cur['ratio']>=drop_threshold:
                findings.append(_f('ratio_drop',cur,f"ratio dropped by {prev['ratio']-cur['ratio']:.4g}", previous_metric_id=prev['metric_id']))
    return _finish(ARTIFACT_TYPE,gen,{'min_ratio':min_ratio,'drop_threshold':drop_threshold,'limit':limit},len(norm),findings,limit,missing_tables,missing_columns)
def _f(reason,row,detail,**extra):
    return {'reason':reason,'send_id':row.get('send_id'),'issue_id':row.get('issue_id'),'metric_id':row.get('metric_id'),'previous_metric_id':extra.get('previous_metric_id'),'open_rate':row.get('open_rate'),'click_rate':row.get('click_rate'),'ratio':row.get('ratio'),'detail':detail}
def build_newsletter_open_click_ratio_drift_report_from_db(db_or_conn, **kwargs):
    conn=_conn(db_or_conn); s=_schema(conn); cols=s.get('newsletter_engagement_metrics')
    if cols is None: return build_newsletter_open_click_ratio_drift_report([], missing_tables=['newsletter_engagement_metrics'], **kwargs)
    send=s.get('newsletter_sends',set()); rows=[]
    sel=[_expr(cols,'id',alias='metric_id'),_expr(cols,'send_id'),_expr(cols,'issue_id'),_expr(cols,'open_rate'),_expr(cols,'click_rate'),_expr(cols,'fetched_at','created_at',alias='fetched_at')]
    if 'newsletter_sends' in s and 'send_id' in cols and 'id' in send:
        sel.append(_expr(send,'issue_id',alias='send_issue_id',prefix='ns.')); rows=[dict(r) for r in conn.execute(f"SELECT {', '.join(sel)} FROM newsletter_engagement_metrics nem LEFT JOIN newsletter_sends ns ON nem.send_id=ns.id")]
        for r in rows: r['issue_id']=r.get('issue_id') or r.get('send_issue_id')
    else: rows=[dict(r) for r in conn.execute(f"SELECT {', '.join(sel)} FROM newsletter_engagement_metrics")]
    return build_newsletter_open_click_ratio_drift_report(rows, **kwargs)
def format_newsletter_open_click_ratio_drift_json(report): return _json(report)
def format_newsletter_open_click_ratio_drift_text(report): return _text('Newsletter Open Click Ratio Drift', report)
