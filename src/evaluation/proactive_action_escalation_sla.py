"""Detect proactive actions over SLA without escalation or owner assignment."""
from ._focused_report_helpers import *
ARTIFACT_TYPE="proactive_action_escalation_sla"; DEFAULT_SLA_HOURS=24
def build_proactive_action_escalation_sla_report(rows, *, sla_hours=DEFAULT_SLA_HOURS, action_type=None, source=None, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
 _positive('sla_hours',sla_hours); _positive('limit',limit); gen=_now(now); findings=[]; scoped=[]
 for r in rows:
  if action_type and r.get('action_type')!=action_type: continue
  if source and r.get('source')!=source: continue
  created=_dt(r.get('created_at')); age=(gen-(created or gen)).total_seconds()/3600; status=_lower(r.get('status'))
  if status in {'resolved','done','closed','completed'}: continue
  scoped.append(r); due=(created+timedelta(hours=sla_hours)).isoformat() if created else None; base={'action_id':r.get('action_id') or r.get('id'),'action_type':r.get('action_type'),'source':r.get('source'),'status':r.get('status'),'owner':r.get('owner') or r.get('owner_id'),'created_at':created.isoformat() if created else r.get('created_at'),'age_hours':round(age,2),'escalation_due_at':due}
  if age>=sla_hours and not _clean(r.get('escalated_at') or r.get('escalation_state')): findings.append({**base,'reason':'missing_escalation','detail':'action is over SLA without escalation'})
  if age>=sla_hours and not _clean(r.get('owner') or r.get('owner_id')): findings.append({**base,'reason':'unowned_over_sla','detail':'action is over SLA without owner'})
 return _finish(ARTIFACT_TYPE,gen,{'sla_hours':sla_hours,'action_type':action_type,'source':source,'limit':limit},len(scoped),findings,limit,missing_tables,missing_columns)
def build_proactive_action_escalation_sla_report_from_db(db_or_conn, **kwargs):
 conn=_conn(db_or_conn); s=_schema(conn); c=s.get('proactive_actions')
 if c is None: return build_proactive_action_escalation_sla_report([], missing_tables=['proactive_actions'], **kwargs)
 sel=[_expr(c,'id',alias='action_id'),_expr(c,'action_type'),_expr(c,'source'),_expr(c,'status'),_expr(c,'owner','owner_id',alias='owner'),_expr(c,'owner_id'),_expr(c,'created_at'),_expr(c,'escalated_at','escalation_state',alias='escalated_at')]
 return build_proactive_action_escalation_sla_report([dict(r) for r in conn.execute(f"SELECT {', '.join(sel)} FROM proactive_actions")], **kwargs)
def format_proactive_action_escalation_sla_json(report): return _json(report)
def format_proactive_action_escalation_sla_text(report): return _text('Proactive Action Escalation SLA', report)
