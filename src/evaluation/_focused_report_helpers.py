
from __future__ import annotations
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json, re, sqlite3
from statistics import median
from typing import Any

DEFAULT_LIMIT=100
__all__ = [
    "Any",
    "Counter",
    "DEFAULT_LIMIT",
    "defaultdict",
    "datetime",
    "json",
    "median",
    "re",
    "sqlite3",
    "timedelta",
    "timezone",
    "_clean",
    "_conn",
    "_dt",
    "_expr",
    "_finish",
    "_float",
    "_groups",
    "_int",
    "_json",
    "_lower",
    "_nonneg",
    "_now",
    "_positive",
    "_schema",
    "_text",
]

def _conn(db_or_conn: Any) -> sqlite3.Connection:
    if isinstance(db_or_conn, sqlite3.Connection):
        db_or_conn.row_factory=sqlite3.Row
        return db_or_conn
    conn=sqlite3.connect(db_or_conn); conn.row_factory=sqlite3.Row; return conn

def _schema(conn):
    rows=conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {r[0]: {c[1] for c in conn.execute(f"PRAGMA table_info({r[0]})")} for r in rows}

def _expr(cols,*names,default='NULL',alias=None,prefix=''):
    for n in names:
        if n in cols:
            val=f"{prefix}{n}" if prefix else n
            return f"{val} AS {alias or names[0]}"
    return f"{default} AS {alias or names[0]}"

def _clean(v):
    return str(v).strip() if v is not None else ''

def _lower(v): return _clean(v).lower()
def _int(v):
    try: return int(v) if v is not None and str(v).strip()!='' else None
    except (TypeError,ValueError): return None
def _float(v):
    try: return float(v) if v is not None and str(v).strip()!='' else None
    except (TypeError,ValueError): return None

def _dt(v):
    if isinstance(v, datetime): return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    t=_clean(v)
    if not t: return None
    try: return datetime.fromisoformat(t.replace('Z','+00:00')).astimezone(timezone.utc)
    except ValueError: return None

def _now(now=None): return _dt(now) if now is not None else datetime.now(timezone.utc)
def _positive(name,v):
    if v<=0: raise ValueError(f"{name} must be positive")
def _nonneg(name,v):
    if v<0: raise ValueError(f"{name} must be non-negative")

def _groups(findings):
    d=defaultdict(list)
    for f in findings: d[f['reason']].append(f)
    return [{'reason':k,'count':len(v),'items':v} for k,v in sorted(d.items())]

def _finish(artifact, generated_at, filters, scanned, findings, limit, missing_tables=None, missing_columns=None, totals_extra=None):
    findings=sorted(findings,key=lambda f:(str(f.get('reason')), str(f.get('claim_id') or f.get('send_id') or f.get('attempt_id') or f.get('queue_id') or f.get('prompt_name') or f.get('segment') or f.get('reply_id') or f.get('action_id') or f.get('canonical_url') or ''), str(f.get('created_at') or f.get('checked_at') or f.get('scheduled_at') or '')))
    shown=findings[:limit]
    totals={'row_count':scanned,'finding_count':len(findings),'shown_count':len(shown),'by_reason':dict(Counter(f['reason'] for f in findings))}
    if totals_extra: totals.update(totals_extra)
    return {'artifact_type':artifact,'generated_at':generated_at.isoformat(),'filters':filters,'totals':totals,'findings':_groups(shown),'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items()) if v},'empty_state':{'is_empty':not findings,'message':f'No {artifact.replace("_"," ")} findings found.' if not findings else None}}

def _json(report): return json.dumps(report,indent=2,sort_keys=True)
def _text(title, report):
    lines=[title, f"Generated: {report['generated_at']}", 'Filters: '+', '.join(f"{k}={v}" for k,v in report['filters'].items()), f"Totals: rows={report['totals']['row_count']} findings={report['totals']['finding_count']} shown={report['totals']['shown_count']}"]
    if report['missing_tables']: lines.append('Missing tables: '+', '.join(report['missing_tables']))
    if report['missing_columns']: lines.append('Missing columns: '+', '.join(f"{t}={','.join(c)}" for t,c in report['missing_columns'].items()))
    if not report['findings']:
        lines.append(report['empty_state']['message']); return '\n'.join(lines)
    for g in report['findings']:
        lines.append(''); lines.append(f"{g['reason']} ({g['count']})")
        for item in g['items']:
            detail=item.get('detail') or ''
            ident=item.get('claim_id') or item.get('send_id') or item.get('attempt_id') or item.get('queue_id') or item.get('prompt_name') or item.get('segment') or item.get('reply_id') or item.get('action_id') or item.get('canonical_url')
            lines.append(f"- {ident}: {detail}")
    return '\n'.join(lines)
