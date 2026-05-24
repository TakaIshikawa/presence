#!/usr/bin/env python3
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))
from evaluation.blog_draft_social_card_export import build_blog_draft_social_card_export_report_from_db, format_blog_draft_social_card_export_json, format_blog_draft_social_card_export_text  # noqa: E402
from runner import script_context  # noqa: E402
def _positive_int(v):
 p=int(v)
 if p<=0: raise argparse.ArgumentTypeError('value must be positive')
 return p
def parse_args(argv=None):
 p=argparse.ArgumentParser(description=__doc__); p.add_argument('--db'); p.add_argument('--draft-id'); p.add_argument('--platform', action='append'); p.add_argument('--format', choices=('json','text'), default='json'); p.add_argument('--limit', type=_positive_int, default=100); return p.parse_args(argv)
def main(argv=None):
 try: a=parse_args(argv)
 except SystemExit as e: return int(e.code or 0)
 kw={'platform':a.platform,'limit':a.limit}
 try:
  if a.db:
   with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_blog_draft_social_card_export_report_from_db(c, **kw)
  else:
   with script_context() as (_config, db): r=build_blog_draft_social_card_export_report_from_db(db, **kw)
  if a.draft_id: r['packets']=[p for p in r.get('packets',[]) if str(p.get('draft_id'))==str(a.draft_id)]; r['findings']=r['packets']; r['totals']['packets']=len(r['packets'])
 except (OSError, sqlite3.Error, TypeError, ValueError) as e:
  print(f'error: {e}', file=sys.stderr); return 1
 print(format_blog_draft_social_card_export_text(r) if a.format=='text' else format_blog_draft_social_card_export_json(r)); return 0
if __name__=='__main__': raise SystemExit(main())
