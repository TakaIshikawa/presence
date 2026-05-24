#!/usr/bin/env python3
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.content_variant_localization_readiness import build_content_variant_localization_readiness_report_from_db, format_content_variant_localization_readiness_json, format_content_variant_localization_readiness_text  # noqa: E402
from runner import script_context  # noqa: E402
def _positive_int(v):
 p=int(v)
 if p<=0: raise argparse.ArgumentTypeError("value must be positive")
 return p
def _ratio(v):
 p=float(v)
 if not 0<=p<=1: raise argparse.ArgumentTypeError("value must be between 0 and 1")
 return p
def parse_args(argv=None):
 p=argparse.ArgumentParser(description=__doc__); p.add_argument('--db'); p.add_argument('--format',choices=('json','text'),default='json')
 p.add_argument('--required-locale', action='append')
 p.add_argument('--platform')
 p.add_argument('--limit', type=_positive_int, default=100)
 return p.parse_args(argv)
def main(argv=None):
 try: a=parse_args(argv)
 except SystemExit as e: return int(e.code or 0)
 kw={'required_locale':a.required_locale,'platform':a.platform,'limit':a.limit}
 try:
  if a.db:
   with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_content_variant_localization_readiness_report_from_db(c, **kw)
  else:
   with script_context() as (_config, db): r=build_content_variant_localization_readiness_report_from_db(db, **kw)
 except (OSError, sqlite3.Error, TypeError, ValueError) as e:
  print(f"error: {e}", file=sys.stderr); return 1
 print(format_content_variant_localization_readiness_text(r) if a.format=='text' else format_content_variant_localization_readiness_json(r)); return 0
if __name__=='__main__': raise SystemExit(main())
