#!/usr/bin/env python3
"""Report content claim evidence domain concentration."""
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.content_claim_evidence_domain_concentration import DEFAULT_LIMIT,DEFAULT_MAX_DOMAIN_SHARE,DEFAULT_MIN_CLAIMS,build_content_claim_evidence_domain_concentration_report_from_db,format_content_claim_evidence_domain_concentration_json,format_content_claim_evidence_domain_concentration_text  # noqa:E402
from runner import script_context  # noqa:E402
def pos(v:str)->int:
 n=int(v)
 if n<=0: raise argparse.ArgumentTypeError("value must be positive")
 return n
def share(v:str)->float:
 n=float(v)
 if not 0<n<=1: raise argparse.ArgumentTypeError("value must be between 0 and 1")
 return n
def main(argv:list[str]|None=None)->int:
 p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--format",choices=("json","text"),default="json"); p.add_argument("--min-claims",type=pos,default=DEFAULT_MIN_CLAIMS); p.add_argument("--max-domain-share",type=share,default=DEFAULT_MAX_DOMAIN_SHARE); p.add_argument("--limit",type=pos,default=DEFAULT_LIMIT)
 try:
  a=p.parse_args(argv); kw={"min_claims":a.min_claims,"max_domain_share":a.max_domain_share,"limit":a.limit}
  if a.db:
   with sqlite3.connect(a.db) as c: r=build_content_claim_evidence_domain_concentration_report_from_db(c,**kw)
  else:
   with script_context() as (_c,db): r=build_content_claim_evidence_domain_concentration_report_from_db(db,**kw)
 except SystemExit as e: return int(e.code or 0)
 except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_content_claim_evidence_domain_concentration_text(r) if a.format=="text" else format_content_claim_evidence_domain_concentration_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
