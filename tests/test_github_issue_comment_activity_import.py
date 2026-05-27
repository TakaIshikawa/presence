from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from ingestion.github_issue_comment_activity_import import *
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_github_issue_comment_activity.py"; spec=importlib.util.spec_from_file_location("s",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_github_issue_comment_import_and_cli(tmp_path,capsys):
 rows=parse_github_issue_comment_activity_payload('[{"repository":"Owner/Repo","issue_number":"2","comment_id":"9","author":"me","created_at":"2026","is_pr":"true"}]'); assert rows[0]["repository"]=="owner/repo" and rows[0]["is_pr"]==1
 c=sqlite3.connect(":memory:"); assert import_github_issue_comment_activity(c,rows)["summary"]["inserted_count"]==1; assert import_github_issue_comment_activity(c,rows,dry_run=True)["summary"]["updated_count"]==1
 p=tmp_path/"in.json"; p.write_text(json.dumps(rows)); db=tmp_path/"d.sqlite"; assert script.main([str(p),"--db",str(db),"--format","json"])==0; assert json.loads(capsys.readouterr().out)["artifact_type"]=="github_issue_comment_activity_import"
