from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from evaluation.newsletter_issue_section_dropouts import build_newsletter_issue_section_dropouts_report_from_db, format_newsletter_issue_section_dropouts_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"newsletter_issue_section_dropouts.py"; spec=importlib.util.spec_from_file_location("newsletter_issue_section_dropouts_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_section_dropouts_and_cli(tmp_path,capsys):
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE newsletter_issues (id TEXT, planned_sections TEXT, rendered_sections TEXT, metadata TEXT)")
 c.execute("INSERT INTO newsletter_issues VALUES ('i1','[\"intro\",\"links\"]','{\"intro\":\"hello\"}','{}')")
 c.execute("INSERT INTO newsletter_issues VALUES ('i2',NULL,NULL,'{\"planned_sections\":[\"intro\"],\"rendered_sections\":{}}')")
 r=build_newsletter_issue_section_dropouts_report_from_db(c,min_repeat_count=1)
 assert {"links","intro"} <= {x["section"] for x in r["findings"]}
 assert "Section Dropouts" in format_newsletter_issue_section_dropouts_text(r)
 c.commit(); path=tmp_path/"db.sqlite"
 with sqlite3.connect(path) as out: c.backup(out)
 assert script.main(["--db",str(path),"--format","text","--min-repeat-count","1"])==0; assert "Dropouts" in capsys.readouterr().out
 assert script.main(["--db",str(path),"--min-repeat-count","0"])==2
def test_section_schema_gap(): assert build_newsletter_issue_section_dropouts_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]==["newsletter_issues"]
