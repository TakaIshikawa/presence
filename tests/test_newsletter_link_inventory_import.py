from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.newsletter_link_inventory_import import parse_newsletter_link_inventory, upsert_newsletter_link_inventory
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_newsletter_link_inventory.py"; spec=importlib.util.spec_from_file_location("import_newsletter_link_inventory_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_link_inventory_import_cli(tmp_path):
    raw='{"issue_id":"i1","url":"HTTPS://Example.COM/a?utm=1#frag","link_text":"A","observed_at":"2026-05-01"}\n'; rows=parse_newsletter_link_inventory(raw); assert rows[0]["url"]=="https://example.com/a?utm=1"
    c=sqlite3.connect(":memory:"); upsert_newsletter_link_inventory(c,rows); upsert_newsletter_link_inventory(c,[{**rows[0],"section":"top"}]); assert c.execute("SELECT section FROM newsletter_link_inventory").fetchone()[0]=="top"
    p=tmp_path/"links.jsonl"; p.write_text(raw); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--dry-run","--format","text"])==0
