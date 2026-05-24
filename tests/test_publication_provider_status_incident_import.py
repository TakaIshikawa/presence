from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation.publication_provider_status_incident_import import import_publication_provider_status_incidents,parse_incident_payload,format_publication_provider_status_incident_import_json,format_publication_provider_status_incident_import_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_publication_provider_status_incidents.py"; spec=importlib.util.spec_from_file_location("import_publication_provider_status_incidents_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_incident_import_json_jsonl_and_cli(tmp_path,capsys):
 rows=parse_incident_payload('[{"id":"i1","name":"Outage","status":"resolved","impact":"major","components":["api","web"],"url":"https://s"}]',"pub")
 assert rows[0]["affected_components"]=='["api", "web"]'
 c=sqlite3.connect(":memory:"); r=import_publication_provider_status_incidents(c,rows); assert r["summary"]["applied_count"]==1
 r2=import_publication_provider_status_incidents(c,parse_incident_payload('{"id":"i1","name":"Outage 2"}\n',"pub"),dry_run=True); assert r2["summary"]["updated_count"]==1 and r2["summary"]["applied_count"]==0
 assert json.loads(format_publication_provider_status_incident_import_json(r))["artifact_type"]=="publication_provider_status_incident_import"; assert "Publication Provider" in format_publication_provider_status_incident_import_text(r)
 db=tmp_path/"db.sqlite"; payload=tmp_path/"incidents.jsonl"; payload.write_text('{"id":"i2","title":"Lag"}\n')
 assert script.main([str(payload),"--db",str(db),"--provider","pub","--format","text"])==0; assert "Publication Provider" in capsys.readouterr().out
