from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.buttondown_device_client_import import parse_buttondown_device_clients, upsert_buttondown_device_clients
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_buttondown_device_clients.py"; spec=importlib.util.spec_from_file_location("import_buttondown_device_clients_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_buttondown_device_clients_aliases_normalization_counts(tmp_path):
    rows=parse_buttondown_device_clients("campaign_slug,event_date,email_client,device_type,opens,clicks,recipients\nSpring,2026-05-01,Gmail,Mobile,2.0,bad,10\n")
    assert rows[0]["campaign_id"]=="Spring"; assert rows[0]["email_client"]=="gmail"; assert rows[0]["device_type"]=="mobile"; assert rows[0]["clicks"]==0
    c=sqlite3.connect(":memory:"); upsert_buttondown_device_clients(c,rows); upsert_buttondown_device_clients(c,[{**rows[0],"opens":9}])
    assert c.execute("SELECT COUNT(*),opens FROM buttondown_device_clients").fetchone()==(1,9)
    assert parse_buttondown_device_clients('{"campaign_id":"c","event_date":"d"}')[0]["device_type"]=="unknown"
    p=tmp_path/"bd.json"; p.write_text('{"campaign_id":"c","event_date":"d"}'); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--dry-run"])==0
