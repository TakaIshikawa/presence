from __future__ import annotations
from contextlib import contextmanager
import importlib.util, json, sqlite3
from pathlib import Path
from types import SimpleNamespace
import pytest
from evaluation.content_variant_localization_readiness import build_content_variant_localization_readiness_report, build_content_variant_localization_readiness_report_from_db
SCRIPT_PATH=Path(__file__).resolve().parent.parent/'scripts'/'content_variant_localization_readiness.py'
spec=importlib.util.spec_from_file_location('content_variant_localization_readiness_script', SCRIPT_PATH); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
@contextmanager
def _script_context(db): yield SimpleNamespace(), db

def test_builder_and_cli(tmp_path, monkeypatch, capsys):
 r=build_content_variant_localization_readiness_report([{'content_id':1,'locale':'','platform':'x','body':'a'},{'content_id':1,'locale':'ja','platform':'x','body':'same','source_body':'same','cta':''}], required_locale=['fr'])
 assert r['artifact_type']=='content_variant_localization_readiness'; assert {'missing_locale','untranslated_body','missing_localized_cta','missing_platform_locale_variant'} <= {f['reason'] for f in r['findings']}
 assert build_content_variant_localization_readiness_report_from_db(sqlite3.connect(':memory:'))['missing_tables']==['content_variants']
 p=tmp_path/'d.sqlite'; db=sqlite3.connect(p); db.executescript("CREATE TABLE content_variants(id INTEGER, content_id INTEGER, locale TEXT, platform TEXT, body TEXT, source_body TEXT, cta TEXT); INSERT INTO content_variants VALUES(1,1,'ja','x','same','same','');"); db.close()
 assert script.main(['--db',str(p),'--format','json','--required-locale','fr'])==0; assert json.loads(capsys.readouterr().out)['artifact_type']=='content_variant_localization_readiness'
 monkeypatch.setattr(script,'script_context',lambda:_script_context(sqlite3.connect(':memory:'))); assert script.main(['--format','json'])==0
 with pytest.raises(SystemExit): script.parse_args(['--limit','0'])
