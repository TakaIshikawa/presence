"""Tests for local config drift auditing."""

from __future__ import annotations

import json

import yaml

from config_drift import audit_config_drift
from scripts import config_drift as config_drift_script


def _write_yaml(path, data: dict) -> str:
    path.write_text(yaml.safe_dump(data, sort_keys=True))
    return str(path)


def _findings_by_kind(result: dict) -> dict[str, list[dict]]:
    findings: dict[str, list[dict]] = {}
    for finding in result["findings"]:
        findings.setdefault(finding["kind"], []).append(finding)
    return findings


def test_audit_reports_supported_drift_categories_without_secret_values(
    tmp_path,
    monkeypatch,
):
    monkeypatch.delenv("SERVICE_TOKEN", raising=False)
    reference = {
        "service": {
            "api_key": "${SERVICE_TOKEN}",
            "enabled": True,
            "retries": 3,
            "nested": {"required_name": "presence"},
        },
        "paths": {"database": "./presence.db"},
    }
    local = {
        "service": {
            "api_key": "${SERVICE_TOKEN}",
            "enabled": "yes",
            "nested": {"required_name": ""},
            "extra_setting": "local-only",
        },
        "paths": {"database": "./presence.db"},
    }

    result = audit_config_drift(
        _write_yaml(tmp_path / "config.yaml", reference),
        _write_yaml(tmp_path / "config.local.yaml", local),
    )

    findings = _findings_by_kind(result)
    assert findings["missing"][0]["path"] == "service.retries"
    assert findings["extra"][0]["path"] == "service.extra_setting"
    assert findings["unresolved-env"][0]["path"] == "service.api_key"
    assert findings["empty-value"][0]["path"] == "service.nested.required_name"
    assert findings["type-mismatch"][0]["path"] == "service.enabled"
    assert result["blocking_count"] == 4
    assert result["status"] == "failed"

    payload = json.dumps(result, sort_keys=True)
    assert "actual-secret" not in payload
    assert "${SERVICE_TOKEN}" not in payload
    assert findings["unresolved-env"][0]["redacted"] is True


def test_matching_configs_pass_with_stable_json(tmp_path, capsys):
    reference = {
        "github": {"token": "${GITHUB_TOKEN}", "repositories": []},
        "polling": {"enabled": True},
    }
    local = {
        "github": {"token": "actual-secret", "repositories": []},
        "polling": {"enabled": True},
    }
    config_path = _write_yaml(tmp_path / "config.yaml", reference)
    local_path = _write_yaml(tmp_path / "config.local.yaml", local)

    exit_code = config_drift_script.main(
        ["--config", config_path, "--local-config", local_path, "--json", "--strict"]
    )

    output = capsys.readouterr().out
    payload = json.loads(output)
    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["findings"] == []
    assert json.loads(output) == json.loads(
        json.dumps(payload, indent=2, sort_keys=True)
    )
    assert "actual-secret" not in output


def test_text_output_is_readable_and_strict_fails_for_blocking_drift(
    tmp_path,
    capsys,
):
    reference = {"x": {"api_secret": "${X_API_SECRET}", "access_token": "token"}}
    local = {"x": {"api_secret": "", "access_token": "token", "extra": True}}
    config_path = _write_yaml(tmp_path / "config.yaml", reference)
    local_path = _write_yaml(tmp_path / "config.local.yaml", local)

    exit_code = config_drift_script.main(
        ["--config", config_path, "--local-config", local_path, "--strict"]
    )

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "Config drift audit: failed" in output
    assert "empty-value x.api_secret" in output
    assert "[redacted]" in output
    assert "${X_API_SECRET}" not in output


def test_extra_only_drift_does_not_fail_strict_mode(tmp_path):
    reference = {"paths": {"database": "./presence.db"}}
    local = {"paths": {"database": "./presence.db", "scratch": "/tmp/presence"}}
    result = audit_config_drift(
        _write_yaml(tmp_path / "config.yaml", reference),
        _write_yaml(tmp_path / "config.local.yaml", local),
    )

    assert result["blocking_count"] == 0
    assert result["status"] == "passed"
    assert result["counts"]["extra"] == 1
