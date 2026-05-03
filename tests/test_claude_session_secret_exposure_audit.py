"""Tests for Claude session secret exposure auditing."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

from ingestion.claude_session_secret_exposure_audit import (
    build_claude_session_secret_exposure_audit_report,
    format_claude_session_secret_exposure_audit_json,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "claude_session_secret_exposure_audit.py"
)
spec = importlib.util.spec_from_file_location("claude_session_secret_exposure_audit_script", SCRIPT_PATH)
claude_session_secret_exposure_audit_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_secret_exposure_audit_script)


def test_detects_api_key_bearer_private_key_and_env_assignment():
    raw_api_key = "sk-" + "A" * 28
    raw_bearer = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
    raw_env = "abc1234567890SECRET"
    rows = [
        {
            "session_id": "sess-secrets",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Bash",
            "command": f"curl -H 'Authorization: Bearer {raw_bearer}' https://example.test",
        },
        {
            "session_id": "sess-secrets",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "content": f"Standalone key {raw_api_key}\nSERVICE_TOKEN={raw_env}",
        },
        {
            "session_id": "sess-secrets",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "content": "-----BEGIN PRIVATE KEY-----\nredacted body omitted",
        },
    ]

    report = build_claude_session_secret_exposure_audit_report(rows, now=NOW)
    families = {finding.secret_family for finding in report.findings}
    evidence_blob = json.dumps(format_claude_session_secret_exposure_audit_json(report))

    assert families == {"api_key", "bearer_token", "env_secret", "private_key"}
    assert report.totals["critical_severity_count"] == 1
    assert report.totals["high_severity_count"] == 3
    assert raw_api_key not in evidence_blob
    assert raw_bearer not in evidence_blob
    assert raw_env not in evidence_blob


def test_benign_token_like_words_are_ignored():
    rows = [
        {
            "session_id": "sess-benign",
            "content": "The design tokenization step produced api_key_placeholder and bearer-like wording.",
        },
        {
            "session_id": "sess-benign",
            "command": "echo TOKEN=short",
        },
    ]

    report = build_claude_session_secret_exposure_audit_report(rows, now=NOW)

    assert report.findings == ()
    assert report.totals["finding_count"] == 0


def test_scans_nested_tool_inputs_and_redacts_middle():
    secret = "abc1234567890SECRET"
    rows = [
        {
            "session_id": "sess-nested",
            "metadata": {
                "tool_use": {
                    "name": "Bash",
                    "input": {"command": f"printf 'SERVICE_SECRET={secret}'"},
                }
            },
        }
    ]

    report = build_claude_session_secret_exposure_audit_report(rows, now=NOW)
    evidence = report.findings[0].evidence

    assert report.findings[0].source_field in {
        "metadata.tool_use.input.command",
        "tool_use.input.command",
    }
    assert "abc1...[REDACTED]...CRET" in evidence
    assert secret not in evidence


def test_cli_reads_jsonl_and_emits_json(capsys, tmp_path):
    input_path = tmp_path / "session.jsonl"
    secret = "sk-" + "B" * 28
    input_path.write_text(
        json.dumps(
            {
                "session_id": "sess-cli",
                "timestamp": "2026-05-01T10:00:00+00:00",
                "content": f"OPENAI_API_KEY={secret}",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    assert claude_session_secret_exposure_audit_script.main([str(input_path)]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["findings"][0]["session_id"] == "sess-cli"
    assert payload["findings"][0]["secret_family"] in {"api_key", "env_secret"}
    assert secret not in json.dumps(payload)
    assert claude_session_secret_exposure_audit_script.main([str(input_path), "--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
