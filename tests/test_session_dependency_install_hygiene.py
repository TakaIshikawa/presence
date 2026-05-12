from __future__ import annotations

import pytest

from synthesis.session_dependency_install_hygiene import (
    analyze_session_dependency_install_hygiene,
)


def test_empty_input_returns_zeroed_metrics():
    assert analyze_session_dependency_install_hygiene(None) == {
        "install_command_count": 0,
        "sessions_with_installs": 0,
        "inspected_manifest_before_install": 0,
        "manifest_or_lockfile_updated": 0,
        "risky_global_installs": 0,
        "examples": [],
    }


def test_rejects_non_list_input():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_session_dependency_install_hygiene({"command": "pip install requests"})


def test_python_install_with_manifest_inspection_and_lockfile_update():
    records = [
        {
            "session_id": "s1",
            "tool_calls": [
                {"tool": "bash", "command": "sed -n '1,120p' pyproject.toml"},
                {"tool": "bash", "command": "python -m pip install -r requirements.txt"},
                {
                    "tool": "functions.apply_patch",
                    "input": "*** Begin Patch\n*** Update File: uv.lock\n",
                },
            ],
        }
    ]

    result = analyze_session_dependency_install_hygiene(records)

    assert result["install_command_count"] == 1
    assert result["sessions_with_installs"] == 1
    assert result["inspected_manifest_before_install"] == 1
    assert result["manifest_or_lockfile_updated"] == 1
    assert result["risky_global_installs"] == 0
    assert result["examples"][0]["uses_project_local_or_isolated_flags"] is True


def test_counts_risky_global_installs_for_pip_npm_brew_and_apt():
    records = [
        {"session_id": "s1", "tool": "bash", "command": "pip install black"},
        {"session_id": "s1", "tool": "bash", "command": "npm install -g eslint"},
        {"session_id": "s2", "tool": "bash", "command": "brew install ripgrep"},
        {"session_id": "s3", "tool": "bash", "command": "sudo apt-get install jq"},
    ]

    result = analyze_session_dependency_install_hygiene(records)

    assert result["install_command_count"] == 4
    assert result["sessions_with_installs"] == 3
    assert result["risky_global_installs"] == 4


def test_recognizes_common_javascript_local_installs():
    records = [
        {"session_id": "s1", "tool": "bash", "command": "cat package.json"},
        {"session_id": "s1", "tool": "bash", "command": "npm install react"},
        {"session_id": "s1", "tool": "bash", "command": "pnpm add zod"},
        {"session_id": "s1", "tool": "bash", "command": "yarn add vite"},
    ]

    result = analyze_session_dependency_install_hygiene(records)

    assert result["install_command_count"] == 3
    assert result["inspected_manifest_before_install"] == 3
    assert result["risky_global_installs"] == 0
    assert all(example["uses_project_local_or_isolated_flags"] for example in result["examples"])


def test_uv_add_is_project_local_and_manifest_update_stops_at_next_install():
    records = [
        {"session_id": "s1", "tool": "bash", "command": "rg dependencies pyproject.toml"},
        {"session_id": "s1", "tool": "bash", "command": "uv add pytest"},
        {"session_id": "s1", "tool": "bash", "command": "pip install requests"},
        {"session_id": "s1", "tool": "functions.apply_patch", "input": "*** Update File: pyproject.toml\n"},
    ]

    result = analyze_session_dependency_install_hygiene(records)

    assert result["install_command_count"] == 2
    assert result["manifest_or_lockfile_updated"] == 1
    assert result["risky_global_installs"] == 1
    assert result["examples"][0]["manager"] == "uv"
    assert result["examples"][0]["manifest_or_lockfile_updated"] is False
