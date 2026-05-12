from __future__ import annotations

import pytest

from synthesis.pack_branch_rebase_behavior import analyze_pack_branch_rebase_behavior


def test_empty_input_returns_zeroed_metrics():
    assert analyze_pack_branch_rebase_behavior(None) == {
        "total_packs": 0,
        "packs_with_update_signal": 0,
        "packs_with_rebase": 0,
        "packs_with_conflicts": 0,
        "packs_verified_after_rebase": 0,
        "stale_merge_risk_examples": [],
    }


def test_rejects_non_list_input():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_pack_branch_rebase_behavior({"commands": []})


def test_recognizes_fetch_rebase_conflict_and_verification_variants():
    records = [
        {
            "pack_id": "p1",
            "commands": [
                "git fetch origin",
                "git pull --rebase origin main",
                "resolve conflicts in src/app.py",
                "pytest tests/test_app.py -q",
                "git merge origin/main",
            ],
        },
        {
            "pack_id": "p2",
            "tool_calls": [
                {"cmd": "git rebase origin/main"},
                {"cmd": "npm test"},
            ],
        },
    ]

    result = analyze_pack_branch_rebase_behavior(records)

    assert result["total_packs"] == 2
    assert result["packs_with_update_signal"] == 2
    assert result["packs_with_rebase"] == 2
    assert result["packs_with_conflicts"] == 1
    assert result["packs_verified_after_rebase"] == 2
    assert result["stale_merge_risk_examples"] == []


def test_reports_merge_without_prior_update_signal():
    records = [
        {"pack_id": "stale", "commands": ["pytest -q", "git merge main"]},
        {"pack_id": "fresh", "commands": ["git fetch", "git merge origin/main"]},
    ]

    result = analyze_pack_branch_rebase_behavior(records)

    assert result["packs_with_update_signal"] == 1
    assert result["stale_merge_risk_examples"] == [
        {
            "pack_id": "stale",
            "merge_command": "git merge main",
            "reason": "merged_without_prior_update_signal",
        }
    ]
