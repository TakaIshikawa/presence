"""Tests for newsletter CTA rotation planning."""

from __future__ import annotations

import importlib.util
import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.newsletter_cta import (
    CtaCandidate,
    fetch_recent_newsletter_sends,
    plan_newsletter_cta,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_cta.py"
spec = importlib.util.spec_from_file_location("newsletter_cta_script", SCRIPT_PATH)
newsletter_cta_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(newsletter_cta_script)


BASE_TIME = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _candidate(
    cta_id: str,
    *,
    tags: list[str] | None = None,
    cooldown: int = 1,
    priority: float = 0.0,
) -> CtaCandidate:
    return CtaCandidate(
        id=cta_id,
        label=cta_id.title(),
        text=f"Read {cta_id}",
        campaign_tags=tuple(tags or []),
        cooldown_count=cooldown,
        priority_weight=priority,
    )


def _insert_send(db, issue_id: str, cta_id: str, offset: int) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=f"Subject {issue_id}",
        content_ids=[],
        metadata={"cta": {"id": cta_id}},
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        ((BASE_TIME - timedelta(days=offset)).isoformat(), send_id),
    )
    db.conn.commit()
    return send_id


def test_planner_avoids_ctas_within_configured_cooldown_window(db):
    _insert_send(db, "issue-1", "demo", 0)
    _insert_send(db, "issue-2", "signup", 1)
    recent_sends = fetch_recent_newsletter_sends(db, limit=3)
    candidates = [
        _candidate("demo", tags=["launch"], cooldown=1, priority=100),
        _candidate("signup", tags=["launch"], cooldown=2, priority=80),
        _candidate("survey", tags=["launch"], cooldown=1, priority=1),
    ]

    selection = plan_newsletter_cta(
        candidates,
        recent_sends=recent_sends,
        campaign_tags=["launch"],
    )

    assert selection.selected.id == "survey"
    assert selection.blocked_candidate_ids == ("demo", "signup")
    assert selection.recent_cta_ids == ("demo", "signup")


def test_planner_prefers_campaign_matches_then_priority_weight():
    candidates = [
        _candidate("generic", priority=100),
        _candidate("launch-low", tags=["launch"], priority=1),
        _candidate("launch-high", tags=["launch"], priority=5),
    ]

    selection = plan_newsletter_cta(candidates, campaign_tags=["launch"])

    assert selection.selected.id == "launch-high"
    assert selection.scores["generic"]["campaign_match_count"] == 0
    assert selection.scores["launch-high"]["campaign_matches"] == ["launch"]


def test_invalid_candidate_files_return_cli_error(tmp_path, capsys):
    path = tmp_path / "ctas.yaml"
    path.write_text("not_candidates: true\n", encoding="utf-8")

    exit_code = newsletter_cta_script.main([str(path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "candidate file must contain" in captured.err


def test_deterministic_tie_breaking_uses_candidate_id():
    candidates = [
        _candidate("zeta", tags=["launch"], priority=10),
        _candidate("alpha", tags=["launch"], priority=10),
    ]

    first = plan_newsletter_cta(candidates, campaign_tags=["launch"])
    second = plan_newsletter_cta(list(reversed(candidates)), campaign_tags=["launch"])

    assert first.selected.id == "alpha"
    assert second.selected.id == "alpha"


def test_cli_accepts_json_and_yaml_candidate_files(db, tmp_path, capsys):
    _insert_send(db, "issue-1", "demo", 0)
    yaml_path = tmp_path / "ctas.yaml"
    yaml_path.write_text(
        """
candidates:
  - id: demo
    label: Demo
    campaign_tags: [launch]
    priority_weight: 10
  - id: signup
    label: Signup
    campaign_tags: [launch]
    priority_weight: 1
""".strip(),
        encoding="utf-8",
    )
    json_path = tmp_path / "ctas.json"
    json_path.write_text(
        json.dumps(
            [
                {"id": "demo", "label": "Demo", "priority_weight": 10},
                {"id": "signup", "label": "Signup", "priority_weight": 1},
            ]
        ),
        encoding="utf-8",
    )

    with patch.object(
        newsletter_cta_script,
        "script_context",
        return_value=_script_context(db),
    ):
        text_code = newsletter_cta_script.main(
            [str(yaml_path), "--campaign-tag", "launch", "--format", "text"]
        )
    text_out = capsys.readouterr().out

    with patch.object(
        newsletter_cta_script,
        "script_context",
        return_value=_script_context(db),
    ):
        json_code = newsletter_cta_script.main([str(json_path), "--format", "json"])
    json_out = capsys.readouterr().out

    assert text_code == 0
    assert "Selected CTA: Signup (signup)" in text_out
    assert json_code == 0
    assert json.loads(json_out)["selected"]["id"] == "signup"
