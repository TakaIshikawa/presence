"""Tests for final queued-publication clearance checks."""

from types import SimpleNamespace

from output.publish_clearance import (
    ALT_TEXT_FAILED,
    MISSING_ATTRIBUTION,
    PERSONA_GUARD_FAILED,
    UNSUPPORTED_CLAIMS,
    check_publication_clearance,
)


class FakeDb:
    def __init__(self, claim_summary=None, persona_summary=None):
        self.claim_summary = claim_summary
        self.persona_summary = persona_summary

    def get_claim_check_summary(self, content_id):
        return self.claim_summary

    def get_persona_guard_summary(self, content_id):
        return self.persona_summary


def _item(**overrides):
    item = {
        "content_id": 42,
        "content": "I shipped a small queue fix.",
        "content_type": "x_post",
        "image_path": None,
        "image_prompt": None,
        "image_alt_text": None,
    }
    item.update(overrides)
    return item


def _attribution_result(blocked=False):
    return SimpleNamespace(
        as_dict=lambda: {
            "status": "blocked" if blocked else "passed",
            "passed": not blocked,
            "blocked": blocked,
            "required_sources": [],
            "missing_sources": [{"knowledge_id": 7}] if blocked else [],
        }
    )


def test_clearance_blocks_unsupported_claim_summary(monkeypatch):
    monkeypatch.setattr(
        "output.publish_clearance.check_publication_attribution_guard",
        lambda db, content_id, text: _attribution_result(),
    )
    db = FakeDb(claim_summary={"supported_count": 1, "unsupported_count": 1})

    result = check_publication_clearance(db, _item())

    assert result.blocked is True
    assert result.hold_reason == UNSUPPORTED_CLAIMS
    assert result.checks["claim_check"]["unsupported_count"] == 1


def test_clearance_blocks_failed_persona_guard(monkeypatch):
    monkeypatch.setattr(
        "output.publish_clearance.check_publication_attribution_guard",
        lambda db, content_id, text: _attribution_result(),
    )
    db = FakeDb(
        claim_summary={"supported_count": 1, "unsupported_count": 0},
        persona_summary={
            "checked": True,
            "passed": False,
            "status": "failed",
            "score": 0.2,
            "reasons": ["banned tone markers: unlock"],
            "metrics": {},
        },
    )

    result = check_publication_clearance(db, _item())

    assert result.blocked is True
    assert result.hold_reason == PERSONA_GUARD_FAILED


def test_clearance_blocks_missing_attribution(monkeypatch):
    monkeypatch.setattr(
        "output.publish_clearance.check_publication_attribution_guard",
        lambda db, content_id, text: _attribution_result(blocked=True),
    )
    db = FakeDb(
        claim_summary={"supported_count": 1, "unsupported_count": 0},
        persona_summary={"checked": True, "passed": True, "status": "passed"},
    )

    result = check_publication_clearance(db, _item(), platform_texts={"x": "copy"})

    assert result.blocked is True
    assert result.hold_reason == MISSING_ATTRIBUTION
    assert result.checks["attribution_guard"]["x"]["blocked"] is True


def test_clearance_blocks_failed_alt_text_in_strict_mode(monkeypatch):
    monkeypatch.setattr(
        "output.publish_clearance.check_publication_attribution_guard",
        lambda db, content_id, text: _attribution_result(),
    )
    db = FakeDb(
        claim_summary={"supported_count": 1, "unsupported_count": 0},
        persona_summary={"checked": True, "passed": True, "status": "passed"},
    )

    result = check_publication_clearance(
        db,
        _item(content_type="x_visual", image_path="/tmp/visual.png", image_alt_text=""),
        alt_text_guard_mode="strict",
    )

    assert result.blocked is True
    assert result.hold_reason == ALT_TEXT_FAILED


def test_clearance_allows_failed_alt_text_in_warning_mode(monkeypatch):
    monkeypatch.setattr(
        "output.publish_clearance.check_publication_attribution_guard",
        lambda db, content_id, text: _attribution_result(),
    )
    db = FakeDb(
        claim_summary={"supported_count": 1, "unsupported_count": 0},
        persona_summary={"checked": True, "passed": True, "status": "passed"},
    )

    result = check_publication_clearance(
        db,
        _item(content_type="x_visual", image_path="/tmp/visual.png", image_alt_text=""),
        alt_text_guard_mode="warning",
    )

    assert result.passed is True
    assert result.hold_reason is None
    assert result.checks["alt_text"]["status"] == "failed"


def test_clearance_hold_reasons_are_short_stable_strings():
    reasons = {
        UNSUPPORTED_CLAIMS,
        PERSONA_GUARD_FAILED,
        MISSING_ATTRIBUTION,
        ALT_TEXT_FAILED,
    }

    assert reasons == {
        "unsupported_claims",
        "persona_guard_failed",
        "missing_attribution",
        "alt_text_failed",
    }
    assert all(" " not in reason and len(reason) <= 32 for reason in reasons)
