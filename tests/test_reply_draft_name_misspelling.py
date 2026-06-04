from __future__ import annotations

from evaluation.reply_draft_name_misspelling import build_reply_draft_name_misspelling_report


def test_close_misspellings_are_flagged():
    report = build_reply_draft_name_misspelling_report([
        {"draft_id": "d1", "recipient_name": "Martha Jones", "draft_text": "Thanks Marhta, this helps."}
    ])
    assert report["findings"][0]["expected_name"] == "Martha Jones"
    assert report["findings"][0]["matched_text"] == "Marhta"
    assert report["findings"][0]["edit_distance"] <= 2


def test_exact_names_handles_and_no_usage_are_ignored():
    report = build_reply_draft_name_misspelling_report([
        {"draft_id": "exact", "recipient_name": "Martha Jones", "draft_text": "Thanks Martha, this helps."},
        {"draft_id": "handle", "handle": "taka", "draft_text": "Thanks @taka for the note."},
        {"draft_id": "none", "recipient_name": "Martha Jones", "draft_text": "Thanks for the note."},
    ])
    assert report["findings"] == []


def test_handle_misspelling_is_flagged():
    report = build_reply_draft_name_misspelling_report([
        {"draft_id": "d2", "handle": "alexdev", "draft_text": "Looping in @alexdv here."}
    ])
    assert report["findings"][0]["matched_text"] == "@alexdv"
