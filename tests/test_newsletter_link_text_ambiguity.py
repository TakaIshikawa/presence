from __future__ import annotations

from evaluation.newsletter_link_text_ambiguity import build_newsletter_link_text_ambiguity_report


def test_generic_and_bare_url_anchors_are_flagged():
    report = build_newsletter_link_text_ambiguity_report([
        {"newsletter_id": "n1", "anchor_text": "here", "destination_url": "https://a.test"},
        {"newsletter_id": "n1", "anchor_text": "https://b.test", "destination_url": "https://b.test"},
    ])
    assert [row["reason_code"] for row in report["findings"]] == ["bare_url", "generic_anchor"]


def test_duplicated_anchor_with_different_urls_is_flagged_from_html():
    report = build_newsletter_link_text_ambiguity_report([
        {"newsletter_id": "n2", "html": '<a href="https://a.test">Guide</a><a href="https://b.test">Guide</a>'}
    ])
    assert {row["reason_code"] for row in report["findings"]} == {"duplicated_anchor_different_urls"}
    assert report["findings"][0]["occurrence_count"] == 1


def test_descriptive_anchor_is_ignored():
    report = build_newsletter_link_text_ambiguity_report([
        {"newsletter_id": "n3", "anchor_text": "May launch checklist", "destination_url": "https://example.test/checklist"}
    ])
    assert report["findings"] == []
