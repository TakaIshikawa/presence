from __future__ import annotations

from evaluation.newsletter_section_repetition import build_newsletter_section_repetition_report


def test_flags_duplicate_titles_and_near_identical_bodies():
    rows = [
        {"issue_id": "i1", "section_id": "s1", "section_title": "Links", "section_body": "Three useful product links for launch planning."},
        {"issue_id": "i1", "section_id": "s2", "section_title": "Links", "section_body": "A different link roundup."},
        {"issue_id": "i2", "section_id": "s3", "section_title": "Notes", "section_body": "Launch notes explain retry budgets and approval checks for teams."},
        {"issue_id": "i2", "section_id": "s4", "section_title": "Learned", "section_body": "Launch notes explain retry budgets and approval checks for builders."},
    ]

    report = build_newsletter_section_repetition_report(rows, similarity_threshold=0.75)

    assert [issue["issue_id"] for issue in report["issues"]] == ["i2", "i1"]
    assert report["issues"][1]["repeated_section_title"] == "Links"
    assert report["issues"][1]["section_count"] == 2
    assert report["issues"][0]["max_similarity"] >= 0.75
    assert report["issues"][0]["representative_section_ids"] == ["s3", "s4"]


def test_distinct_sections_are_ignored():
    report = build_newsletter_section_repetition_report(
        [
            {"issue_id": "i1", "section_id": "s1", "section_title": "Shipped", "section_body": "A concrete release note."},
            {"issue_id": "i1", "section_id": "s2", "section_title": "Reading", "section_body": "A book excerpt and source notes."},
        ],
        similarity_threshold=0.8,
    )

    assert report["issues"] == []
    assert report["empty_state"]["is_empty"] is True


def test_threshold_and_limit_behavior():
    rows = [
        {"issue_id": "i1", "section_id": "a", "section_body": "Alpha beta gamma delta epsilon."},
        {"issue_id": "i1", "section_id": "b", "section_body": "Alpha beta gamma delta zeta."},
        {"issue_id": "i2", "section_id": "c", "section_title": "Repeat", "section_body": "One."},
        {"issue_id": "i2", "section_id": "d", "section_title": "Repeat", "section_body": "Two."},
    ]

    strict = build_newsletter_section_repetition_report(rows, similarity_threshold=0.95)
    limited = build_newsletter_section_repetition_report(rows, similarity_threshold=0.5, limit=1)

    assert [issue["issue_id"] for issue in strict["issues"]] == ["i2"]
    assert len(limited["issues"]) == 1
    assert limited["summary"]["flagged_issue_count"] == 2
