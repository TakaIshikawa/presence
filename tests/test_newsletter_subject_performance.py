"""Tests for newsletter subject performance scoring."""

from evaluation.newsletter_subject_performance import (
    NewsletterSubjectPerformance,
    score_subject_performance,
)


def _record_issue(db, issue_id, subject, opens, clicks, subscriber_count=100):
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=subject,
        content_ids=[1],
        subscriber_count=subscriber_count,
    )
    db.insert_newsletter_subject_candidates(
        [
            {"subject": subject, "score": 8.0, "rationale": "selected"},
            {"subject": f"{subject} alt", "score": 7.0, "rationale": "alternative"},
        ],
        content_ids=[1],
        selected_subject=subject,
        newsletter_send_id=send_id,
        issue_id=issue_id,
    )
    db.insert_newsletter_engagement(
        newsletter_send_id=send_id,
        issue_id=issue_id,
        opens=opens,
        clicks=clicks,
        unsubscribes=0,
    )


def test_score_subject_performance_weights_clicks_and_penalizes_unsubscribes():
    score = score_subject_performance(
        open_rate=0.4,
        click_rate=0.08,
        unsubscribes=2,
        subscriber_count=100,
    )

    assert score == 62.0


def test_summarize_ranks_selected_subjects_and_includes_alternatives(db):
    _record_issue(db, "issue-low", "Decent subject", opens=35, clicks=2)
    _record_issue(db, "issue-high", "Strong subject", opens=45, clicks=8)

    summary = NewsletterSubjectPerformance(db).summarize(days=30)

    assert summary.subject_count == 2
    assert summary.best_subject.subject == "Strong subject"
    assert summary.ranked_subjects[0].open_rate == 0.45
    assert summary.ranked_subjects[0].click_rate == 0.08
    assert summary.ranked_subjects[0].alternatives[0].subject == "Strong subject alt"
    assert summary.average_open_rate == 0.4
    assert summary.average_click_rate == 0.05


def test_summarize_handles_no_metrics(db):
    summary = NewsletterSubjectPerformance(db).summarize(days=30)

    assert summary.subject_count == 0
    assert summary.best_subject is None
    assert summary.average_open_rate is None
