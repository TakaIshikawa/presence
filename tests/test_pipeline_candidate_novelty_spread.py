from __future__ import annotations

from evaluation.pipeline_candidate_novelty_spread import build_pipeline_candidate_novelty_spread_report


def test_low_spread_duplicate_batch_is_flagged():
    report = build_pipeline_candidate_novelty_spread_report([
        {"batch_id": "b1", "candidate_id": "a", "candidate_text": "Launch the retry guide with practical team workflows."},
        {"batch_id": "b1", "candidate_id": "b", "candidate_text": "Launch the retry guide with practical team workflow."},
    ], similarity_threshold=0.7)
    row = report["findings"][0]
    assert row["batch_id"] == "b1"
    assert row["candidate_count"] == 2
    assert row["average_similarity"] >= 0.7
    assert row["low_novelty_pair_ids"] == [["a", "b"]]


def test_diverse_and_single_candidate_batches_are_ignored():
    report = build_pipeline_candidate_novelty_spread_report([
        {"batch_id": "diverse", "candidate_id": "a", "candidate_text": "A launch note about retry budgets."},
        {"batch_id": "diverse", "candidate_id": "b", "candidate_text": "A customer story about onboarding research."},
        {"batch_id": "single", "candidate_id": "c", "candidate_text": "Only one candidate."},
    ], similarity_threshold=0.8)
    assert report["findings"] == []
