from __future__ import annotations

from evaluation.generated_content_claim_density import build_generated_content_claim_density_report


def test_high_density_content_is_flagged():
    report = build_generated_content_claim_density_report([
        {"content_id": "c1", "content_type": "post", "content": "In 2026 teams released 42 updates. Data shows adoption increased 18%. Research supports the rollout."}
    ], min_density=8)
    row = report["findings"][0]
    assert row["content_id"] == "c1"
    assert row["claim_count"] == 3
    assert row["claim_density"] >= 8
    assert row["sample_claims"]


def test_low_density_and_empty_content_are_ignored():
    report = build_generated_content_claim_density_report([
        {"content_id": "low", "content": "A quiet note about the product journey with practical reflections and next steps for readers."},
        {"content_id": "empty", "content": ""},
    ])
    assert report["findings"] == []
    assert report["empty_state"]["is_empty"] is True
