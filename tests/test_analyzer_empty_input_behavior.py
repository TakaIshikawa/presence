"""Regression tests for empty input behavior across synthesis analyzers."""

from synthesis.conversation_turn_depth import analyze_conversation_turn_depth
from synthesis.prompt_effectiveness_score import calculate_prompt_effectiveness_score
from synthesis.session_coherence_breakdown import analyze_session_coherence
from synthesis.task_switching_overhead import analyze_task_switching_overhead


def test_prompt_effectiveness_empty_metrics_returns_zero_score():
    report = calculate_prompt_effectiveness_score(0, 0, 0, 0, 0, 0)

    assert report.metrics.total_prompts == 0
    assert report.score == 0.0
    assert report.component_scores["completion_rate"] == 0.0


def test_task_switching_no_switches_returns_focused_empty_summary():
    report = analyze_task_switching_overhead([], session_duration_minutes=30.0)

    assert report.metrics.total_switches == 0
    assert report.metrics.overhead_minutes == 0.0
    assert report.overhead_tier == "efficient"


def test_session_coherence_empty_session_returns_perfect_empty_summary():
    report = analyze_session_coherence("empty", [], [])

    assert report.total_turns == 0
    assert report.breakdown_events == []
    assert report.overall_coherence_score == 1.0


def test_conversation_turn_depth_empty_turns_returns_zero_summary():
    report = analyze_conversation_turn_depth([])

    assert report["turn_depth_stats"]["avg_depth"] == 0.0
    assert report["nesting_histogram"] == {}
    assert report["context_switches"] == []
