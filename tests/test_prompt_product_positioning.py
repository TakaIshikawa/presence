"""Regression tests for product-insight content positioning."""

from pathlib import Path


PROMPT_DIR = Path(__file__).parent.parent / "src" / "synthesis" / "prompts"


def _prompt(name: str) -> str:
    return (PROMPT_DIR / name).read_text()


def test_active_prompts_center_ai_agent_product_experiments():
    for name in (
        "x_post_v2.txt",
        "x_long_post_v2.txt",
        "x_thread_v2.txt",
        "blog_post_v2.txt",
    ):
        text = _prompt(name)
        assert "AI agent product" in text or "AI agent products" in text
        assert "hypothesis" in text
        assert "intervention" in text
        assert "measurement" in text or "measured" in text
        assert "not summarize" in text or "not recap" in text or "not the narrative" in text


def test_active_prompts_include_source_blocks():
    for name in (
        "x_post_v2.txt",
        "x_long_post_v2.txt",
        "x_thread_v2.txt",
        "blog_post_v2.txt",
    ):
        text = _prompt(name)
        assert "COMMITS (implementation evidence, not the narrative)" in text
        assert "PROMPTS (product intent, hypotheses, and evaluation goals)" in text


def test_longer_prompts_treat_commits_as_evidence_not_story():
    for name in ("x_long_post_v2.txt", "x_thread_v2.txt", "blog_post_v2.txt"):
        text = _prompt(name)
        assert "Write as the founder" in text or "operator explaining an experiment" in text


def test_longer_prompts_prevent_invented_principles():
    for name in ("x_thread_v2.txt", "blog_post_v2.txt"):
        text = _prompt(name)
        assert "Do not invent" in text or "Do not add abstract labels" in text
        assert "source evidence" in text or "source material" in text
        assert "do not claim" in text.lower()


def test_active_prompts_require_evidence_contract():
    for name in (
        "x_post_v2.txt",
        "x_long_post_v2.txt",
        "x_thread_v2.txt",
        "blog_post_v2.txt",
    ):
        text = _prompt(name)
        assert "evidence contract" in text
        assert "what the source does NOT prove" in text
        assert "does not appear in the source" in text


def test_refiner_preserves_original_claim_set():
    text = _prompt("refiner.txt")
    assert "Do NOT add new facts" in text
    assert "Preserve the original factual claim set" in text
    assert "Do not turn uncertainty into certainty" in text


def test_final_gate_rejects_refined_unsupported_specifics():
    text = _prompt("final_gate.txt")
    assert "Pick ORIGINAL" in text
    assert "Adds any new fact" in text
    assert "adds unsupported specifics" in text
