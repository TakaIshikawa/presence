"""Quality evaluation for reply drafts.

Flag-only mode: low-scoring drafts are flagged but still queued for
human review. No auto-rejection.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

import anthropic

if TYPE_CHECKING:
    from engagement.cultivate_bridge import PersonContext


EVAL_SYSTEM_PROMPT = """\
You evaluate reply drafts for quality. Score the reply on a 0-10 scale and flag issues.

Evaluation criteria:
- Authenticity: Does it sound like a real person? (not corporate, not bot-like)
- Engagement: Does it actually engage with the content? (not generic)
- Tone match: Does it match the conversation's tone?
- Value-add: Does it contribute something? (insight, question, acknowledgment with depth)
- Conciseness: Is it appropriately brief for the platform?

Flags to check:
- sycophantic: Generic praise ("Great point!", "Love this!", "Couldn't agree more!")
- hashtags: Contains any hashtags
- generic: Could be sent to any tweet without modification
- stage_mismatch: Too familiar for a new/distant connection, or too formal for a close one
- em_dash_pivot: Uses em-dashes for dramatic "X — but Y" pivots

Respond with ONLY valid JSON:
{
  "score": 7.5,
  "feedback": "Brief explanation",
  "flags": ["flag1", "flag2"]
}
"""

# Fast regex checks (skip LLM for obvious issues)
_SYCOPHANTIC_PATTERNS = [
    re.compile(r"(?i)^(great|excellent|amazing|awesome|fantastic|brilliant|wonderful) (point|take|insight|thread|post)"),
    re.compile(r"(?i)^(love this|so true|couldn.t agree more|this is (so )?spot on)"),
    re.compile(r"(?i)^(thank you for sharing|thanks for (sharing|this))"),
]

_HASHTAG_PATTERN = re.compile(r"#\w+")


@dataclass
class ReplyEvalResult:
    score: float
    passes: bool
    feedback: str
    flags: list[str] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps({
            "score": self.score,
            "passes": self.passes,
            "feedback": self.feedback,
            "flags": self.flags,
        })


class ReplyEvaluator:
    """Evaluate reply draft quality. Flag-only — does not auto-reject."""

    def __init__(self, api_key: str, model: str, timeout: float = 300.0):
        self.client = anthropic.Anthropic(api_key=api_key, timeout=timeout)
        self.model = model

    def evaluate(
        self,
        draft: str,
        our_post: str,
        their_reply: str,
        threshold: float = 6.0,
        person_context: Optional["PersonContext"] = None,
    ) -> ReplyEvalResult:
        """Evaluate a reply draft for quality.

        Uses fast regex checks first, then LLM evaluation for borderline cases.
        """
        # Fast checks
        fast_flags = self._fast_check(draft)
        if fast_flags:
            return ReplyEvalResult(
                score=2.0,
                passes=False,
                feedback=f"Fast-flagged: {', '.join(fast_flags)}",
                flags=fast_flags,
            )

        # LLM evaluation
        return self._llm_evaluate(draft, our_post, their_reply, threshold, person_context)

    def _fast_check(self, draft: str) -> list[str]:
        """Quick regex checks for obvious issues."""
        flags = []
        for pattern in _SYCOPHANTIC_PATTERNS:
            if pattern.search(draft):
                flags.append("sycophantic")
                break
        if _HASHTAG_PATTERN.search(draft):
            flags.append("hashtags")
        return flags

    def _llm_evaluate(
        self,
        draft: str,
        our_post: str,
        their_reply: str,
        threshold: float,
        person_context: Optional["PersonContext"],
    ) -> ReplyEvalResult:
        """Full LLM evaluation."""
        context_note = ""
        if person_context and person_context.is_known:
            stage = person_context.engagement_stage
            tier = person_context.dunbar_tier
            if stage is not None:
                context_note = (
                    f"\nRelationship context: {person_context.stage_name} (stage {stage}), "
                    f"{person_context.tier_name} (tier {tier}). "
                    "Flag 'stage_mismatch' if the reply's familiarity doesn't match this stage."
                )

        prompt = (
            f"Our post: \"{our_post}\"\n\n"
            f"Their reply: \"{their_reply}\"\n\n"
            f"Draft reply to evaluate: \"{draft}\""
            f"{context_note}"
        )

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=200,
                system=EVAL_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )

            raw = response.content[0].text.strip()
            return self._parse_response(raw, threshold)
        except Exception as e:
            # On error, pass the draft through (don't block review)
            return ReplyEvalResult(
                score=5.0,
                passes=False,
                feedback=f"Evaluation error: {e}",
                flags=["eval_error"],
            )

    def _parse_response(self, raw: str, threshold: float) -> ReplyEvalResult:
        """Parse LLM JSON response into ReplyEvalResult."""
        # Strip markdown code fences if present
        text = raw.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1]
            text = text.rsplit("```", 1)[0]

        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            start = raw.find("{")
            end = raw.rfind("}") + 1
            if start >= 0 and end > start:
                try:
                    data = json.loads(raw[start:end])
                except json.JSONDecodeError:
                    return ReplyEvalResult(
                        score=5.0, passes=False,
                        feedback="Could not parse evaluation response",
                        flags=["parse_error"],
                    )
            else:
                return ReplyEvalResult(
                    score=5.0, passes=False,
                    feedback="Could not parse evaluation response",
                    flags=["parse_error"],
                )

        score = max(0.0, min(10.0, float(data.get("score", 5.0))))
        flags = data.get("flags", [])
        if not isinstance(flags, list):
            flags = []

        return ReplyEvalResult(
            score=score,
            passes=score >= threshold,
            feedback=data.get("feedback", ""),
            flags=flags,
        )
