"""Claude-powered reply drafting for reply-to-reply engagement."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import anthropic

if TYPE_CHECKING:
    from engagement.cultivate_bridge import PersonContext


SYSTEM_PROMPT = """\
You are helping a developer engage authentically on X (Twitter). \
You draft replies to people who replied to the developer's posts.

Your replies should:
- Be conversational and natural, like a real person typing quickly
- Add value: share an insight, ask a follow-up question, or acknowledge their point
- Match the tone of the conversation (casual if they're casual, technical if they're technical)
- Stay under 280 characters
- Be calibrated to the relationship stage (see context if provided)

Your replies must NOT:
- Use hashtags
- Be sycophantic ("Great point!", "Love this!", "So true!")
- Sound like a corporate account
- Use em-dashes for dramatic pivots
- Start with "I" too often

Relationship stage guidelines (when context is provided):
- Observation/Ambient (stage 0-1): Keep it light. A brief acknowledgment or question. \
No familiarity assumed.
- Light/Active (stage 2-3): Can reference shared interests or past interactions. More substance.
- Relationship/Alliance (stage 4-5): Conversational, can be more personal, reference shared history.
"""


class ReplyDrafter:
    def __init__(self, api_key: str, model: str, timeout: float = 300.0):
        self.client = anthropic.Anthropic(api_key=api_key, timeout=timeout)
        self.model = model

    def draft(
        self,
        our_post: str,
        their_reply: str,
        their_handle: str,
        self_handle: str,
        person_context: Optional["PersonContext"] = None,
    ) -> str:
        """Draft a contextual reply, optionally enriched with relationship context."""
        context_section = ""
        if person_context and person_context.is_known:
            context_section = self._build_context_section(person_context)

        prompt = (
            f"I am @{self_handle}. Someone replied to my post.\n\n"
            f"My original post: \"{our_post}\"\n\n"
            f"@{their_handle}'s reply: \"{their_reply}\"\n\n"
        )
        if context_section:
            prompt += f"{context_section}\n\n"
        prompt += (
            "Draft a reply that continues this conversation naturally. "
            "Respond with ONLY the reply text, nothing else."
        )

        response = self.client.messages.create(
            model=self.model,
            max_tokens=150,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )

        return response.content[0].text.strip().strip('"')

    @staticmethod
    def _build_context_section(ctx: "PersonContext") -> str:
        """Build relationship context section for the LLM prompt."""
        lines = [f"## Relationship Context for @{ctx.x_handle}"]
        if ctx.bio:
            lines.append(f"Bio: {ctx.bio}")
        if ctx.engagement_stage is not None:
            lines.append(f"Engagement stage: {ctx.stage_name} (stage {ctx.engagement_stage})")
        if ctx.dunbar_tier is not None:
            lines.append(f"Dunbar tier: {ctx.tier_name} (tier {ctx.dunbar_tier})")
        if ctx.relationship_strength is not None:
            lines.append(f"Relationship strength: {ctx.relationship_strength:.2f}")
        if ctx.recent_interactions:
            lines.append("Recent interaction history:")
            for ix in ctx.recent_interactions[:5]:
                snippet = ix.get("snippet", "")[:60]
                date = ix.get("date", "")[:10]
                lines.append(f"  - [{date}] {ix['type']} ({ix['direction']}){': ' + snippet if snippet else ''}")
        return "\n".join(lines)
