"""Claude-powered reply drafting for reply-to-reply engagement."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional
from dataclasses import dataclass

import anthropic

if TYPE_CHECKING:
    from engagement.cultivate_bridge import PersonContext
    from knowledge.store import KnowledgeStore, KnowledgeItem


PROACTIVE_SYSTEM_PROMPT = """\
You are helping a developer engage authentically on X (Twitter). \
You draft proactive replies to interesting tweets by others — \
joining their conversation with genuine value.

Your replies should:
- Add genuine value: share a relevant experience, insight, or ask a thoughtful question
- Be conversational and natural, like a real person typing quickly
- Match the tone of their tweet (casual if casual, technical if technical)
- Stay under 280 characters
- Be calibrated to the relationship stage (see context if provided)

Your replies must NOT:
- Use hashtags
- Be sycophantic ("Great point!", "Love this!", "So true!")
- Sound like a corporate account
- Use em-dashes for dramatic pivots
- Start with "I" too often
- Plug or reference your own posts/projects
- Simply agree — add something new

Relationship stage guidelines (when context is provided):
- Observation/Ambient (stage 0-1): Keep it very light. Brief observation or question. \
No familiarity assumed.
- Light/Active (stage 2-3): Can reference shared interests. More substance.
- Relationship/Alliance (stage 4-5): Conversational, can reference shared history.

When past insights are provided, naturally weave your perspective if relevant. \
Do NOT force-fit insights — only use them if they genuinely connect.
"""


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

When past insights are provided, naturally reference your established perspective if relevant. \
Do NOT force-fit insights — only use them if they genuinely connect to the conversation.
"""


@dataclass
class ReplyDraft:
    """Result of reply drafting with knowledge lineage."""
    reply_text: str
    knowledge_ids: list[tuple[int, float]]  # (knowledge_id, relevance_score)


class ReplyDrafter:
    def __init__(
        self,
        api_key: str,
        model: str,
        timeout: float = 300.0,
        knowledge_store: Optional["KnowledgeStore"] = None
    ):
        self.client = anthropic.Anthropic(api_key=api_key, timeout=timeout)
        self.model = model
        self.knowledge_store = knowledge_store

    def draft(
        self,
        our_post: str,
        their_reply: str,
        their_handle: str,
        self_handle: str,
        person_context: Optional["PersonContext"] = None,
    ) -> str:
        """Draft a contextual reply, optionally enriched with relationship context.

        This is the backward-compatible method that returns only the reply text.
        For knowledge lineage tracking, use draft_with_lineage() instead.
        """
        result = self.draft_with_lineage(
            our_post=our_post,
            their_reply=their_reply,
            their_handle=their_handle,
            self_handle=self_handle,
            person_context=person_context,
        )
        return result.reply_text

    def draft_with_lineage(
        self,
        our_post: str,
        their_reply: str,
        their_handle: str,
        self_handle: str,
        person_context: Optional["PersonContext"] = None,
    ) -> ReplyDraft:
        """Draft a contextual reply with knowledge lineage tracking."""
        context_section = ""
        if person_context and person_context.is_known:
            context_section = self._build_context_section(person_context)

        # Retrieve relevant knowledge
        knowledge_items = []
        if self.knowledge_store is not None:
            knowledge_items = self._retrieve_reply_context(our_post, their_reply)

        knowledge_section = ""
        if knowledge_items:
            knowledge_section = self._build_knowledge_section(knowledge_items)

        prompt = (
            f"I am @{self_handle}. Someone replied to my post.\n\n"
            f"My original post: \"{our_post}\"\n\n"
            f"@{their_handle}'s reply: \"{their_reply}\"\n\n"
        )
        if context_section:
            prompt += f"{context_section}\n\n"
        if knowledge_section:
            prompt += f"{knowledge_section}\n\n"
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

        reply_text = response.content[0].text.strip().strip('"')

        # Extract knowledge IDs for lineage
        knowledge_ids = [
            (item.id, relevance)
            for item, relevance in knowledge_items
            if item.id is not None
        ]

        return ReplyDraft(reply_text=reply_text, knowledge_ids=knowledge_ids)

    def draft_proactive(
        self,
        their_tweet: str,
        their_handle: str,
        self_handle: str,
        person_context: Optional["PersonContext"] = None,
        knowledge_items: Optional[list] = None,
    ) -> ReplyDraft:
        """Draft a proactive reply to someone else's tweet.

        Unlike draft_with_lineage(), there is no "our post" — we're jumping
        into their conversation with relevant knowledge from our own experience.

        Args:
            knowledge_items: Pre-fetched list of (KnowledgeItem, similarity)
                tuples. If provided, skips the knowledge_store.search_similar()
                call (avoids extra embedding API calls).
        """
        context_section = ""
        if person_context and person_context.is_known:
            context_section = self._build_context_section(person_context)

        # Use pre-fetched knowledge or retrieve from store
        if knowledge_items is None:
            knowledge_items = []
            if self.knowledge_store is not None:
                knowledge_items = self.knowledge_store.search_similar(
                    their_tweet,
                    source_types=["own_post", "own_conversation", "curated_x"],
                    limit=3,
                    min_similarity=0.40,
                )

        knowledge_section = ""
        if knowledge_items:
            knowledge_section = self._build_knowledge_section(knowledge_items)

        prompt = (
            f"I am @{self_handle}. I want to reply to this tweet.\n\n"
            f"@{their_handle}'s tweet: \"{their_tweet}\"\n\n"
        )
        if context_section:
            prompt += f"{context_section}\n\n"
        if knowledge_section:
            prompt += f"{knowledge_section}\n\n"
        prompt += (
            "Draft a reply that adds genuine value to their conversation. "
            "Respond with ONLY the reply text, nothing else."
        )

        response = self.client.messages.create(
            model=self.model,
            max_tokens=150,
            system=PROACTIVE_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )

        reply_text = response.content[0].text.strip().strip('"')

        knowledge_ids = [
            (item.id, relevance)
            for item, relevance in knowledge_items
            if item.id is not None
        ]

        return ReplyDraft(reply_text=reply_text, knowledge_ids=knowledge_ids)

    def _retrieve_reply_context(
        self,
        our_post: str,
        their_reply: str,
        limit: int = 3
    ) -> list[tuple["KnowledgeItem", float]]:
        """Retrieve relevant knowledge for reply context.

        Args:
            our_post: Our original post they're replying to
            their_reply: Their reply text
            limit: Maximum number of knowledge items to retrieve

        Returns:
            List of (KnowledgeItem, relevance_score) tuples
        """
        if self.knowledge_store is None:
            return []

        # Build query from both our post and their reply
        query = f"{our_post}\n{their_reply}"

        # Search for relevant own insights
        results = self.knowledge_store.search_similar(
            query,
            source_types=['own_post', 'own_conversation'],
            limit=limit,
            min_similarity=0.45
        )

        return results

    def _build_knowledge_section(
        self, items: list[tuple["KnowledgeItem", float]]
    ) -> str:
        """Format retrieved knowledge for prompt inclusion.

        Args:
            items: List of (KnowledgeItem, relevance_score) tuples

        Returns:
            Formatted knowledge section for the prompt
        """
        if not items:
            return ""

        lines = ["## Your Relevant Past Insights"]
        for item, _ in items:
            # Use insight if available, otherwise truncate content
            text = item.insight if item.insight else item.content[:150]
            lines.append(f"- {text}")

        return "\n".join(lines)

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
