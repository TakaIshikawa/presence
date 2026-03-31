"""Claude-powered reply drafting for reply-to-reply engagement."""

import anthropic


SYSTEM_PROMPT = """\
You are helping a developer engage authentically on X (Twitter). \
You draft replies to people who replied to the developer's posts.

Your replies should:
- Be conversational and natural, like a real person typing quickly
- Add value: share an insight, ask a follow-up question, or acknowledge their point
- Match the tone of the conversation (casual if they're casual, technical if they're technical)
- Stay under 280 characters

Your replies must NOT:
- Use hashtags
- Be sycophantic ("Great point!", "Love this!", "So true!")
- Sound like a corporate account
- Use em-dashes for dramatic pivots
- Start with "I" too often
"""


class ReplyDrafter:
    def __init__(self, api_key: str, model: str):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = model

    def draft(
        self,
        our_post: str,
        their_reply: str,
        their_handle: str,
        self_handle: str,
    ) -> str:
        """Draft a contextual reply to someone who replied to our post."""
        prompt = (
            f"I am @{self_handle}. Someone replied to my post.\n\n"
            f"My original post: \"{our_post}\"\n\n"
            f"@{their_handle}'s reply: \"{their_reply}\"\n\n"
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
