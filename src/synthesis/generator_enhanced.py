"""Enhanced content generation using knowledge retrieval."""

import anthropic
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

from knowledge.store import KnowledgeStore, KnowledgeItem


@dataclass
class GeneratedContent:
    content_type: str
    content: str
    source_prompts: list[str]
    source_commits: list[str]
    knowledge_used: list[tuple[KnowledgeItem, float]]  # (item, relevance)
    attributions: list[str]


class EnhancedContentGenerator:
    PROMPTS_DIR = Path(__file__).parent / "prompts"

    def __init__(
        self,
        api_key: str,
        knowledge_store: Optional[KnowledgeStore] = None,
        model: str = "claude-sonnet-4-20250514"
    ):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = model
        self.knowledge_store = knowledge_store

    def _load_prompt(self, prompt_type: str) -> str:
        # Try enhanced version first, fall back to basic
        enhanced_file = self.PROMPTS_DIR / f"{prompt_type}_enhanced.txt"
        basic_file = self.PROMPTS_DIR / f"{prompt_type}.txt"

        if enhanced_file.exists() and self.knowledge_store:
            return enhanced_file.read_text()
        return basic_file.read_text()

    def _retrieve_knowledge(
        self,
        query: str,
        limit_own: int = 3,
        limit_external: int = 2
    ) -> tuple[list[tuple[KnowledgeItem, float]], list[tuple[KnowledgeItem, float]]]:
        """Retrieve relevant knowledge for synthesis."""
        if not self.knowledge_store:
            return [], []

        # Get own insights
        own_insights = self.knowledge_store.search_similar(
            query,
            source_types=["own_post", "own_conversation"],
            limit=limit_own,
            min_similarity=0.4
        )

        # Get external insights
        external_insights = self.knowledge_store.search_similar(
            query,
            source_types=["curated_x", "curated_article"],
            limit=limit_external,
            min_similarity=0.5
        )

        return own_insights, external_insights

    def _format_insights(
        self,
        insights: list[tuple[KnowledgeItem, float]]
    ) -> str:
        """Format insights for prompt inclusion."""
        if not insights:
            return "(none available)"

        formatted = []
        for item, score in insights:
            source_info = f"[{item.author}]" if item.author else ""
            insight_text = item.insight or item.content[:200]
            formatted.append(f"- {source_info} {insight_text}")

        return "\n".join(formatted)

    def generate_x_post(
        self,
        prompt: str,
        commit_message: str,
        repo_name: str
    ) -> GeneratedContent:
        """Generate a single X post with knowledge enhancement."""
        # Build query for knowledge retrieval
        query = f"{prompt}\n{commit_message}"

        own_insights, external_insights = self._retrieve_knowledge(query)

        # Load template
        template = self._load_prompt("x_post")

        # Check if using enhanced template
        if "own_insights" in template:
            filled = template.format(
                prompt=prompt,
                commit_message=commit_message,
                repo_name=repo_name,
                own_insights=self._format_insights(own_insights),
                external_insights=self._format_insights(external_insights)
            )
        else:
            # Basic template
            filled = template.format(
                prompt=prompt,
                commit_message=commit_message,
                repo_name=repo_name
            )

        response = self.client.messages.create(
            model=self.model,
            max_tokens=500,
            messages=[{"role": "user", "content": filled}]
        )

        # Extract attributions from external insights used
        attributions = [
            f"@{item.author}" if item.source_type == "curated_x" else item.author
            for item, _ in external_insights
            if item.author
        ]

        return GeneratedContent(
            content_type="x_post",
            content=response.content[0].text.strip(),
            source_prompts=[prompt],
            source_commits=[commit_message],
            knowledge_used=own_insights + external_insights,
            attributions=attributions
        )

    def generate_x_thread(
        self,
        prompts: list[str],
        commits: list[dict]
    ) -> GeneratedContent:
        """Generate an X thread with knowledge enhancement."""
        # Build query from all prompts and commits
        query_parts = prompts[:5] + [c["message"] for c in commits[:5]]
        query = "\n".join(query_parts)

        own_insights, external_insights = self._retrieve_knowledge(
            query,
            limit_own=5,
            limit_external=3
        )

        template = self._load_prompt("x_thread")

        prompts_text = "\n\n".join(f"- {p}" for p in prompts)
        commits_text = "\n\n".join(
            f"- [{c['repo_name']}] {c['message']}"
            for c in commits
        )

        if "own_insights" in template:
            filled = template.format(
                prompts=prompts_text,
                commits=commits_text,
                own_insights=self._format_insights(own_insights),
                external_insights=self._format_insights(external_insights)
            )
        else:
            filled = template.format(
                prompts=prompts_text,
                commits=commits_text
            )

        response = self.client.messages.create(
            model=self.model,
            max_tokens=2000,
            messages=[{"role": "user", "content": filled}]
        )

        attributions = [
            f"@{item.author}" if item.source_type == "curated_x" else item.author
            for item, _ in external_insights
            if item.author
        ]

        return GeneratedContent(
            content_type="x_thread",
            content=response.content[0].text.strip(),
            source_prompts=prompts,
            source_commits=[c["message"] for c in commits],
            knowledge_used=own_insights + external_insights,
            attributions=attributions
        )
