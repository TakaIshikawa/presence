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
    knowledge_ids: list[tuple[int, float]]  # (knowledge_id, relevance_score) for lineage tracking
    prompt_type: Optional[str] = None
    prompt_version: Optional[int] = None
    prompt_hash: Optional[str] = None


class EnhancedContentGenerator:
    PROMPTS_DIR = Path(__file__).parent / "prompts"

    def __init__(
        self,
        api_key: str,
        knowledge_store: Optional[KnowledgeStore] = None,
        model: str = "claude-sonnet-4-6",
        timeout: float = 300.0,
        restricted_prompt_behavior: str = KnowledgeStore.STRICT_LICENSE_BEHAVIOR,
        freshness_half_life_days: Optional[float] = None,
        max_knowledge_per_author: Optional[int] = None,
        max_knowledge_per_source_type: Optional[int] = None,
        db=None,
    ):
        self.client = anthropic.Anthropic(api_key=api_key, timeout=timeout)
        self.model = model
        self.knowledge_store = knowledge_store
        self.restricted_prompt_behavior = restricted_prompt_behavior
        self.freshness_half_life_days = freshness_half_life_days
        self.max_knowledge_per_author = max_knowledge_per_author
        self.max_knowledge_per_source_type = max_knowledge_per_source_type
        self.db = db
        self.prompt_versions: dict[str, dict] = {}
        self.loaded_prompt_types: dict[str, str] = {}

    def _load_prompt(self, prompt_type: str) -> str:
        # Try enhanced version first, fall back to basic
        enhanced_file = self.PROMPTS_DIR / f"{prompt_type}_enhanced.txt"
        basic_file = self.PROMPTS_DIR / f"{prompt_type}.txt"

        if enhanced_file.exists() and self.knowledge_store:
            prompt_text = enhanced_file.read_text()
            registered_type = f"{prompt_type}_enhanced"
            self.loaded_prompt_types[prompt_type] = registered_type
            self._register_prompt(registered_type, prompt_text)
            return prompt_text
        prompt_text = basic_file.read_text()
        self.loaded_prompt_types[prompt_type] = prompt_type
        self._register_prompt(prompt_type, prompt_text)
        return prompt_text

    def _register_prompt(self, prompt_type: str, prompt_text: str) -> dict | None:
        if not self.db or not hasattr(self.db, "register_prompt_version"):
            return None
        record = self.db.register_prompt_version(prompt_type, prompt_text)
        self.prompt_versions[prompt_type] = record
        return record

    def _prompt_metadata(self, prompt_type: str) -> dict:
        return self.prompt_versions.get(prompt_type, {})

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
            min_similarity=0.4,
            max_per_author=self.max_knowledge_per_author,
            max_per_source_type=self.max_knowledge_per_source_type,
        )

        # Get external insights
        external_insights = self.knowledge_store.search_similar(
            query,
            source_types=["curated_x", "curated_article"],
            limit=limit_external,
            min_similarity=0.5,
            freshness_half_life_days=self.freshness_half_life_days,
            max_per_author=self.max_knowledge_per_author,
            max_per_source_type=self.max_knowledge_per_source_type,
        )

        prompt_safe_own = KnowledgeStore.filter_prompt_safe(
            own_insights, self.restricted_prompt_behavior
        )
        prompt_safe_external = KnowledgeStore.filter_prompt_safe(
            external_insights, self.restricted_prompt_behavior
        )

        return self._apply_prompt_diversity(prompt_safe_own, prompt_safe_external)

    def _apply_prompt_diversity(
        self,
        own_insights: list[tuple[KnowledgeItem, float]],
        external_insights: list[tuple[KnowledgeItem, float]],
    ) -> tuple[list[tuple[KnowledgeItem, float]], list[tuple[KnowledgeItem, float]]]:
        if (
            self.max_knowledge_per_author is None
            and self.max_knowledge_per_source_type is None
        ):
            return own_insights, external_insights

        allowed = KnowledgeStore.apply_diversity_caps(
            own_insights + external_insights,
            max_per_author=self.max_knowledge_per_author,
            max_per_source_type=self.max_knowledge_per_source_type,
        )
        allowed_ids = {id(result) for result in allowed}
        return (
            [result for result in own_insights if id(result) in allowed_ids],
            [result for result in external_insights if id(result) in allowed_ids],
        )

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
        prompt_template_type = self.loaded_prompt_types.get("x_post", "x_post")

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

        # Extract knowledge IDs for lineage tracking
        knowledge_ids = [
            (item.id, relevance)
            for item, relevance in own_insights + external_insights
            if item.id is not None
        ]

        return GeneratedContent(
            content_type="x_post",
            content=response.content[0].text.strip(),
            source_prompts=[prompt],
            source_commits=[commit_message],
            knowledge_used=own_insights + external_insights,
            attributions=attributions,
            knowledge_ids=knowledge_ids,
            prompt_type=prompt_template_type,
            prompt_version=self._prompt_metadata(prompt_template_type).get("version"),
            prompt_hash=self._prompt_metadata(prompt_template_type).get("prompt_hash"),
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
        prompt_template_type = self.loaded_prompt_types.get("x_thread", "x_thread")

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

        # Extract knowledge IDs for lineage tracking
        knowledge_ids = [
            (item.id, relevance)
            for item, relevance in own_insights + external_insights
            if item.id is not None
        ]

        return GeneratedContent(
            content_type="x_thread",
            content=response.content[0].text.strip(),
            source_prompts=prompts,
            source_commits=[c["message"] for c in commits],
            knowledge_used=own_insights + external_insights,
            attributions=attributions,
            knowledge_ids=knowledge_ids,
            prompt_type=prompt_template_type,
            prompt_version=self._prompt_metadata(prompt_template_type).get("version"),
            prompt_hash=self._prompt_metadata(prompt_template_type).get("prompt_hash"),
        )
