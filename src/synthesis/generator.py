"""Content generation using Claude API."""

import logging

import anthropic
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class GeneratedContent:
    content_type: str
    content: str
    source_prompts: list[str]
    source_commits: list[str]


class ContentGenerator:
    PROMPTS_DIR = Path(__file__).parent / "prompts"

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514", timeout: float = 300.0):
        self.client = anthropic.Anthropic(api_key=api_key, timeout=timeout)
        self.model = model

    def _load_prompt(self, prompt_type: str) -> str:
        prompt_file = self.PROMPTS_DIR / f"{prompt_type}.txt"
        return prompt_file.read_text()

    def generate_x_post(
        self,
        prompt: str,
        commit_message: str,
        repo_name: str
    ) -> GeneratedContent:
        """Generate a single X post from a prompt and commit."""
        template = self._load_prompt("x_post")
        filled = template.format(
            prompt=prompt,
            commit_message=commit_message,
            repo_name=repo_name
        )

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=500,
                messages=[{"role": "user", "content": filled}]
            )
        except anthropic.APIConnectionError as e:
            error_name = type(e).__name__
            logger.error(f"Failed to connect to Anthropic API: {error_name}: {e}")
            raise
        except anthropic.APIStatusError as e:
            error_name = type(e).__name__
            logger.error(f"Anthropic API status error: {error_name}: {e}")
            raise

        return GeneratedContent(
            content_type="x_post",
            content=response.content[0].text.strip(),
            source_prompts=[prompt],
            source_commits=[commit_message]
        )

    def generate_x_post_batched(
        self,
        prompts: list[str],
        commits: list[dict]
    ) -> GeneratedContent:
        """Generate a single X post synthesizing multiple commits."""
        template = self._load_prompt("x_post_batched")

        prompts_text = "\n\n".join(f"- {p[:500]}" for p in prompts[:5])  # Limit context
        commits_text = "\n\n".join(
            f"- [{c['repo_name']}] {c['message']}"
            for c in commits[:10]
        )

        filled = template.format(
            prompts=prompts_text,
            commits=commits_text,
            commit_count=len(commits)
        )

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=500,
                messages=[{"role": "user", "content": filled}]
            )
        except anthropic.APIConnectionError as e:
            error_name = type(e).__name__
            logger.error(f"Failed to connect to Anthropic API: {error_name}: {e}")
            raise
        except anthropic.APIStatusError as e:
            error_name = type(e).__name__
            logger.error(f"Anthropic API status error: {error_name}: {e}")
            raise

        return GeneratedContent(
            content_type="x_post",
            content=response.content[0].text.strip(),
            source_prompts=prompts,
            source_commits=[c["message"] for c in commits]
        )

    # Content-type settings for multi-candidate generation
    CONTENT_TYPE_CONFIG = {
        "x_post": {"template": "x_post_v2", "max_tokens": 150},
        "x_long_post": {"template": "x_long_post_v2", "max_tokens": 1000},
        "x_visual": {"template": "x_visual_v2", "max_tokens": 150},
        "x_thread": {"template": "x_thread_v2", "max_tokens": 2000},
        "blog_post": {"template": "blog_post_v2", "max_tokens": 4000},
    }

    def generate_candidates(
        self,
        prompts: list[str],
        commits: list[dict],
        content_type: str = "x_post",
        few_shot_examples: str = "",
        num_candidates: int = 3,
        format_directives: list[str] | None = None,
        avoidance_context: str = "",
        pattern_context: str = "",
        trend_context: str = "",
    ) -> list[GeneratedContent]:
        """Generate multiple candidates with temperature and format variation."""
        type_config = self.CONTENT_TYPE_CONFIG.get(
            content_type, self.CONTENT_TYPE_CONFIG["x_post"]
        )
        template = self._load_prompt(type_config["template"])
        max_tokens = type_config["max_tokens"]

        prompts_text = "\n\n".join(f"- {p[:500]}" for p in prompts[:5])

        # Split current vs historical commits
        current_commits = [c for c in commits if not c.get("historical")]
        historical_commits = [c for c in commits if c.get("historical")]

        commits_text = "\n\n".join(
            f"- [{c.get('repo_name', '')}] {c.get('message') or c.get('commit_message', '')}"
            for c in current_commits[:10]
        )

        if historical_commits:
            historical_text = "\n".join(
                f"- [{c.get('repo_name', '')}] {c.get('message') or c.get('commit_message', '')}"
                for c in historical_commits[:5]
            )
            historical_section = (
                "HISTORICAL CONTEXT (from your past work):\n"
                "These relate to what you're building now. "
                "Draw connections or contrast past vs present.\n\n"
                f"{historical_text}\n\n"
                "The post should still be grounded in CURRENT activity. "
                "Use history for depth, not replacement.\n"
            )
        else:
            historical_section = ""

        if few_shot_examples:
            few_shot_section = (
                "EXAMPLES OF CONTENT THAT RESONATED:\n\n"
                f"{few_shot_examples}\n\n"
                "Match this quality level. Be specific and concrete like these examples.\n"
            )
        else:
            few_shot_section = ""

        temperatures = [0.5, 0.7, 0.9][:num_candidates]
        candidates = []

        for i, temp in enumerate(temperatures):
            # Each candidate gets a different format directive
            format_directive = ""
            if format_directives and i < len(format_directives):
                format_directive = format_directives[i]

            filled = template.format(
                prompts=prompts_text,
                commits=commits_text,
                commit_count=len(current_commits),
                few_shot_section=few_shot_section,
                format_directive=format_directive,
                historical_section=historical_section,
                avoidance_context=avoidance_context,
                pattern_context=pattern_context,
                trend_context=trend_context,
            )

            try:
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=max_tokens,
                    temperature=temp,
                    messages=[{"role": "user", "content": filled}],
                )
            except anthropic.APIConnectionError as e:
                error_name = type(e).__name__
                logger.error(f"Failed to connect to Anthropic API: {error_name}: {e}")
                raise
            except anthropic.APIStatusError as e:
                error_name = type(e).__name__
                logger.error(f"Anthropic API status error: {error_name}: {e}")
                raise

            candidates.append(
                GeneratedContent(
                    content_type=content_type,
                    content=response.content[0].text.strip(),
                    source_prompts=prompts,
                    source_commits=[c.get("message") or c.get("commit_message", "") for c in commits],
                )
            )

        return candidates

    def condense(self, content: str, max_chars: int = 280) -> str:
        """Condense content to fit character limit."""
        template = self._load_prompt("condense")
        filled = template.format(content=content, char_count=len(content))

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=200,
                messages=[{"role": "user", "content": filled}],
            )
        except anthropic.APIConnectionError as e:
            error_name = type(e).__name__
            logger.error(f"Failed to connect to Anthropic API: {error_name}: {e}")
            raise
        except anthropic.APIStatusError as e:
            error_name = type(e).__name__
            logger.error(f"Anthropic API status error: {error_name}: {e}")
            raise

        return response.content[0].text.strip()

    def generate_x_thread(
        self,
        prompts: list[str],
        commits: list[dict]
    ) -> GeneratedContent:
        """Generate an X thread from a day's prompts and commits."""
        template = self._load_prompt("x_thread")

        prompts_text = "\n\n".join(f"- {p}" for p in prompts)
        commits_text = "\n\n".join(
            f"- [{c['repo_name']}] {c.get('message') or c.get('commit_message')}"
            for c in commits
        )

        filled = template.format(
            prompts=prompts_text,
            commits=commits_text
        )

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2000,
                messages=[{"role": "user", "content": filled}]
            )
        except anthropic.APIConnectionError as e:
            error_name = type(e).__name__
            logger.error(f"Failed to connect to Anthropic API: {error_name}: {e}")
            raise
        except anthropic.APIStatusError as e:
            error_name = type(e).__name__
            logger.error(f"Anthropic API status error: {error_name}: {e}")
            raise

        return GeneratedContent(
            content_type="x_thread",
            content=response.content[0].text.strip(),
            source_prompts=prompts,
            source_commits=[c.get("message") or c.get("commit_message") for c in commits]
        )

    def generate_blog_post(
        self,
        prompts: list[str],
        commits: list[dict]
    ) -> GeneratedContent:
        """Generate a blog post from a week's prompts and commits."""
        template = self._load_prompt("blog_post")

        prompts_text = "\n\n".join(f"- {p}" for p in prompts)
        commits_text = "\n\n".join(
            f"- [{c['repo_name']}] {c.get('message') or c.get('commit_message')}"
            for c in commits
        )

        filled = template.format(
            prompts=prompts_text,
            commits=commits_text
        )

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=4000,
                messages=[{"role": "user", "content": filled}]
            )
        except anthropic.APIConnectionError as e:
            error_name = type(e).__name__
            logger.error(f"Failed to connect to Anthropic API: {error_name}: {e}")
            raise
        except anthropic.APIStatusError as e:
            error_name = type(e).__name__
            logger.error(f"Anthropic API status error: {error_name}: {e}")
            raise

        return GeneratedContent(
            content_type="blog_post",
            content=response.content[0].text.strip(),
            source_prompts=prompts,
            source_commits=[c.get("message") or c.get("commit_message") for c in commits]
        )
