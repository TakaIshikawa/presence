"""Content generation using Claude API."""

import anthropic
from pathlib import Path
from typing import Optional
from dataclasses import dataclass


@dataclass
class GeneratedContent:
    content_type: str
    content: str
    source_prompts: list[str]
    source_commits: list[str]


class ContentGenerator:
    PROMPTS_DIR = Path(__file__).parent / "prompts"

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514"):
        self.client = anthropic.Anthropic(api_key=api_key)
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

        response = self.client.messages.create(
            model=self.model,
            max_tokens=500,
            messages=[{"role": "user", "content": filled}]
        )

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

        response = self.client.messages.create(
            model=self.model,
            max_tokens=500,
            messages=[{"role": "user", "content": filled}]
        )

        return GeneratedContent(
            content_type="x_post",
            content=response.content[0].text.strip(),
            source_prompts=prompts,
            source_commits=[c["message"] for c in commits]
        )

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

        response = self.client.messages.create(
            model=self.model,
            max_tokens=2000,
            messages=[{"role": "user", "content": filled}]
        )

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

        response = self.client.messages.create(
            model=self.model,
            max_tokens=4000,
            messages=[{"role": "user", "content": filled}]
        )

        return GeneratedContent(
            content_type="blog_post",
            content=response.content[0].text.strip(),
            source_prompts=prompts,
            source_commits=[c.get("message") or c.get("commit_message") for c in commits]
        )
