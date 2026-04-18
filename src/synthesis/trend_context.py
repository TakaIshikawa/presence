"""Extract trending themes from curated content for pipeline injection."""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional

import anthropic

from knowledge.store import KnowledgeStore, KnowledgeItem

logger = logging.getLogger(__name__)


class TrendContextBuilder:
    """Builds a trend/discourse context section from recent curated content."""

    def __init__(
        self,
        knowledge_store: KnowledgeStore,
        api_key: str,
        model: str = "claude-sonnet-4-20250514",
        timeout: float = 300.0,
        db=None,
    ):
        self.store = knowledge_store
        self.client = anthropic.Anthropic(api_key=api_key, timeout=timeout)
        self.model = model
        self.db = db  # Optional Database for caching

    def build_context(
        self,
        max_items: int = 15,
        max_age_hours: int = 72,
        cache_ttl_hours: int = 4,
    ) -> str:
        """Build trend context from recent curated items.

        Checks cache first (if db provided). Returns empty string if no
        recent curated items exist.
        """
        # Check cache
        if self.db:
            cached = self._get_cached(cache_ttl_hours)
            if cached is not None:
                return cached

        items = self.store.get_recent_by_source_type(
            source_type="curated_x",
            limit=max_items,
            max_age_hours=max_age_hours,
        )

        if len(items) < 3:
            return ""

        themes = self._extract_themes(items)
        if not themes:
            return ""

        result = self._format_context(themes, items)

        # Cache result
        if self.db:
            self._cache_result(themes, items)

        return result

    def build_context_with_ids(
        self,
        max_items: int = 15,
        max_age_hours: int = 72,
        cache_ttl_hours: int = 4,
    ) -> tuple[str, list[int]]:
        """Build trend context and return knowledge item IDs used.

        Returns (context_text, knowledge_ids).
        """
        # Check cache
        cached_context = None
        cached_ids = []
        if self.db:
            cached_context = self._get_cached(cache_ttl_hours)
            if cached_context is not None:
                # Also get cached IDs
                raw = self.db.get_meta("trend_themes")
                if raw:
                    try:
                        cached_data = json.loads(raw)
                        cached_ids = cached_data.get("knowledge_ids", [])
                        return cached_context, cached_ids
                    except json.JSONDecodeError:
                        pass

        items = self.store.get_recent_by_source_type(
            source_type="curated_x",
            limit=max_items,
            max_age_hours=max_age_hours,
        )

        if len(items) < 3:
            return "", []

        themes = self._extract_themes(items)
        if not themes:
            return "", []

        result = self._format_context(themes, items)

        # Extract knowledge IDs
        knowledge_ids = [item.id for item in items if item.id is not None]

        # Cache result with IDs
        if self.db:
            self._cache_result(themes, items, knowledge_ids)

        return result, knowledge_ids

    def build_hook_context(
        self,
        prompts: list[str],
        commits: list[dict],
        max_items: int = 15,
        max_age_hours: int = 72,
        max_hooks: int = 3,
    ) -> str:
        """Build concrete bridge hooks from current work to current discourse."""
        items = self.store.get_recent_by_source_type(
            source_type="curated_x",
            limit=max_items,
            max_age_hours=max_age_hours,
        )

        if len(items) < 3 or (not prompts and not commits):
            return ""

        themes = self._extract_themes(items)
        if not themes:
            return ""

        hooks = self._extract_hooks(themes, items, prompts, commits, max_hooks=max_hooks)
        if not hooks:
            return ""

        lines = [
            "TREND HOOKS (bridges from your work to current discourse):",
            "Only use these when the connection is real. Borrow the angle, not the claim.",
        ]
        for hook in hooks[:max_hooks]:
            lines.append(f"- {hook}")
        lines.append("")
        return "\n".join(lines)

    def _get_cached(self, cache_ttl_hours: int) -> Optional[str]:
        """Return cached trend context if fresh enough."""
        raw = self.db.get_meta("trend_themes")
        if not raw:
            return None

        try:
            cached = json.loads(raw)
        except json.JSONDecodeError:
            return None

        cached_at = cached.get("cached_at")
        if not cached_at:
            return None

        cached_time = datetime.fromisoformat(cached_at)
        age_hours = (
            datetime.now(timezone.utc) - cached_time
        ).total_seconds() / 3600

        if age_hours > cache_ttl_hours:
            return None

        # Reconstruct context from cached data
        themes = cached.get("themes", [])
        notable = cached.get("notable_takes", [])
        if not themes:
            return None

        lines = self._build_header()
        lines.append("Trending themes:")
        for theme in themes[:5]:
            lines.append(f"- {theme}")
        lines.append("")
        if notable:
            lines.append("Notable recent takes:")
            for take in notable[:5]:
                lines.append(f"- {take}")
            lines.append("")
        return "\n".join(lines)

    def _cache_result(
        self, themes: list[str], items: list[KnowledgeItem], knowledge_ids: list[int] = None
    ) -> None:
        """Cache extracted themes and notable takes."""
        notable = []
        seen_authors = set()
        for item in items[:5]:
            if item.author in seen_authors:
                continue
            seen_authors.add(item.author)
            insight = item.insight or item.content[:150]
            notable.append(f"@{item.author}: {insight}")

        cache_data = {
            "themes": themes,
            "notable_takes": notable,
            "cached_at": datetime.now(timezone.utc).isoformat(),
            "item_count": len(items),
        }

        if knowledge_ids is not None:
            cache_data["knowledge_ids"] = knowledge_ids

        self.db.set_meta("trend_themes", json.dumps(cache_data))

    def _extract_themes(self, items: list[KnowledgeItem]) -> list[str]:
        """Use Claude to extract 3-5 trending themes from curated items."""
        items_text = "\n\n".join(
            f"@{item.author}: {item.content[:300]}"
            for item in items
        )

        prompt = f"""Below are recent posts from influential tech builders.
Identify 3-5 themes or topics currently being discussed.
Return ONLY a JSON array of theme strings, each 5-15 words.
Focus on specific technical topics, not meta-observations.

Posts:
{items_text}

JSON array:"""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=300,
                messages=[{"role": "user", "content": prompt}],
            )
        except anthropic.APIConnectionError as e:
            error_name = type(e).__name__
            logger.error(f"Failed to connect to Anthropic API: {error_name}: {e}")
            return []
        except anthropic.APIStatusError as e:
            # Includes rate limits, auth errors, and other HTTP status errors
            error_name = type(e).__name__
            logger.error(f"Anthropic API status error: {error_name}: {e}")
            return []
        except Exception as e:
            # Catch-all for unexpected errors
            error_name = type(e).__name__
            logger.error(f"Trend theme extraction failed: {error_name}: {e}")
            return []

        text = response.content[0].text.strip()
        # Handle potential markdown wrapping
        if text.startswith("```"):
            parts = text.split("```")
            text = parts[1] if len(parts) > 1 else text
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

        try:
            themes = json.loads(text)
            if isinstance(themes, list):
                return [str(t) for t in themes[:5]]
        except json.JSONDecodeError:
            # Try extracting JSON array from response
            match = re.search(r"\[.*?\]", text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(0))[:5]
                except json.JSONDecodeError:
                    pass
            logger.warning("Failed to parse trend themes from Claude response")
        return []

    def _extract_hooks(
        self,
        themes: list[str],
        items: list[KnowledgeItem],
        prompts: list[str],
        commits: list[dict],
        max_hooks: int = 3,
    ) -> list[str]:
        """Generate concrete content angles that connect current work to recent discourse."""
        prompt_text = "\n".join(f"- {p[:220]}" for p in prompts[:5]) or "- none"
        commit_text = "\n".join(
            f"- [{c.get('repo_name', '')}] {c.get('message') or c.get('commit_message', '')}"
            for c in commits[:6]
        ) or "- none"
        notable = "\n".join(
            f"- @{item.author}: {(item.insight or item.content)[:220]}"
            for item in items[:5]
        )
        themes_text = "\n".join(f"- {theme}" for theme in themes[:5])

        prompt = f"""You are connecting a builder's real work to current tech discourse on X.

Current work:
Prompts:
{prompt_text}

Commits:
{commit_text}

Current discourse themes:
{themes_text}

Notable recent takes:
{notable}

Return ONLY a JSON array with up to {max_hooks} hook strings.
Each hook should:
- be 8-20 words
- name a current theme people are reacting to
- show the bridge to this builder's actual work
- suggest an angle, tension, or contrast worth posting about
- avoid generic advice and avoid pretending the builder is commenting on news they didn't touch

Good example shape:
"Everyone is posting about agent autonomy; your validator/test work is the unsexy part that makes it real."

JSON array:"""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=300,
                messages=[{"role": "user", "content": prompt}],
            )
        except anthropic.APIConnectionError as e:
            logger.error(f"Failed to connect to Anthropic API: {type(e).__name__}: {e}")
            return []
        except anthropic.APIStatusError as e:
            logger.error(f"Anthropic API status error: {type(e).__name__}: {e}")
            return []
        except Exception as e:
            logger.error(f"Trend hook extraction failed: {type(e).__name__}: {e}")
            return []

        text = response.content[0].text.strip()
        if text.startswith("```"):
            parts = text.split("```")
            text = parts[1] if len(parts) > 1 else text
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

        try:
            hooks = json.loads(text)
            if isinstance(hooks, list):
                return [str(h) for h in hooks[:max_hooks]]
        except json.JSONDecodeError:
            match = re.search(r"\[.*?\]", text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(0))[:max_hooks]
                except json.JSONDecodeError:
                    pass
            logger.warning("Failed to parse trend hooks from Claude response")
        return []

    @staticmethod
    def _build_header() -> list[str]:
        return [
            "CURRENT DISCOURSE (what tech builders are discussing right now):",
            "If your current work connects to any of these themes, "
            "weave that connection naturally — do NOT force it.",
            "",
        ]

    def _format_context(
        self, themes: list[str], items: list[KnowledgeItem]
    ) -> str:
        """Format themes and source insights into a prompt section."""
        lines = self._build_header()

        lines.append("Trending themes:")
        for theme in themes[:5]:
            lines.append(f"- {theme}")

        lines.append("")
        lines.append("Notable recent takes:")
        seen_authors = set()
        for item in items[:5]:
            if item.author in seen_authors:
                continue
            seen_authors.add(item.author)
            insight = item.insight or item.content[:150]
            lines.append(f"- @{item.author}: {insight}")

        lines.append("")
        return "\n".join(lines)
