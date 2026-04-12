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
        self, themes: list[str], items: list[KnowledgeItem]
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

        self.db.set_meta("trend_themes", json.dumps({
            "themes": themes,
            "notable_takes": notable,
            "cached_at": datetime.now(timezone.utc).isoformat(),
            "item_count": len(items),
        }))

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

        response = self.client.messages.create(
            model=self.model,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )

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
