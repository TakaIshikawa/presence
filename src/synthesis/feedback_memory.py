"""Prompt-safe synthesis guidance from durable content feedback."""

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class FeedbackGuidance:
    avoid: tuple[str, ...] = ()
    prefer: tuple[str, ...] = ()

    def is_empty(self) -> bool:
        return not self.avoid and not self.prefer


class FeedbackMemory:
    """Build concise prompt constraints from recent reject/revise feedback."""

    def __init__(
        self,
        db=None,
        lookback_days: int = 30,
        max_items: int = 6,
    ):
        self.db = db
        self.lookback_days = max(1, int(lookback_days))
        self.max_items = max(0, int(max_items))

    def build_guidance(self, content_type: str | None = None) -> FeedbackGuidance:
        rows = self._fetch_rows(content_type)
        avoid: list[str] = []
        prefer: list[str] = []

        for row in rows:
            note = self._clean(row.get("notes", ""))
            replacement = self._clean(row.get("replacement_text", ""))
            content = self._clean(row.get("content", ""))

            if note:
                self._add_unique(avoid, self._sentence(note))
            else:
                for theme in self._classify_avoid_themes(content):
                    self._add_unique(avoid, theme)

            if replacement:
                preference = self._replacement_preference(replacement)
                if preference:
                    self._add_unique(prefer, preference)

        return FeedbackGuidance(tuple(avoid[: self.max_items]), tuple(prefer[: self.max_items]))

    def build_prompt_constraints(self, content_type: str | None = None) -> str:
        guidance = self.build_guidance(content_type)
        if guidance.is_empty():
            return ""

        lines = [
            "RECENT USER FEEDBACK CONSTRAINTS:",
            "- These are derived from reject/revise feedback. Do not quote or imitate rejected drafts.",
        ]
        if guidance.avoid:
            lines.append("Avoid:")
            lines.extend(f"- {item}" for item in guidance.avoid)
        if guidance.prefer:
            lines.append("Prefer:")
            lines.extend(f"- {item}" for item in guidance.prefer)
        return "\n".join(lines)

    def _fetch_rows(self, content_type: str | None) -> list[dict]:
        if not self.db or self.max_items <= 0:
            return []
        getter = getattr(self.db, "get_recent_content_feedback", None)
        if not getter:
            return []
        rows = self._call_getter(getter, content_type)
        if not rows and content_type:
            rows = self._call_getter(getter, None)
        return rows[: self.max_items]

    def _call_getter(self, getter, content_type: str | None) -> list[dict]:
        try:
            return getter(
                content_type=content_type,
                feedback_types=["reject", "revise"],
                limit=self.max_items,
                days=self.lookback_days,
            )
        except TypeError:
            rows = getter(
                content_type=content_type,
                feedback_types=["reject", "revise"],
                limit=self.max_items,
            )
            return rows

    @staticmethod
    def _clean(text: str | None) -> str:
        text = re.sub(r"\s+", " ", text or "").strip()
        text = re.sub(r"[\w.+-]+@[\w-]+\.[\w.-]+", "[email]", text)
        text = re.sub(r"https?://\S+", "[url]", text)
        text = re.sub(r"/Users/\S+", "[local-path]", text)
        text = re.sub(r"\b(?:sk|ghp|xoxb|pat)_[A-Za-z0-9_-]{12,}\b", "[secret]", text)
        return text

    @staticmethod
    def _sentence(text: str, max_len: int = 140) -> str:
        sentence = re.split(r"(?<=[.!?])\s+", text, maxsplit=1)[0].strip()
        if len(sentence) <= max_len:
            return sentence
        return sentence[: max_len - 1].rstrip(" ,.;:") + "."

    @staticmethod
    def _add_unique(items: list[str], value: str) -> None:
        value = value.strip()
        if value and value.lower() not in {item.lower() for item in items}:
            items.append(value)

    @staticmethod
    def _classify_avoid_themes(content: str) -> list[str]:
        lowered = content.lower()
        themes: list[str] = []
        if not content:
            return themes
        if "breakthrough" in lowered or lowered.startswith("today's "):
            themes.append("Avoid stale announcement hooks like breakthrough or today's insight framing.")
        if re.search(r"\b(fixed|added|updated|implemented|built)\b", lowered):
            themes.append("Avoid changelog-style summaries of what was built.")
        if " isn't about " in lowered or " is not about " in lowered:
            themes.append("Avoid contrast-pivot phrasing that says X is not about Y.")
        if len(content) > 240:
            themes.append("Prefer tighter drafts that fit the platform without compression.")
        if not themes:
            themes.append("Avoid repeating the framing of recently rejected drafts.")
        return themes

    @staticmethod
    def _replacement_preference(replacement: str) -> str:
        lowered = replacement.lower()
        if "i " in lowered or "we " in lowered:
            return "Prefer concrete first-person builder observations when they fit the source material."
        if "?" in replacement:
            return "Prefer genuine questions over declarative thought-leadership framing when appropriate."
        if len(replacement) <= 160:
            return "Prefer concise revisions that keep one clear idea."
        return "Prefer revised framing that is specific, grounded, and less generic."
