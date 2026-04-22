"""Deterministic alt-text validation for generated visual posts."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path


DEFAULT_MAX_ALT_TEXT_LENGTH = 1000
DEFAULT_MIN_ALT_TEXT_CHARS = 20
DEFAULT_MIN_ALT_TEXT_WORDS = 4

_GENERIC_DESCRIPTIONS = {
    "image",
    "photo",
    "picture",
    "graphic",
    "illustration",
    "visual",
    "screenshot",
    "generated image",
    "an image",
    "a photo",
    "a picture",
    "a graphic",
    "an illustration",
    "a visual",
    "a screenshot",
    "alt text",
}

_GENERIC_TOKENS = {
    "a",
    "an",
    "and",
    "annotated",
    "at",
    "by",
    "for",
    "from",
    "generated",
    "graphic",
    "image",
    "in",
    "into",
    "of",
    "on",
    "or",
    "photo",
    "picture",
    "post",
    "showing",
    "screenshot",
    "the",
    "this",
    "to",
    "visual",
    "with",
}


@dataclass(frozen=True)
class AltTextIssue:
    """One deterministic alt-text validation issue."""

    code: str
    message: str
    severity: str = "error"

    def as_dict(self) -> dict:
        return {
            "code": self.code,
            "message": self.message,
            "severity": self.severity,
        }


@dataclass(frozen=True)
class AltTextValidation:
    """Result of alt-text validation."""

    required: bool
    checked: bool
    passed: bool
    status: str
    issues: tuple[AltTextIssue, ...]

    def as_dict(self) -> dict:
        return {
            "required": self.required,
            "checked": self.checked,
            "passed": self.passed,
            "status": self.status,
            "issues": [issue.as_dict() for issue in self.issues],
        }


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def _tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-zA-Z][a-zA-Z0-9]+", text.lower())
        if len(token) > 2 and token not in _GENERIC_TOKENS
    }


def _word_count(text: str) -> int:
    return len(re.findall(r"[A-Za-z0-9]+", text))


def _is_visual_content(
    *,
    image_path: str | None,
    content_type: str | None,
) -> bool:
    return bool(image_path) or content_type in {"x_visual", "visual"}


def _file_name_leaks(alt_text: str, image_path: str | None) -> bool:
    if not image_path:
        return False

    normalized_alt = _normalize_text(alt_text)
    path = Path(image_path)
    basename = path.name.lower()
    stem = path.stem.lower()

    if basename and basename in normalized_alt:
        return True
    if any(part in normalized_alt for part in (".png", ".jpg", ".jpeg", ".webp")):
        return True

    # Avoid flagging ordinary words such as "visual"; only catch filename-like
    # stems that still carry generated identifiers or separators.
    filename_like_stem = bool(re.search(r"[_-]|\d", stem))
    return filename_like_stem and len(stem) > 5 and stem in normalized_alt


def _prompt_keywords(image_prompt: str | None) -> set[str]:
    return _tokens(image_prompt or "")


def validate_alt_text(
    alt_text: str | None,
    *,
    image_prompt: str | None = None,
    image_path: str | None = None,
    content_type: str | None = None,
    max_length: int = DEFAULT_MAX_ALT_TEXT_LENGTH,
    min_chars: int = DEFAULT_MIN_ALT_TEXT_CHARS,
    min_words: int = DEFAULT_MIN_ALT_TEXT_WORDS,
) -> AltTextValidation:
    """Validate alt text for visual content using deterministic checks."""
    required = _is_visual_content(image_path=image_path, content_type=content_type)
    if not required:
        return AltTextValidation(
            required=False,
            checked=False,
            passed=True,
            status="not_required",
            issues=(),
        )

    text = (alt_text or "").strip()
    normalized = _normalize_text(text)
    issues: list[AltTextIssue] = []

    if not text:
        issues.append(
            AltTextIssue(
                "missing_alt_text",
                "Visual posts require alt text before publishing.",
            )
        )
    else:
        if len(text) > max_length:
            issues.append(
                AltTextIssue(
                    "alt_text_too_long",
                    f"Alt text must be {max_length} characters or fewer.",
                )
            )
        if len(text) < min_chars or _word_count(text) < min_words:
            issues.append(
                AltTextIssue(
                    "alt_text_too_short",
                    "Alt text should describe the visual in at least a short sentence.",
                )
            )
        if normalized in _GENERIC_DESCRIPTIONS:
            issues.append(
                AltTextIssue(
                    "generic_alt_text",
                    "Alt text is too generic to describe the visual.",
                )
            )
        if _file_name_leaks(text, image_path):
            issues.append(
                AltTextIssue(
                    "file_name_leakage",
                    "Alt text should describe the visual, not expose the image file name.",
                )
            )

        prompt_keywords = _prompt_keywords(image_prompt)
        alt_keywords = _tokens(text)
        if prompt_keywords and not prompt_keywords.intersection(alt_keywords):
            issues.append(
                AltTextIssue(
                    "image_prompt_mismatch",
                    "Alt text does not mention any key terms from the image prompt.",
                )
            )

    passed = not issues
    return AltTextValidation(
        required=True,
        checked=True,
        passed=passed,
        status="passed" if passed else "failed",
        issues=tuple(issues),
    )
