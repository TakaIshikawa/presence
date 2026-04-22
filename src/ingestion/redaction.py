"""Reusable text redaction for ingestion sources."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Pattern


DEFAULT_REDACTION_PATTERNS: tuple[dict[str, str], ...] = (
    {
        "name": "private_key",
        "pattern": r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z0-9 ]*PRIVATE KEY-----",
        "placeholder": "[REDACTED_PRIVATE_KEY]",
    },
    {
        "name": "bearer_token",
        "pattern": r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{8,}",
        "placeholder": "[REDACTED_BEARER]",
    },
    {
        "name": "github_token",
        "pattern": r"\b(?:gh[opsu]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,})\b",
        "placeholder": "[REDACTED_SECRET]",
    },
    {
        "name": "llm_api_key",
        "pattern": r"\b(?:sk-ant-[A-Za-z0-9_-]{16,}|sk-[A-Za-z0-9_-]{20,})\b",
        "placeholder": "[REDACTED_SECRET]",
    },
    {
        "name": "slack_token",
        "pattern": r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b",
        "placeholder": "[REDACTED_SECRET]",
    },
    {
        "name": "jwt",
        "pattern": r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b",
        "placeholder": "[REDACTED_SECRET]",
    },
    {
        "name": "secret_assignment",
        "pattern": r"(?i)\b((?:api[_-]?key|token|secret|password|passwd|pwd|access[_-]?token|refresh[_-]?token|client[_-]?secret)\s*[:=]\s*)(['\"]?)([^'\"\s,`]+)(['\"]?)",
        "replacement": r"\1\2[REDACTED_SECRET]\4",
    },
    {
        "name": "email",
        "pattern": r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b",
        "placeholder": "[REDACTED_EMAIL]",
        "flags": "IGNORECASE",
    },
    {
        "name": "macos_user_path",
        "pattern": r"(?<!\w)/Users/[^/\s]+(?:/[^\s,;:'\")\]]+)*",
        "placeholder": "[REDACTED_PATH]",
    },
    {
        "name": "linux_home_path",
        "pattern": r"(?<!\w)/home/[^/\s]+(?:/[^\s,;:'\")\]]+)*",
        "placeholder": "[REDACTED_PATH]",
    },
    {
        "name": "windows_user_path",
        "pattern": r"\b[A-Za-z]:\\Users\\[^\\\s]+(?:\\[^\s,;:'\")\]]+)*",
        "placeholder": "[REDACTED_PATH]",
    },
)

_FLAG_MAP = {
    "ASCII": re.ASCII,
    "IGNORECASE": re.IGNORECASE,
    "MULTILINE": re.MULTILINE,
    "DOTALL": re.DOTALL,
}


@dataclass(frozen=True)
class RedactionPattern:
    name: str
    regex: Pattern[str]
    replacement: str

    @classmethod
    def from_config(cls, config: str | dict[str, Any], index: int = 0) -> "RedactionPattern":
        if isinstance(config, str):
            raw = {"name": f"custom_{index}", "pattern": config}
        else:
            raw = dict(config)

        pattern = raw.get("pattern")
        if not pattern:
            raise ValueError("Redaction pattern must include a non-empty 'pattern'")

        flags = _parse_flags(raw.get("flags"))
        replacement = raw.get("replacement", raw.get("placeholder", "[REDACTED_SECRET]"))
        return cls(
            name=raw.get("name", f"custom_{index}"),
            regex=re.compile(pattern, flags),
            replacement=replacement,
        )

    def apply(self, text: str) -> str:
        return self.regex.sub(self.replacement, text)


def _parse_flags(flags: Any) -> int:
    if flags is None:
        return 0
    if isinstance(flags, str):
        flags = [flags]
    parsed = 0
    for flag in flags:
        parsed |= _FLAG_MAP.get(str(flag).upper(), 0)
    return parsed


class Redactor:
    """Apply ordered regex redaction rules to text."""

    def __init__(self, patterns: Iterable[str | dict[str, Any]] | None = None):
        raw_patterns = DEFAULT_REDACTION_PATTERNS if patterns is None else tuple(patterns)
        self.patterns = [
            RedactionPattern.from_config(pattern, index=i)
            for i, pattern in enumerate(raw_patterns)
        ]

    def redact(self, text: str) -> str:
        if not text:
            return text

        redacted = text
        for pattern in self.patterns:
            redacted = pattern.apply(redacted)
        return redacted


def redact_text(
    text: str,
    patterns: Iterable[str | dict[str, Any]] | None = None,
) -> str:
    """Redact sensitive spans from *text* using default or supplied patterns."""
    return Redactor(patterns).redact(text)
