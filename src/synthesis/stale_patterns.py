"""Shared stale rhetorical pattern detection for content filtering.

Patterns that produce structural monotony are filtered from:
- Few-shot examples (to prevent the generator from learning to imitate them)
- Generated candidates (to reject overused rhetorical patterns)
"""

import re


# Overused rhetorical patterns to reject
STALE_PATTERNS = [
    re.compile(r"(?i)^AI\s"),
    re.compile(r"(?i)\bbreakthrough\b"),
    re.compile(r"(?i)perfect (prompts?|memory|agents?|handoffs?|context)"),
    re.compile(r"\d+ commits? across \d+"),
    re.compile(r"(?i)^(TWEET 1:\s*\n)?Today.s (insight|breakthrough|lesson)"),
    # Engagement-bait openings
    re.compile(r"(?i)^(unpopular opinion|controversial take)\s*[:\-–—]"),
    re.compile(r"(?i)\bnobody (is )?(talk(s|ing) about|mentions?)"),
    re.compile(r"(?i)^the (secret|trick) to\b"),
    re.compile(r"(?i)^stop \w[\w ]{0,30}\.\s*start \w"),
    re.compile(r"(?i)\w[\w ]{0,30} (is|are) dead\.\s*long live\b"),
    re.compile(r"(?i)^most (people|developers?|devs|engineers?) don.t\b"),
    re.compile(r"(?i)^everyone (says|preaches|thinks|knows|believes)\b"),
]


def has_stale_pattern(text: str) -> bool:
    """Check if text matches any stale rhetorical pattern."""
    return any(p.search(text) for p in STALE_PATTERNS)
