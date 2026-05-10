"""Pack-level verification coverage analyzer.

Examines tool calls across a pack to measure verification discipline:
how consistently implementation turns are followed by verification turns
(tests, builds, lints), and whether tests are targeted or broad.

Metrics:
- implementation_turns: Messages containing Edit/Write/NotebookEdit calls
- verification_turns: Messages containing test/build/lint/verify calls
- implementation_verification_ratio: verification / implementation turns
- unverified_edits: Edits not followed by verification within 5 turns
- late_verification: All verification in last 20% of session turns
- targeted_tests / broad_tests: Test specificity breakdown
- verification_coverage_score: Weighted composite (0-1)
"""

from __future__ import annotations

import re
from typing import Any, Mapping


_IMPLEMENTATION_TOOLS = {"Edit", "Write", "NotebookEdit"}

_TEST_PATTERNS = re.compile(
    r"\b(?:pytest|npm\s+test|vitest|jest|cargo\s+test|go\s+test)\b"
)

_BUILD_PATTERNS = re.compile(
    r"\b(?:npm\s+run\s+build|tsc|cargo\s+build|make)\b"
)

_LINT_PATTERNS = re.compile(
    r"\b(?:eslint|ruff|flake8|mypy|pyright)\b"
)

# For targeted test detection: test command followed by a path-like argument
_TARGETED_TEST_PATTERN = re.compile(
    r"\b(?:pytest|vitest|jest|cargo\s+test|go\s+test)\b\s+\S+"
)


def _empty_result() -> dict[str, Any]:
    return {
        "total_messages": 0,
        "implementation_turns": 0,
        "verification_turns": 0,
        "implementation_verification_ratio": 0.0,
        "unverified_edits": 0,
        "late_verification": False,
        "targeted_tests": 0,
        "broad_tests": 0,
        "test_specificity_ratio": 0.0,
        "verification_coverage_score": 1.0,
    }


def _is_implementation_turn(tool_calls: list) -> bool:
    """Check if any tool call in the message is an implementation tool."""
    for tc in tool_calls:
        if not isinstance(tc, Mapping):
            continue
        if tc.get("tool_name") in _IMPLEMENTATION_TOOLS:
            return True
    return False


def _is_verification_turn(tool_calls: list) -> bool:
    """Check if any tool call in the message is a verification action."""
    for tc in tool_calls:
        if not isinstance(tc, Mapping):
            continue
        tool_name = tc.get("tool_name", "")

        if tool_name == "Bash":
            command = str(tc.get("command", ""))
            if _TEST_PATTERNS.search(command):
                return True
            if _BUILD_PATTERNS.search(command):
                return True
            if _LINT_PATTERNS.search(command):
                return True

        if tool_name == "Skill":
            skill = str(tc.get("skill", ""))
            if skill == "verify":
                return True

    return False


def _classify_tests(tool_calls: list) -> tuple[int, int]:
    """Return (targeted, broad) test counts from tool calls in a message."""
    targeted = 0
    broad = 0
    for tc in tool_calls:
        if not isinstance(tc, Mapping):
            continue
        if tc.get("tool_name") != "Bash":
            continue
        command = str(tc.get("command", ""))
        if not _TEST_PATTERNS.search(command):
            continue
        if _TARGETED_TEST_PATTERN.search(command):
            targeted += 1
        else:
            broad += 1
    return targeted, broad


def analyze_pack_verification_coverage(records: object) -> dict[str, Any]:
    """Analyze verification coverage across pack records.

    Args:
        records: List of pack dictionaries with sessions/messages/tool_calls.

    Returns:
        Dict with verification coverage metrics and composite score.

    Raises:
        ValueError: If records is not a list.
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")
    if not records:
        return _empty_result()

    total_messages = 0
    implementation_turns = 0
    verification_turns = 0
    unverified_edits = 0
    targeted_tests = 0
    broad_tests = 0
    late_verification = False

    # Track across all sessions; late_verification is per-session
    any_session_has_verification = False
    all_sessions_late = True

    for record in records:
        if not isinstance(record, Mapping):
            continue

        sessions = record.get("sessions")
        if not isinstance(sessions, list):
            continue

        for session in sessions:
            if not isinstance(session, Mapping):
                continue

            messages = session.get("messages")
            if not isinstance(messages, list):
                continue

            # Build per-session turn classification arrays
            session_impl = []
            session_verif = []

            for message in messages:
                if not isinstance(message, Mapping):
                    continue

                tool_calls = message.get("tool_calls")
                if not isinstance(tool_calls, list):
                    tool_calls = []

                total_messages += 1
                is_impl = _is_implementation_turn(tool_calls)
                is_verif = _is_verification_turn(tool_calls)

                session_impl.append(is_impl)
                session_verif.append(is_verif)

                if is_impl:
                    implementation_turns += 1
                if is_verif:
                    verification_turns += 1

                # Classify test specificity
                t, b = _classify_tests(tool_calls)
                targeted_tests += t
                broad_tests += b

            n_turns = len(session_impl)

            # Unverified edits: implementation turns not followed by
            # any verification within 5 subsequent turns
            for i, is_impl in enumerate(session_impl):
                if not is_impl:
                    continue
                found_verif = False
                for j in range(i + 1, min(i + 6, n_turns)):
                    if session_verif[j]:
                        found_verif = True
                        break
                if not found_verif:
                    unverified_edits += 1

            # Late verification: all verification in last 20% of turns
            verif_indices = [i for i, v in enumerate(session_verif) if v]
            if verif_indices:
                any_session_has_verification = True
                threshold = n_turns * 0.8
                if all(i >= threshold for i in verif_indices):
                    pass  # this session is late
                else:
                    all_sessions_late = False
            # Sessions with no verification don't affect late_verification

    if any_session_has_verification and all_sessions_late:
        late_verification = True

    if total_messages == 0:
        return _empty_result()

    # Ratios
    iv_ratio = (
        verification_turns / implementation_turns
        if implementation_turns > 0
        else 0.0
    )
    test_specificity_ratio = targeted_tests / max(1, targeted_tests + broad_tests)

    # Score
    iv_ratio_score = min(1.0, iv_ratio)
    unverified_penalty = unverified_edits / max(1, implementation_turns)
    late_penalty = 0.2 if late_verification else 0.0

    score = (
        0.4 * iv_ratio_score
        + 0.3 * (1.0 - unverified_penalty)
        + 0.2 * test_specificity_ratio
        + 0.1 * (1.0 - late_penalty)
    )
    score = max(0.0, min(1.0, score))

    return {
        "total_messages": total_messages,
        "implementation_turns": implementation_turns,
        "verification_turns": verification_turns,
        "implementation_verification_ratio": round(iv_ratio, 3),
        "unverified_edits": unverified_edits,
        "late_verification": late_verification,
        "targeted_tests": targeted_tests,
        "broad_tests": broad_tests,
        "test_specificity_ratio": round(test_specificity_ratio, 3),
        "verification_coverage_score": round(score, 4),
    }
