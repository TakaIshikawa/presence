"""Session error message clarity analyzer for workflow quality reports.

Evaluates the clarity and actionability of error messages in agent sessions.
Clear, actionable errors reduce debugging time and improve task completion rates.

Error clarity indicators:
- File paths and line numbers (e.g., "file.py:42")
- Specific error types (TypeError, ValueError, etc.)
- Actionable remediation suggestions
- Stack traces with context
- Clear problem descriptions

Vague error patterns:
- "Something went wrong"
- "Error occurred"
- "Failed" (without context)
- Generic exception messages
- Missing location information
"""

from __future__ import annotations

import re
from typing import Any, Mapping


# Patterns for detecting file/line location references
LOCATION_PATTERNS = (
    r'\b[\w/.-]+\.py:\d+',  # Python: file.py:42
    r'\b[\w/.-]+\.ts:\d+',  # TypeScript: file.ts:42
    r'\b[\w/.-]+\.js:\d+',  # JavaScript: file.js:42
    r'\b[\w/.-]+\.java:\d+',  # Java: File.java:42
    r'\bline\s+\d+',  # Generic "line 42"
    r'\bat\s+[\w/.-]+:\d+',  # "at file:42"
)

# Actionable keywords that suggest remediation
ACTIONABLE_KEYWORDS = (
    'fix',
    'change',
    'update',
    'modify',
    'replace',
    'add',
    'remove',
    'install',
    'upgrade',
    'check',
    'verify',
    'ensure',
    'try',
    'should',
    'need',
    'must',
    'expected',
    'instead',
)

# Vague error patterns that provide little value
VAGUE_ERROR_PATTERNS = (
    r'\bsomething\s+went\s+wrong\b',
    r'\berror\s+occurred\b',
    r'\bfailed\b(?!\s+\w)',  # "failed" without context
    r'\bunexpected\s+error\b',
    r'\bunknown\s+error\b',
    r'\bgeneric\s+error\b',
    r'\ban\s+error\s+has\s+occurred\b',
)

# Specific error type indicators
SPECIFIC_ERROR_TYPES = (
    'TypeError',
    'ValueError',
    'AttributeError',
    'KeyError',
    'IndexError',
    'NameError',
    'SyntaxError',
    'ImportError',
    'ModuleNotFoundError',
    'FileNotFoundError',
    'PermissionError',
    'RuntimeError',
    'AssertionError',
    'ZeroDivisionError',
)


def analyze_session_error_message_clarity(records: object) -> dict[str, Any]:
    """Analyze error message clarity and actionability in a session.

    Evaluates error messages from tool results and agent responses to assess
    their quality, actionability, and potential impact on debugging efficiency.

    Args:
        records: List of error message dictionaries with keys:
            - message: The error message text
            - source: Source of error ('tool_result' or 'agent_message')
            - turn_index: Turn number when error occurred
            - tool_type: Optional tool that generated the error

    Returns:
        Dict with:
            - total_errors: Total error messages detected
            - errors_with_location: Number with file/line references
            - errors_with_actionable_guidance: Number with remediation suggestions
            - vague_errors: Number with vague/generic messages
            - errors_with_specific_types: Number with specific error types
            - location_rate: Percentage with location info
            - actionable_rate: Percentage with actionable guidance
            - vague_rate: Percentage that are vague
            - avg_message_length: Average message length in characters
            - avg_message_complexity: Average word count
            - clarity_score: Overall quality score (0-100)
            - examples: Sample errors with quality assessments

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of error message dictionaries")

    total_errors = 0
    errors_with_location = 0
    errors_with_actionable_guidance = 0
    vague_errors = 0
    errors_with_specific_types = 0
    total_length = 0
    total_words = 0
    examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        message = _string(record.get("message"))
        if not message:
            continue

        total_errors += 1
        turn_index = record.get("turn_index", index)
        source = _string(record.get("source"))
        tool_type = _string(record.get("tool_type"))

        # Calculate message metrics
        message_length = len(message)
        word_count = len(message.split())
        total_length += message_length
        total_words += word_count

        # Assess clarity indicators
        has_location = _has_location_info(message)
        has_actionable = _has_actionable_guidance(message)
        is_vague = _is_vague_error(message)
        has_specific_type = _has_specific_error_type(message)

        if has_location:
            errors_with_location += 1
        if has_actionable:
            errors_with_actionable_guidance += 1
        if is_vague:
            vague_errors += 1
        if has_specific_type:
            errors_with_specific_types += 1

        # Calculate quality score for this error
        quality_score = _calculate_error_quality_score(
            has_location=has_location,
            has_actionable=has_actionable,
            is_vague=is_vague,
            has_specific_type=has_specific_type,
        )

        # Add example if we have fewer than 5
        if len(examples) < 5:
            examples.append({
                'turn_index': turn_index,
                'source': source or 'unknown',
                'tool_type': tool_type or None,
                'message_excerpt': message[:200],  # Limit to 200 chars
                'length': message_length,
                'word_count': word_count,
                'has_location': has_location,
                'has_actionable_guidance': has_actionable,
                'is_vague': is_vague,
                'has_specific_type': has_specific_type,
                'quality_score': quality_score,
            })

    # Calculate aggregate metrics
    location_rate = _percentage(errors_with_location, total_errors)
    actionable_rate = _percentage(errors_with_actionable_guidance, total_errors)
    vague_rate = _percentage(vague_errors, total_errors)
    specific_type_rate = _percentage(errors_with_specific_types, total_errors)

    avg_message_length = round(total_length / total_errors, 2) if total_errors > 0 else 0.0
    avg_message_complexity = round(total_words / total_errors, 2) if total_errors > 0 else 0.0

    # Calculate overall clarity score (0-100)
    clarity_score = _calculate_overall_clarity_score(
        location_rate=location_rate,
        actionable_rate=actionable_rate,
        vague_rate=vague_rate,
        specific_type_rate=specific_type_rate,
    )

    return {
        'total_errors': total_errors,
        'errors_with_location': errors_with_location,
        'errors_with_actionable_guidance': errors_with_actionable_guidance,
        'vague_errors': vague_errors,
        'errors_with_specific_types': errors_with_specific_types,
        'location_rate': location_rate,
        'actionable_rate': actionable_rate,
        'vague_rate': vague_rate,
        'specific_type_rate': specific_type_rate,
        'avg_message_length': avg_message_length,
        'avg_message_complexity': avg_message_complexity,
        'clarity_score': clarity_score,
        'examples': examples[:5],
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _has_location_info(message: str) -> bool:
    """Check if message contains file/line location references."""
    if not message:
        return False
    for pattern in LOCATION_PATTERNS:
        if re.search(pattern, message, re.IGNORECASE):
            return True
    return False


def _has_actionable_guidance(message: str) -> bool:
    """Check if message contains actionable remediation suggestions."""
    if not message:
        return False
    normalized = message.lower()
    return any(keyword in normalized for keyword in ACTIONABLE_KEYWORDS)


def _is_vague_error(message: str) -> bool:
    """Check if message is vague or generic."""
    if not message:
        return True
    normalized = message.lower()
    for pattern in VAGUE_ERROR_PATTERNS:
        if re.search(pattern, normalized):
            return True
    return False


def _has_specific_error_type(message: str) -> bool:
    """Check if message contains specific error type identifiers."""
    if not message:
        return False
    return any(error_type in message for error_type in SPECIFIC_ERROR_TYPES)


def _calculate_error_quality_score(
    has_location: bool,
    has_actionable: bool,
    is_vague: bool,
    has_specific_type: bool,
) -> int:
    """Calculate quality score for an individual error message (0-100)."""
    score = 0

    # Location info is highly valuable
    if has_location:
        score += 35

    # Actionable guidance is critical
    if has_actionable:
        score += 30

    # Specific error types help diagnosis
    if has_specific_type:
        score += 20

    # Vague errors are severely penalized
    if is_vague:
        score -= 50

    # Ensure score stays in 0-100 range
    return max(0, min(100, score))


def _calculate_overall_clarity_score(
    location_rate: float,
    actionable_rate: float,
    vague_rate: float,
    specific_type_rate: float,
) -> float:
    """Calculate overall clarity score for the session (0-100)."""
    # If all rates are zero (no errors), return 0
    if location_rate == 0.0 and actionable_rate == 0.0 and specific_type_rate == 0.0 and vague_rate == 0.0:
        return 0.0

    # Weight different factors
    score = (
        location_rate * 0.35 +
        actionable_rate * 0.30 +
        specific_type_rate * 0.20 +
        (100.0 - vague_rate) * 0.15  # Penalize vague errors
    )
    return round(score, 2)


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
