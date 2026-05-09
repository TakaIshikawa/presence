"""Session Grep output mode selection and efficiency analyzer.

Analyzes Grep tool usage patterns in session transcripts to measure output mode
selection appropriateness, context line efficiency, head_limit usage, multiline
mode adoption, and pattern specificity.

Grep efficiency dimensions:
1. output_mode selection:
   - files_with_matches: For discovery and file listing
   - content: For context extraction and code review
   - count: For statistics and quantitative analysis

2. Context lines usage:
   - -A/-B/-C flags for targeted context extraction
   - Efficiency vs full-file reads

3. head_limit usage:
   - Prevents output overflow
   - Controls result volume

4. multiline mode:
   - Cross-line pattern matching
   - Appropriate for structural patterns

5. Pattern specificity:
   - Literal vs regex complexity
   - Pattern precision and targeting

Quality indicators:
- Appropriate output_mode selection for task
- Context lines used instead of full reads
- head_limit prevents excessive output
- multiline mode for cross-line patterns
- Specific patterns minimize noise
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class GrepToolCall:
    """Represents a Grep tool call with parameters."""

    turn_index: int
    pattern: str
    output_mode: str  # files_with_matches, content, count
    context_lines: int  # Combined -A/-B/-C value
    has_context_a: bool
    has_context_b: bool
    has_context_c: bool
    head_limit: int
    multiline: bool
    glob_filter: str
    type_filter: str
    result_count: int


@dataclass(frozen=True)
class Finding:
    """Represents an efficiency finding with severity."""

    severity: str  # critical, warning, info
    category: str
    message: str
    turn_index: int
    example: str  # Concrete transcript excerpt


@dataclass(frozen=True)
class GrepOutputModeMetrics:
    """Aggregate metrics for Grep usage efficiency."""

    total_grep_calls: int
    output_mode_files_count: int
    output_mode_content_count: int
    output_mode_count_count: int
    context_lines_usage_count: int
    context_lines_usage_rate: float
    head_limit_usage_count: int
    head_limit_usage_rate: float
    multiline_usage_count: int
    multiline_usage_rate: float
    specific_pattern_count: int
    broad_pattern_count: int
    pattern_specificity_score: float
    findings_count: int
    critical_findings: int
    warning_findings: int
    info_findings: int


def analyze_session_grep_output_mode(
    tool_calls: Sequence[GrepToolCall],
) -> tuple[GrepOutputModeMetrics, Sequence[Finding]]:
    """Analyze Grep tool usage for efficiency patterns.

    Args:
        tool_calls: Sequence of GrepToolCall instances from session transcript

    Returns:
        Tuple of (metrics, findings) where:
            - metrics: Aggregate statistics about Grep usage
            - findings: Sequence of efficiency findings with severity

    Raises:
        ValueError: If tool_calls is invalid or contains invalid data
    """
    _validate_tool_calls(tool_calls)

    if not tool_calls:
        return (
            GrepOutputModeMetrics(
                total_grep_calls=0,
                output_mode_files_count=0,
                output_mode_content_count=0,
                output_mode_count_count=0,
                context_lines_usage_count=0,
                context_lines_usage_rate=0.0,
                head_limit_usage_count=0,
                head_limit_usage_rate=0.0,
                multiline_usage_count=0,
                multiline_usage_rate=0.0,
                specific_pattern_count=0,
                broad_pattern_count=0,
                pattern_specificity_score=0.0,
                findings_count=0,
                critical_findings=0,
                warning_findings=0,
                info_findings=0,
            ),
            (),
        )

    findings: list[Finding] = []

    # Count output modes
    output_mode_files_count = sum(
        1 for call in tool_calls if call.output_mode == "files_with_matches"
    )
    output_mode_content_count = sum(
        1 for call in tool_calls if call.output_mode == "content"
    )
    output_mode_count_count = sum(
        1 for call in tool_calls if call.output_mode == "count"
    )

    # Count context line usage
    context_lines_usage_count = sum(
        1 for call in tool_calls if call.context_lines > 0
    )

    # Count head_limit usage
    head_limit_usage_count = sum(
        1 for call in tool_calls if call.head_limit > 0
    )

    # Count multiline usage
    multiline_usage_count = sum(1 for call in tool_calls if call.multiline)

    # Analyze pattern specificity
    specific_pattern_count = 0
    broad_pattern_count = 0
    specificity_scores: list[float] = []

    for call in tool_calls:
        score = _calculate_pattern_specificity(call.pattern)
        specificity_scores.append(score)

        if score >= 70.0:
            specific_pattern_count += 1
        elif score <= 30.0:
            broad_pattern_count += 1

    # Detect findings
    findings.extend(_detect_output_mode_misuse(tool_calls))
    findings.extend(_detect_missing_context_lines(tool_calls))
    findings.extend(_detect_missing_head_limit(tool_calls))
    findings.extend(_detect_multiline_opportunities(tool_calls))
    findings.extend(_detect_pattern_specificity_issues(tool_calls))

    # Count findings by severity
    critical_findings = sum(1 for f in findings if f.severity == "critical")
    warning_findings = sum(1 for f in findings if f.severity == "warning")
    info_findings = sum(1 for f in findings if f.severity == "info")

    total = len(tool_calls)
    avg_specificity = sum(specificity_scores) / total if total > 0 else 0.0

    metrics = GrepOutputModeMetrics(
        total_grep_calls=total,
        output_mode_files_count=output_mode_files_count,
        output_mode_content_count=output_mode_content_count,
        output_mode_count_count=output_mode_count_count,
        context_lines_usage_count=context_lines_usage_count,
        context_lines_usage_rate=_percentage(context_lines_usage_count, total),
        head_limit_usage_count=head_limit_usage_count,
        head_limit_usage_rate=_percentage(head_limit_usage_count, total),
        multiline_usage_count=multiline_usage_count,
        multiline_usage_rate=_percentage(multiline_usage_count, total),
        specific_pattern_count=specific_pattern_count,
        broad_pattern_count=broad_pattern_count,
        pattern_specificity_score=round(avg_specificity, 2),
        findings_count=len(findings),
        critical_findings=critical_findings,
        warning_findings=warning_findings,
        info_findings=info_findings,
    )

    return metrics, tuple(findings)


def _validate_tool_calls(tool_calls: Sequence[GrepToolCall]) -> None:
    """Validate tool_calls structure and content."""
    if not isinstance(tool_calls, (list, tuple)):
        raise ValueError("tool_calls must be a list or tuple")

    for i, call in enumerate(tool_calls):
        if not isinstance(call, GrepToolCall):
            raise ValueError(
                f"tool_calls[{i}] must be a GrepToolCall instance"
            )

        if not isinstance(call.turn_index, int) or isinstance(call.turn_index, bool):
            raise ValueError(
                f"tool_calls[{i}].turn_index must be an integer"
            )

        if call.turn_index < 0:
            raise ValueError(
                f"tool_calls[{i}].turn_index must be non-negative"
            )

        if not isinstance(call.pattern, str):
            raise ValueError(
                f"tool_calls[{i}].pattern must be a string"
            )

        if call.output_mode not in {"files_with_matches", "content", "count"}:
            raise ValueError(
                f"tool_calls[{i}].output_mode must be files_with_matches, content, or count"
            )


def _detect_output_mode_misuse(
    tool_calls: Sequence[GrepToolCall],
) -> list[Finding]:
    """Detect inappropriate output_mode selection."""
    findings: list[Finding] = []

    for call in tool_calls:
        # Using content mode without context lines is inefficient
        if call.output_mode == "content" and call.context_lines == 0:
            findings.append(
                Finding(
                    severity="warning",
                    category="output_mode_selection",
                    message=(
                        "Grep with output_mode='content' but no context lines (-A/-B/-C). "
                        "Consider using context flags for targeted extraction or "
                        "files_with_matches mode for discovery."
                    ),
                    turn_index=call.turn_index,
                    example=f"pattern='{call.pattern}', output_mode='content', no context",
                )
            )

        # Using count mode when files_with_matches would suffice
        if call.output_mode == "count" and call.result_count <= 10:
            findings.append(
                Finding(
                    severity="info",
                    category="output_mode_selection",
                    message=(
                        f"Grep with output_mode='count' returned only {call.result_count} results. "
                        "Consider files_with_matches mode for low-count results."
                    ),
                    turn_index=call.turn_index,
                    example=f"pattern='{call.pattern}', output_mode='count', result_count={call.result_count}",
                )
            )

    return findings


def _detect_missing_context_lines(
    tool_calls: Sequence[GrepToolCall],
) -> list[Finding]:
    """Detect content mode without context lines."""
    findings: list[Finding] = []

    for call in tool_calls:
        # Content mode is more useful with context
        if call.output_mode == "content" and call.context_lines == 0:
            if call.result_count > 5:
                findings.append(
                    Finding(
                        severity="warning",
                        category="context_lines_efficiency",
                        message=(
                            f"Grep returned {call.result_count} content results without context lines. "
                            "Consider using -A/-B/-C flags to reduce need for full-file reads."
                        ),
                        turn_index=call.turn_index,
                        example=f"pattern='{call.pattern}', no -A/-B/-C, {call.result_count} results",
                    )
                )

    return findings


def _detect_missing_head_limit(
    tool_calls: Sequence[GrepToolCall],
) -> list[Finding]:
    """Detect Grep calls that should use head_limit."""
    findings: list[Finding] = []

    for call in tool_calls:
        # Large result sets without head_limit cause output overflow
        if call.head_limit == 0 and call.result_count > 100:
            findings.append(
                Finding(
                    severity="critical",
                    category="head_limit_usage",
                    message=(
                        f"Grep returned {call.result_count} results without head_limit. "
                        "Use head_limit parameter to prevent output overflow and token waste."
                    ),
                    turn_index=call.turn_index,
                    example=f"pattern='{call.pattern}', result_count={call.result_count}, no head_limit",
                )
            )
        elif call.head_limit == 0 and call.result_count > 50:
            findings.append(
                Finding(
                    severity="warning",
                    category="head_limit_usage",
                    message=(
                        f"Grep returned {call.result_count} results without head_limit. "
                        "Consider using head_limit to control output volume."
                    ),
                    turn_index=call.turn_index,
                    example=f"pattern='{call.pattern}', result_count={call.result_count}, no head_limit",
                )
            )

    return findings


def _detect_multiline_opportunities(
    tool_calls: Sequence[GrepToolCall],
) -> list[Finding]:
    """Detect patterns that suggest multiline mode should be used."""
    findings: list[Finding] = []

    for call in tool_calls:
        # Detect cross-line patterns without multiline mode
        cross_line_indicators = [
            r"\n",
            r"\r",
            "newline",
            r"[\s\S]",
            r"[^]",
            ".*.*",  # Multiple wildcards suggest cross-line intent
        ]

        has_cross_line_pattern = any(
            indicator in call.pattern for indicator in cross_line_indicators
        )

        if has_cross_line_pattern and not call.multiline:
            findings.append(
                Finding(
                    severity="warning",
                    category="multiline_mode_usage",
                    message=(
                        "Pattern appears to target cross-line matches but multiline mode not enabled. "
                        "Enable multiline=true for patterns spanning multiple lines."
                    ),
                    turn_index=call.turn_index,
                    example=f"pattern='{call.pattern}', multiline=false",
                )
            )

    return findings


def _detect_pattern_specificity_issues(
    tool_calls: Sequence[GrepToolCall],
) -> list[Finding]:
    """Detect overly broad or vague patterns."""
    findings: list[Finding] = []

    for call in tool_calls:
        specificity = _calculate_pattern_specificity(call.pattern)

        # Very broad patterns
        if specificity <= 20.0 and call.result_count > 50:
            findings.append(
                Finding(
                    severity="critical",
                    category="pattern_specificity",
                    message=(
                        f"Very broad pattern '{call.pattern}' returned {call.result_count} results. "
                        "Refine pattern with more specific terms or add glob/type filters."
                    ),
                    turn_index=call.turn_index,
                    example=f"pattern='{call.pattern}', specificity={specificity:.1f}%, results={call.result_count}",
                )
            )
        elif specificity <= 40.0 and call.result_count > 100:
            findings.append(
                Finding(
                    severity="warning",
                    category="pattern_specificity",
                    message=(
                        f"Broad pattern '{call.pattern}' returned {call.result_count} results. "
                        "Consider more specific search terms."
                    ),
                    turn_index=call.turn_index,
                    example=f"pattern='{call.pattern}', specificity={specificity:.1f}%, results={call.result_count}",
                )
            )

    return findings


def _calculate_pattern_specificity(pattern: str) -> float:
    """Calculate pattern specificity score (0-100).

    Higher scores indicate more specific patterns.

    Scoring factors:
    - Length: Longer patterns are more specific
    - Literal characters: More literals = more specific
    - Anchors: ^ and $ increase specificity
    - Word boundaries: \\b increases specificity
    - Wildcards: .* reduces specificity
    - Character classes: Moderate specificity

    Returns:
        Specificity score from 0.0 to 100.0
    """
    if not pattern:
        return 0.0

    score = 50.0  # Base score

    # Length factor
    length_score = min(len(pattern) * 2, 30)
    score += length_score

    # Anchors
    if pattern.startswith("^"):
        score += 10
    if pattern.endswith("$"):
        score += 10

    # Word boundaries
    score += pattern.count(r"\b") * 5

    # Wildcards (reduce specificity)
    score -= pattern.count(".*") * 15
    score -= pattern.count(".+") * 10
    score -= pattern.count("*") * 5

    # Character classes (moderate specificity)
    score += pattern.count("[") * 2

    # Very short patterns are less specific
    if len(pattern) <= 2:
        score *= 0.5

    # Common generic patterns
    generic_patterns = [".", ".*", ".+", "\\w+", "\\s+", "\\d+"]
    if pattern in generic_patterns:
        score = min(score, 10.0)

    return max(0.0, min(100.0, score))


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
