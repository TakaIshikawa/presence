"""Error message clarity analyzer for debugging effectiveness measurement.

Assesses error message quality and debugging effectiveness by analyzing
error message specificity, actionability, context completeness, resolution
guidance, and correlation between error clarity and fix success rate.

Clarity dimensions:
- Specificity: How precisely the error identifies the problem
- Actionability: Whether the message suggests clear next steps
- Context completeness: Presence of relevant context (line numbers, values, stack traces)
- Resolution guidance: Explicit or implicit guidance toward fixes
- Effectiveness correlation: Success rate of fixes based on error clarity

Quality tiers:
- Excellent: Specific, actionable, complete context, clear guidance
- Good: Mostly specific and actionable, some guidance
- Moderate: Generic but some useful information
- Poor: Vague, no actionability, minimal context
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


# Clarity scoring thresholds (0-100 scale)
CLARITY_EXCELLENT = 80
CLARITY_GOOD = 60
CLARITY_MODERATE = 40
CLARITY_POOR = 0

# Specificity levels
SPECIFICITY_SPECIFIC = "specific"  # Identifies exact problem
SPECIFICITY_PARTIAL = "partial"  # Identifies problem area
SPECIFICITY_GENERIC = "generic"  # Vague problem indication

# Actionability levels
ACTIONABILITY_HIGH = "high"  # Clear next steps
ACTIONABILITY_MODERATE = "moderate"  # Some suggestions
ACTIONABILITY_LOW = "low"  # No clear actions

# Context completeness thresholds
MIN_CONTEXT_ELEMENTS = 2  # Line number, value, etc.
GOOD_CONTEXT_ELEMENTS = 4


@dataclass(frozen=True)
class ErrorMessage:
    """An error message with clarity attributes."""

    error_id: str
    message_text: str
    has_line_number: bool
    has_error_value: bool
    has_stack_trace: bool
    has_file_path: bool
    has_resolution_hint: bool
    specificity_level: str  # specific, partial, generic
    actionability_level: str  # high, moderate, low
    was_fixed_successfully: bool | None  # None if not yet resolved


@dataclass(frozen=True)
class ClarityMetrics:
    """Clarity metrics for error messages."""

    avg_specificity_score: float  # 0-100
    avg_actionability_score: float  # 0-100
    avg_context_score: float  # 0-100
    guidance_presence_rate: float  # 0-1 proportion


@dataclass(frozen=True)
class ActionabilityDistribution:
    """Distribution of actionability levels."""

    high_count: int
    moderate_count: int
    low_count: int
    total_count: int


@dataclass(frozen=True)
class ContextScores:
    """Context completeness scoring."""

    avg_context_elements: float
    complete_context_rate: float  # Proportion with good context
    missing_line_numbers: int
    missing_stack_traces: int


@dataclass(frozen=True)
class EffectivenessCorrelation:
    """Correlation between error clarity and fix success."""

    high_clarity_fix_rate: float  # Fix rate for clarity >= 80
    moderate_clarity_fix_rate: float  # Fix rate for 40 <= clarity < 80
    low_clarity_fix_rate: float  # Fix rate for clarity < 40
    correlation_strength: float  # 0-1 scale


@dataclass(frozen=True)
class ErrorMessageClarityAnalysis:
    """Complete error message clarity analysis."""

    clarity_metrics: ClarityMetrics
    actionability_distribution: ActionabilityDistribution
    context_scores: ContextScores
    guidance_presence: float  # 0-1 proportion
    effectiveness_correlation: EffectivenessCorrelation
    total_errors: int
    insights: list[str]


def analyze_error_message_clarity(
    errors: Sequence[ErrorMessage],
) -> dict:
    """Analyze error message quality and debugging effectiveness.

    Analyzes error message specificity, actionability scores, context completeness,
    resolution guidance presence, and correlation between error clarity and fix
    success rate.

    Args:
        errors: Sequence of error messages to analyze

    Returns:
        Dict with:
            - clarity_metrics: ClarityMetrics with avg scores
            - actionability_distribution: ActionabilityDistribution counts
            - context_scores: ContextScores with completeness metrics
            - guidance_presence: Proportion with resolution hints
            - effectiveness_correlation: EffectivenessCorrelation metrics

    Raises:
        ValueError: If errors contains invalid data
    """
    if not isinstance(errors, (list, tuple)):
        raise ValueError("errors must be a sequence (list or tuple)")

    # Validate errors
    for error in errors:
        if not isinstance(error, ErrorMessage):
            raise ValueError("errors must contain ErrorMessage instances")
        if not error.error_id:
            raise ValueError("error_id cannot be empty")
        if error.specificity_level not in [SPECIFICITY_SPECIFIC, SPECIFICITY_PARTIAL, SPECIFICITY_GENERIC]:
            raise ValueError(f"invalid specificity_level: {error.specificity_level}")
        if error.actionability_level not in [ACTIONABILITY_HIGH, ACTIONABILITY_MODERATE, ACTIONABILITY_LOW]:
            raise ValueError(f"invalid actionability_level: {error.actionability_level}")

    # Handle empty errors
    if not errors:
        return {
            "clarity_metrics": {
                "avg_specificity_score": 0.0,
                "avg_actionability_score": 0.0,
                "avg_context_score": 0.0,
                "guidance_presence_rate": 0.0,
            },
            "actionability_distribution": {
                "high_count": 0,
                "moderate_count": 0,
                "low_count": 0,
                "total_count": 0,
            },
            "context_scores": {
                "avg_context_elements": 0.0,
                "complete_context_rate": 0.0,
                "missing_line_numbers": 0,
                "missing_stack_traces": 0,
            },
            "guidance_presence": 0.0,
            "effectiveness_correlation": {
                "high_clarity_fix_rate": 0.0,
                "moderate_clarity_fix_rate": 0.0,
                "low_clarity_fix_rate": 0.0,
                "correlation_strength": 0.0,
            },
        }

    # Calculate clarity metrics
    clarity_metrics = _calculate_clarity_metrics(errors)

    # Calculate actionability distribution
    actionability_distribution = _calculate_actionability_distribution(errors)

    # Calculate context scores
    context_scores = _calculate_context_scores(errors)

    # Calculate guidance presence
    guidance_presence = sum(1 for e in errors if e.has_resolution_hint) / len(errors)

    # Calculate effectiveness correlation
    effectiveness_correlation = _calculate_effectiveness_correlation(errors)

    # Generate insights
    insights = _generate_clarity_insights(
        clarity_metrics=clarity_metrics,
        actionability_distribution=actionability_distribution,
        context_scores=context_scores,
        guidance_presence=guidance_presence,
        effectiveness_correlation=effectiveness_correlation,
        total_errors=len(errors),
    )

    # Build analysis object
    analysis = ErrorMessageClarityAnalysis(
        clarity_metrics=clarity_metrics,
        actionability_distribution=actionability_distribution,
        context_scores=context_scores,
        guidance_presence=guidance_presence,
        effectiveness_correlation=effectiveness_correlation,
        total_errors=len(errors),
        insights=insights,
    )

    # Convert to dict for return
    return {
        "clarity_metrics": {
            "avg_specificity_score": analysis.clarity_metrics.avg_specificity_score,
            "avg_actionability_score": analysis.clarity_metrics.avg_actionability_score,
            "avg_context_score": analysis.clarity_metrics.avg_context_score,
            "guidance_presence_rate": analysis.clarity_metrics.guidance_presence_rate,
        },
        "actionability_distribution": {
            "high_count": analysis.actionability_distribution.high_count,
            "moderate_count": analysis.actionability_distribution.moderate_count,
            "low_count": analysis.actionability_distribution.low_count,
            "total_count": analysis.actionability_distribution.total_count,
        },
        "context_scores": {
            "avg_context_elements": analysis.context_scores.avg_context_elements,
            "complete_context_rate": analysis.context_scores.complete_context_rate,
            "missing_line_numbers": analysis.context_scores.missing_line_numbers,
            "missing_stack_traces": analysis.context_scores.missing_stack_traces,
        },
        "guidance_presence": analysis.guidance_presence,
        "effectiveness_correlation": {
            "high_clarity_fix_rate": analysis.effectiveness_correlation.high_clarity_fix_rate,
            "moderate_clarity_fix_rate": analysis.effectiveness_correlation.moderate_clarity_fix_rate,
            "low_clarity_fix_rate": analysis.effectiveness_correlation.low_clarity_fix_rate,
            "correlation_strength": analysis.effectiveness_correlation.correlation_strength,
        },
    }


def _calculate_clarity_metrics(errors: Sequence[ErrorMessage]) -> ClarityMetrics:
    """Calculate clarity metrics from error messages.

    Args:
        errors: Error messages

    Returns:
        ClarityMetrics with avg scores
    """
    if not errors:
        return ClarityMetrics(
            avg_specificity_score=0.0,
            avg_actionability_score=0.0,
            avg_context_score=0.0,
            guidance_presence_rate=0.0,
        )

    # Calculate specificity scores
    specificity_scores = []
    for error in errors:
        if error.specificity_level == SPECIFICITY_SPECIFIC:
            specificity_scores.append(100.0)
        elif error.specificity_level == SPECIFICITY_PARTIAL:
            specificity_scores.append(60.0)
        else:  # GENERIC
            specificity_scores.append(20.0)

    # Calculate actionability scores
    actionability_scores = []
    for error in errors:
        if error.actionability_level == ACTIONABILITY_HIGH:
            actionability_scores.append(100.0)
        elif error.actionability_level == ACTIONABILITY_MODERATE:
            actionability_scores.append(60.0)
        else:  # LOW
            actionability_scores.append(20.0)

    # Calculate context scores (based on number of context elements)
    context_scores = []
    for error in errors:
        context_count = sum([
            error.has_line_number,
            error.has_error_value,
            error.has_stack_trace,
            error.has_file_path,
        ])
        # Normalize to 0-100 (4 elements = 100)
        context_scores.append((context_count / 4.0) * 100.0)

    # Calculate guidance presence rate
    guidance_presence_rate = sum(1 for e in errors if e.has_resolution_hint) / len(errors)

    return ClarityMetrics(
        avg_specificity_score=round(sum(specificity_scores) / len(specificity_scores), 2),
        avg_actionability_score=round(sum(actionability_scores) / len(actionability_scores), 2),
        avg_context_score=round(sum(context_scores) / len(context_scores), 2),
        guidance_presence_rate=round(guidance_presence_rate, 3),
    )


def _calculate_actionability_distribution(
    errors: Sequence[ErrorMessage],
) -> ActionabilityDistribution:
    """Calculate distribution of actionability levels.

    Args:
        errors: Error messages

    Returns:
        ActionabilityDistribution with counts
    """
    high_count = sum(1 for e in errors if e.actionability_level == ACTIONABILITY_HIGH)
    moderate_count = sum(1 for e in errors if e.actionability_level == ACTIONABILITY_MODERATE)
    low_count = sum(1 for e in errors if e.actionability_level == ACTIONABILITY_LOW)

    return ActionabilityDistribution(
        high_count=high_count,
        moderate_count=moderate_count,
        low_count=low_count,
        total_count=len(errors),
    )


def _calculate_context_scores(errors: Sequence[ErrorMessage]) -> ContextScores:
    """Calculate context completeness scores.

    Args:
        errors: Error messages

    Returns:
        ContextScores with completeness metrics
    """
    if not errors:
        return ContextScores(
            avg_context_elements=0.0,
            complete_context_rate=0.0,
            missing_line_numbers=0,
            missing_stack_traces=0,
        )

    # Count context elements per error
    context_element_counts = []
    for error in errors:
        count = sum([
            error.has_line_number,
            error.has_error_value,
            error.has_stack_trace,
            error.has_file_path,
        ])
        context_element_counts.append(count)

    avg_context_elements = sum(context_element_counts) / len(context_element_counts)

    # Calculate complete context rate (>= GOOD_CONTEXT_ELEMENTS)
    complete_count = sum(1 for count in context_element_counts if count >= GOOD_CONTEXT_ELEMENTS)
    complete_context_rate = complete_count / len(errors)

    # Count missing critical elements
    missing_line_numbers = sum(1 for e in errors if not e.has_line_number)
    missing_stack_traces = sum(1 for e in errors if not e.has_stack_trace)

    return ContextScores(
        avg_context_elements=round(avg_context_elements, 2),
        complete_context_rate=round(complete_context_rate, 3),
        missing_line_numbers=missing_line_numbers,
        missing_stack_traces=missing_stack_traces,
    )


def _calculate_effectiveness_correlation(
    errors: Sequence[ErrorMessage],
) -> EffectivenessCorrelation:
    """Calculate correlation between clarity and fix success.

    Args:
        errors: Error messages

    Returns:
        EffectivenessCorrelation metrics
    """
    # Calculate clarity score for each error
    error_clarity_scores = []
    for error in errors:
        # Average of specificity, actionability, and context
        specificity = 100.0 if error.specificity_level == SPECIFICITY_SPECIFIC else (60.0 if error.specificity_level == SPECIFICITY_PARTIAL else 20.0)
        actionability = 100.0 if error.actionability_level == ACTIONABILITY_HIGH else (60.0 if error.actionability_level == ACTIONABILITY_MODERATE else 20.0)
        context_count = sum([error.has_line_number, error.has_error_value, error.has_stack_trace, error.has_file_path])
        context_score = (context_count / 4.0) * 100.0

        clarity = (specificity + actionability + context_score) / 3.0
        error_clarity_scores.append((error, clarity))

    # Group by clarity level and calculate fix rates
    high_clarity_errors = [(e, c) for e, c in error_clarity_scores if c >= CLARITY_EXCELLENT]
    moderate_clarity_errors = [(e, c) for e, c in error_clarity_scores if CLARITY_MODERATE <= c < CLARITY_EXCELLENT]
    low_clarity_errors = [(e, c) for e, c in error_clarity_scores if c < CLARITY_MODERATE]

    def calculate_fix_rate(error_list):
        if not error_list:
            return 0.0
        fixed_count = sum(1 for e, _ in error_list if e.was_fixed_successfully is True)
        total_resolved = sum(1 for e, _ in error_list if e.was_fixed_successfully is not None)
        return fixed_count / total_resolved if total_resolved > 0 else 0.0

    high_clarity_fix_rate = calculate_fix_rate(high_clarity_errors)
    moderate_clarity_fix_rate = calculate_fix_rate(moderate_clarity_errors)
    low_clarity_fix_rate = calculate_fix_rate(low_clarity_errors)

    # Calculate correlation strength (simplified)
    # Higher clarity should have higher fix rate
    correlation_strength = 0.0
    if high_clarity_fix_rate > moderate_clarity_fix_rate > low_clarity_fix_rate:
        correlation_strength = 1.0  # Perfect correlation
    elif high_clarity_fix_rate > low_clarity_fix_rate:
        correlation_strength = 0.6  # Moderate correlation
    elif high_clarity_fix_rate == moderate_clarity_fix_rate == low_clarity_fix_rate:
        correlation_strength = 0.0  # No correlation

    return EffectivenessCorrelation(
        high_clarity_fix_rate=round(high_clarity_fix_rate, 3),
        moderate_clarity_fix_rate=round(moderate_clarity_fix_rate, 3),
        low_clarity_fix_rate=round(low_clarity_fix_rate, 3),
        correlation_strength=round(correlation_strength, 3),
    )


def _generate_clarity_insights(
    clarity_metrics: ClarityMetrics,
    actionability_distribution: ActionabilityDistribution,
    context_scores: ContextScores,
    guidance_presence: float,
    effectiveness_correlation: EffectivenessCorrelation,
    total_errors: int,
) -> list[str]:
    """Generate actionable insights about error message clarity.

    Args:
        clarity_metrics: Clarity metrics
        actionability_distribution: Actionability distribution
        context_scores: Context scores
        guidance_presence: Guidance presence rate
        effectiveness_correlation: Effectiveness correlation
        total_errors: Total error count

    Returns:
        List of insight strings
    """
    insights = []

    # Overall clarity assessment
    avg_clarity = (
        clarity_metrics.avg_specificity_score +
        clarity_metrics.avg_actionability_score +
        clarity_metrics.avg_context_score
    ) / 3.0

    if avg_clarity >= CLARITY_EXCELLENT:
        insights.append(
            f"Excellent error clarity (avg {avg_clarity:.0f}/100) - "
            "errors are specific, actionable, and well-contextualized"
        )
    elif avg_clarity >= CLARITY_GOOD:
        insights.append(
            f"Good error clarity (avg {avg_clarity:.0f}/100) - "
            "most errors provide useful debugging information"
        )
    elif avg_clarity >= CLARITY_MODERATE:
        insights.append(
            f"Moderate error clarity (avg {avg_clarity:.0f}/100) - "
            "errors could be more specific and actionable"
        )
    else:
        insights.append(
            f"Poor error clarity (avg {avg_clarity:.0f}/100) - "
            "errors lack specificity, actionability, or context"
        )

    # Specificity insights
    if clarity_metrics.avg_specificity_score < 50:
        insights.append(
            f"Low specificity (avg {clarity_metrics.avg_specificity_score:.0f}/100) - "
            "errors are too generic to quickly identify problems"
        )

    # Actionability insights
    if actionability_distribution.low_count > actionability_distribution.high_count:
        insights.append(
            f"{actionability_distribution.low_count}/{total_errors} errors have low actionability - "
            "difficult to determine next steps"
        )
    elif actionability_distribution.high_count > total_errors * 0.7:
        insights.append(
            f"{actionability_distribution.high_count}/{total_errors} errors are highly actionable - "
            "clear debugging path provided"
        )

    # Context completeness insights
    if context_scores.complete_context_rate < 0.5:
        insights.append(
            f"Incomplete context ({context_scores.complete_context_rate:.1%} complete) - "
            f"{context_scores.missing_line_numbers} missing line numbers, "
            f"{context_scores.missing_stack_traces} missing stack traces"
        )
    elif context_scores.complete_context_rate >= 0.8:
        insights.append(
            f"Good context completeness ({context_scores.complete_context_rate:.1%}) - "
            "most errors include relevant debugging context"
        )

    # Guidance presence insights
    if guidance_presence < 0.3:
        insights.append(
            f"Low resolution guidance ({guidance_presence:.1%}) - "
            "consider adding hints or suggestions to error messages"
        )
    elif guidance_presence >= 0.7:
        insights.append(
            f"High resolution guidance ({guidance_presence:.1%}) - "
            "most errors include helpful hints"
        )

    # Effectiveness correlation insights
    if effectiveness_correlation.correlation_strength >= 0.6:
        insights.append(
            f"Clear errors correlate with fix success "
            f"({effectiveness_correlation.high_clarity_fix_rate:.1%} fix rate for high-clarity errors)"
        )
    elif effectiveness_correlation.correlation_strength == 0.0:
        insights.append(
            "No correlation between error clarity and fix success - "
            "clarity improvements may not impact debugging effectiveness"
        )

    return insights
