"""Session context retention analysis for conversation coherence tracking.

Measures how well Claude sessions maintain context over time by analyzing
context decay rates, reference chain lengths, and topic drift metrics.

Coherence metrics:
- Context decay rate: How quickly context degrades over turns
- Reference chain length: Depth of reference chains to earlier context
- Topic drift: Deviation from initial topics over session
- Context reactivation: References to context from earlier turns

Session quality assessment:
- Coherent: Strong context retention throughout
- Moderate: Some drift but manageable
- Fragmented: Significant context loss
- Disconnected: Minimal context retention
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Optional


# Context retention quality tiers
TIER_COHERENT = "coherent"
TIER_MODERATE = "moderate"
TIER_FRAGMENTED = "fragmented"
TIER_DISCONNECTED = "disconnected"

# Thresholds for quality classification (0-100 scale)
THRESHOLD_MODERATE = 60
THRESHOLD_FRAGMENTED = 40
THRESHOLD_DISCONNECTED = 20

# Decay rate thresholds
DECAY_RATE_LOW = 0.1  # <10% decay per turn
DECAY_RATE_MODERATE = 0.3  # 10-30% decay per turn
DECAY_RATE_HIGH = 0.3  # >30% decay per turn

# Reference chain thresholds
MIN_HEALTHY_CHAIN_LENGTH = 3  # Should reference back at least 3 turns


@dataclass(frozen=True)
class SessionTurn:
    """Single turn in a conversation session."""

    turn_number: int
    references_turn: Optional[int]  # None if no explicit reference
    topic_keywords: Sequence[str]  # Keywords representing topics


@dataclass(frozen=True)
class ContextRetentionMetrics:
    """Context retention metrics for a session."""

    decay_rate: float  # 0-1 scale, average decay per turn
    avg_reference_chain_length: float  # Average length of reference chains
    topic_drift_score: float  # 0-1 scale, 0=no drift, 1=complete drift
    reactivation_rate: float  # Proportion of turns referencing earlier context


@dataclass(frozen=True)
class SessionContextRetention:
    """Session context retention analysis."""

    coherence_score: float  # 0-100 composite score
    quality_tier: str  # coherent, moderate, fragmented, disconnected
    metrics: ContextRetentionMetrics
    insights: list[str]


def analyze_session_context_retention(
    turns: Sequence[SessionTurn],
) -> SessionContextRetention:
    """Analyze context retention across session turns.

    Args:
        turns: Sequence of session turns with reference information

    Returns:
        SessionContextRetention with coherence score and insights

    Raises:
        ValueError: If turns contains invalid data
    """
    if not isinstance(turns, (list, tuple)):
        raise ValueError("turns must be a sequence (list or tuple)")

    # Validate turns
    for turn in turns:
        if not isinstance(turn, SessionTurn):
            raise ValueError("turns must contain SessionTurn instances")
        if turn.turn_number < 0:
            raise ValueError("turn_number must be non-negative")
        if turn.references_turn is not None and turn.references_turn < 0:
            raise ValueError("references_turn must be non-negative or None")

    # Handle empty session
    if not turns:
        return SessionContextRetention(
            coherence_score=0.0,
            quality_tier=TIER_DISCONNECTED,
            metrics=ContextRetentionMetrics(
                decay_rate=0.0,
                avg_reference_chain_length=0.0,
                topic_drift_score=0.0,
                reactivation_rate=0.0,
            ),
            insights=["Empty session - no context to analyze"],
        )

    # Calculate metrics
    decay_rate = _calculate_context_decay_rate(turns)
    avg_chain_length = _calculate_avg_reference_chain_length(turns)
    topic_drift = _calculate_topic_drift_score(turns)
    reactivation_rate = _calculate_reactivation_rate(turns)

    metrics = ContextRetentionMetrics(
        decay_rate=round(decay_rate, 3),
        avg_reference_chain_length=round(avg_chain_length, 2),
        topic_drift_score=round(topic_drift, 3),
        reactivation_rate=round(reactivation_rate, 3),
    )

    # Calculate composite coherence score (0-100)
    coherence_score = _calculate_coherence_score(metrics)

    # Classify quality tier
    quality_tier = _classify_quality_tier(coherence_score)

    # Generate insights
    insights = _generate_insights(
        coherence_score=coherence_score,
        quality_tier=quality_tier,
        metrics=metrics,
        turn_count=len(turns),
    )

    return SessionContextRetention(
        coherence_score=round(coherence_score, 2),
        quality_tier=quality_tier,
        metrics=metrics,
        insights=insights,
    )


def _calculate_context_decay_rate(turns: Sequence[SessionTurn]) -> float:
    """Calculate average context decay rate per turn.

    Decay is measured as the proportion of turns that don't reference
    recent context (within last 3 turns).

    Args:
        turns: Session turns

    Returns:
        Decay rate (0-1 scale)
    """
    if len(turns) <= 1:
        return 0.0

    decay_count = 0
    for i, turn in enumerate(turns[1:], start=1):  # Skip first turn
        # Check if this turn references any of the last 3 turns
        recent_turns = set(range(max(0, i - 3), i))
        if turn.references_turn is None or turn.references_turn not in recent_turns:
            decay_count += 1

    return decay_count / (len(turns) - 1)


def _calculate_avg_reference_chain_length(turns: Sequence[SessionTurn]) -> float:
    """Calculate average reference chain length.

    Chain length is how many turns back a reference goes.

    Args:
        turns: Session turns

    Returns:
        Average chain length (0+ scale)
    """
    if not turns:
        return 0.0

    chain_lengths = []
    for turn in turns:
        if turn.references_turn is not None:
            chain_length = turn.turn_number - turn.references_turn
            chain_lengths.append(chain_length)

    if not chain_lengths:
        return 0.0

    return sum(chain_lengths) / len(chain_lengths)


def _calculate_topic_drift_score(turns: Sequence[SessionTurn]) -> float:
    """Calculate topic drift score.

    Measures how much topics have diverged from initial topics.

    Args:
        turns: Session turns

    Returns:
        Drift score (0-1 scale, 0=no drift, 1=complete drift)
    """
    if len(turns) <= 1:
        return 0.0

    # Get initial topics (first turn)
    initial_topics = set(turns[0].topic_keywords)

    if not initial_topics:
        return 0.0

    # Calculate drift for each subsequent turn
    drift_scores = []
    for turn in turns[1:]:
        current_topics = set(turn.topic_keywords)
        if not current_topics:
            # No topics - complete drift
            drift_scores.append(1.0)
            continue

        # Jaccard distance: 1 - (intersection / union)
        intersection = len(initial_topics & current_topics)
        union = len(initial_topics | current_topics)
        drift = 1.0 - (intersection / union if union > 0 else 0.0)
        drift_scores.append(drift)

    return sum(drift_scores) / len(drift_scores) if drift_scores else 0.0


def _calculate_reactivation_rate(turns: Sequence[SessionTurn]) -> float:
    """Calculate context reactivation rate.

    Proportion of turns that reference earlier context.

    Args:
        turns: Session turns

    Returns:
        Reactivation rate (0-1 scale)
    """
    if not turns:
        return 0.0

    referenced_count = sum(1 for turn in turns if turn.references_turn is not None)

    return referenced_count / len(turns)


def _calculate_coherence_score(metrics: ContextRetentionMetrics) -> float:
    """Calculate composite coherence score (0-100).

    Weights:
    - Low decay: 35%
    - Good reference chains: 25%
    - Low topic drift: 25%
    - High reactivation: 15%

    Args:
        metrics: Context retention metrics

    Returns:
        Coherence score (0-100)
    """
    # Invert decay rate (low decay is good)
    decay_component = (1.0 - metrics.decay_rate) * 35

    # Normalize reference chain length (cap at 10)
    chain_component = min(1.0, metrics.avg_reference_chain_length / 10.0) * 25

    # Invert topic drift (low drift is good)
    drift_component = (1.0 - metrics.topic_drift_score) * 25

    # Reactivation rate (high is good)
    reactivation_component = metrics.reactivation_rate * 15

    score = decay_component + chain_component + drift_component + reactivation_component

    return min(100.0, max(0.0, score))


def _classify_quality_tier(coherence_score: float) -> str:
    """Classify coherence score into quality tier.

    Args:
        coherence_score: Coherence score (0-100)

    Returns:
        Quality tier name
    """
    if coherence_score >= THRESHOLD_MODERATE:
        return TIER_COHERENT
    elif coherence_score >= THRESHOLD_FRAGMENTED:
        return TIER_MODERATE
    elif coherence_score >= THRESHOLD_DISCONNECTED:
        return TIER_FRAGMENTED
    else:
        return TIER_DISCONNECTED


def _generate_insights(
    coherence_score: float,
    quality_tier: str,
    metrics: ContextRetentionMetrics,
    turn_count: int,
) -> list[str]:
    """Generate actionable insights for context retention.

    Args:
        coherence_score: Overall coherence score
        quality_tier: Quality tier classification
        metrics: Context retention metrics
        turn_count: Number of turns in session

    Returns:
        List of actionable insights
    """
    insights = []

    # Tier-based insights
    if quality_tier == TIER_COHERENT:
        insights.append(
            f"Coherent session ({coherence_score:.1f}/100) - strong context retention throughout"
        )
    elif quality_tier == TIER_MODERATE:
        insights.append(
            f"Moderate coherence ({coherence_score:.1f}/100) - some context drift but manageable"
        )
    elif quality_tier == TIER_FRAGMENTED:
        insights.append(
            f"Fragmented session ({coherence_score:.1f}/100) - significant context loss"
        )
    else:  # DISCONNECTED
        insights.append(
            f"Disconnected session ({coherence_score:.1f}/100) - minimal context retention"
        )

    # Decay rate insights
    if metrics.decay_rate > DECAY_RATE_HIGH:
        insights.append(
            f"High context decay ({metrics.decay_rate:.1%} per turn) - "
            "turns not referencing recent context"
        )
    elif metrics.decay_rate < DECAY_RATE_LOW:
        insights.append(
            f"Low context decay ({metrics.decay_rate:.1%}) - strong short-term memory"
        )

    # Reference chain insights
    if metrics.avg_reference_chain_length < 1.0 and turn_count > 5:
        insights.append(
            "Very short reference chains - turns only reference immediate previous context"
        )
    elif metrics.avg_reference_chain_length >= MIN_HEALTHY_CHAIN_LENGTH:
        insights.append(
            f"Good reference depth ({metrics.avg_reference_chain_length:.1f} turns back) - "
            "maintaining longer-term context"
        )

    # Topic drift insights
    if metrics.topic_drift_score > 0.7:
        insights.append(
            f"High topic drift ({metrics.topic_drift_score:.1%}) - "
            "conversation has diverged significantly from initial topics"
        )
    elif metrics.topic_drift_score < 0.3:
        insights.append(
            f"Low topic drift ({metrics.topic_drift_score:.1%}) - staying focused on core topics"
        )

    # Reactivation insights
    if metrics.reactivation_rate < 0.3 and turn_count > 5:
        insights.append(
            f"Low reactivation rate ({metrics.reactivation_rate:.1%}) - "
            "many turns without explicit context references"
        )
    elif metrics.reactivation_rate > 0.7:
        insights.append(
            f"High reactivation rate ({metrics.reactivation_rate:.1%}) - "
            "frequent references to earlier context"
        )

    # Session length insights
    if turn_count > 20:
        if quality_tier == TIER_COHERENT:
            insights.append(
                f"Impressive coherence over {turn_count} turns - well-structured long session"
            )
        elif quality_tier in [TIER_FRAGMENTED, TIER_DISCONNECTED]:
            insights.append(
                f"Long session ({turn_count} turns) with poor coherence - "
                "consider breaking into focused subsessions"
            )

    # No references at all
    if metrics.reactivation_rate == 0.0 and turn_count > 3:
        insights.append(
            "No explicit context references detected - session may lack conversational continuity"
        )

    return insights
