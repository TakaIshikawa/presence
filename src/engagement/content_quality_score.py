"""Content quality score calculation for publication effectiveness tracking.

Calculates composite quality metrics for published content to track
publication effectiveness, engagement patterns, and content freshness.

Quality signals:
- Engagement rate: Interaction density (likes, replies, shares per view)
- Reply depth: Quality of conversation generated (reply chain length)
- Content freshness: Recency and timeliness of content
- Signal consistency: Variance in engagement across similar content

Composite scoring:
- Weighted combination of individual quality signals
- Normalized to 0-100 scale for consistency
- Handles missing data gracefully with configurable defaults
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


# Weight factors for composite score
WEIGHT_ENGAGEMENT_RATE = 0.40  # Primary signal
WEIGHT_REPLY_DEPTH = 0.25  # Conversation quality
WEIGHT_FRESHNESS = 0.20  # Timeliness
WEIGHT_CONSISTENCY = 0.15  # Reliability

# Normalization constants
MAX_ENGAGEMENT_RATE = 0.10  # 10% engagement is excellent
MAX_REPLY_DEPTH = 10.0  # 10-deep reply chains are excellent
FRESHNESS_DECAY_DAYS = 30.0  # Content decays over 30 days
MIN_CONSISTENCY_SCORE = 0.0  # Lower variance is better

# Quality tiers
TIER_POOR = "poor"
TIER_BELOW_AVERAGE = "below_average"
TIER_AVERAGE = "average"
TIER_GOOD = "good"
TIER_EXCELLENT = "excellent"

THRESHOLD_BELOW_AVERAGE = 30
THRESHOLD_AVERAGE = 50
THRESHOLD_GOOD = 70
THRESHOLD_EXCELLENT = 85


@dataclass(frozen=True)
class ContentMetrics:
    """Raw content metrics for quality calculation."""

    views: int
    likes: int
    replies: int
    shares: int
    reply_depth_avg: float  # Average depth of reply chains
    published_at: datetime
    engagement_variance: Optional[float] = None  # Variance across similar content


@dataclass(frozen=True)
class ContentQualityScore:
    """Content quality score and breakdown."""

    score: float  # 0-100 composite score
    tier: str  # Quality tier classification
    metrics: ContentMetrics  # Raw input metrics
    component_scores: dict[str, float]  # Breakdown by signal
    insights: list[str]  # Actionable insights


def calculate_content_quality_score(
    views: int,
    likes: int,
    replies: int,
    shares: int,
    reply_depth_avg: float,
    published_at: datetime,
    engagement_variance: Optional[float] = None,
    now: Optional[datetime] = None,
) -> ContentQualityScore:
    """Calculate composite content quality score.

    Args:
        views: Number of views/impressions
        likes: Number of likes/favorites
        replies: Number of direct replies
        shares: Number of shares/reposts
        reply_depth_avg: Average depth of reply chains
        published_at: Publication timestamp
        engagement_variance: Optional variance in engagement across similar content
        now: Optional current time (defaults to UTC now)

    Returns:
        ContentQualityScore with composite score, tier, and insights

    Raises:
        ValueError: If metrics are invalid (negative values, future dates)
    """
    # Validate inputs
    if views < 0:
        raise ValueError("views must be non-negative")
    if likes < 0:
        raise ValueError("likes must be non-negative")
    if replies < 0:
        raise ValueError("replies must be non-negative")
    if shares < 0:
        raise ValueError("shares must be non-negative")
    if reply_depth_avg < 0:
        raise ValueError("reply_depth_avg must be non-negative")
    if engagement_variance is not None and engagement_variance < 0:
        raise ValueError("engagement_variance must be non-negative or None")

    # Ensure timezone-aware datetimes
    if published_at.tzinfo is None:
        raise ValueError("published_at must be timezone-aware")

    current_time = now or datetime.now(timezone.utc)
    if current_time.tzinfo is None:
        raise ValueError("now must be timezone-aware if provided")

    if published_at > current_time:
        raise ValueError("published_at cannot be in the future")

    # Create metrics
    metrics = ContentMetrics(
        views=views,
        likes=likes,
        replies=replies,
        shares=shares,
        reply_depth_avg=reply_depth_avg,
        published_at=published_at,
        engagement_variance=engagement_variance,
    )

    # Calculate component scores
    engagement_rate = _calculate_engagement_rate_score(
        views=views,
        likes=likes,
        replies=replies,
        shares=shares,
    )

    reply_depth_score = _calculate_reply_depth_score(reply_depth_avg)

    freshness_score = _calculate_freshness_score(
        published_at=published_at,
        now=current_time,
    )

    consistency_score = _calculate_consistency_score(engagement_variance)

    # Weighted composite score
    composite = (
        engagement_rate * WEIGHT_ENGAGEMENT_RATE
        + reply_depth_score * WEIGHT_REPLY_DEPTH
        + freshness_score * WEIGHT_FRESHNESS
        + consistency_score * WEIGHT_CONSISTENCY
    )

    # Scale to 0-100
    score = min(100.0, max(0.0, composite * 100.0))

    # Component breakdown
    component_scores = {
        "engagement_rate": round(engagement_rate * 100.0, 2),
        "reply_depth": round(reply_depth_score * 100.0, 2),
        "freshness": round(freshness_score * 100.0, 2),
        "consistency": round(consistency_score * 100.0, 2),
    }

    # Categorize tier
    tier = _categorize_quality_tier(score)

    # Generate insights
    insights = _generate_insights(
        score=score,
        tier=tier,
        metrics=metrics,
        component_scores=component_scores,
        current_time=current_time,
    )

    return ContentQualityScore(
        score=round(score, 2),
        tier=tier,
        metrics=metrics,
        component_scores=component_scores,
        insights=insights,
    )


def _calculate_engagement_rate_score(
    views: int,
    likes: int,
    replies: int,
    shares: int,
) -> float:
    """Calculate engagement rate score (0-1 normalized).

    Args:
        views: Number of views
        likes: Number of likes
        replies: Number of replies
        shares: Number of shares

    Returns:
        Normalized engagement rate score (0-1)
    """
    if views == 0:
        return 0.0

    # Total engagement actions
    total_engagement = likes + (replies * 2) + (shares * 3)  # Weight replies and shares higher

    # Engagement rate
    rate = total_engagement / views

    # Normalize against max
    return min(1.0, rate / MAX_ENGAGEMENT_RATE)


def _calculate_reply_depth_score(reply_depth_avg: float) -> float:
    """Calculate reply depth score (0-1 normalized).

    Args:
        reply_depth_avg: Average depth of reply chains

    Returns:
        Normalized reply depth score (0-1)
    """
    if reply_depth_avg <= 0:
        return 0.0

    return min(1.0, reply_depth_avg / MAX_REPLY_DEPTH)


def _calculate_freshness_score(
    published_at: datetime,
    now: datetime,
) -> float:
    """Calculate freshness score (0-1 normalized).

    Uses exponential decay over time.

    Args:
        published_at: Publication timestamp
        now: Current timestamp

    Returns:
        Normalized freshness score (0-1)
    """
    # Age in days
    age_days = (now - published_at).total_seconds() / 86400.0

    if age_days <= 0:
        return 1.0

    # Exponential decay: score = e^(-age/decay_constant)
    import math
    decay_rate = age_days / FRESHNESS_DECAY_DAYS
    return math.exp(-decay_rate)


def _calculate_consistency_score(engagement_variance: Optional[float]) -> float:
    """Calculate consistency score (0-1 normalized).

    Lower variance is better (more consistent engagement).

    Args:
        engagement_variance: Variance in engagement, or None if unavailable

    Returns:
        Normalized consistency score (0-1)
    """
    if engagement_variance is None:
        # No variance data - assume neutral consistency
        return 0.5

    if engagement_variance == 0:
        # Perfect consistency
        return 1.0

    # Lower variance is better - invert the relationship
    # Use 1 / (1 + variance) to normalize
    return 1.0 / (1.0 + engagement_variance)


def _categorize_quality_tier(score: float) -> str:
    """Categorize quality score into tier.

    Args:
        score: Quality score (0-100)

    Returns:
        Tier name
    """
    if score < THRESHOLD_BELOW_AVERAGE:
        return TIER_POOR
    elif score < THRESHOLD_AVERAGE:
        return TIER_BELOW_AVERAGE
    elif score < THRESHOLD_GOOD:
        return TIER_AVERAGE
    elif score < THRESHOLD_EXCELLENT:
        return TIER_GOOD
    else:
        return TIER_EXCELLENT


def _generate_insights(
    score: float,
    tier: str,
    metrics: ContentMetrics,
    component_scores: dict[str, float],
    current_time: datetime,
) -> list[str]:
    """Generate actionable insights for content quality.

    Args:
        score: Overall quality score
        tier: Quality tier
        metrics: Raw content metrics
        component_scores: Component score breakdown
        current_time: Current timestamp

    Returns:
        List of actionable insights
    """
    insights = []

    # Tier-based insights
    if tier == TIER_EXCELLENT:
        insights.append(f"Excellent content quality ({score:.1f}/100) - strong publication effectiveness")
    elif tier == TIER_GOOD:
        insights.append(f"Good content quality ({score:.1f}/100) - above average performance")
    elif tier == TIER_AVERAGE:
        insights.append(f"Average content quality ({score:.1f}/100) - standard performance")
    elif tier == TIER_BELOW_AVERAGE:
        insights.append(f"Below average quality ({score:.1f}/100) - room for improvement")
    else:  # TIER_POOR
        insights.append(f"Poor content quality ({score:.1f}/100) - significant improvement needed")

    # Engagement rate insights
    if component_scores["engagement_rate"] < 25:
        if metrics.views > 0:
            actual_rate = (metrics.likes + metrics.replies + metrics.shares) / metrics.views
            insights.append(
                f"Low engagement rate ({actual_rate:.2%}) - content may not resonate with audience"
            )
        else:
            insights.append("No views - content not reaching audience")
    elif component_scores["engagement_rate"] > 75:
        insights.append("High engagement rate - strong audience resonance")

    # Reply depth insights
    if component_scores["reply_depth"] < 20 and metrics.replies > 0:
        insights.append(
            f"Shallow reply depth ({metrics.reply_depth_avg:.1f}) - "
            "conversations not developing significantly"
        )
    elif component_scores["reply_depth"] > 60:
        insights.append(
            f"Good reply depth ({metrics.reply_depth_avg:.1f}) - "
            "generating meaningful conversations"
        )

    # Freshness insights
    age_days = (current_time - metrics.published_at).total_seconds() / 86400.0
    if component_scores["freshness"] < 30:
        insights.append(
            f"Content is aging ({age_days:.0f} days old) - "
            "engagement may be declining due to recency"
        )
    elif age_days < 1:
        insights.append("Very fresh content - still in active engagement window")

    # Consistency insights
    if metrics.engagement_variance is not None:
        if component_scores["consistency"] < 40:
            insights.append(
                "Inconsistent engagement pattern - high variance may indicate volatile content"
            )
        elif component_scores["consistency"] > 80:
            insights.append("Consistent engagement pattern - reliable content performance")

    # Zero engagement warnings
    if metrics.views > 100 and metrics.likes + metrics.replies + metrics.shares == 0:
        insights.append(
            "High views with zero engagement - critical content quality issue"
        )

    # High views, low engagement
    if metrics.views > 100:
        engagement_count = metrics.likes + metrics.replies + metrics.shares
        if engagement_count > 0 and engagement_count / metrics.views < 0.01:
            insights.append(
                "Views not converting to engagement - consider content relevance or CTAs"
            )

    return insights
