"""Engagement score computation from raw X API metrics."""

# Engagement score weights (absolute counts, no impression normalization)
WEIGHT_LIKE = 1.0
WEIGHT_RETWEET = 3.0
WEIGHT_REPLY = 4.0
WEIGHT_QUOTE = 5.0

# Newsletter score weights (Buttondown aggregate counts)
WEIGHT_NEWSLETTER_OPEN = 1.0
WEIGHT_NEWSLETTER_CLICK = 3.0

NEWSLETTER_STATUS_RESONATED = "resonated"
NEWSLETTER_STATUS_LOW_RESONANCE = "low_resonance"

MIN_NEWSLETTER_OPEN_RATE = 0.40
MIN_NEWSLETTER_CLICK_RATE = 0.04
MIN_NEWSLETTER_SCORE_PER_SUBSCRIBER = 0.50


def compute_engagement_score(
    like_count: int,
    retweet_count: int | None = None,
    reply_count: int = 0,
    quote_count: int = 0,
    *,
    repost_count: int | None = None,
) -> float:
    """Compute weighted engagement score from raw metric counts.

    Weights: likes=1, retweets/reposts=3, replies=4, quotes=5.
    """
    if retweet_count is None:
        retweet_count = repost_count or 0
    return (
        like_count * WEIGHT_LIKE
        + retweet_count * WEIGHT_RETWEET
        + reply_count * WEIGHT_REPLY
        + quote_count * WEIGHT_QUOTE
    )


def compute_newsletter_engagement_score(opens: int, clicks: int) -> float:
    """Compute weighted newsletter engagement score from Buttondown metrics."""
    return opens * WEIGHT_NEWSLETTER_OPEN + clicks * WEIGHT_NEWSLETTER_CLICK


def classify_newsletter_engagement(
    opens: int,
    clicks: int,
    subscriber_count: int,
) -> str:
    """Classify a newsletter send using stored Buttondown opens/clicks.

    A send resonates when it clears an open-rate, click-rate, or weighted
    score-per-subscriber threshold. With no subscriber denominator available,
    any non-zero engagement is treated as resonance.
    """
    if subscriber_count <= 0:
        return (
            NEWSLETTER_STATUS_RESONATED
            if opens > 0 or clicks > 0
            else NEWSLETTER_STATUS_LOW_RESONANCE
        )

    open_rate = opens / subscriber_count
    click_rate = clicks / subscriber_count
    score_per_subscriber = (
        compute_newsletter_engagement_score(opens, clicks) / subscriber_count
    )
    if (
        open_rate >= MIN_NEWSLETTER_OPEN_RATE
        or click_rate >= MIN_NEWSLETTER_CLICK_RATE
        or score_per_subscriber >= MIN_NEWSLETTER_SCORE_PER_SUBSCRIBER
    ):
        return NEWSLETTER_STATUS_RESONATED
    return NEWSLETTER_STATUS_LOW_RESONANCE
