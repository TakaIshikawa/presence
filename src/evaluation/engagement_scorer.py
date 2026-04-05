"""Engagement score computation from raw X API metrics."""

# Engagement score weights (absolute counts, no impression normalization)
WEIGHT_LIKE = 1.0
WEIGHT_RETWEET = 3.0
WEIGHT_REPLY = 4.0
WEIGHT_QUOTE = 5.0


def compute_engagement_score(
    like_count: int,
    retweet_count: int,
    reply_count: int,
    quote_count: int,
) -> float:
    """Compute weighted engagement score from raw metric counts.

    Weights: likes=1, retweets=3, replies=4, quotes=5.
    """
    return (
        like_count * WEIGHT_LIKE
        + retweet_count * WEIGHT_RETWEET
        + reply_count * WEIGHT_REPLY
        + quote_count * WEIGHT_QUOTE
    )
