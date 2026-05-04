"""Test helpers package for Presence test suite.

This package provides custom assertion helpers, mock utilities, and test utilities
to make tests more readable, maintainable, and consistent across the codebase.

## Categories

### Assertions (`tests.helpers.assertions`)
Domain-specific assertion helpers that encapsulate complex validation logic with
detailed error messages:

- `assert_valid_post()` - Validate post content for publication
- `assert_valid_thread()` - Validate thread structure and continuity
- `assert_valid_candidate()` - Validate candidate object fields and scores
- `assert_engagement_above_threshold()` - Validate engagement metrics
- `assert_dedup_detected()` - Validate deduplication detection
- `assert_evaluation_scores_valid()` - Validate model evaluation scores
- `assert_database_state()` - Validate database schema and data state
- `assert_no_data_leakage()` - Validate train/test split has no overlap
- `compose_assertions()` - Compose multiple assertions with failure collection

## Quick Start

Import helpers directly from the package:

```python
from tests.helpers import assert_valid_post, assert_evaluation_scores_valid

def test_generate_post():
    result = generate_post()
    assert_valid_post(result.content, char_limit=280)
    assert_evaluation_scores_valid(result.scores)
```

Or import specific modules:

```python
from tests.helpers.assertions import assert_valid_thread, assert_dedup_detected

def test_thread_generation():
    tweets = generate_thread()
    assert_valid_thread(tweets, min_tweets=3, max_tweets=10)
```

## Usage Examples

### Content Validation

```python
# Validate a single post
assert_valid_post(content, char_limit=280, banned_words=["spam"])

# Validate a thread
assert_valid_thread(tweets, min_tweets=2, check_continuity=True)
```

### Evaluation Validation

```python
# Validate candidate object
assert_valid_candidate(candidate, required_fields=["content", "score"])

# Validate model scores
assert_evaluation_scores_valid({"opus": 9.2, "sonnet": 8.5})
```

### Database Validation

```python
# Check database state
assert_database_state(
    db.conn,
    expected_tables=["posts", "users"],
    expected_row_counts={"posts": 10}
)
```

### Data Quality Validation

```python
# Check for data leakage
assert_no_data_leakage(train_data, test_data, identity_key="id")

# Check deduplication
assert_dedup_detected(content1, content2, similarity_threshold=0.8)
```

## Version

Version: 1.0.0
"""

# Assertions - Domain-specific validation helpers
from tests.helpers.assertions import (
    assert_valid_post,
    assert_valid_thread,
    assert_valid_candidate,
    assert_engagement_above_threshold,
    assert_dedup_detected,
    assert_evaluation_scores_valid,
    assert_database_state,
    assert_no_data_leakage,
    compose_assertions,
)

# Version information
__version__ = "1.0.0"

# Public API
__all__ = [
    # Assertions
    "assert_valid_post",
    "assert_valid_thread",
    "assert_valid_candidate",
    "assert_engagement_above_threshold",
    "assert_dedup_detected",
    "assert_evaluation_scores_valid",
    "assert_database_state",
    "assert_no_data_leakage",
    "compose_assertions",
    # Version
    "__version__",
]
