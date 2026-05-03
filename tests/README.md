# Test Suite Documentation

This directory contains the comprehensive test suite for the Presence content synthesis system.

## Custom Assertion Helpers

Custom domain-specific assertions are provided in `tests/helpers/assertions.py` to make tests more readable and maintainable. These assertions encapsulate complex validation logic and provide detailed, actionable error messages.

### Available Assertions

#### `assert_valid_post(content, char_limit=280, banned_words=None, require_proper_formatting=True)`

Validates that a post is ready for publication.

**Checks:**
- Content is non-empty
- Character limit compliance
- No banned words present
- Proper formatting (no TWEET markers, no trailing whitespace)

**Example:**

```python
from tests.helpers.assertions import assert_valid_post

# Basic validation
assert_valid_post("This is a great post about Python!")

# Custom character limit
assert_valid_post(post_content, char_limit=500)

# Check for banned words
assert_valid_post(post_content, banned_words=["spam", "clickbait"])

# Skip formatting checks if needed
assert_valid_post(raw_content, require_proper_formatting=False)
```

**Error Example:**

```
Post exceeds character limit.
Expected: ≤280 characters
Actual: 315 characters (over by 35)
Content preview: This is a very long post that goes on and on...
Debug hint: Consider splitting into a thread or using content shortening
```

#### `assert_valid_thread(tweets, min_tweets=2, max_tweets=10, char_limit_per_tweet=280, total_char_limit=2800, check_continuity=True)`

Validates that a thread structure is valid.

**Checks:**
- Thread length within bounds
- Each tweet within character limit
- Total thread length compliance
- Content continuity and no duplicates

**Example:**

```python
from tests.helpers.assertions import assert_valid_thread

tweets = [
    "First tweet introduces the topic",
    "Second tweet expands on the idea",
    "Third tweet provides conclusion"
]

# Basic validation
assert_valid_thread(tweets)

# Custom limits
assert_valid_thread(tweets, min_tweets=3, max_tweets=5, total_char_limit=1000)

# Skip continuity checks
assert_valid_thread(tweets, check_continuity=False)
```

**Error Example:**

```
Tweet 2 exceeds character limit.
Expected: ≤280 characters
Actual: 315 characters (over by 35)
Tweet content: This tweet is way too long and needs to be split...
Debug hint: Split long tweet or rephrase to fit limit
```

#### `assert_valid_candidate(candidate, required_fields=None, score_field='score', min_score=0.0, max_score=10.0)`

Validates that a candidate object has all required fields and valid scores.

**Checks:**
- Has all required fields
- No None values in required fields
- Score is numeric and within valid range

**Example:**

```python
from tests.helpers.assertions import assert_valid_candidate

candidate = {
    "content": "Generated post content",
    "score": 8.5,
    "model": "claude-opus-4"
}

# Basic validation (checks 'content' and 'score')
assert_valid_candidate(candidate)

# Custom required fields
assert_valid_candidate(
    candidate,
    required_fields=["content", "score", "model"]
)

# Custom score field and range
assert_valid_candidate(
    candidate,
    score_field="rating",
    min_score=0,
    max_score=100
)
```

**Error Example:**

```
Candidate missing required fields.
Expected: content, score, model
Actual: Missing model
Available fields: content, score
Debug hint: Ensure all required fields are populated during candidate creation
```

#### `assert_engagement_above_threshold(metrics, threshold, metric_name=None)`

Validates that engagement metrics meet minimum thresholds.

**Example:**

```python
from tests.helpers.assertions import assert_engagement_above_threshold

metrics = {
    "likes": 150,
    "retweets": 45,
    "replies": 20,
    "engagement_score": 87.5
}

# Check all metrics above threshold
assert_engagement_above_threshold(metrics, threshold=10.0)

# Check specific metric
assert_engagement_above_threshold(
    metrics,
    threshold=80.0,
    metric_name="engagement_score"
)
```

#### `assert_dedup_detected(content1, content2, method='sequence_matcher', similarity_threshold=0.8)`

Validates that deduplication correctly identifies similar content.

**Methods:**
- `exact`: Content must be identical
- `sequence_matcher`: Uses SequenceMatcher for similarity
- `embedding`: Placeholder for semantic similarity (not yet implemented)

**Example:**

```python
from tests.helpers.assertions import assert_dedup_detected

# Check exact duplicates
assert_dedup_detected(
    "Exact same content",
    "Exact same content",
    method="exact"
)

# Check similar content with threshold
assert_dedup_detected(
    "This is about Python programming",
    "This is about Python development",
    method="sequence_matcher",
    similarity_threshold=0.7
)
```

#### `assert_evaluation_scores_valid(scores, require_opus_higher=True, opus_key='opus', sonnet_key='sonnet', min_difference=0.5)`

Validates evaluation scores from different models.

**Checks:**
- All scores in valid range (0-10)
- Opus score higher than Sonnet by threshold (if required)

**Example:**

```python
from tests.helpers.assertions import assert_evaluation_scores_valid

scores = {
    "opus": 9.2,
    "sonnet": 8.5,
    "haiku": 7.8
}

# Basic validation (requires opus > sonnet + 0.5)
assert_evaluation_scores_valid(scores)

# Custom model keys and threshold
assert_evaluation_scores_valid(
    scores,
    opus_key="gpt4",
    sonnet_key="gpt35",
    min_difference=1.0
)

# Don't require Opus higher
assert_evaluation_scores_valid(scores, require_opus_higher=False)
```

**Error Example:**

```
Opus score not sufficiently higher than Sonnet.
Expected: Opus ≥ Sonnet + 0.5 (Opus ≥ 9.00)
Actual: Opus=8.70, Sonnet=8.50, diff=0.20
Debug hint: Opus should consistently score higher; check evaluation calibration
```

#### `assert_database_state(db_connection, expected_tables=None, expected_row_counts=None)`

Validates database schema and data state.

**Example:**

```python
from tests.helpers.assertions import assert_database_state

# Check tables exist
assert_database_state(
    db_conn,
    expected_tables=["users", "posts", "generated_content"]
)

# Check row counts
assert_database_state(
    db_conn,
    expected_row_counts={
        "users": 5,
        "posts": 120,
        "generated_content": 450
    }
)

# Check both
assert_database_state(
    db_conn,
    expected_tables=["users", "posts"],
    expected_row_counts={"users": 5}
)
```

**Error Example:**

```
Table 'posts' has unexpected row count.
Expected: 120 rows
Actual: 95 rows (difference: -25)
Debug hint: Check data insertion/deletion logic or adjust expected count
```

#### `assert_no_data_leakage(train_data, test_data, identity_key=None)`

Validates that train and test datasets are disjoint.

**Example:**

```python
from tests.helpers.assertions import assert_no_data_leakage

train = [1, 2, 3, 4, 5]
test = [6, 7, 8, 9, 10]

# Check simple lists
assert_no_data_leakage(train, test)

# Check dicts with identity key
train_items = [
    {"id": 1, "text": "A"},
    {"id": 2, "text": "B"}
]
test_items = [
    {"id": 3, "text": "C"},
    {"id": 4, "text": "D"}
]

assert_no_data_leakage(train_items, test_items, identity_key="id")
```

**Error Example:**

```
Data leakage detected between train and test sets.
Expected: Disjoint train and test sets
Actual: 3 items appear in both sets
Sample overlapping items: [42, 87, 156]
Train size: 1000, Test size: 200, Overlap: 3
Debug hint: Ensure proper train/test split with no shared items
```

#### `compose_assertions(*assertions)`

Composes multiple assertions into a single check, collecting all failures.

**Example:**

```python
from tests.helpers.assertions import (
    assert_valid_post,
    assert_evaluation_scores_valid,
    compose_assertions
)

def test_full_pipeline_output():
    result = run_synthesis_pipeline()

    # Run all validations and collect failures
    compose_assertions(
        lambda: assert_valid_post(result.content),
        lambda: assert_evaluation_scores_valid(result.scores),
        lambda: assert_engagement_above_threshold(
            result.metrics,
            threshold=50.0
        )
    )
```

**Error Example:**

```
Composed assertion failures (2/3 failed):

Assertion 1 failed: Post exceeds character limit.
Expected: ≤280 characters
Actual: 315 characters (over by 35)
...

Assertion 3 failed: Metric 'engagement_score' below threshold.
Expected: ≥50.0
Actual: 42.5 (shortfall: 7.50)
...

Debug hint: Fix each failing assertion in sequence
```

## Usage Patterns

### Before and After Comparison

**Before (without custom assertions):**

```python
def test_pipeline_output():
    result = pipeline.run()

    # Verbose, repetitive validation
    assert result is not None, "Pipeline returned None"
    assert len(result.content) > 0, "Content is empty"
    assert len(result.content) <= 280, f"Content too long: {len(result.content)} chars"

    # Hard to debug when it fails
    assert result.score >= 0 and result.score <= 10, f"Invalid score: {result.score}"
    assert result.score > result.baseline_score + 0.5, "Score not improved enough"
```

**After (with custom assertions):**

```python
def test_pipeline_output():
    result = pipeline.run()

    # Clear, readable, detailed error messages
    assert_valid_post(result.content)
    assert_valid_candidate(result)
    assert_evaluation_scores_valid(
        {"opus": result.score, "sonnet": result.baseline_score}
    )
```

### Testing Edge Cases

```python
def test_thread_edge_cases():
    # Empty thread
    with pytest.raises(AssertionError, match="Thread is empty"):
        assert_valid_thread([])

    # Single tweet (below minimum)
    with pytest.raises(AssertionError, match="Thread too short"):
        assert_valid_thread(["Only one tweet"])

    # Duplicate tweets
    with pytest.raises(AssertionError, match="nearly identical"):
        assert_valid_thread(["Same", "Same"])
```

### Combining Multiple Validations

```python
def test_complete_synthesis_output():
    batch = run_synthesis_batch()

    compose_assertions(
        lambda: assert_valid_post(batch.final_content),
        lambda: assert_evaluation_scores_valid(batch.scores),
        lambda: assert_database_state(
            db,
            expected_row_counts={"generated_content": 1}
        )
    )
```

## Benefits

1. **Improved Readability**: Test intent is clear from assertion name
2. **Better Error Messages**: Detailed context with expected vs actual and debugging hints
3. **Reduced Duplication**: Common validation logic centralized
4. **Easier Maintenance**: Update validation logic in one place
5. **Consistent Standards**: Same validation criteria across all tests

## Running Tests

```bash
# Run all assertion helper tests
pytest tests/test_assertion_helpers.py -v

# Run specific test class
pytest tests/test_assertion_helpers.py::TestAssertValidPost -v

# Run with coverage
pytest tests/test_assertion_helpers.py --cov=tests.helpers.assertions --cov-report=term-missing
```

## Adding New Custom Assertions

When adding new domain-specific assertions:

1. **Add to `tests/helpers/assertions.py`**:
   - Include detailed docstring with parameters and examples
   - Provide comprehensive error messages with Expected/Actual/Debug hint format
   - Handle edge cases gracefully

2. **Export from `tests/helpers/__init__.py`**:
   - Add to imports and `__all__` list

3. **Add comprehensive tests in `tests/test_assertion_helpers.py`**:
   - Test passing cases
   - Test failure cases with error message validation
   - Test edge cases and boundary conditions
   - Test pytest integration

4. **Document in this README**:
   - Add to Available Assertions section
   - Include clear examples
   - Show error message examples

## See Also

- [pytest documentation](https://docs.pytest.org/)
- [Python unittest assertions](https://docs.python.org/3/library/unittest.html#assert-methods)
