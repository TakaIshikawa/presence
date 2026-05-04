# Test Suite Documentation

This directory contains the comprehensive test suite for the Presence content synthesis system.

## Table of Contents

1. [Test Organization](#test-organization)
2. [Fixture Library](#fixture-library)
3. [Custom Assertion Helpers](#custom-assertion-helpers)
4. [Mock Helpers](#mock-helpers)
5. [Parametrized Testing](#parametrized-testing)
6. [Test Performance Guidelines](#test-performance-guidelines)
7. [TDD Workflow](#tdd-workflow)
8. [Test Debugging Tips](#test-debugging-tips)
9. [Test Coverage Standards](#test-coverage-standards)
10. [Quick Reference Guide](#quick-reference-guide)

## Test Organization

Tests are organized by module and functionality to maintain clarity and enable targeted test execution.

### Test Structure

```
tests/
├── conftest.py              # Shared fixtures and test configuration
├── helpers/                 # Custom test utilities
│   ├── assertions.py       # Domain-specific assertions
│   └── __init__.py
├── fixtures/               # Test data files (XML, HTML, etc.)
├── test_*.py              # Test modules (one per source module)
└── README.md              # This file
```

### Test Categories

While not strictly separated by directory, tests follow these categories:

**Unit Tests** - Test individual functions/classes in isolation
- Fast execution (<100ms per test)
- No external dependencies (database, network, filesystem)
- Use mocks for all dependencies
- Example: `test_engagement_scoring.py`

**Integration Tests** - Test interactions between components
- May use in-memory databases or temporary files
- Test realistic data flows
- Example: `test_synthesis_pipeline.py`

**Smoke Tests** - Quick validation that core functionality works
- Run before full test suite
- Cover critical paths only
- Example: Basic database connection, config loading

### Running Specific Test Categories

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_engagement_scoring.py

# Run tests matching pattern
pytest -k "engagement"

# Run with verbose output
pytest -v

# Run with coverage
pytest --cov=src --cov-report=term-missing
```

## Fixture Library

Fixtures provide reusable test data and setup/teardown logic. All fixtures are defined in `conftest.py` and available to all tests.

### Database Fixtures

#### `db` - In-memory SQLite database

Provides a clean database with schema applied for each test.

```python
def test_insert_message(db):
    """Test inserting a message into the database."""
    db.conn.execute(
        "INSERT INTO claude_messages (session_id, message_uuid, timestamp, prompt_text) VALUES (?, ?, ?, ?)",
        ("sess-001", "uuid-aaa", "2026-03-20T10:00:00+00:00", "test prompt")
    )
    db.conn.commit()

    cursor = db.conn.execute("SELECT COUNT(*) FROM claude_messages")
    count = cursor.fetchone()[0]
    assert count == 1
```

**Characteristics:**
- Clean database for each test (isolated)
- Full schema from `schema.sql` applied
- Automatically cleaned up after test
- Fast (in-memory)

#### `file_db` - File-backed SQLite database

Provides a temporary file-based database when you need persistence across operations.

```python
def test_database_persistence(file_db, tmp_path):
    """Test that data persists in file-backed database."""
    # Insert data
    file_db.conn.execute(
        "INSERT INTO github_commits (repo_name, commit_sha, timestamp) VALUES (?, ?, ?)",
        ("acme/widget", "abc123", "2026-03-20T11:00:00+00:00")
    )
    file_db.conn.commit()

    # Close and reconnect
    db_path = file_db.db_path
    file_db.close()

    # Data should still be there
    import sqlite3
    conn = sqlite3.connect(db_path)
    cursor = conn.execute("SELECT COUNT(*) FROM github_commits")
    assert cursor.fetchone()[0] == 1
```

### Sample Data Fixtures

#### `sample_message` - Minimal claude_messages row

```python
def test_message_processing(db, sample_message):
    """Test processing a Claude message."""
    db.conn.execute(
        """INSERT INTO claude_messages
           (session_id, message_uuid, project_path, timestamp, prompt_text)
           VALUES (?, ?, ?, ?, ?)""",
        (
            sample_message["session_id"],
            sample_message["message_uuid"],
            sample_message["project_path"],
            sample_message["timestamp"],
            sample_message["prompt_text"]
        )
    )
    db.conn.commit()

    # Verify insertion
    cursor = db.conn.execute(
        "SELECT prompt_text FROM claude_messages WHERE message_uuid = ?",
        (sample_message["message_uuid"],)
    )
    assert cursor.fetchone()[0] == "Explain the auth module"
```

#### `sample_commit` - Minimal github_commits row

```python
def test_commit_tracking(db, sample_commit):
    """Test tracking a GitHub commit."""
    db.conn.execute(
        """INSERT INTO github_commits
           (repo_name, commit_sha, commit_message, timestamp, author)
           VALUES (?, ?, ?, ?, ?)""",
        (
            sample_commit["repo_name"],
            sample_commit["commit_sha"],
            sample_commit["commit_message"],
            sample_commit["timestamp"],
            sample_commit["author"]
        )
    )
    db.conn.commit()

    cursor = db.conn.execute(
        "SELECT commit_message FROM github_commits WHERE commit_sha = ?",
        (sample_commit["commit_sha"],)
    )
    assert cursor.fetchone()[0] == "fix: resolve race condition"
```

#### `sample_content` - Minimal generated_content row

```python
def test_content_generation(db, sample_content):
    """Test storing generated content."""
    db.conn.execute(
        """INSERT INTO generated_content
           (content_type, content, eval_score, eval_feedback)
           VALUES (?, ?, ?, ?)""",
        (
            sample_content["content_type"],
            sample_content["content"],
            sample_content["eval_score"],
            sample_content["eval_feedback"]
        )
    )
    db.conn.commit()

    cursor = db.conn.execute(
        "SELECT eval_score FROM generated_content WHERE content_type = ?",
        (sample_content["content_type"],)
    )
    assert cursor.fetchone()[0] == 7.5
```

### Creating Custom Fixtures

Add fixtures to `conftest.py`:

```python
@pytest.fixture
def mock_llm_response():
    """Mock LLM API response for testing."""
    return {
        "content": "Generated post about Python testing best practices.",
        "model": "claude-opus-4",
        "usage": {"input_tokens": 150, "output_tokens": 45}
    }

@pytest.fixture
def temp_config_file(tmp_path):
    """Create a temporary config file."""
    config_path = tmp_path / "config.json"
    config_path.write_text('{"api_key": "test-key", "model": "claude-opus-4"}')
    return config_path
```

## Mock Helpers

Mock helpers simulate external dependencies for isolated testing.

### MockDB - In-memory database mock

Used in tests that need database functionality without a full database setup.

```python
class MockDB:
    """Mock database for testing."""

    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self._setup_schema()

    def _setup_schema(self):
        """Create necessary tables."""
        self.conn.execute(
            """CREATE TABLE predictions (
                id INTEGER PRIMARY KEY,
                predicted_score REAL,
                actual_score REAL
            )"""
        )
        self.conn.commit()

def test_with_mock_db():
    """Test using mock database."""
    db = MockDB()
    db.conn.execute(
        "INSERT INTO predictions (predicted_score, actual_score) VALUES (?, ?)",
        (8.5, 9.0)
    )
    db.conn.commit()

    cursor = db.conn.execute("SELECT predicted_score FROM predictions")
    assert cursor.fetchone()[0] == 8.5
```

### Mocking External APIs

Use `unittest.mock` to simulate API calls without network requests.

#### Mocking HTTP requests

```python
from unittest.mock import Mock, patch

def test_fetch_article_metadata():
    """Test fetching article metadata with mocked HTTP."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = '<html><head><title>Test Article</title></head></html>'

    with patch('requests.get', return_value=mock_response):
        from ingestion.article_metadata import fetch_metadata

        metadata = fetch_metadata('https://example.com/article')
        assert metadata['title'] == 'Test Article'
```

#### Mocking LLM API responses

```python
def test_llm_content_generation():
    """Test content generation with mocked LLM."""
    mock_client = Mock()
    mock_client.messages.create.return_value = Mock(
        content=[Mock(text="Generated post about AI safety.")],
        usage=Mock(input_tokens=100, output_tokens=20)
    )

    with patch('anthropic.Anthropic', return_value=mock_client):
        from synthesis.generate import generate_post

        result = generate_post(prompt="Write about AI safety")
        assert "AI safety" in result["content"]
        assert result["usage"]["output_tokens"] == 20
```

#### Mocking social media APIs (X/Twitter)

```python
def test_post_to_x():
    """Test posting to X with mocked API."""
    mock_client = Mock()
    mock_client.create_tweet.return_value = Mock(
        data={'id': '123456789', 'text': 'Test tweet'}
    )

    with patch('tweepy.Client', return_value=mock_client):
        from publishing.x_publisher import post_tweet

        result = post_tweet("Test tweet")
        assert result['id'] == '123456789'
        mock_client.create_tweet.assert_called_once()
```

#### Mocking file system operations

```python
def test_config_loading():
    """Test config loading with mocked file system."""
    from unittest.mock import mock_open

    mock_config = '{"api_key": "test-key", "model": "claude-opus-4"}'

    with patch('builtins.open', mock_open(read_data=mock_config)):
        from config.loader import load_config

        config = load_config('config.json')
        assert config['api_key'] == 'test-key'
        assert config['model'] == 'claude-opus-4'
```

### Mock Configuration Best Practices

1. **Mock at the boundary**: Mock external calls (HTTP, filesystem), not internal logic
2. **Use realistic data**: Mock responses should match actual API responses
3. **Verify calls**: Use `assert_called_with()` to ensure mocks are called correctly
4. **Isolate tests**: Each test should set up its own mocks
5. **Clean up**: Use context managers (`with patch()`) for automatic cleanup

## Parametrized Testing

Parametrized tests run the same test logic with different inputs, reducing code duplication.

### Basic Parametrization

```python
import pytest

@pytest.mark.parametrize("input,expected", [
    ("Hello world", 11),
    ("Python", 6),
    ("", 0),
    ("Test with spaces", 16),
])
def test_character_count(input, expected):
    """Test character counting with various inputs."""
    assert len(input) == expected
```

### Multiple Parameters

```python
@pytest.mark.parametrize("content,char_limit,should_pass", [
    ("Short tweet", 280, True),
    ("x" * 280, 280, True),
    ("x" * 281, 280, False),
    ("Medium post", 500, True),
])
def test_content_length_validation(content, char_limit, should_pass):
    """Test content length validation."""
    from tests.helpers.assertions import assert_valid_post

    if should_pass:
        assert_valid_post(content, char_limit=char_limit)
    else:
        with pytest.raises(AssertionError):
            assert_valid_post(content, char_limit=char_limit)
```

### Parametrizing with IDs

Use `ids` parameter for readable test output:

```python
@pytest.mark.parametrize("score,label", [
    (9.5, "excellent"),
    (7.5, "good"),
    (5.0, "average"),
    (2.5, "poor"),
], ids=["excellent-score", "good-score", "average-score", "poor-score"])
def test_score_labeling(score, label):
    """Test score to label conversion."""
    from evaluation.scoring import score_to_label
    assert score_to_label(score) == label
```

### Parametrizing Fixtures

```python
@pytest.fixture(params=[
    {"model": "claude-opus-4", "max_tokens": 1000},
    {"model": "claude-sonnet-4", "max_tokens": 500},
    {"model": "claude-haiku-4", "max_tokens": 250},
])
def llm_config(request):
    """Parametrized LLM configuration fixture."""
    return request.param

def test_generation_with_different_models(llm_config):
    """Test generation works with different model configs."""
    # This test runs 3 times with different configs
    assert llm_config["model"].startswith("claude-")
    assert llm_config["max_tokens"] > 0
```

### External Test Data (YAML/CSV)

For complex test cases, use external data files:

```python
import csv
from pathlib import Path

def load_test_cases(filename):
    """Load test cases from CSV file."""
    test_data_path = Path(__file__).parent / "fixtures" / filename
    with open(test_data_path) as f:
        reader = csv.DictReader(f)
        return list(reader)

# Load test cases
engagement_cases = load_test_cases("engagement_test_cases.csv")

@pytest.mark.parametrize("test_case", engagement_cases)
def test_engagement_scoring(test_case):
    """Test engagement scoring with CSV data."""
    from evaluation.engagement import calculate_engagement

    score = calculate_engagement(
        likes=int(test_case["likes"]),
        retweets=int(test_case["retweets"]),
        replies=int(test_case["replies"])
    )
    expected = float(test_case["expected_score"])
    assert abs(score - expected) < 0.1  # Allow small floating point differences
```

**Example CSV file** (`tests/fixtures/engagement_test_cases.csv`):
```csv
likes,retweets,replies,expected_score
100,50,20,8.5
10,5,2,5.0
1000,500,100,9.8
0,0,0,0.0
```

## Test Performance Guidelines

Fast tests enable rapid development cycles and encourage running tests frequently.

### Target Performance

- **Unit tests**: <100ms per test
- **Integration tests**: <500ms per test
- **Full test suite**: <30 seconds

### Performance Best Practices

1. **Use in-memory databases**: Prefer `db` fixture over file-backed databases
2. **Mock external services**: Never make real HTTP requests in tests
3. **Minimize file I/O**: Use `tmp_path` for temporary files
4. **Avoid sleep()**: Use mocks instead of waiting for timeouts
5. **Lazy imports**: Import modules inside test functions if they're slow to import

### Measuring Test Performance

```bash
# Show slowest tests
pytest --durations=10

# Show all test durations
pytest --durations=0

# Profile a specific test
pytest tests/test_synthesis.py --profile
```

### Optimizing Slow Tests

**Before (slow):**
```python
def test_generate_multiple_candidates():
    """Generate 100 candidates - SLOW."""
    candidates = []
    for i in range(100):
        candidate = generate_candidate(f"prompt {i}")
        candidates.append(candidate)
    assert len(candidates) == 100
```

**After (fast):**
```python
def test_generate_multiple_candidates():
    """Generate 100 candidates - FAST."""
    mock_generate = Mock(return_value={"content": "test", "score": 8.0})

    with patch('synthesis.generate.generate_candidate', mock_generate):
        candidates = [generate_candidate(f"prompt {i}") for i in range(100)]

    assert len(candidates) == 100
    assert mock_generate.call_count == 100
```

### Skipping Slow Tests

Mark slow tests for conditional execution:

```python
@pytest.mark.slow
def test_full_pipeline_with_real_llm():
    """Integration test with real LLM - marked as slow."""
    # This test makes real API calls
    pass

# Run only fast tests
# pytest -m "not slow"

# Run all tests including slow ones
# pytest
```

## TDD Workflow

The project includes TDD workflow automation via `scripts/tdd_workflow.py`.

### TDD Commands

#### Initialize TDD session

```bash
python scripts/tdd_workflow.py init feature_name
```

Creates:
- Failing test stub in `tests/test_feature_name.py`
- TDD state file `.tdd_state.json`
- Sets phase to "red"

#### Check TDD status

```bash
python scripts/tdd_workflow.py status
```

Shows:
- Current feature being developed
- Current phase (red/green/refactor)
- Test file path
- Cycle count

#### Run TDD cycle

```bash
python scripts/tdd_workflow.py cycle
```

Runs tests and suggests next step:
- Red → Write minimal implementation
- Green → Refactor if needed, or complete
- Refactor → Run tests again

#### Complete TDD session

```bash
python scripts/tdd_workflow.py complete
```

Validates:
- Tests are passing
- Test was written first (committed before implementation)
- Coverage increased
- Generates TDD metrics

### TDD Workflow Example

```bash
# 1. Start TDD session
python scripts/tdd_workflow.py init user_authentication

# 2. Write failing test (auto-generated stub)
# Edit tests/test_user_authentication.py

# 3. Commit test first
git add tests/test_user_authentication.py
git commit -m "Add failing test for user authentication"

# 4. Run cycle - shows "red" phase
python scripts/tdd_workflow.py cycle

# 5. Write minimal implementation
# Edit src/auth/user_authentication.py

# 6. Run cycle - shows "green" phase
python scripts/tdd_workflow.py cycle

# 7. Refactor if needed
# Edit src/auth/user_authentication.py

# 8. Complete session
python scripts/tdd_workflow.py complete
```

### TDD Metrics

The workflow tracks:
- **Test-first percentage**: % of features where test was committed first
- **Avg red-green cycle time**: Time from failing to passing test
- **Coverage increase**: Coverage gain per commit
- **TDD compliance**: % of developers following TDD

View metrics:
```bash
python scripts/tdd_workflow.py metrics
```

## Test Debugging Tips

### Common Issues and Solutions

#### Import Errors

**Problem**: `ModuleNotFoundError: No module named 'synthesis'`

**Solution**: Ensure `src/` is in Python path (configured in `conftest.py` and `pyproject.toml`)

```python
# conftest.py already adds src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
```

#### Fixture Not Found

**Problem**: `fixture 'db' not found`

**Solution**: Ensure fixture is defined in `conftest.py` or imported properly

```python
# Fixtures in conftest.py are auto-discovered
# No import needed in test files
def test_something(db):  # ✓ Works automatically
    pass
```

#### Database Schema Errors

**Problem**: `sqlite3.OperationalError: no such table: claude_messages`

**Solution**: Ensure `db` or `file_db` fixture is used, which applies schema automatically

```python
def test_insert_message(db):  # ✓ Schema applied
    db.conn.execute("INSERT INTO claude_messages ...")
```

#### Flaky Tests

**Problem**: Test passes sometimes, fails other times

**Common causes**:
1. **Datetime/timestamp issues**: Use fixed timestamps in tests
2. **Dictionary/set ordering**: Don't rely on iteration order
3. **Floating point comparisons**: Use `pytest.approx()` or tolerance
4. **External dependencies**: Mock all external calls

**Solution**:
```python
# Bad - uses current time (flaky)
def test_recent_messages():
    cutoff = datetime.now() - timedelta(days=7)
    # ...

# Good - uses fixed time
def test_recent_messages():
    fixed_now = datetime(2026, 3, 20, 10, 0, 0)
    cutoff = fixed_now - timedelta(days=7)
    # ...

# Bad - floating point comparison
assert score == 7.33333333

# Good - approximate comparison
assert score == pytest.approx(7.333, rel=1e-3)
```

#### Mock Not Working

**Problem**: Mock is set up but real function still called

**Solution**: Patch at the point of use, not definition

```python
# Bad - patches where function is defined
with patch('synthesis.generate.call_llm'):
    from module import function_that_uses_call_llm
    function_that_uses_call_llm()  # Still calls real function

# Good - patches where function is used
with patch('module.call_llm'):  # Patch in module that imports it
    from module import function_that_uses_call_llm
    function_that_uses_call_llm()  # Calls mock
```

### Debugging Test Failures

#### Use verbose output

```bash
pytest -vv tests/test_module.py
```

#### Show local variables on failure

```bash
pytest -l tests/test_module.py
```

#### Drop into debugger on failure

```bash
pytest --pdb tests/test_module.py
```

#### Run specific test

```bash
# Run single test function
pytest tests/test_module.py::test_specific_function

# Run test class
pytest tests/test_module.py::TestClassName

# Run with pattern matching
pytest -k "authentication"
```

#### Print debugging

```python
def test_something():
    result = complex_function()
    print(f"DEBUG: result = {result}")  # Shows in pytest output with -s
    assert result == expected
```

Run with output:
```bash
pytest -s tests/test_module.py
```

## Test Coverage Standards

### Coverage Targets

- **Overall coverage**: >80%
- **Critical modules**: >90%
  - `synthesis/` (content generation)
  - `evaluation/` (scoring and quality)
  - `publishing/` (post publishing)
  - `storage/` (database operations)
- **Acceptable lower coverage**: >60%
  - `scripts/` (CLI tools)
  - `ingestion/` (external data fetching)

### Measuring Coverage

```bash
# Run tests with coverage
pytest --cov=src --cov-report=term-missing

# Generate HTML report
pytest --cov=src --cov-report=html
open htmlcov/index.html

# Show coverage for specific module
pytest --cov=src/synthesis --cov-report=term-missing

# Fail if coverage below threshold
pytest --cov=src --cov-fail-under=80
```

### Coverage Report Example

```
Name                                 Stmts   Miss  Cover   Missing
------------------------------------------------------------------
src/synthesis/generate.py               45      3    93%   67-69
src/synthesis/evaluate.py               38      0   100%
src/synthesis/refine.py                 52      8    85%   23, 45-51
src/publishing/x_publisher.py           29      6    79%   34-39
------------------------------------------------------------------
TOTAL                                  164     17    90%
```

### What to Test

**Always test**:
- Core business logic
- Data transformations
- Error handling
- Edge cases and boundary conditions
- Integration points between modules

**Optional to test**:
- Simple getters/setters
- Configuration loading (if trivial)
- Very thin wrapper functions

**Don't test**:
- Third-party libraries
- Framework code
- Generated code

### Example: Achieving High Coverage

```python
def calculate_engagement(likes: int, retweets: int, replies: int) -> float:
    """Calculate engagement score from metrics."""
    if likes < 0 or retweets < 0 or replies < 0:
        raise ValueError("Metrics cannot be negative")

    if likes == 0 and retweets == 0 and replies == 0:
        return 0.0

    # Weighted formula
    score = (likes * 0.5) + (retweets * 2.0) + (replies * 1.5)
    return min(score / 100.0, 10.0)  # Normalize to 0-10

# Comprehensive tests for 100% coverage
def test_calculate_engagement_basic():
    """Test basic engagement calculation."""
    score = calculate_engagement(likes=100, retweets=50, replies=20)
    assert score == pytest.approx(1.8)  # (100*0.5 + 50*2.0 + 20*1.5) / 100

def test_calculate_engagement_zero():
    """Test zero engagement."""
    assert calculate_engagement(0, 0, 0) == 0.0

def test_calculate_engagement_negative_raises():
    """Test negative metrics raise error."""
    with pytest.raises(ValueError, match="cannot be negative"):
        calculate_engagement(-1, 0, 0)

def test_calculate_engagement_max_score():
    """Test score caps at 10.0."""
    score = calculate_engagement(likes=10000, retweets=5000, replies=1000)
    assert score == 10.0  # Capped at maximum

def test_calculate_engagement_each_metric():
    """Test each metric contributes correctly."""
    likes_only = calculate_engagement(100, 0, 0)
    retweets_only = calculate_engagement(0, 100, 0)
    replies_only = calculate_engagement(0, 0, 100)

    assert likes_only == 0.5  # 100 * 0.5 / 100
    assert retweets_only == 2.0  # 100 * 2.0 / 100
    assert replies_only == 1.5  # 100 * 1.5 / 100
```

## Quick Reference Guide

### Common Testing Tasks

#### Write a new test using fixtures and assertions

```python
from tests.helpers.assertions import assert_valid_post, assert_evaluation_scores_valid

def test_generate_post(db, sample_message):
    """Test generating a post from a message."""
    # Use database fixture
    db.conn.execute(
        "INSERT INTO claude_messages (session_id, message_uuid, prompt_text, timestamp) VALUES (?, ?, ?, ?)",
        (sample_message["session_id"], sample_message["message_uuid"],
         sample_message["prompt_text"], sample_message["timestamp"])
    )
    db.conn.commit()

    # Run function under test
    from synthesis.generate import generate_from_message
    result = generate_from_message(db, sample_message["message_uuid"])

    # Use custom assertions
    assert_valid_post(result["content"], char_limit=280)
    assert_evaluation_scores_valid(result["scores"])
```

#### Add parametrized test cases

```python
@pytest.mark.parametrize("char_limit,content,should_pass", [
    (280, "Short post", True),
    (280, "x" * 280, True),
    (280, "x" * 281, False),
], ids=["short", "exactly-at-limit", "over-limit"])
def test_post_length(char_limit, content, should_pass):
    """Test post length validation."""
    from tests.helpers.assertions import assert_valid_post

    if should_pass:
        assert_valid_post(content, char_limit=char_limit)
    else:
        with pytest.raises(AssertionError):
            assert_valid_post(content, char_limit=char_limit)
```

#### Mock external API

```python
from unittest.mock import Mock, patch

def test_fetch_with_mocked_api():
    """Test fetching data with mocked API."""
    mock_response = Mock()
    mock_response.json.return_value = {"title": "Test Article"}
    mock_response.status_code = 200

    with patch('requests.get', return_value=mock_response):
        from ingestion.fetch import fetch_article
        result = fetch_article("https://example.com")

    assert result["title"] == "Test Article"
```

#### Profile slow tests

```bash
# Find slowest tests
pytest --durations=10

# Profile specific test
pytest tests/test_slow_module.py::test_specific --profile

# Run without slow tests
pytest -m "not slow"
```

### Test Execution Patterns

```bash
# Development: Run fast tests only
pytest -m "not slow" --ff

# Pre-commit: Run all tests with coverage
pytest --cov=src --cov-fail-under=80

# CI: Run all tests with detailed output
pytest -v --cov=src --cov-report=xml

# Debug: Run single test with debugger
pytest tests/test_module.py::test_function --pdb

# Watch mode (requires pytest-watch)
ptw -- --cov=src
```

### Useful pytest Options

```bash
-v              # Verbose output
-vv             # Extra verbose
-s              # Show print statements
-x              # Stop on first failure
--ff            # Run failed tests first
--lf            # Run last failed tests only
-k PATTERN      # Run tests matching pattern
-m MARKER       # Run tests with marker
--pdb           # Drop into debugger on failure
--durations=N   # Show N slowest tests
--cov=PATH      # Measure coverage
--cov-report=   # Coverage report format (term/html/xml)
```

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

- [pytest documentation](https://docs.pytest.org/) - Official pytest documentation
- [Python unittest assertions](https://docs.python.org/3/library/unittest.html#assert-methods) - Standard library assertions
- [pytest-cov documentation](https://pytest-cov.readthedocs.io/) - Coverage plugin
- [unittest.mock documentation](https://docs.python.org/3/library/unittest.mock.html) - Mocking library
- [CONTRIBUTING.md](../CONTRIBUTING.md) - Project contribution guidelines
- [scripts/tdd_workflow.py](../scripts/tdd_workflow.py) - TDD automation tool
