# Contributing to Presence

Thank you for contributing to the Presence content synthesis system!

## Test-Driven Development Workflow

We follow Test-Driven Development (TDD) practices for all new features and bug fixes.

### TDD Best Practices

**The Red-Green-Refactor Cycle:**

1. **Red**: Write a failing test first
2. **Green**: Write minimal code to make it pass
3. **Refactor**: Improve code while keeping tests green

### Using the TDD Workflow Helper

We provide automated TDD workflow tools to help maintain discipline:

```bash
# Start a new TDD session
python scripts/tdd_workflow.py init "user authentication"

# This creates:
# - tests/test_user_authentication.py with a failing test stub
# - .tdd_state.json to track your progress

# Check current status
python scripts/tdd_workflow.py status

# Run test and advance cycle
python scripts/tdd_workflow.py cycle

# Mark cycle complete (validates TDD compliance)
python scripts/tdd_workflow.py complete
```

### TDD Metrics

View TDD metrics and compliance:

```bash
./scripts/tdd_metrics.sh
```

This shows:
- Test-first percentage
- Average red-green-refactor cycle time
- Coverage increase per commit
- Code-first violations
- Per-developer TDD compliance

### TDD Anti-Patterns to Avoid

❌ **Code-First**: Writing implementation before tests
❌ **Testing After**: Adding tests as an afterthought
❌ **Skipping Red**: Not verifying tests fail first
❌ **False Positives**: Tests that pass without implementation

✅ **Good TDD Practice**:
- Always write test first
- Verify test fails (RED)
- Write minimal implementation
- Verify test passes (GREEN)
- Refactor while keeping tests green

## Custom Test Assertions

We provide domain-specific assertion helpers in `tests/helpers/assertions.py` to make tests more readable and maintainable.

See `tests/README.md` for full documentation.

### Quick Examples

```python
from tests.helpers.assertions import (
    assert_valid_post,
    assert_valid_thread,
    assert_evaluation_scores_valid,
)

# Validate post content
assert_valid_post(generated_content, char_limit=280)

# Validate thread structure
assert_valid_thread(tweet_list, check_continuity=True)

# Validate evaluation scores
assert_evaluation_scores_valid({"opus": 9.0, "sonnet": 7.5})
```

## Test Performance Standards

- **Unit tests**: < 100ms per test
- **Integration tests**: < 1s per test
- **Total test suite**: < 10s

Run performance profiling:

```bash
python scripts/profile_tests.py
```

## Test Organization

```
tests/
├── helpers/              # Custom assertion helpers
│   ├── assertions.py     # Domain-specific assertions
│   └── parametrize.py    # Data-driven test utilities
├── fixtures/             # Shared test fixtures
├── test_cases/           # YAML/CSV test case data
├── test_*.py             # Test files (mirror src/ structure)
└── README.md             # Test documentation
```

## Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_synthesis_pipeline.py

# Run with coverage
pytest --cov=src --cov-report=html

# Run tests matching pattern
pytest -k "deduplication"

# Verbose output
pytest -v
```

## Code Style

- Follow PEP 8
- Use type hints for function signatures
- Write docstrings for public functions
- Keep functions focused and small

## Commit Messages

Format:
```
<type>: <brief summary>

<detailed description>

<footer>
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `test`: Add or update tests
- `refactor`: Code refactoring
- `docs`: Documentation changes
- `perf`: Performance improvements

Example:
```
feat: Add deduplication layer to synthesis pipeline

Implements 3-layer deduplication:
- Opening-clause similarity (SequenceMatcher)
- Semantic embedding similarity
- Stale pattern detection

Reduces duplicate content by 85% in testing.
```

## Pull Request Process

1. Create a feature branch
2. Write tests first (TDD)
3. Implement feature
4. Ensure all tests pass
5. Run TDD metrics to verify compliance
6. Submit PR with clear description

## Questions?

Open an issue or reach out to maintainers.
