"""Presence test suite package.

This package marks the tests directory as a Python package, enabling proper
import of test helpers, fixtures, and utilities across test modules.

## Test Organization

The test suite is organized into:
- `tests/test_*.py` - Individual test modules
- `tests/conftest.py` - Shared pytest fixtures
- `tests/helpers/` - Custom assertion helpers and utilities
- `tests/fixtures/` - Test data files

## Importing Test Helpers

Import helpers directly from the helpers package:

```python
from tests.helpers import assert_valid_post, assert_valid_thread
```

## Importing Fixtures

Fixtures defined in conftest.py are automatically available to all tests:

```python
def test_database_operations(db, sample_message):
    # db and sample_message fixtures are auto-injected
    pass
```

## Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_module.py

# Run with coverage
pytest --cov=src --cov-report=term-missing
```

See tests/README.md for comprehensive testing documentation.
"""

# Re-export commonly used fixtures for convenience
# Note: Fixtures are auto-discovered by pytest from conftest.py,
# but we can import them here for type hints or programmatic access

__all__ = []  # No public API from this module
