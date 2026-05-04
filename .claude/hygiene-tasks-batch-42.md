# Hygiene Mode Tasks - Batch 42

Generated: 2026-05-04

## Task 1: Add type stubs for config.py dataclasses
**Priority**: 2
**Family**: tests/quality
**Agent fit**: claude-code

**Description**: Add runtime type validation for all configuration dataclasses in `src/config.py` using `pydantic` or similar validation library to ensure config files are properly validated at load time.

**Files**:
- `src/config.py` (modify: add validators, constraints)
- `tests/test_config_validation.py` (create: validate type checking, invalid config handling)

**Acceptance criteria**:
- All config dataclasses have field validators
- Invalid config files raise clear validation errors
- Tests cover missing required fields, type mismatches, invalid values
- Validation errors include helpful messages with field names

**Verification**: `pytest tests/test_config_validation.py -v && mypy src/config.py`

---

## Task 2: Add integration tests for blog_writer.py end-to-end flow
**Priority**: 1
**Family**: tests/quality
**Agent fit**: claude-code

**Description**: Create comprehensive integration tests for `src/output/blog_writer.py` covering the full blog generation pipeline from content idea to published markdown with frontmatter.

**Files**:
- `tests/integration/test_blog_writer_integration.py` (create: end-to-end tests)
- `src/output/blog_writer.py` (read: understand flow)

**Acceptance criteria**:
- Tests cover full pipeline: idea → draft → refinement → publish
- Mock external API calls (Anthropic, file I/O)
- Validate frontmatter generation, metadata accuracy
- Test error handling for API failures, invalid inputs
- Test coverage >80% for blog_writer.py

**Verification**: `pytest tests/integration/test_blog_writer_integration.py -v && coverage report --include="src/output/blog_writer.py"`

---

## Task 3: Add error boundary pattern to runner.py
**Priority**: 2
**Family**: workspace/config
**Agent fit**: claude-code

**Description**: Implement structured error handling and recovery in `src/runner.py` with proper exception hierarchy, logging, and graceful degradation for pipeline failures.

**Files**:
- `src/runner.py` (modify: add error boundaries)
- `src/exceptions.py` (create: custom exception hierarchy)
- `tests/test_runner_error_handling.py` (create: error scenario tests)

**Acceptance criteria**:
- Custom exceptions for different failure modes (API, data, config)
- Graceful degradation when non-critical components fail
- Comprehensive error logging with context
- Tests cover all error paths
- No bare `except:` clauses

**Verification**: `pytest tests/test_runner_error_handling.py -v && pylint src/runner.py --disable=all --enable=bare-except`

---

## Task 4: Add snapshot tests for proactive action modules
**Priority**: 2
**Family**: tests/quality
**Agent fit**: claude-code

**Description**: Create snapshot tests for proactive action modules (`src/engagement/proactive_*.py`) to detect unintended changes in action selection, cooldown logic, and target identification.

**Files**:
- `tests/snapshots/test_proactive_actions.py` (create: snapshot tests)
- `src/engagement/proactive_cooldown.py` (read)
- `src/engagement/proactive_action_outcomes.py` (read)
- `src/engagement/proactive_action_target_audit.py` (read)

**Acceptance criteria**:
- Snapshot tests for action selection determinism
- Snapshot tests for cooldown calculations
- Snapshot tests for target identification logic
- Tests use pytest-snapshot or similar
- Snapshots checked into git for review

**Verification**: `pytest tests/snapshots/test_proactive_actions.py -v --snapshot-update`

---

## Task 5: Add API client retry and backoff to all external clients
**Priority**: 1
**Family**: api/endpoints
**Agent fit**: claude-code

**Description**: Implement exponential backoff and retry logic for all API clients (GitHub, X/Twitter, Bluesky, Anthropic) using `tenacity` or similar library to handle transient failures.

**Files**:
- `src/ingestion/github_client.py` (modify: add retry decorators)
- `src/output/bluesky_client.py` (modify: add retry decorators)
- `src/output/x_client.py` (modify: add retry decorators)
- `tests/test_api_retry_behavior.py` (create: retry tests with mocked failures)

**Acceptance criteria**:
- All API calls use @retry decorator with exponential backoff
- Retry on 429, 500, 502, 503, 504 status codes
- Max 3 retries with jitter
- Tests verify retry behavior with mocked transient failures
- Logging shows retry attempts

**Verification**: `pytest tests/test_api_retry_behavior.py -v && grep -r "@retry" src/`

---

## Task 6: Add database migration infrastructure
**Priority**: 2
**Family**: data/store
**Agent fit**: claude-code

**Description**: Set up database migration tooling (Alembic or similar) for `src/presence.db` to version-control schema changes and enable safe schema evolution.

**Files**:
- `alembic.ini` (create: Alembic configuration)
- `alembic/` (create: migrations directory)
- `alembic/env.py` (create: migration environment)
- `src/storage/migrations.py` (create: migration helpers)
- `tests/test_migrations.py` (create: migration tests)

**Acceptance criteria**:
- Alembic configured for SQLite
- Initial migration captures current schema
- Migration up/down tested
- Documentation in CONTRIBUTING.md for creating migrations
- CI runs migration tests

**Verification**: `alembic upgrade head && alembic downgrade -1 && alembic upgrade head && pytest tests/test_migrations.py -v`

---

## Task 7: Add property-based tests for evaluation score calculations
**Priority**: 2
**Family**: tests/quality
**Agent fit**: claude-code

**Description**: Create property-based tests using `hypothesis` for evaluation score calculation modules to validate mathematical properties (monotonicity, bounds, consistency).

**Files**:
- `tests/property/test_evaluation_properties.py` (create: property tests)
- `src/evaluation/engagement_predictor.py` (read)
- `src/evaluation/pipeline_refinement_delta.py` (read)
- `src/evaluation/content_idea_aging_pressure.py` (read)

**Acceptance criteria**:
- Property tests verify score bounds (0-1, 0-100, etc.)
- Property tests verify monotonicity where applicable
- Property tests verify score consistency (same input → same output)
- Property tests verify edge cases (empty input, zero values, negative values)
- Uses hypothesis strategies for generating test data

**Verification**: `pytest tests/property/test_evaluation_properties.py -v --hypothesis-show-statistics`

---

## Task 8: Add precommit hooks for code quality automation
**Priority**: 1
**Family**: workspace/config
**Agent fit**: claude-code

**Description**: Set up `pre-commit` framework with hooks for automatic formatting (black), linting (ruff), type checking (mypy), and test running to enforce code quality before commits.

**Files**:
- `.pre-commit-config.yaml` (create: hook configuration)
- `pyproject.toml` (modify: add black/ruff/mypy configuration)
- `CONTRIBUTING.md` (modify: document pre-commit setup)
- `.github/workflows/ci.yml` (modify: run pre-commit in CI)

**Acceptance criteria**:
- Pre-commit hooks configured: black, ruff, mypy, pytest
- Hooks run automatically on `git commit`
- CI runs same hooks to catch skipped local runs
- Documentation explains how to install: `pre-commit install`
- Hooks enforce consistent code style

**Verification**: `pre-commit run --all-files && git commit --allow-empty -m "test hooks" && pre-commit uninstall`

---

## Execution Pack 1: Quality Infrastructure (Tasks 1, 2, 7, 8)
**Goal**: Establish robust testing and validation infrastructure
**Shared verification**: `pytest tests/ -v && mypy src/ && pre-commit run --all-files`

## Execution Pack 2: Reliability & Operations (Tasks 3, 4, 5, 6)
**Goal**: Improve runtime reliability and operational safety
**Shared verification**: `pytest tests/ -v && python src/runner.py --dry-run`
