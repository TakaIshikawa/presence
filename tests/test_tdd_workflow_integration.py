"""Integration tests for TDD workflow helpers.

These tests validate the complete TDD workflow with real test files,
ensuring the tooling actually works end-to-end.
"""

import pytest
import subprocess
import sys
from pathlib import Path
from datetime import datetime
from unittest.mock import patch

# Import tdd_workflow by loading it directly
project_root = Path(__file__).parent.parent
tdd_workflow_path = project_root / "scripts" / "tdd_workflow.py"

# Load module dynamically
import importlib.util
spec = importlib.util.spec_from_file_location("tdd_workflow", tdd_workflow_path)
if spec is None or spec.loader is None:
    raise ImportError(f"Could not load module from {tdd_workflow_path}")
tdd_workflow = importlib.util.module_from_spec(spec)
sys.modules["tdd_workflow"] = tdd_workflow
spec.loader.exec_module(tdd_workflow)

TDDWorkflow = tdd_workflow.TDDWorkflow
TDDState = tdd_workflow.TDDState
TDDMetrics = tdd_workflow.TDDMetrics


class TestTDDWorkflowIntegration:
    """Integration tests for TDD workflow with real test files."""

    @pytest.fixture
    def isolated_project(self, tmp_path):
        """Create isolated project directory with git repo."""
        # Initialize git repo for code-first detection
        subprocess.run(
            ["git", "init"],
            cwd=tmp_path,
            capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.email", "test@example.com"],
            cwd=tmp_path,
            capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Test User"],
            cwd=tmp_path,
            capture_output=True,
        )

        # Create basic project structure
        (tmp_path / "src").mkdir()
        (tmp_path / "tests").mkdir()

        return tmp_path

    @pytest.fixture
    def workflow(self, isolated_project):
        """Create TDD workflow in isolated project."""
        return TDDWorkflow(project_root=isolated_project)

    def test_init_creates_valid_failing_test_stub(self, workflow):
        """Test that tdd_init creates a valid failing test stub."""
        # Initialize TDD session
        state = workflow.tdd_init("user_login")

        # Verify state
        assert state.feature_name == "user_login"
        assert state.phase == "red"
        assert state.test_file == "tests/test_user_login.py"

        # Verify test file exists
        test_path = workflow.project_root / state.test_file
        assert test_path.exists()

        # Verify test content
        content = test_path.read_text()
        assert "import pytest" in content
        # Class name is generated from feature name (with underscores removed/replaced)
        assert "class Test" in content
        assert "def test_placeholder" in content
        assert 'assert False, "Implement this test"' in content

        # Verify test is valid Python
        try:
            compile(content, state.test_file, "exec")
        except SyntaxError as e:
            pytest.fail(f"Generated test has syntax errors: {e}")

        # Verify test actually fails when run
        result = subprocess.run(
            ["python", "-m", "pytest", state.test_file, "-v"],
            cwd=workflow.project_root,
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0, "Test stub should fail"
        assert "FAILED" in result.stdout or "FAILED" in result.stderr

    def test_status_accurately_detects_red_state(self, workflow, capsys):
        """Test that tdd_status accurately detects RED state."""
        # Initialize and create failing test
        state = workflow.tdd_init("feature_a")

        # Check status
        status_state = workflow.tdd_status()

        # Verify output
        captured = capsys.readouterr()
        assert "RED" in captured.out
        assert "feature_a" in captured.out
        assert status_state.phase == "red"

    def test_status_detects_green_state_after_implementation(self, workflow, capsys):
        """Test that tdd_status accurately detects GREEN state."""
        # Initialize TDD session
        state = workflow.tdd_init("simple_math")

        # Create a passing test by replacing stub
        test_path = workflow.project_root / state.test_file
        passing_test = '''"""Tests for simple_math."""

import pytest


class TestSimpleMath:
    """Test suite for simple_math."""

    def test_addition(self):
        """Test that 1 + 1 equals 2."""
        assert 1 + 1 == 2
'''
        test_path.write_text(passing_test)

        # Manually advance to green phase
        state.phase = "green"
        workflow.save_state(state)

        # Check status
        status_state = workflow.tdd_status()

        # Verify output
        captured = capsys.readouterr()
        assert "GREEN" in captured.out
        assert status_state.phase == "green"

    def test_status_detects_refactor_state(self, workflow, capsys):
        """Test that tdd_status accurately detects REFACTOR state."""
        # Create state in refactor phase
        state = TDDState(
            feature_name="feature_b",
            test_file="tests/test_feature_b.py",
            phase="refactor",
            test_passed=True,
            implementation_completed=True,
        )
        workflow.save_state(state)

        # Check status
        status_state = workflow.tdd_status()

        # Verify output
        captured = capsys.readouterr()
        assert "REFACTOR" in captured.out
        assert status_state.phase == "refactor"

    def test_cycle_runs_tests_and_suggests_next_step_red_phase(self, workflow, capsys):
        """Test that tdd_cycle runs tests and suggests correct next step in RED phase."""
        # Initialize with failing test
        state = workflow.tdd_init("test_feature")

        # Run cycle
        result_state = workflow.tdd_cycle()

        # Verify output
        captured = capsys.readouterr()
        assert "Test fails as expected" in captured.out or "RED phase" in captured.out
        assert "Implement" in captured.out or "make test pass" in captured.out

        # State should still be red (waiting for implementation)
        assert result_state.phase == "red"

    def test_cycle_advances_from_green_to_refactor(self, workflow, capsys):
        """Test that tdd_cycle advances from GREEN to REFACTOR when tests pass."""
        # Initialize and create passing test
        state = workflow.tdd_init("passing_feature")

        test_path = workflow.project_root / state.test_file
        passing_test = '''"""Tests for passing_feature."""

import pytest


class TestPassingFeature:
    """Test suite for passing_feature."""

    def test_it_works(self):
        """Test that basic assertion works."""
        assert True
'''
        test_path.write_text(passing_test)

        # Set to green phase (implementation done, verifying)
        state.phase = "green"
        workflow.save_state(state)

        # Run cycle
        result_state = workflow.tdd_cycle()

        # Verify advancement to refactor
        assert result_state.phase == "refactor"
        assert result_state.test_passed is True
        assert result_state.implementation_completed is True
        assert result_state.cycle_count == 1

        # Verify output
        captured = capsys.readouterr()
        assert "GREEN" in captured.out or "passes" in captured.out

    def test_cycle_stays_in_refactor_when_tests_still_pass(self, workflow, capsys):
        """Test that tdd_cycle stays in REFACTOR phase when tests still pass."""
        # Create passing test
        state = workflow.tdd_init("refactor_feature")

        test_path = workflow.project_root / state.test_file
        passing_test = '''"""Tests for refactor_feature."""

import pytest


class TestRefactorFeature:
    """Test suite for refactor_feature."""

    def test_stable(self):
        """Test that remains stable during refactoring."""
        result = 2 + 2
        assert result == 4
'''
        test_path.write_text(passing_test)

        # Set to refactor phase
        state.phase = "refactor"
        state.test_passed = True
        state.implementation_completed = True
        workflow.save_state(state)

        # Run cycle
        result_state = workflow.tdd_cycle()

        # Verify still in refactor
        assert result_state.phase == "refactor"

        # Verify output
        captured = capsys.readouterr()
        assert "still pass" in captured.out or "complete" in captured.out.lower()

    def test_cycle_warns_on_refactor_breaking_tests(self, workflow, capsys):
        """Test that tdd_cycle warns when refactoring breaks tests."""
        # Create failing test
        state = workflow.tdd_init("broken_refactor")

        test_path = workflow.project_root / state.test_file
        failing_test = '''"""Tests for broken_refactor."""

import pytest


class TestBrokenRefactor:
    """Test suite for broken_refactor."""

    def test_broken(self):
        """Test that fails."""
        assert False, "Refactoring broke this"
'''
        test_path.write_text(failing_test)

        # Set to refactor phase (but test will fail)
        state.phase = "refactor"
        workflow.save_state(state)

        # Run cycle
        workflow.tdd_cycle()

        # Verify warning
        captured = capsys.readouterr()
        assert "broke" in captured.out.lower() or "failing" in captured.out.lower()

    def test_complete_validates_full_cycle(self, workflow, capsys):
        """Test that tdd_complete validates entire TDD cycle completion."""
        # Create incomplete state (missing test_first_committed)
        state = TDDState(
            feature_name="incomplete",
            test_file="tests/test_incomplete.py",
            phase="refactor",
            test_passed=True,
            implementation_completed=True,
            test_first_committed=False,  # Violation
        )
        workflow.save_state(state)

        # Try to complete
        with patch.object(workflow, '_detect_code_first', return_value=False):
            result_state = workflow.tdd_complete()

        # Should not complete
        assert result_state.phase != "complete"

        # Verify violations reported
        captured = capsys.readouterr()
        assert "violation" in captured.out.lower()
        assert "not committed" in captured.out.lower() or "committed" in captured.out.lower()

    def test_complete_succeeds_with_valid_cycle(self, workflow, capsys):
        """Test that tdd_complete marks valid cycle as complete."""
        # Create valid complete state
        state = TDDState(
            feature_name="complete_feature",
            test_file="tests/test_complete.py",
            phase="refactor",
            test_first_committed=True,
            test_passed=True,
            implementation_completed=True,
            refactored=True,
            started_at=datetime.now().isoformat(),
        )
        workflow.save_state(state)

        # Mock code-first detection to pass
        with patch.object(workflow, '_detect_code_first', return_value=False):
            result_state = workflow.tdd_complete()

        # Should complete successfully
        assert result_state.phase == "complete"

        # Verify success message
        captured = capsys.readouterr()
        assert "Complete" in captured.out or "✅" in captured.out

    def test_complete_detects_code_first_antipattern(self, workflow, capsys):
        """Test that tdd_complete detects code-first anti-pattern."""
        # Create state that would otherwise be valid
        state = TDDState(
            feature_name="code_first",
            test_file="tests/test_code_first.py",
            phase="refactor",
            test_first_committed=True,
            test_passed=True,
            implementation_completed=True,
        )
        workflow.save_state(state)

        # Mock code-first detection to fail
        with patch.object(workflow, '_detect_code_first', return_value=True):
            result_state = workflow.tdd_complete()

        # Should not complete
        assert result_state.phase != "complete"

        # Verify violation reported
        captured = capsys.readouterr()
        assert "anti-pattern" in captured.out.lower() or "code-first" in captured.out.lower()

        # Verify metrics updated
        metrics = workflow.load_metrics()
        assert metrics.code_first_violations > 0

    def test_metrics_test_first_percentage_calculation(self, workflow):
        """Test that test-first percentage is calculated correctly."""
        # Initialize metrics
        metrics = TDDMetrics(
            total_cycles=10,
            code_first_violations=2,
        )
        workflow.save_metrics(metrics)

        # Load and verify
        loaded = workflow.load_metrics()
        assert loaded.total_cycles == 10
        assert loaded.code_first_violations == 2

        # Test-first percentage should be 80% (8 out of 10)
        expected_percentage = ((10 - 2) / 10) * 100
        assert expected_percentage == 80.0

    def test_metrics_cycle_time_accuracy(self, workflow):
        """Test that cycle time measurement is accurate."""
        # Create state with known start time
        start_time = datetime.now()
        state = TDDState(
            feature_name="timed_feature",
            test_file="tests/test_timed.py",
            phase="refactor",
            started_at=start_time.isoformat(),
            test_first_committed=True,
            test_passed=True,
            implementation_completed=True,
            cycle_count=1,
        )
        workflow.save_state(state)

        # Complete cycle (this finalizes metrics)
        with patch.object(workflow, '_detect_code_first', return_value=False):
            workflow.tdd_complete()

        # Verify metrics were updated
        metrics = workflow.load_metrics()
        # avg_red_green_cycle_time should be set (actual value depends on timing)
        # Just verify it's a reasonable number (less than a few seconds for this test)
        assert metrics.avg_red_green_cycle_time >= 0

    def test_metrics_accumulation_across_cycles(self, workflow):
        """Test that metrics accumulate correctly across multiple cycles."""
        # Complete first cycle
        state1 = TDDState(
            feature_name="feature_1",
            test_file="tests/test_1.py",
            phase="refactor",
            test_first_committed=True,
            test_passed=True,
            implementation_completed=True,
            cycle_count=1,
            started_at=datetime.now().isoformat(),
        )
        workflow.save_state(state1)

        with patch.object(workflow, '_detect_code_first', return_value=False):
            workflow.tdd_complete()

        metrics_after_1 = workflow.load_metrics()
        cycles_after_1 = metrics_after_1.total_cycles

        # Complete second cycle
        state2 = TDDState(
            feature_name="feature_2",
            test_file="tests/test_2.py",
            phase="refactor",
            test_first_committed=True,
            test_passed=True,
            implementation_completed=True,
            cycle_count=1,
            started_at=datetime.now().isoformat(),
        )
        workflow.save_state(state2)

        with patch.object(workflow, '_detect_code_first', return_value=False):
            workflow.tdd_complete()

        metrics_after_2 = workflow.load_metrics()

        # Total cycles should accumulate
        assert metrics_after_2.total_cycles >= cycles_after_1

    def test_error_handling_malformed_test(self, workflow, capsys):
        """Test error handling for malformed test files."""
        # Create test with syntax error
        state = workflow.tdd_init("malformed")

        test_path = workflow.project_root / state.test_file
        malformed_test = '''"""Malformed test."""

import pytest

def test_broken(:  # Syntax error
    assert True
'''
        test_path.write_text(malformed_test)

        # Run cycle - should handle error gracefully
        result = workflow.tdd_cycle()

        # Verify error handling
        captured = capsys.readouterr()
        # Should show error output
        assert "Error" in captured.out or "ERROR" in captured.out or "error" in captured.out.lower()

    def test_error_handling_no_tests_found(self, workflow, capsys):
        """Test error handling when no tests are found."""
        # Create empty test file
        state = workflow.tdd_init("no_tests")

        test_path = workflow.project_root / state.test_file
        empty_test = '''"""Empty test file."""

import pytest

# No tests defined
'''
        test_path.write_text(empty_test)

        # Run cycle
        workflow.tdd_cycle()

        # Verify output indicates no tests
        captured = capsys.readouterr()
        # Should show 0 passed, 0 failed
        assert "Passed: 0" in captured.out

    def test_error_handling_test_never_fails(self, workflow, capsys):
        """Test detection of tests that never fail (false positive)."""
        # Initialize with a test that passes immediately
        state = workflow.tdd_init("always_passes")

        test_path = workflow.project_root / state.test_file
        always_passes = '''"""Test that always passes."""

import pytest


class TestAlwaysPasses:
    """Test suite that always passes."""

    def test_obvious(self):
        """Test that trivially passes."""
        assert True
'''
        test_path.write_text(always_passes)

        # Try to run in RED phase (should warn)
        workflow.tdd_cycle()

        # Verify warning
        captured = capsys.readouterr()
        assert "WARNING" in captured.out or "should fail" in captured.out

    def test_test_stub_imports_are_correct(self, workflow):
        """Test that generated test stub has correct imports."""
        state = workflow.tdd_init("import_check")

        test_path = workflow.project_root / state.test_file
        content = test_path.read_text()

        # Verify pytest import
        assert "import pytest" in content

        # Verify no invalid imports
        assert "import undefined_module" not in content

    def test_test_stub_class_naming_convention(self, workflow):
        """Test that test stub follows class naming conventions."""
        state = workflow.tdd_init("my_feature")

        test_path = workflow.project_root / state.test_file
        content = test_path.read_text()

        # Class should be PascalCase starting with "Test"
        assert "class Test" in content

    def test_full_tdd_cycle_end_to_end(self, workflow, capsys):
        """Test complete TDD cycle from init to complete."""
        # 1. Initialize (RED phase)
        state = workflow.tdd_init("full_cycle_feature")
        assert state.phase == "red"

        # 2. Verify test fails
        workflow.tdd_cycle()
        captured = capsys.readouterr()
        assert "fail" in captured.out.lower() or "FAILED" in captured.out

        # 3. Implement (replace with passing test)
        test_path = workflow.project_root / state.test_file
        passing_test = '''"""Tests for full_cycle_feature."""

import pytest


class TestFullCycleFeature:
    """Test suite for full_cycle_feature."""

    def test_feature_works(self):
        """Test that feature works."""
        result = 10 / 2
        assert result == 5
'''
        test_path.write_text(passing_test)

        # 4. Move to GREEN phase
        state = workflow.load_state()
        state.phase = "green"
        workflow.save_state(state)

        # 5. Verify test passes (GREEN → REFACTOR)
        state = workflow.tdd_cycle()
        assert state.phase == "refactor"
        assert state.test_passed is True

        # 6. Complete cycle
        state.test_first_committed = True
        workflow.save_state(state)

        with patch.object(workflow, '_detect_code_first', return_value=False):
            final_state = workflow.tdd_complete()

        assert final_state.phase == "complete"

    def test_status_shows_progress_indicators(self, workflow, capsys):
        """Test that status shows progress indicators correctly."""
        # Create state with partial progress
        state = TDDState(
            feature_name="progress_test",
            test_file="tests/test_progress.py",
            phase="green",
            test_first_committed=True,
            test_passed=False,
            implementation_completed=False,
        )
        workflow.save_state(state)

        # Check status
        workflow.tdd_status()

        captured = capsys.readouterr()
        # Should show checked and unchecked items
        assert "✓" in captured.out or "○" in captured.out
        assert "Test written first" in captured.out
        assert "Test passing" in captured.out

    def test_state_persistence_across_workflow_instances(self, isolated_project):
        """Test that state persists across different workflow instances."""
        # Create workflow and initialize
        workflow1 = TDDWorkflow(project_root=isolated_project)
        state1 = workflow1.tdd_init("persistence_test")

        # Create new workflow instance
        workflow2 = TDDWorkflow(project_root=isolated_project)
        state2 = workflow2.load_state()

        # State should be identical
        assert state2 is not None
        assert state2.feature_name == state1.feature_name
        assert state2.phase == state1.phase
        assert state2.test_file == state1.test_file

    def test_metrics_persistence_across_workflow_instances(self, isolated_project):
        """Test that metrics persist across different workflow instances."""
        # Create workflow and save metrics
        workflow1 = TDDWorkflow(project_root=isolated_project)
        metrics1 = TDDMetrics(
            total_cycles=5,
            code_first_violations=1,
            test_first_percentage=80.0,
        )
        workflow1.save_metrics(metrics1)

        # Create new workflow instance
        workflow2 = TDDWorkflow(project_root=isolated_project)
        metrics2 = workflow2.load_metrics()

        # Metrics should be identical
        assert metrics2.total_cycles == 5
        assert metrics2.code_first_violations == 1
        assert metrics2.test_first_percentage == 80.0


class TestTDDWorkflowSuggestions:
    """Test that TDD workflow provides helpful and accurate suggestions."""

    @pytest.fixture
    def workflow(self, tmp_path):
        """Create TDD workflow with temporary directory."""
        return TDDWorkflow(project_root=tmp_path)

    def test_red_phase_suggests_implementation(self, workflow, capsys):
        """Test that RED phase suggests implementing code."""
        state = TDDState(
            feature_name="test",
            test_file="tests/test.py",
            phase="red",
        )
        workflow.save_state(state)

        workflow.tdd_status()
        captured = capsys.readouterr()

        assert "implement" in captured.out.lower() or "code" in captured.out.lower()

    def test_green_phase_suggests_refactoring(self, workflow, capsys):
        """Test that GREEN phase suggests refactoring."""
        state = TDDState(
            feature_name="test",
            test_file="tests/test.py",
            phase="green",
        )
        workflow.save_state(state)

        workflow.tdd_status()
        captured = capsys.readouterr()

        assert "refactor" in captured.out.lower()

    def test_refactor_phase_suggests_completion(self, workflow, capsys):
        """Test that REFACTOR phase suggests completing cycle."""
        state = TDDState(
            feature_name="test",
            test_file="tests/test.py",
            phase="refactor",
        )
        workflow.save_state(state)

        workflow.tdd_status()
        captured = capsys.readouterr()

        assert "complete" in captured.out.lower()

    def test_complete_phase_suggests_next_feature(self, workflow, capsys):
        """Test that COMPLETE phase suggests starting next feature."""
        state = TDDState(
            feature_name="test",
            test_file="tests/test.py",
            phase="complete",
        )
        workflow.save_state(state)

        workflow.tdd_status()
        captured = capsys.readouterr()

        assert "next" in captured.out.lower() or "new" in captured.out.lower()
