"""Tests for TDD workflow helpers."""

import json
import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Import tdd_workflow by loading it directly
project_root = Path(__file__).parent.parent
tdd_workflow_path = project_root / "scripts" / "tdd_workflow.py"

# Load module dynamically
import importlib.util
spec = importlib.util.spec_from_file_location("tdd_workflow", tdd_workflow_path)
tdd_workflow = importlib.util.module_from_spec(spec)
sys.modules["tdd_workflow"] = tdd_workflow
spec.loader.exec_module(tdd_workflow)

TDDWorkflow = tdd_workflow.TDDWorkflow
TDDState = tdd_workflow.TDDState
TDDMetrics = tdd_workflow.TDDMetrics


class TestTDDWorkflow:
    """Test TDD workflow management."""

    @pytest.fixture
    def workflow(self, tmp_path):
        """Create TDD workflow with temporary directory."""
        return TDDWorkflow(project_root=tmp_path)

    @pytest.fixture
    def sample_state(self):
        """Create sample TDD state."""
        return TDDState(
            feature_name="authentication",
            test_file="tests/test_authentication.py",
            phase="red",
            started_at=datetime.now().isoformat(),
        )

    def test_init_creates_new_session(self, workflow):
        """Test that init creates a new TDD session."""
        state = workflow.tdd_init("user_authentication")

        assert state.feature_name == "user_authentication"
        assert state.phase == "red"
        assert state.test_file is not None
        assert "test_user_authentication.py" in state.test_file

    def test_init_creates_test_stub(self, workflow):
        """Test that init creates a test stub file."""
        state = workflow.tdd_init("feature_x")

        test_path = workflow.project_root / state.test_file
        assert test_path.exists()

        content = test_path.read_text()
        assert "def test_placeholder" in content
        assert "assert False" in content

    def test_save_and_load_state(self, workflow, sample_state):
        """Test state persistence."""
        workflow.save_state(sample_state)

        loaded = workflow.load_state()

        assert loaded.feature_name == sample_state.feature_name
        assert loaded.phase == sample_state.phase
        assert loaded.test_file == sample_state.test_file

    def test_status_shows_no_session(self, workflow, capsys):
        """Test status with no active session."""
        result = workflow.tdd_status()

        assert result is None
        captured = capsys.readouterr()
        assert "No active TDD session" in captured.out

    def test_status_shows_current_phase(self, workflow, sample_state, capsys):
        """Test status displays current phase."""
        workflow.save_state(sample_state)

        workflow.tdd_status()

        captured = capsys.readouterr()
        assert "RED" in captured.out
        assert "authentication" in captured.out

    @patch("subprocess.run")
    def test_cycle_runs_tests(self, mock_run, workflow, sample_state):
        """Test that cycle runs pytest."""
        workflow.save_state(sample_state)

        # Mock pytest output indicating failure
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="test_auth.py::test_login FAILED",
            stderr="",
        )

        workflow.tdd_cycle()

        # Verify pytest was called
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "pytest" in call_args

    @patch("subprocess.run")
    def test_cycle_advances_on_pass(self, mock_run, workflow):
        """Test that cycle advances phase when tests pass."""
        # Start in green phase (implementation done, need to verify)
        state = TDDState(
            feature_name="test",
            test_file="tests/test_example.py",
            phase="green",
        )
        workflow.save_state(state)

        # Mock passing tests
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="test_example.py::test_foo PASSED",
            stderr="",
        )

        result = workflow.tdd_cycle()

        assert result.phase == "refactor"
        assert result.test_passed is True

    def test_complete_validates_cycle(self, workflow, sample_state):
        """Test that complete validates TDD cycle."""
        # Create incomplete state
        workflow.save_state(sample_state)

        result = workflow.tdd_complete()

        # Should not complete due to violations
        assert result.phase != "complete"

    def test_complete_marks_valid_cycle(self, workflow):
        """Test that complete marks valid cycle as done."""
        # Create complete state
        state = TDDState(
            feature_name="test",
            test_file="tests/test.py",
            phase="refactor",
            test_first_committed=True,
            test_passed=True,
            implementation_completed=True,
            refactored=True,
        )
        workflow.save_state(state)

        with patch.object(workflow, '_detect_code_first', return_value=False):
            result = workflow.tdd_complete()

        assert result.phase == "complete"

    def test_metrics_tracking(self, workflow):
        """Test TDD metrics are tracked."""
        metrics = TDDMetrics(
            test_first_percentage=85.0,
            total_cycles=5,
            code_first_violations=1,
        )

        workflow.save_metrics(metrics)
        loaded = workflow.load_metrics()

        assert loaded.test_first_percentage == 85.0
        assert loaded.total_cycles == 5
        assert loaded.code_first_violations == 1

    def test_detect_code_first_antipattern(self, workflow):
        """Test detection of code-first anti-pattern."""
        # This would require git repo setup, so we just test the method exists
        result = workflow._detect_code_first()
        assert isinstance(result, bool)


class TestTDDMetrics:
    """Test TDD metrics calculations."""

    def test_metrics_initialization(self):
        """Test metrics initialize with defaults."""
        metrics = TDDMetrics()

        assert metrics.test_first_percentage == 0.0
        assert metrics.total_cycles == 0
        assert metrics.code_first_violations == 0

    def test_metrics_with_compliance_data(self):
        """Test metrics with per-developer compliance."""
        metrics = TDDMetrics(
            tdd_compliance_by_developer={"alice": 95.0, "bob": 78.5}
        )

        assert metrics.tdd_compliance_by_developer["alice"] == 95.0
        assert metrics.tdd_compliance_by_developer["bob"] == 78.5
