#!/usr/bin/env python3
"""Test-Driven Development workflow helpers and automation.

Provides commands for TDD cycle management and metrics tracking:
- tdd_init: Create failing test stub
- tdd_status: Show current red/green/refactor state
- tdd_cycle: Run test and show next step
- tdd_complete: Validate TDD cycle completion
"""

import os
import sys
import json
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List
from dataclasses import dataclass, asdict

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


@dataclass
class TDDState:
    """Represents current TDD workflow state."""

    feature_name: str
    test_file: Optional[str] = None
    phase: str = "init"  # init, red, green, refactor, complete
    started_at: Optional[str] = None
    test_first_committed: bool = False
    test_passed: bool = False
    implementation_completed: bool = False
    refactored: bool = False
    cycle_count: int = 0


@dataclass
class TDDMetrics:
    """TDD metrics for a session."""

    test_first_percentage: float = 0.0
    avg_red_green_cycle_time: float = 0.0
    coverage_increase_per_commit: float = 0.0
    tdd_compliance_by_developer: Dict[str, float] = None
    total_cycles: int = 0
    code_first_violations: int = 0


class TDDWorkflow:
    """Manages TDD workflow state and automation."""

    STATE_FILE = ".tdd_state.json"
    METRICS_FILE = ".tdd_metrics.json"

    def __init__(self, project_root: Optional[Path] = None):
        """Initialize TDD workflow manager.

        Args:
            project_root: Project root directory (defaults to current directory)
        """
        self.project_root = project_root or Path.cwd()
        self.state_file = self.project_root / self.STATE_FILE
        self.metrics_file = self.project_root / self.METRICS_FILE

    def load_state(self) -> Optional[TDDState]:
        """Load current TDD state from file."""
        if not self.state_file.exists():
            return None

        with open(self.state_file) as f:
            data = json.load(f)
            return TDDState(**data)

    def save_state(self, state: TDDState) -> None:
        """Save TDD state to file."""
        with open(self.state_file, "w") as f:
            json.dump(asdict(state), f, indent=2)

    def load_metrics(self) -> TDDMetrics:
        """Load TDD metrics from file."""
        if not self.metrics_file.exists():
            return TDDMetrics(tdd_compliance_by_developer={})

        with open(self.metrics_file) as f:
            data = json.load(f)
            if data.get("tdd_compliance_by_developer") is None:
                data["tdd_compliance_by_developer"] = {}
            return TDDMetrics(**data)

    def save_metrics(self, metrics: TDDMetrics) -> None:
        """Save TDD metrics to file."""
        with open(self.metrics_file, "w") as f:
            json.dump(asdict(metrics), f, indent=2)

    def tdd_init(self, feature_name: str, test_file: Optional[str] = None) -> TDDState:
        """Initialize TDD cycle with a failing test stub.

        Args:
            feature_name: Name of the feature being developed
            test_file: Optional path to test file (auto-generated if not provided)

        Returns:
            Initial TDD state
        """
        # Check if there's an active TDD session
        existing_state = self.load_state()
        if existing_state and existing_state.phase != "complete":
            print(f"⚠️  Active TDD session for '{existing_state.feature_name}' found.")
            print(f"Current phase: {existing_state.phase}")
            response = input("Start new session anyway? (y/N): ")
            if response.lower() != "y":
                print("Keeping existing session.")
                return existing_state

        # Generate test file path if not provided
        if not test_file:
            # Convert feature name to snake_case filename
            filename = feature_name.lower().replace(" ", "_").replace("-", "_")
            test_file = f"tests/test_{filename}.py"

        test_path = self.project_root / test_file

        # Create test stub if file doesn't exist
        if not test_path.exists():
            test_path.parent.mkdir(parents=True, exist_ok=True)
            self._create_test_stub(test_path, feature_name)
            print(f"✓ Created test stub: {test_file}")
        else:
            print(f"ℹ Using existing test file: {test_file}")

        # Create initial state
        state = TDDState(
            feature_name=feature_name,
            test_file=test_file,
            phase="red",
            started_at=datetime.now().isoformat(),
        )

        self.save_state(state)

        print(f"\n🔴 TDD Session Started: {feature_name}")
        print(f"Test file: {test_file}")
        print("\nNext step: Run `tdd_cycle` to verify test fails (RED phase)")

        return state

    def _create_test_stub(self, test_path: Path, feature_name: str) -> None:
        """Create a failing test stub."""
        stub_content = f'''"""Tests for {feature_name}."""

import pytest


class Test{feature_name.replace(" ", "").replace("-", "")}:
    """Test suite for {feature_name}."""

    def test_placeholder(self):
        """Placeholder test - replace with actual test."""
        # TODO: Implement test for {feature_name}
        assert False, "Implement this test"
'''

        with open(test_path, "w") as f:
            f.write(stub_content)

    def tdd_status(self) -> Optional[TDDState]:
        """Show current TDD state."""
        state = self.load_state()

        if not state:
            print("No active TDD session.")
            print("\nStart a new session with: tdd_init <feature_name>")
            return None

        # Phase indicators
        phase_symbols = {
            "init": "⚪️",
            "red": "🔴",
            "green": "🟢",
            "refactor": "🔵",
            "complete": "✅",
        }

        symbol = phase_symbols.get(state.phase, "⚪️")

        print(f"\n{symbol} TDD Session: {state.feature_name}")
        print(f"Phase: {state.phase.upper()}")
        print(f"Test file: {state.test_file}")

        if state.started_at:
            started = datetime.fromisoformat(state.started_at)
            duration = datetime.now() - started
            print(f"Duration: {duration.total_seconds() / 60:.1f} minutes")

        print(f"\nProgress:")
        print(f"  {'✓' if state.test_first_committed else '○'} Test written first")
        print(f"  {'✓' if state.test_passed else '○'} Test passing")
        print(f"  {'✓' if state.implementation_completed else '○'} Implementation complete")
        print(f"  {'✓' if state.refactored else '○'} Refactored")

        print(f"\nCycles completed: {state.cycle_count}")

        # Suggest next step
        self._suggest_next_step(state)

        return state

    def _suggest_next_step(self, state: TDDState) -> None:
        """Suggest next step based on current state."""
        suggestions = {
            "red": "Run tests to verify they fail, then implement minimal code to make them pass",
            "green": "Tests are passing! Now refactor if needed, or move to next feature",
            "refactor": "Refactoring complete. Run `tdd_complete` to finish this cycle",
            "complete": "Cycle complete! Start next feature with `tdd_init <feature_name>`",
        }

        suggestion = suggestions.get(state.phase, "Run `tdd_cycle` to continue")
        print(f"\nNext step: {suggestion}")

    def tdd_cycle(self) -> Optional[TDDState]:
        """Run test and advance TDD cycle."""
        state = self.load_state()

        if not state:
            print("❌ No active TDD session. Start one with `tdd_init <feature_name>`")
            return None

        print(f"\n🔄 Running TDD cycle for: {state.feature_name}")

        # Run the test
        test_result = self._run_test(state.test_file)

        if state.phase == "red":
            if test_result["failed"] > 0:
                print("\n✓ Test fails as expected (RED phase)")
                print("Next: Implement minimal code to make test pass")
                # Don't advance yet - let user implement
            else:
                print("\n⚠️  WARNING: Test should fail but it passed!")
                print("This might be a false positive. Review your test.")

        elif state.phase == "green":
            if test_result["passed"] > 0 and test_result["failed"] == 0:
                print("\n✓ Test passes (GREEN phase)")
                print("Next: Refactor if needed, or mark complete")
                state.test_passed = True
                state.implementation_completed = True
                state.phase = "refactor"
                state.cycle_count += 1
                self.save_state(state)
            else:
                print("\n🔴 Test still failing. Continue implementation.")

        elif state.phase == "refactor":
            if test_result["passed"] > 0 and test_result["failed"] == 0:
                print("\n✓ Tests still pass after refactoring")
                print("Cycle complete! Run `tdd_complete` to finish.")
            else:
                print("\n⚠️  Refactoring broke tests! Fix before proceeding.")

        # Update metrics
        self._update_cycle_metrics(state, test_result)

        return state

    def _run_test(self, test_file: str) -> Dict:
        """Run pytest on test file and return results."""
        try:
            result = subprocess.run(
                ["python", "-m", "pytest", test_file, "-v", "--tb=short"],
                capture_output=True,
                text=True,
                cwd=self.project_root,
            )

            output = result.stdout + result.stderr

            # Parse pytest output for results
            passed = output.count(" PASSED")
            failed = output.count(" FAILED")
            errors = output.count(" ERROR")

            print(f"\nTest Results:")
            print(f"  Passed: {passed}")
            print(f"  Failed: {failed}")
            print(f"  Errors: {errors}")

            if failed > 0 or errors > 0:
                print(f"\n{output}")

            return {
                "passed": passed,
                "failed": failed,
                "errors": errors,
                "return_code": result.returncode,
            }

        except Exception as e:
            print(f"❌ Error running tests: {e}")
            return {"passed": 0, "failed": 0, "errors": 1, "return_code": 1}

    def _update_cycle_metrics(self, state: TDDState, test_result: Dict) -> None:
        """Update TDD metrics based on cycle results."""
        metrics = self.load_metrics()
        metrics.total_cycles = state.cycle_count
        self.save_metrics(metrics)

    def tdd_complete(self) -> Optional[TDDState]:
        """Mark TDD cycle as complete and validate."""
        state = self.load_state()

        if not state:
            print("❌ No active TDD session.")
            return None

        # Validation checks
        violations = []

        if not state.test_first_committed:
            violations.append("Test was not committed before implementation")

        if not state.test_passed:
            violations.append("Test is not passing")

        if not state.implementation_completed:
            violations.append("Implementation not marked as complete")

        # Check for code-first anti-pattern
        if self._detect_code_first():
            violations.append("Code-first anti-pattern detected")
            metrics = self.load_metrics()
            metrics.code_first_violations += 1
            self.save_metrics(metrics)

        if violations:
            print("\n⚠️  TDD cycle has violations:")
            for v in violations:
                print(f"  - {v}")
            print("\nFix violations before completing.")
            return state

        # Mark complete
        state.phase = "complete"
        self.save_state(state)

        # Update metrics
        self._finalize_metrics(state)

        print(f"\n✅ TDD Cycle Complete: {state.feature_name}")
        print(f"Total cycles: {state.cycle_count}")
        print("\nStart next feature with `tdd_init <feature_name>`")

        return state

    def _detect_code_first(self) -> bool:
        """Detect if code was committed before test (anti-pattern)."""
        # Simple heuristic: check if there are recent commits without test files
        try:
            result = subprocess.run(
                ["git", "log", "--oneline", "-5", "--name-only"],
                capture_output=True,
                text=True,
                cwd=self.project_root,
            )

            if result.returncode != 0:
                return False

            # Look for commits with src/ changes but no test changes
            commits = result.stdout.split("\n\n")
            for commit in commits:
                files = commit.split("\n")[1:]  # Skip commit message
                has_src = any("src/" in f for f in files)
                has_test = any("test" in f.lower() for f in files)

                if has_src and not has_test:
                    return True

            return False

        except Exception:
            return False

    def _finalize_metrics(self, state: TDDState) -> None:
        """Finalize metrics for completed cycle."""
        metrics = self.load_metrics()

        # Calculate test-first percentage
        if state.test_first_committed:
            # Would need to track this across multiple sessions
            pass

        # Calculate average cycle time
        if state.started_at:
            started = datetime.fromisoformat(state.started_at)
            duration = (datetime.now() - started).total_seconds()
            if metrics.total_cycles > 0:
                metrics.avg_red_green_cycle_time = (
                    metrics.avg_red_green_cycle_time * (metrics.total_cycles - 1)
                    + duration
                ) / metrics.total_cycles

        self.save_metrics(metrics)


def main():
    """CLI entry point for TDD workflow commands."""
    import argparse

    parser = argparse.ArgumentParser(description="TDD workflow helper")
    subparsers = parser.add_subparsers(dest="command", help="TDD commands")

    # tdd_init
    init_parser = subparsers.add_parser("init", help="Initialize TDD cycle")
    init_parser.add_argument("feature_name", help="Name of feature to develop")
    init_parser.add_argument("--test-file", help="Path to test file")

    # tdd_status
    subparsers.add_parser("status", help="Show current TDD status")

    # tdd_cycle
    subparsers.add_parser("cycle", help="Run test and advance cycle")

    # tdd_complete
    subparsers.add_parser("complete", help="Mark cycle as complete")

    args = parser.parse_args()

    workflow = TDDWorkflow()

    if args.command == "init":
        workflow.tdd_init(args.feature_name, args.test_file)
    elif args.command == "status":
        workflow.tdd_status()
    elif args.command == "cycle":
        workflow.tdd_cycle()
    elif args.command == "complete":
        workflow.tdd_complete()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
