"""Tests for pack verification command diversity analyzer."""

import pytest

from synthesis.pack_verification_command_diversity import (
    analyze_pack_verification_command_diversity,
    _classify_command,
    _split_commands,
)


class TestAnalyzePackVerificationCommandDiversity:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_pack_verification_command_diversity([])

        assert result["total_packs"] == 0
        assert result["packs_with_verification"] == 0
        assert result["unique_commands"] == 0
        assert result["avg_commands_per_pack"] == 0.0
        assert result["command_type_distribution"] == []
        assert result["multi_stage_percentage"] == 0.0
        assert result["single_stage_count"] == 0
        assert result["no_verification_count"] == 0
        assert result["avg_file_coverage"] == 0.0
        assert result["success_by_diversity"] == []
        assert result["weak_verification_packs"] == []
        assert result["common_command_patterns"] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_verification_command_diversity(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_verification_command_diversity("not a list")

    def test_pack_with_no_verification(self):
        """Verify pack without verification command is tracked."""
        result = analyze_pack_verification_command_diversity([
            {"pack_id": "pack1", "verification_command": ""}
        ])

        assert result["total_packs"] == 1
        assert result["packs_with_verification"] == 0
        assert result["no_verification_count"] == 1
        assert len(result["weak_verification_packs"]) == 1
        assert result["weak_verification_packs"][0]["reason"] == "No verification command"

    def test_pack_with_single_test_command(self):
        """Verify pack with single test command."""
        result = analyze_pack_verification_command_diversity([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/test_analyzer.py -v"
            }
        ])

        assert result["packs_with_verification"] == 1
        assert result["single_stage_count"] == 1
        assert result["multi_stage_percentage"] == 0.0

        # Check command type distribution
        types = result["command_type_distribution"]
        assert len(types) == 1
        assert types[0]["type"] == "test"

    def test_pack_with_multi_stage_verification(self):
        """Verify pack with multiple verification types."""
        result = analyze_pack_verification_command_diversity([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/ && mypy src/ && ruff check src/"
            }
        ])

        assert result["packs_with_verification"] == 1
        assert result["single_stage_count"] == 0
        assert result["multi_stage_percentage"] == 100.0
        assert result["avg_commands_per_pack"] == 3.0

        # Should have test, typecheck, and lint types
        types = {t["type"] for t in result["command_type_distribution"]}
        assert "test" in types
        assert "typecheck" in types
        assert "lint" in types

    def test_unique_commands_tracking(self):
        """Verify unique command counting."""
        result = analyze_pack_verification_command_diversity([
            {"pack_id": "pack1", "verification_command": "pytest tests/"},
            {"pack_id": "pack2", "verification_command": "pytest tests/"},
            {"pack_id": "pack3", "verification_command": "mypy src/"},
        ])

        assert result["unique_commands"] == 2  # pytest and mypy

    def test_command_type_distribution(self):
        """Verify command type distribution calculation."""
        result = analyze_pack_verification_command_diversity([
            {"pack_id": "pack1", "verification_command": "pytest tests/"},
            {"pack_id": "pack2", "verification_command": "pytest tests/ && mypy src/"},
            {"pack_id": "pack3", "verification_command": "ruff check src/"},
        ])

        types = {t["type"]: t for t in result["command_type_distribution"]}

        assert "test" in types
        assert types["test"]["count"] == 2  # 2 pytest commands
        assert "typecheck" in types
        assert types["typecheck"]["count"] == 1  # 1 mypy command
        assert "lint" in types
        assert types["lint"]["count"] == 1  # 1 ruff command

    def test_file_coverage_calculation(self):
        """Verify file coverage calculation."""
        result = analyze_pack_verification_command_diversity([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/test_file1.py tests/test_file2.py",
                "expected_files": [
                    "tests/test_file1.py",
                    "tests/test_file2.py",
                    "tests/test_file3.py",
                ]
            }
        ])

        # 2 out of 3 files covered = 66.67%
        assert result["avg_file_coverage"] == 66.67

    def test_success_by_diversity_correlation(self):
        """Verify success rate tracking by diversity level."""
        result = analyze_pack_verification_command_diversity([
            # Diversity 0 (no verification)
            {"pack_id": "pack1", "verification_command": "", "success": False},
            # Diversity 1 (single type)
            {"pack_id": "pack2", "verification_command": "pytest tests/", "success": True},
            {"pack_id": "pack3", "verification_command": "mypy src/", "success": False},
            # Diversity 2 (two types)
            {"pack_id": "pack4", "verification_command": "pytest tests/ && mypy src/", "success": True},
        ])

        diversity = {d["diversity_level"]: d for d in result["success_by_diversity"]}

        assert 0 in diversity
        assert diversity[0]["success_rate"] == 0.0

        assert 1 in diversity
        assert diversity[1]["total_packs"] == 2
        assert diversity[1]["success_rate"] == 50.0

        assert 2 in diversity
        assert diversity[2]["success_rate"] == 100.0

    def test_weak_verification_detection(self):
        """Verify detection of weak verification strategies."""
        result = analyze_pack_verification_command_diversity([
            {"pack_id": "pack1", "verification_command": ""},
            {"pack_id": "pack2", "verification_command": "echo ok"},
        ])

        weak = result["weak_verification_packs"]
        assert len(weak) == 2

        pack_ids = {p["pack_id"] for p in weak}
        assert "pack1" in pack_ids
        assert "pack2" in pack_ids

    def test_common_command_patterns(self):
        """Verify tracking of common command type combinations."""
        result = analyze_pack_verification_command_diversity([
            {"pack_id": "pack1", "verification_command": "pytest tests/ && mypy src/"},
            {"pack_id": "pack2", "verification_command": "pytest tests/ && mypy src/"},
            {"pack_id": "pack3", "verification_command": "pytest tests/"},
        ])

        patterns = result["common_command_patterns"]
        assert len(patterns) >= 1

        # Most common should be test+typecheck
        top_pattern = patterns[0]
        assert sorted(top_pattern["types"]) == ["test", "typecheck"]
        assert top_pattern["count"] == 2

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_verification_command_diversity([
            "not a dict",
            {"pack_id": "pack1", "verification_command": "pytest tests/"}
        ])

        assert result["total_packs"] == 1

    def test_missing_pack_id_uses_unknown(self):
        """Verify missing pack_id defaults to unknown."""
        result = analyze_pack_verification_command_diversity([
            {"verification_command": ""}
        ])

        assert result["no_verification_count"] == 1
        weak = result["weak_verification_packs"]
        assert weak[0]["pack_id"] == "unknown"

    def test_command_splitting_on_and_operator(self):
        """Verify commands are split on && operator."""
        result = analyze_pack_verification_command_diversity([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/ && mypy src/ && ruff check"
            }
        ])

        assert result["avg_commands_per_pack"] == 3.0

    def test_command_splitting_on_semicolon(self):
        """Verify commands are split on semicolon."""
        result = analyze_pack_verification_command_diversity([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/; mypy src/"
            }
        ])

        assert result["avg_commands_per_pack"] == 2.0

    def test_build_command_classification(self):
        """Verify build commands are classified correctly."""
        result = analyze_pack_verification_command_diversity([
            {"pack_id": "pack1", "verification_command": "npm run build"}
        ])

        types = result["command_type_distribution"]
        assert types[0]["type"] == "build"

    def test_lint_command_classification(self):
        """Verify lint commands are classified correctly."""
        result = analyze_pack_verification_command_diversity([
            {"pack_id": "pack1", "verification_command": "eslint src/"},
            {"pack_id": "pack2", "verification_command": "black src/"},
        ])

        types = {t["type"] for t in result["command_type_distribution"]}
        assert "lint" in types

    def test_unclassified_command_marked_as_other(self):
        """Verify unrecognized commands are marked as other."""
        result = analyze_pack_verification_command_diversity([
            {"pack_id": "pack1", "verification_command": "custom-verify-script"}
        ])

        types = result["command_type_distribution"]
        assert types[0]["type"] == "other"

    def test_weak_verification_packs_limited_to_20(self):
        """Verify weak packs list is limited to 20 examples."""
        packs = [
            {"pack_id": f"pack{i}", "verification_command": ""}
            for i in range(30)
        ]
        result = analyze_pack_verification_command_diversity(packs)

        assert len(result["weak_verification_packs"]) == 20

    def test_common_patterns_limited_to_10(self):
        """Verify common patterns list is limited to 10."""
        packs = [
            {"pack_id": f"pack{i}", "verification_command": f"tool{i} check"}
            for i in range(15)
        ]
        result = analyze_pack_verification_command_diversity(packs)

        assert len(result["common_command_patterns"]) <= 10


class TestClassifyCommand:
    """Test command classification helper."""

    def test_pytest_classified_as_test(self):
        """Verify pytest is classified as test."""
        assert _classify_command("pytest tests/") == "test"

    def test_jest_classified_as_test(self):
        """Verify jest is classified as test."""
        assert _classify_command("jest --coverage") == "test"

    def test_mypy_classified_as_typecheck(self):
        """Verify mypy is classified as typecheck."""
        assert _classify_command("mypy src/") == "typecheck"

    def test_tsc_classified_as_typecheck(self):
        """Verify tsc is classified as typecheck."""
        assert _classify_command("tsc --noEmit") == "typecheck"

    def test_ruff_classified_as_lint(self):
        """Verify ruff is classified as lint."""
        assert _classify_command("ruff check src/") == "lint"

    def test_eslint_classified_as_lint(self):
        """Verify eslint is classified as lint."""
        assert _classify_command("eslint src/") == "lint"

    def test_npm_build_classified_as_build(self):
        """Verify npm build is classified as build."""
        assert _classify_command("npm run build") == "build"

    def test_make_classified_as_build(self):
        """Verify make is classified as build."""
        assert _classify_command("make all") == "build"

    def test_unknown_command_classified_as_other(self):
        """Verify unknown commands are classified as other."""
        assert _classify_command("custom-script --verify") == "other"

    def test_case_insensitive_classification(self):
        """Verify classification is case-insensitive."""
        assert _classify_command("PYTEST tests/") == "test"
        assert _classify_command("MyPy src/") == "typecheck"


class TestSplitCommands:
    """Test command splitting helper."""

    def test_single_command_returns_list_with_one_element(self):
        """Verify single command returns one-element list."""
        result = _split_commands("pytest tests/")
        assert result == ["pytest tests/"]

    def test_commands_split_on_double_ampersand(self):
        """Verify commands are split on &&."""
        result = _split_commands("pytest tests/ && mypy src/")
        assert result == ["pytest tests/", "mypy src/"]

    def test_commands_split_on_semicolon(self):
        """Verify commands are split on semicolon."""
        result = _split_commands("pytest tests/; mypy src/")
        assert result == ["pytest tests/", "mypy src/"]

    def test_whitespace_stripped_from_split_commands(self):
        """Verify whitespace is stripped from split commands."""
        result = _split_commands("pytest tests/  &&  mypy src/  ")
        assert result == ["pytest tests/", "mypy src/"]

    def test_empty_command_returns_empty_list(self):
        """Verify empty command returns empty list."""
        result = _split_commands("")
        assert result == []

    def test_command_with_only_separators_filtered(self):
        """Verify empty parts are filtered out."""
        result = _split_commands("pytest tests/ && && mypy src/")
        assert result == ["pytest tests/", "mypy src/"]


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_comprehensive_verification_pack(self):
        """Simulate pack with comprehensive verification."""
        result = analyze_pack_verification_command_diversity([
            {
                "pack_id": "comprehensive",
                "verification_command": "pytest tests/ --cov && mypy src/ && ruff check src/ && npm run build",
                "expected_files": ["src/analyzer.py", "tests/test_analyzer.py"],
                "success": True,
            }
        ])

        assert result["multi_stage_percentage"] == 100.0
        assert result["single_stage_count"] == 0

        types = {t["type"] for t in result["command_type_distribution"]}
        assert len(types) == 4  # test, typecheck, lint, build

    def test_single_stage_verification_pack(self):
        """Simulate pack with only test verification."""
        result = analyze_pack_verification_command_diversity([
            {
                "pack_id": "single_stage",
                "verification_command": "pytest tests/",
                "success": True,
            }
        ])

        assert result["single_stage_count"] == 1
        assert result["multi_stage_percentage"] == 0.0

    def test_weak_verification_pack(self):
        """Simulate pack with weak verification."""
        result = analyze_pack_verification_command_diversity([
            {
                "pack_id": "weak",
                "verification_command": "echo 'done'",
                "success": False,
            }
        ])

        weak = result["weak_verification_packs"]
        assert len(weak) == 1
        assert weak[0]["reason"] == "Weak verification strategy"

    def test_correlation_between_diversity_and_success(self):
        """Simulate correlation between verification diversity and success."""
        result = analyze_pack_verification_command_diversity([
            # No verification - fails
            {"pack_id": "p1", "verification_command": "", "success": False},
            {"pack_id": "p2", "verification_command": "", "success": False},
            # Single stage - mixed results
            {"pack_id": "p3", "verification_command": "pytest tests/", "success": True},
            {"pack_id": "p4", "verification_command": "mypy src/", "success": False},
            # Multi-stage - mostly succeeds
            {"pack_id": "p5", "verification_command": "pytest tests/ && mypy src/", "success": True},
            {"pack_id": "p6", "verification_command": "pytest tests/ && ruff check", "success": True},
        ])

        diversity = {d["diversity_level"]: d for d in result["success_by_diversity"]}

        # Higher diversity should correlate with higher success
        assert diversity[0]["success_rate"] == 0.0
        assert diversity[1]["success_rate"] == 50.0
        assert diversity[2]["success_rate"] == 100.0

    def test_multiple_packs_same_verification(self):
        """Simulate multiple packs using same verification strategy."""
        result = analyze_pack_verification_command_diversity([
            {"pack_id": "p1", "verification_command": "pytest tests/ -v"},
            {"pack_id": "p2", "verification_command": "pytest tests/ -v"},
            {"pack_id": "p3", "verification_command": "pytest tests/ -v"},
        ])

        assert result["unique_commands"] == 1
        assert result["avg_commands_per_pack"] == 1.0

        patterns = result["common_command_patterns"]
        assert patterns[0]["count"] == 3

    def test_file_coverage_partial(self):
        """Simulate partial file coverage by verification."""
        result = analyze_pack_verification_command_diversity([
            {
                "pack_id": "partial",
                "verification_command": "pytest tests/test_analyzer.py",
                "expected_files": [
                    "src/analyzer.py",
                    "tests/test_analyzer.py",
                    "src/utils.py",
                    "tests/test_utils.py",
                ]
            }
        ])

        # Only test_analyzer.py is in command, so 1/4 = 25%
        assert result["avg_file_coverage"] == 25.0

    def test_no_success_field_handled_gracefully(self):
        """Verify packs without success field don't cause errors."""
        result = analyze_pack_verification_command_diversity([
            {"pack_id": "p1", "verification_command": "pytest tests/"},
            {"pack_id": "p2", "verification_command": ""},
        ])

        # Should not crash, but won't contribute to success metrics
        assert result["total_packs"] == 2
