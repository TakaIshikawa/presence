"""Tests for pack task description clarity analyzer."""

import pytest

from synthesis.pack_task_description_clarity import analyze_pack_task_description_clarity


class TestAnalyzePackTaskDescriptionClarity:
    """Test main analyzer function."""

    def test_empty_records_returns_zeroed_metrics(self):
        """Verify empty records returns zero metrics."""
        result = analyze_pack_task_description_clarity([])

        assert result["total_tasks"] == 0
        assert result["has_specific_files_count"] == 0
        assert result["has_acceptance_criteria_count"] == 0
        assert result["avg_verb_clarity_score"] == 0.0
        assert result["avg_scope_boundedness"] == 0.0
        assert result["ambiguity_flag_count"] == 0
        assert result["tasks_with_ambiguity"] == 0
        assert result["clear_task_count"] == 0
        assert result["vague_task_count"] == 0
        assert result["red_flag_task_count"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_task_description_clarity(None)
        assert result["total_tasks"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_task_description_clarity("not a list")

    def test_clear_task_with_specific_files_and_acs(self):
        """Verify clear task with specific files and ACs."""
        result = analyze_pack_task_description_clarity([
            {
                "task_id": "task1",
                "prompt": "Create analyzer in src/synthesis/analyzer.py with acceptance criteria: all tests pass",
                "expected_files": ["src/synthesis/analyzer.py"],
                "acceptance_criteria": ["All tests pass", "Code follows patterns"],
            }
        ])

        assert result["has_specific_files_count"] == 1
        assert result["has_acceptance_criteria_count"] == 1
        assert result["avg_verb_clarity_score"] == 100.0  # "create" is clear verb
        assert result["clear_task_count"] == 1
        assert result["vague_task_count"] == 0

    def test_vague_task_without_files_or_acs(self):
        """Verify vague task detection."""
        result = analyze_pack_task_description_clarity([
            {
                "task_id": "task1",
                "prompt": "Improve the system and enhance various components to make them better",
            }
        ])

        assert result["has_specific_files_count"] == 0
        assert result["has_acceptance_criteria_count"] == 0
        assert result["ambiguity_flag_count"] >= 3  # improve, enhance, various, better
        assert result["tasks_with_ambiguity"] == 1
        assert result["vague_task_count"] == 1

    def test_red_flag_task_with_multiple_ambiguities(self):
        """Verify red flag detection for tasks with multiple ambiguities."""
        result = analyze_pack_task_description_clarity([
            {
                "task_id": "task1",
                "prompt": "Optimize various components to improve performance and enhance several modules",
            }
        ])

        assert result["ambiguity_flag_count"] >= 3
        assert result["red_flag_task_count"] == 1

    def test_verb_clarity_clear_verbs(self):
        """Verify clear imperative verbs score highly."""
        clear_verbs = ["create", "implement", "add", "fix", "remove", "update", "refactor"]

        for verb in clear_verbs:
            result = analyze_pack_task_description_clarity([
                {"task_id": "task1", "prompt": f"{verb} the authentication module"}
            ])
            assert result["avg_verb_clarity_score"] == 100.0, f"Verb '{verb}' should score 100"

    def test_verb_clarity_weak_verbs(self):
        """Verify weak verbs score moderately."""
        result = analyze_pack_task_description_clarity([
            {"task_id": "task1", "prompt": "improve the authentication module"}
        ])

        assert result["avg_verb_clarity_score"] == 50.0

    def test_verb_clarity_no_clear_verb(self):
        """Verify tasks without clear verbs score low."""
        result = analyze_pack_task_description_clarity([
            {"task_id": "task1", "prompt": "the authentication module needs attention"}
        ])

        assert result["avg_verb_clarity_score"] == 0.0

    def test_scope_boundedness_short_focused_prompt(self):
        """Verify short focused prompts score highly."""
        result = analyze_pack_task_description_clarity([
            {"task_id": "task1", "prompt": "Fix login bug"}
        ])

        assert result["avg_scope_boundedness"] > 60.0

    def test_scope_boundedness_long_unfocused_prompt(self):
        """Verify long unfocused prompts score low."""
        long_prompt = "Update the entire system and also improve all components and additionally enhance the application architecture and furthermore optimize the codebase and refactor various modules" * 3

        result = analyze_pack_task_description_clarity([
            {"task_id": "task1", "prompt": long_prompt}
        ])

        assert result["avg_scope_boundedness"] < 40.0

    def test_specific_files_detected_in_expected_files(self):
        """Verify specific files detected from expected_files list."""
        result = analyze_pack_task_description_clarity([
            {
                "task_id": "task1",
                "prompt": "Add analyzer",
                "expected_files": ["src/main.py", "tests/test_main.py"],
            }
        ])

        assert result["has_specific_files_count"] == 1

    def test_specific_files_detected_in_prompt(self):
        """Verify specific files detected from prompt text."""
        result = analyze_pack_task_description_clarity([
            {
                "task_id": "task1",
                "prompt": "Update src/synthesis/analyzer.py to add new feature",
            }
        ])

        assert result["has_specific_files_count"] == 1

    def test_acceptance_criteria_detected_in_list(self):
        """Verify ACs detected from acceptance_criteria list."""
        result = analyze_pack_task_description_clarity([
            {
                "task_id": "task1",
                "prompt": "Add feature",
                "acceptance_criteria": ["Tests pass", "Types check"],
            }
        ])

        assert result["has_acceptance_criteria_count"] == 1

    def test_acceptance_criteria_detected_in_prompt(self):
        """Verify ACs detected from prompt keywords."""
        result = analyze_pack_task_description_clarity([
            {
                "task_id": "task1",
                "prompt": "Add feature. Acceptance criteria: all tests must pass and types should verify correctly",
            }
        ])

        assert result["has_acceptance_criteria_count"] == 1

    def test_ambiguity_terms_tracked(self):
        """Verify common ambiguity terms are tracked."""
        result = analyze_pack_task_description_clarity([
            {"task_id": "task1", "prompt": "Improve various components"},
            {"task_id": "task2", "prompt": "Enhance various modules"},
            {"task_id": "task3", "prompt": "Optimize various systems"},
        ])

        assert len(result["common_ambiguity_terms"]) > 0
        # "various" should appear 3 times
        various_term = next((t for t in result["common_ambiguity_terms"] if t["term"] == "various"), None)
        assert various_term is not None
        assert various_term["count"] == 3

    def test_common_ambiguity_terms_limited_to_five(self):
        """Verify common ambiguity terms list is capped at 5."""
        prompts = [
            "improve some things",
            "enhance several items",
            "optimize various components",
            "clean up multiple files",
            "polish many modules",
            "better good code",
            "nice clean implementation",
        ]

        result = analyze_pack_task_description_clarity([
            {"task_id": f"task{i}", "prompt": prompt}
            for i, prompt in enumerate(prompts)
        ])

        assert len(result["common_ambiguity_terms"]) <= 5

    def test_mixed_clear_and_vague_tasks(self):
        """Verify mixed task clarity classification."""
        result = analyze_pack_task_description_clarity([
            {
                "task_id": "task1",
                "prompt": "Create src/main.py with acceptance criteria",
                "expected_files": ["src/main.py"],
                "acceptance_criteria": ["Tests pass"],
            },
            {
                "task_id": "task2",
                "prompt": "Improve various things to make them better",
            },
        ])

        assert result["total_tasks"] == 2
        assert result["clear_task_count"] == 1
        assert result["vague_task_count"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_task_description_clarity([
            "not a dict",
            {"task_id": "task1", "prompt": "Create analyzer"},
        ])

        assert result["total_tasks"] == 1

    def test_empty_prompt_handled(self):
        """Verify empty prompts are handled gracefully."""
        result = analyze_pack_task_description_clarity([
            {"task_id": "task1", "prompt": ""},
            {"task_id": "task2", "prompt": "   "},
        ])

        assert result["total_tasks"] == 2
        assert result["avg_verb_clarity_score"] == 0.0

    def test_scope_boundedness_with_specific_targets(self):
        """Verify specific targets improve scope score."""
        result = analyze_pack_task_description_clarity([
            {"task_id": "task1", "prompt": "Update function calculateTotal in module utils"},
        ])

        assert result["avg_scope_boundedness"] > 60.0

    def test_scope_boundedness_with_vague_targets(self):
        """Verify vague targets decrease scope score."""
        result = analyze_pack_task_description_clarity([
            {"task_id": "task1", "prompt": "Update the entire system and all components in the codebase"},
        ])

        assert result["avg_scope_boundedness"] < 40.0

    def test_ambiguity_detection_comprehensive(self):
        """Verify all ambiguity categories are detected."""
        result = analyze_pack_task_description_clarity([
            {"task_id": "task1", "prompt": "Improve and enhance various components to make them better and cleaner"},
        ])

        # Should detect: improve, enhance, various, better, clean
        assert result["ambiguity_flag_count"] >= 4
        assert result["tasks_with_ambiguity"] == 1

    def test_case_insensitivity_in_pattern_matching(self):
        """Verify pattern matching is case-insensitive."""
        result = analyze_pack_task_description_clarity([
            {"task_id": "task1", "prompt": "CREATE analyzer with ACCEPTANCE CRITERIA"},
        ])

        assert result["avg_verb_clarity_score"] == 100.0
        assert result["has_acceptance_criteria_count"] == 1

    def test_file_pattern_detection_various_formats(self):
        """Verify file path detection for various formats."""
        test_cases = [
            "src/main.py",
            "tests/test_analyzer.py",
            "lib/utils.js",
            "config.json",
            "README.md",
        ]

        for file_path in test_cases:
            result = analyze_pack_task_description_clarity([
                {"task_id": "task1", "prompt": f"Update {file_path}"}
            ])
            assert result["has_specific_files_count"] == 1, f"Should detect {file_path}"

    def test_zero_denominator_in_averages(self):
        """Verify zero denominator in average calculations."""
        result = analyze_pack_task_description_clarity([])

        assert result["avg_verb_clarity_score"] == 0.0
        assert result["avg_scope_boundedness"] == 0.0

    def test_optimal_task_description_pattern(self):
        """Verify optimal task description pattern."""
        result = analyze_pack_task_description_clarity([
            {
                "task_id": "task1",
                "prompt": "Implement authentication in src/auth.py. Must include unit tests and type checking.",
                "expected_files": ["src/auth.py", "tests/test_auth.py"],
                "acceptance_criteria": ["All tests pass", "Types validate", "Coverage > 90%"],
            }
        ])

        assert result["has_specific_files_count"] == 1
        assert result["has_acceptance_criteria_count"] == 1
        assert result["avg_verb_clarity_score"] == 100.0
        assert result["ambiguity_flag_count"] == 0
        assert result["clear_task_count"] == 1
        assert result["vague_task_count"] == 0
        assert result["red_flag_task_count"] == 0

    def test_anti_pattern_vague_unbounded_description(self):
        """Verify anti-pattern detection for vague unbounded descriptions."""
        result = analyze_pack_task_description_clarity([
            {
                "task_id": "task1",
                "prompt": "Improve the entire application by enhancing various components and optimizing multiple systems to make them better and cleaner",
            }
        ])

        assert result["has_specific_files_count"] == 0
        assert result["has_acceptance_criteria_count"] == 0
        assert result["ambiguity_flag_count"] >= 4
        assert result["vague_task_count"] == 1
        assert result["red_flag_task_count"] == 1
