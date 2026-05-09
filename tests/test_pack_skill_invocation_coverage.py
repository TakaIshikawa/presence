"""Tests for pack Skill invocation coverage analyzer."""

import pytest

from synthesis.pack_skill_invocation_coverage import analyze_pack_skill_invocation


class TestAnalyzePackSkillInvocation:
    """Test main analyzer function."""

    def test_empty_events_returns_perfect_discipline(self):
        """Verify empty event list returns perfect discipline scores."""
        result = analyze_pack_skill_invocation([])

        assert result["total_skill_opportunities"] == 0
        assert result["skills_invoked"] == 0
        assert result["skills_available"] == 0
        assert result["skill_invocation_rate"] == 0.0
        assert result["correct_skill_selections"] == 0
        assert result["skill_matching_accuracy"] == 0.0
        assert result["user_invocable_recognized"] == 0
        assert result["recognition_rate"] == 0.0
        assert result["manual_implementations"] == 0
        assert result["skill_vs_manual_ratio"] == 1.0
        assert result["skills_mentioned_not_invoked"] == 0
        assert result["discipline_score"] == 1.0
        assert result["common_skills_used"] == {}
        assert result["missed_opportunities"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_skill_invocation(None)
        assert result["total_skill_opportunities"] == 0
        assert result["discipline_score"] == 1.0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_skill_invocation("not a list")

    def test_skill_correctly_invoked(self):
        """Verify pack that correctly invokes skills."""
        result = analyze_pack_skill_invocation([
            {
                "event_type": "skill_available",
                "skill_name": "commit",
            },
            {
                "event_type": "skill_opportunity",
                "skill_name": "commit",
                "was_invoked": True,
                "correct_skill": True,
            },
            {
                "event_type": "user_invocable_skill",
                "skill_name": "commit",
                "was_invoked": True,
            },
        ])

        assert result["total_skill_opportunities"] == 1
        assert result["skills_invoked"] == 1
        assert result["skills_available"] == 1
        assert result["skill_invocation_rate"] == 100.0
        assert result["correct_skill_selections"] == 1
        assert result["skill_matching_accuracy"] == 100.0
        assert result["common_skills_used"]["commit"] == 1
        assert result["discipline_score"] == 1.0

    def test_missed_skill_opportunities(self):
        """Verify pack that misses skill opportunities."""
        result = analyze_pack_skill_invocation([
            {
                "event_type": "skill_available",
                "skill_name": "commit",
            },
            {
                "event_type": "skill_opportunity",
                "skill_name": "commit",
                "was_invoked": False,
                "manual_implementation": True,
            },
            {
                "event_type": "skill_opportunity",
                "skill_name": "review-pr",
                "was_invoked": False,
                "manual_implementation": True,
            },
        ])

        assert result["total_skill_opportunities"] == 2
        assert result["skills_invoked"] == 0
        assert result["skill_invocation_rate"] == 0.0
        assert result["manual_implementations"] == 2
        assert result["missed_opportunities"] == 2
        assert result["skills_mentioned_not_invoked"] == 2
        # 0 skills vs 2 manual = 0.0 ratio
        assert result["skill_vs_manual_ratio"] == 0.0
        # Low discipline due to missed opportunities
        assert result["discipline_score"] <= 0.3

    def test_wrong_skill_selection(self):
        """Verify detection of incorrect skill selection."""
        result = analyze_pack_skill_invocation([
            {
                "event_type": "skill_opportunity",
                "skill_name": "commit",
                "was_invoked": True,
                "correct_skill": False,  # Wrong skill used
            },
        ])

        assert result["skills_invoked"] == 1
        assert result["correct_skill_selections"] == 0
        assert result["skill_matching_accuracy"] == 0.0

    def test_user_invocable_skill_recognition(self):
        """Verify tracking of user-invocable skill recognition."""
        result = analyze_pack_skill_invocation([
            {
                "event_type": "user_invocable_skill",
                "skill_name": "commit",
                "was_invoked": True,
            },
            {
                "event_type": "user_invocable_skill",
                "skill_name": "review-pr",
                "was_invoked": True,
            },
            {
                "event_type": "user_invocable_skill",
                "skill_name": "cache",
                "was_invoked": False,
            },
        ])

        assert result["user_invocable_recognized"] == 2
        # 2 out of 3 user-invocable skills recognized = 66.67%
        assert result["recognition_rate"] == 66.67

    def test_mixed_skill_and_manual_work(self):
        """Verify calculation of skill vs manual ratio."""
        result = analyze_pack_skill_invocation([
            # 3 skill invocations
            {
                "event_type": "skill_opportunity",
                "skill_name": "commit",
                "was_invoked": True,
            },
            {
                "event_type": "skill_opportunity",
                "skill_name": "review-pr",
                "was_invoked": True,
            },
            {
                "event_type": "skill_opportunity",
                "skill_name": "cache",
                "was_invoked": True,
            },
            # 1 manual implementation
            {
                "event_type": "skill_opportunity",
                "skill_name": "verify",
                "was_invoked": False,
                "manual_implementation": True,
            },
        ])

        assert result["skills_invoked"] == 3
        assert result["manual_implementations"] == 1
        # 3 / (3 + 1) = 0.75
        assert result["skill_vs_manual_ratio"] == 0.75

    def test_skill_usage_tracking(self):
        """Verify tracking of common skills used."""
        result = analyze_pack_skill_invocation([
            {"event_type": "skill_invoked", "skill_name": "commit"},
            {"event_type": "skill_invoked", "skill_name": "commit"},
            {"event_type": "skill_invoked", "skill_name": "commit"},
            {"event_type": "skill_invoked", "skill_name": "review-pr"},
            {"event_type": "skill_invoked", "skill_name": "review-pr"},
            {"event_type": "skill_invoked", "skill_name": "cache"},
        ])

        assert result["common_skills_used"]["commit"] == 3
        assert result["common_skills_used"]["review-pr"] == 2
        assert result["common_skills_used"]["cache"] == 1

    def test_discipline_score_perfect(self):
        """Verify perfect discipline score calculation."""
        result = analyze_pack_skill_invocation([
            {
                "event_type": "skill_opportunity",
                "was_invoked": True,
                "correct_skill": True,
            },
            {
                "event_type": "skill_opportunity",
                "was_invoked": True,
                "correct_skill": True,
            },
            # Add user-invocable recognition
            {
                "event_type": "user_invocable_skill",
                "skill_name": "commit",
                "was_invoked": True,
            },
        ])

        # 100% invocation, 100% accuracy, 100% recognition, 100% skill ratio
        # discipline = 0.4 * 1.0 + 0.25 * 1.0 + 0.2 * 1.0 + 0.15 * 1.0 = 1.0
        assert result["discipline_score"] == 1.0

    def test_discipline_score_components(self):
        """Verify discipline score with all components."""
        result = analyze_pack_skill_invocation([
            # Invocation: 3/4 = 75%
            {"event_type": "skill_opportunity", "was_invoked": True, "correct_skill": True, "skill_name": "s1"},
            {"event_type": "skill_opportunity", "was_invoked": True, "correct_skill": True, "skill_name": "s2"},
            {"event_type": "skill_opportunity", "was_invoked": True, "correct_skill": False, "skill_name": "s3"},
            {"event_type": "skill_opportunity", "was_invoked": False, "manual_implementation": True},
            # Recognition: 2/2 = 100%
            {"event_type": "user_invocable_skill", "skill_name": "u1", "was_invoked": True},
            {"event_type": "user_invocable_skill", "skill_name": "u2", "was_invoked": True},
        ])

        # invocation_rate = 75% → 0.75
        # matching_accuracy = 2/3 = 66.67% → 0.67
        # recognition_rate = 100% → 1.0
        # skill_vs_manual = 3/(3+1) = 0.75
        # discipline = 0.4*0.75 + 0.25*0.67 + 0.2*1.0 + 0.15*0.75
        #            = 0.3 + 0.1675 + 0.2 + 0.1125 = 0.78
        assert result["discipline_score"] == 0.78

    def test_available_skills_tracking(self):
        """Verify tracking of available skills."""
        result = analyze_pack_skill_invocation([
            {"event_type": "skill_available", "skill_name": "commit"},
            {"event_type": "skill_available", "skill_name": "review-pr"},
            {"event_type": "skill_available", "skill_name": "cache"},
            {"event_type": "skill_available", "skill_name": "verify"},
            {"event_type": "skill_available", "skill_name": "commit"},  # Duplicate
        ])

        # Should count unique skills
        assert result["skills_available"] == 4

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_skill_invocation([
            "not a dict",
            {
                "event_type": "skill_invoked",
                "skill_name": "commit",
            },
            None,
        ])

        assert result["common_skills_used"]["commit"] == 1

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_pack_skill_invocation([
            {
                "event_type": "skill_opportunity",
                # Missing most fields
            },
        ])

        assert result["total_skill_opportunities"] == 1
        assert result["skills_invoked"] == 0

    def test_skill_mentioned_not_invoked(self):
        """Verify tracking of skills mentioned but not invoked."""
        result = analyze_pack_skill_invocation([
            {
                "event_type": "skill_opportunity",
                "skill_name": "commit",
                "was_invoked": False,
            },
            {
                "event_type": "skill_opportunity",
                "skill_name": "review-pr",
                "was_invoked": False,
            },
        ])

        assert result["skills_mentioned_not_invoked"] == 2

    def test_comprehensive_pack_all_metrics(self):
        """Verify comprehensive pack with all metrics populated."""
        result = analyze_pack_skill_invocation([
            # Available skills
            {"event_type": "skill_available", "skill_name": "commit"},
            {"event_type": "skill_available", "skill_name": "review-pr"},
            # Invoked correctly
            {
                "event_type": "skill_opportunity",
                "skill_name": "commit",
                "was_invoked": True,
                "correct_skill": True,
            },
            # Missed opportunity
            {
                "event_type": "skill_opportunity",
                "skill_name": "review-pr",
                "was_invoked": False,
                "manual_implementation": True,
            },
            # User-invocable recognition
            {
                "event_type": "user_invocable_skill",
                "skill_name": "cache",
                "was_invoked": True,
            },
        ])

        assert result["skills_available"] == 2
        assert result["total_skill_opportunities"] == 2
        assert result["skills_invoked"] == 1
        assert result["skill_invocation_rate"] == 50.0
        assert result["correct_skill_selections"] == 1
        assert result["manual_implementations"] == 1
        assert result["missed_opportunities"] == 1
        assert result["user_invocable_recognized"] == 1
        assert result["common_skills_used"]["commit"] == 1

    def test_zero_opportunities_perfect_rate(self):
        """Verify zero opportunities results in 0% invocation rate."""
        result = analyze_pack_skill_invocation([
            {"event_type": "skill_available", "skill_name": "commit"},
        ])

        # No opportunities, so rate is 0%
        assert result["skill_invocation_rate"] == 0.0

    def test_all_skills_correct(self):
        """Verify 100% matching accuracy when all correct."""
        result = analyze_pack_skill_invocation([
            {
                "event_type": "skill_opportunity",
                "was_invoked": True,
                "correct_skill": True,
            },
            {
                "event_type": "skill_opportunity",
                "was_invoked": True,
                "correct_skill": True,
            },
        ])

        # All invoked skills are correct
        assert result["skill_matching_accuracy"] == 100.0

    def test_no_user_invocable_skills(self):
        """Verify recognition rate when no user-invocable skills."""
        result = analyze_pack_skill_invocation([
            {"event_type": "skill_opportunity", "was_invoked": True},
        ])

        # No user-invocable skills, so rate is 0%
        assert result["recognition_rate"] == 0.0

    def test_skill_vs_manual_all_manual(self):
        """Verify skill vs manual ratio when all manual."""
        result = analyze_pack_skill_invocation([
            {
                "event_type": "skill_opportunity",
                "was_invoked": False,
                "manual_implementation": True,
            },
            {
                "event_type": "skill_opportunity",
                "was_invoked": False,
                "manual_implementation": True,
            },
        ])

        # 0 skills / 2 manual = 0.0
        assert result["skill_vs_manual_ratio"] == 0.0

    def test_skill_vs_manual_all_skills(self):
        """Verify skill vs manual ratio when all skills."""
        result = analyze_pack_skill_invocation([
            {
                "event_type": "skill_opportunity",
                "was_invoked": True,
            },
            {
                "event_type": "skill_opportunity",
                "was_invoked": True,
            },
        ])

        # 2 skills / 0 manual = 1.0
        assert result["skill_vs_manual_ratio"] == 1.0

    def test_non_string_skill_names_not_tracked(self):
        """Verify non-string skill names are not tracked."""
        result = analyze_pack_skill_invocation([
            {"event_type": "skill_invoked", "skill_name": 123},
            {"event_type": "skill_invoked", "skill_name": None},
            {"event_type": "skill_invoked", "skill_name": "commit"},
        ])

        assert result["common_skills_used"] == {"commit": 1}

    def test_duplicate_skill_availability(self):
        """Verify duplicate available skills counted separately."""
        result = analyze_pack_skill_invocation([
            {"event_type": "skill_available", "skill_name": "commit"},
            {"event_type": "skill_available", "skill_name": "commit"},
            {"event_type": "skill_available", "skill_name": "commit"},
        ])

        # Unique skills = 1
        assert result["skills_available"] == 1

    def test_high_invocation_rate(self):
        """Verify high invocation rate calculation."""
        result = analyze_pack_skill_invocation([
            {"event_type": "skill_opportunity", "was_invoked": True},
            {"event_type": "skill_opportunity", "was_invoked": True},
            {"event_type": "skill_opportunity", "was_invoked": True},
            {"event_type": "skill_opportunity", "was_invoked": True},
            {"event_type": "skill_opportunity", "was_invoked": False},
        ])

        # 4/5 = 80%
        assert result["skill_invocation_rate"] == 80.0

    def test_low_discipline_score(self):
        """Verify low discipline score with poor metrics."""
        result = analyze_pack_skill_invocation([
            # All opportunities missed
            {
                "event_type": "skill_opportunity",
                "was_invoked": False,
                "manual_implementation": True,
            },
            {
                "event_type": "skill_opportunity",
                "was_invoked": False,
                "manual_implementation": True,
            },
        ])

        # invocation = 0%, accuracy = 0%, recognition = 0%, skill_ratio = 0.0
        # discipline = 0
        assert result["discipline_score"] == 0.0
