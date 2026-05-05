"""Tests for idea realization lag analysis."""

import pytest
from datetime import datetime, timedelta, timezone

from synthesis.idea_realization_lag import (
    IdeaRealizationChain,
    LagDistribution,
    PipelineBottleneck,
    RealizationVelocity,
    IdeaRealizationLagAnalysis,
    analyze_idea_realization_lag,
    _calculate_lag_distribution,
    _calculate_realization_velocity,
    _calculate_tier_counts,
    _classify_lag_tier,
    TIER_FAST,
    TIER_NORMAL,
    TIER_SLOW,
    TIER_STALLED,
    TIER_ORPHANED,
    THRESHOLD_FAST_DAYS,
    THRESHOLD_NORMAL_DAYS,
    THRESHOLD_SLOW_DAYS,
)


class TestIdeaRealizationChain:
    """Test IdeaRealizationChain dataclass."""

    def test_create_published_chain(self):
        """Verify chain can be created for published idea."""
        captured = datetime.now(timezone.utc)
        published = captured + timedelta(days=10)
        chain = IdeaRealizationChain(
            idea_id="idea-1",
            captured_at=captured,
            published_at=published,
            lag_days=10.0,
        )
        assert chain.idea_id == "idea-1"
        assert chain.captured_at == captured
        assert chain.published_at == published
        assert chain.lag_days == 10.0

    def test_create_orphaned_chain(self):
        """Verify chain can be created for orphaned idea."""
        captured = datetime.now(timezone.utc)
        chain = IdeaRealizationChain(
            idea_id="idea-2",
            captured_at=captured,
            published_at=None,
            lag_days=None,
        )
        assert chain.published_at is None
        assert chain.lag_days is None

    def test_chain_frozen(self):
        """Verify chain is immutable."""
        captured = datetime.now(timezone.utc)
        chain = IdeaRealizationChain(
            idea_id="idea-1",
            captured_at=captured,
            published_at=None,
            lag_days=None,
        )
        with pytest.raises(AttributeError):
            chain.lag_days = 5.0


class TestClassifyLagTier:
    """Test lag tier classification."""

    def test_orphaned_tier(self):
        """Verify None lag_days returns orphaned."""
        assert _classify_lag_tier(None) == TIER_ORPHANED

    def test_fast_tier(self):
        """Verify <7 days is fast."""
        assert _classify_lag_tier(0.0) == TIER_FAST
        assert _classify_lag_tier(3.0) == TIER_FAST
        assert _classify_lag_tier(6.9) == TIER_FAST

    def test_normal_tier(self):
        """Verify 7-30 days is normal."""
        assert _classify_lag_tier(7.0) == TIER_NORMAL
        assert _classify_lag_tier(15.0) == TIER_NORMAL
        assert _classify_lag_tier(29.9) == TIER_NORMAL

    def test_slow_tier(self):
        """Verify 30-90 days is slow."""
        assert _classify_lag_tier(30.0) == TIER_SLOW
        assert _classify_lag_tier(60.0) == TIER_SLOW
        assert _classify_lag_tier(89.9) == TIER_SLOW

    def test_stalled_tier(self):
        """Verify >90 days is stalled."""
        assert _classify_lag_tier(90.0) == TIER_STALLED
        assert _classify_lag_tier(120.0) == TIER_STALLED
        assert _classify_lag_tier(365.0) == TIER_STALLED


class TestCalculateTierCounts:
    """Test tier count calculation."""

    def test_empty_chains(self):
        """Verify empty chains returns zero counts."""
        counts = _calculate_tier_counts([])
        assert counts[TIER_FAST] == 0
        assert counts[TIER_ORPHANED] == 0

    def test_all_orphaned(self):
        """Verify all orphaned ideas counted correctly."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, None, None),
            IdeaRealizationChain("2", now, None, None),
            IdeaRealizationChain("3", now, None, None),
        ]
        counts = _calculate_tier_counts(chains)
        assert counts[TIER_ORPHANED] == 3
        assert counts[TIER_FAST] == 0

    def test_mixed_tiers(self):
        """Verify mixed tiers counted correctly."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=3), 3.0),  # fast
            IdeaRealizationChain("2", now, now + timedelta(days=15), 15.0),  # normal
            IdeaRealizationChain("3", now, now + timedelta(days=45), 45.0),  # slow
            IdeaRealizationChain("4", now, now + timedelta(days=120), 120.0),  # stalled
            IdeaRealizationChain("5", now, None, None),  # orphaned
        ]
        counts = _calculate_tier_counts(chains)
        assert counts[TIER_FAST] == 1
        assert counts[TIER_NORMAL] == 1
        assert counts[TIER_SLOW] == 1
        assert counts[TIER_STALLED] == 1
        assert counts[TIER_ORPHANED] == 1


class TestCalculateLagDistribution:
    """Test lag distribution calculation."""

    def test_empty_chains_raises(self):
        """Verify empty chains raises ValueError."""
        with pytest.raises(ValueError, match="no published chains"):
            _calculate_lag_distribution([])

    def test_single_published(self):
        """Verify single published idea produces valid distribution."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=10), 10.0)
        ]
        dist = _calculate_lag_distribution(chains)
        assert dist.min_days == 10.0
        assert dist.max_days == 10.0
        assert dist.median_days == 10.0
        assert dist.mean_days == 10.0
        assert dist.sample_size == 1

    def test_even_count_median(self):
        """Verify median calculation for even count."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=5), 5.0),
            IdeaRealizationChain("2", now, now + timedelta(days=15), 15.0),
        ]
        dist = _calculate_lag_distribution(chains)
        assert dist.median_days == 10.0  # Average of 5 and 15

    def test_odd_count_median(self):
        """Verify median calculation for odd count."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=5), 5.0),
            IdeaRealizationChain("2", now, now + timedelta(days=10), 10.0),
            IdeaRealizationChain("3", now, now + timedelta(days=15), 15.0),
        ]
        dist = _calculate_lag_distribution(chains)
        assert dist.median_days == 10.0  # Middle value

    def test_percentiles(self):
        """Verify percentile calculations."""
        now = datetime.now(timezone.utc)
        # Create 10 chains with lags 1, 2, 3, ..., 10
        chains = [
            IdeaRealizationChain(str(i), now, now + timedelta(days=i), float(i))
            for i in range(1, 11)
        ]
        dist = _calculate_lag_distribution(chains)
        assert dist.p25_days <= dist.median_days
        assert dist.median_days <= dist.p75_days
        assert dist.p25_days < dist.p75_days

    def test_statistics_rounded(self):
        """Verify statistics are rounded to 2 decimals."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=3.333), 3.333),
        ]
        dist = _calculate_lag_distribution(chains)
        assert dist.mean_days == round(dist.mean_days, 2)


class TestCalculateRealizationVelocity:
    """Test realization velocity calculation."""

    def test_empty_chains_raises(self):
        """Verify empty chains raises ValueError."""
        with pytest.raises(ValueError, match="no published chains"):
            _calculate_realization_velocity([], None, None)

    def test_single_idea_one_day(self):
        """Verify single idea published in one day."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=1), 1.0)
        ]
        vel = _calculate_realization_velocity(chains, now, now + timedelta(days=1))
        assert vel.ideas_per_day == 1.0
        assert vel.ideas_per_week == 7.0
        assert vel.observation_days == 1

    def test_multiple_ideas_over_week(self):
        """Verify velocity calculation over week."""
        start = datetime.now(timezone.utc)
        end = start + timedelta(days=7)
        chains = [
            IdeaRealizationChain("1", start, start + timedelta(days=1), 1.0),
            IdeaRealizationChain("2", start, start + timedelta(days=3), 3.0),
            IdeaRealizationChain("3", start, start + timedelta(days=5), 5.0),
        ]
        vel = _calculate_realization_velocity(chains, start, end)
        assert vel.ideas_per_day == pytest.approx(3 / 7, abs=0.01)
        assert vel.ideas_per_week == pytest.approx(3.0, abs=0.1)

    def test_auto_observation_period(self):
        """Verify automatic observation period from first to last."""
        start = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", start, start + timedelta(days=10), 10.0),
            IdeaRealizationChain("2", start + timedelta(days=5), start + timedelta(days=20), 15.0),
        ]
        # Should use first capture (start) to last publication (start + 20 days)
        vel = _calculate_realization_velocity(chains, None, None)
        assert vel.observation_days == 20

    def test_same_day_publication(self):
        """Verify handling when all published same day."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now, 0.0),
            IdeaRealizationChain("2", now, now, 0.0),
        ]
        vel = _calculate_realization_velocity(chains, now, now)
        # Should default to 1 day to avoid division by zero
        assert vel.observation_days == 1
        assert vel.ideas_per_day == 2.0


class TestAnalyzeIdeaRealizationLag:
    """Test complete idea realization lag analysis."""

    def test_empty_chains(self):
        """Verify empty chains produces valid analysis."""
        result = analyze_idea_realization_lag([])
        assert result.lag_distribution is None
        assert result.velocity is None
        assert result.orphaned_count == 0
        assert len(result.insights) > 0

    def test_invalid_chains_type_raises(self):
        """Verify invalid chains type raises ValueError."""
        with pytest.raises(ValueError, match="must be a sequence"):
            analyze_idea_realization_lag("not a list")  # type: ignore

    def test_invalid_chain_instance_raises(self):
        """Verify invalid chain instance raises ValueError."""
        with pytest.raises(ValueError, match="IdeaRealizationChain instances"):
            analyze_idea_realization_lag([{"not": "a chain"}])  # type: ignore

    def test_naive_captured_at_raises(self):
        """Verify naive datetime for captured_at raises ValueError."""
        naive_dt = datetime.now()  # No timezone
        chain = IdeaRealizationChain("1", naive_dt, None, None)
        with pytest.raises(ValueError, match="captured_at must be timezone-aware"):
            analyze_idea_realization_lag([chain])

    def test_naive_published_at_raises(self):
        """Verify naive datetime for published_at raises ValueError."""
        now = datetime.now(timezone.utc)
        naive_dt = datetime.now()  # No timezone
        chain = IdeaRealizationChain("1", now, naive_dt, 1.0)
        with pytest.raises(ValueError, match="published_at must be timezone-aware"):
            analyze_idea_realization_lag([chain])

    def test_all_orphaned(self):
        """Verify all orphaned ideas analysis."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, None, None),
            IdeaRealizationChain("2", now, None, None),
        ]
        result = analyze_idea_realization_lag(chains)
        assert result.lag_distribution is None
        assert result.velocity is None
        assert result.orphaned_count == 2
        assert result.tier_counts[TIER_ORPHANED] == 2
        # Should have insight about critical blockage
        insights_text = " ".join(result.insights).lower()
        assert "no published" in insights_text or "blockage" in insights_text

    def test_all_published(self):
        """Verify all published ideas analysis."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=5), 5.0),
            IdeaRealizationChain("2", now, now + timedelta(days=15), 15.0),
            IdeaRealizationChain("3", now, now + timedelta(days=25), 25.0),
        ]
        result = analyze_idea_realization_lag(chains)
        assert result.lag_distribution is not None
        assert result.velocity is not None
        assert result.orphaned_count == 0
        assert result.tier_counts[TIER_ORPHANED] == 0

    def test_mixed_published_orphaned(self):
        """Verify mixed published and orphaned analysis."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=10), 10.0),
            IdeaRealizationChain("2", now, now + timedelta(days=20), 20.0),
            IdeaRealizationChain("3", now, None, None),
            IdeaRealizationChain("4", now, None, None),
        ]
        result = analyze_idea_realization_lag(chains)
        assert result.lag_distribution is not None
        assert result.velocity is not None
        assert result.orphaned_count == 2
        # Should have realization rate insight
        insights_text = " ".join(result.insights).lower()
        assert "realization rate" in insights_text
        assert "50" in insights_text or "2/4" in insights_text

    def test_insights_generated(self):
        """Verify insights are always generated."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=5), 5.0),
        ]
        result = analyze_idea_realization_lag(chains)
        assert isinstance(result.insights, list)
        assert len(result.insights) > 0

    def test_result_immutable(self):
        """Verify result is immutable."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=5), 5.0),
        ]
        result = analyze_idea_realization_lag(chains)
        with pytest.raises(AttributeError):
            result.orphaned_count = 99

    def test_tier_counts_complete(self):
        """Verify tier counts include all tiers."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=5), 5.0),
        ]
        result = analyze_idea_realization_lag(chains)
        assert TIER_FAST in result.tier_counts
        assert TIER_NORMAL in result.tier_counts
        assert TIER_SLOW in result.tier_counts
        assert TIER_STALLED in result.tier_counts
        assert TIER_ORPHANED in result.tier_counts


class TestInsightGeneration:
    """Test insight generation quality."""

    def test_high_orphan_rate_insight(self):
        """Verify high orphan rate generates warning insight."""
        now = datetime.now(timezone.utc)
        # 80% orphaned
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=5), 5.0),
            IdeaRealizationChain("2", now, None, None),
            IdeaRealizationChain("3", now, None, None),
            IdeaRealizationChain("4", now, None, None),
            IdeaRealizationChain("5", now, None, None),
        ]
        result = analyze_idea_realization_lag(chains)
        insights_text = " ".join(result.insights).lower()
        assert "orphan" in insights_text or "never published" in insights_text

    def test_fast_turnaround_insight(self):
        """Verify fast turnaround generates positive insight."""
        now = datetime.now(timezone.utc)
        # All fast (<7 days)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=2), 2.0),
            IdeaRealizationChain("2", now, now + timedelta(days=3), 3.0),
            IdeaRealizationChain("3", now, now + timedelta(days=5), 5.0),
        ]
        result = analyze_idea_realization_lag(chains)
        insights_text = " ".join(result.insights).lower()
        assert "fast" in insights_text or "turnaround" in insights_text

    def test_high_stall_rate_insight(self):
        """Verify high stall rate generates warning."""
        now = datetime.now(timezone.utc)
        # 50% stalled (>90 days)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=100), 100.0),
            IdeaRealizationChain("2", now, now + timedelta(days=10), 10.0),
        ]
        result = analyze_idea_realization_lag(chains)
        insights_text = " ".join(result.insights).lower()
        assert "stall" in insights_text or "delay" in insights_text

    def test_low_velocity_insight(self):
        """Verify low velocity generates throughput warning."""
        now = datetime.now(timezone.utc)
        # 1 idea over 30 days
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=30), 30.0),
        ]
        result = analyze_idea_realization_lag(chains, now, now + timedelta(days=30))
        insights_text = " ".join(result.insights).lower()
        assert "low velocity" in insights_text or "throughput" in insights_text

    def test_high_velocity_insight(self):
        """Verify high velocity generates positive insight."""
        now = datetime.now(timezone.utc)
        # 10 ideas over 7 days
        chains = [
            IdeaRealizationChain(str(i), now, now + timedelta(days=i % 7), float(i % 7))
            for i in range(10)
        ]
        result = analyze_idea_realization_lag(chains, now, now + timedelta(days=7))
        insights_text = " ".join(result.insights).lower()
        assert "velocity" in insights_text or "performance" in insights_text

    def test_wide_variance_insight(self):
        """Verify wide lag variance generates consistency warning."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=1), 1.0),
            IdeaRealizationChain("2", now, now + timedelta(days=120), 120.0),
        ]
        result = analyze_idea_realization_lag(chains)
        insights_text = " ".join(result.insights).lower()
        assert "variance" in insights_text or "inconsistent" in insights_text

    def test_median_lag_mentioned(self):
        """Verify median lag is mentioned in insights."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=10), 10.0),
            IdeaRealizationChain("2", now, now + timedelta(days=20), 20.0),
        ]
        result = analyze_idea_realization_lag(chains)
        insights_text = " ".join(result.insights).lower()
        assert "median" in insights_text


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_single_published_idea(self):
        """Verify single published idea produces valid analysis."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=15), 15.0),
        ]
        result = analyze_idea_realization_lag(chains)
        assert result.lag_distribution is not None
        assert result.lag_distribution.min_days == result.lag_distribution.max_days
        assert result.velocity is not None

    def test_exact_threshold_values(self):
        """Verify behavior at exact threshold boundaries."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=THRESHOLD_FAST_DAYS), float(THRESHOLD_FAST_DAYS)),
            IdeaRealizationChain("2", now, now + timedelta(days=THRESHOLD_NORMAL_DAYS), float(THRESHOLD_NORMAL_DAYS)),
            IdeaRealizationChain("3", now, now + timedelta(days=THRESHOLD_SLOW_DAYS), float(THRESHOLD_SLOW_DAYS)),
        ]
        result = analyze_idea_realization_lag(chains)
        # Should not crash and should classify correctly
        assert result.tier_counts[TIER_NORMAL] == 1
        assert result.tier_counts[TIER_SLOW] == 1
        assert result.tier_counts[TIER_STALLED] == 1

    def test_observation_period_provided(self):
        """Verify explicit observation period is respected."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=5), 5.0),
        ]
        start = now - timedelta(days=10)
        end = now + timedelta(days=20)
        result = analyze_idea_realization_lag(chains, start, end)
        assert result.velocity is not None
        assert result.velocity.observation_days == 30

    def test_large_sample_size(self):
        """Verify handling of large number of chains."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain(str(i), now, now + timedelta(days=i % 100), float(i % 100))
            for i in range(100)
        ]
        result = analyze_idea_realization_lag(chains)
        assert result.lag_distribution is not None
        assert result.lag_distribution.sample_size == 100

    def test_zero_lag_days(self):
        """Verify published same day as capture."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now, 0.0),
        ]
        result = analyze_idea_realization_lag(chains)
        assert result.lag_distribution is not None
        assert result.lag_distribution.min_days == 0.0
        assert result.tier_counts[TIER_FAST] == 1


class TestDistributionStatistics:
    """Test distribution statistics accuracy."""

    def test_mean_calculation(self):
        """Verify mean is calculated correctly."""
        now = datetime.now(timezone.utc)
        # Create chains with lags: 10, 20, 30 -> mean = 20
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=10), 10.0),
            IdeaRealizationChain("2", now, now + timedelta(days=20), 20.0),
            IdeaRealizationChain("3", now, now + timedelta(days=30), 30.0),
        ]
        dist = _calculate_lag_distribution(chains)
        assert dist.mean_days == 20.0

    def test_min_max_calculation(self):
        """Verify min and max are identified correctly."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=5), 5.0),
            IdeaRealizationChain("2", now, now + timedelta(days=50), 50.0),
            IdeaRealizationChain("3", now, now + timedelta(days=25), 25.0),
        ]
        dist = _calculate_lag_distribution(chains)
        assert dist.min_days == 5.0
        assert dist.max_days == 50.0


class TestBottlenecks:
    """Test bottleneck identification (placeholder for future implementation)."""

    def test_bottlenecks_empty_for_now(self):
        """Verify bottlenecks returns empty list (not yet implemented)."""
        now = datetime.now(timezone.utc)
        chains = [
            IdeaRealizationChain("1", now, now + timedelta(days=10), 10.0),
        ]
        result = analyze_idea_realization_lag(chains)
        # Bottleneck detection not yet implemented
        assert len(result.bottlenecks) == 0


class TestLagDistributionDataclass:
    """Test LagDistribution dataclass properties."""

    def test_lag_distribution_frozen(self):
        """Verify LagDistribution is immutable."""
        dist = LagDistribution(
            min_days=1.0,
            max_days=10.0,
            median_days=5.0,
            mean_days=5.5,
            p25_days=3.0,
            p75_days=7.0,
            sample_size=10,
        )
        with pytest.raises(AttributeError):
            dist.mean_days = 99.0


class TestRealizationVelocityDataclass:
    """Test RealizationVelocity dataclass properties."""

    def test_velocity_frozen(self):
        """Verify RealizationVelocity is immutable."""
        vel = RealizationVelocity(
            ideas_per_day=1.5,
            ideas_per_week=10.5,
            ideas_per_month=45.0,
            observation_days=30,
        )
        with pytest.raises(AttributeError):
            vel.ideas_per_day = 99.0

    def test_velocity_conversions_consistent(self):
        """Verify per-day, per-week, per-month conversions are consistent."""
        vel = RealizationVelocity(
            ideas_per_day=1.0,
            ideas_per_week=7.0,
            ideas_per_month=30.0,
            observation_days=30,
        )
        assert vel.ideas_per_week == pytest.approx(vel.ideas_per_day * 7, abs=0.1)
        assert vel.ideas_per_month == pytest.approx(vel.ideas_per_day * 30, abs=0.1)
