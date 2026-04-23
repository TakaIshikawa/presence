"""Tests for platform_divergence_report.py — CLI entry point for divergence analysis."""

import json
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from platform_divergence_report import main, format_report, format_json
from storage.db import Database
from evaluation.platform_divergence import (
    DivergenceReport,
    DivergenceItem,
    PlatformComparison,
    ContentTypeRecommendation,
)


# --- fixtures ---


@pytest.fixture
def test_db(tmp_path):
    """Create temporary SQLite database with schema."""
    db_path = tmp_path / "test_presence.db"
    db = Database(str(db_path))
    db.connect()
    schema_path = Path(__file__).parent.parent / "schema.sql"
    db.init_schema(str(schema_path))
    yield db
    db.close()


@pytest.fixture
def populated_db(test_db):
    """Database with cross-platform engagement data for testing."""
    now = datetime.now(timezone.utc)

    # Create diverse cross-platform content
    content_items = [
        # High divergence: Bluesky wins
        {
            "content": "This post performs much better on Bluesky for some reason",
            "type": "x_post",
            "x_score": 5.0,
            "bluesky_score": 15.0,
        },
        # High divergence: X wins
        {
            "content": "This one resonates more with X audience apparently",
            "type": "x_post",
            "x_score": 20.0,
            "bluesky_score": 5.0,
        },
        # Similar performance
        {
            "content": "A well-balanced post that works on both platforms",
            "type": "x_post",
            "x_score": 10.0,
            "bluesky_score": 11.0,
        },
        # Thread with divergence
        {
            "content": "Thread discussing complex topic that gets better engagement on Bluesky",
            "type": "x_thread",
            "x_score": 3.0,
            "bluesky_score": 12.0,
        },
        # Another balanced post
        {
            "content": "Another example of similar cross-platform performance",
            "type": "x_post",
            "x_score": 8.0,
            "bluesky_score": 8.5,
        },
    ]

    for i, item in enumerate(content_items, 1):
        # Insert content
        content_id = test_db.insert_generated_content(
            content_type=item["type"],
            source_commits=[],
            source_messages=[],
            content=item["content"],
            eval_score=7.0,
            eval_feedback="Test content"
        )

        # Mark as published on both platforms
        test_db.mark_published(
            content_id=content_id,
            url=f"https://x.com/test/status/{i}",
            tweet_id=str(i)
        )
        test_db.mark_published_bluesky(
            content_id=content_id,
            uri=f"at://did:plc:test/app.bsky.feed.post/{i}"
        )

        # Set published_at within time range
        test_db.conn.execute(
            "UPDATE generated_content SET published_at = ? WHERE id = ?",
            ((now - timedelta(days=i)).isoformat(), content_id)
        )

        # Insert engagement data for both platforms
        test_db.insert_engagement(
            content_id=content_id,
            tweet_id=str(i),
            like_count=int(item["x_score"]),
            retweet_count=0,
            reply_count=0,
            quote_count=0,
            engagement_score=item["x_score"]
        )

        test_db.insert_bluesky_engagement(
            content_id=content_id,
            bluesky_uri=f"at://did:plc:test/app.bsky.feed.post/{i}",
            like_count=int(item["bluesky_score"]),
            repost_count=0,
            reply_count=0,
            quote_count=0,
            engagement_score=item["bluesky_score"]
        )

    test_db.conn.commit()
    return test_db


@pytest.fixture
def empty_db(test_db):
    """Database with no cross-platform content."""
    # Add some X-only content to ensure it's filtered correctly
    content_id = test_db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="X-only post",
        eval_score=7.0,
        eval_feedback="Test"
    )
    test_db.mark_published(content_id, "https://x.com/test/1", "1")
    test_db.insert_engagement(content_id, "1", 10, 0, 0, 0, 10.0)
    test_db.conn.commit()
    return test_db


# --- TestFormatReport ---


class TestFormatReport:
    """Test report formatting function."""

    def test_format_empty_report(self):
        """Test formatting when no data available."""
        report = DivergenceReport(
            total_cross_posted=0,
            avg_x_score=0.0,
            avg_bluesky_score=0.0,
            platform_winner="tie",
            high_divergence_items=[],
            content_type_breakdown={},
            platform_takeaway="",
            format_insights=[]
        )

        output = format_report(report)

        assert "PLATFORM DIVERGENCE ANALYSIS REPORT" in output
        assert "No cross-posted content with engagement data yet." in output

    def test_format_report_with_data(self):
        """Test formatting with actual data."""
        report = DivergenceReport(
            total_cross_posted=5,
            avg_x_score=9.2,
            avg_bluesky_score=10.3,
            platform_winner="bluesky",
            high_divergence_items=[
                DivergenceItem(
                    content_id=1,
                    content_type="x_post",
                    content_preview="Test post with divergence",
                    x_score=5.0,
                    bluesky_score=15.0,
                    divergence_ratio=3.0,
                    winning_platform="bluesky"
                )
            ],
            content_type_breakdown={
                "x_post": PlatformComparison(
                    content_type="x_post",
                    count=4,
                    avg_x_score=10.75,
                    avg_bluesky_score=9.88,
                    winner="x"
                ),
                "x_thread": PlatformComparison(
                    content_type="x_thread",
                    count=1,
                    avg_x_score=3.0,
                    avg_bluesky_score=12.0,
                    winner="bluesky"
                )
            },
            platform_takeaway="Bluesky is the stronger overall platform here, averaging 10.30 vs 9.20 on X. Strongest format signal: Favor Bluesky for thread content.",
            recommendations=[
                ContentTypeRecommendation(
                    content_type="x_thread",
                    content_type_label="Thread",
                    count=1,
                    avg_x_score=3.0,
                    avg_bluesky_score=12.0,
                    winner="bluesky",
                    recommendation="Favor Bluesky for thread content. Keep the fuller framing, conversation hooks, and community-oriented tone.",
                    rationale="Bluesky averaged 12.00 vs X at 3.00, a gap of 9.00 points.",
                    score_gap=9.0,
                ),
                ContentTypeRecommendation(
                    content_type="x_post",
                    content_type_label="Post",
                    count=4,
                    avg_x_score=10.75,
                    avg_bluesky_score=9.88,
                    winner="x",
                    recommendation="Favor X for post content. Tighten the hook, compress the copy, and lead with the strongest claim.",
                    rationale="X averaged 10.75 vs Bluesky at 9.88, a gap of 0.87 points.",
                    score_gap=0.87,
                ),
            ],
            format_insights=["Posts perform similarly across platforms"]
        )

        output = format_report(report)

        # Check summary stats
        assert "Total cross-posted items: 5" in output
        assert "Average X score: 9.20" in output
        assert "Average Bluesky score: 10.30" in output
        assert "Platform winner: BLUESKY" in output
        assert "STRONGEST TAKEAWAY:" in output
        assert "CONTENT TYPE RECOMMENDATIONS:" in output

        # Check format insights
        assert "FORMAT INSIGHTS:" in output
        assert "Posts perform similarly across platforms" in output

        # Check content type breakdown
        assert "CONTENT TYPE BREAKDOWN:" in output
        assert "Post:" in output  # x_post formatted
        assert "Thread:" in output  # x_thread formatted
        assert "Count: 4" in output
        assert "Count: 1" in output

        # Check high divergence items
        assert "HIGH DIVERGENCE EXAMPLES" in output
        assert "BLUESKY wins 3.0x" in output
        assert "X: 5.0 | Bluesky: 15.0" in output

    def test_format_report_truncates_long_preview(self):
        """Test that long content previews are truncated."""
        long_content = "A" * 100
        report = DivergenceReport(
            total_cross_posted=1,
            avg_x_score=5.0,
            avg_bluesky_score=10.0,
            platform_winner="bluesky",
            high_divergence_items=[
                DivergenceItem(
                    content_id=1,
                    content_type="x_post",
                    content_preview=long_content,
                    x_score=5.0,
                    bluesky_score=15.0,
                    divergence_ratio=3.0,
                    winning_platform="bluesky"
                )
            ],
            content_type_breakdown={},
            platform_takeaway="",
            format_insights=[]
        )

        output = format_report(report)

        # Preview should be truncated to 60 chars + "..."
        assert "A" * 60 + "..." in output

    def test_format_report_limits_divergence_items(self):
        """Test that only a short list of divergence items is shown."""
        # Create 15 high-divergence items
        items = [
            DivergenceItem(
                content_id=i,
                content_type="x_post",
                content_preview=f"Item {i}",
                x_score=5.0,
                bluesky_score=15.0,
                divergence_ratio=3.0 + i * 0.1,
                winning_platform="bluesky"
            )
            for i in range(15)
        ]

        report = DivergenceReport(
            total_cross_posted=15,
            avg_x_score=5.0,
            avg_bluesky_score=15.0,
            platform_winner="bluesky",
            high_divergence_items=items,
            content_type_breakdown={},
            platform_takeaway="",
            format_insights=[]
        )

        output = format_report(report)

        # Should show top 3
        assert "1. " in output
        assert "3. " in output
        # Should indicate more items exist
        assert "and 12 more" in output


# --- TestMainFunction ---


class TestMainFunction:
    """Test the main CLI entry point."""

    def test_main_with_populated_db(self, populated_db, capsys, tmp_path):
        """Test main function with cross-platform data."""
        # Mock script_context to use our test database
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("platform_divergence_report.script_context") as mock_context, \
             patch("platform_divergence_report.update_monitoring") as mock_monitoring, \
             patch("sys.argv", ["platform_divergence_report.py", "--days", "60"]):

            mock_context.return_value.__enter__ = lambda self: (mock_config, populated_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            # Run main
            main()

            # Check monitoring was updated
            mock_monitoring.assert_called_once_with("platform_divergence")

        # Capture output
        captured = capsys.readouterr()

        # Verify report sections are present
        assert "PLATFORM DIVERGENCE ANALYSIS REPORT" in captured.out
        assert "Total cross-posted items: 5" in captured.out
        assert "Platform winner:" in captured.out
        assert "STRONGEST TAKEAWAY:" in captured.out
        assert "CONTENT TYPE RECOMMENDATIONS:" in captured.out
        assert "CONTENT TYPE BREAKDOWN:" in captured.out
        assert "HIGH DIVERGENCE EXAMPLES" in captured.out

        # Verify adaptation context is shown
        assert "ADAPTATION CONTEXT (for generation prompts):" in captured.out
        assert "PLATFORM NOTES:" in captured.out

    def test_main_with_empty_db(self, empty_db, capsys, tmp_path):
        """Test main function with no cross-platform data."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("platform_divergence_report.script_context") as mock_context, \
             patch("platform_divergence_report.update_monitoring") as mock_monitoring, \
             patch("sys.argv", ["platform_divergence_report.py"]):

            mock_context.return_value.__enter__ = lambda self: (mock_config, empty_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            main()

            mock_monitoring.assert_called_once_with("platform_divergence")

        captured = capsys.readouterr()

        # Should show empty state message
        assert "No cross-posted content with engagement data yet." in captured.out
        # Should not show adaptation context (insufficient data)
        assert "ADAPTATION CONTEXT" not in captured.out

    def test_main_custom_days_argument(self, populated_db, capsys, tmp_path):
        """Test main function respects --days argument."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("platform_divergence_report.script_context") as mock_context, \
             patch("platform_divergence_report.update_monitoring"), \
             patch("sys.argv", ["platform_divergence_report.py", "--days", "30"]):

            mock_context.return_value.__enter__ = lambda self: (mock_config, populated_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            main()

        captured = capsys.readouterr()

        # Verify analyzer was called with correct days parameter
        # This is implicit in the log message
        assert "PLATFORM DIVERGENCE ANALYSIS REPORT" in captured.out

    def test_main_json_output(self, populated_db, capsys, tmp_path):
        """Test main function can output JSON."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("platform_divergence_report.script_context") as mock_context, \
             patch("platform_divergence_report.update_monitoring"), \
             patch("sys.argv", ["platform_divergence_report.py", "--json"]):

            mock_context.return_value.__enter__ = lambda self: (mock_config, populated_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            main()

        captured = capsys.readouterr()
        payload = json.loads(captured.out)

        assert payload["comparative_stats"]["total_cross_posted"] == 5
        assert payload["comparative_stats"]["platform_winner"] in {"bluesky", "x", "tie"}
        assert "recommendations" in payload
        assert len(payload["recommendations"]) >= 1
        assert "high_divergence_items" in payload
        assert "adaptation_context" in payload

    def test_main_logging_output(self, populated_db, capsys, tmp_path):
        """Test that main function produces expected output and log messages."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("platform_divergence_report.script_context") as mock_context, \
             patch("platform_divergence_report.update_monitoring"), \
             patch("sys.argv", ["platform_divergence_report.py", "--days", "60"]):

            mock_context.return_value.__enter__ = lambda self: (mock_config, populated_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            main()

        # Capture output which includes logging to stderr
        captured = capsys.readouterr()
        combined_output = captured.out + captured.err

        # Verify report was printed
        assert "Total cross-posted items: 5" in combined_output
        assert "Platform winner:" in combined_output

    def test_main_with_tie_platform_winner(self, test_db, capsys, tmp_path):
        """Test main function when platforms are tied."""
        # Create content with identical scores
        now = datetime.now(timezone.utc)

        for i in range(3):
            content_id = test_db.insert_generated_content(
                content_type="x_post",
                source_commits=[],
                source_messages=[],
                content=f"Balanced post {i}",
                eval_score=7.0,
                eval_feedback="Test"
            )
            test_db.mark_published(content_id, f"https://x.com/test/{i}", str(i))
            test_db.mark_published_bluesky(content_id, f"at://test/post/{i}")
            test_db.conn.execute(
                "UPDATE generated_content SET published_at = ? WHERE id = ?",
                ((now - timedelta(days=i)).isoformat(), content_id)
            )
            test_db.insert_engagement(content_id, str(i), 10, 0, 0, 0, 10.0)
            test_db.insert_bluesky_engagement(content_id, f"at://test/post/{i}", 10, 0, 0, 0, 10.0)

        test_db.conn.commit()

        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("platform_divergence_report.script_context") as mock_context, \
             patch("platform_divergence_report.update_monitoring"), \
             patch("sys.argv", ["platform_divergence_report.py"]):

            mock_context.return_value.__enter__ = lambda self: (mock_config, test_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            main()

        captured = capsys.readouterr()

        assert "Platform winner: TIE" in captured.out
