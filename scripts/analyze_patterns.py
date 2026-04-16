#!/usr/bin/env python3
"""Analyze engagement patterns and store results for pipeline consumption.

Compares resonated vs low_resonance posts to extract structural patterns.
Results are cached in the DB meta table for injection into generator prompts.
Intended to run weekly or manually before tuning.
"""

import json
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.pattern_analyzer import PatternAnalyzer

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        classified = db.get_all_classified_posts(content_type="x_post")

        res_count = len(classified["resonated"])
        low_count = len(classified["low_resonance"])
        logger.info(f"Classified posts: {res_count} resonated, {low_count} low_resonance")

        if res_count < PatternAnalyzer.MIN_RESONATED:
            logger.info(
                f"Not enough resonated posts for analysis "
                f"(need >= {PatternAnalyzer.MIN_RESONATED})"
            )
            return

        analyzer = PatternAnalyzer(
            api_key=config.anthropic.api_key,
            model=config.synthesis.eval_model,
        )

        logger.info("Running pattern analysis...")
        analysis = analyzer.analyze(
            resonated=classified["resonated"],
            low_resonance=classified["low_resonance"],
        )

        if not analysis:
            logger.warning("Pattern analysis returned None")
            return

        db.set_meta("pattern_analysis", json.dumps({
            "positive_patterns": analysis.positive_patterns,
            "negative_patterns": analysis.negative_patterns,
            "key_differences": analysis.key_differences,
            "actionable_rules": analysis.actionable_rules,
            "analyzed_at": analysis.analyzed_at,
            "resonated_count": res_count,
            "low_resonance_count": low_count,
            "confidence": analysis.confidence,
        }))

        logger.info(
            f"Stored pattern analysis: {len(analysis.actionable_rules)} rules "
            f"from {res_count} resonated, {low_count} low_resonance posts"
        )
        for i, rule in enumerate(analysis.actionable_rules, 1):
            logger.info(f"  Rule {i}: {rule}")


if __name__ == "__main__":
    main()
