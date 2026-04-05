#!/usr/bin/env python3
"""Generate and publish weekly blog post via multi-stage pipeline."""

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from ingestion.claude_logs import ClaudeLogParser
from synthesis.pipeline import SynthesisPipeline
from output.blog_writer import BlogWriter


def main():
    with script_context() as (config, db):
        pipeline = SynthesisPipeline(
            api_key=config.anthropic.api_key,
            generator_model=config.synthesis.model,
            evaluator_model=config.synthesis.eval_model,
            db=db,
            num_candidates=config.synthesis.num_candidates,
        )
        blog_writer = BlogWriter(config.paths.static_site)

        # Get this week's date range (last 7 days, UTC)
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        week_ago = today - timedelta(days=7)

        print(f"Generating weekly digest for {week_ago.date()} to {today.date()}")

        # Get this week's commits
        commits = db.get_commits_in_range(week_ago, today)
        if not commits:
            print("No commits this week, skipping digest")
            return

        print(f"Found {len(commits)} commits")

        # Get this week's Claude prompts
        parser = ClaudeLogParser(config.paths.claude_logs)
        prompts = [
            msg for msg in parser.parse_global_history()
            if week_ago <= msg.timestamp < today
        ]
        prompt_texts = [p.prompt_text for p in prompts]

        print(f"Found {len(prompts)} prompts")

        if not prompt_texts:
            print("No prompts found, skipping digest")
            return

        # Convert commit dicts from db rows to have expected keys
        commit_dicts = [
            {"repo_name": c.get("repo_name", ""), "message": c.get("commit_message", ""),
             "sha": c.get("commit_sha", "")}
            for c in commits
        ]

        # Inject historical context if configured
        if config.historical and config.historical.enabled:
            from synthesis.theme_selector import ThemeSelector
            theme_selector = ThemeSelector(db)
            if theme_selector.should_inject("blog_post", config.historical.injection_frequency):
                ctx = theme_selector.select(
                    commit_dicts, "blog_post",
                    lookback_days=config.historical.lookback_days,
                    min_age_days=config.historical.min_age_days,
                    max_commits=config.historical.max_historical_commits,
                )
                if ctx:
                    print(f"  Historical context: {ctx.theme_description}")
                    for hc in ctx.commits:
                        hc["historical"] = True
                        commit_dicts.append(hc)

        # Run pipeline
        print(f"Running pipeline: {len(commits)} commits, {config.synthesis.num_candidates} candidates...")
        result = pipeline.run(
            prompts=prompt_texts,
            commits=commit_dicts,
            content_type="blog_post",
            threshold=config.synthesis.eval_threshold,
        )

        # Log pipeline stages
        best_idx = result.comparison.ranking[0] if result.comparison.ranking else 0
        print(f"  Best candidate: {chr(65 + best_idx)} (score: {result.comparison.best_score}/10)")
        if result.refinement:
            print(f"  Refinement: picked {result.refinement.picked} (score: {result.refinement.final_score}/10)")
        print(f"  Final score: {result.final_score}/10")

        # Store
        content_id = db.insert_generated_content(
            content_type="blog_post",
            source_commits=[c["sha"] for c in commit_dicts],
            source_messages=[p.message_uuid for p in prompts],
            content=result.final_content,
            eval_score=result.final_score,
            eval_feedback=result.comparison.best_feedback,
        )

        # Record pipeline run
        db.insert_pipeline_run(
            batch_id=result.batch_id,
            content_type="blog_post",
            candidates_generated=len(result.candidates),
            best_candidate_index=best_idx,
            best_score_before_refine=result.comparison.best_score,
            best_score_after_refine=result.refinement.final_score if result.refinement else None,
            refinement_picked=result.refinement.picked if result.refinement else None,
            final_score=result.final_score,
            content_id=content_id,
        )

        # Publish if passes threshold
        passes = result.final_score >= config.synthesis.eval_threshold * 10
        if passes:
            print("Writing blog post...")
            write_result = blog_writer.write_post(result.final_content)

            if write_result.success:
                print(f"Blog post written: {write_result.file_path}")

                # Commit and push
                print("Committing and pushing...")
                title = result.final_content.split("\n")[0].replace("TITLE:", "").strip()
                if blog_writer.commit_and_push(title):
                    db.mark_published(content_id, write_result.url)
                    print(f"Published: {write_result.url}")
                else:
                    print("Git push failed")
            else:
                print(f"Write failed: {write_result.error}")
        else:
            if result.comparison.reject_reason:
                print(f"Rejected: {result.comparison.reject_reason}")
            else:
                print("Below threshold, not publishing")
            print("Generated content:")
            print(result.final_content[:500] + "...")

    update_monitoring("run-weekly")
    print("Done")


if __name__ == "__main__":
    main()
