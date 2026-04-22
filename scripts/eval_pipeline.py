#!/usr/bin/env python3
"""Dry-run pipeline evaluation — generates content without posting or writing to DB."""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from ingestion.claude_logs import ClaudeLogParser
from synthesis.pipeline import SynthesisPipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate pipeline output (dry run)")
    parser.add_argument("--runs", type=int, default=3, help="Number of pipeline runs")
    parser.add_argument("--type", default="x_thread", help="Content type (x_thread, x_post)")
    args = parser.parse_args()

    with script_context() as (config, db):
        # Initialize embedder
        embedder = None
        semantic_threshold = 0.82
        if config.embeddings:
            from knowledge.embeddings import VoyageEmbeddings
            embedder = VoyageEmbeddings(
                api_key=config.embeddings.api_key, model=config.embeddings.model
            )
            semantic_threshold = config.embeddings.semantic_dedup_threshold

        # Initialize knowledge store for trend context
        knowledge_store = None
        if embedder and config.curated_sources:
            from knowledge.store import KnowledgeStore
            knowledge_store = KnowledgeStore(db.conn, embedder)

        pipeline = SynthesisPipeline(
            api_key=config.anthropic.api_key,
            generator_model=config.synthesis.model,
            evaluator_model=config.synthesis.eval_model,
            db=db,
            num_candidates=config.synthesis.num_candidates,
            anthropic_timeout=config.timeouts.anthropic_seconds,
            embedder=embedder,
            semantic_threshold=semantic_threshold,
            knowledge_store=knowledge_store,
            claim_check_enabled=config.synthesis.claim_check_enabled,
        )

        # Gather prompts and commits for different time windows
        log_parser = ClaudeLogParser(config.paths.claude_logs, config.paths.allowed_projects)
        now = datetime.now(timezone.utc)

        # Time slices: 8h, 16h, 24h (or fewer if --runs < 3)
        hours_slices = [8, 16, 24, 48, 72][:args.runs]

        results = []
        for run_idx, hours in enumerate(hours_slices):
            since = now - timedelta(hours=hours)
            commits = db.get_commits_in_range(since, now)
            prompts = list(log_parser.get_messages_since(since))

            if not commits or not prompts:
                print(f"\n{'='*60}")
                print(f"Run {run_idx + 1}/{args.runs} — last {hours}h: "
                      f"skipped (commits={len(commits)}, prompts={len(prompts)})")
                continue

            commit_dicts = [
                {"sha": c.get("commit_sha", ""), "repo_name": c.get("repo_name", ""),
                 "message": c.get("commit_message", "")}
                for c in commits
            ]
            prompt_texts = [p.prompt_text for p in prompts]

            print(f"\n{'='*60}")
            print(f"Run {run_idx + 1}/{args.runs} — last {hours}h")
            print(f"  Commits: {len(commits)}, Prompts: {len(prompts)}")
            print(f"  Content type: {args.type}")

            # Show avoidance context
            avoidance = pipeline._build_avoidance_context()
            if avoidance:
                print(f"\n  AVOIDANCE CONTEXT:")
                for line in avoidance.strip().split("\n"):
                    print(f"    {line}")
            else:
                print(f"\n  AVOIDANCE CONTEXT: (none — no recent published content)")

            # Run pipeline
            print(f"\n  Running pipeline...")
            result = pipeline.run(
                prompts=prompt_texts,
                commits=commit_dicts,
                content_type=args.type,
                threshold=config.synthesis.eval_threshold,
            )

            # Filter stats
            if result.filter_stats:
                print(f"\n  FILTER STATS:")
                for k, v in result.filter_stats.items():
                    if v:
                        print(f"    {k}: {v}")

            # Candidates
            print(f"\n  CANDIDATES ({len(result.candidates)} survived filtering):")
            for i, c in enumerate(result.candidates):
                preview = c[:120].replace("\n", " | ")
                print(f"    [{i}] {preview}...")

            # Evaluator scores
            comp = result.comparison
            if comp.ranking:
                print(f"\n  EVALUATOR SCORES:")
                print(f"    Best score:     {comp.best_score:.1f}/10")
                print(f"    Groundedness:   {comp.groundedness}/10")
                print(f"    Rawness:        {comp.rawness}/10")
                print(f"    Specificity:    {comp.narrative_specificity}/10")
                print(f"    Voice:          {comp.voice}/10")
                print(f"    Engagement:     {comp.engagement_potential}/10")
                if comp.reject_reason:
                    print(f"    REJECT REASON:  {comp.reject_reason}")
                if comp.improvement:
                    print(f"    Improvement:    {comp.improvement[:100]}...")

            # Refinement
            if result.refinement:
                print(f"\n  REFINEMENT:")
                print(f"    Picked: {result.refinement.picked}")
                print(f"    Score:  {result.refinement.final_score}/10")

            # Final content
            print(f"\n  FINAL CONTENT (score: {result.final_score:.1f}/10):")
            print(f"  {'—'*50}")
            for line in result.final_content.split("\n"):
                print(f"  {line}")
            print(f"  {'—'*50}")

            results.append({
                "run": run_idx + 1,
                "hours": hours,
                "commits": len(commits),
                "prompts": len(prompts),
                "candidates": len(result.candidates),
                "score": result.final_score,
                "filter_stats": result.filter_stats,
                "rejected": comp.reject_reason is not None,
            })

        log_parser.log_skipped_project_counts("eval_pipeline")

        # Summary table
        if results:
            print(f"\n{'='*60}")
            print(f"SUMMARY")
            print(f"{'='*60}")
            print(f"{'Run':>4} {'Window':>8} {'Commits':>8} {'Cands':>6} {'Score':>6} {'Status':>12}")
            print(f"{'—'*4:>4} {'—'*8:>8} {'—'*8:>8} {'—'*6:>6} {'—'*6:>6} {'—'*12:>12}")
            for r in results:
                status = "REJECTED" if r["rejected"] else (
                    "PASS" if r["score"] >= config.synthesis.eval_threshold * 10 else "BELOW"
                )
                print(f"{r['run']:>4} {r['hours']:>6}h {r['commits']:>8} "
                      f"{r['candidates']:>6} {r['score']:>6.1f} {status:>12}")

            avg_score = sum(r["score"] for r in results) / len(results)
            print(f"\n  Average score: {avg_score:.1f}/10")
            print(f"  Threshold: {config.synthesis.eval_threshold * 10}/10")


if __name__ == "__main__":
    main()
