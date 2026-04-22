#!/usr/bin/env python3
"""Dry-run pipeline evaluation — generates content without posting or writing to DB."""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from ingestion.claude_logs import ClaudeLogParser
from ingestion.redaction import redact_text
from synthesis.pipeline import SynthesisPipeline


def _redaction_patterns(config):
    privacy = getattr(config, "privacy", None)
    patterns = getattr(privacy, "redaction_patterns", None)
    if isinstance(patterns, (list, tuple)):
        return patterns
    return None


def _redact_final_content(content: str, config) -> str:
    return redact_text(content, _redaction_patterns(config))


def _print_batch_list(batches: list[dict]) -> None:
    if not batches:
        print("No evaluation batches recorded.")
        return

    print(f"{'ID':>4} {'Created':<19} {'Type':<10} {'Label':<24} {'Runs':>4} {'Avg':>6} {'Best':>6}")
    print(f"{'—'*4:>4} {'—'*19:<19} {'—'*10:<10} {'—'*24:<24} {'—'*4:>4} {'—'*6:>6} {'—'*6:>6}")
    for batch in batches:
        label = (batch.get("label") or "")[:24]
        avg = batch.get("average_score")
        best = batch.get("best_score")
        avg_text = f"{avg:.1f}" if avg is not None else "-"
        best_text = f"{best:.1f}" if best is not None else "-"
        print(
            f"{batch['id']:>4} {batch['created_at'][:19]:<19} "
            f"{batch['content_type']:<10} {label:<24} "
            f"{batch['result_count']:>4} {avg_text:>6} {best_text:>6}"
        )


def _print_batch_detail(payload: dict | None) -> None:
    if not payload:
        print("Evaluation batch not found.")
        return

    batch = payload["batch"]
    results = payload["results"]
    print(f"Batch {batch['id']}: {batch.get('label') or '(unlabeled)'}")
    print(f"  Created:         {batch['created_at']}")
    print(f"  Content type:    {batch['content_type']}")
    print(f"  Generator model: {batch['generator_model']}")
    print(f"  Evaluator model: {batch['evaluator_model']}")
    print(f"  Threshold:       {batch['threshold'] * 10:.1f}/10")

    if not results:
        print("\n  No results recorded.")
        return

    print(f"\n{'ID':>4} {'Window':>8} {'Prompts':>8} {'Commits':>8} {'Cands':>6} {'Score':>6} {'Status':>12}")
    print(f"{'—'*4:>4} {'—'*8:>8} {'—'*8:>8} {'—'*8:>8} {'—'*6:>6} {'—'*6:>6} {'—'*12:>12}")
    for result in results:
        rejected = result.get("rejection_reason") is not None
        score = result.get("final_score") or 0.0
        status = "REJECTED" if rejected else (
            "PASS" if score >= result["threshold"] * 10 else "BELOW"
        )
        print(
            f"{result['id']:>4} {result['source_window_hours']:>6}h "
            f"{result['prompt_count']:>8} {result['commit_count']:>8} "
            f"{result['candidate_count']:>6} {score:>6.1f} {status:>12}"
        )

    for result in results:
        print(f"\nResult {result['id']} — last {result['source_window_hours']}h")
        if result.get("rejection_reason"):
            print(f"  Rejection reason: {result['rejection_reason']}")
        if result.get("filter_stats"):
            print("  Filter stats:")
            for k, v in result["filter_stats"].items():
                if v:
                    print(f"    {k}: {v}")
        print("  Final content:")
        for line in (result.get("final_content") or "").split("\n"):
            print(f"    {line}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate pipeline output (dry run)")
    parser.add_argument("--runs", type=int, default=3, help="Number of pipeline runs")
    parser.add_argument("--type", default="x_thread", help="Content type (x_thread, x_post)")
    parser.add_argument("--record", action="store_true", help="Record this evaluation batch")
    parser.add_argument("--label", help="Optional label for a recorded evaluation batch")
    parser.add_argument("--list", action="store_true", help="List recent recorded evaluation batches")
    parser.add_argument("--show", type=int, metavar="BATCH_ID", help="Show a recorded evaluation batch")
    args = parser.parse_args()

    with script_context() as (config, db):
        if args.list:
            _print_batch_list(db.list_eval_batches())
            return

        if args.show is not None:
            _print_batch_detail(db.get_eval_batch(args.show))
            return

        # Initialize embedder
        embedder = None
        semantic_threshold = 0.82
        if config.embeddings:
            semantic_threshold = config.embeddings.semantic_dedup_threshold
            try:
                from knowledge.embeddings import VoyageEmbeddings
                embedder = VoyageEmbeddings(
                    api_key=config.embeddings.api_key, model=config.embeddings.model
                )
            except ModuleNotFoundError as exc:
                if exc.name != "voyageai":
                    raise
                print("Embeddings disabled: voyageai is not installed.")

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
            restricted_prompt_behavior=getattr(
                config.curated_sources, "restricted_prompt_behavior", "strict"
            ) if config.curated_sources else "strict",
        )

        # Gather prompts and commits for different time windows
        log_parser = ClaudeLogParser(
            config.paths.claude_logs,
            config.paths.allowed_projects,
            redaction_patterns=config.privacy.redaction_patterns,
        )
        now = datetime.now(timezone.utc)

        # Time slices: 8h, 16h, 24h (or fewer if --runs < 3)
        hours_slices = [8, 16, 24, 48, 72][:args.runs]

        batch_id = None
        if args.record:
            batch_id = db.create_eval_batch(
                label=args.label,
                content_type=args.type,
                generator_model=config.synthesis.model,
                evaluator_model=config.synthesis.eval_model,
                threshold=config.synthesis.eval_threshold,
            )
            print(f"Recording evaluation batch {batch_id}")

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

            if batch_id is not None:
                db.add_eval_result(
                    batch_id=batch_id,
                    content_type=args.type,
                    generator_model=config.synthesis.model,
                    evaluator_model=config.synthesis.eval_model,
                    threshold=config.synthesis.eval_threshold,
                    source_window_hours=hours,
                    prompt_count=len(prompts),
                    commit_count=len(commits),
                    candidate_count=len(result.candidates),
                    final_score=result.final_score,
                    rejection_reason=comp.reject_reason,
                    filter_stats=result.filter_stats,
                    final_content=_redact_final_content(result.final_content, config),
                )
                print(f"\n  Recorded result in batch {batch_id}")

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
