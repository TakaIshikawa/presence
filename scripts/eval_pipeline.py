#!/usr/bin/env python3
"""Dry-run pipeline evaluation — generates content without posting or writing to DB."""

import argparse
import json
import sys
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from ingestion.claude_logs import ClaudeLogParser
from ingestion.redaction import redact_text
from synthesis.pipeline import SynthesisPipeline


@dataclass(frozen=True)
class MatrixVariant:
    name: str
    prompt_file: Path
    generator_model: str
    evaluator_model: str


@dataclass(frozen=True)
class SourceWindow:
    run: int
    hours: int
    commits: list[dict]
    prompts: list


def _redaction_patterns(config):
    privacy = getattr(config, "privacy", None)
    patterns = getattr(privacy, "redaction_patterns", None)
    if isinstance(patterns, (list, tuple)):
        return patterns
    return None


def _redact_final_content(content: str, config) -> str:
    return redact_text(content, _redaction_patterns(config))


def _parse_variant(spec: str) -> MatrixVariant:
    parts = spec.split(":", 3)
    if len(parts) != 4 or not all(parts):
        raise argparse.ArgumentTypeError(
            "--variant must be NAME:PROMPT_FILE:GENERATOR_MODEL:EVALUATOR_MODEL"
        )

    name, prompt_file, generator_model, evaluator_model = parts
    path = Path(prompt_file).expanduser()
    if not path.exists():
        raise argparse.ArgumentTypeError(f"Prompt file not found: {prompt_file}")
    if not path.is_file():
        raise argparse.ArgumentTypeError(f"Prompt path is not a file: {prompt_file}")

    return MatrixVariant(
        name=name,
        prompt_file=path,
        generator_model=generator_model,
        evaluator_model=evaluator_model,
    )


def _commit_dicts(commits: list[dict]) -> list[dict]:
    return [
        {
            "sha": c.get("commit_sha", ""),
            "repo_name": c.get("repo_name", ""),
            "message": c.get("commit_message", ""),
        }
        for c in commits
    ]


def _prompt_texts(prompts: list) -> list[str]:
    return [p.prompt_text for p in prompts]


def _window_to_dict(window: SourceWindow) -> dict:
    return {
        "run": window.run,
        "hours": window.hours,
        "commit_count": len(window.commits),
        "prompt_count": len(window.prompts),
    }


def _result_status(final_score: float | None, rejection_reason: str | None, threshold: float) -> str:
    if rejection_reason:
        return "REJECTED"
    if final_score is None:
        return "UNKNOWN"
    return "PASS" if final_score >= threshold * 10 else "BELOW"


def _result_to_dict(result, window: SourceWindow, threshold: float, config) -> dict:
    return {
        "run": window.run,
        "hours": window.hours,
        "commit_count": len(window.commits),
        "prompt_count": len(window.prompts),
        "candidate_count": len(result.candidates),
        "final_score": result.final_score,
        "status": _result_status(result.final_score, result.comparison.reject_reason, threshold),
        "rejection_reason": result.comparison.reject_reason,
        "filter_stats": result.filter_stats or {},
        "final_content": _redact_final_content(result.final_content, config),
    }


def _single_run_aggregate(run_rows: list[dict], threshold: float, num_candidates: int) -> dict:
    run_count = len(run_rows)
    if not run_count:
        return {
            "run_count": 0,
            "average_score": None,
            "rejection_rate": 0.0,
            "candidate_survival_rate": 0.0,
            "threshold": threshold,
        }

    avg_score = sum(row["final_score"] for row in run_rows if row["final_score"] is not None) / run_count
    rejection_rate = sum(1 for row in run_rows if row["status"] == "REJECTED") / run_count
    candidate_survival_rate = (
        sum(row["candidate_count"] for row in run_rows) / (run_count * num_candidates)
        if num_candidates else 0.0
    )
    return {
        "run_count": run_count,
        "average_score": avg_score,
        "rejection_rate": rejection_rate,
        "candidate_survival_rate": candidate_survival_rate,
        "threshold": threshold,
    }


def _matrix_variant_aggregate(run_rows: list[dict], num_candidates: int, threshold: float) -> dict:
    run_count = len(run_rows)
    if not run_count:
        return {
            "run_count": 0,
            "average_score": None,
            "rejection_rate": 0.0,
            "candidate_survival_rate": 0.0,
            "threshold": threshold,
        }

    avg_score = sum(row["final_score"] for row in run_rows if row["final_score"] is not None) / run_count
    rejection_rate = sum(1 for row in run_rows if row["status"] == "REJECTED") / run_count
    candidate_survival_rate = (
        sum(row["candidate_count"] for row in run_rows) / (run_count * num_candidates)
        if num_candidates else 0.0
    )
    return {
        "run_count": run_count,
        "average_score": avg_score,
        "rejection_rate": rejection_rate,
        "candidate_survival_rate": candidate_survival_rate,
        "threshold": threshold,
    }


def _matrix_overview(variant_rows: list[dict]) -> dict:
    if not variant_rows:
        return {
            "variant_count": 0,
            "run_count": 0,
            "average_score": None,
            "rejection_rate": 0.0,
            "candidate_survival_rate": 0.0,
            "ranked_variants": [],
        }

    run_count = sum(row["aggregate"]["run_count"] for row in variant_rows)
    average_score = (
        sum(
            row["aggregate"]["average_score"] * row["aggregate"]["run_count"]
            for row in variant_rows
            if row["aggregate"]["average_score"] is not None
        ) / run_count
        if run_count else None
    )
    rejection_rate = (
        sum(row["aggregate"]["rejection_rate"] * row["aggregate"]["run_count"] for row in variant_rows)
        / run_count
        if run_count else 0.0
    )
    candidate_survival_rate = (
        sum(
            row["aggregate"]["candidate_survival_rate"] * row["aggregate"]["run_count"]
            for row in variant_rows
        ) / run_count
        if run_count else 0.0
    )
    ranked_variants = [
        {
            "name": row["name"],
            "batch_id": row["batch_id"],
            "average_score": row["aggregate"]["average_score"],
            "rejection_rate": row["aggregate"]["rejection_rate"],
            "candidate_survival_rate": row["aggregate"]["candidate_survival_rate"],
        }
        for row in variant_rows
    ]
    ranked_variants.sort(
        key=lambda row: (
            -(row["average_score"] if row["average_score"] is not None else float("-inf")),
            row["rejection_rate"],
            -row["candidate_survival_rate"],
            row["name"],
        )
    )
    return {
        "variant_count": len(variant_rows),
        "run_count": run_count,
        "average_score": average_score,
        "rejection_rate": rejection_rate,
        "candidate_survival_rate": candidate_survival_rate,
        "ranked_variants": ranked_variants,
    }


def _write_json_artifact(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _select_source_windows(
    *,
    db,
    log_parser: ClaudeLogParser,
    now: datetime,
    hours_slices: list[int],
    total_runs: int,
) -> list[SourceWindow]:
    windows = []
    for run_idx, hours in enumerate(hours_slices):
        since = now - timedelta(hours=hours)
        commits = db.get_commits_in_range(since, now)
        prompts = list(log_parser.get_messages_since(since))

        if not commits or not prompts:
            print(f"\n{'='*60}")
            print(f"Run {run_idx + 1}/{total_runs} — last {hours}h: "
                  f"skipped (commits={len(commits)}, prompts={len(prompts)})")
            continue

        windows.append(
            SourceWindow(
                run=run_idx + 1,
                hours=hours,
                commits=commits,
                prompts=prompts,
            )
        )
    return windows


def _build_pipeline(
    *,
    config,
    db,
    embedder,
    semantic_threshold: float,
    knowledge_store,
    generator_model: str,
    evaluator_model: str,
) -> SynthesisPipeline:
    return SynthesisPipeline(
        api_key=config.anthropic.api_key,
        generator_model=generator_model,
        evaluator_model=evaluator_model,
        db=db,
        num_candidates=config.synthesis.num_candidates,
        anthropic_timeout=config.timeouts.anthropic_seconds,
        embedder=embedder,
        semantic_threshold=semantic_threshold,
        knowledge_store=knowledge_store,
        format_weighting_enabled=config.synthesis.format_weighting_enabled,
        format_cooldown_recent_posts=config.synthesis.format_cooldown_recent_posts,
        format_cooldown_penalty=config.synthesis.format_cooldown_penalty,
        claim_check_enabled=config.synthesis.claim_check_enabled,
        persona_guard_enabled=config.synthesis.persona_guard_enabled,
        persona_guard_min_score=config.synthesis.persona_guard_min_score,
        persona_guard_min_phrase_overlap=config.synthesis.persona_guard_min_phrase_overlap,
        persona_guard_max_banned_markers=config.synthesis.persona_guard_max_banned_markers,
        persona_guard_max_abstraction_ratio=config.synthesis.persona_guard_max_abstraction_ratio,
        persona_guard_min_grounding_score=config.synthesis.persona_guard_min_grounding_score,
        persona_guard_recent_limit=config.synthesis.persona_guard_recent_limit,
        persona_guard_min_recent_posts=config.synthesis.persona_guard_min_recent_posts,
        restricted_prompt_behavior=getattr(
            config.curated_sources, "restricted_prompt_behavior", "strict"
        ) if config.curated_sources else "strict",
        feedback_lookback_days=config.synthesis.feedback_lookback_days,
        feedback_max_items=config.synthesis.feedback_max_items,
    )


def _apply_prompt_variant(pipeline: SynthesisPipeline, content_type: str, prompt_file: Path) -> str:
    type_config = pipeline.generator.CONTENT_TYPE_CONFIG.get(
        content_type,
        pipeline.generator.CONTENT_TYPE_CONFIG["x_post"],
    )
    prompt_type = type_config["template"]
    pipeline.generator.set_prompt_file_override(prompt_type, prompt_file)
    return prompt_type


def _print_run_result(result, threshold: float) -> None:
    if result.filter_stats:
        print(f"\n  FILTER STATS:")
        for k, v in result.filter_stats.items():
            if v:
                print(f"    {k}: {v}")

    print(f"\n  CANDIDATES ({len(result.candidates)} survived filtering):")
    for i, c in enumerate(result.candidates):
        preview = c[:120].replace("\n", " | ")
        print(f"    [{i}] {preview}...")

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

    if result.refinement:
        print(f"\n  REFINEMENT:")
        print(f"    Picked: {result.refinement.picked}")
        print(f"    Score:  {result.refinement.final_score}/10")

    print(f"\n  FINAL CONTENT (score: {result.final_score:.1f}/10):")
    print(f"  {'—'*50}")
    for line in result.final_content.split("\n"):
        print(f"  {line}")
    print(f"  {'—'*50}")


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


def _record_eval_result(
    *,
    db,
    batch_id: int,
    content_type: str,
    generator_model: str,
    evaluator_model: str,
    threshold: float,
    window: SourceWindow,
    result,
    config,
) -> None:
    db.add_eval_result(
        batch_id=batch_id,
        content_type=content_type,
        generator_model=generator_model,
        evaluator_model=evaluator_model,
        threshold=threshold,
        source_window_hours=window.hours,
        prompt_count=len(window.prompts),
        commit_count=len(window.commits),
        candidate_count=len(result.candidates),
        final_score=result.final_score,
        rejection_reason=result.comparison.reject_reason,
        filter_stats=result.filter_stats,
        final_content=_redact_final_content(result.final_content, config),
    )


def _run_single(
    args,
    config,
    db,
    embedder,
    semantic_threshold,
    knowledge_store,
    log_parser,
):
    pipeline = _build_pipeline(
        config=config,
        db=db,
        embedder=embedder,
        semantic_threshold=semantic_threshold,
        knowledge_store=knowledge_store,
        generator_model=config.synthesis.model,
        evaluator_model=config.synthesis.eval_model,
    )
    now = datetime.now(timezone.utc)
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

    windows = _select_source_windows(
        db=db,
        log_parser=log_parser,
        now=now,
        hours_slices=hours_slices,
        total_runs=args.runs,
    )

    results = []
    artifact_rows = []
    for window in windows:
        print(f"\n{'='*60}")
        print(f"Run {window.run}/{args.runs} — last {window.hours}h")
        print(f"  Commits: {len(window.commits)}, Prompts: {len(window.prompts)}")
        print(f"  Content type: {args.type}")

        avoidance = pipeline._build_avoidance_context()
        if avoidance:
            print(f"\n  AVOIDANCE CONTEXT:")
            for line in avoidance.strip().split("\n"):
                print(f"    {line}")
        else:
            print(f"\n  AVOIDANCE CONTEXT: (none — no recent published content)")

        print(f"\n  Running pipeline...")
        result = pipeline.run(
            prompts=_prompt_texts(window.prompts),
            commits=_commit_dicts(window.commits),
            content_type=args.type,
            threshold=config.synthesis.eval_threshold,
        )
        _print_run_result(result, config.synthesis.eval_threshold)

        if batch_id is not None:
            _record_eval_result(
                db=db,
                batch_id=batch_id,
                content_type=args.type,
                generator_model=config.synthesis.model,
                evaluator_model=config.synthesis.eval_model,
                threshold=config.synthesis.eval_threshold,
                window=window,
                result=result,
                config=config,
            )
            print(f"\n  Recorded result in batch {batch_id}")

        row = {
            "window": window,
            "result": result,
        }
        results.append(row)
        artifact_rows.append(_result_to_dict(result, window, config.synthesis.eval_threshold, config))

    log_parser.log_skipped_project_counts("eval_pipeline")

    if results:
        summary_rows = []
        for row in results:
            window = row["window"]
            result = row["result"]
            summary_rows.append({
                "run": window.run,
                "hours": window.hours,
                "commits": len(window.commits),
                "candidates": len(result.candidates),
                "score": result.final_score,
                "rejected": result.comparison.reject_reason is not None,
            })

        print(f"\n{'='*60}")
        print(f"SUMMARY")
        print(f"{'='*60}")
        print(f"{'Run':>4} {'Window':>8} {'Commits':>8} {'Cands':>6} {'Score':>6} {'Status':>12}")
        print(f"{'—'*4:>4} {'—'*8:>8} {'—'*8:>8} {'—'*6:>6} {'—'*6:>6} {'—'*12:>12}")
        for r in summary_rows:
            status = "REJECTED" if r["rejected"] else (
                "PASS" if r["score"] >= config.synthesis.eval_threshold * 10 else "BELOW"
            )
            print(
                f"{r['run']:>4} {r['hours']:>6}h {r['commits']:>8} "
                f"{r['candidates']:>6} {r['score']:>6.1f} {status:>12}"
            )

        avg_score = sum(r["score"] for r in summary_rows) / len(summary_rows)
        print(f"\n  Average score: {avg_score:.1f}/10")
        print(f"  Threshold: {config.synthesis.eval_threshold * 10}/10")

        if args.out:
            artifact = {
                "schema_version": 1,
                "mode": "single",
                "label": args.label,
                "content_type": args.type,
                "recorded": bool(batch_id is not None),
                "batch_id": batch_id,
                "generator_model": config.synthesis.model,
                "evaluator_model": config.synthesis.eval_model,
                "source_windows": [_window_to_dict(window) for window in windows],
                "runs": artifact_rows,
                "aggregate": {
                    **_single_run_aggregate(
                        artifact_rows,
                        config.synthesis.eval_threshold,
                        config.synthesis.num_candidates,
                    ),
                    "batch_id": batch_id,
                },
            }
            _write_json_artifact(args.out, artifact)
    elif args.out:
        artifact = {
            "schema_version": 1,
            "mode": "single",
            "label": args.label,
            "content_type": args.type,
            "recorded": bool(batch_id is not None),
            "batch_id": batch_id,
            "generator_model": config.synthesis.model,
            "evaluator_model": config.synthesis.eval_model,
            "source_windows": [_window_to_dict(window) for window in windows],
            "runs": artifact_rows,
            "aggregate": {
                **_single_run_aggregate(
                    artifact_rows,
                    config.synthesis.eval_threshold,
                    config.synthesis.num_candidates,
                ),
                "batch_id": batch_id,
            },
        }
        _write_json_artifact(args.out, artifact)


def _run_matrix(
    args,
    config,
    db,
    embedder,
    semantic_threshold,
    knowledge_store,
    log_parser,
):
    variants = args.variant or []
    if not variants:
        raise SystemExit("--matrix requires at least one --variant")

    now = datetime.now(timezone.utc)
    hours_slices = [8, 16, 24, 48, 72][:args.runs]
    parent_label = args.label or f"matrix-{now.strftime('%Y%m%d-%H%M%S')}"
    windows = _select_source_windows(
        db=db,
        log_parser=log_parser,
        now=now,
        hours_slices=hours_slices,
        total_runs=args.runs,
    )

    print(f"\nMatrix label: {parent_label}")
    print(f"Selected windows: {', '.join(f'{w.hours}h' for w in windows) or '(none)'}")

    summaries = []
    for variant in variants:
        pipeline = _build_pipeline(
            config=config,
            db=db,
            embedder=embedder,
            semantic_threshold=semantic_threshold,
            knowledge_store=knowledge_store,
            generator_model=variant.generator_model,
            evaluator_model=variant.evaluator_model,
        )
        prompt_type = _apply_prompt_variant(pipeline, args.type, variant.prompt_file)
        batch_label = f"{parent_label}/{variant.name}"
        batch_id = db.create_eval_batch(
            label=batch_label,
            content_type=args.type,
            generator_model=variant.generator_model,
            evaluator_model=variant.evaluator_model,
            threshold=config.synthesis.eval_threshold,
        )

        print(f"\n{'='*60}")
        print(f"Variant {variant.name} — batch {batch_id}")
        print(f"  Prompt: {variant.prompt_file} ({prompt_type})")
        print(f"  Generator: {variant.generator_model}")
        print(f"  Evaluator: {variant.evaluator_model}")

        variant_results = []
        for window in windows:
            print(f"\n  Run {window.run}/{args.runs} — last {window.hours}h")
            print(f"    Commits: {len(window.commits)}, Prompts: {len(window.prompts)}")
            result = pipeline.run(
                prompts=_prompt_texts(window.prompts),
                commits=_commit_dicts(window.commits),
                content_type=args.type,
                threshold=config.synthesis.eval_threshold,
            )
            comp = result.comparison
            status = "REJECTED" if comp.reject_reason else (
                "PASS" if result.final_score >= config.synthesis.eval_threshold * 10 else "BELOW"
            )
            print(
                f"    Score: {result.final_score:.1f}/10, "
                f"Candidates: {len(result.candidates)}/{config.synthesis.num_candidates}, "
                f"Status: {status}"
            )
            _record_eval_result(
                db=db,
                batch_id=batch_id,
                content_type=args.type,
                generator_model=variant.generator_model,
                evaluator_model=variant.evaluator_model,
                threshold=config.synthesis.eval_threshold,
                window=window,
                result=result,
                config=config,
            )
            variant_results.append({
                "window": window,
                "result": result,
            })

        run_rows = [
            _result_to_dict(
                row["result"],
                row["window"],
                config.synthesis.eval_threshold,
                config,
            )
            for row in variant_results
        ]
        aggregate = _matrix_variant_aggregate(
            run_rows,
            config.synthesis.num_candidates,
            config.synthesis.eval_threshold,
        )
        summaries.append({
            "name": variant.name,
            "batch_id": batch_id,
            "prompt_file": str(variant.prompt_file),
            "prompt_type": prompt_type,
            "generator_model": variant.generator_model,
            "evaluator_model": variant.evaluator_model,
            "runs": run_rows,
            "aggregate": aggregate,
        })

    log_parser.log_skipped_project_counts("eval_pipeline")

    summaries.sort(
        key=lambda row: (
            -(row["aggregate"]["average_score"] if row["aggregate"]["average_score"] is not None else float("-inf")),
            row["aggregate"]["rejection_rate"],
            -row["aggregate"]["candidate_survival_rate"],
            row["name"],
        )
    )
    print(f"\n{'='*60}")
    print("MATRIX SUMMARY")
    print(f"{'='*60}")
    print(f"{'Rank':>4} {'Variant':<24} {'Batch':>6} {'Runs':>4} {'Avg':>6} {'Reject':>8} {'Survive':>8}")
    print(f"{'—'*4:>4} {'—'*24:<24} {'—'*6:>6} {'—'*4:>4} {'—'*6:>6} {'—'*8:>8} {'—'*8:>8}")
    for idx, row in enumerate(summaries, start=1):
        print(
            f"{idx:>4} {row['name'][:24]:<24} {row['batch_id']:>6} {row['aggregate']['run_count']:>4} "
            f"{row['aggregate']['average_score']:>6.1f} {row['aggregate']['rejection_rate']*100:>7.0f}% "
            f"{row['aggregate']['candidate_survival_rate']*100:>7.0f}%"
        )

    if args.out:
        artifact = {
            "schema_version": 1,
            "mode": "matrix",
            "label": parent_label,
            "content_type": args.type,
            "recorded": True,
            "source_windows": [_window_to_dict(window) for window in windows],
            "variants": summaries,
            "aggregate": _matrix_overview(summaries),
        }
        _write_json_artifact(args.out, artifact)


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate pipeline output (dry run)")
    parser.add_argument("--runs", type=int, default=3, help="Number of pipeline runs")
    parser.add_argument("--type", default="x_thread", help="Content type (x_thread, x_post)")
    parser.add_argument("--out", type=Path, help="Write a JSON summary artifact to this path")
    parser.add_argument("--record", action="store_true", help="Record this evaluation batch")
    parser.add_argument("--label", help="Optional label for a recorded evaluation batch")
    parser.add_argument("--list", action="store_true", help="List recent recorded evaluation batches")
    parser.add_argument("--show", type=int, metavar="BATCH_ID", help="Show a recorded evaluation batch")
    parser.add_argument("--matrix", action="store_true", help="Compare prompt/model variants")
    parser.add_argument(
        "--variant",
        action="append",
        type=_parse_variant,
        metavar="NAME:PROMPT_FILE:GENERATOR_MODEL:EVALUATOR_MODEL",
        help="Variant for --matrix; repeat for each prompt/model combination",
    )
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

        log_parser = ClaudeLogParser(
            config.paths.claude_logs,
            config.paths.allowed_projects,
            redaction_patterns=config.privacy.redaction_patterns,
        )
        if args.matrix:
            _run_matrix(
                args, config, db, embedder, semantic_threshold, knowledge_store, log_parser
            )
        else:
            _run_single(
                args, config, db, embedder, semantic_threshold, knowledge_store, log_parser
            )


if __name__ == "__main__":
    main()
