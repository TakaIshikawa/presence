#!/usr/bin/env python3
"""Expand high-performing X threads into reviewable blog drafts."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.blog_writer import BlogWriter
from runner import script_context, update_monitoring
from synthesis.thread_expander import (
    SourceCommit,
    SourceMessage,
    ThreadExpander,
    ThreadExpansionCandidate,
)

logger = logging.getLogger(__name__)


@dataclass
class ExpansionOutcome:
    source_content_id: int
    generated_content_id: int | None
    draft_path: str | None
    dry_run: bool
    success: bool
    error: str | None = None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-engagement", type=float, default=10.0)
    parser.add_argument("--limit", type=int, default=3)
    parser.add_argument("--max-age-days", type=int, default=30)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate drafts and log metadata without writing DB rows or files.",
    )
    return parser.parse_args(argv)


def select_candidates(
    db,
    min_engagement: float = 10.0,
    limit: int = 3,
    max_age_days: int = 30,
) -> list[ThreadExpansionCandidate]:
    """Select published X threads whose latest engagement clears the threshold."""
    cursor = db.conn.execute(
        """SELECT gc.id, gc.content, gc.source_commits, gc.source_messages,
                  gc.published_url, pe.engagement_score
           FROM generated_content gc
           INNER JOIN (
               SELECT content_id, engagement_score,
                      ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY fetched_at DESC) AS rn
               FROM post_engagement
           ) pe ON pe.content_id = gc.id AND pe.rn = 1
           WHERE gc.content_type = 'x_thread'
             AND gc.published = 1
             AND pe.engagement_score >= ?
             AND gc.published_at >= datetime('now', ?)
             AND gc.id NOT IN (
                 SELECT repurposed_from
                 FROM generated_content
                 WHERE repurposed_from IS NOT NULL
                   AND content_type = 'blog_post'
             )
           ORDER BY pe.engagement_score DESC
           LIMIT ?""",
        (min_engagement, f"-{max_age_days} days", limit),
    )

    rows = [dict(row) for row in cursor.fetchall()]
    return [_candidate_from_row(db, row) for row in rows]


def expand_candidates(
    db,
    candidates: list[ThreadExpansionCandidate],
    expander: ThreadExpander,
    blog_writer: BlogWriter,
    dry_run: bool = False,
) -> list[ExpansionOutcome]:
    outcomes: list[ExpansionOutcome] = []

    for candidate in candidates:
        logger.info(
            "Expanding thread #%s (engagement %.1f)",
            candidate.content_id,
            candidate.engagement_score,
        )
        result = expander.expand(candidate)

        if dry_run:
            title = result.content.splitlines()[0] if result.content else "(empty)"
            logger.info("Dry run draft for #%s: %s", candidate.content_id, title)
            outcomes.append(
                ExpansionOutcome(
                    source_content_id=candidate.content_id,
                    generated_content_id=None,
                    draft_path=None,
                    dry_run=True,
                    success=True,
                )
            )
            continue

        generated_id = db.insert_repurposed_content(
            content_type="blog_post",
            source_content_id=candidate.content_id,
            content=result.content,
            eval_score=candidate.engagement_score,
            eval_feedback="Expanded from high-performing X thread.",
        )
        draft_result = blog_writer.write_draft(
            result.content,
            source_content_id=candidate.content_id,
            generated_content_id=generated_id,
        )
        if draft_result.success:
            logger.info("Wrote blog draft: %s", draft_result.file_path)
            outcomes.append(
                ExpansionOutcome(
                    source_content_id=candidate.content_id,
                    generated_content_id=generated_id,
                    draft_path=draft_result.file_path,
                    dry_run=False,
                    success=True,
                )
            )
        else:
            logger.error("Draft write failed for #%s: %s", candidate.content_id, draft_result.error)
            outcomes.append(
                ExpansionOutcome(
                    source_content_id=candidate.content_id,
                    generated_content_id=generated_id,
                    draft_path=None,
                    dry_run=False,
                    success=False,
                    error=draft_result.error,
                )
            )

    return outcomes


def run(args: argparse.Namespace) -> list[ExpansionOutcome]:
    with script_context() as (config, db):
        candidates = select_candidates(
            db,
            min_engagement=args.min_engagement,
            limit=args.limit,
            max_age_days=args.max_age_days,
        )
        if not candidates:
            logger.info("No high-performing X threads found")
            return []

        expander = ThreadExpander(
            api_key=config.anthropic.api_key,
            model=config.synthesis.model,
            timeout=config.timeouts.anthropic_seconds,
        )
        blog_writer = BlogWriter(
            config.paths.static_site,
            default_social_image_path=getattr(
                config.blog, "default_social_image_path", None
            ),
        )
        return expand_candidates(
            db,
            candidates,
            expander,
            blog_writer,
            dry_run=args.dry_run,
        )


def _candidate_from_row(db, row: dict) -> ThreadExpansionCandidate:
    source_commits = _load_json_list(row.get("source_commits"))
    source_messages = _load_json_list(row.get("source_messages"))
    return ThreadExpansionCandidate(
        content_id=row["id"],
        original_thread=row["content"],
        engagement_score=row["engagement_score"] or 0.0,
        source_commits=source_commits,
        source_messages=source_messages,
        commit_context=_load_commit_context(db, source_commits),
        message_context=_load_message_context(db, source_messages),
        published_url=row.get("published_url"),
    )


def _load_json_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed]


def _load_commit_context(db, shas: list[str]) -> list[SourceCommit]:
    if not shas:
        return []
    placeholders = ",".join("?" for _ in shas)
    cursor = db.conn.execute(
        f"""SELECT commit_sha, repo_name, commit_message
            FROM github_commits
            WHERE commit_sha IN ({placeholders})""",
        shas,
    )
    by_sha = {row["commit_sha"]: dict(row) for row in cursor.fetchall()}
    commits = []
    for sha in shas:
        row = by_sha.get(sha)
        if row:
            commits.append(
                SourceCommit(
                    sha=sha,
                    repo_name=row.get("repo_name") or "",
                    commit_message=row.get("commit_message") or "",
                )
            )
    return commits


def _load_message_context(db, message_uuids: list[str]) -> list[SourceMessage]:
    if not message_uuids:
        return []
    placeholders = ",".join("?" for _ in message_uuids)
    cursor = db.conn.execute(
        f"""SELECT message_uuid, project_path, prompt_text
            FROM claude_messages
            WHERE message_uuid IN ({placeholders})""",
        message_uuids,
    )
    by_uuid = {row["message_uuid"]: dict(row) for row in cursor.fetchall()}
    messages = []
    for message_uuid in message_uuids:
        row = by_uuid.get(message_uuid)
        if row:
            messages.append(
                SourceMessage(
                    message_uuid=message_uuid,
                    project_path=row.get("project_path") or "",
                    prompt_text=row.get("prompt_text") or "",
                )
            )
    return messages


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    run(args)
    update_monitoring("expand-threads-to-blog")


if __name__ == "__main__":
    main()
