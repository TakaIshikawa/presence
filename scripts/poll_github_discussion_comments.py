#!/usr/bin/env python3
"""Poll GitHub discussion comments."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.github_discussion_comments import poll_new_discussion_comments
from runner import script_context, update_monitoring

logger = logging.getLogger(__name__)


def parse_since(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def determine_since(
    db,
    explicit_since: datetime | None,
    lookback_minutes: int,
    state_file: str | None = None,
) -> datetime:
    if explicit_since:
        return explicit_since
    state_since = read_state_file(state_file)
    if state_since:
        return state_since
    method = getattr(db, "get_last_github_discussion_comment_poll_time", None)
    last_poll = method() if method else None
    if last_poll:
        if last_poll.tzinfo is None:
            last_poll = last_poll.replace(tzinfo=timezone.utc)
        return last_poll
    return datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)


def read_state_file(path: str | None) -> datetime | None:
    if not path:
        return None
    state_path = Path(path)
    if not state_path.exists():
        return None
    data = json.loads(state_path.read_text() or "{}")
    value = data.get("last_poll_time") if isinstance(data, dict) else None
    return parse_since(value)


def write_state_file(path: str | None, poll_time: datetime) -> None:
    if not path:
        return
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps({"last_poll_time": poll_time.isoformat()}, indent=2) + "\n")


def ingest_github_discussion_comments(
    db,
    token: str,
    username: str,
    since: datetime,
    repositories: list[str | dict] | None = None,
    dry_run: bool = False,
    limit: int = 100,
    timeout: int = 30,
    redaction_patterns: list[str | dict] | None = None,
) -> list:
    return poll_new_discussion_comments(
        token=token,
        username=username,
        since=since,
        db=db,
        repositories=repositories,
        dry_run=dry_run,
        limit_per_repo=limit,
        timeout=timeout,
        redaction_patterns=redaction_patterns,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Poll recently updated GitHub discussion comments.")
    parser.add_argument(
        "--repo",
        action="append",
        help="Repository to poll as owner/name. May be passed more than once.",
    )
    parser.add_argument(
        "--since",
        help="ISO timestamp to poll from. Defaults to the state file, DB watermark, or lookback window.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum discussion comments to inspect per repository (default: 100).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print proposed discussion comments without writing to the database.",
    )
    parser.add_argument(
        "--state-file",
        help="JSON file storing last_poll_time. Takes precedence over the DB watermark.",
    )
    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=1440,
        help="Initial lookback window when no previous discussion comment poll exists.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)

    with script_context() as (config, db):
        since = determine_since(db, parse_since(args.since), args.lookback_minutes, args.state_file)
        current_poll_time = datetime.now(timezone.utc)
        repositories = args.repo or getattr(config.github, "repositories", None) or None

        logger.info("Polling GitHub discussion comments since %s", since.isoformat())
        if repositories:
            logger.info("Using %d repositories", len(repositories))

        comments = ingest_github_discussion_comments(
            db=db,
            token=config.github.token,
            username=config.github.username,
            since=since,
            repositories=repositories,
            dry_run=args.dry_run,
            limit=args.limit,
            timeout=config.timeouts.github_seconds,
            redaction_patterns=config.privacy.redaction_patterns,
        )

        for comment in comments:
            line = (
                f"{'Would ingest' if args.dry_run else 'Ingested'} "
                f"{comment.activity_id} discussion=#{comment.discussion_number} "
                f"{comment.updated_at.isoformat()} {comment.url}"
            )
            if args.dry_run:
                print(line)
            logger.info(line)

        if args.dry_run:
            logger.info("Dry run complete. %d new/updated discussion comments found.", len(comments))
        else:
            if args.state_file:
                write_state_file(args.state_file, current_poll_time)
            else:
                method = getattr(db, "set_last_github_discussion_comment_poll_time", None)
                if method:
                    method(current_poll_time)
            update_monitoring("poll-github-discussion-comments")
            logger.info("Done. Ingested %d GitHub discussion comments.", len(comments))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
