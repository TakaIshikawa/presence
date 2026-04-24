#!/usr/bin/env python3
"""Preview what a generated content item would publish to X and Bluesky."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.preview import (  # noqa: E402
    PreviewRecordNotFound,
    build_publication_preview,
    format_preview,
    preview_to_json,
)
from output.license_guard import (  # noqa: E402
    restricted_prompt_behavior_from_config,
)
from output.linkedin_export import (  # noqa: E402
    LinkedInExportError,
    LinkedInExportOptions,
    build_linkedin_export_from_db,
    write_linkedin_markdown,
)
from runner import script_context  # noqa: E402


def _alt_text_guard_mode(config: object) -> str:
    publishing = getattr(config, "publishing", None)
    mode = getattr(publishing, "alt_text_guard_mode", "strict")
    if mode in {"strict", "warning"}:
        return mode
    return "strict"


def _alt_text_guard_messages(preview: dict) -> list[str]:
    alt_text = preview.get("alt_text") or {}
    if alt_text.get("passed", True):
        return []
    return [
        f"{issue['code']}: {issue['message']}"
        for issue in alt_text.get("issues", [])
    ]


def _enforce_alt_text_guard(preview: dict, config: object) -> bool:
    messages = _alt_text_guard_messages(preview)
    if not messages:
        return True

    mode = _alt_text_guard_mode(config)
    prefix = "Alt text guard failed" if mode == "strict" else "Alt text guard warning"
    print(f"{prefix}:", file=sys.stderr)
    for message in messages:
        print(f"- {message}", file=sys.stderr)
    return mode != "strict"


def _license_guard_messages(preview: dict) -> list[str]:
    license_guard = preview.get("license_guard") or {}
    return [
        "knowledge {knowledge_id}: {license} {source_url}".format(
            knowledge_id=source["knowledge_id"],
            license=source["license"],
            source_url=source.get("source_url") or "no source URL",
        )
        for source in license_guard.get("restricted_sources", [])
    ]


def _enforce_license_guard(preview: dict) -> bool:
    license_guard = preview.get("license_guard") or {}
    messages = _license_guard_messages(preview)
    if not messages:
        return True

    if license_guard.get("blocked"):
        print("License guard blocked:", file=sys.stderr)
    else:
        print("License guard warning:", file=sys.stderr)
    for message in messages:
        print(f"- {message}", file=sys.stderr)
    return not license_guard.get("blocked")


def _attribution_guard_messages(preview: dict) -> list[str]:
    attribution_guard = preview.get("attribution_guard") or {}
    return [
        "knowledge {knowledge_id}: {license} {author} {source_url}".format(
            knowledge_id=source["knowledge_id"],
            license=source["license"],
            author=source.get("author") or "unknown author",
            source_url=source.get("source_url") or "no source URL",
        )
        for source in attribution_guard.get("missing_sources", [])
    ]


def _enforce_attribution_guard(preview: dict) -> bool:
    attribution_guard = preview.get("attribution_guard") or {}
    messages = _attribution_guard_messages(preview)
    if not messages:
        return True

    if attribution_guard.get("blocked"):
        print("Attribution guard blocked:", file=sys.stderr)
    else:
        print("Attribution guard warning:", file=sys.stderr)
    for message in messages:
        print(f"- {message}", file=sys.stderr)
    return not attribution_guard.get("blocked")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--content-id", type=int, help="generated_content id to preview")
    target.add_argument("--queue-id", type=int, help="publish_queue id to preview")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON, including full evidence, instead of the text preview",
    )
    parser.add_argument(
        "--linkedin-out",
        type=Path,
        help="Write a LinkedIn-ready markdown artifact to this path without publishing",
    )
    parser.add_argument(
        "--linkedin-max-length",
        type=int,
        default=3000,
        help="Maximum LinkedIn post length in graphemes for --linkedin-out",
    )
    parser.add_argument(
        "--suggest-hashtags",
        action="store_true",
        help="Show deterministic hashtag suggestions in the preview without saving them",
    )
    parser.add_argument(
        "--refresh-variants",
        action="store_true",
        help="Regenerate and store deterministic Bluesky and LinkedIn variants before previewing",
    )
    parser.add_argument(
        "--allow-restricted-knowledge",
        action="store_true",
        help="Allow publication preview for content linked to restricted knowledge",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.WARNING)

    with script_context() as (config, db):
        try:
            preview = build_publication_preview(
                db,
                content_id=args.content_id,
                queue_id=args.queue_id,
                include_hashtag_suggestions=args.suggest_hashtags,
                restricted_prompt_behavior=restricted_prompt_behavior_from_config(config),
                allow_restricted_knowledge=args.allow_restricted_knowledge,
                refresh_variants=args.refresh_variants,
            )
        except PreviewRecordNotFound as exc:
            print(str(exc), file=sys.stderr)
            return 1

        if not _enforce_license_guard(preview):
            return 1

        if not _enforce_attribution_guard(preview):
            return 1

        if not _enforce_alt_text_guard(preview, config):
            return 1

        try:
            if args.linkedin_out:
                linkedin_export = build_linkedin_export_from_db(
                    db,
                    content_id=args.content_id,
                    queue_id=args.queue_id,
                    options=LinkedInExportOptions(
                        max_length=args.linkedin_max_length,
                    ),
                )
                write_linkedin_markdown(linkedin_export, args.linkedin_out)
        except LinkedInExportError as exc:
            print(str(exc), file=sys.stderr)
            return 1

    print(preview_to_json(preview) if args.json else format_preview(preview))
    if args.linkedin_out:
        print(f"LinkedIn artifact: {args.linkedin_out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
