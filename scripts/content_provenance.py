#!/usr/bin/env python3
"""Inspect provenance for one generated content item."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context


def _shorten(value: Any, width: int = 96) -> str:
    if value is None:
        return "-"
    text = str(value).replace("\n", " ")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _content_status(code: int | None) -> str:
    if code == 1:
        return "published"
    if code == -1:
        return "abandoned"
    return "unpublished"


def load_content_provenance(db: Any, content_id: int) -> dict:
    """Return provenance for one content item or raise if missing."""
    provenance = db.get_content_provenance(content_id)
    if provenance is None:
        raise ValueError(f"Content ID {content_id} not found")
    return provenance


def format_json_provenance(provenance: dict) -> str:
    """Format provenance as machine-readable JSON."""
    return json.dumps(provenance, indent=2, sort_keys=True, default=str)


def _format_content_header(content: dict[str, Any]) -> list[str]:
    lines = [
        f"Content #{content.get('id')} ({content.get('content_type')}, {_content_status(content.get('published'))})",
        f"Generated: {content.get('created_at') or '-'}",
        f"Eval: {content.get('eval_score') if content.get('eval_score') is not None else '-'}",
    ]
    if content.get("content_format"):
        lines.append(f"Format: {content['content_format']}")
    if content.get("published_url"):
        lines.append(f"Published URL: {content['published_url']}")
    lines.append(f"Text: {_shorten(content.get('content'), 180)}")
    return lines


def _format_commit_line(commit: dict[str, Any]) -> str:
    label = commit.get("commit_sha")
    if commit.get("matched"):
        label = f"{label} {commit.get('repo_name') or ''}".strip()
    return f"- {label}: {_shorten(commit.get('commit_message'), 90)}"


def _format_message_line(message: dict[str, Any]) -> str:
    prefix = message.get("message_uuid")
    timestamp = message.get("timestamp")
    if timestamp:
        prefix = f"{prefix} @ {timestamp}"
    return f"- {prefix}: {_shorten(message.get('prompt_text'), 90)}"


def _format_activity_line(item: dict[str, Any]) -> str:
    label = item.get("activity_id")
    if item.get("matched"):
        activity_type = "PR" if item.get("activity_type") == "pull_request" else item.get("activity_type")
        label = f"{item.get('repo_name')} {activity_type} #{item.get('number')}"
    return f"- {label}: {_shorten(item.get('title'), 90)}"


def _format_link_line(link: dict[str, Any]) -> str:
    source = link.get("author") or link.get("source_type") or f"knowledge #{link.get('id')}"
    score = link.get("relevance_score")
    score_text = f"{score:.3f}" if isinstance(score, (int, float)) else "-"
    preview = link.get("insight") or link.get("content")
    return f"- {source} ({score_text}): {_shorten(preview, 90)}"


def _format_variant_line(variant: dict[str, Any]) -> str:
    key = f"{variant.get('platform')}/{variant.get('variant_type')}"
    return f"- {key}: {_shorten(variant.get('content'), 90)}"


def _format_publication_line(pub: dict[str, Any]) -> str:
    detail = pub.get("platform_post_id") or pub.get("platform_url") or pub.get("error") or "-"
    return (
        f"- {pub.get('platform')}: {pub.get('status')} "
        f"(attempts {pub.get('attempt_count')}, published {pub.get('published_at') or '-'}) {detail}"
    )


def _format_snapshot_line(snapshot: dict[str, Any]) -> str:
    platform = snapshot["platform"]
    reposts = snapshot.get("retweet_count", snapshot.get("repost_count", 0))
    return (
        f"- {platform} @ {snapshot.get('fetched_at')}: "
        f"likes {snapshot.get('like_count', 0)}, reposts {reposts}, "
        f"replies {snapshot.get('reply_count', 0)}, quotes {snapshot.get('quote_count', 0)}, "
        f"score {snapshot.get('engagement_score')}"
    )


def _format_run_line(run: dict[str, Any]) -> list[str]:
    lines = [
        f"- {run.get('batch_id')} @ {run.get('created_at')}: "
        f"outcome {run.get('outcome') or '-'}, final {run.get('final_score')}, "
        f"published {bool(run.get('published'))}"
    ]
    if run.get("rejection_reason"):
        lines.append(f"  rejection: {_shorten(run['rejection_reason'], 100)}")
    return lines


def _section_lines(
    title: str,
    items: list[dict[str, Any]],
    item_formatter: Any,
    *,
    heading_prefix: str = "",
) -> list[str]:
    lines = [f"\n{heading_prefix}{title} ({len(items)})"]
    if items:
        for item in items:
            formatted = item_formatter(item)
            if isinstance(formatted, list):
                lines.extend(formatted)
            else:
                lines.append(formatted)
    else:
        lines.append("- none")
    return lines


def format_provenance_markdown(provenance: dict) -> str:
    """Format provenance as shareable markdown."""
    content = provenance.get("content", {})
    lines = [f"# Content #{content.get('id')} ({content.get('content_type')}, {_content_status(content.get('published'))})"]
    lines.extend(f"- {line}" for line in _format_content_header(content)[1:])

    lines.extend(
        _section_lines("Source commits", provenance.get("source_commits", []), _format_commit_line, heading_prefix="## ")
    )
    lines.extend(
        _section_lines("Claude messages", provenance.get("source_messages", []), _format_message_line, heading_prefix="## ")
    )
    lines.extend(
        _section_lines("GitHub activity", provenance.get("source_activity", []), _format_activity_line, heading_prefix="## ")
    )
    lines.extend(
        _section_lines("Knowledge links", provenance.get("knowledge_links", []), _format_link_line, heading_prefix="## ")
    )
    lines.extend(_section_lines("Variants", provenance.get("variants", []), _format_variant_line, heading_prefix="## "))
    lines.extend(
        _section_lines("Publications", provenance.get("publications", []), _format_publication_line, heading_prefix="## ")
    )
    lines.extend(
        _section_lines(
            "Engagement snapshots",
            provenance.get("engagement_snapshots", []),
            _format_snapshot_line,
            heading_prefix="## ",
        )
    )
    lines.extend(_section_lines("Pipeline runs", provenance.get("pipeline_runs", []), _format_run_line, heading_prefix="## "))
    return "\n".join(lines)


def format_human_provenance(provenance: dict) -> str:
    """Format provenance as concise human-readable text."""
    content = provenance.get("content", {})
    lines = _format_content_header(content)
    lines.extend(_section_lines("Source commits", provenance.get("source_commits", []), _format_commit_line))
    lines.extend(_section_lines("Claude messages", provenance.get("source_messages", []), _format_message_line))
    lines.extend(_section_lines("GitHub activity", provenance.get("source_activity", []), _format_activity_line))
    lines.extend(_section_lines("Knowledge links", provenance.get("knowledge_links", []), _format_link_line))
    lines.extend(_section_lines("Variants", provenance.get("variants", []), _format_variant_line))
    lines.extend(_section_lines("Publications", provenance.get("publications", []), _format_publication_line))
    lines.extend(_section_lines("Engagement snapshots", provenance.get("engagement_snapshots", []), _format_snapshot_line))
    lines.extend(_section_lines("Pipeline runs", provenance.get("pipeline_runs", []), _format_run_line))

    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("content_id", type=int, help="generated_content.id to inspect")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--json", action="store_true", help="Output machine-readable JSON")
    mode.add_argument("--markdown", action="store_true", help="Output markdown-formatted provenance")
    parser.add_argument("--output", help="Write output to this path instead of stdout")
    return parser.parse_args(argv)


def _render_provenance(provenance: dict, *, json_output: bool, markdown_output: bool) -> str:
    if json_output:
        return format_json_provenance(provenance)
    if markdown_output:
        return format_provenance_markdown(provenance)
    return format_human_provenance(provenance)


def _write_text(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        try:
            provenance = load_content_provenance(db, args.content_id)
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc

    if args.output:
        output_path = Path(args.output).expanduser()
        body = _render_provenance(
            provenance,
            json_output=not args.markdown,
            markdown_output=args.markdown,
        )
        _write_text(output_path, body)
        print(f"Exported provenance bundle to {output_path}", file=sys.stderr)
    elif args.json:
        print(format_json_provenance(provenance))
    elif args.markdown:
        print(format_provenance_markdown(provenance))
    else:
        print(format_human_provenance(provenance))


if __name__ == "__main__":
    main()
