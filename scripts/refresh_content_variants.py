#!/usr/bin/env python3
"""Refresh deterministic platform copy variants for generated content."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.platform_adapter import (  # noqa: E402
    SUPPORTED_ADAPTER_PLATFORMS,
    build_platform_adapter,
    count_graphemes,
)
from runner import script_context  # noqa: E402
from storage.db import Database  # noqa: E402


DEFAULT_CONTENT_TYPES = ("x_post", "x_thread", "blog_seed")
VARIANT_TYPE = "post"


@dataclass(frozen=True)
class VariantRefreshOptions:
    platforms: tuple[str, ...] = SUPPORTED_ADAPTER_PLATFORMS
    content_type: str | None = None
    content_id: int | None = None
    limit: int = 50
    dry_run: bool = False


@dataclass
class VariantRefreshResult:
    created: int = 0
    updated: int = 0
    unchanged: int = 0
    skipped: int = 0
    variants: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "created": self.created,
            "updated": self.updated,
            "unchanged": self.unchanged,
            "skipped": self.skipped,
            "variants": self.variants,
        }


def refresh_content_variants(
    db: Database,
    options: VariantRefreshOptions,
) -> VariantRefreshResult:
    """Regenerate deterministic variants and upsert changed rows."""
    result = VariantRefreshResult()
    content_rows = _select_content_rows(db, options)
    platforms = _unique_platforms(options.platforms)

    for content in content_rows:
        for platform in platforms:
            operation = _refresh_one_variant(db, content, platform, options.dry_run)
            _record_operation(result, operation)

    return result


def _select_content_rows(db: Database, options: VariantRefreshOptions) -> list[dict[str, Any]]:
    if options.content_id is not None:
        content = db.get_generated_content(options.content_id)
        return [content] if content else []

    content_types = (options.content_type,) if options.content_type else DEFAULT_CONTENT_TYPES
    return db.list_generated_content_for_variant_refresh(
        limit=options.limit,
        content_types=content_types,
    )


def _unique_platforms(platforms: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    unique: list[str] = []
    for platform in platforms:
        normalized = platform.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return tuple(unique)


def _refresh_one_variant(
    db: Database,
    content: dict[str, Any],
    platform: str,
    dry_run: bool,
) -> dict[str, Any]:
    content_id = content["id"]
    base_text = content.get("content") or ""
    content_type = content.get("content_type") or ""
    operation = {
        "content_id": content_id,
        "platform": platform,
        "variant_type": VARIANT_TYPE,
        "content_type": content_type,
    }

    if not base_text.strip():
        return {**operation, "status": "skipped", "reason": "empty_content"}

    try:
        adapter = build_platform_adapter(platform)
    except ValueError as exc:
        return {**operation, "status": "skipped", "reason": str(exc)}

    variant_text = adapter.adapt(base_text, content_type=content_type)
    metadata = _variant_metadata(content, platform, adapter.__class__.__name__, variant_text)
    existing = db.get_content_variant(content_id, platform, VARIANT_TYPE)

    operation = {
        **operation,
        "content": variant_text,
        "metadata": metadata,
        "graphemes": count_graphemes(variant_text),
    }

    if existing is None:
        status = "created"
    elif existing["content"] == variant_text and existing.get("metadata", {}) == metadata:
        status = "unchanged"
    else:
        status = "updated"

    if not dry_run and status in {"created", "updated"}:
        variant_id = db.upsert_content_variant(
            content_id,
            platform,
            VARIANT_TYPE,
            variant_text,
            metadata,
        )
        operation["variant_id"] = variant_id
    elif existing is not None:
        operation["variant_id"] = existing["id"]

    return {**operation, "status": status}


def _variant_metadata(
    content: dict[str, Any],
    platform: str,
    adapter_name: str,
    variant_text: str,
) -> dict[str, Any]:
    return {
        "source": "deterministic_platform_adapter",
        "adapter": adapter_name,
        "platform": platform,
        "content_type": content.get("content_type"),
        "variant_type": VARIANT_TYPE,
        "original_graphemes": count_graphemes(content.get("content") or ""),
        "variant_graphemes": count_graphemes(variant_text),
    }


def _record_operation(result: VariantRefreshResult, operation: dict[str, Any]) -> None:
    status = operation["status"]
    if status == "created":
        result.created += 1
    elif status == "updated":
        result.updated += 1
    elif status == "unchanged":
        result.unchanged += 1
    elif status == "skipped":
        result.skipped += 1
    else:
        raise ValueError(f"Unknown refresh status: {status}")
    result.variants.append(operation)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--platform",
        choices=SUPPORTED_ADAPTER_PLATFORMS,
        action="append",
        help="Destination platform to refresh. Repeat to refresh multiple platforms.",
    )
    parser.add_argument(
        "--content-type",
        help="Batch refresh only this generated_content.content_type",
    )
    parser.add_argument(
        "--content-id",
        type=int,
        help="Refresh one generated_content row by id",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum rows to batch refresh when --content-id is not set",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report changes without writing content_variants",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON",
    )
    return parser.parse_args(argv)


def _format_text_result(result: VariantRefreshResult, *, dry_run: bool) -> str:
    prefix = "Dry run: " if dry_run else ""
    lines = [
        (
            f"{prefix}created={result.created} updated={result.updated} "
            f"unchanged={result.unchanged} skipped={result.skipped}"
        )
    ]
    for variant in result.variants:
        detail = (
            f"{variant['status']}: content_id={variant['content_id']} "
            f"platform={variant['platform']} variant_type={variant['variant_type']}"
        )
        if variant.get("reason"):
            detail += f" reason={variant['reason']}"
        lines.append(detail)
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.limit <= 0:
        print("error: --limit must be greater than 0", file=sys.stderr)
        return 2

    platforms = tuple(args.platform) if args.platform else SUPPORTED_ADAPTER_PLATFORMS
    options = VariantRefreshOptions(
        platforms=platforms,
        content_type=args.content_type,
        content_id=args.content_id,
        limit=args.limit,
        dry_run=args.dry_run,
    )

    with script_context() as (_config, db):
        result = refresh_content_variants(db, options)

    if args.json:
        print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        print(_format_text_result(result, dry_run=args.dry_run))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
