#!/usr/bin/env python3
"""Preview knowledge ingestion changes from a JSONL candidate file."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.embeddings import get_embedding_provider
from knowledge.ingest_diff import KnowledgeIngestDiff, generate_ingest_diff
from knowledge.store import KnowledgeStore
from runner import script_context

logger = logging.getLogger(__name__)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    with path.open() as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                value = json.loads(stripped)
            except json.JSONDecodeError as exc:
                candidates.append(
                    {
                        "_invalid_json": stripped,
                        "_line_number": line_number,
                        "_error": str(exc),
                    }
                )
                continue
            if isinstance(value, dict):
                candidates.append(value)
            else:
                candidates.append(
                    {
                        "_invalid_json": value,
                        "_line_number": line_number,
                        "_error": "JSONL rows must be objects",
                    }
                )
    return candidates


def render_text(diff: KnowledgeIngestDiff) -> str:
    payload = diff.to_dict()
    summary = payload["summary"]
    lines = [
        "Knowledge ingest diff",
        (
            f"new={summary['new']} unchanged={summary['unchanged']} "
            f"changed={summary['changed']} duplicates={summary['duplicates']} "
            f"rejected={summary['rejected']}"
        ),
        "",
    ]

    sections = [
        ("New", diff.new_items),
        ("Unchanged", diff.existing_items),
        ("Changed", diff.changed_items),
        ("Duplicate candidates", diff.duplicate_candidates),
        ("Rejected", diff.rejected_items),
    ]
    for title, items in sections:
        lines.append(f"{title}: {len(items)}")
        for item in items:
            data = item.__dict__
            source = f"{data.get('source_type')}:{data.get('source_id')}"
            if title == "Changed":
                lines.append(
                    f"  [{data['input_index']}] {source} fields={','.join(data['changed_fields'])}"
                )
            elif title == "Duplicate candidates":
                lines.append(
                    f"  [{data['input_index']}] {source} -> "
                    f"{data['duplicate_of_source_type']}:{data['duplicate_of_source_id']} "
                    f"({data['match_type']} {data['similarity']:.3f})"
                )
            elif title == "Rejected":
                lines.append(f"  [{data['input_index']}] {data['reason']}")
            else:
                lines.append(f"  [{data['input_index']}] {source}")
        lines.append("")

    return "\n".join(lines).rstrip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Preview knowledge ingestion changes from JSONL candidates"
    )
    parser.add_argument("jsonl_file", type=Path, help="Candidate knowledge JSONL file")
    parser.add_argument("--json", action="store_true", help="Emit stable JSON")
    parser.add_argument(
        "--duplicate-similarity-threshold",
        type=float,
        default=0.92,
        help="Similarity threshold for near-duplicate detection (default: 0.92)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    candidates = read_jsonl(args.jsonl_file)
    with script_context() as (config, db):
        if not config.embeddings:
            raise SystemExit("embeddings not configured in config.yaml")
        embedder = get_embedding_provider(
            config.embeddings.provider,
            config.embeddings.api_key,
            config.embeddings.model,
        )
        store = KnowledgeStore(db.conn, embedder)
        diff = generate_ingest_diff(
            store,
            candidates,
            duplicate_similarity_threshold=args.duplicate_similarity_threshold,
        )

    if args.json:
        print(json.dumps(diff.to_dict(), indent=2, sort_keys=True))
    else:
        print(render_text(diff))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
