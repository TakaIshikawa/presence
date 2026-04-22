#!/usr/bin/env python3
"""Explain which deduplication filters would reject candidate content."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Sequence

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.embeddings import (  # noqa: E402
    cosine_similarity,
    deserialize_embedding,
    get_embedding_provider,
)
from storage.db import Database  # noqa: E402
from synthesis.stale_patterns import STALE_PATTERNS  # noqa: E402


OPENING_SIMILARITY_THRESHOLD = 0.55
DEFAULT_SEMANTIC_THRESHOLD = 0.82


@dataclass
class OpeningClauseResult:
    checked: bool
    rejected: bool
    threshold: float
    candidate_opening: str
    max_similarity: float | None = None
    matched_content_id: int | None = None
    matched_opening: str | None = None
    matched_content_preview: str | None = None


@dataclass
class StalePatternResult:
    checked: bool
    rejected: bool
    matches: list[str]


@dataclass
class SemanticResult:
    checked: bool
    enabled: bool
    rejected: bool
    threshold: float
    max_similarity: float | None = None
    matched_content_id: int | None = None
    matched_content_type: str | None = None
    matched_content_preview: str | None = None
    skipped_reason: str | None = None


@dataclass
class DedupReport:
    content_type: str
    rejected: bool
    opening_clause_similarity: OpeningClauseResult
    stale_patterns: StalePatternResult
    semantic_similarity: SemanticResult


def extract_opening(text: str, max_len: int = 100) -> str:
    """Match SynthesisPipeline opening-clause extraction."""
    import re

    stripped = re.sub(r"^TWEET\s+\d+:\s*\n?", "", text).strip()
    match = re.split(r"[—:\.]", stripped, maxsplit=1)
    opening = match[0].strip().lower() if match else stripped[:max_len].lower()
    return opening[:max_len]


def check_opening_clause(
    text: str,
    db: Database,
    content_type: str,
    limit: int = 20,
    threshold: float = OPENING_SIMILARITY_THRESHOLD,
) -> OpeningClauseResult:
    """Check the same opening-clause similarity layer used by the pipeline."""
    candidate_opening = extract_opening(text)
    recent = db.get_recent_published_content(content_type, limit=limit)
    result = OpeningClauseResult(
        checked=True,
        rejected=False,
        threshold=threshold,
        candidate_opening=candidate_opening,
    )
    if not recent:
        return result

    best_similarity = -1.0
    best_row: dict | None = None
    best_opening: str | None = None
    for row in recent:
        recent_opening = extract_opening(row["content"])
        similarity = SequenceMatcher(None, candidate_opening, recent_opening).ratio()
        if similarity > best_similarity:
            best_similarity = similarity
            best_row = row
            best_opening = recent_opening

    result.max_similarity = best_similarity
    if best_row is not None and best_similarity > threshold:
        result.rejected = True
        result.matched_content_id = best_row.get("id")
        result.matched_opening = best_opening
        result.matched_content_preview = best_row.get("content", "")[:120]
    return result


def check_stale_patterns(text: str) -> StalePatternResult:
    """Check shared stale rhetorical regex filters."""
    matches = [pattern.pattern for pattern in STALE_PATTERNS if pattern.search(text)]
    return StalePatternResult(
        checked=True,
        rejected=bool(matches),
        matches=matches,
    )


def check_semantic_similarity(
    text: str,
    db: Database,
    embedder,
    limit: int = 30,
    threshold: float = DEFAULT_SEMANTIC_THRESHOLD,
) -> SemanticResult:
    """Check semantic duplicate similarity against recent stored embeddings."""
    result = SemanticResult(
        checked=True,
        enabled=True,
        rejected=False,
        threshold=threshold,
    )
    if embedder is None:
        result.checked = False
        result.enabled = False
        result.skipped_reason = "semantic checking disabled"
        return result

    recent = db.get_recent_published_content_all(limit=limit)
    recent_with_embeddings = [row for row in recent if row.get("content_embedding")]
    if not recent_with_embeddings:
        result.skipped_reason = "no recent published content has stored embeddings"
        return result

    try:
        candidate_embedding = embedder.embed(text)
    except Exception as exc:  # pragma: no cover - provider-specific failures
        result.skipped_reason = f"candidate embedding failed: {exc}"
        return result

    best_similarity = -1.0
    best_row: dict | None = None
    for row in recent_with_embeddings:
        recent_embedding = deserialize_embedding(row["content_embedding"])
        similarity = cosine_similarity(candidate_embedding, recent_embedding)
        if similarity > best_similarity:
            best_similarity = similarity
            best_row = row

    result.max_similarity = best_similarity
    if best_row is not None and best_similarity > threshold:
        result.rejected = True
        result.matched_content_id = best_row.get("id")
        result.matched_content_type = best_row.get("content_type")
        result.matched_content_preview = best_row.get("content", "")[:120]
    return result


def build_report(
    text: str,
    db: Database,
    content_type: str = "x_post",
    embedder=None,
    semantic_enabled: bool = False,
    opening_limit: int = 20,
    semantic_limit: int = 30,
    opening_threshold: float = OPENING_SIMILARITY_THRESHOLD,
    semantic_threshold: float = DEFAULT_SEMANTIC_THRESHOLD,
) -> DedupReport:
    opening = check_opening_clause(
        text,
        db,
        content_type=content_type,
        limit=opening_limit,
        threshold=opening_threshold,
    )
    stale = check_stale_patterns(text)
    semantic = check_semantic_similarity(
        text,
        db,
        embedder=embedder if semantic_enabled else None,
        limit=semantic_limit,
        threshold=semantic_threshold,
    )
    rejected = opening.rejected or stale.rejected or semantic.rejected
    return DedupReport(
        content_type=content_type,
        rejected=rejected,
        opening_clause_similarity=opening,
        stale_patterns=stale,
        semantic_similarity=semantic,
    )


def read_candidate_text(args: argparse.Namespace) -> str:
    if args.text_file:
        return Path(args.text_file).read_text().strip()
    if args.text:
        if isinstance(args.text, list):
            return " ".join(args.text).strip()
        return args.text.strip()
    if not sys.stdin.isatty():
        return sys.stdin.read().strip()
    raise SystemExit("candidate text required as an argument, --text-file, or stdin")


def load_embedding_config(config_path: str | None):
    from config import load_config

    config = load_config(config_path)
    return config.embeddings


def make_embedder(args: argparse.Namespace):
    if not args.semantic:
        return None

    provider = args.embedding_provider
    model = args.embedding_model
    api_key = args.embedding_api_key

    if not (provider and model and api_key):
        embeddings = load_embedding_config(args.config)
        if embeddings:
            provider = provider or embeddings.provider
            model = model or embeddings.model
            api_key = api_key or embeddings.api_key

    if not (provider and api_key):
        raise SystemExit(
            "semantic checking requires --embedding-provider and --embedding-api-key "
            "or an embeddings section in config"
        )

    return get_embedding_provider(provider, api_key, model)


def format_text_report(report: DedupReport) -> str:
    lines = [
        f"Dedup result: {'REJECTED' if report.rejected else 'ACCEPTED'}",
        f"Content type: {report.content_type}",
        "",
        "Opening-clause similarity:",
        f"  rejected: {report.opening_clause_similarity.rejected}",
        f"  candidate opening: {report.opening_clause_similarity.candidate_opening!r}",
        f"  max similarity: {report.opening_clause_similarity.max_similarity}",
    ]
    if report.opening_clause_similarity.matched_content_id is not None:
        lines.append(
            f"  matched content #{report.opening_clause_similarity.matched_content_id}: "
            f"{report.opening_clause_similarity.matched_opening!r}"
        )

    lines.extend([
        "",
        "Stale regex patterns:",
        f"  rejected: {report.stale_patterns.rejected}",
        f"  matches: {report.stale_patterns.matches}",
        "",
        "Semantic similarity:",
        f"  enabled: {report.semantic_similarity.enabled}",
        f"  rejected: {report.semantic_similarity.rejected}",
        f"  max similarity: {report.semantic_similarity.max_similarity}",
    ])
    if report.semantic_similarity.skipped_reason:
        lines.append(f"  skipped: {report.semantic_similarity.skipped_reason}")
    if report.semantic_similarity.matched_content_id is not None:
        lines.append(
            f"  matched content #{report.semantic_similarity.matched_content_id}: "
            f"{report.semantic_similarity.matched_content_preview!r}"
        )
    return "\n".join(lines)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report which deduplication layers would reject candidate text."
    )
    parser.add_argument("text", nargs="*", help="Candidate text to check")
    parser.add_argument("--text-file", help="Read candidate text from a file")
    parser.add_argument("--db", default="./presence.db", help="SQLite database path")
    parser.add_argument("--config", help="Config YAML path for embeddings settings")
    parser.add_argument("--content-type", default="x_post")
    parser.add_argument("--opening-limit", type=int, default=20)
    parser.add_argument("--semantic-limit", type=int, default=30)
    parser.add_argument(
        "--opening-threshold",
        type=float,
        default=OPENING_SIMILARITY_THRESHOLD,
    )
    parser.add_argument(
        "--semantic-threshold",
        type=float,
        default=DEFAULT_SEMANTIC_THRESHOLD,
    )
    parser.add_argument(
        "--semantic",
        action="store_true",
        help="Enable semantic similarity using the configured embedding provider",
    )
    parser.add_argument(
        "--no-semantic",
        action="store_false",
        dest="semantic",
        help="Disable semantic similarity (default; no network access required)",
    )
    parser.add_argument("--embedding-provider", choices=["voyage", "openai"])
    parser.add_argument("--embedding-model")
    parser.add_argument("--embedding-api-key")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    text = read_candidate_text(args)
    embedder = make_embedder(args)

    db = Database(args.db)
    db.connect()
    try:
        report = build_report(
            text,
            db,
            content_type=args.content_type,
            embedder=embedder,
            semantic_enabled=args.semantic,
            opening_limit=args.opening_limit,
            semantic_limit=args.semantic_limit,
            opening_threshold=args.opening_threshold,
            semantic_threshold=args.semantic_threshold,
        )
    finally:
        db.close()

    if args.json:
        print(json.dumps(asdict(report), indent=2, sort_keys=True))
    else:
        print(format_text_report(report))
    return 1 if report.rejected else 0


if __name__ == "__main__":
    raise SystemExit(main())
