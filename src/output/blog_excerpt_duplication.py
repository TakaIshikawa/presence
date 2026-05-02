"""Find repeated excerpts across generated blog posts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
import json
import re
from pathlib import Path
from typing import Any, Iterable, Mapping

from .blog_frontmatter_validator import parse_markdown_frontmatter


DEFAULT_SIMILARITY_THRESHOLD = 0.86


@dataclass(frozen=True)
class BlogPostRecord:
    """Minimum blog post fields needed for excerpt duplication checks."""

    slug: str
    title: str
    excerpt: str = ""
    summary: str = ""
    body: str = ""
    published_at: str | None = None

    @property
    def comparison_text(self) -> str:
        return (self.excerpt or self.summary or "").strip()

    def to_dict(self) -> dict[str, Any]:
        return {
            "slug": self.slug,
            "title": self.title,
            "excerpt": self.excerpt,
            "summary": self.summary,
            "body": self.body,
            "published_at": self.published_at,
        }


@dataclass(frozen=True)
class BlogExcerptClusterPost:
    """One post participating in a duplicate excerpt cluster."""

    slug: str
    title: str
    published_at: str | None
    text_source: str
    text: str
    normalized_text: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "slug": self.slug,
            "title": self.title,
            "published_at": self.published_at,
            "text_source": self.text_source,
            "text": self.text,
            "normalized_text": self.normalized_text,
        }


@dataclass(frozen=True)
class BlogExcerptSimilarity:
    """Similarity score between two clustered posts."""

    left_slug: str
    right_slug: str
    score: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "left_slug": self.left_slug,
            "right_slug": self.right_slug,
            "score": round(self.score, 6),
        }


@dataclass(frozen=True)
class BlogExcerptDuplicateCluster:
    """A group of posts whose excerpts are exact or near duplicates."""

    representative_text: str
    posts: tuple[BlogExcerptClusterPost, ...]
    similarity_scores: tuple[BlogExcerptSimilarity, ...]

    @property
    def slugs(self) -> tuple[str, ...]:
        return tuple(post.slug for post in self.posts)

    @property
    def max_similarity(self) -> float:
        return max((item.score for item in self.similarity_scores), default=0.0)

    @property
    def min_similarity(self) -> float:
        return min((item.score for item in self.similarity_scores), default=0.0)

    def to_dict(self) -> dict[str, Any]:
        return {
            "representative_text": self.representative_text,
            "slugs": list(self.slugs),
            "titles": [post.title for post in self.posts],
            "max_similarity": round(self.max_similarity, 6),
            "min_similarity": round(self.min_similarity, 6),
            "similarity_scores": [score.to_dict() for score in self.similarity_scores],
            "posts": [post.to_dict() for post in self.posts],
        }


@dataclass(frozen=True)
class BlogExcerptDuplicationReport:
    """Aggregated duplicate excerpt report for a set of blog posts."""

    source: str
    similarity_threshold: float
    lookback_days: int | None
    total_posts: int
    compared_posts: int
    skipped_posts: tuple[dict[str, Any], ...]
    clusters: tuple[BlogExcerptDuplicateCluster, ...]

    @property
    def cluster_count(self) -> int:
        return len(self.clusters)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "blog_excerpt_duplication",
            "source": self.source,
            "similarity_threshold": self.similarity_threshold,
            "lookback_days": self.lookback_days,
            "total_posts": self.total_posts,
            "compared_posts": self.compared_posts,
            "skipped_posts": [dict(item) for item in self.skipped_posts],
            "cluster_count": self.cluster_count,
            "clusters": [cluster.to_dict() for cluster in self.clusters],
        }


def build_blog_excerpt_duplication_report(
    records: Iterable[BlogPostRecord | Mapping[str, Any]],
    *,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    lookback_days: int | None = None,
    now: datetime | None = None,
    source: str = "records",
) -> BlogExcerptDuplicationReport:
    """Detect exact and near-duplicate excerpts or summaries."""
    threshold = float(similarity_threshold)
    if threshold <= 0 or threshold > 1:
        raise ValueError("similarity_threshold must be greater than 0 and at most 1")
    if lookback_days is not None and lookback_days <= 0:
        raise ValueError("lookback_days must be positive")

    posts = tuple(_coerce_record(record) for record in records)
    filtered = _filter_lookback(posts, lookback_days=lookback_days, now=now)
    comparable: list[tuple[BlogPostRecord, str]] = []
    skipped: list[dict[str, Any]] = []
    for post in filtered:
        normalized = normalize_excerpt_text(post.comparison_text)
        if not normalized:
            skipped.append(
                {
                    "slug": post.slug,
                    "title": post.title,
                    "reason": "missing_excerpt_or_summary",
                }
            )
            continue
        comparable.append((post, normalized))

    pairs: list[tuple[int, int, float]] = []
    for left_index, (left, left_text) in enumerate(comparable):
        for right_index in range(left_index + 1, len(comparable)):
            right, right_text = comparable[right_index]
            score = _similarity(left_text, right_text)
            if score >= threshold:
                pairs.append((left_index, right_index, score))

    clusters = _clusters(comparable, pairs)
    return BlogExcerptDuplicationReport(
        source=source,
        similarity_threshold=threshold,
        lookback_days=lookback_days,
        total_posts=len(filtered),
        compared_posts=len(comparable),
        skipped_posts=tuple(skipped),
        clusters=clusters,
    )


def load_blog_posts_from_paths(paths: Iterable[str | Path]) -> list[BlogPostRecord]:
    """Load markdown blog posts from explicit file paths."""
    return [_record_from_markdown_path(Path(path)) for path in paths]


def load_blog_posts_from_directory(path: str | Path) -> list[BlogPostRecord]:
    """Load markdown blog posts from one directory."""
    root = Path(path)
    return load_blog_posts_from_paths(sorted(root.glob("*.md")))


def format_blog_excerpt_duplication_json(report: BlogExcerptDuplicationReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_blog_excerpt_duplication_text(report: BlogExcerptDuplicationReport) -> str:
    """Render a compact human-readable duplicate excerpt report."""
    lines = [
        "Blog Excerpt Duplication",
        f"Source: {report.source}",
        f"Posts: {report.total_posts}",
        f"Compared: {report.compared_posts}",
        f"Clusters: {report.cluster_count}",
        f"Similarity threshold: {report.similarity_threshold:g}",
    ]
    if report.lookback_days:
        lines.append(f"Lookback: {report.lookback_days} days")
    if not report.clusters:
        lines.append("No duplicate excerpt clusters found.")
        return "\n".join(lines)

    for index, cluster in enumerate(report.clusters, start=1):
        lines.append("")
        lines.append(
            f"Cluster {index}: {len(cluster.posts)} posts "
            f"(similarity {cluster.min_similarity:.0%}-{cluster.max_similarity:.0%})"
        )
        lines.append(f"  Representative: {cluster.representative_text}")
        for post in cluster.posts:
            lines.append(f"  - {post.slug}: {post.title}")
        for score in cluster.similarity_scores:
            lines.append(f"    {score.left_slug} <-> {score.right_slug}: {score.score:.0%}")
    return "\n".join(lines)


def normalize_excerpt_text(text: str) -> str:
    """Normalize case, whitespace, and simple punctuation before comparison."""
    lowered = str(text or "").casefold()
    without_markdown = re.sub(r"[*_`~>#\[\](){}]", " ", lowered)
    without_punctuation = re.sub(r"[^\w\s]", " ", without_markdown)
    return re.sub(r"\s+", " ", without_punctuation).strip()


def _coerce_record(record: BlogPostRecord | Mapping[str, Any]) -> BlogPostRecord:
    if isinstance(record, BlogPostRecord):
        return record
    slug = str(record.get("slug") or record.get("id") or "").strip()
    title = str(record.get("title") or slug or "Untitled").strip()
    return BlogPostRecord(
        slug=slug or _slugify(title),
        title=title,
        excerpt=str(record.get("excerpt") or record.get("description") or "").strip(),
        summary=str(record.get("summary") or "").strip(),
        body=str(record.get("body") or record.get("content") or "").strip(),
        published_at=_string_or_none(record.get("published_at") or record.get("date")),
    )


def _record_from_markdown_path(path: Path) -> BlogPostRecord:
    frontmatter, body, _issues = parse_markdown_frontmatter(path.read_text(), path=str(path))
    title = str(frontmatter.get("title") or path.stem).strip()
    slug = str(frontmatter.get("slug") or path.stem).strip()
    return BlogPostRecord(
        slug=slug or _slugify(title),
        title=title or slug or path.stem,
        excerpt=str(
            frontmatter.get("excerpt")
            or frontmatter.get("description")
            or ""
        ).strip(),
        summary=str(frontmatter.get("summary") or "").strip(),
        body=body.strip(),
        published_at=_string_or_none(
            frontmatter.get("published_at")
            or frontmatter.get("date")
            or frontmatter.get("created_at")
        ),
    )


def _filter_lookback(
    posts: tuple[BlogPostRecord, ...],
    *,
    lookback_days: int | None,
    now: datetime | None,
) -> tuple[BlogPostRecord, ...]:
    if lookback_days is None:
        return posts
    current = _normalize_now(now)
    cutoff = current - timedelta(days=lookback_days)
    return tuple(
        post
        for post in posts
        if (published := _parse_datetime(post.published_at)) is None or published >= cutoff
    )


def _clusters(
    comparable: list[tuple[BlogPostRecord, str]],
    pairs: list[tuple[int, int, float]],
) -> tuple[BlogExcerptDuplicateCluster, ...]:
    if not pairs:
        return ()

    parent = list(range(len(comparable)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    for left, right, _score in pairs:
        union(left, right)

    grouped: dict[int, set[int]] = {}
    for index in range(len(comparable)):
        grouped.setdefault(find(index), set()).add(index)

    clusters: list[BlogExcerptDuplicateCluster] = []
    for indexes in grouped.values():
        if len(indexes) < 2:
            continue
        posts = tuple(
            _cluster_post(comparable[index][0], comparable[index][1])
            for index in sorted(indexes, key=lambda item: _post_sort_key(comparable[item][0]))
        )
        scores = tuple(
            BlogExcerptSimilarity(
                left_slug=comparable[left][0].slug,
                right_slug=comparable[right][0].slug,
                score=score,
            )
            for left, right, score in sorted(
                pairs,
                key=lambda item: (
                    comparable[item[0]][0].slug,
                    comparable[item[1]][0].slug,
                ),
            )
            if left in indexes and right in indexes
        )
        first_index = min(indexes, key=lambda item: _post_sort_key(comparable[item][0]))
        representative = comparable[first_index][0].comparison_text
        clusters.append(
            BlogExcerptDuplicateCluster(
                representative_text=representative,
                posts=posts,
                similarity_scores=scores,
            )
        )

    return tuple(
        sorted(
            clusters,
            key=lambda cluster: (-cluster.max_similarity, cluster.slugs),
        )
    )


def _cluster_post(post: BlogPostRecord, normalized_text: str) -> BlogExcerptClusterPost:
    return BlogExcerptClusterPost(
        slug=post.slug,
        title=post.title,
        published_at=post.published_at,
        text_source="excerpt" if post.excerpt.strip() else "summary",
        text=post.comparison_text,
        normalized_text=normalized_text,
    )


def _similarity(left: str, right: str) -> float:
    if left == right:
        return 1.0
    return SequenceMatcher(None, left, right, autojunk=False).ratio()


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_now(now: datetime | None) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _post_sort_key(post: BlogPostRecord) -> tuple[str, str]:
    published = _parse_datetime(post.published_at)
    return (published.isoformat() if published else "", post.slug)


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")
    return slug or "untitled"
