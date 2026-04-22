"""Turn resonated published posts into reviewable blog seed artifacts."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from output.blog_writer import BlogWriter


BLOG_SEED_VARIANT_PLATFORM = "blog"
BLOG_SEED_VARIANT_TYPE = "post_mortem_seed"
DEFAULT_MIN_ENGAGEMENT = 10.0
DEFAULT_MAX_AGE_DAYS = 30
DEFAULT_LIMIT = 5


class PostMortemRepurposerError(ValueError):
    """Raised when a post-mortem blog seed cannot be built."""


@dataclass(frozen=True)
class ResonatedPostCandidate:
    """Published content whose measured engagement warrants expansion."""

    content_id: int
    content_type: str
    content: str
    engagement_score: float
    published_url: str | None = None
    published_at: str | None = None
    auto_quality: str | None = None


@dataclass(frozen=True)
class BlogSeedArtifact:
    """Structured review artifact for a future long-form blog post."""

    artifact_type: str
    source_content_id: int
    source_content_type: str
    generated_at: str
    title: str
    outline: list[str]
    draft_seed: str
    source_links: list[dict[str, str]]
    source_artifacts: dict[str, list[dict[str, Any]]]
    engagement: dict[str, Any]
    claim_check: dict[str, Any] | None
    risk_notes: list[str] = field(default_factory=list)


class PostMortemRepurposer:
    """Builds blog seed artifacts from posts that already resonated."""

    def __init__(self, db: Any) -> None:
        self.db = db

    def find_eligible_posts(
        self,
        *,
        min_engagement: float = DEFAULT_MIN_ENGAGEMENT,
        max_age_days: int = DEFAULT_MAX_AGE_DAYS,
        limit: int = DEFAULT_LIMIT,
    ) -> list[ResonatedPostCandidate]:
        """Return published posts whose latest engagement clears the threshold."""
        if max_age_days <= 0:
            raise PostMortemRepurposerError("max_age_days must be positive")
        if limit <= 0:
            return []

        cursor = self.db.conn.execute(
            """SELECT gc.id, gc.content_type, gc.content, gc.published_url,
                      gc.published_at, gc.auto_quality, pe.engagement_score
               FROM generated_content gc
               INNER JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id
                              ORDER BY fetched_at DESC, id DESC
                          ) AS rn
                   FROM post_engagement
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               WHERE gc.published = 1
                 AND gc.content_type IN ('x_post', 'x_thread', 'x_visual')
                 AND (gc.auto_quality = 'resonated' OR pe.engagement_score >= ?)
                 AND gc.published_at >= datetime('now', ?)
                 AND gc.id NOT IN (
                     SELECT repurposed_from
                     FROM generated_content
                     WHERE repurposed_from IS NOT NULL
                       AND content_type IN ('blog_seed', 'blog_post')
                 )
                 AND gc.id NOT IN (
                     SELECT content_id
                     FROM content_variants
                     WHERE platform = ?
                       AND variant_type = ?
                 )
               ORDER BY pe.engagement_score DESC, gc.published_at DESC
               LIMIT ?""",
            (
                min_engagement,
                f"-{max_age_days} days",
                BLOG_SEED_VARIANT_PLATFORM,
                BLOG_SEED_VARIANT_TYPE,
                limit,
            ),
        )
        return [
            ResonatedPostCandidate(
                content_id=row["id"],
                content_type=row["content_type"],
                content=row["content"],
                engagement_score=float(row["engagement_score"] or 0.0),
                published_url=row["published_url"],
                published_at=row["published_at"],
                auto_quality=row["auto_quality"],
            )
            for row in cursor.fetchall()
        ]

    def build_seed(self, candidate: ResonatedPostCandidate) -> BlogSeedArtifact:
        """Assemble source context, claim checks, and a structured blog seed."""
        provenance = self.db.get_content_provenance(candidate.content_id) or {}
        claim_check = self.db.get_claim_check_summary(candidate.content_id)
        source_artifacts = self._source_artifacts(provenance)
        source_links = self._source_links(candidate, provenance)
        title = self._title(candidate.content)
        outline = self._outline(candidate.content, claim_check)
        risk_notes = self._risk_notes(source_artifacts, source_links, claim_check)

        return BlogSeedArtifact(
            artifact_type="post_mortem_blog_seed",
            source_content_id=candidate.content_id,
            source_content_type=candidate.content_type,
            generated_at=datetime.now(timezone.utc).isoformat(),
            title=title,
            outline=outline,
            draft_seed=self._draft_seed(title, candidate.content, outline, claim_check),
            source_links=source_links,
            source_artifacts=source_artifacts,
            engagement={
                "score": candidate.engagement_score,
                "published_at": candidate.published_at,
                "published_url": candidate.published_url,
                "auto_quality": candidate.auto_quality,
            },
            claim_check=claim_check,
            risk_notes=risk_notes,
        )

    def record_seed_variant(self, artifact: BlogSeedArtifact) -> int:
        """Persist the artifact as a durable blog seed variant on the source post."""
        return self.db.upsert_content_variant(
            content_id=artifact.source_content_id,
            platform=BLOG_SEED_VARIANT_PLATFORM,
            variant_type=BLOG_SEED_VARIANT_TYPE,
            content=artifact_to_json(artifact),
            metadata={
                "artifact_type": artifact.artifact_type,
                "title": artifact.title,
                "generated_at": artifact.generated_at,
            },
        )

    def _source_artifacts(self, provenance: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        commits = [
            {
                "sha": item.get("commit_sha", ""),
                "repo": item.get("repo_name", ""),
                "message": _truncate(item.get("commit_message") or "", 240),
                "matched": bool(item.get("matched", True)),
            }
            for item in provenance.get("source_commits", [])
        ]
        messages = [
            {
                "message_uuid": item.get("message_uuid", ""),
                "project_path": item.get("project_path", ""),
                "prompt_text": _truncate(item.get("prompt_text") or "", 500),
                "matched": bool(item.get("matched", True)),
            }
            for item in provenance.get("source_messages", [])
        ]
        activity = [
            {
                "activity_id": item.get("activity_id", ""),
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "state": item.get("state", ""),
                "matched": bool(item.get("matched", True)),
            }
            for item in provenance.get("source_activity", [])
        ]
        knowledge = [
            {
                "source_type": item.get("source_type", ""),
                "source_id": item.get("source_id", ""),
                "source_url": item.get("source_url", ""),
                "author": item.get("author", ""),
                "insight": _truncate(item.get("insight") or "", 300),
                "relevance_score": item.get("relevance_score"),
            }
            for item in provenance.get("knowledge_links", [])
        ]
        return {
            "commits": commits,
            "messages": messages,
            "github_activity": activity,
            "knowledge": knowledge,
        }

    def _source_links(
        self,
        candidate: ResonatedPostCandidate,
        provenance: dict[str, Any],
    ) -> list[dict[str, str]]:
        links: list[dict[str, str]] = []
        if candidate.published_url:
            links.append({"type": "published_post", "url": candidate.published_url})
        for publication in provenance.get("publications", []):
            url = publication.get("platform_url")
            if url:
                links.append({"type": f"{publication.get('platform', 'platform')}_publication", "url": url})
        for activity in provenance.get("source_activity", []):
            url = activity.get("url")
            if url:
                links.append({"type": "github_activity", "url": url})
        for knowledge in provenance.get("knowledge_links", []):
            url = knowledge.get("source_url")
            if url:
                links.append({"type": "knowledge", "url": url})
        return _dedupe_links(links)

    def _risk_notes(
        self,
        source_artifacts: dict[str, list[dict[str, Any]]],
        source_links: list[dict[str, str]],
        claim_check: dict[str, Any] | None,
    ) -> list[str]:
        notes: list[str] = []
        if not any(source_artifacts.values()):
            notes.append("No source artifacts were found; keep the blog seed close to the published post.")
        if not source_links:
            notes.append("No source links are available for reader-facing citations.")
        if claim_check is None:
            notes.append("No claim-check summary is recorded for the source post.")
        elif int(claim_check.get("unsupported_count") or 0) > 0:
            notes.append("Claim-check summary includes unsupported claims that need revision or removal.")
        missing_refs = [
            item
            for items in source_artifacts.values()
            for item in items
            if item.get("matched") is False
        ]
        if missing_refs:
            notes.append("Some recorded source references no longer match local artifact tables.")
        return notes

    def _title(self, content: str) -> str:
        hook = _sentences(_strip_thread_markers(content))[0] if _sentences(_strip_thread_markers(content)) else "A Resonant Post"
        hook = _truncate(hook, 82).rstrip(".!?")
        return f"What Resonated: {hook}"

    def _outline(self, content: str, claim_check: dict[str, Any] | None) -> list[str]:
        sentences = _sentences(_strip_thread_markers(content))
        core = _truncate(sentences[0], 96).rstrip(".!?") if sentences else "The core observation"
        outline = [
            f"Open with the post-mortem: why '{core}' caught attention.",
            "Reconstruct the source context and the concrete work behind the claim.",
            "Separate supported claims from interpretation before expanding the argument.",
            "Turn the reader response into a practical lesson or reusable heuristic.",
        ]
        if claim_check and int(claim_check.get("unsupported_count") or 0) > 0:
            outline.insert(3, "Resolve unsupported claims before drafting the final version.")
        return outline

    def _draft_seed(
        self,
        title: str,
        content: str,
        outline: list[str],
        claim_check: dict[str, Any] | None,
    ) -> str:
        claim_line = "Claim check: not recorded."
        if claim_check:
            claim_line = (
                "Claim check: "
                f"{claim_check.get('supported_count', 0)} supported, "
                f"{claim_check.get('unsupported_count', 0)} unsupported."
            )
        outline_text = "\n".join(f"{index}. {item}" for index, item in enumerate(outline, start=1))
        return (
            f"TITLE: {title}\n\n"
            f"Original resonated post:\n{_strip_thread_markers(content)}\n\n"
            f"OUTLINE:\n{outline_text}\n\n"
            f"RISK NOTES:\n- {claim_line}\n"
        )


def artifact_to_dict(artifact: BlogSeedArtifact) -> dict[str, Any]:
    """Return a JSON-safe artifact mapping."""
    return asdict(artifact)


def artifact_to_json(artifact: BlogSeedArtifact) -> str:
    """Serialize a blog seed artifact as stable JSON."""
    return json.dumps(artifact_to_dict(artifact), indent=2, sort_keys=True)


def format_artifact_markdown(artifact: BlogSeedArtifact) -> str:
    """Render a blog seed artifact for editorial review."""
    lines = [
        f"# {artifact.title}",
        "",
        f"- Artifact type: {artifact.artifact_type}",
        f"- Source content ID: {artifact.source_content_id}",
        f"- Source content type: {artifact.source_content_type}",
        f"- Engagement score: {artifact.engagement['score']}",
        "",
        "## Outline",
    ]
    lines.extend(f"{index}. {item}" for index, item in enumerate(artifact.outline, start=1))
    lines.extend(["", "## Draft Seed", "", artifact.draft_seed.rstrip()])

    lines.extend(["", "## Source Links"])
    if artifact.source_links:
        lines.extend(f"- {link['type']}: {link['url']}" for link in artifact.source_links)
    else:
        lines.append("- none")

    lines.extend(["", "## Claim Check"])
    if artifact.claim_check:
        lines.extend(
            [
                f"- Supported claims: {artifact.claim_check.get('supported_count', 0)}",
                f"- Unsupported claims: {artifact.claim_check.get('unsupported_count', 0)}",
            ]
        )
        annotation = artifact.claim_check.get("annotation_text")
        if annotation:
            lines.append(f"- Notes: {annotation}")
    else:
        lines.append("- not recorded")

    lines.extend(["", "## Risk Notes"])
    lines.extend(f"- {note}" for note in artifact.risk_notes) if artifact.risk_notes else lines.append("- none")
    return "\n".join(lines).rstrip() + "\n"


def write_artifact(
    artifact: BlogSeedArtifact,
    path: str | Path,
    *,
    artifact_format: str = "json",
) -> Path:
    """Write a JSON or markdown blog seed artifact."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if artifact_format == "json":
        body = artifact_to_json(artifact) + "\n"
    elif artifact_format == "markdown":
        body = format_artifact_markdown(artifact)
    else:
        raise PostMortemRepurposerError("artifact_format must be 'json' or 'markdown'")
    target.write_text(body, encoding="utf-8")
    return target


def artifact_filename(artifact: BlogSeedArtifact, *, artifact_format: str = "json") -> str:
    """Return a stable filename using BlogWriter slug conventions."""
    extension = "json" if artifact_format == "json" else "md"
    slug = BlogWriter("/tmp")._slugify(artifact.title) or f"content-{artifact.source_content_id}"
    return f"{artifact.source_content_id}-{slug}.{extension}"


def _sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?])\s+|\n+", text.strip())
    return [part.strip() for part in parts if part.strip()]


def _strip_thread_markers(content: str) -> str:
    content = re.sub(r"(?im)^\s*TWEET\s+\d+\s*:\s*", "", content)
    return re.sub(r"\s+", " ", content).strip()


def _truncate(text: str, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(text)).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _dedupe_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    seen = set()
    deduped = []
    for link in links:
        url = link.get("url")
        if not url or url in seen:
            continue
        seen.add(url)
        deduped.append(link)
    return deduped
