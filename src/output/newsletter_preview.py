"""Local newsletter preview artifact export."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from output.attribution_guard import check_publication_attribution_guard
from output.newsletter import (
    NewsletterAssembler,
    NewsletterContent,
    NewsletterSubjectCandidate,
)


@dataclass(frozen=True)
class NewsletterPreviewOptions:
    """Configuration for rendering a newsletter preview."""

    site_url: str = "https://takaishikawa.com"
    utm_source: str = ""
    utm_medium: str = ""
    utm_campaign_template: str = ""
    manual_subject: str = ""
    include_metadata: bool = False


@dataclass(frozen=True)
class NewsletterPreview:
    """Rendered local preview of a newsletter issue."""

    selected_subject: str
    subject_candidates: list[dict[str, Any]]
    source_content_ids: list[int]
    body_markdown: str
    links: list[dict[str, Any]]
    warnings: list[str]
    week_start: str
    week_end: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self, *, include_metadata: bool = False) -> dict[str, Any]:
        payload = {
            "selected_subject": self.selected_subject,
            "subject": self.selected_subject,
            "subject_candidates": self.subject_candidates,
            "source_content_ids": self.source_content_ids,
            "body_markdown": self.body_markdown,
            "links": self.links,
            "outbound_links": self.links,
            "warnings": self.warnings,
            "week_range": {
                "start": self.week_start,
                "end": self.week_end,
            },
        }
        if include_metadata:
            payload["metadata"] = self.metadata
        return payload


def config_text(obj: object, *names: str) -> str:
    """Return the first configured string value from a possibly mocked config."""
    for name in names:
        value = getattr(obj, name, "")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def manual_subject_override(config: object, cli_subject: str = "") -> str:
    """Resolve an explicit subject override from CLI or newsletter config."""
    newsletter = getattr(config, "newsletter", None)
    return (
        (cli_subject or "").strip()
        or config_text(
            newsletter,
            "subject_override",
            "manual_subject",
            "subject",
        )
    )


def make_newsletter_assembler(db: object, config: object) -> NewsletterAssembler:
    """Create the same assembler configuration used by the send path."""
    newsletter = getattr(config, "newsletter", None)
    return NewsletterAssembler(
        db,
        site_url=config_text(newsletter, "site_url") or "https://takaishikawa.com",
        utm_source=config_text(newsletter, "utm_source"),
        utm_medium=config_text(newsletter, "utm_medium"),
        utm_campaign_template=config_text(newsletter, "utm_campaign_template"),
    )


def build_newsletter_preview(
    db: Any,
    week_start: datetime,
    week_end: datetime,
    options: NewsletterPreviewOptions | None = None,
) -> NewsletterPreview:
    """Assemble newsletter content and return a local review preview."""
    options = options or NewsletterPreviewOptions()
    assembler = NewsletterAssembler(
        db,
        site_url=options.site_url,
        utm_source=options.utm_source,
        utm_medium=options.utm_medium,
        utm_campaign_template=options.utm_campaign_template,
    )
    content = assembler.assemble(week_start, week_end)
    selected_subject = select_subject(content, options.manual_subject)
    warnings = _preview_warnings(db, content)
    metadata = {
        "content_metadata": dict(content.metadata or {}),
        "include_metadata": options.include_metadata,
    }
    if options.manual_subject:
        metadata["manual_subject"] = options.manual_subject

    return NewsletterPreview(
        selected_subject=selected_subject,
        subject_candidates=[
            candidate_to_dict(candidate)
            for candidate in subject_candidates_for_storage(
                content,
                selected_subject=selected_subject,
                manual_subject=options.manual_subject,
            )
        ],
        source_content_ids=list(content.source_content_ids or []),
        body_markdown=content.body_markdown,
        links=extract_outbound_links(content.body_markdown),
        warnings=warnings,
        week_start=week_start.isoformat(),
        week_end=week_end.isoformat(),
        metadata=metadata,
    )


def assemble_newsletter_preview(
    db: object,
    config: object,
    week_start: datetime,
    week_end: datetime,
    manual_subject: str = "",
) -> dict:
    """Assemble newsletter content and return a compatibility preview payload."""
    preview = build_newsletter_preview(
        db,
        week_start,
        week_end,
        NewsletterPreviewOptions(
            site_url=config_text(getattr(config, "newsletter", None), "site_url")
            or "https://takaishikawa.com",
            utm_source=config_text(getattr(config, "newsletter", None), "utm_source"),
            utm_medium=config_text(getattr(config, "newsletter", None), "utm_medium"),
            utm_campaign_template=config_text(
                getattr(config, "newsletter", None),
                "utm_campaign_template",
            ),
            manual_subject=manual_subject,
            include_metadata=True,
        ),
    )
    return preview.as_dict(include_metadata=True)


def render_newsletter_preview_markdown(
    preview: NewsletterPreview,
    *,
    include_metadata: bool = False,
) -> str:
    """Render a human-readable Markdown preview artifact."""
    payload = preview.as_dict(include_metadata=include_metadata)
    candidate_lines = [
        (
            f"- {candidate['subject']} "
            f"(score: {candidate['score']}, source: {candidate['source']})"
            + (f" - {candidate['rationale']}" if candidate["rationale"] else "")
        )
        for candidate in payload["subject_candidates"]
    ] or ["- None"]
    link_lines = [
        f"- [{link['label']}]({link['url']})"
        for link in payload["outbound_links"]
    ] or ["- None"]
    warning_lines = [f"- {warning}" for warning in payload["warnings"]] or ["- None"]
    source_ids = ", ".join(str(item) for item in payload["source_content_ids"]) or "None"

    sections = [
        "# Newsletter Preview",
        "## Week Range",
        f"- Start: {payload['week_range']['start']}",
        f"- End: {payload['week_range']['end']}",
        "## Selected Subject",
        payload["selected_subject"] or "None",
        "## Subject Candidates",
        "\n".join(candidate_lines),
        "## Source Content IDs",
        source_ids,
        "## Outbound Links",
        "\n".join(link_lines),
        "## Warnings",
        "\n".join(warning_lines),
    ]
    if include_metadata:
        sections.extend(
            [
                "## Metadata",
                "```json\n"
                + json.dumps(payload.get("metadata", {}), indent=2, sort_keys=True)
                + "\n```",
            ]
        )
    sections.extend(["## Body", payload["body_markdown"]])
    return "\n\n".join(sections).rstrip() + "\n"


def render_newsletter_preview_json(
    preview: NewsletterPreview,
    *,
    include_metadata: bool = False,
) -> str:
    """Render a deterministic JSON preview artifact."""
    return json.dumps(
        preview.as_dict(include_metadata=include_metadata),
        indent=2,
        sort_keys=True,
    ) + "\n"


def format_preview_markdown(payload: dict) -> str:
    """Render a compatibility preview payload as Markdown."""
    preview = NewsletterPreview(
        selected_subject=payload.get("selected_subject") or payload.get("subject") or "",
        subject_candidates=list(payload.get("subject_candidates") or []),
        source_content_ids=list(payload.get("source_content_ids") or []),
        body_markdown=payload.get("body_markdown") or "",
        links=list(payload.get("outbound_links") or payload.get("links") or []),
        warnings=list(payload.get("warnings") or []),
        week_start=(payload.get("week_range") or {}).get("start", ""),
        week_end=(payload.get("week_range") or {}).get("end", ""),
        metadata=dict(payload.get("metadata") or {}),
    )
    return render_newsletter_preview_markdown(preview, include_metadata=bool(payload.get("metadata")))


def format_preview_json(payload: dict) -> str:
    """Render a compatibility preview payload as JSON."""
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def write_newsletter_preview(
    preview: NewsletterPreview,
    output_path: str | Path,
    *,
    json_mode: bool = False,
    include_metadata: bool = False,
) -> None:
    """Write a preview artifact as Markdown or JSON."""
    path = Path(output_path)
    rendered = (
        render_newsletter_preview_json(
            preview,
            include_metadata=include_metadata,
        )
        if json_mode
        else render_newsletter_preview_markdown(
            preview,
            include_metadata=include_metadata,
        )
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(rendered, encoding="utf-8")


def write_preview_artifact(path: Path, payload: dict) -> None:
    """Write a newsletter preview as JSON or Markdown based on extension."""
    suffix = path.suffix.lower()
    rendered = (
        format_preview_markdown(payload)
        if suffix in {".md", ".markdown"}
        else format_preview_json(payload)
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(rendered, encoding="utf-8")


def select_subject(content: NewsletterContent, manual_subject: str = "") -> str:
    if manual_subject:
        return manual_subject
    candidates = content.subject_candidates or []
    if candidates:
        return candidates[0].subject
    return content.subject


def subject_candidates_for_storage(
    content: NewsletterContent,
    selected_subject: str,
    manual_subject: str,
) -> list[NewsletterSubjectCandidate]:
    """Include manual overrides in candidates without dropping evaluated ones."""
    candidates = list(getattr(content, "subject_candidates", None) or [])
    if manual_subject and all(c.subject != manual_subject for c in candidates):
        top_score = max((c.score for c in candidates), default=0.0)
        candidates.insert(
            0,
            NewsletterSubjectCandidate(
                subject=manual_subject,
                score=round(top_score + 0.01, 2),
                rationale="manual override",
                source="manual",
                metadata={"manual_override": True},
            ),
        )
    return candidates


def candidate_to_dict(candidate: NewsletterSubjectCandidate) -> dict[str, Any]:
    return {
        "subject": candidate.subject,
        "score": candidate.score,
        "rationale": candidate.rationale,
        "source": candidate.source,
        "metadata": candidate.metadata,
    }


def extract_outbound_links(markdown: str) -> list[dict[str, Any]]:
    links = []
    seen = set()
    for match in re.finditer(r"\[([^\]]+)\]\(([^)\s]+)\)", markdown or ""):
        label = re.sub(r"\s+", " ", match.group(1)).strip()
        url = match.group(2).strip()
        if not url or url.startswith("#"):
            continue
        key = (label, url)
        if key in seen:
            continue
        seen.add(key)
        links.append({"label": label, "url": url})
    return links


def _preview_warnings(db: Any, content: NewsletterContent) -> list[str]:
    warnings = []
    if not (content.body_markdown or "").strip():
        warnings.append("No source content selected for newsletter preview.")

    for content_id in content.source_content_ids or []:
        source = _load_generated_content(db, content_id)
        if source is None:
            warnings.append(f"Content {content_id}: selected source content not found.")
            continue
        if not (source.get("content") or "").strip():
            warnings.append(f"Content {content_id}: selected source content is empty.")
        try:
            guard = check_publication_attribution_guard(
                db,
                content_id,
                content.body_markdown,
            )
        except Exception as exc:
            warnings.append(
                f"Content {content_id}: attribution check failed: {exc}"
            )
            continue
        for missing in guard.missing_sources:
            warnings.append(
                "Content {content_id}: missing attribution for knowledge "
                "{knowledge_id}".format(
                    content_id=content_id,
                    knowledge_id=missing.knowledge_id,
                )
            )
    return warnings


def _load_generated_content(db: Any, content_id: int) -> dict[str, Any] | None:
    getter = getattr(db, "get_generated_content", None)
    if callable(getter):
        return getter(content_id)
    row = db.conn.execute(
        "SELECT * FROM generated_content WHERE id = ?",
        (content_id,),
    ).fetchone()
    return dict(row) if row else None
