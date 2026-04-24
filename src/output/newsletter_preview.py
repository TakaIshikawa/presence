"""Newsletter draft preview artifact helpers."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from output.newsletter import (
    NewsletterAssembler,
    NewsletterContent,
    NewsletterSubjectCandidate,
)


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


def select_subject(content: NewsletterContent, manual_subject: str = "") -> str:
    """Pick the outgoing subject while letting manual overrides win."""
    if manual_subject:
        return manual_subject
    candidates = getattr(content, "subject_candidates", None) or []
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


def candidate_to_dict(candidate: NewsletterSubjectCandidate | None) -> dict:
    """Serialize a subject candidate for preview output."""
    if candidate is None:
        return {}
    return {
        "subject": candidate.subject,
        "score": candidate.score,
        "rationale": candidate.rationale,
        "source": candidate.source,
        "metadata": candidate.metadata,
    }


def selected_candidate(
    candidates: list[NewsletterSubjectCandidate], selected_subject: str
) -> NewsletterSubjectCandidate | None:
    """Return the stored candidate that matches the selected subject."""
    for candidate in candidates:
        if candidate.subject == selected_subject:
            return candidate
    return None


def subject_selection_payload(
    selected_subject: str,
    candidates: list[NewsletterSubjectCandidate],
    manual_subject: str,
) -> dict:
    """Build a structured explanation for the selected subject line."""
    ranked_candidates = [candidate_to_dict(candidate) for candidate in candidates]
    picked = selected_candidate(candidates, selected_subject)
    payload = {
        "selected_subject": selected_subject,
        "manual_subject": manual_subject,
        "selected_candidate": candidate_to_dict(picked),
        "ranked_candidates": ranked_candidates,
        "alternatives": [
            candidate
            for candidate in ranked_candidates
            if candidate["subject"] != selected_subject
        ],
    }
    if picked and picked.metadata.get("history"):
        payload["history"] = picked.metadata["history"]
    return payload


def utm_metadata(config: object, content: NewsletterContent) -> dict:
    """Collect UTM configuration and generated campaign metadata for review."""
    newsletter = getattr(config, "newsletter", None)
    metadata = {
        "utm_source": config_text(newsletter, "utm_source"),
        "utm_medium": config_text(newsletter, "utm_medium"),
        "utm_campaign_template": config_text(newsletter, "utm_campaign_template"),
    }
    metadata.update(getattr(content, "metadata", None) or {})
    return metadata


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


def assemble_newsletter_preview(
    db: object,
    config: object,
    week_start: datetime,
    week_end: datetime,
    manual_subject: str = "",
) -> dict:
    """Assemble newsletter content and return a complete preview payload."""
    content = make_newsletter_assembler(db, config).assemble(week_start, week_end)
    selected_subject = select_subject(content, manual_subject)
    candidates = subject_candidates_for_storage(
        content,
        selected_subject=selected_subject,
        manual_subject=manual_subject,
    )
    return build_preview_payload(
        db,
        config,
        content,
        selected_subject,
        candidates,
        manual_subject,
        week_start,
        week_end,
    )


def build_preview_payload(
    db: object,
    config: object,
    content: NewsletterContent,
    selected_subject: str,
    candidates: list[NewsletterSubjectCandidate],
    manual_subject: str,
    week_start: datetime,
    week_end: datetime,
) -> dict:
    """Build the structured newsletter preview payload."""
    body = content.body_markdown
    selected_posts, warnings = selected_post_payloads(db, content.source_content_ids, body)
    warnings.extend(body_warning_payloads(body))
    outbound_links = extract_outbound_links(body)
    return {
        "subject": selected_subject,
        "selected_subject": selected_subject,
        "intro": extract_intro(body),
        "body_markdown": body,
        "body_sections": extract_body_sections(body),
        "selected_posts": selected_posts,
        "source_content_ids": content.source_content_ids,
        "outbound_links": outbound_links,
        "warnings": warnings,
        "warning_metadata": warning_metadata(warnings),
        "subject_candidates": [candidate_to_dict(candidate) for candidate in candidates],
        "subject_selection": subject_selection_payload(
            selected_subject,
            candidates,
            manual_subject,
        ),
        "week_range": {
            "start": week_start.isoformat(),
            "end": week_end.isoformat(),
        },
        "utm_metadata": utm_metadata(config, content),
    }


def selected_post_payloads(
    db: object,
    content_ids: list[int],
    newsletter_body: str,
) -> tuple[list[dict], list[dict]]:
    """Return selected source content metadata and source-level warnings."""
    selected = []
    warnings = []
    for content_id in content_ids:
        item = _get_generated_content(db, content_id)
        if not item:
            warnings.append(
                {
                    "type": "missing_content",
                    "content_id": content_id,
                    "message": f"Selected content {content_id} was not found.",
                }
            )
            continue

        metrics = _engagement_summary(db, content_id)
        citations = _citation_summary(db, content_id, newsletter_body)
        selected.append(
            {
                "id": item["id"],
                "content_type": item.get("content_type"),
                "content_format": item.get("content_format"),
                "published_url": item.get("published_url") or "",
                "published_at": item.get("published_at") or "",
                "content": item.get("content") or "",
                "metrics": metrics,
                "citations": citations,
            }
        )
        if not metrics.get("has_metrics"):
            warnings.append(
                {
                    "type": "missing_metrics",
                    "content_id": content_id,
                    "message": f"Selected content {content_id} has no engagement metrics.",
                }
            )
        for source in citations["missing"]:
            warnings.append(
                {
                    "type": "missing_citation",
                    "content_id": content_id,
                    "knowledge_id": source["knowledge_id"],
                    "source_type": source.get("source_type") or "",
                    "message": (
                        f"Selected content {content_id} is missing citation "
                        f"metadata for knowledge {source['knowledge_id']}."
                    ),
                }
            )
    return selected, warnings


def extract_intro(markdown: str) -> str:
    """Return body text before the first second-level section heading."""
    text = markdown or ""
    match = re.search(r"^##\s+", text, flags=re.MULTILINE)
    if match:
        text = text[: match.start()]
    return text.strip()


def extract_body_sections(markdown: str) -> list[dict]:
    """Split rendered newsletter Markdown into reviewable section blocks."""
    sections = []
    matches = list(re.finditer(r"^##\s+(.+?)\s*$", markdown or "", flags=re.MULTILINE))
    for index, match in enumerate(matches):
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(markdown)
        sections.append(
            {
                "title": match.group(1).strip(),
                "markdown": (markdown[start:end] or "").strip(),
            }
        )
    return sections


def extract_outbound_links(markdown: str) -> list[dict]:
    """Extract unique outbound links from Markdown and bare URLs."""
    links = []
    seen = set()

    def add(url: str, label: str = "") -> None:
        cleaned = (url or "").strip().rstrip(".,)")
        if not cleaned or cleaned in seen:
            return
        seen.add(cleaned)
        links.append({"url": cleaned, "label": label.strip()})

    for label, url in re.findall(r"\[([^\]]+)\]\((https?://[^)\s]+)\)", markdown or ""):
        add(url, label)
    for url in re.findall(r"(?<!\()https?://[^\s<>()]+", markdown or ""):
        add(url)
    return links


def body_warning_payloads(markdown: str) -> list[dict]:
    """Return body-level warnings for review metadata."""
    warnings = []
    if not extract_outbound_links(markdown):
        warnings.append(
            {
                "type": "missing_outbound_links",
                "message": "Newsletter body contains no outbound links.",
            }
        )
    return warnings


def warning_metadata(warnings: list[dict]) -> dict:
    """Summarize warning counts by type."""
    by_type: dict[str, int] = {}
    for warning in warnings:
        key = warning.get("type") or "unknown"
        by_type[key] = by_type.get(key, 0) + 1
    return {"count": len(warnings), "by_type": by_type}


def format_preview_json(payload: dict) -> str:
    """Format a newsletter preview as JSON."""
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def format_preview_markdown(payload: dict) -> str:
    """Format a newsletter preview as Markdown."""
    candidate_lines = [
        (
            f"- {candidate['subject']} "
            f"(score: {candidate['score']}, source: {candidate['source']})"
            + (f" - {candidate['rationale']}" if candidate["rationale"] else "")
        )
        for candidate in payload.get("subject_candidates", [])
    ] or ["- None"]
    source_ids = ", ".join(str(item) for item in payload.get("source_content_ids", [])) or "None"
    link_lines = [
        f"- [{link.get('label') or link['url']}]({link['url']})"
        for link in payload.get("outbound_links", [])
    ] or ["- None"]
    warning_lines = [
        f"- {warning.get('type', 'warning')}: {warning.get('message', '')}"
        for warning in payload.get("warnings", [])
    ] or ["- None"]
    post_lines = [
        (
            f"- {post['id']} ({post.get('content_type') or 'unknown'}): "
            f"{post.get('published_url') or 'no published URL'}"
        )
        for post in payload.get("selected_posts", [])
    ] or ["- None"]
    utm_block = format_preview_json(payload.get("utm_metadata", {})).strip()
    warning_block = format_preview_json(payload.get("warning_metadata", {})).strip()
    selection = payload.get("subject_selection") or {}
    return (
        "# Newsletter Preview\n\n"
        "## Week Range\n\n"
        f"- Start: {payload['week_range']['start']}\n"
        f"- End: {payload['week_range']['end']}\n\n"
        "## Selected Subject\n\n"
        f"{payload['subject']}\n\n"
        "## Intro\n\n"
        f"{payload.get('intro') or 'None'}\n\n"
        "## Source Content IDs\n\n"
        f"{source_ids}\n\n"
        "## Selected Posts\n\n"
        f"{chr(10).join(post_lines)}\n\n"
        "## Outbound Links\n\n"
        f"{chr(10).join(link_lines)}\n\n"
        "## Warnings\n\n"
        f"{chr(10).join(warning_lines)}\n\n"
        "## Warning Metadata\n\n"
        f"```json\n{warning_block}\n```\n\n"
        "## UTM Metadata\n\n"
        f"```json\n{utm_block}\n```\n\n"
        "## Subject Selection\n\n"
        f"- Manual subject: {selection.get('manual_subject') or 'None'}\n"
        f"- Rationale: {selection.get('selected_candidate', {}).get('rationale') or 'None'}\n"
        f"- History: {json.dumps(selection.get('history', {}), sort_keys=True)}\n\n"
        "## Subject Candidates\n\n"
        f"{chr(10).join(candidate_lines)}\n\n"
        "## Body\n\n"
        f"{payload['body_markdown']}\n"
    )


def write_preview_artifact(path: Path, payload: dict) -> None:
    """Write a newsletter preview as JSON or Markdown based on extension."""
    suffix = path.suffix.lower()
    if suffix in {".md", ".markdown"}:
        rendered = format_preview_markdown(payload)
    else:
        rendered = format_preview_json(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(rendered, encoding="utf-8")


def _get_generated_content(db: object, content_id: int) -> dict | None:
    getter = getattr(db, "get_generated_content", None)
    if callable(getter):
        item = getter(content_id)
        return item if isinstance(item, dict) else None
    conn = getattr(db, "conn", None)
    if conn is None:
        return None
    row = conn.execute("SELECT * FROM generated_content WHERE id = ?", (content_id,)).fetchone()
    return dict(row) if row else None


def _engagement_summary(db: object, content_id: int) -> dict:
    getter = getattr(db, "get_engagement_snapshots_for_content", None)
    snapshots = getter(content_id) if callable(getter) else []
    snapshots = snapshots if isinstance(snapshots, list) else []
    latest = snapshots[-1] if snapshots else None
    return {
        "has_metrics": bool(snapshots),
        "snapshot_count": len(snapshots),
        "latest": _jsonable_dict(latest) if latest else {},
    }


def _citation_summary(db: object, content_id: int, newsletter_body: str) -> dict:
    getter = getattr(db, "get_content_lineage", None)
    links = getter(content_id) if callable(getter) else []
    links = links if isinstance(links, list) else []
    cited = []
    missing = []
    for link in links:
        source_url = (link.get("source_url") or "").strip()
        source = {
            "knowledge_id": link.get("id") or link.get("knowledge_id"),
            "source_type": link.get("source_type") or "",
            "source_url": source_url,
            "author": link.get("author") or "",
        }
        is_external = source["source_type"] not in {"own_post", "own_conversation"}
        if not is_external:
            continue
        if not source_url or source_url not in newsletter_body:
            missing.append(source)
        else:
            cited.append(source)
    return {
        "linked_count": len(links),
        "cited_count": len(cited),
        "missing_count": len(missing),
        "missing": missing,
    }


def _jsonable_dict(value: dict[str, Any] | None) -> dict:
    if not value:
        return {}
    return {key: item for key, item in value.items()}
