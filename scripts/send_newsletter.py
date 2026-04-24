#!/usr/bin/env python3
"""Assemble and send weekly newsletter via Buttondown."""

import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from output.newsletter import (
    NewsletterAssembler,
    NewsletterSubjectCandidate,
    ButtondownClient,
)
from output.newsletter_preview import (
    build_preview_payload as _build_shared_preview_payload,
    write_preview_artifact as _write_shared_preview_artifact,
)


def _arg_value(name: str) -> str:
    """Read a simple --name value CLI option without changing existing argv usage."""
    if name not in sys.argv:
        return ""
    index = sys.argv.index(name)
    if index + 1 >= len(sys.argv):
        return ""
    return sys.argv[index + 1].strip()


def _config_text(obj, *names: str) -> str:
    """Return the first configured string value from a possibly mocked config."""
    for name in names:
        value = getattr(obj, name, "")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _manual_subject_override(config) -> str:
    """Resolve an explicit subject override from CLI or newsletter config."""
    return (
        _arg_value("--subject")
        or _config_text(
            config.newsletter,
            "subject_override",
            "manual_subject",
            "subject",
        )
    )


def _select_subject(content, manual_subject: str = "") -> str:
    """Pick the outgoing subject while letting manual overrides win."""
    if manual_subject:
        return manual_subject
    candidates = getattr(content, "subject_candidates", None) or []
    if candidates:
        return candidates[0].subject
    return content.subject


def _subject_candidates_for_storage(content, selected_subject: str, manual_subject: str):
    """Include manual overrides in persisted candidates without dropping evaluated ones."""
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


def _candidate_to_dict(candidate: NewsletterSubjectCandidate) -> dict:
    """Serialize a subject candidate for preview output."""
    return {
        "subject": candidate.subject,
        "score": candidate.score,
        "rationale": candidate.rationale,
        "source": candidate.source,
        "metadata": candidate.metadata,
    }


def _selected_candidate(
    candidates: list[NewsletterSubjectCandidate], selected_subject: str
) -> NewsletterSubjectCandidate | None:
    """Return the stored candidate that matches the selected subject."""
    for candidate in candidates:
        if candidate.subject == selected_subject:
            return candidate
    return None


def _subject_selection_payload(
    selected_subject: str,
    candidates: list[NewsletterSubjectCandidate],
    manual_subject: str,
) -> dict:
    """Build a structured explanation for the selected subject line."""
    ranked_candidates = [_candidate_to_dict(candidate) for candidate in candidates]
    selected_candidate = _selected_candidate(candidates, selected_subject)
    payload = {
        "selected_subject": selected_subject,
        "manual_subject": manual_subject,
        "selected_candidate": _candidate_to_dict(selected_candidate)
        if selected_candidate
        else {},
        "ranked_candidates": ranked_candidates,
        "alternatives": [
            candidate for candidate in ranked_candidates if candidate["subject"] != selected_subject
        ],
    }
    if selected_candidate and selected_candidate.metadata.get("history"):
        payload["history"] = selected_candidate.metadata["history"]
    return payload


def _utm_metadata(config, content) -> dict:
    """Collect UTM configuration and generated campaign metadata for review."""
    newsletter = config.newsletter
    metadata = {
        "utm_source": _config_text(newsletter, "utm_source"),
        "utm_medium": _config_text(newsletter, "utm_medium"),
        "utm_campaign_template": _config_text(newsletter, "utm_campaign_template"),
    }
    metadata.update(getattr(content, "metadata", None) or {})
    return metadata


def _preview_payload(
    db,
    config,
    content,
    selected_subject: str,
    candidates: list[NewsletterSubjectCandidate],
    manual_subject: str,
    week_start: datetime,
    week_end: datetime,
) -> dict:
    """Build the structured newsletter preview payload."""
    return _build_shared_preview_payload(
        db,
        config,
        content,
        selected_subject,
        candidates,
        manual_subject,
        week_start,
        week_end,
    )


def _format_preview_json(payload: dict) -> str:
    """Format a newsletter preview as JSON."""
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _format_preview_markdown(payload: dict) -> str:
    """Format a newsletter preview as Markdown."""
    candidates = payload["subject_candidates"]
    selection = payload.get("subject_selection") or {}
    candidate_lines = [
        (
            f"- {candidate['subject']} "
            f"(score: {candidate['score']}, source: {candidate['source']})"
            + (f" - {candidate['rationale']}" if candidate["rationale"] else "")
        )
        for candidate in candidates
    ]
    if not candidate_lines:
        candidate_lines = ["- None"]

    source_ids = ", ".join(str(item) for item in payload["source_content_ids"]) or "None"
    utm_block = _format_preview_json(payload["utm_metadata"]).strip()
    return (
        f"# Newsletter Preview\n\n"
        f"## Week Range\n\n"
        f"- Start: {payload['week_range']['start']}\n"
        f"- End: {payload['week_range']['end']}\n\n"
        f"## Selected Subject\n\n"
        f"{payload['selected_subject']}\n\n"
        f"## Subject Selection\n\n"
        f"- Manual subject: {selection.get('manual_subject') or 'None'}\n"
        f"- Rationale: {selection.get('selected_candidate', {}).get('rationale') or 'None'}\n"
        f"- History: {json.dumps(selection.get('history', {}), sort_keys=True)}\n\n"
        f"## Source Content IDs\n\n"
        f"{source_ids}\n\n"
        f"## UTM Metadata\n\n"
        f"```json\n{utm_block}\n```\n\n"
        f"## Subject Candidates\n\n"
        f"{chr(10).join(candidate_lines)}\n\n"
        f"## Body\n\n"
        f"{payload['body_markdown']}\n"
    )


def _write_preview_artifact(path: Path, payload: dict) -> None:
    """Write a newsletter preview as JSON or Markdown based on extension."""
    _write_shared_preview_artifact(path, payload)


def _persist_subject_candidates(
    db,
    candidates: list[NewsletterSubjectCandidate],
    content_ids: list[int],
    week_start: datetime,
    week_end: datetime,
    selected_subject: str,
):
    """Persist evaluated subject candidates when storage support is available."""
    inserter = getattr(db, "insert_newsletter_subject_candidates", None)
    if not callable(inserter) or not candidates:
        return []

    try:
        return inserter(
            candidates,
            content_ids=content_ids,
            week_start=week_start,
            week_end=week_end,
            selected_subject=selected_subject,
        )
    except Exception as e:
        logger.debug("Newsletter subject candidate storage failed: %s", e)
        return []


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    with script_context() as (config, db):
        if not config.newsletter or not config.newsletter.enabled:
            logger.info("Newsletter not enabled, skipping")
            return

        preview_out = _arg_value("--preview-out")
        preview_mode = bool(preview_out)

        if not preview_mode and not config.newsletter.api_key:
            logger.warning("Newsletter API key not configured, skipping")
            return

        # Idempotency: skip if already sent this week
        last_send = db.get_last_newsletter_send()
        if (
            not preview_mode
            and last_send
            and (datetime.now(timezone.utc) - last_send).days < 6
        ):
            logger.info(f"Newsletter already sent {(datetime.now(timezone.utc) - last_send).days} days ago, skipping")
            return

        # Compute week range (last 7 days)
        now = datetime.now(timezone.utc)
        week_end = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = week_end - timedelta(days=7)

        logger.info(f"Assembling newsletter for {week_start.date()} to {week_end.date()}")

        # Assemble content
        assembler = NewsletterAssembler(
            db,
            site_url="https://takaishikawa.com",
            utm_source=getattr(config.newsletter, "utm_source", ""),
            utm_medium=getattr(config.newsletter, "utm_medium", ""),
            utm_campaign_template=getattr(
                config.newsletter, "utm_campaign_template", ""
            ),
        )
        content = assembler.assemble(week_start, week_end)

        if not content.body_markdown.strip():
            logger.info("No content published this week, skipping newsletter")
            return

        manual_subject = _manual_subject_override(config)
        selected_subject = _select_subject(content, manual_subject)
        logger.info(f"Subject: {selected_subject}")
        logger.debug(f"Content IDs included: {content.source_content_ids}")

        candidates_for_storage = _subject_candidates_for_storage(
            content,
            selected_subject=selected_subject,
            manual_subject=manual_subject,
        )

        if preview_mode:
            payload = _preview_payload(
                db,
                config,
                content,
                selected_subject,
                candidates_for_storage,
                manual_subject,
                week_start,
                week_end,
            )
            _write_preview_artifact(Path(preview_out), payload)
            logger.info(f"Newsletter preview written to {preview_out}")
            if "--persist-candidates" in sys.argv:
                _persist_subject_candidates(
                    db,
                    candidates_for_storage,
                    content.source_content_ids,
                    week_start,
                    week_end,
                    selected_subject,
                )
            return

        # Check for --dry-run flag
        if "--dry-run" in sys.argv:
            logger.info("\n--- DRY RUN (not sending) ---\n")
            logger.info(content.body_markdown)
            return

        # Send via Buttondown
        client = ButtondownClient(config.newsletter.api_key, timeout=config.timeouts.http_seconds)
        subscriber_count = client.get_subscriber_count()
        logger.info(f"Subscribers: {subscriber_count}")

        candidate_ids = _persist_subject_candidates(
            db,
            candidates_for_storage,
            content.source_content_ids,
            week_start,
            week_end,
            selected_subject,
        )

        result = client.send(selected_subject, content.body_markdown)

        if result.success:
            subject_selection = _subject_selection_payload(
                selected_subject,
                candidates_for_storage,
                manual_subject,
            )
            send_metadata = dict(getattr(content, "metadata", None) or {})
            send_metadata["subject_selection"] = subject_selection
            send_kwargs = {
                "issue_id": result.issue_id or "",
                "subject": selected_subject,
                "content_ids": content.source_content_ids,
                "subscriber_count": subscriber_count,
                "metadata": send_metadata,
            }
            newsletter_send_id = db.insert_newsletter_send(**send_kwargs)
            updater = getattr(db, "update_newsletter_subject_candidates_send", None)
            if callable(updater) and candidate_ids:
                try:
                    updater(
                        candidate_ids,
                        newsletter_send_id=newsletter_send_id,
                        issue_id=result.issue_id or "",
                    )
                except Exception as e:
                    logger.debug("Newsletter subject candidate linking failed: %s", e)
            logger.info(f"Newsletter sent: {result.url}")
        else:
            logger.error(f"Send failed: {result.error}")

    update_monitoring("send-newsletter")
    logger.info("Done")


if __name__ == "__main__":
    main()
