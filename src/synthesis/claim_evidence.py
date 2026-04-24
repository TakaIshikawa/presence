"""Export review artifacts for generated-content claim evidence."""

from __future__ import annotations

import json
from typing import Any

from synthesis.claim_checker import Claim, ClaimChecker


SUPPORTED_STATUSES = {"all", "supported", "unsupported"}


def _shorten(value: Any, width: int = 180) -> str:
    if value is None:
        return ""
    text = str(value).replace("\n", " ").strip()
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)].rstrip() + "..."


def _json_safe(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"encoding": "hex", "data": value.hex()}
    if isinstance(value, dict):
        return {str(key): _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_json_safe(inner) for inner in value]
    if isinstance(value, tuple):
        return [_json_safe(inner) for inner in value]
    return value


def _content_status(code: int | None) -> str:
    if code == 1:
        return "published"
    if code == -1:
        return "abandoned"
    return "unpublished"


def _claim_check_status(summary: dict[str, Any] | None) -> str:
    if summary is None:
        return "unchecked"
    if int(summary.get("unsupported_count") or 0) > 0:
        return "unsupported"
    return "supported"


def _claim_supported(claim: Claim, unsupported: set[str]) -> bool:
    return claim.text not in unsupported


def _source_texts(
    commits: list[dict[str, Any]],
    messages: list[dict[str, Any]],
    knowledge_links: list[dict[str, Any]],
) -> tuple[list[str], list[str], list[str]]:
    commit_texts = [
        str(row.get("commit_message") or "")
        for row in commits
        if row.get("matched", True) and row.get("commit_message")
    ]
    message_texts = [
        str(row.get("prompt_text") or "")
        for row in messages
        if row.get("matched", True) and row.get("prompt_text")
    ]
    knowledge_texts = []
    for row in knowledge_links:
        if not row.get("matched", True):
            continue
        text = "\n".join(
            str(part)
            for part in (row.get("insight"), row.get("content"))
            if part
        )
        if text:
            knowledge_texts.append(text)
    return commit_texts, message_texts, knowledge_texts


def _claim_payload(claim: Claim, supported: bool) -> dict[str, Any]:
    return {
        "text": claim.text,
        "kind": claim.kind,
        "supported": supported,
        "terms": list(claim.terms),
        "matched_terms": list(claim.matched_terms),
        "reason": "" if supported else claim.reason,
    }


def _commit_reference(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "commit",
        "ref": row.get("commit_sha"),
        "matched": bool(row.get("matched", True)),
        "label": " ".join(
            str(part)
            for part in (row.get("repo_name"), row.get("commit_sha"))
            if part
        )
        or row.get("commit_sha"),
        "text": _shorten(row.get("commit_message"), 220),
        "timestamp": row.get("timestamp"),
        "author": row.get("author"),
    }


def _message_reference(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "message",
        "ref": row.get("message_uuid"),
        "matched": bool(row.get("matched", True)),
        "label": row.get("message_uuid"),
        "text": _shorten(row.get("prompt_text"), 220),
        "timestamp": row.get("timestamp"),
        "author": None,
    }


def _knowledge_reference(row: dict[str, Any]) -> dict[str, Any]:
    label_parts = [
        row.get("author") or row.get("source_type"),
        row.get("source_id") or f"knowledge #{row.get('knowledge_id') or row.get('id')}",
    ]
    return {
        "type": "knowledge",
        "ref": row.get("knowledge_id") or row.get("id"),
        "matched": bool(row.get("matched", True)),
        "label": " ".join(str(part) for part in label_parts if part),
        "text": _shorten(row.get("insight") or row.get("content"), 220),
        "url": row.get("source_url"),
        "relevance_score": row.get("relevance_score"),
    }


def _warning_messages(
    commits: list[dict[str, Any]],
    messages: list[dict[str, Any]],
    knowledge_links: list[dict[str, Any]],
) -> list[str]:
    warnings = []
    for row in commits:
        if not row.get("matched", True):
            warnings.append(f"Missing source commit row for {row.get('commit_sha')}")
    for row in messages:
        if not row.get("matched", True):
            warnings.append(f"Missing source message row for {row.get('message_uuid')}")
    for row in knowledge_links:
        if not row.get("matched", True):
            warnings.append(f"Missing knowledge row for {row.get('knowledge_id')}")
    return warnings


def _content_metadata(content: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": content.get("id"),
        "content_type": content.get("content_type"),
        "content_format": content.get("content_format"),
        "publication_status": _content_status(content.get("published")),
        "published_url": content.get("published_url"),
        "created_at": content.get("created_at"),
        "published_at": content.get("published_at"),
        "eval_score": content.get("eval_score"),
        "eval_feedback": content.get("eval_feedback"),
    }


def get_content_knowledge_links(db: Any, content_id: int) -> list[dict[str, Any]]:
    """Return knowledge links, keeping dangling link rows as unmatched references."""
    rows = db.conn.execute(
        """SELECT ckl.knowledge_id,
                  ckl.relevance_score,
                  ckl.created_at AS linked_at,
                  k.id,
                  k.source_type,
                  k.source_id,
                  k.source_url,
                  k.author,
                  k.content,
                  k.insight,
                  k.attribution_required
           FROM content_knowledge_links ckl
           LEFT JOIN knowledge k ON k.id = ckl.knowledge_id
           WHERE ckl.content_id = ?
           ORDER BY ckl.relevance_score DESC, ckl.id ASC""",
        (content_id,),
    ).fetchall()
    links = []
    for row in rows:
        link = dict(row)
        link["matched"] = link.get("id") is not None
        links.append(link)
    return links


def list_claim_checked_content_ids(db: Any, status: str = "all") -> list[int]:
    """List generated_content IDs that have claim-check summaries."""
    if status not in SUPPORTED_STATUSES:
        raise ValueError(f"status must be one of: {', '.join(sorted(SUPPORTED_STATUSES))}")

    where = []
    if status == "supported":
        where.append("ccc.unsupported_count = 0")
    elif status == "unsupported":
        where.append("ccc.unsupported_count > 0")
    where_sql = "WHERE " + " AND ".join(where) if where else ""
    rows = db.conn.execute(
        f"""SELECT gc.id
            FROM generated_content gc
            INNER JOIN content_claim_checks ccc ON ccc.content_id = gc.id
            {where_sql}
            ORDER BY ccc.updated_at DESC, gc.id DESC"""
    ).fetchall()
    return [int(row["id"]) for row in rows]


def load_claim_evidence(db: Any, content_id: int) -> dict[str, Any]:
    """Build one compact claim-evidence review payload."""
    content = db.get_generated_content(content_id)
    if content is None:
        raise ValueError(f"Content ID {content_id} not found")

    summary = db.get_claim_check_summary(content_id)
    commits = db.get_source_commits_for_content(content_id)
    messages = db.get_source_messages_for_content(content_id)
    knowledge_links = get_content_knowledge_links(db, content_id)

    commit_texts, message_texts, knowledge_texts = _source_texts(
        commits,
        messages,
        knowledge_links,
    )
    result = ClaimChecker().check(
        content.get("content") or "",
        source_prompts=message_texts,
        source_commits=commit_texts,
        linked_knowledge=knowledge_texts,
    )
    unsupported_texts = {claim.text for claim in result.unsupported_claims}
    claims = [
        _claim_payload(claim, _claim_supported(claim, unsupported_texts))
        for claim in result.claims
    ]
    claims.sort(key=lambda claim: (claim["supported"], claim["text"]))

    references = (
        [_commit_reference(row) for row in commits]
        + [_message_reference(row) for row in messages]
        + [_knowledge_reference(row) for row in knowledge_links]
    )

    payload = {
        "content": _content_metadata(content),
        "text": content.get("content"),
        "claim_check": {
            "checked": summary is not None,
            "status": _claim_check_status(summary),
            "supported_count": int(summary.get("supported_count") or 0) if summary else 0,
            "unsupported_count": int(summary.get("unsupported_count") or 0) if summary else 0,
            "annotation_text": summary.get("annotation_text") if summary else None,
            "created_at": summary.get("created_at") if summary else None,
            "updated_at": summary.get("updated_at") if summary else None,
        },
        "claims": claims,
        "source_references": references,
        "warnings": _warning_messages(commits, messages, knowledge_links),
    }
    return _json_safe(payload)


def load_claim_evidence_export(
    db: Any,
    *,
    content_id: int | None = None,
    status: str = "all",
) -> dict[str, Any] | list[dict[str, Any]]:
    """Load one content payload or a status-filtered list of payloads."""
    if content_id is not None:
        payload = load_claim_evidence(db, content_id)
        if status != "all" and payload["claim_check"]["status"] != status:
            return []
        return payload
    return [load_claim_evidence(db, item_id) for item_id in list_claim_checked_content_ids(db, status)]


def format_claim_evidence_json(payload: dict[str, Any] | list[dict[str, Any]]) -> str:
    """Render claim evidence as deterministic JSON."""
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


def _format_reference(reference: dict[str, Any]) -> str:
    marker = "" if reference.get("matched") else " [missing]"
    label = reference.get("label") or reference.get("ref") or "-"
    text = reference.get("text") or "no text available"
    score = reference.get("relevance_score")
    score_text = f" ({score:.3f})" if isinstance(score, (int, float)) else ""
    url = f" <{reference['url']}>" if reference.get("url") else ""
    return f"- {reference.get('type')}: {label}{score_text}{marker} - {text}{url}"


def _format_one_markdown(payload: dict[str, Any]) -> list[str]:
    content = payload["content"]
    claim_check = payload["claim_check"]
    lines = [
        f"# Claim Evidence: Content #{content.get('id')}",
        "",
        "## Content",
        f"- Type: {content.get('content_type') or '-'}",
        f"- Format: {content.get('content_format') or '-'}",
        f"- Publication: {content.get('publication_status') or '-'}",
        f"- Created: {content.get('created_at') or '-'}",
        f"- Eval score: {content.get('eval_score') if content.get('eval_score') is not None else '-'}",
        "",
        payload.get("text") or "",
        "",
        "## Claim Check Summary",
        f"- Status: {claim_check.get('status')}",
        f"- Supported claims: {claim_check.get('supported_count')}",
        f"- Unsupported claims: {claim_check.get('unsupported_count')}",
    ]
    if claim_check.get("annotation_text"):
        lines.extend(["- Annotations:"] + [f"  - {line}" for line in claim_check["annotation_text"].splitlines()])

    unsupported = [claim for claim in payload["claims"] if not claim["supported"]]
    supported = [claim for claim in payload["claims"] if claim["supported"]]
    lines.extend(["", f"## Unsupported Claims ({len(unsupported)})"])
    if unsupported:
        for claim in unsupported:
            matched = ", ".join(claim["matched_terms"]) or "none"
            lines.append(f"- {claim['kind']}: {claim['text']}")
            lines.append(f"  - Reason: {claim['reason'] or '-'}")
            lines.append(f"  - Matched terms: {matched}")
    else:
        lines.append("- none")

    lines.extend(["", f"## Supported Claims ({len(supported)})"])
    if supported:
        for claim in supported:
            matched = ", ".join(claim["matched_terms"]) or "none"
            lines.append(f"- {claim['kind']}: {claim['text']}")
            lines.append(f"  - Matched terms: {matched}")
    else:
        lines.append("- none")

    lines.extend(["", f"## Source References ({len(payload['source_references'])})"])
    if payload["source_references"]:
        lines.extend(_format_reference(reference) for reference in payload["source_references"])
    else:
        lines.append("- none")

    lines.extend(["", f"## Warnings ({len(payload['warnings'])})"])
    if payload["warnings"]:
        lines.extend(f"- {warning}" for warning in payload["warnings"])
    else:
        lines.append("- none")
    return lines


def format_claim_evidence_markdown(payload: dict[str, Any] | list[dict[str, Any]]) -> str:
    """Render claim evidence as readable markdown, with unsupported claims first."""
    if isinstance(payload, list):
        lines = [f"# Claim Evidence Export ({len(payload)} items)"]
        for item in payload:
            lines.extend(["", "---", ""])
            lines.extend(_format_one_markdown(item))
        return "\n".join(lines)
    return "\n".join(_format_one_markdown(payload))
