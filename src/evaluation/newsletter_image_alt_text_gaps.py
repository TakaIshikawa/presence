"""Find newsletter images with missing or weak alt text."""
from __future__ import annotations
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from ._batch_report_common import *

ARTIFACT_TYPE = "newsletter_image_alt_text_gaps"
DEFAULT_LIMIT = 50
GENERIC_ALT_TEXT = {"image", "photo", "picture", "screenshot", "graphic", "chart", "diagram", "img"}
_MD_IMAGE_RE = re.compile(r"!\[([^\]]*)\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
_IMG_RE = re.compile(r"<img\b[^>]*>", re.I)
_ATTR_RE = re.compile(r"([a-zA-Z_:][-a-zA-Z0-9_:.]*)\s*=\s*(['\"])(.*?)\2", re.S)


def _attrs(tag: str) -> dict[str, str]:
    return {m.group(1).lower(): m.group(3) for m in _ATTR_RE.finditer(tag)}


def classify_newsletter_image_alt_text(alt_text: Any, src: Any = None) -> tuple[str | None, str]:
    if alt_text is None:
        return "missing_alt", "missing"
    alt = clean(alt_text)
    if not alt:
        return "empty_alt", "empty"
    normalized = re.sub(r"[\W_]+", " ", alt.lower()).strip()
    if normalized in GENERIC_ALT_TEXT or re.fullmatch(r"(image|photo|screenshot|picture)\s*\d*", normalized):
        return "generic_alt", "generic"
    stem = Path(urlparse(clean(src)).path).stem.lower() if src else ""
    alt_stem = re.sub(r"[\W_]+", " ", alt.lower()).strip()
    stem_words = re.sub(r"[\W_]+", " ", stem).strip()
    if stem_words and alt_stem == stem_words:
        return "filename_like_alt", "filename_like"
    return None, "descriptive"


def _image_occurrences(text: Any) -> list[dict[str, Any]]:
    body = clean(text)
    images = []
    for match in _MD_IMAGE_RE.finditer(body):
        images.append({"image_url": match.group(2), "alt_text": match.group(1), "evidence_type": "markdown", "position": match.start()})
    for match in _IMG_RE.finditer(body):
        attrs = _attrs(match.group(0))
        images.append({"image_url": attrs.get("src"), "alt_text": attrs.get("alt"), "evidence_type": "html", "position": match.start()})
    return images


def build_newsletter_image_alt_text_gaps_report(
    rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    now=None,
) -> dict[str, Any]:
    positive("limit", limit)
    gen = now_value(now)
    findings = []
    records_scanned = 0
    image_count = 0
    for row in rows:
        record_images = []
        for field in ("body", "html", "content"):
            record_images.extend(_image_occurrences(row.get(field)))
        if not record_images:
            continue
        records_scanned += 1
        evidence_at = dt(row.get("updated_at") or row.get("created_at") or row.get("sent_at"))
        for image in record_images:
            image_count += 1
            reason, severity = classify_newsletter_image_alt_text(image.get("alt_text"), image.get("image_url"))
            if reason is None:
                continue
            findings.append(
                {
                    "record_id": clean(row.get("id") or row.get("record_id")),
                    "campaign_id": clean(row.get("campaign_id") or row.get("newsletter_id") or row.get("issue_id")) or None,
                    "draft_id": clean(row.get("draft_id") or row.get("id")) or None,
                    "title": clean(row.get("title") or row.get("subject")),
                    "image_url": clean(image.get("image_url")) or None,
                    "alt_text": image.get("alt_text"),
                    "reason": reason,
                    "severity": severity,
                    "evidence_type": image["evidence_type"],
                    "evidence_at": evidence_at.isoformat() if evidence_at else None,
                }
            )
    severity_rank = {"missing": 0, "empty": 1, "generic": 2, "filename_like": 3}
    findings.sort(key=lambda i: (severity_rank[i["severity"]], -(dt(i["evidence_at"]).timestamp() if i["evidence_at"] else 0), i["record_id"], i["image_url"] or ""))
    findings = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": gen.isoformat(),
        "filters": {"limit": limit},
        "summary": {"records_scanned": records_scanned, "image_count": image_count, "gap_count": len(findings)},
        "gaps": findings,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())},
        "empty_state": empty_state(findings, "No newsletter image alt text gaps found.", schema_gap=bool(missing_tables or missing_columns)),
    }


def build_newsletter_image_alt_text_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    sch = schema(conn)
    table_names = [t for t in ("newsletter_drafts", "newsletter_campaigns", "newsletter_sends") if t in sch]
    if not table_names:
        return build_newsletter_image_alt_text_gaps_report([], missing_tables=["newsletter_drafts"], **kwargs)
    rows = []
    for table in table_names:
        rows.extend(
            load_table(
                conn,
                table,
                sch[table],
                {
                    "id": ("id", "draft_id", "campaign_id"),
                    "draft_id": ("draft_id", "id"),
                    "campaign_id": ("campaign_id", "newsletter_id", "issue_id"),
                    "title": ("title", "subject"),
                    "body": ("body",),
                    "html": ("html",),
                    "content": ("content",),
                    "updated_at": ("updated_at", "created_at", "sent_at"),
                },
            )
        )
    return build_newsletter_image_alt_text_gaps_report(rows, **kwargs)


def format_newsletter_image_alt_text_gaps_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_newsletter_image_alt_text_gaps_text(report: dict[str, Any]) -> str:
    lines = [
        "Newsletter Image Alt Text Gaps",
        f"Generated: {report['generated_at']}",
        f"Totals: records={report['summary']['records_scanned']} images={report['summary']['image_count']} gaps={report['summary']['gap_count']}",
    ]
    if not report["gaps"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines += ["", "record_id | campaign_id | draft_id | severity | reason | alt_text | image_url"]
    lines += [f"{i['record_id']} | {i['campaign_id']} | {i['draft_id']} | {i['severity']} | {i['reason']} | {i['alt_text']} | {i['image_url']}" for i in report["gaps"]]
    return "\n".join(lines)
