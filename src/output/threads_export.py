"""Build manual Threads publishing artifacts from generated content."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .platform_adapter import count_graphemes
from .x_client import parse_thread_content


THREADS_CHARACTER_LIMIT = 500
THREADS_PLATFORM = "threads"
_WHITESPACE_RE = re.compile(r"[ \t]+")


@dataclass(frozen=True)
class ThreadsExportOptions:
    """Configuration for Threads artifact generation."""

    character_limit: int = THREADS_CHARACTER_LIMIT


@dataclass(frozen=True)
class ThreadsExport:
    """A Threads-ready review artifact."""

    platform: str
    content_id: int
    source_content_id: int
    text: str
    thread_parts: tuple[str, ...]
    scheduled_at: str | None
    provenance: dict[str, Any]
    validation_warnings: tuple[str, ...]
    variant: dict[str, Any] | None = None


class ThreadsExportError(ValueError):
    """Raised when a Threads export cannot be built."""


def build_threads_export(
    content: dict[str, Any],
    *,
    variants: list[dict[str, Any]] | None = None,
    scheduled_at: str | None = None,
    options: ThreadsExportOptions | None = None,
) -> ThreadsExport:
    """Transform one generated content row into a Threads review artifact."""
    options = options or ThreadsExportOptions()
    if options.character_limit <= 0:
        raise ThreadsExportError("character_limit must be positive")

    content_id = int(content["id"])
    variant = _select_threads_variant(variants or [])
    source_text = str((variant or content).get("content") or "")
    content_type = str(content.get("content_type") or "")
    variant_type = str((variant or {}).get("variant_type") or "")
    parts = _thread_parts(source_text, content_type=content_type, variant_type=variant_type)
    text = "\n\n".join(parts) if len(parts) > 1 else (parts[0] if parts else "")
    provenance = _provenance(content)
    warnings = _validation_warnings(
        text=text,
        thread_parts=parts,
        provenance=provenance,
        character_limit=options.character_limit,
    )

    return ThreadsExport(
        platform=THREADS_PLATFORM,
        content_id=content_id,
        source_content_id=content_id,
        text=text,
        thread_parts=tuple(parts) if len(parts) > 1 else (),
        scheduled_at=scheduled_at,
        provenance=provenance,
        validation_warnings=tuple(warnings),
        variant=_variant_summary(variant),
    )


def build_threads_exports_from_db(
    db: Any,
    *,
    content_id: int | None = None,
    status: str | None = None,
    limit: int = 20,
    options: ThreadsExportOptions | None = None,
) -> list[ThreadsExport]:
    """Fetch generated content rows and build Threads artifacts."""
    if limit <= 0:
        raise ThreadsExportError("limit must be positive")

    rows = _content_rows(db, content_id=content_id, status=status, limit=limit)
    exports: list[ThreadsExport] = []
    for row in rows:
        content = dict(row)
        row_content_id = int(content["id"])
        variants = _content_variants(db, row_content_id)
        exports.append(
            build_threads_export(
                content,
                variants=variants,
                scheduled_at=content.get("scheduled_at"),
                options=options,
            )
        )
    return exports


def threads_export_to_dict(export: ThreadsExport) -> dict[str, Any]:
    """Return a JSON-safe dictionary for a Threads export."""
    data = asdict(export)
    data["character_count"] = count_graphemes(export.text)
    if not export.thread_parts:
        data.pop("thread_parts")
    if export.variant is None:
        data.pop("variant")
    return data


def threads_exports_to_json(exports: list[ThreadsExport]) -> str:
    """Serialize Threads exports as stable JSON."""
    payload = [threads_export_to_dict(export) for export in exports]
    return json.dumps(payload, indent=2, sort_keys=True)


def write_threads_json(exports: list[ThreadsExport], path: str | Path) -> Path:
    """Write Threads exports to a JSON artifact."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(threads_exports_to_json(exports) + "\n", encoding="utf-8")
    return target


def _content_rows(
    db: Any,
    *,
    content_id: int | None,
    status: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    if content_id is not None:
        row = db.conn.execute(
            """SELECT gc.*, pq.scheduled_at
               FROM generated_content gc
               LEFT JOIN publish_queue pq
                 ON pq.content_id = gc.id
                AND pq.platform IN ('threads', 'all')
               WHERE gc.id = ?
               ORDER BY pq.scheduled_at IS NULL, pq.scheduled_at DESC
               LIMIT 1""",
            (content_id,),
        ).fetchone()
        return [dict(row)] if row else []

    if status:
        cursor = db.conn.execute(
            """SELECT gc.*, pq.scheduled_at
               FROM publish_queue pq
               INNER JOIN generated_content gc ON gc.id = pq.content_id
               WHERE pq.platform IN ('threads', 'all')
                 AND pq.status = ?
               ORDER BY pq.scheduled_at IS NULL, pq.scheduled_at, gc.id
               LIMIT ?""",
            (status, limit),
        )
        return [dict(row) for row in cursor.fetchall()]

    cursor = db.conn.execute(
        """SELECT gc.*, NULL AS scheduled_at
           FROM generated_content gc
           ORDER BY gc.created_at DESC, gc.id DESC
           LIMIT ?""",
        (limit,),
    )
    return [dict(row) for row in cursor.fetchall()]


def _content_variants(db: Any, content_id: int) -> list[dict[str, Any]]:
    getter = getattr(db, "list_content_variants", None)
    if callable(getter):
        return [dict(row) for row in getter(content_id)]

    cursor = db.conn.execute(
        """SELECT * FROM content_variants
           WHERE content_id = ?
           ORDER BY created_at, id""",
        (content_id,),
    )
    return [dict(row) for row in cursor.fetchall()]


def _select_threads_variant(variants: list[dict[str, Any]]) -> dict[str, Any] | None:
    threads_variants = [
        variant for variant in variants
        if str(variant.get("platform") or "").lower() == THREADS_PLATFORM
    ]
    for variant_type in ("thread", "post"):
        for variant in threads_variants:
            if str(variant.get("variant_type") or "").lower() == variant_type:
                return variant
    return threads_variants[0] if threads_variants else None


def _thread_parts(text: str, *, content_type: str, variant_type: str) -> list[str]:
    normalized = _normalize_spacing(text)
    if content_type == "x_thread" or variant_type == "thread":
        parts = parse_thread_content(normalized)
    else:
        parts = parse_thread_content(normalized)
        if len(parts) <= 1 and not re.search(r"(?m)^TWEET \d+:", normalized):
            parts = [normalized] if normalized else []
    return [_normalize_spacing(part) for part in parts if _normalize_spacing(part)]


def _provenance(content: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_commits": _json_list(content.get("source_commits")),
        "source_messages": _json_list(content.get("source_messages")),
        "source_activity_ids": _json_list(content.get("source_activity_ids")),
        "repurposed_from": content.get("repurposed_from"),
        "published_url": content.get("published_url"),
        "content_type": content.get("content_type"),
    }


def _validation_warnings(
    *,
    text: str,
    thread_parts: list[str],
    provenance: dict[str, Any],
    character_limit: int,
) -> list[str]:
    warnings: list[str] = []
    parts = thread_parts or ([text] if text else [])
    for index, part in enumerate(parts, start=1):
        count = count_graphemes(part)
        if count > character_limit:
            warnings.append(
                f"part {index} exceeds Threads character limit "
                f"({count}/{character_limit})"
            )

    if not any(
        provenance.get(key)
        for key in (
            "source_commits",
            "source_messages",
            "source_activity_ids",
            "repurposed_from",
            "published_url",
        )
    ):
        warnings.append("missing provenance")
    return warnings


def _variant_summary(variant: dict[str, Any] | None) -> dict[str, Any] | None:
    if not variant:
        return None
    return {
        "id": variant.get("id"),
        "platform": variant.get("platform"),
        "variant_type": variant.get("variant_type"),
        "created_at": variant.get("created_at"),
        "metadata": _json_object(variant.get("metadata")),
    }


def _json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _normalize_spacing(text: str) -> str:
    paragraphs = []
    for paragraph in re.split(r"\n{2,}", text.strip()):
        normalized = _WHITESPACE_RE.sub(" ", paragraph).strip()
        normalized = re.sub(r"\s+([,.;:!?])", r"\1", normalized)
        if normalized:
            paragraphs.append(normalized)
    return "\n\n".join(paragraphs)
