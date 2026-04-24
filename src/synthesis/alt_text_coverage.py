"""Coverage audit for generated visual post alt text."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from synthesis.alt_text_guard import validate_alt_text


DEFAULT_MIN_ALT_TEXT_LENGTH = 20


@dataclass(frozen=True)
class AltTextCoverageItem:
    """One generated visual content row and its audit status."""

    content_id: int | str | None
    image_path: str | None
    content_type: str | None
    created_at: str | None
    status: str
    issue_codes: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "content_id": self.content_id,
            "image_path": self.image_path,
            "content_type": self.content_type,
            "created_at": self.created_at,
            "status": self.status,
            "issue_codes": list(self.issue_codes),
        }


@dataclass(frozen=True)
class AltTextCoverageReport:
    """Aggregate alt-text coverage audit results."""

    total: int
    ok: int
    missing: int
    too_short: int
    duplicate_content: int
    low_quality: int
    items: tuple[AltTextCoverageItem, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "totals": {
                "total": self.total,
                "ok": self.ok,
                "missing": self.missing,
                "too_short": self.too_short,
                "duplicate_content": self.duplicate_content,
                "low_quality": self.low_quality,
            },
            "items": [item.as_dict() for item in self.items],
        }


def _get(row: Mapping[str, Any] | Any, key: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return getattr(row, key, None)


def _normalize_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _issue_status(issue_codes: tuple[str, ...]) -> str:
    if "missing_alt_text" in issue_codes:
        return "missing"
    if "alt_text_too_short" in issue_codes:
        return "too_short"
    if "duplicate_content" in issue_codes:
        return "duplicate_content"
    if issue_codes:
        return "low_quality"
    return "ok"


def audit_alt_text_coverage(
    rows: Iterable[Mapping[str, Any] | Any],
    *,
    min_length: int = DEFAULT_MIN_ALT_TEXT_LENGTH,
    include_ok: bool = False,
) -> AltTextCoverageReport:
    """Audit generated visual content rows for image alt-text coverage.

    The input rows are expected to expose the generated_content fields used by
    the report: id/content_id, content, image_path, image_prompt, image_alt_text,
    content_type, and created_at.
    """
    row_list = list(rows)
    normalized_alt_counts = Counter(
        _normalize_text(_get(row, "image_alt_text"))
        for row in row_list
        if _normalize_text(_get(row, "image_alt_text"))
    )

    items: list[AltTextCoverageItem] = []
    status_counts = Counter()
    for row in row_list:
        content_id = _get(row, "content_id")
        if content_id is None:
            content_id = _get(row, "id")
        image_path = _get(row, "image_path")
        content_type = _get(row, "content_type")
        created_at = _get(row, "created_at")
        alt_text = _get(row, "image_alt_text")
        normalized_alt = _normalize_text(alt_text)
        normalized_content = _normalize_text(_get(row, "content"))

        validation = validate_alt_text(
            alt_text,
            image_prompt=_get(row, "image_prompt"),
            image_path=image_path,
            content_type=content_type,
            min_chars=min_length,
            min_words=1,
        )
        issue_codes = [issue.code for issue in validation.issues]

        if normalized_alt and (
            normalized_alt == normalized_content
            or normalized_alt_counts[normalized_alt] > 1
        ):
            issue_codes.append("duplicate_content")

        issue_tuple = tuple(dict.fromkeys(issue_codes))
        status = _issue_status(issue_tuple)
        status_counts[status] += 1

        if include_ok or status != "ok":
            items.append(
                AltTextCoverageItem(
                    content_id=content_id,
                    image_path=image_path,
                    content_type=content_type,
                    created_at=created_at,
                    status=status,
                    issue_codes=issue_tuple,
                )
            )

    return AltTextCoverageReport(
        total=len(row_list),
        ok=status_counts["ok"],
        missing=status_counts["missing"],
        too_short=status_counts["too_short"],
        duplicate_content=status_counts["duplicate_content"],
        low_quality=status_counts["low_quality"],
        items=tuple(items),
    )
