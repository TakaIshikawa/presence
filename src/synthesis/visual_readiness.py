"""Readiness reporting for generated visual post assets."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from synthesis.alt_text_guard import validate_alt_text


VISUAL_CONTENT_TYPES = {"x_visual", "visual"}


@dataclass(frozen=True)
class VisualAssetIssue:
    """One readiness issue for a generated visual asset."""

    code: str
    message: str
    severity: str = "error"
    source: str = "visual_readiness"

    def as_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "message": self.message,
            "severity": self.severity,
            "source": self.source,
        }


@dataclass(frozen=True)
class VisualFileMetadata:
    """Basic local file metadata for a visual asset."""

    path: str | None
    exists: bool
    is_file: bool
    size_bytes: int | None = None
    modified_at: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "exists": self.exists,
            "is_file": self.is_file,
            "size_bytes": self.size_bytes,
            "modified_at": self.modified_at,
        }


@dataclass(frozen=True)
class VisualReadinessItem:
    """Readiness result for one generated_content row."""

    content_id: int
    content_type: str
    created_at: str | None
    status: str
    ready: bool
    image_prompt: str | None
    image_alt_text: str | None
    file: VisualFileMetadata
    alt_text: dict[str, Any]
    issues: tuple[VisualAssetIssue, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "content_id": self.content_id,
            "content_type": self.content_type,
            "created_at": self.created_at,
            "status": self.status,
            "ready": self.ready,
            "image_prompt": self.image_prompt,
            "image_alt_text": self.image_alt_text,
            "file": self.file.as_dict(),
            "alt_text": self.alt_text,
            "issues": [issue.as_dict() for issue in self.issues],
        }


@dataclass(frozen=True)
class VisualReadinessReport:
    """Aggregate visual asset readiness report."""

    items: tuple[VisualReadinessItem, ...]

    @property
    def summary(self) -> dict[str, Any]:
        statuses: dict[str, int] = {}
        issue_counts: dict[str, int] = {}
        for item in self.items:
            statuses[item.status] = statuses.get(item.status, 0) + 1
            for issue in item.issues:
                issue_counts[issue.code] = issue_counts.get(issue.code, 0) + 1

        return {
            "total": len(self.items),
            "ready": statuses.get("ready", 0),
            "missing_file": statuses.get("missing_file", 0),
            "needs_alt_text": statuses.get("needs_alt_text", 0),
            "failed": statuses.get("failed", 0),
            "statuses": statuses,
            "issue_counts": issue_counts,
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "summary": self.summary,
            "items": [item.as_dict() for item in self.items],
        }


def _cutoff_timestamp(days: int) -> str:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return cutoff.strftime("%Y-%m-%d %H:%M:%S")


def _rows_for_content_id(db: Any, content_id: int) -> list[dict[str, Any]]:
    row = db.conn.execute(
        "SELECT * FROM generated_content WHERE id = ?",
        (content_id,),
    ).fetchone()
    return [dict(row)] if row else []


def _rows_for_lookback(db: Any, days: int) -> list[dict[str, Any]]:
    rows = db.conn.execute(
        """
        SELECT *
          FROM generated_content
         WHERE created_at >= ?
           AND (
                content_type IN ('x_visual', 'visual')
                OR (image_path IS NOT NULL AND TRIM(image_path) != '')
           )
         ORDER BY created_at DESC, id DESC
        """,
        (_cutoff_timestamp(days),),
    ).fetchall()
    return [dict(row) for row in rows]


def _file_metadata(image_path: str | None) -> tuple[VisualFileMetadata, list[VisualAssetIssue]]:
    if not image_path or not image_path.strip():
        return (
            VisualFileMetadata(path=image_path, exists=False, is_file=False),
            [
                VisualAssetIssue(
                    "missing_image_path",
                    "Generated visual post has no local image path.",
                )
            ],
        )

    path = Path(image_path).expanduser()
    if not path.exists():
        return (
            VisualFileMetadata(path=image_path, exists=False, is_file=False),
            [
                VisualAssetIssue(
                    "missing_image_file",
                    "Image file referenced by generated_content does not exist.",
                )
            ],
        )

    if not path.is_file():
        return (
            VisualFileMetadata(path=image_path, exists=True, is_file=False),
            [
                VisualAssetIssue(
                    "image_path_not_file",
                    "Image path exists but is not a regular file.",
                )
            ],
        )

    stat = path.stat()
    metadata = VisualFileMetadata(
        path=image_path,
        exists=True,
        is_file=True,
        size_bytes=stat.st_size,
        modified_at=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
    )
    issues: list[VisualAssetIssue] = []
    if stat.st_size <= 0:
        issues.append(
            VisualAssetIssue(
                "empty_image_file",
                "Image file exists but is empty.",
            )
        )
    return metadata, issues


def inspect_visual_asset(row: dict[str, Any]) -> VisualReadinessItem:
    """Inspect one generated_content row for visual asset readiness."""
    image_path = row.get("image_path")
    image_prompt = row.get("image_prompt")
    image_alt_text = row.get("image_alt_text")
    content_type = row.get("content_type")

    file, file_issues = _file_metadata(image_path)
    alt_validation = validate_alt_text(
        image_alt_text,
        image_prompt=image_prompt,
        image_path=image_path,
        content_type=content_type,
    )
    alt_issues = [
        VisualAssetIssue(
            issue.code,
            issue.message,
            severity=issue.severity,
            source="alt_text_guard",
        )
        for issue in alt_validation.issues
    ]
    issues = tuple(file_issues + alt_issues)

    ready = (
        file.exists
        and file.is_file
        and (file.size_bytes or 0) > 0
        and alt_validation.passed
    )
    status = _status_for(ready=ready, issues=issues)

    return VisualReadinessItem(
        content_id=int(row["id"]),
        content_type=content_type or "",
        created_at=row.get("created_at"),
        status=status,
        ready=ready,
        image_prompt=image_prompt,
        image_alt_text=image_alt_text,
        file=file,
        alt_text=alt_validation.as_dict(),
        issues=issues,
    )


def _status_for(*, ready: bool, issues: tuple[VisualAssetIssue, ...]) -> str:
    if ready:
        return "ready"

    issue_codes = {issue.code for issue in issues}
    if issue_codes.intersection(
        {"missing_image_path", "missing_image_file", "image_path_not_file", "empty_image_file"}
    ):
        return "missing_file"
    if issue_codes.intersection(
        {
            "missing_alt_text",
            "alt_text_too_short",
            "generic_alt_text",
            "alt_text_too_long",
            "file_name_leakage",
            "image_prompt_mismatch",
        }
    ):
        return "needs_alt_text"
    return "failed"


def build_visual_readiness_report(
    db: Any,
    *,
    days: int = 7,
    content_id: int | None = None,
    missing_only: bool = False,
) -> VisualReadinessReport:
    """Build a readiness report for generated visual posts."""
    rows = (
        _rows_for_content_id(db, content_id)
        if content_id is not None
        else _rows_for_lookback(db, days)
    )
    items = tuple(inspect_visual_asset(row) for row in rows)
    if missing_only:
        items = tuple(item for item in items if not item.ready)
    return VisualReadinessReport(items=items)


def format_visual_readiness_report(report: VisualReadinessReport) -> str:
    """Format a human-readable readiness report."""
    summary = report.summary
    lines = [
        "VISUAL ASSET READINESS",
        f"Total: {summary['total']}",
        f"Ready: {summary['ready']}",
        f"Missing file: {summary['missing_file']}",
        f"Needs alt text: {summary['needs_alt_text']}",
    ]

    if not report.items:
        lines.append("No visual assets matched the filters.")
        return "\n".join(lines)

    lines.append("")
    for item in report.items:
        lines.append(f"[{item.status}] generated_content #{item.content_id}")
        lines.append(f"  image: {item.file.path or 'n/a'}")
        if item.file.size_bytes is not None:
            lines.append(f"  size: {item.file.size_bytes} bytes")
        if item.issues:
            for issue in item.issues:
                lines.append(f"  - {issue.code}: {issue.message}")
        else:
            lines.append("  - ready")
    return "\n".join(lines)
