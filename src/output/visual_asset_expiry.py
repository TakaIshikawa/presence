"""Dry-run expiry planning for generated visual asset files."""

from __future__ import annotations

import json
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


DEFAULT_MINIMUM_AGE_DAYS = 30
DEFAULT_ROOT_PATH = str(Path(tempfile.gettempdir()) / "presence_images")
IMAGE_EXTENSIONS = (".avif", ".gif", ".jpeg", ".jpg", ".png", ".webp")
EXPIRY_ACTIONS = ("keep", "review", "archive")
_ACTIVE_QUEUE_STATUSES = {"held", "queued"}
_TERMINAL_STATUSES = {"cancelled", "failed"}
_VISUAL_VALUE_KEYS = {
    "artifact_path",
    "asset_path",
    "image_path",
    "media_path",
    "path",
    "visual_path",
}
_VISUAL_CONTAINER_KEYS = {
    "artifact",
    "artifacts",
    "image",
    "images",
    "media",
    "visual",
    "visual_asset",
    "visual_assets",
    "visual_artifact",
    "visual_artifacts",
}


@dataclass(frozen=True)
class VisualAssetExpiryPolicy:
    """Policy switches for read-only visual asset expiry planning."""

    minimum_age_days: int = DEFAULT_MINIMUM_AGE_DAYS
    include_unpublished: bool = False
    include_published_or_queued: bool = False


@dataclass(frozen=True)
class VisualAssetExpiryItem:
    """One asset path with a proposed dry-run expiry action."""

    action: str
    asset_path: str
    reasons: tuple[str, ...]
    source: str
    content_id: int | None = None
    content_type: str | None = None
    publication_status: str | None = None
    created_at: str | None = None
    modified_at: str | None = None
    age_days: int | None = None
    size_bytes: int | None = None
    exists: bool = True
    orphan: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reasons"] = list(self.reasons)
        return payload


@dataclass(frozen=True)
class VisualAssetExpiryReport:
    """Aggregate dry-run visual asset expiry plan."""

    root_path: str
    minimum_age_days: int
    include_unpublished: bool
    include_published_or_queued: bool
    generated_asset_count: int
    orphan_file_count: int
    missing_file_count: int
    items: tuple[VisualAssetExpiryItem, ...]
    missing_files: tuple[VisualAssetExpiryItem, ...]
    orphan_files: tuple[VisualAssetExpiryItem, ...]

    @property
    def summary(self) -> dict[str, Any]:
        actions = {action: 0 for action in EXPIRY_ACTIONS}
        for item in self.items:
            actions[item.action] = actions.get(item.action, 0) + 1
        return {
            "total": len(self.items),
            "generated_assets": self.generated_asset_count,
            "orphan_files": self.orphan_file_count,
            "missing_files": self.missing_file_count,
            "actions": actions,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "root_path": self.root_path,
            "minimum_age_days": self.minimum_age_days,
            "include_unpublished": self.include_unpublished,
            "include_published_or_queued": self.include_published_or_queued,
            "summary": self.summary,
            "items": [item.to_dict() for item in self.items],
            "missing_files": [item.to_dict() for item in self.missing_files],
            "orphan_files": [item.to_dict() for item in self.orphan_files],
        }


@dataclass(frozen=True)
class _AssetReference:
    content_id: int | None
    content_type: str | None
    asset_path: str
    source: str
    created_at: str | None
    publication_status: str


def build_visual_asset_expiry_plan(
    db: Any,
    *,
    root_path: str | Path = DEFAULT_ROOT_PATH,
    minimum_age_days: int = DEFAULT_MINIMUM_AGE_DAYS,
    include_unpublished: bool = False,
    include_published_or_queued: bool = False,
    now: datetime | None = None,
) -> VisualAssetExpiryReport:
    """Build a deterministic dry-run archive plan for local visual assets."""
    if minimum_age_days < 0:
        raise ValueError("minimum_age_days must be non-negative")

    root = Path(root_path).expanduser()
    policy = VisualAssetExpiryPolicy(
        minimum_age_days=minimum_age_days,
        include_unpublished=include_unpublished,
        include_published_or_queued=include_published_or_queued,
    )
    current_time = _ensure_utc(now or datetime.now(timezone.utc))
    conn = getattr(db, "conn", db)
    schema = _schema(conn)

    references = _asset_references(conn, schema)
    reference_items = [
        _classify_reference(reference, policy=policy, now=current_time)
        for reference in references
    ]
    referenced_paths = {
        _resolved_path(reference.asset_path)
        for reference in references
        if _is_local_path(reference.asset_path)
    }
    orphan_items = [
        _classify_orphan(path, policy=policy, now=current_time)
        for path in _orphan_paths(root, referenced_paths)
    ]

    items = tuple(
        sorted(
            reference_items + orphan_items,
            key=lambda item: (
                item.action,
                item.content_id if item.content_id is not None else -1,
                item.asset_path,
                item.source,
            ),
        )
    )
    missing_files = tuple(item for item in items if not item.exists)
    orphan_files = tuple(item for item in items if item.orphan)
    return VisualAssetExpiryReport(
        root_path=str(root),
        minimum_age_days=minimum_age_days,
        include_unpublished=include_unpublished,
        include_published_or_queued=include_published_or_queued,
        generated_asset_count=len(references),
        orphan_file_count=len(orphan_files),
        missing_file_count=len(missing_files),
        items=items,
        missing_files=missing_files,
        orphan_files=orphan_files,
    )


def format_visual_asset_expiry_json(report: VisualAssetExpiryReport) -> str:
    """Render a visual asset expiry plan as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_visual_asset_expiry_text(report: VisualAssetExpiryReport) -> str:
    """Render a stable human-readable visual asset expiry plan."""
    summary = report.summary
    lines = [
        "VISUAL ASSET EXPIRY PLAN",
        f"Root: {report.root_path}",
        f"Minimum age: {report.minimum_age_days} days",
        (
            "Policy: unpublished="
            f"{'included' if report.include_unpublished else 'protected'}, "
            "published_or_queued="
            f"{'included' if report.include_published_or_queued else 'protected'}"
        ),
        (
            f"Summary: total={summary['total']} keep={summary['actions'].get('keep', 0)} "
            f"review={summary['actions'].get('review', 0)} "
            f"archive={summary['actions'].get('archive', 0)} "
            f"missing={summary['missing_files']} orphans={summary['orphan_files']}"
        ),
    ]
    if not report.items:
        lines.append("No local visual asset files or references found.")
        return "\n".join(lines)

    for title, items in (
        ("Archive candidates", [item for item in report.items if item.action == "archive"]),
        ("Review separately", [item for item in report.items if item.action == "review"]),
        ("Kept", [item for item in report.items if item.action == "keep"]),
    ):
        lines.append("")
        lines.append(f"{title}:")
        if not items:
            lines.append("  none")
            continue
        for item in items:
            label = "orphan" if item.orphan else f"generated_content #{item.content_id}"
            status = item.publication_status or "n/a"
            lines.append(f"  [{item.action}] {label} {item.asset_path}")
            lines.append(f"    status={status} age_days={item.age_days if item.age_days is not None else 'n/a'}")
            for reason in item.reasons:
                lines.append(f"    - {reason}")
    return "\n".join(lines)


def _asset_references(conn: Any, schema: dict[str, set[str]]) -> list[_AssetReference]:
    if "generated_content" not in schema:
        return []
    states = _content_states(conn, schema)
    rows = _generated_content_references(conn, schema, states)
    rows.extend(_variant_metadata_references(conn, schema, states))
    return _dedupe_references(rows)


def _generated_content_references(
    conn: Any,
    schema: dict[str, set[str]],
    states: dict[int, str],
) -> list[_AssetReference]:
    columns = schema["generated_content"]
    if "image_path" not in columns:
        return []
    rows = conn.execute(
        f"""SELECT gc.id,
                  {_value_expr(columns, "content_type", "gc")} AS content_type,
                  gc.image_path,
                  {_value_expr(columns, "created_at", "gc")} AS created_at,
                  {_value_expr(columns, "published", "gc")} AS published
            FROM generated_content gc
            WHERE gc.image_path IS NOT NULL
              AND TRIM(gc.image_path) != ''
            ORDER BY gc.id"""
    ).fetchall()
    references: list[_AssetReference] = []
    for row in rows:
        data = dict(row)
        content_id = int(data["id"])
        references.append(
            _AssetReference(
                content_id=content_id,
                content_type=data.get("content_type"),
                asset_path=str(data["image_path"]).strip(),
                source="generated_content.image_path",
                created_at=data.get("created_at"),
                publication_status=states.get(
                    content_id,
                    "published" if int(data.get("published") or 0) == 1 else "unpublished",
                ),
            )
        )
    return references


def _variant_metadata_references(
    conn: Any,
    schema: dict[str, set[str]],
    states: dict[int, str],
) -> list[_AssetReference]:
    if "content_variants" not in schema:
        return []
    columns = schema["content_variants"]
    if not {"content_id", "metadata"}.issubset(columns):
        return []
    gc_columns = schema.get("generated_content", set())
    rows = conn.execute(
        f"""SELECT cv.content_id,
                  cv.metadata,
                  {_value_expr(columns, "created_at", "cv")} AS variant_created_at,
                  {_value_expr(gc_columns, "content_type", "gc")} AS content_type,
                  {_value_expr(gc_columns, "created_at", "gc")} AS generated_created_at,
                  {_value_expr(gc_columns, "published", "gc")} AS published
            FROM content_variants cv
            LEFT JOIN generated_content gc ON gc.id = cv.content_id
            ORDER BY cv.id"""
    ).fetchall()
    references: list[_AssetReference] = []
    for row in rows:
        data = dict(row)
        metadata = _decode_json_object(data.get("metadata"))
        if metadata is None:
            continue
        content_id = _int_or_none(data.get("content_id"))
        for asset in _extract_visual_assets(metadata):
            references.append(
                _AssetReference(
                    content_id=content_id,
                    content_type=data.get("content_type"),
                    asset_path=asset["asset_path"],
                    source=f"content_variants.metadata.{asset['source']}",
                    created_at=data.get("variant_created_at") or data.get("generated_created_at"),
                    publication_status=states.get(
                        content_id or -1,
                        "published" if int(data.get("published") or 0) == 1 else "unpublished",
                    ),
                )
            )
    return references


def _classify_reference(
    reference: _AssetReference,
    *,
    policy: VisualAssetExpiryPolicy,
    now: datetime,
) -> VisualAssetExpiryItem:
    if _is_url(reference.asset_path):
        return VisualAssetExpiryItem(
            action="keep",
            asset_path=reference.asset_path,
            reasons=("remote asset URL; no local file to archive",),
            source=reference.source,
            content_id=reference.content_id,
            content_type=reference.content_type,
            publication_status=reference.publication_status,
            created_at=reference.created_at,
        )

    path = Path(reference.asset_path).expanduser()
    file_state = _file_state(path, now)
    base = {
        "asset_path": reference.asset_path,
        "source": reference.source,
        "content_id": reference.content_id,
        "content_type": reference.content_type,
        "publication_status": reference.publication_status,
        "created_at": reference.created_at,
        **file_state,
    }
    if not file_state["exists"]:
        return VisualAssetExpiryItem(
            action="review",
            reasons=("referenced asset file is missing; report separately before changing records",),
            **base,
        )
    if _is_protected_status(reference.publication_status) and not policy.include_published_or_queued:
        return VisualAssetExpiryItem(
            action="keep",
            reasons=(f"{reference.publication_status} assets are protected by policy",),
            **base,
        )
    if reference.publication_status == "unpublished" and not policy.include_unpublished:
        return VisualAssetExpiryItem(
            action="keep",
            reasons=("unpublished generated assets are protected unless --include-unpublished is used",),
            **base,
        )
    if file_state["age_days"] is not None and file_state["age_days"] < policy.minimum_age_days:
        return VisualAssetExpiryItem(
            action="keep",
            reasons=(f"asset is newer than minimum age of {policy.minimum_age_days} days",),
            **base,
        )
    if reference.publication_status in _TERMINAL_STATUSES:
        reason = f"{reference.publication_status} asset is older than minimum age"
    elif reference.publication_status == "unpublished":
        reason = "unpublished asset is older than minimum age and included by policy"
    elif policy.include_published_or_queued:
        reason = f"{reference.publication_status} asset is explicitly included by policy"
    else:
        reason = "asset is older than minimum age"
    return VisualAssetExpiryItem(action="archive", reasons=(reason,), **base)


def _classify_orphan(
    path: Path,
    *,
    policy: VisualAssetExpiryPolicy,
    now: datetime,
) -> VisualAssetExpiryItem:
    file_state = _file_state(path, now)
    age_days = file_state["age_days"]
    if age_days is not None and age_days >= policy.minimum_age_days:
        action = "archive"
        reasons = ("orphaned local file is older than minimum age",)
    else:
        action = "review"
        reasons = ("orphaned local file is newer than minimum age",)
    return VisualAssetExpiryItem(
        action=action,
        asset_path=str(path),
        source="root_path.orphan_scan",
        publication_status="orphaned",
        orphan=True,
        reasons=reasons,
        **file_state,
    )


def _file_state(path: Path, now: datetime) -> dict[str, Any]:
    try:
        stat = path.stat()
    except OSError:
        return {
            "exists": False,
            "size_bytes": None,
            "modified_at": None,
            "age_days": None,
        }
    modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
    return {
        "exists": path.is_file(),
        "size_bytes": stat.st_size if path.is_file() else None,
        "modified_at": modified.isoformat(),
        "age_days": max(0, int((now - modified).total_seconds() // 86400)),
    }


def _orphan_paths(root: Path, referenced_paths: set[Path]) -> list[Path]:
    if not root.exists() or not root.is_dir():
        return []
    paths = [
        path
        for path in root.rglob("*")
        if path.is_file()
        and path.suffix.lower() in IMAGE_EXTENSIONS
        and _resolved_path(str(path)) not in referenced_paths
    ]
    return sorted(paths, key=lambda item: str(item))


def _content_states(conn: Any, schema: dict[str, set[str]]) -> dict[int, str]:
    ranked: dict[int, str] = {}
    if "content_publications" in schema:
        columns = schema["content_publications"]
        if {"content_id", "status"}.issubset(columns):
            for row in conn.execute(
                "SELECT content_id, status FROM content_publications ORDER BY content_id, id"
            ).fetchall():
                _merge_state(ranked, int(row["content_id"]), _publication_status(row["status"]))
    if "publish_queue" in schema:
        columns = schema["publish_queue"]
        if {"content_id", "status"}.issubset(columns):
            for row in conn.execute(
                "SELECT content_id, status FROM publish_queue ORDER BY content_id, id"
            ).fetchall():
                status = _queue_status(row["status"])
                if status:
                    _merge_state(ranked, int(row["content_id"]), status)
    return ranked


def _merge_state(states: dict[int, str], content_id: int, candidate: str) -> None:
    rank = {
        "unpublished": 0,
        "cancelled": 1,
        "failed": 1,
        "queued": 2,
        "held": 2,
        "published": 3,
    }
    current = states.get(content_id)
    if current is None or rank.get(candidate, 0) > rank.get(current, 0):
        states[content_id] = candidate


def _publication_status(value: Any) -> str:
    raw = str(value or "").lower()
    if raw == "published":
        return "published"
    if raw in _ACTIVE_QUEUE_STATUSES:
        return "queued"
    if raw in _TERMINAL_STATUSES:
        return raw
    return "unpublished"


def _queue_status(value: Any) -> str | None:
    raw = str(value or "").lower()
    if raw == "published":
        return "published"
    if raw in _ACTIVE_QUEUE_STATUSES:
        return "queued"
    if raw in _TERMINAL_STATUSES:
        return raw
    return None


def _is_protected_status(status: str | None) -> bool:
    return status in {"held", "published", "queued"}


def _extract_visual_assets(value: Any, *, path: str = "", visual_context: bool = False) -> list[dict[str, str]]:
    found: list[dict[str, str]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            key_text = str(key)
            child_path = f"{path}.{key_text}" if path else key_text
            is_visual_container = key_text in _VISUAL_CONTAINER_KEYS
            if isinstance(item, str) and (visual_context or key_text in _VISUAL_VALUE_KEYS):
                text = item.strip()
                if text and _is_local_path(text):
                    found.append({"asset_path": text, "source": child_path})
            elif isinstance(item, (dict, list)):
                found.extend(
                    _extract_visual_assets(
                        item,
                        path=child_path,
                        visual_context=visual_context or is_visual_container,
                    )
                )
    elif isinstance(value, list):
        for index, item in enumerate(value):
            child_path = f"{path}[{index}]" if path else f"[{index}]"
            found.extend(
                _extract_visual_assets(
                    item,
                    path=child_path,
                    visual_context=visual_context,
                )
            )
    return found


def _schema(conn: Any) -> dict[str, set[str]]:
    tables = {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        for table in tables
    }


def _dedupe_references(references: list[_AssetReference]) -> list[_AssetReference]:
    deduped: dict[tuple[Any, ...], _AssetReference] = {}
    for reference in references:
        key = (reference.content_id, reference.asset_path, reference.source)
        deduped[key] = reference
    return list(deduped.values())


def _decode_json_object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if not value or not isinstance(value, str):
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _is_local_path(value: str) -> bool:
    return not _is_url(value)


def _is_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme not in {"", "file"} and not (
        len(parsed.scheme) == 1 and value[1:3] in {":/", ":\\"}
    )


def _resolved_path(value: str) -> Path:
    return Path(value).expanduser().resolve(strict=False)


def _value_expr(columns: set[str], column: str, alias: str) -> str:
    if column in columns:
        return f"{alias}.{column}"
    return "NULL"


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
