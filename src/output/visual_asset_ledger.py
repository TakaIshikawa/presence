"""Build a usage ledger for generated visual assets."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


DEFAULT_DAYS = 30
LEDGER_STATUSES = ("published", "queued", "draft_only", "orphaned")
_ACTIVE_QUEUE_STATUSES = {"queued", "held"}
_VISUAL_VALUE_KEYS = {
    "artifact_path",
    "asset_path",
    "image_path",
    "image_url",
    "media_path",
    "path",
    "url",
    "visual_path",
    "visual_url",
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
class VisualAssetLedgerRow:
    """One visual asset reference and its effective publication state."""

    status: str
    platform: str | None
    content_id: int | None
    content_type: str | None
    artifact: str
    artifact_kind: str
    source: str
    created_at: str | None
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_visual_asset_ledger(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    status: str | None = None,
    missing_only: bool = False,
    now: datetime | None = None,
) -> list[VisualAssetLedgerRow]:
    """Return visual asset references from generated content and variant metadata."""
    if days <= 0:
        raise ValueError("days must be positive")
    if status is not None and status not in LEDGER_STATUSES:
        raise ValueError(f"status must be one of: {', '.join(LEDGER_STATUSES)}")

    conn = getattr(db, "conn", db)
    schema = _schema(conn)
    if "generated_content" not in schema:
        return []

    cutoff = _to_iso(_ensure_utc(now or datetime.now(timezone.utc)) - timedelta(days=days))
    rows: list[VisualAssetLedgerRow] = []
    rows.extend(_generated_content_rows(conn, schema, cutoff))
    rows.extend(_variant_metadata_rows(conn, schema, cutoff))

    rows = _dedupe_rows(rows)
    if status is not None:
        rows = [row for row in rows if row.status == status]
    if missing_only:
        rows = [row for row in rows if row.warnings]
    rows.sort(
        key=lambda row: (
            row.created_at or "",
            row.content_id or 0,
            row.platform or "",
            row.artifact,
        ),
        reverse=True,
    )
    return rows


def format_visual_asset_ledger_json(rows: list[VisualAssetLedgerRow]) -> str:
    """Render visual asset ledger rows as deterministic JSON."""
    return json.dumps([row.to_dict() for row in rows], indent=2, sort_keys=True)


def format_visual_asset_ledger_table(rows: list[VisualAssetLedgerRow], *, days: int) -> str:
    """Render visual asset ledger rows as a compact table."""
    lines = [
        f"Visual Asset Ledger (last {days} days)",
        f"assets={len(rows)}",
        "",
    ]
    if not rows:
        lines.append("No visual assets found.")
        return "\n".join(lines)

    columns = [
        ("status", "STATUS", 10),
        ("platform", "PLATFORM", 9),
        ("content_id", "CID", 5),
        ("created_at", "CREATED", 19),
        ("kind", "KIND", 4),
        ("source", "SOURCE", 18),
        ("artifact", "ARTIFACT", 44),
        ("warnings", "WARNINGS", 32),
    ]
    lines.append("  ".join(label.ljust(width) for _, label, width in columns))
    lines.append("  ".join("-" * width for _, _, width in columns))
    for row in rows:
        data = {
            "status": row.status,
            "platform": row.platform or "-",
            "content_id": row.content_id if row.content_id is not None else "-",
            "created_at": row.created_at or "-",
            "kind": row.artifact_kind,
            "source": row.source,
            "artifact": row.artifact,
            "warnings": "; ".join(row.warnings) if row.warnings else "-",
        }
        lines.append(
            "  ".join(
                _clip(data[key], width).ljust(width)
                for key, _, width in columns
            )
        )
    return "\n".join(lines)


def _generated_content_rows(
    conn: Any,
    schema: dict[str, set[str]],
    cutoff: str,
) -> list[VisualAssetLedgerRow]:
    gc_columns = schema["generated_content"]
    if "image_path" not in gc_columns:
        return []

    select_columns = [
        "gc.id",
        _column_expr(gc_columns, "content_type"),
        _column_expr(gc_columns, "image_path"),
        _column_expr(gc_columns, "created_at"),
        _column_expr(gc_columns, "published"),
        _column_expr(gc_columns, "published_at"),
    ]
    where = "gc.created_at >= ?" if "created_at" in gc_columns else "1 = 1"
    params: list[Any] = [cutoff] if "created_at" in gc_columns else []

    rows = conn.execute(
        f"""SELECT {", ".join(select_columns)}
            FROM generated_content gc
            WHERE {where}
              AND gc.image_path IS NOT NULL
              AND TRIM(gc.image_path) != ''
            ORDER BY gc.created_at DESC, gc.id DESC""",
        tuple(params),
    ).fetchall()

    states = _content_states(conn, schema)
    ledger_rows: list[VisualAssetLedgerRow] = []
    for row in rows:
        data = dict(row)
        content_id = int(data["id"])
        ledger_rows.extend(
            _rows_for_asset(
                content_id=content_id,
                content_type=data.get("content_type"),
                artifact=str(data["image_path"]).strip(),
                source="generated_content.image_path",
                created_at=data.get("created_at"),
                states=states.get(content_id, {}),
                legacy_published=bool(int(data.get("published") or 0) == 1),
            )
        )
    return ledger_rows


def _variant_metadata_rows(
    conn: Any,
    schema: dict[str, set[str]],
    cutoff: str,
) -> list[VisualAssetLedgerRow]:
    if "content_variants" not in schema:
        return []
    variant_columns = schema["content_variants"]
    required = {"content_id", "metadata"}
    if not required.issubset(variant_columns):
        return []

    gc_columns = schema.get("generated_content", set())
    where = "COALESCE(cv.created_at, gc.created_at) >= ?"
    params: list[Any] = [cutoff]
    rows = conn.execute(
        f"""SELECT cv.id AS variant_id,
                  cv.content_id,
                  {_value_expr(variant_columns, "platform", "cv")} AS variant_platform,
                  {_value_expr(variant_columns, "metadata", "cv")} AS metadata,
                  COALESCE({_value_expr(variant_columns, "created_at", "cv")},
                           {_value_expr(gc_columns, "created_at", "gc")}) AS created_at,
                  {_column_expr(gc_columns, "content_type", "gc")} AS content_type,
                  {_column_expr(gc_columns, "published", "gc")} AS published
            FROM content_variants cv
            LEFT JOIN generated_content gc ON gc.id = cv.content_id
            WHERE {where}
            ORDER BY created_at DESC, cv.id DESC""",
        tuple(params),
    ).fetchall()

    states = _content_states(conn, schema)
    ledger_rows: list[VisualAssetLedgerRow] = []
    for row in rows:
        data = dict(row)
        metadata = _decode_json_object(data.get("metadata"))
        if metadata is None:
            continue
        content_id = _int_or_none(data.get("content_id"))
        extracted = _extract_visual_assets(metadata)
        for asset in extracted:
            source = f"content_variants.metadata.{asset['source']}"
            platform = data.get("variant_platform")
            platform_states = states.get(content_id or -1, {})
            if platform:
                platform_states = {
                    str(platform): platform_states.get(str(platform), "draft_only")
                }
            ledger_rows.extend(
                _rows_for_asset(
                    content_id=content_id,
                    content_type=data.get("content_type"),
                    artifact=asset["artifact"],
                    source=source,
                    created_at=data.get("created_at"),
                    states=platform_states,
                    legacy_published=bool(int(data.get("published") or 0) == 1),
                )
            )
    return ledger_rows


def _rows_for_asset(
    *,
    content_id: int | None,
    content_type: str | None,
    artifact: str,
    source: str,
    created_at: str | None,
    states: dict[str, str],
    legacy_published: bool,
) -> list[VisualAssetLedgerRow]:
    artifact = artifact.strip()
    if not artifact:
        return []
    artifact_kind = "url" if _is_url(artifact) else "path"
    warnings = tuple(_missing_file_warnings(artifact))
    if content_id is None:
        return [
            VisualAssetLedgerRow(
                status="orphaned",
                platform=None,
                content_id=None,
                content_type=content_type,
                artifact=artifact,
                artifact_kind=artifact_kind,
                source=source,
                created_at=created_at,
                warnings=warnings,
            )
        ]

    if not states:
        states = {"all": "published" if legacy_published else "draft_only"}

    return [
        VisualAssetLedgerRow(
            status=_normalize_status(state),
            platform=platform,
            content_id=content_id,
            content_type=content_type,
            artifact=artifact,
            artifact_kind=artifact_kind,
            source=source,
            created_at=created_at,
            warnings=warnings,
        )
        for platform, state in sorted(states.items())
    ]


def _content_states(conn: Any, schema: dict[str, set[str]]) -> dict[int, dict[str, str]]:
    states: dict[int, dict[str, str]] = {}

    if "content_publications" in schema:
        columns = schema["content_publications"]
        if {"content_id", "platform", "status"}.issubset(columns):
            for row in conn.execute(
                """SELECT content_id, platform, status
                   FROM content_publications
                   ORDER BY content_id, platform, id"""
            ).fetchall():
                content_id = int(row["content_id"])
                platform = str(row["platform"] or "all")
                status = _publication_status(row["status"])
                states.setdefault(content_id, {})[platform] = _merge_status(
                    states.get(content_id, {}).get(platform),
                    status,
                )

    if "publish_queue" in schema:
        columns = schema["publish_queue"]
        if {"content_id", "platform", "status"}.issubset(columns):
            for row in conn.execute(
                """SELECT content_id, platform, status
                   FROM publish_queue
                   ORDER BY content_id, platform, id"""
            ).fetchall():
                status = _queue_status(row["status"])
                if status is None:
                    continue
                content_id = int(row["content_id"])
                platform = str(row["platform"] or "all")
                states.setdefault(content_id, {})[platform] = _merge_status(
                    states.get(content_id, {}).get(platform),
                    status,
                )
    return states


def _publication_status(value: Any) -> str:
    return "published" if str(value or "").lower() == "published" else "draft_only"


def _queue_status(value: Any) -> str | None:
    raw = str(value or "").lower()
    if raw == "published":
        return "published"
    if raw in _ACTIVE_QUEUE_STATUSES:
        return "queued"
    return None


def _merge_status(current: str | None, candidate: str) -> str:
    rank = {"draft_only": 0, "queued": 1, "published": 2, "orphaned": -1}
    if current is None or rank[candidate] > rank[current]:
        return candidate
    return current


def _normalize_status(value: str) -> str:
    return value if value in LEDGER_STATUSES else "draft_only"


def _extract_visual_assets(value: Any, *, path: str = "", visual_context: bool = False) -> list[dict[str, str]]:
    found: list[dict[str, str]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            key_text = str(key)
            child_path = f"{path}.{key_text}" if path else key_text
            is_visual_container = key_text in _VISUAL_CONTAINER_KEYS
            if isinstance(item, str) and (visual_context or key_text in _VISUAL_VALUE_KEYS):
                text = item.strip()
                if text:
                    found.append({"artifact": text, "source": child_path})
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


def _missing_file_warnings(artifact: str) -> list[str]:
    if not _is_filesystem_path(artifact):
        return []
    path = Path(artifact).expanduser()
    if not path.exists():
        return [f"missing_file: {artifact}"]
    if not path.is_file():
        return [f"not_file: {artifact}"]
    return []


def _is_filesystem_path(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme == "" or (len(parsed.scheme) == 1 and value[1:3] in {":/", ":\\"})


def _is_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme not in {"", "file"} and not (
        len(parsed.scheme) == 1 and value[1:3] in {":/", ":\\"}
    )


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


def _dedupe_rows(rows: list[VisualAssetLedgerRow]) -> list[VisualAssetLedgerRow]:
    deduped: dict[tuple[Any, ...], VisualAssetLedgerRow] = {}
    for row in rows:
        key = (row.content_id, row.platform, row.artifact, row.source)
        deduped[key] = row
    return list(deduped.values())


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


def _column_expr(columns: set[str], column: str, alias: str = "gc") -> str:
    if column in columns:
        return f"{alias}.{column}"
    return f"NULL AS {column}"


def _value_expr(columns: set[str], column: str, alias: str) -> str:
    if column in columns:
        return f"{alias}.{column}"
    return "NULL"


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _to_iso(value: datetime) -> str:
    return value.isoformat()


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clip(value: Any, width: int) -> str:
    if value is None:
        text = "-"
    else:
        text = str(value).replace("\n", " ")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)].rstrip() + "..."
