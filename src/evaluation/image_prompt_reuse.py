"""Report repeated and near-repeated generated image prompts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
import csv
import io
import json
import re
import sqlite3
import string
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_REUSE = 2
DEFAULT_SIMILARITY_THRESHOLD = 0.82

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_CSV_FIELDS = (
    "bucket_id",
    "bucket_type",
    "similarity_bucket",
    "reuse_count",
    "min_similarity",
    "max_similarity",
    "content_id",
    "content_type",
    "image_path",
    "has_image_alt_text",
    "created_at",
    "normalized_prompt",
)


@dataclass(frozen=True)
class ImagePromptReuseItem:
    """One generated content row participating in a reuse finding."""

    content_id: int
    content_type: str
    image_path: str | None
    has_image_alt_text: bool
    created_at: str | None
    normalized_prompt: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ImagePromptReuseFinding:
    """A deterministic exact or near image prompt reuse bucket."""

    bucket_id: str
    bucket_type: str
    similarity_bucket: str
    reuse_count: int
    min_similarity: float
    max_similarity: float
    normalized_prompt: str | None
    items: tuple[ImagePromptReuseItem, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["items"] = [item.to_dict() for item in self.items]
        return payload


@dataclass(frozen=True)
class ImagePromptReuseReport:
    """Image prompt reuse report with filters and schema metadata."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    findings: tuple[ImagePromptReuseFinding, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "image_prompt_reuse",
            "filters": dict(self.filters),
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_image_prompt_reuse_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_reuse: int = DEFAULT_MIN_REUSE,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    now: datetime | None = None,
) -> ImagePromptReuseReport:
    """Return exact and near image prompt reuse findings for generated content."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_reuse <= 1:
        raise ValueError("min_reuse must be greater than 1")
    if not 0 <= similarity_threshold <= 1:
        raise ValueError("similarity_threshold must be between 0 and 1")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "cutoff": cutoff.isoformat(),
        "min_reuse": min_reuse,
        "similarity_threshold": similarity_threshold,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or _missing_required_columns(missing_columns):
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_rows(conn, schema=schema, cutoff=cutoff, now=generated_at)
    exact_findings = _exact_findings(rows, min_reuse=min_reuse)
    near_findings = _near_findings(
        rows,
        min_reuse=min_reuse,
        similarity_threshold=similarity_threshold,
    )
    findings = tuple(_assign_bucket_ids([*exact_findings, *near_findings]))
    return ImagePromptReuseReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "scanned_prompts": len(rows),
            "finding_count": len(findings),
            "exact_buckets": sum(
                1 for finding in findings if finding.bucket_type == "exact"
            ),
            "near_buckets": sum(
                1 for finding in findings if finding.bucket_type == "near"
            ),
        },
        findings=findings,
        missing_tables=(),
        missing_columns=missing_columns,
    )


def format_image_prompt_reuse_json(report: ImagePromptReuseReport) -> str:
    """Serialize the image prompt reuse report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_image_prompt_reuse_csv(report: ImagePromptReuseReport) -> str:
    """Render the image prompt reuse report as one CSV row per content item."""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=_CSV_FIELDS, lineterminator="\n")
    writer.writeheader()
    for row in _flat_rows(report):
        writer.writerow(row)
    return output.getvalue().rstrip("\n")


def normalize_image_prompt(prompt: str | None) -> str:
    """Normalize an image prompt for exact matching and stable similarity."""
    if prompt is None:
        return ""
    text = prompt.casefold()
    text = text.translate(str.maketrans({char: " " for char in string.punctuation}))
    return " ".join(_TOKEN_RE.findall(text))


def sequence_similarity(left: str | None, right: str | None) -> float:
    """Return SequenceMatcher similarity for normalized prompt strings."""
    normalized_left = normalize_image_prompt(left)
    normalized_right = normalize_image_prompt(right)
    if not normalized_left or not normalized_right:
        return 0.0
    return SequenceMatcher(None, normalized_left, normalized_right, autojunk=False).ratio()


def _load_rows(
    conn: sqlite3.Connection,
    *,
    schema: dict[str, set[str]],
    cutoff: datetime,
    now: datetime,
) -> list[ImagePromptReuseItem]:
    columns = schema["generated_content"]
    content_type_expr = _column_expr(columns, "content_type", "'unknown'", alias="gc")
    image_path_expr = _column_expr(columns, "image_path", "NULL", alias="gc")
    image_alt_text_expr = _column_expr(columns, "image_alt_text", "NULL", alias="gc")
    created_at_expr = _column_expr(columns, "created_at", "NULL", alias="gc")
    rows = conn.execute(
        f"""SELECT
               gc.id AS content_id,
               {content_type_expr} AS content_type,
               {image_path_expr} AS image_path,
               gc.image_prompt AS image_prompt,
               {image_alt_text_expr} AS image_alt_text,
               {created_at_expr} AS created_at
           FROM generated_content gc
           WHERE gc.image_prompt IS NOT NULL
             AND TRIM(gc.image_prompt) != ''
             AND datetime({created_at_expr}) >= datetime(?)
             AND datetime({created_at_expr}) <= datetime(?)
           ORDER BY datetime({created_at_expr}) ASC, gc.id ASC""",
        (cutoff.isoformat(), now.isoformat()),
    ).fetchall()

    items: list[ImagePromptReuseItem] = []
    for row in rows:
        normalized = normalize_image_prompt(row["image_prompt"])
        if not normalized:
            continue
        items.append(
            ImagePromptReuseItem(
                content_id=int(row["content_id"]),
                content_type=str(row["content_type"] or "unknown"),
                image_path=row["image_path"],
                has_image_alt_text=bool(str(row["image_alt_text"] or "").strip()),
                created_at=row["created_at"],
                normalized_prompt=normalized,
            )
        )
    return items


def _exact_findings(
    rows: list[ImagePromptReuseItem],
    *,
    min_reuse: int,
) -> list[ImagePromptReuseFinding]:
    groups = _prompt_groups(rows)
    findings: list[ImagePromptReuseFinding] = []
    for normalized_prompt, items in groups.items():
        if len(items) < min_reuse:
            continue
        findings.append(
            ImagePromptReuseFinding(
                bucket_id="",
                bucket_type="exact",
                similarity_bucket="exact",
                reuse_count=len(items),
                min_similarity=1.0,
                max_similarity=1.0,
                normalized_prompt=normalized_prompt,
                items=tuple(items),
            )
        )
    return sorted(findings, key=_finding_sort_key)


def _near_findings(
    rows: list[ImagePromptReuseItem],
    *,
    min_reuse: int,
    similarity_threshold: float,
) -> list[ImagePromptReuseFinding]:
    groups = _prompt_groups(rows)
    prompts = sorted(groups)
    if len(prompts) < 2:
        return []

    parent = list(range(len(prompts)))
    scores: dict[tuple[int, int], float] = {}
    for left_index, left_prompt in enumerate(prompts):
        for right_index in range(left_index + 1, len(prompts)):
            right_prompt = prompts[right_index]
            score = sequence_similarity(left_prompt, right_prompt)
            if score >= similarity_threshold:
                _union(parent, left_index, right_index)
                scores[(left_index, right_index)] = score

    components: dict[int, list[int]] = {}
    for index in range(len(prompts)):
        components.setdefault(_find(parent, index), []).append(index)

    findings: list[ImagePromptReuseFinding] = []
    for indexes in components.values():
        if len(indexes) < 2:
            continue
        component_scores = [
            score
            for (left_index, right_index), score in scores.items()
            if left_index in indexes and right_index in indexes
        ]
        if not component_scores:
            continue
        items = [
            item
            for index in indexes
            for item in groups[prompts[index]]
        ]
        items.sort(key=_item_sort_key)
        if len(items) < min_reuse:
            continue
        findings.append(
            ImagePromptReuseFinding(
                bucket_id="",
                bucket_type="near",
                similarity_bucket=f">={similarity_threshold:.2f}",
                reuse_count=len(items),
                min_similarity=round(min(component_scores), 4),
                max_similarity=round(max(component_scores), 4),
                normalized_prompt=None,
                items=tuple(items),
            )
        )
    return sorted(findings, key=_finding_sort_key)


def _assign_bucket_ids(
    findings: list[ImagePromptReuseFinding],
) -> list[ImagePromptReuseFinding]:
    sorted_findings = sorted(findings, key=_finding_sort_key)
    assigned: list[ImagePromptReuseFinding] = []
    for index, finding in enumerate(sorted_findings, start=1):
        assigned.append(
            ImagePromptReuseFinding(
                bucket_id=f"{finding.bucket_type}_{index:03d}",
                bucket_type=finding.bucket_type,
                similarity_bucket=finding.similarity_bucket,
                reuse_count=finding.reuse_count,
                min_similarity=finding.min_similarity,
                max_similarity=finding.max_similarity,
                normalized_prompt=finding.normalized_prompt,
                items=finding.items,
            )
        )
    return assigned


def _flat_rows(report: ImagePromptReuseReport) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for finding in report.findings:
        for item in finding.items:
            rows.append(
                {
                    "bucket_id": finding.bucket_id,
                    "bucket_type": finding.bucket_type,
                    "similarity_bucket": finding.similarity_bucket,
                    "reuse_count": finding.reuse_count,
                    "min_similarity": f"{finding.min_similarity:.4f}",
                    "max_similarity": f"{finding.max_similarity:.4f}",
                    "content_id": item.content_id,
                    "content_type": item.content_type,
                    "image_path": item.image_path or "",
                    "has_image_alt_text": str(item.has_image_alt_text).lower(),
                    "created_at": item.created_at or "",
                    "normalized_prompt": item.normalized_prompt,
                }
            )
    return rows


def _prompt_groups(
    rows: list[ImagePromptReuseItem],
) -> dict[str, list[ImagePromptReuseItem]]:
    groups: dict[str, list[ImagePromptReuseItem]] = {}
    for row in rows:
        groups.setdefault(row.normalized_prompt, []).append(row)
    for items in groups.values():
        items.sort(key=_item_sort_key)
    return groups


def _finding_sort_key(finding: ImagePromptReuseFinding) -> tuple[Any, ...]:
    first_item = finding.items[0] if finding.items else None
    first_prompt = first_item.normalized_prompt if first_item else ""
    return (
        finding.bucket_type != "exact",
        -finding.reuse_count,
        -finding.max_similarity,
        first_prompt,
        first_item.content_id if first_item else 0,
    )


def _item_sort_key(item: ImagePromptReuseItem) -> tuple[Any, ...]:
    return (item.created_at or "", item.content_id)


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    expected = {
        "generated_content": {
            "id",
            "content_type",
            "created_at",
            "image_path",
            "image_prompt",
            "image_alt_text",
        },
    }
    missing_tables = tuple(table for table in expected if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in expected.items()
        if table in schema and columns - schema[table]
    }
    return missing_tables, missing_columns


def _missing_required_columns(missing_columns: dict[str, tuple[str, ...]]) -> bool:
    required = {"id", "created_at", "image_prompt"}
    return bool(required & set(missing_columns.get("generated_content", ())))


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> ImagePromptReuseReport:
    return ImagePromptReuseReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "scanned_prompts": 0,
            "finding_count": 0,
            "exact_buckets": 0,
            "near_buckets": 0,
        },
        findings=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str,
) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _find(parent: list[int], index: int) -> int:
    while parent[index] != index:
        parent[index] = parent[parent[index]]
        index = parent[index]
    return index


def _union(parent: list[int], left: int, right: int) -> None:
    left_root = _find(parent, left)
    right_root = _find(parent, right)
    if left_root != right_root:
        parent[right_root] = left_root
