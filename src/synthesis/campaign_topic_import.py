"""Import campaign topic backlogs into planned_topics."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable


ACCEPTED_FIELDS = ("topic", "angle", "target_date", "priority", "source", "notes")
REQUIRED_FIELDS = ("topic", "target_date")
VALID_PRIORITIES = ("high", "normal", "low")


@dataclass(frozen=True)
class CampaignTopicImportRow:
    """One parsed input row and the decision made for it."""

    reference: str
    status: str
    topic: str | None = None
    angle: str | None = None
    target_date: str | None = None
    priority: str | None = None
    source: str | None = None
    notes: str | None = None
    record_id: int | None = None
    duplicate_id: int | None = None
    errors: tuple[str, ...] = field(default_factory=tuple)
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["errors"] = list(self.errors)
        return payload


@dataclass(frozen=True)
class CampaignTopicImportReport:
    """Structured campaign topic import plan and apply result."""

    campaign_id: int
    dry_run: bool
    skip_duplicates: bool
    rows: tuple[CampaignTopicImportRow, ...]

    @property
    def planned(self) -> tuple[CampaignTopicImportRow, ...]:
        return tuple(row for row in self.rows if row.status == "planned")

    @property
    def inserted(self) -> tuple[CampaignTopicImportRow, ...]:
        return tuple(row for row in self.rows if row.status == "inserted")

    @property
    def duplicates(self) -> tuple[CampaignTopicImportRow, ...]:
        return tuple(row for row in self.rows if row.status in {"duplicate", "skipped_duplicate"})

    @property
    def invalid(self) -> tuple[CampaignTopicImportRow, ...]:
        return tuple(row for row in self.rows if row.status == "invalid")

    @property
    def blocked(self) -> tuple[CampaignTopicImportRow, ...]:
        return tuple(row for row in self.rows if row.status == "blocked")

    def to_dict(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "dry_run": self.dry_run,
            "skip_duplicates": self.skip_duplicates,
            "rows": [row.to_dict() for row in self.rows],
            "summary": {
                "planned": len(self.planned),
                "inserted": len(self.inserted),
                "duplicates": len(self.duplicates),
                "invalid": len(self.invalid),
                "blocked": len(self.blocked),
                "total": len(self.rows),
            },
        }


def import_campaign_topics(
    db: Any,
    *,
    campaign_id: int,
    file_path: str | Path,
    apply: bool = False,
    skip_duplicates: bool = False,
) -> CampaignTopicImportReport:
    """Parse, validate, and optionally insert campaign planned topics."""
    if db.get_campaign(campaign_id) is None:
        raise ValueError(f"Campaign {campaign_id} does not exist")

    source_rows = parse_campaign_topic_file(file_path)
    planned_rows = build_campaign_topic_import_plan(
        db,
        campaign_id=campaign_id,
        source_rows=source_rows,
        dry_run=not apply,
        skip_duplicates=skip_duplicates,
    )
    if not apply:
        return planned_rows

    inserted: list[CampaignTopicImportRow] = []
    for row in planned_rows.rows:
        if row.status == "duplicate":
            if skip_duplicates:
                inserted.append(_replace_status(row, "skipped_duplicate", reason=row.reason or "duplicate"))
            else:
                inserted.append(
                    _replace_status(
                        row,
                        "blocked",
                        reason="duplicate; rerun with --skip-duplicates to skip",
                    )
                )
            continue
        if row.status != "planned":
            inserted.append(row)
            continue
        record_id = db.insert_planned_topic(
            topic=row.topic or "",
            angle=row.angle,
            target_date=row.target_date,
            source_material=_source_material(row),
            campaign_id=campaign_id,
            status="planned",
        )
        inserted.append(_replace_status(row, "inserted", record_id=record_id, reason="inserted"))

    return CampaignTopicImportReport(
        campaign_id=campaign_id,
        dry_run=False,
        skip_duplicates=skip_duplicates,
        rows=tuple(inserted),
    )


def parse_campaign_topic_file(file_path: str | Path) -> list[tuple[str, dict[str, Any]]]:
    """Return input rows with human-readable source references."""
    path = Path(file_path)
    suffix = path.suffix.lower()
    if suffix == ".json":
        return _parse_json_rows(path)
    if suffix == ".csv":
        return _parse_csv_rows(path)
    raise ValueError("file must be a .csv or .json file")


def build_campaign_topic_import_plan(
    db: Any,
    *,
    campaign_id: int,
    source_rows: Iterable[tuple[str, dict[str, Any]]],
    dry_run: bool = True,
    skip_duplicates: bool = False,
) -> CampaignTopicImportReport:
    """Validate input rows and mark planned inserts, duplicates, and invalid rows."""
    existing_keys = _load_existing_keys(db, campaign_id)
    seen_input: dict[tuple[int, str, str], str] = {}
    rows: list[CampaignTopicImportRow] = []

    for reference, raw in source_rows:
        row = _normalize_row(reference, raw)
        if row.errors:
            rows.append(row)
            continue

        key = _duplicate_key(campaign_id, row.topic, row.angle)
        duplicate_id = existing_keys.get(key)
        if duplicate_id is not None:
            rows.append(
                _replace_status(
                    row,
                    "duplicate",
                    duplicate_id=duplicate_id,
                    reason=f"matches planned_topic #{duplicate_id}",
                )
            )
            continue
        first_reference = seen_input.get(key)
        if first_reference is not None:
            rows.append(
                _replace_status(
                    row,
                    "duplicate",
                    reason=f"duplicates input row {first_reference}",
                )
            )
            continue

        seen_input[key] = reference
        rows.append(row)

    return CampaignTopicImportReport(
        campaign_id=campaign_id,
        dry_run=dry_run,
        skip_duplicates=skip_duplicates,
        rows=tuple(rows),
    )


def format_campaign_topic_import_json(report: CampaignTopicImportReport) -> str:
    """Format an import report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_campaign_topic_import_text(report: CampaignTopicImportReport) -> str:
    """Format an import report for terminal review."""
    summary = report.to_dict()["summary"]
    mode = "dry-run" if report.dry_run else "apply"
    lines = [
        "",
        "=" * 70,
        "Campaign Topic Import",
        "=" * 70,
        "",
        f"Campaign: #{report.campaign_id}",
        f"Mode: {mode}",
        (
            f"Rows: total={summary['total']} planned={summary['planned']} "
            f"inserted={summary['inserted']} duplicates={summary['duplicates']} "
            f"invalid={summary['invalid']} blocked={summary['blocked']}"
        ),
    ]
    if not report.rows:
        lines.extend(["", "- none", "", "=" * 70])
        return "\n".join(lines)

    lines.append("")
    for row in report.rows:
        detail = row.topic or "(missing topic)"
        if row.angle:
            detail = f"{detail} / {row.angle}"
        if row.target_date:
            detail = f"{detail} @ {row.target_date}"
        if row.record_id is not None:
            detail = f"#{row.record_id} {detail}"
        suffix = ""
        if row.errors:
            suffix = f" - {'; '.join(row.errors)}"
        elif row.reason:
            suffix = f" - {row.reason}"
        lines.append(f"{row.status:17s} {row.reference:>8s}  {detail}{suffix}")

    lines.extend(["", "=" * 70])
    return "\n".join(lines)


def _parse_json_rows(path: Path) -> list[tuple[str, dict[str, Any]]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON: {exc.msg} at line {exc.lineno}") from exc

    if isinstance(payload, dict) and isinstance(payload.get("topics"), list):
        payload = payload["topics"]
    if not isinstance(payload, list):
        raise ValueError("JSON input must be a list of objects or an object with a topics list")

    rows: list[tuple[str, dict[str, Any]]] = []
    for index, item in enumerate(payload):
        if isinstance(item, dict):
            rows.append((f"index {index}", dict(item)))
        else:
            rows.append((f"index {index}", {"__row_error__": "row must be an object"}))
    return rows


def _parse_csv_rows(path: Path) -> list[tuple[str, dict[str, Any]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows: list[tuple[str, dict[str, Any]]] = []
        for line_number, item in enumerate(reader, start=2):
            rows.append((f"line {line_number}", dict(item)))
    return rows


def _normalize_row(reference: str, raw: dict[str, Any]) -> CampaignTopicImportRow:
    errors: list[str] = []
    if raw.get("__row_error__"):
        errors.append(str(raw["__row_error__"]))

    topic = _clean(raw.get("topic"))
    angle = _clean(raw.get("angle"))
    target_date = _clean(raw.get("target_date"))
    priority = _clean(raw.get("priority")) or "normal"
    source = _clean(raw.get("source"))
    notes = _clean(raw.get("notes"))

    for field_name in REQUIRED_FIELDS:
        if not _clean(raw.get(field_name)):
            errors.append(f"{field_name} is required")
    if target_date:
        try:
            target_date = normalize_target_date(target_date)
        except ValueError as exc:
            errors.append(str(exc))
    if priority and priority not in VALID_PRIORITIES:
        errors.append("priority must be one of: high, normal, low")

    return CampaignTopicImportRow(
        reference=reference,
        status="invalid" if errors else "planned",
        topic=topic,
        angle=angle,
        target_date=target_date,
        priority=priority,
        source=source,
        notes=notes,
        errors=tuple(errors),
        reason="ready" if not errors else None,
    )


def normalize_target_date(value: object) -> str:
    """Normalize a date or datetime-like value to YYYY-MM-DD."""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = _clean(value)
    if not text:
        raise ValueError("target_date is required")
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text).date().isoformat()
    except ValueError:
        try:
            return date.fromisoformat(text).isoformat()
        except ValueError as exc:
            raise ValueError("target_date must be an ISO date or datetime") from exc


def _load_existing_keys(db: Any, campaign_id: int) -> dict[tuple[int, str, str], int]:
    cursor = db.conn.execute(
        """SELECT id, topic, angle
           FROM planned_topics
           WHERE campaign_id = ?""",
        (campaign_id,),
    )
    return {
        _duplicate_key(campaign_id, row["topic"], row["angle"]): int(row["id"])
        for row in cursor.fetchall()
    }


def _duplicate_key(campaign_id: int, topic: str | None, angle: str | None) -> tuple[int, str, str]:
    return (
        int(campaign_id),
        " ".join(str(topic or "").casefold().split()),
        " ".join(str(angle or "").casefold().split()),
    )


def _source_material(row: CampaignTopicImportRow) -> str | None:
    payload = {
        "campaign_topic_import": {
            "priority": row.priority or "normal",
            "source": row.source,
            "notes": row.notes,
        }
    }
    if not row.source and not row.notes and (row.priority or "normal") == "normal":
        return None
    return json.dumps(payload, sort_keys=True)


def _clean(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _replace_status(
    row: CampaignTopicImportRow,
    status: str,
    *,
    record_id: int | None = None,
    duplicate_id: int | None = None,
    reason: str | None = None,
) -> CampaignTopicImportRow:
    return CampaignTopicImportRow(
        reference=row.reference,
        status=status,
        topic=row.topic,
        angle=row.angle,
        target_date=row.target_date,
        priority=row.priority,
        source=row.source,
        notes=row.notes,
        record_id=row.record_id if record_id is None else record_id,
        duplicate_id=row.duplicate_id if duplicate_id is None else duplicate_id,
        errors=row.errors,
        reason=row.reason if reason is None else reason,
    )
