#!/usr/bin/env python3
"""Import and export content campaigns as YAML."""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.topic_extractor import TOPIC_TAXONOMY
from runner import script_context


CAMPAIGN_FIELDS = ("goal", "start_date", "end_date", "status")
PLANNED_TOPIC_FIELDS = ("angle", "source_material", "status")


def normalize_date(value, field_name: str) -> str | None:
    """Normalize a YAML date/datetime/string to an ISO string."""
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO date string")
    try:
        datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date string") from exc
    return value


def load_campaign_yaml(path: str) -> dict:
    """Load campaign YAML from a file path."""
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError("Campaign YAML must be a mapping")
    return data


def _optional_str(value) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _campaign_payload(raw: dict) -> dict:
    if not isinstance(raw, dict):
        raise ValueError("Each campaign must be a mapping")
    name = _optional_str(raw.get("name"))
    if not name:
        raise ValueError("Campaign name is required")
    status = _optional_str(raw.get("status")) or "planned"
    return {
        "name": name,
        "goal": _optional_str(raw.get("goal")),
        "start_date": normalize_date(raw.get("start_date"), f"{name} start_date"),
        "end_date": normalize_date(raw.get("end_date"), f"{name} end_date"),
        "status": status,
    }


def _planned_topic_payload(raw: dict, campaign_id: int | None) -> dict:
    if not isinstance(raw, dict):
        raise ValueError("Each planned topic must be a mapping")
    topic = _optional_str(raw.get("topic"))
    if not topic:
        raise ValueError("Planned topic requires topic")
    if topic not in TOPIC_TAXONOMY:
        raise ValueError(f"Invalid topic '{topic}'. Must be one of: {', '.join(TOPIC_TAXONOMY)}")
    return {
        "topic": topic,
        "angle": _optional_str(raw.get("angle")),
        "target_date": normalize_date(raw.get("target_date"), f"{topic} target_date"),
        "source_material": _optional_str(raw.get("source_material")),
        "campaign_id": campaign_id,
        "status": _optional_str(raw.get("status")) or "planned",
    }


def _diff(existing: dict, desired: dict, fields: tuple[str, ...]) -> list[str]:
    changes = []
    for field in fields:
        if existing.get(field) != desired.get(field):
            changes.append(f"{field}: {existing.get(field)!r} -> {desired.get(field)!r}")
    return changes


def _print_or_collect(changes: list[str], message: str) -> None:
    changes.append(message)
    print(message)


def import_campaigns(db, data: dict, dry_run: bool = False) -> list[str]:
    """Create or update campaigns and planned topics from parsed YAML."""
    changes = []

    for raw_campaign in data.get("campaigns") or []:
        campaign = _campaign_payload(raw_campaign)
        existing_campaign = db.get_campaign_by_name(campaign["name"])
        campaign_id = existing_campaign["id"] if existing_campaign else None

        if existing_campaign is None:
            _print_or_collect(changes, f"create campaign: {campaign['name']}")
            if not dry_run:
                campaign_id = db.create_campaign(**campaign)
        else:
            diffs = _diff(existing_campaign, campaign, CAMPAIGN_FIELDS)
            if diffs:
                _print_or_collect(changes, f"update campaign: {campaign['name']} ({'; '.join(diffs)})")
                if not dry_run:
                    db.update_campaign(campaign_id, **campaign)

        for raw_topic in raw_campaign.get("planned_topics") or raw_campaign.get("topics") or []:
            topic = _planned_topic_payload(raw_topic, campaign_id)
            _upsert_planned_topic(
                db,
                topic,
                dry_run,
                changes,
                campaign_label=f"campaign={campaign['name']}",
                lookup_existing=campaign_id is not None,
            )

    for raw_topic in data.get("planned_topics") or []:
        topic = _planned_topic_payload(raw_topic, None)
        _upsert_planned_topic(db, topic, dry_run, changes)

    if dry_run and not changes:
        print("No changes.")
    return changes


def _upsert_planned_topic(
    db,
    topic: dict,
    dry_run: bool,
    changes: list[str],
    campaign_label: str | None = None,
    lookup_existing: bool = True,
) -> None:
    existing = None
    if lookup_existing:
        existing = db.find_planned_topic(
            topic=topic["topic"],
            target_date=topic["target_date"],
            campaign_id=topic["campaign_id"],
        )
    if campaign_label is None:
        campaign_label = (
            f"campaign_id={topic['campaign_id']}"
            if topic["campaign_id"] is not None
            else "no campaign"
        )
    label = f"{topic['topic']} @ {topic['target_date'] or 'unscheduled'} ({campaign_label})"

    if existing is None:
        _print_or_collect(changes, f"create planned topic: {label}")
        if not dry_run:
            db.insert_planned_topic(**topic)
        return

    diffs = _diff(existing, topic, PLANNED_TOPIC_FIELDS)
    if diffs:
        _print_or_collect(changes, f"update planned topic: {label} ({'; '.join(diffs)})")
        if not dry_run:
            db.update_planned_topic(existing["id"], **topic)


def export_campaigns(db) -> dict:
    """Export active/planned campaigns and their planned topics."""
    planned_topics = db.get_planned_topics(status="planned")
    topics_by_campaign: dict[int | None, list[dict]] = {}
    for topic in planned_topics:
        topics_by_campaign.setdefault(topic.get("campaign_id"), []).append(topic)

    campaigns = [
        campaign
        for campaign in db.get_campaigns()
        if campaign.get("status") in {"active", "planned"}
    ]

    output_campaigns = []
    for campaign in campaigns:
        output_campaigns.append({
            "name": campaign["name"],
            "goal": campaign.get("goal"),
            "start_date": campaign.get("start_date"),
            "end_date": campaign.get("end_date"),
            "status": campaign.get("status") or "planned",
            "planned_topics": [
                _export_topic(topic)
                for topic in topics_by_campaign.get(campaign["id"], [])
            ],
        })

    output = {"campaigns": output_campaigns}
    uncampaigned = topics_by_campaign.get(None, [])
    if uncampaigned:
        output["planned_topics"] = [_export_topic(topic) for topic in uncampaigned]
    return output


def _export_topic(topic: dict) -> dict:
    return {
        "topic": topic["topic"],
        "angle": topic.get("angle"),
        "target_date": topic.get("target_date"),
        "source_material": topic.get("source_material"),
        "status": topic.get("status") or "planned",
    }


def dump_campaign_yaml(data: dict) -> str:
    """Render campaign data as stable YAML."""
    return yaml.safe_dump(data, sort_keys=False, allow_unicode=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Import and export content campaigns as YAML")
    subparsers = parser.add_subparsers(dest="command", required=True)

    import_parser = subparsers.add_parser("import", help="Import campaigns from YAML")
    import_parser.add_argument("path", help="YAML file to import")
    import_parser.add_argument("--dry-run", action="store_true", help="Print changes without writing")

    export_parser = subparsers.add_parser("export", help="Export active/planned campaigns as YAML")
    export_parser.add_argument("--output", "-o", help="Write YAML to a file instead of stdout")

    args = parser.parse_args()

    with script_context() as (_config, db):
        if args.command == "import":
            data = load_campaign_yaml(args.path)
            import_campaigns(db, data, dry_run=args.dry_run)
        elif args.command == "export":
            rendered = dump_campaign_yaml(export_campaigns(db))
            if args.output:
                Path(args.output).write_text(rendered, encoding="utf-8")
            else:
                print(rendered, end="")


if __name__ == "__main__":
    main()
