"""Shared parsing helpers for compact import adapters."""
from __future__ import annotations
import csv, io, json
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from evaluation._batch_report_utils import dump_json, text

def records(raw: str, *container_keys: str) -> list[dict[str, Any]]:
    raw = raw.strip()
    if not raw:
        return []
    if raw[0] in "[{":
        data = json.loads(raw)
        if isinstance(data, dict):
            for key in container_keys:
                value = data.get(key)
                if isinstance(value, list):
                    return value
            return [data]
        return data
    first = raw.splitlines()[0]
    if "," in first:
        return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(line) for line in raw.splitlines() if line.strip()]

def one(row: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in row and text(row.get(name)):
            return row.get(name)
    return ""

def req(row: dict[str, Any], *names: str, label: str | None = None) -> str:
    value = text(one(row, *names))
    if not value:
        raise ValueError(f"{label or '/'.join(names)} is required")
    return value

def int0(value: Any) -> int:
    try:
        return int(float(text(value) or "0"))
    except (TypeError, ValueError):
        return 0

def float0(value: Any) -> float:
    raw = text(value).rstrip("%")
    try:
        num = float(raw or "0")
    except (TypeError, ValueError):
        return 0.0
    return num / 100 if text(value).endswith("%") else num

def norm_url(value: Any, *, strip_query: bool = False) -> str:
    raw = text(value)
    if not raw:
        return ""
    parsed = urlsplit(raw)
    if not parsed.scheme and parsed.netloc:
        parsed = urlsplit("https:" + raw)
    elif not parsed.scheme:
        parsed = urlsplit("https://" + raw)
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, "" if strip_query else parsed.query, ""))

def raw_payload(row: dict[str, Any]) -> str:
    value = row.get("raw_payload") or row.get("metadata") or row
    return value if isinstance(value, str) else json.dumps(value, sort_keys=True)

def summary(name: str, parsed: int, upserted: int, dry_run: bool, skipped: list[str] | None = None) -> dict[str, Any]:
    return {"artifact_type": name, "dry_run": dry_run, "parsed_count": parsed, "upserted_count": upserted, "skipped_count": len(skipped or []), "errors": skipped or []}

def fmt_json(payload: dict[str, Any]) -> str:
    return dump_json(payload)

def fmt_text(title: str, payload: dict[str, Any]) -> str:
    return f"{title}\nparsed={payload['parsed_count']} upserted={payload['upserted_count']} skipped={payload.get('skipped_count',0)} dry_run={payload['dry_run']}"
