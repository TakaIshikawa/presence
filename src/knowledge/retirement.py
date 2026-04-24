"""Identify and retire stale low-value knowledge items."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any


REASON_OLD = "old"
REASON_UNUSED = "unused"
REASON_RESTRICTED = "restricted"
REASON_REPEATEDLY_UNCITED = "repeatedly_uncited"
REASON_RECENT_USAGE = "recent_usage"


@dataclass(frozen=True)
class KnowledgeRetirementPolicy:
    """Selection policy for knowledge retirement."""

    older_than_days: int = 180
    source_type: str | None = None
    license: str | None = None
    min_unused_days: int = 30
    uncited_threshold: int = 2
    now: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        if self.older_than_days < 1:
            raise ValueError("older_than_days must be at least 1")
        if self.min_unused_days < 0:
            raise ValueError("min_unused_days must be non-negative")
        if self.uncited_threshold < 1:
            raise ValueError("uncited_threshold must be at least 1")

    @property
    def older_than_cutoff(self) -> str:
        return (self.now - timedelta(days=self.older_than_days)).isoformat()

    @property
    def unused_since_cutoff(self) -> str:
        return (self.now - timedelta(days=self.min_unused_days)).isoformat()


def _count_values(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts = Counter(str(item.get(key) or "unknown") for item in items)
    return dict(sorted(counts.items()))


def _count_reasons(items: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for item in items:
        for reason in item.get("reasons", []):
            counts[str(reason)] += 1
    return dict(sorted(counts.items()))


def build_retirement_report(
    db: Any,
    policy: KnowledgeRetirementPolicy,
    *,
    apply: bool = False,
) -> dict[str, Any]:
    """Return a retirement report and optionally mark selected rows unapproved."""
    items = db.get_knowledge_retirement_candidates(
        older_than_cutoff=policy.older_than_cutoff,
        unused_since_cutoff=policy.unused_since_cutoff,
        source_type=policy.source_type,
        license=policy.license,
        uncited_threshold=policy.uncited_threshold,
    )
    retire_items = [item for item in items if item["action"] == "retire"]
    retained_items = [item for item in items if item["action"] == "retain"]
    retired_ids = [item["id"] for item in retire_items]

    if apply and retired_ids:
        db.retire_knowledge_items(retired_ids)

    return {
        "mode": "apply" if apply else "dry_run",
        "policy": {
            "older_than_days": policy.older_than_days,
            "older_than_cutoff": policy.older_than_cutoff,
            "source_type": policy.source_type,
            "license": policy.license,
            "min_unused_days": policy.min_unused_days,
            "unused_since_cutoff": policy.unused_since_cutoff,
            "uncited_threshold": policy.uncited_threshold,
        },
        "totals": {
            "considered": len(items),
            "retained": len(retained_items),
            "retired": len(retire_items),
            "by_source_type": _count_values(retire_items, "source_type"),
            "by_license": _count_values(retire_items, "license"),
            "by_reason": _count_reasons(retire_items),
        },
        "items": items,
    }
