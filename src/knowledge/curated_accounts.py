"""Merged curated account list from config + DB."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config import Config, CuratedSource
    from storage.db import Database


def get_active_x_accounts(config: Config, db: Database) -> list:
    """Merge config curated X accounts with DB-approved accounts, deduplicated.

    Returns a list of objects with .identifier, .name, .license attributes.
    Config accounts always included. DB-active accounts not in config are appended.
    """
    accounts = []
    seen = set()

    # Config sources (always canonical)
    if config.curated_sources and config.curated_sources.x_accounts:
        for acc in config.curated_sources.x_accounts:
            accounts.append(acc)
            seen.add(acc.identifier.lower())

    # DB-active sources not already in config
    db_rows = db.get_active_curated_sources("x_account")
    for row in db_rows:
        if row["identifier"].lower() not in seen:
            accounts.append(SimpleNamespace(
                identifier=row["identifier"],
                name=row["name"] or row["identifier"],
                license=row["license"] or "attribution_required",
            ))
            seen.add(row["identifier"].lower())

    return accounts
