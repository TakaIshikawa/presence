"""Tests for deterministic content variant refresh CLI."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from refresh_content_variants import (  # noqa: E402
    VariantRefreshOptions,
    main,
    refresh_content_variants,
)


def _insert_content(
    db,
    *,
    content_type: str = "x_post",
    content: str = "Tweeting this on X about platform adapters. #python",
) -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=["sha-refresh"],
        source_messages=["message-refresh"],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
    )


def test_dry_run_reports_creates_without_writing_variants(db):
    content_id = _insert_content(db)

    result = refresh_content_variants(
        db,
        VariantRefreshOptions(platforms=("bluesky",), content_id=content_id, dry_run=True),
    )

    assert result.created == 1
    assert result.updated == 0
    assert result.unchanged == 0
    assert result.skipped == 0
    assert result.variants[0]["content_id"] == content_id
    assert result.variants[0]["platform"] == "bluesky"
    assert db.list_content_variants(content_id) == []


def test_non_dry_run_upserts_one_row_per_content_platform_type(db):
    content_id = _insert_content(db)

    result = refresh_content_variants(
        db,
        VariantRefreshOptions(platforms=("bluesky", "linkedin"), content_id=content_id),
    )

    assert result.created == 2
    variants = db.list_content_variants(content_id)
    keys = {(variant["platform"], variant["variant_type"]) for variant in variants}
    assert keys == {("bluesky", "post"), ("linkedin", "post")}
    assert all("Tweeting" not in variant["content"] for variant in variants)

    rerun = refresh_content_variants(
        db,
        VariantRefreshOptions(platforms=("bluesky", "linkedin"), content_id=content_id),
    )

    assert rerun.created == 0
    assert rerun.updated == 0
    assert rerun.unchanged == 2
    assert len(db.list_content_variants(content_id)) == 2


def test_updates_existing_variant_when_adapter_output_changes(db):
    content_id = _insert_content(db, content="Tweeting this on X.")
    db.upsert_content_variant(
        content_id,
        "bluesky",
        "post",
        "stale copy",
        {"source": "old"},
    )

    result = refresh_content_variants(
        db,
        VariantRefreshOptions(platforms=("bluesky",), content_id=content_id),
    )

    assert result.updated == 1
    variants = db.list_content_variants(content_id)
    assert len(variants) == 1
    assert variants[0]["content"] == "Posting this."
    assert variants[0]["metadata"]["adapter"] == "BlueskyPlatformAdapter"


def test_batch_filters_by_platform_content_type_and_limit(db):
    first_id = _insert_content(db, content_type="x_thread", content="1/ Tweeting this on X.")
    _insert_content(db, content_type="blog_seed", content="Blog seed")
    _insert_content(db, content_type="newsletter", content="Newsletter")

    result = refresh_content_variants(
        db,
        VariantRefreshOptions(platforms=("linkedin",), content_type="x_thread", limit=1),
    )

    assert result.created == 1
    assert db.get_content_variant(first_id, "linkedin", "post") is not None
    assert all(variant["platform"] == "linkedin" for variant in result.variants)
    assert all(variant["content_type"] == "x_thread" for variant in result.variants)


def test_duplicate_platform_filters_are_deduped(db):
    content_id = _insert_content(db)

    result = refresh_content_variants(
        db,
        VariantRefreshOptions(platforms=("bluesky", "bluesky"), content_id=content_id),
    )

    assert result.created == 1
    assert len(result.variants) == 1
    assert len(db.list_content_variants(content_id)) == 1


def test_single_content_id_missing_counts_as_noop(db):
    result = refresh_content_variants(
        db,
        VariantRefreshOptions(content_id=9999, platforms=("bluesky",)),
    )

    assert result.to_dict()["created"] == 0
    assert result.to_dict()["updated"] == 0
    assert result.to_dict()["unchanged"] == 0
    assert result.to_dict()["skipped"] == 0
    assert result.variants == []


def test_json_cli_output_includes_counts(db, capsys):
    content_id = _insert_content(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("refresh_content_variants.script_context", fake_script_context):
        exit_code = main(
            [
                "--content-id",
                str(content_id),
                "--platform",
                "bluesky",
                "--dry-run",
                "--json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["created"] == 1
    assert payload["updated"] == 0
    assert payload["unchanged"] == 0
    assert payload["skipped"] == 0
    assert payload["variants"][0]["content_id"] == content_id
    assert db.list_content_variants(content_id) == []
