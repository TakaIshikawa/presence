"""Tests for the Bluesky engagement import CLI."""

import json

from scripts import import_bluesky_engagement as cli


class DummyContext:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return None, self.db

    def __exit__(self, exc_type, exc, tb):
        return False


def test_cli_json_dry_run_reports_rows(
    db, sample_content, tmp_path, monkeypatch, capsys
):
    content_id = db.insert_generated_content(**sample_content)
    db.mark_published_bluesky(
        content_id,
        "at://did:plc:test/app.bsky.feed.post/cli",
    )
    csv_path = tmp_path / "bluesky.csv"
    csv_path.write_text(
        "content_id,likes,reposts,replies,quotes\n"
        f"{content_id},7,1,2,0\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(cli, "script_context", lambda: DummyContext(db))
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: cli.argparse.Namespace(
            csv=str(csv_path),
            dry_run=True,
            json=True,
        ),
    )

    cli.main()

    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["counts"]["matched"] == 1
    assert payload["rows"][0]["status"] == "matched"
    assert db.get_bluesky_engagement(content_id) == []
