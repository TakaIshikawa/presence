"""Tests for GitHub discussion comment ingestion."""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import requests

from ingestion.github_discussion_comments import (
    ACTIVITY_TYPE,
    GitHubDiscussionComment,
    GitHubDiscussionCommentClient,
    normalize_discussion_comment_payload,
    poll_new_discussion_comments,
)

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from poll_github_discussion_comments import determine_since, main, parse_since


TIMESTAMP = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

if not hasattr(requests, "exceptions"):
    requests.exceptions = SimpleNamespace(
        HTTPError=requests.HTTPError,
        ConnectionError=requests.ConnectionError,
    )


def _mock_response(status_code: int = 200, json_data=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data if json_data is not None else {}
    if status_code < 400:
        resp.raise_for_status.side_effect = None
    else:
        error = requests.exceptions.HTTPError("HTTP error")
        error.response = resp
        resp.raise_for_status.side_effect = error
    return resp


def _discussion_payload(comment_nodes=None, has_next=False) -> dict:
    return {
        "id": "D_kwDO_discussion",
        "number": 17,
        "title": "Launch planning",
        "url": "https://github.com/acme/widget/discussions/17",
        "updatedAt": "2026-04-01T12:00:00Z",
        "category": {"name": "Ideas", "slug": "ideas", "emoji": ":bulb:"},
        "labels": {"nodes": [{"name": "strategy"}, {"name": "customer"}]},
        "comments": {
            "nodes": comment_nodes if comment_nodes is not None else [_comment_payload()],
            "pageInfo": {"hasNextPage": has_next, "endCursor": "comment-cursor"},
        },
    }


def _comment_payload(comment_id: int = 901, updated_at: str = "2026-04-01T12:00:00Z") -> dict:
    return {
        "databaseId": comment_id,
        "id": f"DC_kwDO_{comment_id}",
        "bodyText": "Discussion comment body with ticket-1234",
        "author": {"login": "maintainer"},
        "url": f"https://github.com/acme/widget/discussions/17#discussioncomment-{comment_id}",
        "createdAt": "2026-04-01T11:00:00Z",
        "updatedAt": updated_at,
        "isAnswer": False,
    }


def _graphql_discussions(nodes, has_next=False):
    return {
        "data": {
            "repository": {
                "discussions": {
                    "nodes": nodes,
                    "pageInfo": {"hasNextPage": has_next, "endCursor": "discussion-cursor"},
                }
            }
        }
    }


def _graphql_comments(nodes, has_next=False):
    return {
        "data": {
            "node": {
                "comments": {
                    "nodes": nodes,
                    "pageInfo": {"hasNextPage": has_next, "endCursor": "next-comment-cursor"},
                }
            }
        }
    }


class TestNormalizeDiscussionComment:
    def test_normalizes_payload_to_stable_activity_shape_and_redacts(self):
        client = GitHubDiscussionCommentClient(
            "tok",
            "taka",
            redaction_patterns=[
                {"name": "ticket", "pattern": r"ticket-\d+", "placeholder": "[REDACTED_TICKET]"}
            ],
        )

        record = normalize_discussion_comment_payload(
            _comment_payload(),
            _discussion_payload(),
            repo="acme/widget",
            redactor=client.redactor,
        )

        assert record["source_type"] == ACTIVITY_TYPE
        assert record["repo"] == "acme/widget"
        assert record["discussion_number"] == 17
        assert record["comment_id"] == 901
        assert record["author"] == "maintainer"
        assert "ticket-1234" not in record["body"]
        assert "[REDACTED_TICKET]" in record["body"]
        assert record["url"].endswith("#discussioncomment-901")
        assert record["created_at"].isoformat() == "2026-04-01T11:00:00+00:00"
        assert record["updated_at"].isoformat() == "2026-04-01T12:00:00+00:00"
        assert record["discussion_title"] == "Launch planning"
        assert record["category"]["slug"] == "ideas"
        assert record["labels"] == ["strategy", "customer"]
        assert record["metadata"]["node_id"] == "DC_kwDO_901"

    def test_uses_node_id_when_database_id_is_missing(self):
        payload = _comment_payload()
        payload.pop("databaseId")

        record = normalize_discussion_comment_payload(payload, _discussion_payload(), repo="acme/widget")

        assert record["comment_id"] == "DC_kwDO_901"


class TestGitHubDiscussionCommentClient:
    def test_get_repo_discussion_comments_paginates_comment_pages_and_filters_since(self):
        first_comment = _comment_payload(901)
        second_comment = _comment_payload(902, "2026-04-01T12:30:00Z")
        old_comment = _comment_payload(903, "2026-03-01T12:00:00Z")
        discussion = _discussion_payload(comment_nodes=[first_comment], has_next=True)
        session = MagicMock()
        session.post.side_effect = [
            _mock_response(json_data=_graphql_discussions([discussion])),
            _mock_response(json_data=_graphql_comments([second_comment, old_comment])),
        ]
        client = GitHubDiscussionCommentClient("tok", "taka", session=session)

        comments = list(
            client.get_repo_discussion_comments(
                "acme",
                "widget",
                repo_name="acme/widget",
                since=TIMESTAMP,
                limit=10,
            )
        )

        assert [comment.comment_id for comment in comments] == [901, 902]
        assert session.post.call_count == 2
        first_vars = session.post.call_args_list[0].kwargs["json"]["variables"]
        assert first_vars["owner"] == "acme"
        assert first_vars["name"] == "widget"
        assert first_vars["commentsFirst"] == 10
        second_vars = session.post.call_args_list[1].kwargs["json"]["variables"]
        assert second_vars["discussionId"] == "D_kwDO_discussion"
        assert second_vars["after"] == "comment-cursor"

    def test_get_repo_discussion_comments_paginates_discussions(self):
        first_discussion = _discussion_payload(comment_nodes=[_comment_payload(901)])
        second_discussion = _discussion_payload(comment_nodes=[_comment_payload(902)])
        second_discussion["number"] = 18
        session = MagicMock()
        session.post.side_effect = [
            _mock_response(json_data=_graphql_discussions([first_discussion], has_next=True)),
            _mock_response(json_data=_graphql_discussions([second_discussion])),
        ]
        client = GitHubDiscussionCommentClient("tok", "taka", session=session)

        comments = list(
            client.get_repo_discussion_comments(
                "acme",
                "widget",
                repo_name="acme/widget",
                since=TIMESTAMP,
                limit=5,
            )
        )

        assert [comment.discussion_number for comment in comments] == [17, 18]
        assert session.post.call_args_list[1].kwargs["json"]["variables"]["after"] == "discussion-cursor"

    def test_comment_to_activity_dict_uses_comment_key_and_discussion_metadata(self):
        comment = GitHubDiscussionComment(
            repo="acme/widget",
            discussion_number=17,
            comment_id=901,
            author="maintainer",
            body="Body",
            url="https://github.com/acme/widget/discussions/17#discussioncomment-901",
            created_at=TIMESTAMP - timedelta(hours=1),
            updated_at=TIMESTAMP,
            discussion_title="Launch planning",
            discussion_url="https://github.com/acme/widget/discussions/17",
            category={"name": "Ideas", "slug": "ideas"},
            labels=["strategy"],
            metadata={"node_id": "DC_kwDO_901"},
        )

        activity = comment.to_activity_dict()

        assert comment.activity_id == f"acme/widget#901:{ACTIVITY_TYPE}"
        assert activity["repo_name"] == "acme/widget"
        assert activity["activity_type"] == ACTIVITY_TYPE
        assert activity["number"] == "901"
        assert activity["title"] == "Discussion comment on #17"
        assert activity["metadata"]["discussion_number"] == 17
        assert activity["metadata"]["parent_type"] == "discussion"
        assert activity["metadata"]["category"]["slug"] == "ideas"
        assert activity["labels"] == ["strategy"]


class TestPollNewDiscussionComments:
    @patch.object(GitHubDiscussionCommentClient, "get_all_recent_discussion_comments")
    def test_persists_only_new_unique_comments(self, mock_comments):
        new_comment = GitHubDiscussionComment(
            repo="acme/widget",
            discussion_number=17,
            comment_id=901,
            author="maintainer",
            body="Body",
            url="url",
            created_at=TIMESTAMP,
            updated_at=TIMESTAMP,
        )
        existing = GitHubDiscussionComment(
            repo="acme/widget",
            discussion_number=18,
            comment_id=902,
            author="maintainer",
            body="Old",
            url="url",
            created_at=TIMESTAMP,
            updated_at=TIMESTAMP,
        )
        duplicate = GitHubDiscussionComment(**new_comment.__dict__)
        mock_comments.return_value = iter([new_comment, duplicate, existing])
        db = MagicMock()
        db.is_github_activity_processed.side_effect = [False, True]

        result = poll_new_discussion_comments("tok", "taka", TIMESTAMP, db, repositories=["acme/widget"])

        assert result == [new_comment]
        assert db.is_github_activity_processed.call_count == 2
        db.upsert_github_activity.assert_called_once_with(**new_comment.to_activity_dict())

    @patch.object(GitHubDiscussionCommentClient, "get_all_recent_discussion_comments")
    def test_dry_run_does_not_persist(self, mock_comments):
        comment = GitHubDiscussionComment(
            repo="acme/widget",
            discussion_number=17,
            comment_id=901,
            author="maintainer",
            body="Body",
            url="url",
            created_at=TIMESTAMP,
            updated_at=TIMESTAMP,
        )
        mock_comments.return_value = iter([comment])
        db = MagicMock()
        db.is_github_activity_processed.return_value = False

        assert poll_new_discussion_comments("tok", "taka", TIMESTAMP, db, dry_run=True) == [comment]
        db.upsert_github_activity.assert_not_called()

    @patch.object(GitHubDiscussionCommentClient, "get_all_recent_discussion_comments")
    def test_persists_to_existing_github_activity_table(self, mock_comments, db):
        initial = GitHubDiscussionComment(
            repo="acme/widget",
            discussion_number=17,
            comment_id=901,
            author="maintainer",
            body="Body",
            url="url",
            created_at=TIMESTAMP,
            updated_at=TIMESTAMP,
            category={"name": "Ideas"},
            labels=["strategy"],
        )
        updated = GitHubDiscussionComment(
            repo="acme/widget",
            discussion_number=17,
            comment_id=901,
            author="maintainer",
            body="Updated body",
            url="url",
            created_at=TIMESTAMP,
            updated_at=TIMESTAMP + timedelta(minutes=5),
            category={"name": "Ideas"},
            labels=["strategy"],
        )
        mock_comments.return_value = iter([initial])
        assert poll_new_discussion_comments("tok", "taka", TIMESTAMP, db) == [initial]
        mock_comments.return_value = iter([updated])
        assert poll_new_discussion_comments("tok", "taka", TIMESTAMP, db) == [updated]

        rows = db.get_github_activity_in_range(TIMESTAMP, TIMESTAMP + timedelta(hours=1))

        assert len(rows) == 1
        assert rows[0]["activity_type"] == ACTIVITY_TYPE
        assert str(rows[0]["number"]) == "901"
        assert rows[0]["body"] == "Updated body"
        assert rows[0]["metadata"]["comment_id"] == 901
        assert rows[0]["metadata"]["discussion_number"] == 17
        assert rows[0]["metadata"]["category"]["name"] == "Ideas"
        assert rows[0]["labels"] == ["strategy"]


def test_parse_since_accepts_z_suffix():
    assert parse_since("2026-04-01T12:00:00Z") == TIMESTAMP


def test_determine_since_uses_state_file_before_db_watermark(tmp_path, db):
    state_file = tmp_path / "discussion-comments-state.json"
    state_file.write_text(json.dumps({"last_poll_time": TIMESTAMP.isoformat()}))
    db.set_last_github_discussion_comment_poll_time(TIMESTAMP - timedelta(hours=1))

    assert determine_since(db, None, 90, str(state_file)) == TIMESTAMP


def test_determine_since_uses_discussion_comment_poll_watermark(db):
    db.set_last_github_discussion_comment_poll_time(TIMESTAMP - timedelta(hours=1))

    assert determine_since(db, None, 90) == TIMESTAMP - timedelta(hours=1)


@patch("poll_github_discussion_comments.update_monitoring")
@patch("poll_github_discussion_comments.ingest_github_discussion_comments")
@patch("poll_github_discussion_comments.script_context")
def test_main_dry_run_prints_comments_without_watermark(
    mock_context,
    mock_ingest,
    mock_update,
    db,
    capsys,
):
    config = MagicMock()
    config.github.token = "tok"
    config.github.username = "taka"
    config.github.repositories = ["acme/widget"]
    config.privacy.redaction_patterns = []
    config.timeouts.github_seconds = 10
    mock_context.return_value.__enter__.return_value = (config, db)
    mock_context.return_value.__exit__.return_value = None
    comment = GitHubDiscussionComment(
        repo="acme/widget",
        discussion_number=17,
        comment_id=901,
        author="maintainer",
        body="Body",
        url="https://github.com/acme/widget/discussions/17#discussioncomment-901",
        created_at=TIMESTAMP,
        updated_at=TIMESTAMP,
    )
    mock_ingest.return_value = [comment]

    assert main(["--dry-run", "--since", "2026-04-01T12:00:00Z"]) == 0

    assert "Would ingest acme/widget#901:discussion_comment" in capsys.readouterr().out
    assert db.get_last_github_discussion_comment_poll_time() is None
    mock_update.assert_not_called()
    assert mock_ingest.call_args.kwargs["dry_run"] is True


@patch("poll_github_discussion_comments.update_monitoring")
@patch("poll_github_discussion_comments.ingest_github_discussion_comments")
@patch("poll_github_discussion_comments.script_context")
def test_main_persists_state_file_when_enabled(mock_context, mock_ingest, mock_update, db, tmp_path):
    config = MagicMock()
    config.github.token = "tok"
    config.github.username = "taka"
    config.github.repositories = []
    config.privacy.redaction_patterns = []
    config.timeouts.github_seconds = 10
    mock_context.return_value.__enter__.return_value = (config, db)
    mock_context.return_value.__exit__.return_value = None
    mock_ingest.return_value = []
    state_file = tmp_path / "state.json"

    assert main(["--since", "2026-04-01T12:00:00Z", "--state-file", str(state_file)]) == 0

    assert json.loads(state_file.read_text())["last_poll_time"]
    assert db.get_last_github_discussion_comment_poll_time() is None
    mock_update.assert_called_once_with("poll-github-discussion-comments")
