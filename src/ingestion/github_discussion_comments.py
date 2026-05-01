"""Fetch GitHub discussion comments as first-class activity rows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Iterator, Optional

import requests

from ingestion.github_activity import BODY_EXCERPT_MAX_CHARS, _body_excerpt
from ingestion.github_commits import (
    GitHubAuthError,
    GitHubClientError,
    GitHubNotFoundError,
    GitHubRateLimitError,
)
from ingestion.redaction import Redactor
from output.api_rate_guard import record_snapshot

ACTIVITY_TYPE = "discussion_comment"


@dataclass
class GitHubDiscussionComment:
    repo: str
    discussion_number: int
    comment_id: int | str
    author: str
    body: str
    url: str
    created_at: datetime | None
    updated_at: datetime
    discussion_title: str = ""
    discussion_url: str = ""
    category: dict = field(default_factory=dict)
    labels: list[str] = field(default_factory=list)
    source_type: str = ACTIVITY_TYPE
    metadata: dict = field(default_factory=dict)

    @property
    def activity_id(self) -> str:
        return f"{self.repo}#{self.comment_id}:{self.source_type}"

    def to_dict(self) -> dict:
        return {
            "source_type": self.source_type,
            "repo": self.repo,
            "discussion_number": self.discussion_number,
            "comment_id": self.comment_id,
            "author": self.author,
            "body": self.body,
            "url": self.url,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat(),
            "discussion_title": self.discussion_title,
            "discussion_url": self.discussion_url,
            "category": self.category,
            "labels": self.labels,
            "metadata": self.metadata,
        }

    def to_activity_dict(self) -> dict:
        metadata = {
            **self.metadata,
            "activity_id": self.activity_id,
            "source_type": self.source_type,
            "comment_id": self.comment_id,
            "discussion_number": self.discussion_number,
            "parent_number": self.discussion_number,
            "parent_type": "discussion",
            "discussion_title": self.discussion_title,
            "discussion_url": self.discussion_url,
            "category": self.category,
            "labels": self.labels,
        }
        return {
            "repo_name": self.repo,
            "activity_type": self.source_type,
            "number": str(self.comment_id),
            "title": f"Discussion comment on #{self.discussion_number}",
            "body": self.body,
            "state": "commented",
            "author": self.author,
            "url": self.url,
            "updated_at": self.updated_at.isoformat(),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "labels": self.labels,
            "metadata": {key: value for key, value in metadata.items() if value not in (None, "")},
        }


def _parse_github_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def normalize_discussion_comment_payload(
    payload: dict,
    discussion: dict,
    repo: str,
    redactor: Redactor | None = None,
) -> dict:
    """Normalize a GitHub GraphQL discussion comment payload."""
    redactor = redactor or Redactor()
    updated_at = _parse_github_datetime(payload.get("updatedAt")) or _parse_github_datetime(
        payload.get("createdAt")
    )
    if not updated_at:
        raise ValueError("GitHub discussion comment payload is missing updatedAt/createdAt")

    comment_id = payload.get("databaseId") or payload.get("id")
    if comment_id is None:
        raise ValueError("GitHub discussion comment payload is missing databaseId/id")

    category = discussion.get("category") or {}
    label_nodes = ((discussion.get("labels") or {}).get("nodes") or [])
    labels = [label.get("name", "") for label in label_nodes if label.get("name")]
    metadata = {
        "node_id": payload.get("id"),
        "discussion_node_id": discussion.get("id"),
        "is_answer": payload.get("isAnswer"),
    }
    return {
        "source_type": ACTIVITY_TYPE,
        "repo": repo,
        "discussion_number": int(discussion["number"]),
        "comment_id": comment_id,
        "author": (payload.get("author") or {}).get("login", ""),
        "body": _body_excerpt(
            redactor.redact(payload.get("bodyText") or payload.get("body") or ""),
            max_len=BODY_EXCERPT_MAX_CHARS,
        ),
        "url": payload.get("url") or "",
        "created_at": _parse_github_datetime(payload.get("createdAt")),
        "updated_at": updated_at,
        "discussion_title": redactor.redact(discussion.get("title") or ""),
        "discussion_url": discussion.get("url") or "",
        "category": {
            key: value
            for key, value in {
                "name": category.get("name"),
                "slug": category.get("slug"),
                "emoji": category.get("emoji"),
            }.items()
            if value is not None
        },
        "labels": labels,
        "metadata": {key: value for key, value in metadata.items() if value is not None},
    }


class GitHubDiscussionCommentClient:
    GRAPHQL_URL = "https://api.github.com/graphql"
    BASE_URL = "https://api.github.com"

    def __init__(
        self,
        token: str,
        username: str,
        timeout: int = 30,
        redaction_patterns: Optional[Iterable[str | dict]] = None,
        db=None,
        session=None,
    ):
        self.token = token
        self.username = username
        self.timeout = timeout
        self.db = db
        self.session = session or requests
        self.redactor = Redactor(redaction_patterns)
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
        }

    def _post_graphql(self, query: str, variables: dict) -> dict:
        try:
            response = self.session.post(
                self.GRAPHQL_URL,
                headers=self.headers,
                json={"query": query, "variables": variables},
                timeout=self.timeout,
            )
            response.raise_for_status()
            self._record_rate_limit(response, endpoint="/graphql")
        except requests.exceptions.ConnectionError as e:
            raise GitHubClientError(f"Connection error: {e}") from e
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                raise GitHubAuthError(f"Authentication failed: {e}") from e
            if e.response.status_code == 403:
                raise GitHubRateLimitError(f"Rate limit exceeded: {e}") from e
            if e.response.status_code == 404:
                raise GitHubNotFoundError(f"Resource not found: {e}") from e
            raise GitHubClientError(f"HTTP error: {e}") from e

        payload = response.json()
        errors = payload.get("errors") or []
        if errors:
            messages = "; ".join(error.get("message", "") for error in errors)
            if "discussions" in messages.lower() and "repository" in messages.lower():
                return {"data": {"repository": {"discussions": {"nodes": [], "pageInfo": {}}}}}
            raise GitHubClientError(f"GraphQL error: {messages}")
        return payload

    def _get(self, path: str, params: dict) -> list[dict]:
        try:
            response = self.session.get(
                f"{self.BASE_URL}{path}",
                headers=self.headers,
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
            self._record_rate_limit(response, endpoint=path)
        except requests.exceptions.ConnectionError as e:
            raise GitHubClientError(f"Connection error: {e}") from e
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                raise GitHubAuthError(f"Authentication failed: {e}") from e
            if e.response.status_code == 403:
                raise GitHubRateLimitError(f"Rate limit exceeded: {e}") from e
            if e.response.status_code == 404:
                raise GitHubNotFoundError(f"Resource not found: {e}") from e
            raise GitHubClientError(f"HTTP error: {e}") from e
        data = response.json()
        return data if isinstance(data, list) else []

    def _record_rate_limit(self, response, endpoint: str) -> None:
        try:
            record_snapshot(
                self.db,
                "github",
                headers=getattr(response, "headers", None),
                endpoint=endpoint,
            )
        except Exception:
            pass

    def get_configured_repos(
        self,
        repositories: Optional[list[str | dict]] = None,
        include_forks: bool = False,
    ) -> list[dict]:
        if repositories:
            return [self._normalize_repo_config(repo) for repo in repositories]

        repos = []
        page = 1
        while True:
            data = self._get(
                "/user/repos",
                {
                    "affiliation": "owner",
                    "sort": "pushed",
                    "per_page": 100,
                    "page": page,
                },
            )
            if not data:
                break
            for repo in data:
                if not include_forks and repo.get("fork"):
                    continue
                repos.append(
                    {
                        "owner": repo.get("owner", {}).get("login", self.username),
                        "name": repo["name"],
                        "repo_name": repo.get("full_name") or repo["name"],
                    }
                )
            page += 1
        return repos

    def _normalize_repo_config(self, repo: str | dict) -> dict:
        if isinstance(repo, str):
            if "/" in repo:
                owner, name = repo.split("/", 1)
                return {"owner": owner, "name": name, "repo_name": repo}
            return {"owner": self.username, "name": repo, "repo_name": repo}

        owner = repo.get("owner") or self.username
        name = repo.get("name") or repo.get("repo") or repo.get("repository")
        if not name:
            raise ValueError("GitHub repository config requires a name")
        repo_name = repo.get("repo_name") or repo.get("full_name") or (
            f"{owner}/{name}" if owner != self.username else name
        )
        return {"owner": owner, "name": name, "repo_name": repo_name}

    def get_repo_discussion_comments(
        self,
        owner: str,
        repo: str,
        repo_name: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> Iterator[GitHubDiscussionComment]:
        yielded = 0
        discussion_cursor = None
        while yielded < limit:
            payload = self._post_graphql(
                _RECENT_DISCUSSIONS_QUERY,
                {
                    "owner": owner,
                    "name": repo,
                    "first": min(100, limit - yielded),
                    "after": discussion_cursor,
                    "commentsFirst": min(100, limit - yielded),
                },
            )
            repository = (payload.get("data") or {}).get("repository") or {}
            discussions = repository.get("discussions") or {}
            nodes = discussions.get("nodes") or []
            if not nodes:
                break

            for discussion in nodes:
                discussion_updated_at = _parse_github_datetime(discussion.get("updatedAt"))
                if since and discussion_updated_at and discussion_updated_at < since:
                    return
                for comment in self._iter_discussion_comment_payloads(discussion, since, limit - yielded):
                    yield GitHubDiscussionComment(
                        **normalize_discussion_comment_payload(
                            comment,
                            discussion,
                            repo=repo_name or repo,
                            redactor=self.redactor,
                        )
                    )
                    yielded += 1
                    if yielded >= limit:
                        break
                if yielded >= limit:
                    break

            page_info = discussions.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            discussion_cursor = page_info.get("endCursor")

    def _iter_discussion_comment_payloads(
        self,
        discussion: dict,
        since: Optional[datetime],
        limit: int,
    ) -> Iterator[dict]:
        yielded = 0
        comments = discussion.get("comments") or {}
        nodes = comments.get("nodes") or []
        for comment in nodes:
            updated_at = _parse_github_datetime(comment.get("updatedAt")) or _parse_github_datetime(
                comment.get("createdAt")
            )
            if since and updated_at and updated_at < since:
                return
            yield comment
            yielded += 1
            if yielded >= limit:
                return

        page_info = comments.get("pageInfo") or {}
        cursor = page_info.get("endCursor")
        while yielded < limit and page_info.get("hasNextPage"):
            payload = self._post_graphql(
                _DISCUSSION_COMMENTS_QUERY,
                {
                    "discussionId": discussion.get("id"),
                    "first": min(100, limit - yielded),
                    "after": cursor,
                },
            )
            node = (payload.get("data") or {}).get("node") or {}
            comments = node.get("comments") or {}
            nodes = comments.get("nodes") or []
            if not nodes:
                break
            for comment in nodes:
                updated_at = _parse_github_datetime(comment.get("updatedAt")) or _parse_github_datetime(
                    comment.get("createdAt")
                )
                if since and updated_at and updated_at < since:
                    return
                yield comment
                yielded += 1
                if yielded >= limit:
                    return
            page_info = comments.get("pageInfo") or {}
            cursor = page_info.get("endCursor")

    def get_all_recent_discussion_comments(
        self,
        since: Optional[datetime] = None,
        repositories: Optional[list[str | dict]] = None,
        include_forks: bool = False,
        limit_per_repo: int = 100,
    ) -> Iterator[GitHubDiscussionComment]:
        for repo in self.get_configured_repos(repositories, include_forks=include_forks):
            try:
                yield from self.get_repo_discussion_comments(
                    repo["owner"],
                    repo["name"],
                    repo_name=repo["repo_name"],
                    since=since,
                    limit=limit_per_repo,
                )
            except (GitHubRateLimitError, GitHubNotFoundError):
                pass


def poll_new_discussion_comments(
    token: str,
    username: str,
    since: datetime,
    db,
    repositories: Optional[list[str | dict]] = None,
    dry_run: bool = False,
    limit_per_repo: int = 100,
    timeout: int = 30,
    redaction_patterns: Optional[Iterable[str | dict]] = None,
    session=None,
) -> list[GitHubDiscussionComment]:
    """Poll for new or updated discussion comments and optionally persist them."""
    client = GitHubDiscussionCommentClient(
        token,
        username,
        timeout=timeout,
        redaction_patterns=redaction_patterns,
        db=db,
        session=session,
    )
    comments = []
    seen: set[tuple[str, str]] = set()

    for comment in client.get_all_recent_discussion_comments(
        since=since,
        repositories=repositories,
        limit_per_repo=limit_per_repo,
    ):
        identity = (comment.repo, str(comment.comment_id))
        if identity in seen:
            continue
        seen.add(identity)

        if db.is_github_activity_processed(
            comment.repo,
            comment.source_type,
            str(comment.comment_id),
            comment.updated_at.isoformat(),
        ):
            continue

        if not dry_run:
            db.upsert_github_activity(**comment.to_activity_dict())
        comments.append(comment)

    return comments


_COMMENT_FIELDS = """
databaseId
id
bodyText
url
createdAt
updatedAt
isAnswer
author { login }
"""

_RECENT_DISCUSSIONS_QUERY = f"""
query RecentDiscussionComments(
  $owner: String!,
  $name: String!,
  $first: Int!,
  $after: String,
  $commentsFirst: Int!
) {{
  repository(owner: $owner, name: $name) {{
    discussions(first: $first, after: $after, orderBy: {{field: UPDATED_AT, direction: DESC}}) {{
      nodes {{
        id
        number
        title
        url
        updatedAt
        category {{ name slug emoji }}
        labels(first: 20) {{ nodes {{ name }} }}
        comments(first: $commentsFirst, orderBy: {{field: UPDATED_AT, direction: DESC}}) {{
          nodes {{
            {_COMMENT_FIELDS}
          }}
          pageInfo {{
            hasNextPage
            endCursor
          }}
        }}
      }}
      pageInfo {{
        hasNextPage
        endCursor
      }}
    }}
  }}
}}
"""

_DISCUSSION_COMMENTS_QUERY = f"""
query DiscussionComments($discussionId: ID!, $first: Int!, $after: String) {{
  node(id: $discussionId) {{
    ... on Discussion {{
      comments(first: $first, after: $after, orderBy: {{field: UPDATED_AT, direction: DESC}}) {{
        nodes {{
          {_COMMENT_FIELDS}
        }}
        pageInfo {{
          hasNextPage
          endCursor
        }}
      }}
    }}
  }}
}}
"""
