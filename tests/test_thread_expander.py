from unittest.mock import MagicMock

from synthesis.thread_expander import (
    SourceCommit,
    SourceMessage,
    ThreadExpander,
    ThreadExpansionCandidate,
)


def _mock_response(text):
    response = MagicMock()
    response.content = [MagicMock(text=text)]
    return response


def test_expand_calls_anthropic_with_thread_sources_and_returns_result():
    client = MagicMock()
    client.messages.create.return_value = _mock_response(
        "TITLE: The Blog Version\n\n## Context\n\nExpanded draft."
    )
    expander = ThreadExpander(api_key="test-key", model="claude-test", client=client)
    candidate = ThreadExpansionCandidate(
        content_id=17,
        original_thread="TWEET 1: Shipping the feature taught me where the edge is.",
        engagement_score=14.5,
        source_commits=["abc123"],
        source_messages=["msg-1"],
        commit_context=[
            SourceCommit(
                sha="abc123",
                repo_name="presence",
                commit_message="feat: add reply quality scoring",
            )
        ],
        message_context=[
            SourceMessage(
                message_uuid="msg-1",
                project_path="/tmp/presence",
                prompt_text="Add scoring to classify replies before publication.",
            )
        ],
        published_url="https://x.com/taka/status/123",
    )

    result = expander.expand(candidate)

    client.messages.create.assert_called_once()
    call = client.messages.create.call_args.kwargs
    assert call["model"] == "claude-test"
    assert call["max_tokens"] == 3500
    prompt = call["messages"][0]["content"]
    assert "Original X thread" in prompt
    assert "Shipping the feature taught me" in prompt
    assert "feat: add reply quality scoring" in prompt
    assert "Add scoring to classify replies" in prompt
    assert "https://x.com/taka/status/123" in prompt
    assert result.source_id == 17
    assert result.content == "TITLE: The Blog Version\n\n## Context\n\nExpanded draft."
    assert result.generation_prompt == prompt


def test_build_prompt_lists_missing_source_metadata():
    expander = ThreadExpander(api_key="test-key", model="claude-test", client=MagicMock())
    candidate = ThreadExpansionCandidate(
        content_id=18,
        original_thread="Thread text",
        engagement_score=11.0,
        source_commits=["missing-sha"],
        source_messages=["missing-msg"],
    )

    prompt = expander.build_prompt(candidate)

    assert "missing-sha: (metadata unavailable)" in prompt
    assert "missing-msg: (metadata unavailable)" in prompt
