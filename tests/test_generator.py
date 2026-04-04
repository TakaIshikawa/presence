"""Tests for the content generator (synthesis/generator.py)."""

from unittest.mock import MagicMock, patch, call

import pytest

from synthesis.generator import ContentGenerator, GeneratedContent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_response(text: str) -> MagicMock:
    """Build a mock anthropic response with a single TextBlock."""
    response = MagicMock()
    response.content = [MagicMock(text=text)]
    return response


def _make_mock_response_empty() -> MagicMock:
    """Build a mock anthropic response with an empty content list."""
    response = MagicMock()
    response.content = []
    return response


SAMPLE_COMMITS = [
    {"repo_name": "relay", "message": "feat: add pipeline retry logic"},
    {"repo_name": "relay", "message": "fix: handle empty API response"},
    {"repo_name": "relay", "commit_message": "refactor: split evaluator module"},
]

SAMPLE_PROMPTS = [
    "Add retry logic to the pipeline runner",
    "Fix the edge case when API returns no data",
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_client():
    """Return a mock anthropic.Anthropic client instance."""
    return MagicMock()


@pytest.fixture
def generator(mock_client):
    """ContentGenerator with a mocked anthropic client."""
    with patch("synthesis.generator.anthropic.Anthropic", return_value=mock_client):
        gen = ContentGenerator(api_key="test-key")
    return gen


@pytest.fixture
def generator_with_client(mock_client):
    """Return (generator, mock_client) tuple for assertions on the client."""
    with patch("synthesis.generator.anthropic.Anthropic", return_value=mock_client):
        gen = ContentGenerator(api_key="test-key")
    return gen, mock_client


# ---------------------------------------------------------------------------
# 1. generate_candidates() creates exactly 3 candidates at [0.5, 0.7, 0.9]
# ---------------------------------------------------------------------------

class TestGenerateCandidates:
    def test_returns_three_candidates(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("post text")
        gen._load_prompt = MagicMock(
            return_value="{prompts}\n{commits}\n{commit_count}\n{few_shot_section}\n{format_directive}"
        )

        results = gen.generate_candidates(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        assert len(results) == 3
        assert client.messages.create.call_count == 3

    def test_temperatures_are_0_5_0_7_0_9(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("post text")
        gen._load_prompt = MagicMock(
            return_value="{prompts}\n{commits}\n{commit_count}\n{few_shot_section}\n{format_directive}"
        )

        gen.generate_candidates(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        temps = [c.kwargs["temperature"] for c in client.messages.create.call_args_list]
        assert temps == [0.5, 0.7, 0.9]

    def test_num_candidates_limits_output(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("post text")
        gen._load_prompt = MagicMock(
            return_value="{prompts}\n{commits}\n{commit_count}\n{few_shot_section}\n{format_directive}"
        )

        results = gen.generate_candidates(
            SAMPLE_PROMPTS, SAMPLE_COMMITS, num_candidates=2
        )

        assert len(results) == 2
        temps = [c.kwargs["temperature"] for c in client.messages.create.call_args_list]
        assert temps == [0.5, 0.7]

    def test_each_candidate_is_generated_content(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("  post text  ")
        gen._load_prompt = MagicMock(
            return_value="{prompts}\n{commits}\n{commit_count}\n{few_shot_section}\n{format_directive}"
        )

        results = gen.generate_candidates(
            SAMPLE_PROMPTS, SAMPLE_COMMITS, content_type="x_post"
        )

        for r in results:
            assert isinstance(r, GeneratedContent)
            assert r.content_type == "x_post"
            assert r.content == "post text"  # stripped


# ---------------------------------------------------------------------------
# 2. generate_x_post() respects 280-char token limit (max_tokens=500)
# ---------------------------------------------------------------------------

class TestGenerateXPost:
    def test_max_tokens_is_500(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("short post")
        gen._load_prompt = MagicMock(
            return_value="{prompt}\n{commit_message}\n{repo_name}"
        )

        gen.generate_x_post(
            prompt="test prompt",
            commit_message="feat: add feature",
            repo_name="relay",
        )

        call_kwargs = client.messages.create.call_args.kwargs
        assert call_kwargs["max_tokens"] == 500

    def test_returns_generated_content(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("  a great post  ")
        gen._load_prompt = MagicMock(
            return_value="{prompt}\n{commit_message}\n{repo_name}"
        )

        result = gen.generate_x_post(
            prompt="test prompt",
            commit_message="feat: add feature",
            repo_name="relay",
        )

        assert result.content_type == "x_post"
        assert result.content == "a great post"
        assert result.source_prompts == ["test prompt"]
        assert result.source_commits == ["feat: add feature"]


# ---------------------------------------------------------------------------
# 3. generate_x_thread() and generate_blog_post() use appropriate max_tokens
# ---------------------------------------------------------------------------

class TestMaxTokensByContentType:
    def test_x_thread_max_tokens_2000(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("TWEET 1:\nthread")
        gen._load_prompt = MagicMock(return_value="{prompts}\n{commits}")

        gen.generate_x_thread(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        call_kwargs = client.messages.create.call_args.kwargs
        assert call_kwargs["max_tokens"] == 2000

    def test_blog_post_max_tokens_4000(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("TITLE: Blog\n\nbody")
        gen._load_prompt = MagicMock(return_value="{prompts}\n{commits}")

        gen.generate_blog_post(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        call_kwargs = client.messages.create.call_args.kwargs
        assert call_kwargs["max_tokens"] == 4000

    def test_generate_candidates_x_post_max_tokens_150(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("post")
        gen._load_prompt = MagicMock(
            return_value="{prompts}\n{commits}\n{commit_count}\n{few_shot_section}\n{format_directive}"
        )

        gen.generate_candidates(SAMPLE_PROMPTS, SAMPLE_COMMITS, content_type="x_post")

        for c in client.messages.create.call_args_list:
            assert c.kwargs["max_tokens"] == 150

    def test_generate_candidates_x_thread_max_tokens_2000(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("thread")
        gen._load_prompt = MagicMock(
            return_value="{prompts}\n{commits}\n{commit_count}\n{few_shot_section}\n{format_directive}"
        )

        gen.generate_candidates(
            SAMPLE_PROMPTS, SAMPLE_COMMITS, content_type="x_thread"
        )

        for c in client.messages.create.call_args_list:
            assert c.kwargs["max_tokens"] == 2000

    def test_generate_candidates_blog_post_max_tokens_4000(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("blog")
        gen._load_prompt = MagicMock(
            return_value="{prompts}\n{commits}\n{commit_count}\n{few_shot_section}\n{format_directive}"
        )

        gen.generate_candidates(
            SAMPLE_PROMPTS, SAMPLE_COMMITS, content_type="blog_post"
        )

        for c in client.messages.create.call_args_list:
            assert c.kwargs["max_tokens"] == 4000

    def test_unknown_content_type_falls_back_to_x_post(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("post")
        gen._load_prompt = MagicMock(
            return_value="{prompts}\n{commits}\n{commit_count}\n{few_shot_section}\n{format_directive}"
        )

        gen.generate_candidates(
            SAMPLE_PROMPTS, SAMPLE_COMMITS, content_type="unknown_type"
        )

        for c in client.messages.create.call_args_list:
            assert c.kwargs["max_tokens"] == 150


# ---------------------------------------------------------------------------
# 4. condense() returns condensed text
# ---------------------------------------------------------------------------

class TestCondense:
    def test_returns_condensed_text(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("  short version  ")
        gen._load_prompt = MagicMock(return_value="{content}\n{char_count}")

        result = gen.condense("This is a very long post that needs shortening")

        assert result == "short version"

    def test_passes_content_and_char_count_to_template(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("short")
        gen._load_prompt = MagicMock(return_value="{content}\n{char_count}")

        content = "A post that is too long"
        gen.condense(content)

        filled = gen._load_prompt.return_value.format(
            content=content, char_count=len(content)
        )
        call_kwargs = client.messages.create.call_args.kwargs
        assert call_kwargs["messages"][0]["content"] == filled

    def test_condense_max_tokens_is_200(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("short")
        gen._load_prompt = MagicMock(return_value="{content}\n{char_count}")

        gen.condense("long text")

        call_kwargs = client.messages.create.call_args.kwargs
        assert call_kwargs["max_tokens"] == 200


# ---------------------------------------------------------------------------
# 5. Few-shot example injection
# ---------------------------------------------------------------------------

class TestFewShotExamples:
    def test_few_shot_examples_appear_in_prompt(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("post")
        gen._load_prompt = MagicMock(
            return_value="{prompts}\n{commits}\n{commit_count}\n{few_shot_section}\n{format_directive}"
        )

        examples = "Example 1: Great post about AI\nExample 2: Another banger"
        gen.generate_candidates(
            SAMPLE_PROMPTS, SAMPLE_COMMITS, few_shot_examples=examples
        )

        call_kwargs = client.messages.create.call_args.kwargs
        prompt_text = call_kwargs["messages"][0]["content"]
        assert "EXAMPLES OF CONTENT THAT RESONATED" in prompt_text
        assert "Example 1: Great post about AI" in prompt_text
        assert "Example 2: Another banger" in prompt_text
        assert "Match this quality level" in prompt_text

    def test_no_few_shot_section_when_empty(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("post")
        gen._load_prompt = MagicMock(
            return_value="{prompts}\n{commits}\n{commit_count}\n{few_shot_section}\n{format_directive}"
        )

        gen.generate_candidates(SAMPLE_PROMPTS, SAMPLE_COMMITS, few_shot_examples="")

        call_kwargs = client.messages.create.call_args.kwargs
        prompt_text = call_kwargs["messages"][0]["content"]
        assert "EXAMPLES OF CONTENT THAT RESONATED" not in prompt_text


# ---------------------------------------------------------------------------
# 6. Format directives are correctly applied to prompts
# ---------------------------------------------------------------------------

class TestFormatDirectives:
    def test_format_directives_applied_per_candidate(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("post")
        gen._load_prompt = MagicMock(
            return_value="{prompts}\n{commits}\n{commit_count}\n{few_shot_section}\n{format_directive}"
        )

        directives = [
            "Use a question hook",
            "Start with a bold claim",
            "Open with a surprising stat",
        ]
        gen.generate_candidates(
            SAMPLE_PROMPTS, SAMPLE_COMMITS, format_directives=directives
        )

        calls = client.messages.create.call_args_list
        for i, directive in enumerate(directives):
            prompt_text = calls[i].kwargs["messages"][0]["content"]
            assert directive in prompt_text

    def test_no_format_directive_when_none(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("post")
        gen._load_prompt = MagicMock(
            return_value="[{format_directive}]\n{prompts}\n{commits}\n{commit_count}\n{few_shot_section}"
        )

        gen.generate_candidates(
            SAMPLE_PROMPTS, SAMPLE_COMMITS, format_directives=None
        )

        for c in client.messages.create.call_args_list:
            prompt_text = c.kwargs["messages"][0]["content"]
            assert prompt_text.startswith("[]\n")

    def test_fewer_directives_than_candidates(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("post")
        gen._load_prompt = MagicMock(
            return_value="[{format_directive}]\n{prompts}\n{commits}\n{commit_count}\n{few_shot_section}"
        )

        directives = ["Use a question hook"]
        gen.generate_candidates(
            SAMPLE_PROMPTS, SAMPLE_COMMITS, format_directives=directives
        )

        calls = client.messages.create.call_args_list
        # First candidate gets the directive
        assert "[Use a question hook]" in calls[0].kwargs["messages"][0]["content"]
        # Remaining candidates get empty directive
        assert calls[1].kwargs["messages"][0]["content"].startswith("[]\n")
        assert calls[2].kwargs["messages"][0]["content"].startswith("[]\n")


# ---------------------------------------------------------------------------
# 7. GeneratedContent dataclass fields populated correctly
# ---------------------------------------------------------------------------

class TestGeneratedContentFields:
    def test_x_post_fields(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("post content")
        gen._load_prompt = MagicMock(
            return_value="{prompt}\n{commit_message}\n{repo_name}"
        )

        result = gen.generate_x_post(
            prompt="my prompt",
            commit_message="feat: new feature",
            repo_name="my-repo",
        )

        assert result.content == "post content"
        assert result.content_type == "x_post"
        assert result.source_prompts == ["my prompt"]
        assert result.source_commits == ["feat: new feature"]

    def test_x_thread_fields(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("TWEET 1:\ntweet")
        gen._load_prompt = MagicMock(return_value="{prompts}\n{commits}")

        result = gen.generate_x_thread(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        assert result.content_type == "x_thread"
        assert result.source_prompts == SAMPLE_PROMPTS
        assert result.source_commits == [
            "feat: add pipeline retry logic",
            "fix: handle empty API response",
            "refactor: split evaluator module",
        ]

    def test_blog_post_fields(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("TITLE: Blog\nbody")
        gen._load_prompt = MagicMock(return_value="{prompts}\n{commits}")

        result = gen.generate_blog_post(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        assert result.content_type == "blog_post"
        assert result.source_prompts == SAMPLE_PROMPTS

    def test_generate_candidates_fields(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("candidate text")
        gen._load_prompt = MagicMock(
            return_value="{prompts}\n{commits}\n{commit_count}\n{few_shot_section}\n{format_directive}"
        )

        results = gen.generate_candidates(
            SAMPLE_PROMPTS, SAMPLE_COMMITS, content_type="x_post"
        )

        for r in results:
            assert r.content_type == "x_post"
            assert r.source_prompts == SAMPLE_PROMPTS
            assert r.source_commits == [
                "feat: add pipeline retry logic",
                "fix: handle empty API response",
                "refactor: split evaluator module",
            ]

    def test_batched_post_fields(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("batched post")
        gen._load_prompt = MagicMock(
            return_value="{prompts}\n{commits}\n{commit_count}"
        )

        # generate_x_post_batched requires 'repo_name' and 'message' keys
        commits = [
            {"repo_name": "relay", "message": "feat: add pipeline retry logic"},
            {"repo_name": "relay", "message": "fix: handle empty API response"},
        ]
        result = gen.generate_x_post_batched(SAMPLE_PROMPTS, commits)

        assert result.content_type == "x_post"
        assert result.source_prompts == SAMPLE_PROMPTS
        assert result.source_commits == [c["message"] for c in commits]

    def test_content_is_stripped(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response("\n  padded text  \n")
        gen._load_prompt = MagicMock(
            return_value="{prompt}\n{commit_message}\n{repo_name}"
        )

        result = gen.generate_x_post(
            prompt="p", commit_message="c", repo_name="r"
        )

        assert result.content == "padded text"


# ---------------------------------------------------------------------------
# 8. Error handling when API returns empty response
# ---------------------------------------------------------------------------

class TestErrorHandling:
    def test_empty_content_list_raises(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response_empty()
        gen._load_prompt = MagicMock(
            return_value="{prompt}\n{commit_message}\n{repo_name}"
        )

        with pytest.raises(IndexError):
            gen.generate_x_post(
                prompt="p", commit_message="c", repo_name="r"
            )

    def test_empty_content_in_generate_candidates_raises(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response_empty()
        gen._load_prompt = MagicMock(
            return_value="{prompts}\n{commits}\n{commit_count}\n{few_shot_section}\n{format_directive}"
        )

        with pytest.raises(IndexError):
            gen.generate_candidates(SAMPLE_PROMPTS, SAMPLE_COMMITS)

    def test_empty_content_in_condense_raises(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.return_value = _make_mock_response_empty()
        gen._load_prompt = MagicMock(return_value="{content}\n{char_count}")

        with pytest.raises(IndexError):
            gen.condense("some text")

    def test_api_exception_propagates(self, generator_with_client):
        gen, client = generator_with_client
        client.messages.create.side_effect = Exception("API timeout")
        gen._load_prompt = MagicMock(
            return_value="{prompt}\n{commit_message}\n{repo_name}"
        )

        with pytest.raises(Exception, match="API timeout"):
            gen.generate_x_post(
                prompt="p", commit_message="c", repo_name="r"
            )


# ---------------------------------------------------------------------------
# Constructor and config
# ---------------------------------------------------------------------------

class TestConstructor:
    def test_client_initialized_with_api_key_and_timeout(self):
        with patch("synthesis.generator.anthropic.Anthropic") as mock_anthropic:
            ContentGenerator(api_key="sk-test-123")
            mock_anthropic.assert_called_once_with(api_key="sk-test-123", timeout=300.0)

    def test_default_model(self):
        with patch("synthesis.generator.anthropic.Anthropic"):
            gen = ContentGenerator(api_key="test")
            assert gen.model == "claude-sonnet-4-20250514"

    def test_custom_model(self):
        with patch("synthesis.generator.anthropic.Anthropic"):
            gen = ContentGenerator(api_key="test", model="claude-haiku-4-5-20251001")
            assert gen.model == "claude-haiku-4-5-20251001"

    def test_content_type_config_entries(self):
        assert "x_post" in ContentGenerator.CONTENT_TYPE_CONFIG
        assert "x_thread" in ContentGenerator.CONTENT_TYPE_CONFIG
        assert "blog_post" in ContentGenerator.CONTENT_TYPE_CONFIG
        assert ContentGenerator.CONTENT_TYPE_CONFIG["x_post"]["max_tokens"] == 150
        assert ContentGenerator.CONTENT_TYPE_CONFIG["x_thread"]["max_tokens"] == 2000
        assert ContentGenerator.CONTENT_TYPE_CONFIG["blog_post"]["max_tokens"] == 4000
