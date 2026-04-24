"""Tests for EnhancedContentGenerator — knowledge-enhanced content generation."""

import hashlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from synthesis.generator_enhanced import EnhancedContentGenerator, GeneratedContent
from knowledge.store import KnowledgeItem, KnowledgeSearchResult


@pytest.fixture
def mock_anthropic():
    """Mock anthropic module and client."""
    with patch("synthesis.generator_enhanced.anthropic") as mock_anthropic:
        mock_client = MagicMock()
        mock_anthropic.Anthropic.return_value = mock_client
        yield mock_anthropic, mock_client


@pytest.fixture
def mock_knowledge_store():
    """Mock KnowledgeStore."""
    store = MagicMock()
    store.search_similar.return_value = []
    return store


@pytest.fixture
def generator_no_store(mock_anthropic):
    """Generator without knowledge store."""
    _, mock_client = mock_anthropic
    return EnhancedContentGenerator(api_key="test-key", model="test-model")


@pytest.fixture
def generator_with_store(mock_anthropic, mock_knowledge_store):
    """Generator with knowledge store."""
    _, mock_client = mock_anthropic
    gen = EnhancedContentGenerator(
        api_key="test-key",
        knowledge_store=mock_knowledge_store,
        model="test-model"
    )
    return gen


def _mock_llm_response(client, content: str):
    """Set up mock LLM response."""
    mock_msg = MagicMock()
    mock_msg.content = [MagicMock(text=content)]
    client.messages.create.return_value = mock_msg


def _make_knowledge_item(
    source_type: str,
    content: str,
    author: str | None = None,
    insight: str | None = None,
    license: str = "attribution_required",
) -> KnowledgeItem:
    """Create a test KnowledgeItem."""
    return KnowledgeItem(
        id=1,
        source_type=source_type,
        source_id="test-id",
        source_url="https://example.com",
        author=author,
        content=content,
        insight=insight,
        embedding=[0.1, 0.2, 0.3],
        attribution_required=True,
        approved=True,
        created_at=None,
        license=license,
    )


# -- _format_insights tests ---------------------------------------------------


class TestFormatInsights:
    def test_empty_list_returns_none_available(self, generator_no_store):
        result = generator_no_store._format_insights([])
        assert result == "(none available)"

    def test_formats_item_with_author_and_insight(self, generator_no_store):
        item = _make_knowledge_item(
            source_type="curated_x",
            content="Full content here",
            author="alice",
            insight="Key insight extracted"
        )
        result = generator_no_store._format_insights([(item, 0.85)])
        assert result == "- [alice] Key insight extracted"

    def test_formats_item_without_author(self, generator_no_store):
        item = _make_knowledge_item(
            source_type="own_post",
            content="Post content",
            author=None,
            insight="The insight"
        )
        result = generator_no_store._format_insights([(item, 0.75)])
        assert result == "-  The insight"

    def test_formats_item_with_author_no_insight_uses_content_truncated(self, generator_no_store):
        long_content = "A" * 300
        item = _make_knowledge_item(
            source_type="curated_article",
            content=long_content,
            author="bob",
            insight=None
        )
        result = generator_no_store._format_insights([(item, 0.90)])
        assert result.startswith("- [bob] " + "A" * 200)
        assert len(result) == len("- [bob] ") + 200

    def test_formats_multiple_items(self, generator_no_store):
        items = [
            (_make_knowledge_item("own_post", "Content 1", None, "Insight 1"), 0.9),
            (_make_knowledge_item("curated_x", "Content 2", "alice", "Insight 2"), 0.8),
        ]
        result = generator_no_store._format_insights(items)
        lines = result.split("\n")
        assert len(lines) == 2
        assert lines[0] == "-  Insight 1"
        assert lines[1] == "- [alice] Insight 2"

    def test_formats_freshness_adjusted_result_metadata(self, generator_no_store):
        item = _make_knowledge_item(
            source_type="curated_x",
            content="Full content here",
            author="alice",
            insight="Fresh insight",
        )
        result = KnowledgeSearchResult(
            item=item,
            raw_similarity=0.76,
            combined_score=1.21,
            freshness_score=0.59,
        )

        formatted = generator_no_store._format_insights([result])

        assert formatted == (
            "- [alice] Fresh insight "
            "(freshness-adjusted relevance 1.21; semantic similarity 0.76)"
        )


# -- _retrieve_knowledge tests ------------------------------------------------


class TestRetrieveKnowledge:
    def test_returns_empty_when_no_store(self, generator_no_store):
        own, external = generator_no_store._retrieve_knowledge("test query")
        assert own == []
        assert external == []

    def test_calls_search_similar_for_own_insights(self, generator_with_store):
        own_item = _make_knowledge_item("own_post", "My post", "me", "My insight")
        generator_with_store.knowledge_store.search_similar.return_value = [(own_item, 0.8)]

        own, external = generator_with_store._retrieve_knowledge("test query", limit_own=3, limit_external=2)

        # First call for own insights
        calls = generator_with_store.knowledge_store.search_similar.call_args_list
        assert len(calls) == 2
        assert calls[0][0][0] == "test query"
        assert calls[0][1]["source_types"] == ["own_post", "own_conversation"]
        assert calls[0][1]["limit"] == 3
        assert calls[0][1]["min_similarity"] == 0.4

    def test_calls_search_similar_for_external_insights(self, generator_with_store):
        external_item = _make_knowledge_item("curated_x", "Tweet", "alice", "Good point")
        generator_with_store.knowledge_store.search_similar.side_effect = [
            [],  # own insights
            [(external_item, 0.9)]  # external insights
        ]

        own, external = generator_with_store._retrieve_knowledge("test query")

        # Second call for external insights
        calls = generator_with_store.knowledge_store.search_similar.call_args_list
        assert len(calls) == 2
        assert calls[1][0][0] == "test query"
        assert calls[1][1]["source_types"] == ["curated_x", "curated_article"]
        assert calls[1][1]["limit"] == 2
        assert calls[1][1]["min_similarity"] == 0.5

    def test_requests_freshness_adjusted_external_insights_when_enabled(
        self, mock_anthropic, mock_knowledge_store
    ):
        _, _ = mock_anthropic
        gen = EnhancedContentGenerator(
            api_key="test-key",
            knowledge_store=mock_knowledge_store,
            model="test-model",
            freshness_half_life_days=14,
        )

        gen._retrieve_knowledge("test query")

        calls = mock_knowledge_store.search_similar.call_args_list
        assert "freshness_half_life_days" not in calls[0][1]
        assert calls[1][1]["freshness_half_life_days"] == 14

    def test_passes_knowledge_diversity_caps_to_search(self, mock_anthropic, mock_knowledge_store):
        _, _ = mock_anthropic
        gen = EnhancedContentGenerator(
            api_key="test-key",
            knowledge_store=mock_knowledge_store,
            model="test-model",
            max_knowledge_per_author=1,
            max_knowledge_per_source_type=2,
        )

        gen._retrieve_knowledge("test query")

        for call in mock_knowledge_store.search_similar.call_args_list:
            assert call[1]["max_per_author"] == 1
            assert call[1]["max_per_source_type"] == 2

    def test_applies_diversity_caps_across_combined_prompt_context(
        self, mock_anthropic, mock_knowledge_store
    ):
        _, _ = mock_anthropic
        gen = EnhancedContentGenerator(
            api_key="test-key",
            knowledge_store=mock_knowledge_store,
            model="test-model",
            max_knowledge_per_author=1,
        )
        own_item = _make_knowledge_item("own_post", "Own", "alice")
        external_item = _make_knowledge_item("curated_x", "External", "alice")
        other_external_item = _make_knowledge_item("curated_x", "Other", "bob")
        mock_knowledge_store.search_similar.side_effect = [
            [(own_item, 0.9)],
            [(external_item, 0.8), (other_external_item, 0.7)],
        ]

        own, external = gen._retrieve_knowledge("test query")

        assert own == [(own_item, 0.9)]
        assert external == [(other_external_item, 0.7)]

    def test_returns_both_insight_types(self, generator_with_store):
        own_item = _make_knowledge_item("own_post", "My post", "me")
        external_item = _make_knowledge_item("curated_x", "Tweet", "alice")

        generator_with_store.knowledge_store.search_similar.side_effect = [
            [(own_item, 0.8)],
            [(external_item, 0.9)]
        ]

        own, external = generator_with_store._retrieve_knowledge("test query")

        assert len(own) == 1
        assert len(external) == 1
        assert own[0][0] == own_item
        assert external[0][0] == external_item

    def test_filters_restricted_external_insights_from_prompt_context(self, generator_with_store):
        attribution_item = _make_knowledge_item(
            "curated_x", "Attribution needed", "alice", "Allowed insight",
            license="attribution_required"
        )
        restricted_item = _make_knowledge_item(
            "curated_x", "Restricted", "bob", "Blocked insight",
            license="restricted"
        )
        generator_with_store.knowledge_store.search_similar.side_effect = [
            [],
            [(attribution_item, 0.8), (restricted_item, 0.9)]
        ]

        own, external = generator_with_store._retrieve_knowledge("test query")

        assert own == []
        assert external == [(attribution_item, 0.8)]

    def test_permissive_mode_allows_restricted_insights(self, mock_anthropic, mock_knowledge_store):
        _, _ = mock_anthropic
        gen = EnhancedContentGenerator(
            api_key="test-key",
            knowledge_store=mock_knowledge_store,
            model="test-model",
            restricted_prompt_behavior="permissive",
        )
        restricted_item = _make_knowledge_item(
            "curated_x", "Restricted", "bob", "Allowed in permissive",
            license="restricted"
        )
        mock_knowledge_store.search_similar.side_effect = [[], [(restricted_item, 0.9)]]

        _, external = gen._retrieve_knowledge("test query")

        assert external == [(restricted_item, 0.9)]


# -- _load_prompt tests -------------------------------------------------------


class TestLoadPrompt:
    def test_loads_basic_when_no_store(self, generator_no_store, tmp_path):
        # Create temp prompts dir with basic template
        prompts_dir = tmp_path / "prompts"
        prompts_dir.mkdir()
        basic_file = prompts_dir / "test_type.txt"
        basic_file.write_text("Basic prompt template")

        # Patch PROMPTS_DIR
        with patch.object(EnhancedContentGenerator, "PROMPTS_DIR", prompts_dir):
            result = generator_no_store._load_prompt("test_type")

        assert result == "Basic prompt template"

    def test_loads_basic_when_enhanced_missing(self, generator_with_store, tmp_path):
        prompts_dir = tmp_path / "prompts"
        prompts_dir.mkdir()
        basic_file = prompts_dir / "test_type.txt"
        basic_file.write_text("Basic template")
        # No enhanced file

        with patch.object(EnhancedContentGenerator, "PROMPTS_DIR", prompts_dir):
            result = generator_with_store._load_prompt("test_type")

        assert result == "Basic template"

    def test_loads_enhanced_when_store_and_file_exist(self, generator_with_store, tmp_path):
        prompts_dir = tmp_path / "prompts"
        prompts_dir.mkdir()
        basic_file = prompts_dir / "test_type.txt"
        basic_file.write_text("Basic template")
        enhanced_file = prompts_dir / "test_type_enhanced.txt"
        enhanced_file.write_text("Enhanced template with {own_insights}")

        with patch.object(EnhancedContentGenerator, "PROMPTS_DIR", prompts_dir):
            result = generator_with_store._load_prompt("test_type")

        assert result == "Enhanced template with {own_insights}"

    def test_prefers_enhanced_over_basic(self, generator_with_store, tmp_path):
        prompts_dir = tmp_path / "prompts"
        prompts_dir.mkdir()
        basic_file = prompts_dir / "x_post.txt"
        basic_file.write_text("Basic: {prompt} {commit_message}")
        enhanced_file = prompts_dir / "x_post_enhanced.txt"
        enhanced_file.write_text("Enhanced: {prompt} {own_insights}")

        with patch.object(EnhancedContentGenerator, "PROMPTS_DIR", prompts_dir):
            result = generator_with_store._load_prompt("x_post")

        assert "Enhanced:" in result
        assert "own_insights" in result

    def test_registers_enhanced_prompt_version_when_used(self, mock_anthropic, mock_knowledge_store, db, tmp_path):
        prompts_dir = tmp_path / "prompts"
        prompts_dir.mkdir()
        prompt_text = "Enhanced: {prompt} {own_insights}"
        (prompts_dir / "x_post_enhanced.txt").write_text(prompt_text)
        gen = EnhancedContentGenerator(
            api_key="test-key",
            knowledge_store=mock_knowledge_store,
            model="test-model",
            db=db,
        )

        with patch.object(EnhancedContentGenerator, "PROMPTS_DIR", prompts_dir):
            result = gen._load_prompt("x_post")

        prompt_hash = hashlib.sha256(prompt_text.encode("utf-8")).hexdigest()
        row = db.get_prompt_version("x_post_enhanced", prompt_hash)
        assert result == prompt_text
        assert row["version"] == 1
        assert row["usage_count"] == 1


# -- generate_x_post tests ----------------------------------------------------


class TestGenerateXPost:
    def test_generates_post_without_knowledge_store(self, generator_no_store, tmp_path):
        # Setup basic template
        prompts_dir = tmp_path / "prompts"
        prompts_dir.mkdir()
        template = prompts_dir / "x_post.txt"
        template.write_text("Prompt: {prompt}\nCommit: {commit_message}\nRepo: {repo_name}")

        _mock_llm_response(generator_no_store.client, "Generated post content")

        with patch.object(EnhancedContentGenerator, "PROMPTS_DIR", prompts_dir):
            result = generator_no_store.generate_x_post(
                prompt="Test prompt",
                commit_message="feat: add feature",
                repo_name="test-repo"
            )

        assert isinstance(result, GeneratedContent)
        assert result.content_type == "x_post"
        assert result.content == "Generated post content"
        assert result.source_prompts == ["Test prompt"]
        assert result.source_commits == ["feat: add feature"]
        assert result.knowledge_used == []
        assert result.attributions == []

    def test_includes_feedback_constraints_without_rejected_draft(
        self, mock_anthropic, db, tmp_path
    ):
        _, mock_client = mock_anthropic
        rejected_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Today's breakthrough: added retries and fixed queue failures.",
            eval_score=4.0,
            eval_feedback="",
        )
        db.add_content_feedback(rejected_id, "reject", "Too much like a changelog.")
        gen = EnhancedContentGenerator(api_key="test-key", model="test-model", db=db)
        prompts_dir = tmp_path / "prompts"
        prompts_dir.mkdir()
        (prompts_dir / "x_post.txt").write_text(
            "Prompt: {prompt}\nCommit: {commit_message}\nRepo: {repo_name}"
        )

        _mock_llm_response(mock_client, "Generated post")

        with patch.object(EnhancedContentGenerator, "PROMPTS_DIR", prompts_dir):
            gen.generate_x_post("prompt", "commit", "repo")

        prompt_text = mock_client.messages.create.call_args.kwargs["messages"][0]["content"]
        assert "RECENT USER FEEDBACK CONSTRAINTS" in prompt_text
        assert "Too much like a changelog." in prompt_text
        assert "Today's breakthrough" not in prompt_text

    def test_generates_post_with_knowledge_enhancement(self, generator_with_store, tmp_path):
        # Setup enhanced template
        prompts_dir = tmp_path / "prompts"
        prompts_dir.mkdir()
        template = prompts_dir / "x_post_enhanced.txt"
        template.write_text(
            "Prompt: {prompt}\nCommit: {commit_message}\nRepo: {repo_name}\n"
            "Own: {own_insights}\nExternal: {external_insights}"
        )

        own_item = _make_knowledge_item("own_post", "My content", "me", "My insight")
        external_item = _make_knowledge_item("curated_x", "Tweet", "alice", "Good point")

        generator_with_store.knowledge_store.search_similar.side_effect = [
            [(own_item, 0.8)],
            [(external_item, 0.9)]
        ]

        _mock_llm_response(generator_with_store.client, "Enhanced post")

        with patch.object(EnhancedContentGenerator, "PROMPTS_DIR", prompts_dir):
            result = generator_with_store.generate_x_post(
                prompt="Test prompt",
                commit_message="feat: add feature",
                repo_name="test-repo"
            )

        assert result.content == "Enhanced post"
        assert len(result.knowledge_used) == 2
        assert result.knowledge_used[0][0] == own_item
        assert result.knowledge_used[1][0] == external_item

    def test_extracts_attributions_from_curated_x(self, generator_with_store, tmp_path):
        prompts_dir = tmp_path / "prompts"
        prompts_dir.mkdir()
        template = prompts_dir / "x_post_enhanced.txt"
        template.write_text("Prompt: {prompt}\nOwn: {own_insights}\nExternal: {external_insights}")

        external_x = _make_knowledge_item("curated_x", "Tweet", "alice")
        external_article = _make_knowledge_item("curated_article", "Article", "bob")

        generator_with_store.knowledge_store.search_similar.side_effect = [
            [],  # no own insights
            [(external_x, 0.9), (external_article, 0.8)]
        ]

        _mock_llm_response(generator_with_store.client, "Post with attributions")

        with patch.object(EnhancedContentGenerator, "PROMPTS_DIR", prompts_dir):
            result = generator_with_store.generate_x_post(
                prompt="Test",
                commit_message="test",
                repo_name="test"
            )

        # curated_x should get @handle format, article should use plain author
        assert result.attributions == ["@alice", "bob"]

    def test_skips_attributions_for_items_without_author(self, generator_with_store, tmp_path):
        prompts_dir = tmp_path / "prompts"
        prompts_dir.mkdir()
        template = prompts_dir / "x_post_enhanced.txt"
        template.write_text("Prompt: {prompt}\nCommit: {commit_message}\nRepo: {repo_name}\nOwn: {own_insights}\nExternal: {external_insights}")

        no_author = _make_knowledge_item("curated_x", "Tweet", author=None)
        with_author = _make_knowledge_item("curated_x", "Tweet2", "alice")

        generator_with_store.knowledge_store.search_similar.side_effect = [
            [],
            [(no_author, 0.9), (with_author, 0.8)]
        ]

        _mock_llm_response(generator_with_store.client, "Post")

        with patch.object(EnhancedContentGenerator, "PROMPTS_DIR", prompts_dir):
            result = generator_with_store.generate_x_post(
                prompt="Test",
                commit_message="test",
                repo_name="test"
            )

        # Only alice should be in attributions
        assert result.attributions == ["@alice"]


# -- generate_x_thread tests --------------------------------------------------


class TestGenerateXThread:
    def test_generates_thread_without_knowledge(self, generator_no_store, tmp_path):
        prompts_dir = tmp_path / "prompts"
        prompts_dir.mkdir()
        template = prompts_dir / "x_thread.txt"
        template.write_text("Prompts: {prompts}\nCommits: {commits}")

        _mock_llm_response(generator_no_store.client, "Thread content")

        with patch.object(EnhancedContentGenerator, "PROMPTS_DIR", prompts_dir):
            result = generator_no_store.generate_x_thread(
                prompts=["Prompt 1", "Prompt 2"],
                commits=[
                    {"message": "feat: add A", "repo_name": "repo-a"},
                    {"message": "fix: bug B", "repo_name": "repo-b"}
                ]
            )

        assert result.content_type == "x_thread"
        assert result.content == "Thread content"
        assert result.source_prompts == ["Prompt 1", "Prompt 2"]
        assert result.source_commits == ["feat: add A", "fix: bug B"]

    def test_generates_thread_with_knowledge_enhancement(self, generator_with_store, tmp_path):
        prompts_dir = tmp_path / "prompts"
        prompts_dir.mkdir()
        template = prompts_dir / "x_thread_enhanced.txt"
        template.write_text(
            "Prompts: {prompts}\nCommits: {commits}\n"
            "Own: {own_insights}\nExternal: {external_insights}"
        )

        own_item = _make_knowledge_item("own_conversation", "Chat", "me")
        external_item = _make_knowledge_item("curated_article", "Article", "alice")

        generator_with_store.knowledge_store.search_similar.side_effect = [
            [(own_item, 0.8)],
            [(external_item, 0.9)]
        ]

        _mock_llm_response(generator_with_store.client, "Enhanced thread")

        with patch.object(EnhancedContentGenerator, "PROMPTS_DIR", prompts_dir):
            result = generator_with_store.generate_x_thread(
                prompts=["Prompt 1", "Prompt 2"],
                commits=[{"message": "feat: X", "repo_name": "repo"}]
            )

        assert result.content == "Enhanced thread"
        assert len(result.knowledge_used) == 2

    def test_uses_higher_limits_for_thread_knowledge_retrieval(self, generator_with_store, tmp_path):
        prompts_dir = tmp_path / "prompts"
        prompts_dir.mkdir()
        template = prompts_dir / "x_thread_enhanced.txt"
        template.write_text("Prompts: {prompts}\nOwn: {own_insights}")

        generator_with_store.knowledge_store.search_similar.return_value = []
        _mock_llm_response(generator_with_store.client, "Thread")

        with patch.object(EnhancedContentGenerator, "PROMPTS_DIR", prompts_dir):
            generator_with_store.generate_x_thread(
                prompts=["P1", "P2"],
                commits=[{"message": "M1", "repo_name": "R1"}]
            )

        # Check that limits are 5 for own, 3 for external
        calls = generator_with_store.knowledge_store.search_similar.call_args_list
        assert calls[0][1]["limit"] == 5  # limit_own
        assert calls[1][1]["limit"] == 3  # limit_external

    def test_builds_query_from_prompts_and_commits(self, generator_with_store, tmp_path):
        prompts_dir = tmp_path / "prompts"
        prompts_dir.mkdir()
        template = prompts_dir / "x_thread_enhanced.txt"
        template.write_text("Query: {prompts}\n{commits}\n{own_insights}")

        generator_with_store.knowledge_store.search_similar.return_value = []
        _mock_llm_response(generator_with_store.client, "Thread")

        with patch.object(EnhancedContentGenerator, "PROMPTS_DIR", prompts_dir):
            generator_with_store.generate_x_thread(
                prompts=["P1", "P2", "P3", "P4", "P5", "P6"],  # 6 prompts, only first 5 used
                commits=[
                    {"message": "M1", "repo_name": "R1"},
                    {"message": "M2", "repo_name": "R2"},
                    {"message": "M3", "repo_name": "R3"},
                    {"message": "M4", "repo_name": "R4"},
                    {"message": "M5", "repo_name": "R5"},
                    {"message": "M6", "repo_name": "R6"},  # 6 commits, only first 5 used
                ]
            )

        # Check that query was built from first 5 prompts and first 5 commits
        first_call_query = generator_with_store.knowledge_store.search_similar.call_args_list[0][0][0]
        query_lines = first_call_query.split("\n")
        assert len(query_lines) == 10  # 5 prompts + 5 commits

    def test_extracts_attributions_from_external_insights(self, generator_with_store, tmp_path):
        prompts_dir = tmp_path / "prompts"
        prompts_dir.mkdir()
        template = prompts_dir / "x_thread_enhanced.txt"
        template.write_text("Thread: {prompts}\n{commits}\n{own_insights}\n{external_insights}")

        ext1 = _make_knowledge_item("curated_x", "Tweet", "alice")
        ext2 = _make_knowledge_item("curated_article", "Article", "bob")
        ext3 = _make_knowledge_item("curated_x", "Tweet", None)  # No author

        generator_with_store.knowledge_store.search_similar.side_effect = [
            [],
            [(ext1, 0.9), (ext2, 0.8), (ext3, 0.7)]
        ]

        _mock_llm_response(generator_with_store.client, "Thread")

        with patch.object(EnhancedContentGenerator, "PROMPTS_DIR", prompts_dir):
            result = generator_with_store.generate_x_thread(
                prompts=["P1"],
                commits=[{"message": "M1", "repo_name": "R1"}]
            )

        assert result.attributions == ["@alice", "bob"]
