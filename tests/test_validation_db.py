"""Comprehensive unit tests for ValidationDatabase class."""

import pytest

from src.evaluation.validation_db import ValidationDatabase


@pytest.fixture
def db():
    """In-memory ValidationDatabase with schema initialized."""
    vdb = ValidationDatabase(":memory:")
    vdb.connect()
    vdb.init_schema()
    yield vdb
    vdb.close()


def _create_account(db, user_id="1", username="testuser"):
    """Helper to create a test account."""
    db.upsert_account(
        user_id=user_id,
        username=username,
        display_name="Test User",
        bio="A test account",
        follower_count=5000,
        following_count=200,
        tweet_count=1000,
    )
    return db.get_account_by_user_id(user_id)


# --- Context Manager Tests ---


class TestContextManager:
    def test_context_manager_opens_and_closes_connection(self):
        vdb = ValidationDatabase(":memory:")
        assert vdb.conn is None

        with vdb as db:
            assert db.conn is not None
            db.init_schema()

        assert vdb.conn is None


# --- Prompt Version Operations Tests ---


class TestPromptVersionOperations:
    def test_register_prompt_version_is_deterministic(self, db):
        first = db.register_prompt_version("predict_engagement_v1", "Prompt text")
        second = db.register_prompt_version("predict_engagement_v1", "Prompt text")

        assert first["id"] == second["id"]
        assert first["version"] == 1
        assert second["usage_count"] == 2
        assert len(second["prompt_hash"]) == 64

    def test_insert_evaluation_stores_prompt_lineage(self, db):
        acct = _create_account(db)
        db.insert_tweet("t1", acct["id"], "tweet", 5, 1, 0, 0, 11.0, "2025-01-01")
        prompt = db.register_prompt_version("predict_engagement_v1", "Prompt text")

        db.insert_evaluation(
            tweet_id="t1",
            evaluator_version="v1",
            model="sonnet",
            predicted_score=7.0,
            hook_strength=7.0,
            specificity=7.0,
            emotional_resonance=7.0,
            novelty=7.0,
            actionability=7.0,
            raw_response="raw",
            prompt_type=prompt["prompt_type"],
            prompt_version=prompt["version"],
            prompt_hash=prompt["prompt_hash"],
        )

        row = db.conn.execute("SELECT * FROM evaluations WHERE tweet_id = 't1'").fetchone()
        assert row["prompt_type"] == "predict_engagement_v1"
        assert row["prompt_version"] == 1
        assert row["prompt_hash"] == prompt["prompt_hash"]


# --- Account Operations Tests ---


class TestAccountOperations:
    def test_upsert_account_insert(self, db):
        """Test inserting a new account."""
        row_id = db.upsert_account(
            user_id="user123",
            username="alice",
            display_name="Alice Smith",
            bio="Software engineer",
            follower_count=1500,
            following_count=300,
            tweet_count=450,
        )
        assert row_id > 0

        acct = db.get_account_by_user_id("user123")
        assert acct["username"] == "alice"
        assert acct["display_name"] == "Alice Smith"
        assert acct["bio"] == "Software engineer"
        assert acct["follower_count"] == 1500
        assert acct["following_count"] == 300
        assert acct["tweet_count"] == 450

    def test_upsert_account_update_on_conflict(self, db):
        """Test upserting updates existing account on user_id conflict."""
        db.upsert_account(
            user_id="user456",
            username="bob",
            display_name="Bob",
            bio="original",
            follower_count=100,
            following_count=50,
            tweet_count=25,
        )

        row_id = db.upsert_account(
            user_id="user456",
            username="bob_updated",
            display_name="Bob Updated",
            bio="updated bio",
            follower_count=200,
            following_count=75,
            tweet_count=40,
        )
        assert row_id > 0

        acct = db.get_account_by_user_id("user456")
        assert acct["username"] == "bob_updated"
        assert acct["display_name"] == "Bob Updated"
        assert acct["bio"] == "updated bio"
        assert acct["follower_count"] == 200

    def test_get_account_by_user_id_found(self, db):
        """Test retrieving an existing account by user_id."""
        _create_account(db, user_id="xyz", username="charlie")
        acct = db.get_account_by_user_id("xyz")
        assert acct is not None
        assert acct["user_id"] == "xyz"
        assert acct["username"] == "charlie"

    def test_get_account_by_user_id_not_found(self, db):
        """Test retrieving a non-existent account returns None."""
        acct = db.get_account_by_user_id("nonexistent")
        assert acct is None

    def test_get_all_accounts_ordered_by_username(self, db):
        """Test get_all_accounts returns accounts ordered by username."""
        _create_account(db, user_id="1", username="zebra")
        _create_account(db, user_id="2", username="apple")
        _create_account(db, user_id="3", username="mango")

        accounts = db.get_all_accounts()
        assert len(accounts) == 3
        assert accounts[0]["username"] == "apple"
        assert accounts[1]["username"] == "mango"
        assert accounts[2]["username"] == "zebra"

    def test_get_all_accounts_empty(self, db):
        """Test get_all_accounts returns empty list when no accounts exist."""
        accounts = db.get_all_accounts()
        assert accounts == []


# --- Tweet Operations Tests ---


class TestTweetOperations:
    def test_insert_tweet_success(self, db):
        """Test successful tweet insertion."""
        acct = _create_account(db)
        row_id = db.insert_tweet(
            tweet_id="tweet123",
            account_id=acct["id"],
            text="This is a test tweet",
            like_count=10,
            retweet_count=2,
            reply_count=1,
            quote_count=0,
            engagement_score=25.0,
            tweet_created_at="2025-01-15T12:00:00Z",
        )
        assert row_id is not None
        assert row_id > 0

    def test_insert_tweet_duplicate_returns_none(self, db):
        """Test inserting duplicate tweet_id returns None (IntegrityError path)."""
        acct = _create_account(db)
        kwargs = dict(
            tweet_id="tweet456",
            account_id=acct["id"],
            text="Original tweet",
            like_count=5,
            retweet_count=1,
            reply_count=0,
            quote_count=0,
            engagement_score=11.0,
            tweet_created_at="2025-01-10T10:00:00Z",
        )

        row_id1 = db.insert_tweet(**kwargs)
        row_id2 = db.insert_tweet(**kwargs)

        assert row_id1 is not None
        assert row_id2 is None

    def test_get_tweets_for_account_ordered_by_engagement_desc(self, db):
        """Test tweets returned in descending engagement_score order."""
        acct = _create_account(db)
        db.insert_tweet("t1", acct["id"], "low", 1, 0, 0, 0, 2.0, "2025-01-01")
        db.insert_tweet("t2", acct["id"], "high", 50, 10, 5, 2, 125.0, "2025-01-02")
        db.insert_tweet("t3", acct["id"], "mid", 10, 2, 1, 0, 25.0, "2025-01-03")

        tweets = db.get_tweets_for_account(acct["id"])
        assert len(tweets) == 3
        assert tweets[0]["tweet_id"] == "t2"
        assert tweets[0]["engagement_score"] == 125.0
        assert tweets[1]["tweet_id"] == "t3"
        assert tweets[1]["engagement_score"] == 25.0
        assert tweets[2]["tweet_id"] == "t1"
        assert tweets[2]["engagement_score"] == 2.0

    def test_get_tweets_for_account_empty(self, db):
        """Test get_tweets_for_account returns empty list for account with no tweets."""
        acct = _create_account(db)
        tweets = db.get_tweets_for_account(acct["id"])
        assert tweets == []

    def test_get_unevaluated_tweets_returns_unevaluated(self, db):
        """Test unevaluated tweets are returned for a given evaluator version."""
        acct = _create_account(db)
        db.insert_tweet("t1", acct["id"], "tweet one", 5, 1, 0, 0, 11.0, "2025-01-01")
        db.insert_tweet("t2", acct["id"], "tweet two", 3, 0, 1, 0, 7.0, "2025-01-02")

        uneval = db.get_unevaluated_tweets("v1", limit=500)
        assert len(uneval) == 2

    def test_get_unevaluated_tweets_excludes_evaluated(self, db):
        """Test tweets evaluated for a version are excluded from unevaluated."""
        acct = _create_account(db)
        db.insert_tweet("t1", acct["id"], "evaluated", 5, 1, 0, 0, 11.0, "2025-01-01")
        db.insert_tweet("t2", acct["id"], "not evaluated", 3, 0, 1, 0, 7.0, "2025-01-02")

        db.insert_evaluation(
            tweet_id="t1",
            evaluator_version="v1",
            model="test-model",
            predicted_score=6.0,
            hook_strength=6.0,
            specificity=5.0,
            emotional_resonance=5.0,
            novelty=5.0,
            actionability=5.0,
            raw_response="test",
        )

        uneval = db.get_unevaluated_tweets("v1")
        assert len(uneval) == 1
        assert uneval[0]["tweet_id"] == "t2"

    def test_get_unevaluated_tweets_version_scoped(self, db):
        """Test evaluations are version-scoped; v1 eval doesn't affect v2 query."""
        acct = _create_account(db)
        db.insert_tweet("t1", acct["id"], "tweet", 5, 1, 0, 0, 11.0, "2025-01-01")

        db.insert_evaluation(
            tweet_id="t1", evaluator_version="v1", model="m",
            predicted_score=6.0, hook_strength=6.0, specificity=5.0,
            emotional_resonance=5.0, novelty=5.0, actionability=5.0,
            raw_response="v1 eval",
        )

        uneval_v1 = db.get_unevaluated_tweets("v1")
        uneval_v2 = db.get_unevaluated_tweets("v2")

        assert len(uneval_v1) == 0
        assert len(uneval_v2) == 1

    def test_get_unevaluated_tweets_joins_account_fields(self, db):
        """Test unevaluated tweets include joined account fields."""
        acct = _create_account(
            db,
            user_id="u1",
            username="testuser123"
        )
        db.insert_tweet("t1", acct["id"], "test", 5, 1, 0, 0, 11.0, "2025-01-01")

        uneval = db.get_unevaluated_tweets("v1")
        assert len(uneval) == 1
        assert uneval[0]["username"] == "testuser123"
        assert uneval[0]["follower_count"] == 5000
        assert uneval[0]["bio"] == "A test account"

    def test_get_unevaluated_tweets_respects_limit(self, db):
        """Test limit parameter restricts number of returned tweets."""
        acct = _create_account(db)
        for i in range(10):
            db.insert_tweet(f"t{i}", acct["id"], f"tweet {i}", 1, 0, 0, 0, 2.0, "2025-01-01")

        uneval = db.get_unevaluated_tweets("v1", limit=5)
        assert len(uneval) == 5

    def test_get_purged_tweet_ids_returns_empty_text_tweets(self, db):
        """Test get_purged_tweet_ids returns tweet_ids where text is empty."""
        acct = _create_account(db)
        db.insert_tweet("t1", acct["id"], "has text", 5, 1, 0, 0, 11.0, "2025-01-01")
        db.insert_tweet("t2", acct["id"], "", 3, 0, 0, 0, 6.0, "2025-01-02")
        db.insert_tweet("t3", acct["id"], "", 2, 0, 0, 0, 4.0, "2025-01-03")

        purged_ids = db.get_purged_tweet_ids()
        assert len(purged_ids) == 2
        assert "t2" in purged_ids
        assert "t3" in purged_ids
        assert "t1" not in purged_ids

    def test_get_purged_tweet_ids_respects_limit(self, db):
        """Test limit parameter restricts number of returned purged tweet IDs."""
        acct = _create_account(db)
        for i in range(10):
            db.insert_tweet(f"t{i}", acct["id"], "", 1, 0, 0, 0, 2.0, "2025-01-01")

        purged_ids = db.get_purged_tweet_ids(limit=3)
        assert len(purged_ids) == 3

    def test_update_tweet_text_restores_purged_text(self, db):
        """Test update_tweet_text restores text for a purged tweet."""
        acct = _create_account(db)
        db.insert_tweet("t1", acct["id"], "original text", 5, 1, 0, 0, 11.0, "2025-01-01")
        db.purge_tweet_text()

        tweets = db.get_tweets_for_account(acct["id"])
        assert tweets[0]["text"] == ""

        db.update_tweet_text("t1", "restored text")

        tweets = db.get_tweets_for_account(acct["id"])
        assert tweets[0]["text"] == "restored text"

    def test_get_all_tweets_with_accounts_joins_account_fields(self, db):
        """Test get_all_tweets_with_accounts includes username and follower_count."""
        acct1 = _create_account(db, user_id="u1", username="alice")
        acct2 = _create_account(db, user_id="u2", username="bob")

        # Update bob's follower count to distinguish accounts
        db.upsert_account(
            user_id="u2",
            username="bob",
            display_name="Bob",
            bio="Bob's bio",
            follower_count=3000,
            following_count=100,
            tweet_count=500,
        )

        db.insert_tweet("t1", acct1["id"], "alice tweet", 10, 2, 1, 0, 27.0, "2025-01-01")
        db.insert_tweet("t2", acct2["id"], "bob tweet", 5, 1, 0, 0, 11.0, "2025-01-02")

        tweets = db.get_all_tweets_with_accounts()
        assert len(tweets) == 2

        alice_tweet = [t for t in tweets if t["tweet_id"] == "t1"][0]
        bob_tweet = [t for t in tweets if t["tweet_id"] == "t2"][0]

        assert alice_tweet["username"] == "alice"
        assert alice_tweet["follower_count"] == 5000
        assert bob_tweet["username"] == "bob"
        assert bob_tweet["follower_count"] == 3000

    def test_get_all_tweets_with_accounts_ordered_by_account_and_engagement(self, db):
        """Test tweets ordered by account_id then engagement_score DESC."""
        acct1 = _create_account(db, user_id="u1", username="alice")
        acct2 = _create_account(db, user_id="u2", username="bob")

        db.insert_tweet("t1", acct1["id"], "alice low", 1, 0, 0, 0, 2.0, "2025-01-01")
        db.insert_tweet("t2", acct1["id"], "alice high", 50, 10, 5, 0, 115.0, "2025-01-02")
        db.insert_tweet("t3", acct2["id"], "bob low", 2, 0, 0, 0, 4.0, "2025-01-03")
        db.insert_tweet("t4", acct2["id"], "bob high", 30, 5, 3, 0, 71.0, "2025-01-04")

        tweets = db.get_all_tweets_with_accounts()

        # Tweets should be grouped by account, with higher engagement first within each account
        assert tweets[0]["tweet_id"] == "t2"  # alice high
        assert tweets[1]["tweet_id"] == "t1"  # alice low
        assert tweets[2]["tweet_id"] == "t4"  # bob high
        assert tweets[3]["tweet_id"] == "t3"  # bob low


# --- Evaluation Operations Tests ---


class TestEvaluationOperations:
    def test_insert_evaluation_insert(self, db):
        """Test inserting a new evaluation."""
        acct = _create_account(db)
        db.insert_tweet("t1", acct["id"], "tweet", 5, 1, 0, 0, 11.0, "2025-01-01")

        row_id = db.insert_evaluation(
            tweet_id="t1",
            evaluator_version="v1",
            model="sonnet",
            predicted_score=7.5,
            hook_strength=8.0,
            specificity=7.0,
            emotional_resonance=6.5,
            novelty=7.5,
            actionability=6.0,
            raw_response="detailed response",
        )

        assert row_id > 0

    def test_insert_evaluation_upsert_on_conflict(self, db):
        """Test upserting updates existing evaluation on (tweet_id, version) conflict."""
        acct = _create_account(db)
        db.insert_tweet("t1", acct["id"], "tweet", 5, 1, 0, 0, 11.0, "2025-01-01")

        db.insert_evaluation(
            tweet_id="t1", evaluator_version="v1", model="sonnet",
            predicted_score=5.0, hook_strength=5.0, specificity=5.0,
            emotional_resonance=5.0, novelty=5.0, actionability=5.0,
            raw_response="first eval",
        )

        row_id = db.insert_evaluation(
            tweet_id="t1", evaluator_version="v1", model="opus",
            predicted_score=8.0, hook_strength=8.0, specificity=8.0,
            emotional_resonance=8.0, novelty=8.0, actionability=8.0,
            raw_response="second eval",
        )

        assert row_id > 0

        evals = db.get_evaluations_for_version("v1")
        assert len(evals) == 1
        assert evals[0]["predicted_score"] == 8.0
        assert evals[0]["model"] == "opus"
        assert evals[0]["raw_response"] == "second eval"

    def test_get_evaluations_for_version_joins_three_tables(self, db):
        """Test get_evaluations_for_version performs 3-table join correctly."""
        acct = _create_account(db, user_id="u1", username="alice")
        db.insert_tweet("t1", acct["id"], "great tweet", 20, 5, 3, 1, 54.0, "2025-01-01")

        db.insert_evaluation(
            tweet_id="t1", evaluator_version="v2", model="haiku",
            predicted_score=7.0, hook_strength=7.5, specificity=6.5,
            emotional_resonance=6.0, novelty=7.0, actionability=6.5,
            raw_response="evaluation text",
        )

        evals = db.get_evaluations_for_version("v2")
        assert len(evals) == 1

        eval_row = evals[0]
        # Evaluation fields
        assert eval_row["tweet_id"] == "t1"
        assert eval_row["evaluator_version"] == "v2"
        assert eval_row["model"] == "haiku"
        assert eval_row["predicted_score"] == 7.0
        # Tweet fields
        assert eval_row["text"] == "great tweet"
        assert eval_row["engagement_score"] == 54.0
        assert eval_row["like_count"] == 20
        assert eval_row["retweet_count"] == 5
        assert eval_row["reply_count"] == 3
        assert eval_row["quote_count"] == 1
        # Account fields
        assert eval_row["username"] == "alice"
        assert eval_row["follower_count"] == 5000

    def test_get_evaluations_for_version_filters_by_version(self, db):
        """Test only evaluations for specified version are returned."""
        acct = _create_account(db)
        db.insert_tweet("t1", acct["id"], "tweet one", 5, 1, 0, 0, 11.0, "2025-01-01")
        db.insert_tweet("t2", acct["id"], "tweet two", 3, 0, 1, 0, 7.0, "2025-01-02")

        db.insert_evaluation(
            tweet_id="t1", evaluator_version="v1", model="m",
            predicted_score=6.0, hook_strength=6.0, specificity=5.0,
            emotional_resonance=5.0, novelty=5.0, actionability=5.0,
            raw_response="v1",
        )
        db.insert_evaluation(
            tweet_id="t2", evaluator_version="v2", model="m",
            predicted_score=7.0, hook_strength=7.0, specificity=6.0,
            emotional_resonance=6.0, novelty=6.0, actionability=6.0,
            raw_response="v2",
        )

        evals_v1 = db.get_evaluations_for_version("v1")
        evals_v2 = db.get_evaluations_for_version("v2")

        assert len(evals_v1) == 1
        assert evals_v1[0]["tweet_id"] == "t1"
        assert len(evals_v2) == 1
        assert evals_v2[0]["tweet_id"] == "t2"


# --- Purge Operation Tests ---


class TestPurgeOperation:
    def test_purge_tweet_text_clears_all_non_empty_text(self, db):
        """Test purge_tweet_text clears all non-empty tweet text."""
        acct = _create_account(db)
        db.insert_tweet("t1", acct["id"], "tweet one", 5, 1, 0, 0, 11.0, "2025-01-01")
        db.insert_tweet("t2", acct["id"], "tweet two", 3, 0, 1, 0, 7.0, "2025-01-02")
        db.insert_tweet("t3", acct["id"], "tweet three", 10, 2, 2, 0, 28.0, "2025-01-03")

        rowcount = db.purge_tweet_text()
        assert rowcount == 3

        tweets = db.get_tweets_for_account(acct["id"])
        assert all(t["text"] == "" for t in tweets)

    def test_purge_tweet_text_returns_correct_rowcount(self, db):
        """Test purge returns number of rows updated."""
        acct = _create_account(db)
        db.insert_tweet("t1", acct["id"], "has text", 5, 1, 0, 0, 11.0, "2025-01-01")
        db.insert_tweet("t2", acct["id"], "also text", 3, 0, 1, 0, 7.0, "2025-01-02")

        rowcount = db.purge_tweet_text()
        assert rowcount == 2

    def test_purge_tweet_text_preserves_ids_and_metrics(self, db):
        """Test purge preserves tweet_id and engagement metrics."""
        acct = _create_account(db)
        db.insert_tweet("t1", acct["id"], "original", 5, 1, 0, 0, 11.0, "2025-01-01")

        db.purge_tweet_text()

        tweets = db.get_tweets_for_account(acct["id"])
        assert len(tweets) == 1
        assert tweets[0]["tweet_id"] == "t1"
        assert tweets[0]["text"] == ""
        assert tweets[0]["like_count"] == 5
        assert tweets[0]["retweet_count"] == 1
        assert tweets[0]["engagement_score"] == 11.0

    def test_purge_tweet_text_already_empty_rows_unaffected(self, db):
        """Test purge does not update rows where text is already empty."""
        acct = _create_account(db)
        db.insert_tweet("t1", acct["id"], "", 5, 1, 0, 0, 11.0, "2025-01-01")
        db.insert_tweet("t2", acct["id"], "", 3, 0, 1, 0, 7.0, "2025-01-02")

        rowcount = db.purge_tweet_text()
        assert rowcount == 0

    def test_purge_tweet_text_idempotent(self, db):
        """Test calling purge twice has no effect on second call."""
        acct = _create_account(db)
        db.insert_tweet("t1", acct["id"], "text", 5, 1, 0, 0, 11.0, "2025-01-01")

        first_purge = db.purge_tweet_text()
        second_purge = db.purge_tweet_text()

        assert first_purge == 1
        assert second_purge == 0


# --- Backtest Run Operations Tests ---


class TestBacktestRunOperations:
    def test_insert_backtest_run(self, db):
        """Test inserting a backtest run record."""
        row_id = db.insert_backtest_run(
            run_id="run123",
            evaluator_version="v1.0",
            model="sonnet-4.5",
            num_tweets=250,
            num_accounts=10,
            spearman_overall=0.42,
            spearman_within_account=0.48,
            pearson_log=0.35,
            top_quartile_precision=0.45,
            bottom_quartile_precision=0.52,
            notes="baseline run with default params",
        )

        assert row_id > 0

        cursor = db.conn.execute("SELECT * FROM backtest_runs WHERE run_id = ?", ("run123",))
        row = dict(cursor.fetchone())

        assert row["run_id"] == "run123"
        assert row["evaluator_version"] == "v1.0"
        assert row["model"] == "sonnet-4.5"
        assert row["num_tweets"] == 250
        assert row["num_accounts"] == 10
        assert row["spearman_overall"] == 0.42
        assert row["spearman_within_account"] == 0.48
        assert row["pearson_log"] == 0.35
        assert row["top_quartile_precision"] == 0.45
        assert row["bottom_quartile_precision"] == 0.52
        assert row["notes"] == "baseline run with default params"

    def test_insert_backtest_run_with_empty_notes(self, db):
        """Test inserting backtest run with empty notes string."""
        row_id = db.insert_backtest_run(
            run_id="run456",
            evaluator_version="v2.0",
            model="opus",
            num_tweets=100,
            num_accounts=5,
            spearman_overall=0.38,
            spearman_within_account=0.41,
            pearson_log=0.30,
            top_quartile_precision=0.40,
            bottom_quartile_precision=0.48,
            notes="",
        )

        assert row_id > 0

        cursor = db.conn.execute("SELECT notes FROM backtest_runs WHERE run_id = ?", ("run456",))
        row = dict(cursor.fetchone())
        assert row["notes"] == ""
