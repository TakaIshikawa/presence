# Presence

Autonomous content pipeline that ingests GitHub commits and Claude Code sessions, synthesizes social media posts (X posts/threads) and newsletters, and publishes them through a multi-stage quality gate. Designed to maintain an authentic developer presence from real work artifacts.

## Architecture

```
src/
├── config.py                  # YAML config loader (config.yaml / config.local.yaml)
├── runner.py                  # script_context() — shared Config + DB bootstrap
├── ingestion/
│   ├── github_commits.py      # GitHub API polling for new commits
│   └── claude_logs.py         # Claude Code session log parser
├── synthesis/
│   ├── pipeline.py            # 5-stage orchestrator (SynthesisPipeline)
│   ├── generator.py           # Multi-candidate LLM generation with format variation
│   ├── evaluator_v2.py        # Cross-model comparative evaluation (Opus judges Sonnet)
│   ├── refiner.py             # Guided refinement + final gate
│   ├── few_shot.py            # Engagement-weighted example selection
│   ├── stale_patterns.py      # Regex-based rhetorical pattern rejection
│   ├── theme_selector.py      # Theme/angle selection
│   ├── generator_enhanced.py  # Knowledge-augmented generation
│   └── prompts/               # Versioned prompt templates (x_post, x_thread)
├── evaluation/
│   ├── engagement_predictor.py # Predict engagement from content features
│   ├── engagement_scorer.py    # Score published posts by real metrics
│   └── validation_db.py        # Evaluation tracking DB
├── knowledge/
│   ├── store.py               # Semantic knowledge store (embeddings + search)
│   ├── embeddings.py          # Embedding provider abstraction
│   └── ingest.py              # Knowledge ingestion from curated sources
├── engagement/
│   ├── reply_drafter.py       # LLM-drafted replies to inbound mentions
│   ├── reply_evaluator.py     # Reply quality scoring and sycophancy detection
│   └── cultivate_bridge.py    # Relationship context enrichment (Cultivate integration)
├── output/
│   ├── x_client.py            # Tweepy-based X/Twitter publisher
│   ├── newsletter.py          # Buttondown newsletter assembly and delivery
│   └── blog_writer.py         # Static site blog post generation
└── storage/
    └── db.py                  # SQLite storage layer (Database class)

scripts/                       # Cron-driven entry points (macOS LaunchAgents)
├── poll_commits.py            # Ingest commits → synthesize → publish
├── daily_digest.py            # Daily content generation run
├── weekly_digest.py           # Weekly summary / thread generation
├── send_newsletter.py         # Assemble and send weekly newsletter
├── poll_replies.py            # Detect and draft replies to mentions
├── resolve_actions.py         # Resolve Cultivate strategic actions into reviewable items
├── review_proactive.py        # Proactive engagement review
├── retry_unpublished.py       # Re-attempt failed publishes
├── fetch_engagement.py        # Pull engagement metrics for published posts
├── fetch_curated.py           # Ingest content from curated X accounts/blogs
├── build_knowledge.py         # Build knowledge store from ingested content
├── backfill_embeddings.py     # One-time embedding backfill for semantic dedup
├── eval_pipeline.py           # Dry-run pipeline evaluation harness
├── update_operations_state.py # Sync run state to tact monitoring
├── manage.sh                  # LaunchAgent lifecycle manager (start/stop/status/logs)
└── curate.py                  # Manual curation CLI

tests/                         # pytest + in-memory SQLite fixtures (49 test files)
schema.sql                     # Database schema (15 tables)
config.yaml                    # Reference configuration
```

Data flows linearly: **ingest** (commits, sessions) → **synthesize** (generate, evaluate, refine) → **publish** (X, newsletter, blog). Engagement metrics feed back into few-shot selection and evaluation calibration.

## Key Features

**Multi-stage synthesis pipeline** — Candidates are generated with format variation (5 post formats, 5 thread hooks), evaluated comparatively by a stronger model (Opus judges Sonnet output), refined with targeted feedback, and gated before publish.

**3-layer deduplication** — Opening-clause similarity (SequenceMatcher), regex-based stale pattern rejection, and semantic embedding similarity against recent posts prevent repetitive content.

**Engagement feedback loop** — Real engagement metrics (likes, retweets, replies) are fetched back, posts are auto-classified as `resonated` or `low_resonance`, and these signals calibrate future evaluation and few-shot selection.

**Knowledge-augmented generation** — A semantic knowledge store (Voyage embeddings) enriches generation with insights from curated X accounts, blogs, and the author's own conversation history.

**Reply system** — Inbound mentions are detected, replies are drafted with relationship context from Cultivate integration, scored for quality (sycophancy detection, generic-reply filtering), and queued for review.

**Strategic engagement** — `resolve_actions.py` bridges Cultivate's relationship management: parses execution tags from strategic actions, fetches target tweets, pre-drafts reply/quote content, and writes resolved payloads back for review.

**Newsletter delivery** — Weekly Buttondown newsletters are auto-assembled from the week's published posts.

## API Overview

**`SynthesisPipeline.run(prompts, commits, content_type, threshold)`** — Main entry point. Executes the 5-stage pipeline (few-shot retrieval → multi-candidate generation → cross-model evaluation → guided refinement → final gate) and returns `PipelineResult`.

**`script_context()`** — Context manager yielding `(Config, Database)`. All scripts use this for consistent bootstrap:
```python
with script_context() as (config, db):
    # config: Config dataclass, db: connected Database
```

**`Database`** — SQLite storage with schema auto-migration. Key methods: `store_content()`, `mark_published()`, `get_recent_published_content()`, `get_unpublished_content()`, `get_curated_posts()`, `get_auto_classified_posts()`.

**`XClient.post() / post_thread()`** — Publish to X. **`NewsletterAssembler.assemble() / ButtondownSender.send()`** — Newsletter pipeline.

## Development

```bash
pip install -r requirements.txt

# Run tests
pytest

# Run with coverage
pytest --cov=src --cov-report=term-missing

# Single script run
python scripts/poll_commits.py
python scripts/daily_digest.py --dry-run

# Pipeline evaluation (dry run, no publish)
python scripts/eval_pipeline.py --runs 5 --type x_post

# Automation lifecycle (macOS LaunchAgents)
scripts/manage.sh start|stop|restart|status|logs
```

**Configuration**: Copy `config.yaml` to `config.local.yaml` and fill in API keys. Environment variables are resolved via `${ENV_VAR}` syntax in YAML values. Required services: GitHub API, X/Twitter API, Anthropic API. Optional: Voyage AI (embeddings), Buttondown (newsletter), Cultivate (relationship context).

**Database**: SQLite, auto-initialized from `schema.sql` on first `script_context()` call. Migrations are handled inline in `db.py`.

**Testing**: pytest with in-memory SQLite fixtures. Tests mock external APIs (Anthropic, X, GitHub). 49 test files covering storage, synthesis, engagement, newsletter, and script entry points.

## Direction

Recent work has focused on **operational hardening** — migrating all scripts to the `script_context()` pattern with structured logging, expanding test coverage (engagement prediction, stale patterns, newsletter assembly, storage edge cases), and adding pipeline run observability via `pipeline_runs` table.

Next areas:
- Pipeline evaluation harness (`eval_pipeline.py`) for systematic prompt/model tuning
- Semantic dedup coverage via embedding backfill
- Cultivate action resolution pipeline for strategic engagement
- Richer engagement signal integration into the evaluation stage
