-- Presence Database Schema

-- Track processed Claude Code sessions/messages
CREATE TABLE IF NOT EXISTS claude_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    message_uuid TEXT UNIQUE NOT NULL,
    project_path TEXT,
    timestamp TEXT NOT NULL,
    prompt_text TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Track processed GitHub commits
CREATE TABLE IF NOT EXISTS github_commits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_name TEXT NOT NULL,
    commit_sha TEXT UNIQUE NOT NULL,
    commit_message TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    author TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Track processed GitHub issues and pull requests
CREATE TABLE IF NOT EXISTS github_activity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_name TEXT NOT NULL,
    activity_type TEXT NOT NULL,  -- 'issue' or 'pull_request'
    number INTEGER NOT NULL,
    title TEXT NOT NULL,
    body TEXT,
    state TEXT,
    author TEXT,
    url TEXT,
    updated_at TEXT NOT NULL,
    created_at_github TEXT,
    closed_at TEXT,
    merged_at TEXT,
    labels TEXT,  -- JSON array of label names
    metadata JSON,
    ingested_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(repo_name, activity_type, number)
);

-- Link commits to Claude messages (same time window = same work session)
CREATE TABLE IF NOT EXISTS commit_prompt_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    commit_id INTEGER REFERENCES github_commits(id),
    message_id INTEGER REFERENCES claude_messages(id),
    confidence REAL,  -- how confident we are these are related
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Generated content
-- published: 0 = unpublished, 1 = published, -1 = abandoned (max retries exceeded)
CREATE TABLE IF NOT EXISTS generated_content (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_type TEXT NOT NULL,  -- 'x_post', 'x_thread', 'blog_post'
    source_commits TEXT,  -- JSON array of commit SHAs
    source_messages TEXT,  -- JSON array of message UUIDs
    source_activity_ids TEXT,  -- JSON array of GitHub issue/PR activity IDs
    content TEXT NOT NULL,
    eval_score REAL,
    eval_feedback TEXT,
    published INTEGER DEFAULT 0,
    published_url TEXT,
    tweet_id TEXT,
    published_at TEXT,
    retry_count INTEGER DEFAULT 0,
    last_retry_at TEXT,
    curation_quality TEXT,  -- 'good', 'too_specific', or NULL (unreviewed)
    auto_quality TEXT,      -- 'resonated', 'low_resonance', or NULL (too young)
    content_embedding BLOB, -- serialized embedding vector for semantic dedup
    content_format TEXT,    -- format used for generation: 'micro_story', 'question', 'contrarian', 'tip', 'observation', 'mid_action', 'bold_claim', 'question_hook', 'surprising_result', 'contrarian_thread'
    image_path TEXT,        -- local path to generated image (for x_visual posts)
    image_prompt TEXT,      -- prompt/description used to generate the image
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Durable platform-specific copy variants for generated content reuse
CREATE TABLE IF NOT EXISTS content_variants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES generated_content(id),
    platform TEXT NOT NULL,      -- 'x', 'bluesky', 'linkedin', 'newsletter', 'blog'
    variant_type TEXT NOT NULL,  -- e.g. 'post', 'thread', 'summary', 'seed'
    content TEXT NOT NULL,
    metadata JSON,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(content_id, platform, variant_type)
);
CREATE INDEX IF NOT EXISTS idx_content_variants_content ON content_variants(content_id);

-- Track engagement metrics for published posts (time-series)
CREATE TABLE IF NOT EXISTS post_engagement (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES generated_content(id),
    tweet_id TEXT NOT NULL,
    like_count INTEGER DEFAULT 0,
    retweet_count INTEGER DEFAULT 0,
    reply_count INTEGER DEFAULT 0,
    quote_count INTEGER DEFAULT 0,
    engagement_score REAL,
    fetched_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Track Bluesky engagement metrics (time-series)
CREATE TABLE IF NOT EXISTS bluesky_engagement (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES generated_content(id),
    bluesky_uri TEXT NOT NULL,
    like_count INTEGER DEFAULT 0,
    repost_count INTEGER DEFAULT 0,
    reply_count INTEGER DEFAULT 0,
    quote_count INTEGER DEFAULT 0,
    engagement_score REAL,
    fetched_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Durable per-platform publication state
CREATE TABLE IF NOT EXISTS content_publications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES generated_content(id),
    platform TEXT NOT NULL,              -- 'x', 'bluesky'
    status TEXT NOT NULL DEFAULT 'queued', -- 'queued', 'published', 'failed'
    platform_post_id TEXT,               -- tweet ID, AT URI, or platform-native ID
    platform_url TEXT,
    error TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    last_error_at TEXT,
    published_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(content_id, platform)
);
CREATE INDEX IF NOT EXISTS idx_content_publications_content ON content_publications(content_id);
CREATE INDEX IF NOT EXISTS idx_content_publications_platform_status ON content_publications(platform, status);
CREATE INDEX IF NOT EXISTS idx_content_publications_retry ON content_publications(status, next_retry_at);

-- Track engagement predictions from EngagementPredictor
CREATE TABLE IF NOT EXISTS engagement_predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER REFERENCES generated_content(id),
    predicted_score REAL NOT NULL,
    hook_strength REAL,
    specificity REAL,
    emotional_resonance REAL,
    novelty REAL,
    actionability REAL,
    prompt_type TEXT,
    prompt_version TEXT,
    prompt_hash TEXT,
    actual_engagement_score REAL,  -- backfilled from post_engagement
    prediction_error REAL,         -- actual - predicted, backfilled
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_predictions_content ON engagement_predictions(content_id);

-- Prompt versions for eval tracking
CREATE TABLE IF NOT EXISTS prompt_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prompt_type TEXT NOT NULL,  -- 'x_post', 'x_thread', 'blog_post'
    version INTEGER NOT NULL,
    prompt_hash TEXT NOT NULL,
    prompt_text TEXT NOT NULL,
    avg_score REAL,
    usage_count INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(prompt_type, version),
    UNIQUE(prompt_type, prompt_hash)
);

-- Poll state tracking
CREATE TABLE IF NOT EXISTS poll_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),  -- singleton row
    last_poll_time TEXT NOT NULL,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Knowledge items with embeddings for semantic search
CREATE TABLE IF NOT EXISTS knowledge (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL,  -- 'own_post', 'own_conversation', 'curated_x', 'curated_article'
    source_id TEXT,             -- original ID (tweet ID, message UUID, etc.)
    source_url TEXT,
    author TEXT,
    content TEXT NOT NULL,
    insight TEXT,               -- extracted insight/summary
    embedding BLOB,             -- serialized embedding vector
    attribution_required INTEGER DEFAULT 1,
    license TEXT DEFAULT 'attribution_required',  -- 'open', 'attribution_required', 'restricted'
    approved INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_type, source_id)
);

-- Curated sources (approved accounts/blogs + discovered candidates)
CREATE TABLE IF NOT EXISTS curated_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL,  -- 'x_account', 'blog', 'newsletter'
    identifier TEXT NOT NULL,   -- @username or domain
    name TEXT,
    license TEXT DEFAULT 'attribution_required',  -- 'open', 'attribution_required', 'restricted'
    feed_url TEXT,
    feed_etag TEXT,
    feed_last_modified TEXT,
    notes TEXT,
    active INTEGER DEFAULT 1,
    status TEXT DEFAULT 'active',         -- 'candidate', 'active', 'rejected', 'paused'
    discovery_source TEXT,                -- 'config', 'proactive_mining', 'search'
    relevance_score REAL,                 -- avg semantic similarity at discovery
    sample_count INTEGER DEFAULT 0,       -- tweets sampled for relevance scoring
    reviewed_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_type, identifier)
);
-- idx_curated_sources_status is created in db.py migration block
-- (must run after ALTER TABLE adds the status column on existing DBs)

-- Track which knowledge items were used in generated content
CREATE TABLE IF NOT EXISTS content_knowledge_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER REFERENCES generated_content(id),
    knowledge_id INTEGER REFERENCES knowledge(id),
    relevance_score REAL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Track pipeline runs for observability
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT UNIQUE NOT NULL,
    content_type TEXT NOT NULL,
    candidates_generated INTEGER,
    best_candidate_index INTEGER,
    best_score_before_refine REAL,
    best_score_after_refine REAL,
    refinement_picked TEXT,  -- 'REFINED', 'ORIGINAL', or NULL if skipped
    final_score REAL,
    published INTEGER DEFAULT 0,
    content_id INTEGER REFERENCES generated_content(id),
    outcome TEXT,           -- 'published', 'below_threshold', 'all_filtered', 'dry_run'
    rejection_reason TEXT,  -- human-readable reason when not published
    filter_stats TEXT,  -- JSON: rejection counts per filter stage
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_knowledge_source_type ON knowledge(source_type);
CREATE INDEX IF NOT EXISTS idx_knowledge_author ON knowledge(author);
CREATE INDEX IF NOT EXISTS idx_curated_sources_type ON curated_sources(source_type);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_claude_messages_session ON claude_messages(session_id);
CREATE INDEX IF NOT EXISTS idx_claude_messages_timestamp ON claude_messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_github_commits_repo ON github_commits(repo_name);
CREATE INDEX IF NOT EXISTS idx_github_commits_timestamp ON github_commits(timestamp);
CREATE INDEX IF NOT EXISTS idx_github_activity_repo_type ON github_activity(repo_name, activity_type);
CREATE INDEX IF NOT EXISTS idx_github_activity_updated ON github_activity(updated_at);
CREATE INDEX IF NOT EXISTS idx_generated_content_type ON generated_content(content_type);
CREATE INDEX IF NOT EXISTS idx_post_engagement_content ON post_engagement(content_id);
CREATE INDEX IF NOT EXISTS idx_post_engagement_tweet ON post_engagement(tweet_id);
CREATE INDEX IF NOT EXISTS idx_bluesky_engagement_content ON bluesky_engagement(content_id);
CREATE INDEX IF NOT EXISTS idx_bluesky_engagement_uri ON bluesky_engagement(bluesky_uri);

-- Newsletter send tracking
CREATE TABLE IF NOT EXISTS newsletter_sends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id TEXT,
    subject TEXT NOT NULL,
    source_content_ids TEXT,   -- JSON array of generated_content IDs
    subscriber_count INTEGER,
    status TEXT DEFAULT 'sent',
    metadata JSON,
    sent_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_newsletter_sends_sent_at ON newsletter_sends(sent_at);

-- Store evaluated newsletter subject candidates before delivery
CREATE TABLE IF NOT EXISTS newsletter_subject_candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    newsletter_send_id INTEGER REFERENCES newsletter_sends(id),
    issue_id TEXT,
    subject TEXT NOT NULL,
    score REAL NOT NULL,
    rationale TEXT,
    source TEXT DEFAULT 'heuristic',
    rank INTEGER,
    selected INTEGER DEFAULT 0,
    source_content_ids TEXT,
    week_start TEXT,
    week_end TEXT,
    metadata JSON,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_newsletter_subject_candidates_send
    ON newsletter_subject_candidates(newsletter_send_id);
CREATE INDEX IF NOT EXISTS idx_newsletter_subject_candidates_created
    ON newsletter_subject_candidates(created_at);

-- Track newsletter engagement metrics from Buttondown (time-series)
CREATE TABLE IF NOT EXISTS newsletter_engagement (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    newsletter_send_id INTEGER REFERENCES newsletter_sends(id),
    issue_id TEXT NOT NULL,
    opens INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    unsubscribes INTEGER DEFAULT 0,
    fetched_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_newsletter_engagement_send ON newsletter_engagement(newsletter_send_id);
CREATE INDEX IF NOT EXISTS idx_newsletter_engagement_issue ON newsletter_engagement(issue_id);

-- Track aggregate newsletter subscriber metrics from Buttondown (time-series)
CREATE TABLE IF NOT EXISTS newsletter_subscriber_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subscriber_count INTEGER NOT NULL DEFAULT 0,
    active_subscriber_count INTEGER,
    unsubscribes INTEGER,
    churn_rate REAL,
    new_subscribers INTEGER,
    net_subscriber_change INTEGER,
    raw_metrics JSON,
    fetched_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_newsletter_subscriber_metrics_fetched
    ON newsletter_subscriber_metrics(fetched_at);

-- Reply queue for reply-to-reply engagement
CREATE TABLE IF NOT EXISTS reply_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inbound_tweet_id TEXT UNIQUE NOT NULL,  -- their reply tweet ID
    platform TEXT DEFAULT 'x',              -- x | bluesky
    inbound_author_handle TEXT,
    inbound_author_id TEXT,
    inbound_text TEXT NOT NULL,
    our_tweet_id TEXT NOT NULL,              -- which of our posts they replied to
    inbound_url TEXT,
    inbound_cid TEXT,
    our_platform_id TEXT,
    platform_metadata TEXT,                  -- JSON: platform-specific reply context
    our_content_id INTEGER REFERENCES generated_content(id),
    our_post_text TEXT,                      -- our original post content
    draft_text TEXT,                         -- Claude-drafted reply
    intent TEXT DEFAULT 'other',             -- question | appreciation | disagreement | bug_report | spam | other
    priority TEXT DEFAULT 'normal',          -- high | normal | low
    relationship_context TEXT,               -- JSON: cultivate enrichment {stage, tier, strength, ...}
    quality_score REAL,                      -- Reply quality evaluation score (0-10)
    quality_flags TEXT,                      -- JSON array of flags: ["sycophantic", "generic", ...]
    status TEXT DEFAULT 'pending',           -- pending | approved | posted | dismissed
    posted_tweet_id TEXT,                    -- our reply's tweet ID after posting
    detected_at TEXT DEFAULT CURRENT_TIMESTAMP,
    reviewed_at TEXT,
    posted_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_reply_queue_status ON reply_queue(status);
CREATE INDEX IF NOT EXISTS idx_reply_queue_inbound ON reply_queue(inbound_tweet_id);

-- Track which knowledge items were used in reply drafts
CREATE TABLE IF NOT EXISTS reply_knowledge_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reply_queue_id INTEGER REFERENCES reply_queue(id),
    knowledge_id INTEGER REFERENCES knowledge(id),
    relevance_score REAL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_reply_knowledge_links_reply ON reply_knowledge_links(reply_queue_id);
CREATE INDEX IF NOT EXISTS idx_reply_knowledge_links_knowledge ON reply_knowledge_links(knowledge_id);

-- Reply poll state tracking (singleton)
CREATE TABLE IF NOT EXISTS reply_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_mention_id TEXT,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS platform_reply_state (
    platform TEXT PRIMARY KEY,
    cursor TEXT,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Key-value store for cached analytics (pattern analysis, trend themes, etc.)
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Content topic tracking
CREATE TABLE IF NOT EXISTS content_topics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER REFERENCES generated_content(id),
    topic TEXT NOT NULL,           -- extracted topic label (e.g., 'testing', 'architecture', 'ai-agents')
    subtopic TEXT,                 -- more specific subtopic
    confidence REAL DEFAULT 1.0,   -- extraction confidence
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_content_topics_topic ON content_topics(topic);
CREATE INDEX IF NOT EXISTS idx_content_topics_content ON content_topics(content_id);

-- Planned topics for forward-looking content calendar
CREATE TABLE IF NOT EXISTS content_campaigns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    goal TEXT,
    start_date TEXT,
    end_date TEXT,
    daily_limit INTEGER,
    weekly_limit INTEGER,
    status TEXT DEFAULT 'planned', -- 'planned', 'active', 'completed', 'paused'
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_content_campaigns_status ON content_campaigns(status);
CREATE INDEX IF NOT EXISTS idx_content_campaigns_dates ON content_campaigns(start_date, end_date);

CREATE TABLE IF NOT EXISTS planned_topics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id INTEGER REFERENCES content_campaigns(id),
    topic TEXT NOT NULL,
    angle TEXT,                    -- specific angle to cover
    source_material TEXT,          -- optional: commit SHAs or session IDs to draw from
    target_date TEXT,              -- when to aim for publication
    status TEXT DEFAULT 'planned', -- 'planned', 'generated', 'skipped'
    content_id INTEGER REFERENCES generated_content(id),  -- link when generated
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_planned_topics_campaign ON planned_topics(campaign_id);

-- Lightweight manual idea inbox for future content seeds
CREATE TABLE IF NOT EXISTS content_ideas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    note TEXT NOT NULL,
    topic TEXT,
    priority TEXT DEFAULT 'normal',      -- high | normal | low
    status TEXT DEFAULT 'open',          -- open | promoted | dismissed
    source TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_content_ideas_status_priority
    ON content_ideas(status, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_content_ideas_topic
    ON content_ideas(topic);

-- Publish queue for scheduled posting at optimal times
CREATE TABLE IF NOT EXISTS publish_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES generated_content(id),
    scheduled_at TEXT NOT NULL,     -- ISO timestamp for when to publish
    platform TEXT DEFAULT 'all',    -- 'x', 'bluesky', 'all'
    status TEXT DEFAULT 'queued',   -- 'queued', 'published', 'failed', 'cancelled'
    published_at TEXT,
    error TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_publish_queue_status ON publish_queue(status);
CREATE INDEX IF NOT EXISTS idx_publish_queue_scheduled ON publish_queue(scheduled_at);

-- Profile metrics time-series (follower growth tracking)
CREATE TABLE IF NOT EXISTS profile_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL DEFAULT 'x',
    follower_count INTEGER NOT NULL,
    following_count INTEGER NOT NULL,
    tweet_count INTEGER NOT NULL,
    listed_count INTEGER,
    fetched_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_profile_metrics_fetched ON profile_metrics(fetched_at);
CREATE INDEX IF NOT EXISTS idx_profile_metrics_platform ON profile_metrics(platform);

-- Proactive engagement actions (discovered reply/like/quote opportunities)
CREATE TABLE IF NOT EXISTS proactive_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action_type TEXT NOT NULL,            -- 'reply', 'quote_tweet', 'like', 'retweet'
    target_tweet_id TEXT,
    target_tweet_text TEXT,
    target_author_handle TEXT,
    target_author_id TEXT,
    discovery_source TEXT,                -- 'curated_timeline', 'search', 'cultivate'
    relevance_score REAL,
    draft_text TEXT,
    status TEXT DEFAULT 'pending',        -- 'pending', 'approved', 'posted', 'dismissed'
    relationship_context TEXT,            -- JSON PersonContext
    knowledge_ids TEXT,                   -- JSON list of (knowledge_id, relevance) tuples
    posted_tweet_id TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    reviewed_at TEXT,
    posted_at TEXT,
    UNIQUE(target_tweet_id, action_type)
);
CREATE INDEX IF NOT EXISTS idx_proactive_status ON proactive_actions(status);
CREATE INDEX IF NOT EXISTS idx_proactive_author ON proactive_actions(target_author_handle);
CREATE INDEX IF NOT EXISTS idx_proactive_created ON proactive_actions(created_at);
