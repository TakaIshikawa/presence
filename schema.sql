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

-- Link commits to Claude messages (same time window = same work session)
CREATE TABLE IF NOT EXISTS commit_prompt_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    commit_id INTEGER REFERENCES github_commits(id),
    message_id INTEGER REFERENCES claude_messages(id),
    confidence REAL,  -- how confident we are these are related
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Generated content
CREATE TABLE IF NOT EXISTS generated_content (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_type TEXT NOT NULL,  -- 'x_post', 'x_thread', 'blog_post'
    source_commits TEXT,  -- JSON array of commit SHAs
    source_messages TEXT,  -- JSON array of message UUIDs
    content TEXT NOT NULL,
    eval_score REAL,
    eval_feedback TEXT,
    published INTEGER DEFAULT 0,
    published_url TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Prompt versions for eval tracking
CREATE TABLE IF NOT EXISTS prompt_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prompt_type TEXT NOT NULL,  -- 'x_post', 'x_thread', 'blog_post'
    version INTEGER NOT NULL,
    prompt_text TEXT NOT NULL,
    avg_score REAL,
    usage_count INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(prompt_type, version)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_claude_messages_session ON claude_messages(session_id);
CREATE INDEX IF NOT EXISTS idx_claude_messages_timestamp ON claude_messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_github_commits_repo ON github_commits(repo_name);
CREATE INDEX IF NOT EXISTS idx_github_commits_timestamp ON github_commits(timestamp);
CREATE INDEX IF NOT EXISTS idx_generated_content_type ON generated_content(content_type);
