# Presence

Automated personal branding pipeline that turns your coding activity into content.

```
[Your Code] → [Claude Code Prompts] → [AI Synthesis] → [Eval] → [X / Blog]
```

## What It Does

- **Per-commit posts**: Every 10 minutes, checks for new commits. Finds related Claude Code prompts, synthesizes an X post, evaluates quality, and auto-posts if it passes threshold.
- **Daily digest**: At 23:59, generates an X thread summarizing the day's work.
- **Weekly blog**: Sundays at 12:00, generates a blog post from the week's activity and publishes to your static site.

## Prerequisites

- Python 3.11+
- macOS (uses launchd for scheduling)
- [Claude Code](https://claude.ai/claude-code) for coding (prompts are extracted from local logs)
- GitHub account
- X (Twitter) developer account
- Anthropic API key

## Installation

```bash
git clone https://github.com/TakaIshikawa/presence.git
cd presence
pip install -r requirements.txt
```

## Configuration

1. Copy the config template:
```bash
cp config.yaml config.local.yaml
```

2. Edit `config.local.yaml` with your credentials:

```yaml
github:
  username: "YOUR_GITHUB_USERNAME"
  token: "ghp_xxxxxxxxxxxx"  # Settings → Developer settings → Personal access tokens

x:
  api_key: "xxxxxxxxxx"
  api_secret: "xxxxxxxxxx"
  access_token: "xxxxxxxxxx"
  access_token_secret: "xxxxxxxxxx"

anthropic:
  api_key: "sk-ant-xxxxxxxxxxxx"

paths:
  claude_logs: "~/.claude"
  static_site: "/path/to/your/static/site"  # Optional: for blog posts
  database: "./presence.db"

synthesis:
  model: "claude-sonnet-4-20250514"
  eval_threshold: 0.7  # 0-1, minimum score to auto-post

polling:
  interval_minutes: 10
  daily_digest_hour: 23
  weekly_digest_day: "sunday"
```

### Getting API Keys

**GitHub Token:**
1. Go to GitHub → Settings → Developer settings → Personal access tokens → Fine-grained tokens
2. Create token with read-only access to Contents and Metadata

**X (Twitter) API:**
1. Go to [developer.twitter.com](https://developer.twitter.com)
2. Create a project and app
3. Generate API keys and access tokens with read/write permissions

**Anthropic API:**
1. Go to [console.anthropic.com](https://console.anthropic.com)
2. Create an API key

## Usage

### Manual Commands

```bash
# Run once manually
./scripts/manage.sh run poll     # Check for new commits
./scripts/manage.sh run daily    # Generate daily digest
./scripts/manage.sh run weekly   # Generate weekly blog post
```

### Start Automation

```bash
# Start all scheduled jobs
./scripts/manage.sh start

# Check status
./scripts/manage.sh status

# View logs
./scripts/manage.sh logs

# Stop automation
./scripts/manage.sh stop

# Restart
./scripts/manage.sh restart
```

### Schedule

| Job | Frequency | Output |
|-----|-----------|--------|
| poll | Every 10 min | X post per commit |
| daily | 23:59 | X thread |
| weekly | Sunday 12:00 | Blog post |

## How It Works

### Data Flow

1. **Ingestion**
   - Parses Claude Code logs from `~/.claude/history.jsonl`
   - Fetches commits from GitHub API across all your repos

2. **Correlation**
   - Matches commits with Claude prompts by timestamp (±30 min window)
   - Your prompts contain intent/strategy; commits contain what was built

3. **Synthesis**
   - Uses Claude API to generate content from prompt + commit context
   - Customizable prompts in `src/synthesis/prompts/`

4. **Evaluation**
   - LLM-as-judge scores content on authenticity, insight depth, clarity, voice match
   - Only publishes if score ≥ threshold (default 70%)

5. **Output**
   - X posts via Twitter API
   - Blog posts as HTML committed to static site repo

### Project Structure

```
presence/
├── src/
│   ├── ingestion/
│   │   ├── claude_logs.py      # Parse ~/.claude/ logs
│   │   └── github_commits.py   # Fetch commits via API
│   ├── storage/
│   │   └── db.py               # SQLite tracking layer
│   ├── synthesis/
│   │   ├── generator.py        # Content generation
│   │   ├── evaluator.py        # LLM-as-judge scoring
│   │   └── prompts/            # Prompt templates
│   ├── output/
│   │   ├── x_client.py         # X API client
│   │   └── blog_writer.py      # Static site writer
│   └── config.py               # Config loader
├── scripts/
│   ├── poll_commits.py         # Per-commit job
│   ├── daily_digest.py         # Daily thread job
│   ├── weekly_digest.py        # Weekly blog job
│   └── manage.sh               # Automation manager
├── config.yaml                 # Config template
├── schema.sql                  # Database schema
└── requirements.txt
```

## Customization

### Voice & Style

Edit the prompt templates in `src/synthesis/prompts/`:

- `x_post.txt` — Single tweet from commit
- `x_thread.txt` — Daily thread
- `blog_post.txt` — Weekly blog post
- `evaluator.txt` — Quality evaluation criteria

### Eval Threshold

Adjust `synthesis.eval_threshold` in config (0.0-1.0). Lower = more posts, higher = stricter quality gate.

### Static Site

The blog writer expects a plain HTML static site structure:
```
your-site/
├── index.html      # Must have <ul class="posts"> for blog list
├── style.css
└── blog/
    └── *.html      # Blog posts
```

## Logs & Debugging

```bash
# View all logs
./scripts/manage.sh logs

# Or individually
tail -f logs/poll.log
tail -f logs/daily.log
tail -f logs/weekly.log
```

## License

MIT
