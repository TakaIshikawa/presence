# Presence

## Vision
Automated personal branding pipeline that turns coding activity into content. Monitors GitHub commits and Claude Code prompts, synthesizes them through a multi-stage AI pipeline (few-shot selection, multi-candidate generation, cross-model evaluation, guided refinement), and publishes to X and a static blog.

## Built So Far
- **Core pipeline**: 5-stage synthesis (few-shot retrieval, multi-candidate generation with temperature variation, cross-model evaluation via Opus, guided refinement, final gate) with format directives for structural variety
- **Three content types**: X threads (poll-driven), daily digest threads, weekly blog posts — each with dedicated prompt templates and format directives
- **Three-layer semantic dedup**: (1) Avoidance context in generation prompt listing recent topics, (2) SequenceMatcher + stale regex pattern filtering, (3) Voyage embedding cosine similarity at 0.82 threshold
- **Ingestion**: GitHub commit polling across all repos, Claude Code log parsing with commit-prompt correlation via time-window linking
- **Output**: X client with single post and thread posting, blog writer with git commit/push, Buttondown newsletter distribution
- **Engagement feedback loop**: Fetch post engagement metrics, auto-classify posts as resonated/low_resonance, use top performers as few-shot examples, engagement-weighted candidate filtering
- **Reply system**: Mention detection, AI-drafted replies with quality evaluation, relationship-aware context via Cultivate integration
- **Knowledge base**: Curated source ingestion (X accounts, blogs), Voyage embeddings for semantic search, knowledge-linked content generation
- **Operational infrastructure**: launchd scheduling via manage.sh, script_context shared boilerplate, operations state tracking, watchdog timeouts
- **Test suite**: 1232 tests covering all pipeline stages, scripts, DB layer, and integrations

## Latest
Added three-layer semantic dedup and thread consolidation: switched poll_commits from x_post to x_thread content type, wired Voyage embeddings into all three scripts for dedup at generation and insert time, and backfilled embeddings for 160 published posts. Evaluation run confirmed pipeline quality (avg 7.7/10 across 3 runs).

## Next
TBD
