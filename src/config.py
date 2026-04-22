"""Configuration loading."""

import os
import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class GitHubConfig:
    username: str
    token: str


@dataclass
class XConfig:
    api_key: str
    api_secret: str
    access_token: str
    access_token_secret: str


@dataclass
class BlueskyConfig:
    enabled: bool
    handle: str
    app_password: str


@dataclass
class AnthropicConfig:
    api_key: str


@dataclass
class PathsConfig:
    claude_logs: str
    static_site: str
    database: str
    allowed_projects: Optional[list[str]] = None


@dataclass
class SynthesisConfig:
    model: str
    eval_model: str
    eval_threshold: float
    num_candidates: int
    format_weighting_enabled: bool = True
    claim_check_enabled: bool = True


@dataclass
class PollingConfig:
    interval_minutes: int
    daily_digest_hour: int
    weekly_digest_day: str
    readiness_token_threshold: int
    max_post_gap_hours: int
    max_daily_posts: int


@dataclass
class RepliesConfig:
    enabled: bool
    max_daily_replies: int
    draft_ttl_hours: int = 48
    classifier_fallback_enabled: bool = False
    spam_action: str = "dismissed"
    low_value_action: str = "low_priority"


@dataclass
class EmbeddingsConfig:
    provider: str
    model: str
    api_key: str
    semantic_dedup_threshold: float = 0.82


@dataclass
class CuratedSource:
    identifier: str  # username or domain
    name: str
    license: str
    feed_url: Optional[str] = None


@dataclass
class CuratedSourcesConfig:
    x_accounts: list[CuratedSource]
    blogs: list[CuratedSource]
    newsletters: list[CuratedSource] = field(default_factory=list)
    restricted_prompt_behavior: str = "strict"
    max_x_accounts_per_run: int = 25
    x_tweets_per_account: int = 5
    rss_entries_per_source: int = 5


@dataclass
class NewsletterConfig:
    enabled: bool
    provider: str
    api_key: str
    send_day: str
    send_hour: int


@dataclass
class HistoricalConfig:
    enabled: bool
    lookback_days: int
    injection_frequency: int
    min_age_days: int
    max_historical_commits: int


@dataclass
class CultivateIntegrationConfig:
    enabled: bool
    db_path: str
    forward_mentions: bool
    enrich_replies: bool
    proactive_review: bool
    reply_quality_threshold: float


@dataclass
class TimeoutsConfig:
    anthropic_seconds: int = 300
    github_seconds: int = 30
    http_seconds: int = 30


@dataclass
class SchedulingConfig:
    enabled: bool = False
    min_samples: int = 20


@dataclass
class PublishQueueConfig:
    max_retry_delay_minutes: int = 360


@dataclass
class OperationsHealthConfig:
    max_poll_age_minutes: int = 30
    max_reply_state_age_hours: int = 6
    max_platform_reply_state_age_hours: int = 6
    max_failed_queue_items: int = 0
    pipeline_window_hours: int = 24
    min_pipeline_runs_for_rejection_rate: int = 3
    max_pipeline_rejection_rate: float = 0.5
    max_engagement_fetch_age_hours: int = 36


@dataclass
class ImageGenConfig:
    provider: str = "pillow"
    output_dir: str = "generated_images"


@dataclass
class ProactiveConfig:
    enabled: bool = False
    max_daily_replies: int = 5
    account_cooldown_hours: int = 72
    min_relevance: float = 0.50
    max_tweet_age_hours: int = 24
    reply_cap_per_account: int = 2
    search_enabled: bool = False
    search_keywords: list[str] | None = None
    account_discovery_enabled: bool = True
    max_candidates_per_run: int = 5
    min_discovery_relevance: float = 0.45
    min_discovery_samples: int = 3
    max_accounts_per_run: int = 25
    tweets_per_account: int = 5


@dataclass
class Config:
    github: GitHubConfig
    x: XConfig
    bluesky: Optional[BlueskyConfig]
    anthropic: AnthropicConfig
    paths: PathsConfig
    synthesis: SynthesisConfig
    polling: PollingConfig
    replies: Optional[RepliesConfig]
    embeddings: Optional[EmbeddingsConfig]
    curated_sources: Optional[CuratedSourcesConfig]
    newsletter: Optional[NewsletterConfig]
    historical: Optional[HistoricalConfig]
    cultivate: Optional[CultivateIntegrationConfig]
    timeouts: TimeoutsConfig
    scheduling: Optional[SchedulingConfig]
    publish_queue: PublishQueueConfig
    operations_health: OperationsHealthConfig
    proactive: Optional[ProactiveConfig]
    image_gen: Optional[ImageGenConfig]


def _resolve_env_var(value: str) -> str:
    """Resolve ${ENV_VAR} patterns in config values."""
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        env_var = value[2:-1]
        return os.environ.get(env_var, "")
    return value


def _require(data: dict, *keys: str, section: str) -> any:
    """Traverse nested keys and return the value, raising ValueError if missing.

    Args:
        data: The root config dictionary
        *keys: Sequence of keys to traverse (e.g., 'github', 'username')
        section: Human-readable section name for error messages

    Returns:
        The value at the nested key path

    Raises:
        ValueError: If any key is missing or if a parent is not a dict
    """
    current = data
    for i, key in enumerate(keys):
        if not isinstance(current, dict):
            # Parent exists but isn't a dict
            parent_path = '.'.join(keys[:i])
            raise ValueError(
                f"Invalid config section: '{parent_path}' must be a dictionary"
            )
        if key not in current:
            key_path = '.'.join(keys)
            raise ValueError(f"Missing required config field: {key_path}")
        current = current[key]
    return current


def load_config(config_path: Optional[str] = None) -> Config:
    """Load configuration from YAML file."""
    if config_path is None:
        # Look for config.local.yaml first, then config.yaml
        base_path = Path(__file__).parent.parent
        local_config = base_path / "config.local.yaml"
        default_config = base_path / "config.yaml"

        if local_config.exists():
            config_path = local_config
        else:
            config_path = default_config

    with open(config_path, "r") as f:
        data = yaml.safe_load(f)

    # Parse replies config
    replies_config = None
    if "replies" in data:
        replies_config = RepliesConfig(
            enabled=data["replies"].get("enabled", True),
            max_daily_replies=data["replies"].get("max_daily_replies", 10),
            draft_ttl_hours=data["replies"].get("draft_ttl_hours", 48),
            classifier_fallback_enabled=data["replies"].get("classifier_fallback_enabled", False),
            spam_action=data["replies"].get("spam_action", "dismissed"),
            low_value_action=data["replies"].get("low_value_action", "low_priority"),
        )
    else:
        replies_config = RepliesConfig(enabled=True, max_daily_replies=10)

    # Parse embeddings config if present
    embeddings_config = None
    if "embeddings" in data:
        embeddings_config = EmbeddingsConfig(
            provider=data["embeddings"]["provider"],
            model=data["embeddings"]["model"],
            api_key=_resolve_env_var(data["embeddings"]["api_key"]),
            semantic_dedup_threshold=data["embeddings"].get("semantic_dedup_threshold", 0.82),
        )

    # Parse curated sources if present
    curated_sources_config = None
    if "curated_sources" in data:
        x_accounts = [
            CuratedSource(
                identifier=acc.get("username", ""),
                name=acc.get("name", ""),
                license=acc.get("license", "attribution_required"),
                feed_url=acc.get("feed_url"),
            )
            for acc in data["curated_sources"].get("x_accounts", [])
        ]
        blogs = [
            CuratedSource(
                identifier=blog.get("domain", ""),
                name=blog.get("name", ""),
                license=blog.get("license", "attribution_required"),
                feed_url=blog.get("feed_url"),
            )
            for blog in data["curated_sources"].get("blogs", [])
        ]
        newsletters = [
            CuratedSource(
                identifier=newsletter.get("domain", newsletter.get("identifier", "")),
                name=newsletter.get("name", ""),
                license=newsletter.get("license", "attribution_required"),
                feed_url=newsletter.get("feed_url"),
            )
            for newsletter in data["curated_sources"].get("newsletters", [])
        ]
        curated_sources_config = CuratedSourcesConfig(
            x_accounts=x_accounts,
            blogs=blogs,
            newsletters=newsletters,
            restricted_prompt_behavior=data["curated_sources"].get(
                "restricted_prompt_behavior", "strict"
            ),
            max_x_accounts_per_run=data["curated_sources"].get("max_x_accounts_per_run", 25),
            x_tweets_per_account=data["curated_sources"].get("x_tweets_per_account", 5),
            rss_entries_per_source=data["curated_sources"].get("rss_entries_per_source", 5),
        )

    # Parse newsletter config if present
    newsletter_config = None
    if "newsletter" in data:
        newsletter_config = NewsletterConfig(
            enabled=data["newsletter"].get("enabled", True),
            provider=data["newsletter"].get("provider", "buttondown"),
            api_key=_resolve_env_var(data["newsletter"].get("api_key", "")),
            send_day=data["newsletter"].get("send_day", "monday"),
            send_hour=data["newsletter"].get("send_hour", 9),
        )

    # Parse historical config if present
    historical_config = None
    if "historical" in data:
        historical_config = HistoricalConfig(
            enabled=data["historical"].get("enabled", True),
            lookback_days=data["historical"].get("lookback_days", 180),
            injection_frequency=data["historical"].get("injection_frequency", 3),
            min_age_days=data["historical"].get("min_age_days", 30),
            max_historical_commits=data["historical"].get("max_historical_commits", 5),
        )

    # Parse cultivate integration config if present
    cultivate_config = None
    if "cultivate" in data:
        cultivate_config = CultivateIntegrationConfig(
            enabled=data["cultivate"].get("enabled", True),
            db_path=data["cultivate"].get("db_path", "~/.cultivate/cultivate.db"),
            forward_mentions=data["cultivate"].get("forward_mentions", True),
            enrich_replies=data["cultivate"].get("enrich_replies", True),
            proactive_review=data["cultivate"].get("proactive_review", True),
            reply_quality_threshold=data["cultivate"].get("reply_quality_threshold", 6.0),
        )

    # Parse Bluesky config if present
    bluesky_config = None
    if "bluesky" in data:
        bluesky_config = BlueskyConfig(
            enabled=data["bluesky"].get("enabled", True),
            handle=_resolve_env_var(data["bluesky"].get("handle", "")),
            app_password=_resolve_env_var(data["bluesky"].get("app_password", "")),
        )

    # Validate required sections exist and are dictionaries
    _require(data, "github", section="github")
    _require(data, "x", section="x")
    _require(data, "anthropic", section="anthropic")
    _require(data, "paths", section="paths")
    _require(data, "synthesis", section="synthesis")
    _require(data, "polling", section="polling")

    # Parse timeouts config if present
    timeouts_config = TimeoutsConfig()
    if "timeouts" in data:
        timeouts_config = TimeoutsConfig(
            anthropic_seconds=data["timeouts"].get("anthropic_seconds", 300),
            github_seconds=data["timeouts"].get("github_seconds", 30),
            http_seconds=data["timeouts"].get("http_seconds", 30),
        )

    # Parse scheduling config if present
    scheduling_config = None
    if "scheduling" in data:
        scheduling_config = SchedulingConfig(
            enabled=data["scheduling"].get("enabled", False),
            min_samples=data["scheduling"].get("min_samples", 20),
        )

    publish_queue_config = PublishQueueConfig()
    if "publish_queue" in data:
        publish_queue_config = PublishQueueConfig(
            max_retry_delay_minutes=data["publish_queue"].get("max_retry_delay_minutes", 360),
        )

    default_poll_health_minutes = data["polling"].get("interval_minutes", 10) * 3
    operations_health_config = OperationsHealthConfig(
        max_poll_age_minutes=default_poll_health_minutes,
    )
    if "operations_health" in data:
        health_data = data["operations_health"]
        operations_health_config = OperationsHealthConfig(
            max_poll_age_minutes=health_data.get(
                "max_poll_age_minutes", default_poll_health_minutes
            ),
            max_reply_state_age_hours=health_data.get("max_reply_state_age_hours", 6),
            max_platform_reply_state_age_hours=health_data.get(
                "max_platform_reply_state_age_hours", 6
            ),
            max_failed_queue_items=health_data.get("max_failed_queue_items", 0),
            pipeline_window_hours=health_data.get("pipeline_window_hours", 24),
            min_pipeline_runs_for_rejection_rate=health_data.get(
                "min_pipeline_runs_for_rejection_rate", 3
            ),
            max_pipeline_rejection_rate=health_data.get(
                "max_pipeline_rejection_rate", 0.5
            ),
            max_engagement_fetch_age_hours=health_data.get(
                "max_engagement_fetch_age_hours", 36
            ),
        )

    # Parse proactive engagement config if present
    proactive_config = None
    if "proactive" in data:
        proactive_config = ProactiveConfig(
            enabled=data["proactive"].get("enabled", False),
            max_daily_replies=data["proactive"].get("max_daily_replies", 5),
            account_cooldown_hours=data["proactive"].get("account_cooldown_hours", 72),
            min_relevance=data["proactive"].get("min_relevance", 0.50),
            max_tweet_age_hours=data["proactive"].get("max_tweet_age_hours", 24),
            reply_cap_per_account=data["proactive"].get("reply_cap_per_account", 2),
            search_enabled=data["proactive"].get("search_enabled", False),
            search_keywords=data["proactive"].get("search_keywords"),
            account_discovery_enabled=data["proactive"].get("account_discovery_enabled", True),
            max_candidates_per_run=data["proactive"].get("max_candidates_per_run", 5),
            min_discovery_relevance=data["proactive"].get("min_discovery_relevance", 0.45),
            min_discovery_samples=data["proactive"].get("min_discovery_samples", 3),
            max_accounts_per_run=data["proactive"].get("max_accounts_per_run", 25),
            tweets_per_account=data["proactive"].get("tweets_per_account", 5),
        )

    # Parse image generation config if present
    image_gen_config = None
    if "image_gen" in data:
        image_gen_config = ImageGenConfig(
            provider=data["image_gen"].get("provider", "pillow"),
            output_dir=data["image_gen"].get("output_dir", "generated_images"),
        )

    return Config(
        github=GitHubConfig(
            username=_require(data, "github", "username", section="github"),
            token=_resolve_env_var(_require(data, "github", "token", section="github"))
        ),
        x=XConfig(
            api_key=_resolve_env_var(_require(data, "x", "api_key", section="x")),
            api_secret=_resolve_env_var(_require(data, "x", "api_secret", section="x")),
            access_token=_resolve_env_var(_require(data, "x", "access_token", section="x")),
            access_token_secret=_resolve_env_var(_require(data, "x", "access_token_secret", section="x"))
        ),
        bluesky=bluesky_config,
        anthropic=AnthropicConfig(
            api_key=_resolve_env_var(_require(data, "anthropic", "api_key", section="anthropic"))
        ),
        paths=PathsConfig(
            claude_logs=_require(data, "paths", "claude_logs", section="paths"),
            static_site=_require(data, "paths", "static_site", section="paths"),
            database=_require(data, "paths", "database", section="paths"),
            allowed_projects=data["paths"].get("allowed_projects"),
        ),
        synthesis=SynthesisConfig(
            model=_require(data, "synthesis", "model", section="synthesis"),
            eval_model=data["synthesis"].get("eval_model", _require(data, "synthesis", "model", section="synthesis")),
            eval_threshold=_require(data, "synthesis", "eval_threshold", section="synthesis"),
            num_candidates=data["synthesis"].get("num_candidates", 3),
            format_weighting_enabled=data["synthesis"].get("format_weighting_enabled", True),
            claim_check_enabled=data["synthesis"].get("claim_check_enabled", True),
        ),
        polling=PollingConfig(
            interval_minutes=_require(data, "polling", "interval_minutes", section="polling"),
            daily_digest_hour=_require(data, "polling", "daily_digest_hour", section="polling"),
            weekly_digest_day=_require(data, "polling", "weekly_digest_day", section="polling"),
            readiness_token_threshold=data["polling"].get("readiness_token_threshold", 500),
            max_post_gap_hours=data["polling"].get("max_post_gap_hours", 12),
            max_daily_posts=data["polling"].get("max_daily_posts", 3),
        ),
        replies=replies_config,
        embeddings=embeddings_config,
        curated_sources=curated_sources_config,
        newsletter=newsletter_config,
        historical=historical_config,
        cultivate=cultivate_config,
        timeouts=timeouts_config,
        scheduling=scheduling_config,
        publish_queue=publish_queue_config,
        operations_health=operations_health_config,
        proactive=proactive_config,
        image_gen=image_gen_config,
    )
