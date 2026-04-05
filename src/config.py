"""Configuration loading."""

import os
import yaml
from pathlib import Path
from dataclasses import dataclass
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
class AnthropicConfig:
    api_key: str


@dataclass
class PathsConfig:
    claude_logs: str
    static_site: str
    database: str


@dataclass
class SynthesisConfig:
    model: str
    eval_model: str
    eval_threshold: float
    num_candidates: int


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


@dataclass
class EmbeddingsConfig:
    provider: str
    model: str
    api_key: str


@dataclass
class CuratedSource:
    identifier: str  # username or domain
    name: str
    license: str


@dataclass
class CuratedSourcesConfig:
    x_accounts: list[CuratedSource]
    blogs: list[CuratedSource]


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
class Config:
    github: GitHubConfig
    x: XConfig
    anthropic: AnthropicConfig
    paths: PathsConfig
    synthesis: SynthesisConfig
    polling: PollingConfig
    replies: Optional[RepliesConfig]
    embeddings: Optional[EmbeddingsConfig]
    curated_sources: Optional[CuratedSourcesConfig]
    newsletter: Optional[NewsletterConfig]
    historical: Optional[HistoricalConfig]


def _resolve_env_var(value: str) -> str:
    """Resolve ${ENV_VAR} patterns in config values."""
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        env_var = value[2:-1]
        return os.environ.get(env_var, "")
    return value


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
        )
    else:
        replies_config = RepliesConfig(enabled=True, max_daily_replies=10)

    # Parse embeddings config if present
    embeddings_config = None
    if "embeddings" in data:
        embeddings_config = EmbeddingsConfig(
            provider=data["embeddings"]["provider"],
            model=data["embeddings"]["model"],
            api_key=_resolve_env_var(data["embeddings"]["api_key"])
        )

    # Parse curated sources if present
    curated_sources_config = None
    if "curated_sources" in data:
        x_accounts = [
            CuratedSource(
                identifier=acc.get("username", ""),
                name=acc.get("name", ""),
                license=acc.get("license", "attribution_required")
            )
            for acc in data["curated_sources"].get("x_accounts", [])
        ]
        blogs = [
            CuratedSource(
                identifier=blog.get("domain", ""),
                name=blog.get("name", ""),
                license=blog.get("license", "attribution_required")
            )
            for blog in data["curated_sources"].get("blogs", [])
        ]
        curated_sources_config = CuratedSourcesConfig(
            x_accounts=x_accounts,
            blogs=blogs
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

    return Config(
        github=GitHubConfig(
            username=data["github"]["username"],
            token=_resolve_env_var(data["github"]["token"])
        ),
        x=XConfig(
            api_key=_resolve_env_var(data["x"]["api_key"]),
            api_secret=_resolve_env_var(data["x"]["api_secret"]),
            access_token=_resolve_env_var(data["x"]["access_token"]),
            access_token_secret=_resolve_env_var(data["x"]["access_token_secret"])
        ),
        anthropic=AnthropicConfig(
            api_key=_resolve_env_var(data["anthropic"]["api_key"])
        ),
        paths=PathsConfig(
            claude_logs=data["paths"]["claude_logs"],
            static_site=data["paths"]["static_site"],
            database=data["paths"]["database"]
        ),
        synthesis=SynthesisConfig(
            model=data["synthesis"]["model"],
            eval_model=data["synthesis"].get("eval_model", data["synthesis"]["model"]),
            eval_threshold=data["synthesis"]["eval_threshold"],
            num_candidates=data["synthesis"].get("num_candidates", 3),
        ),
        polling=PollingConfig(
            interval_minutes=data["polling"]["interval_minutes"],
            daily_digest_hour=data["polling"]["daily_digest_hour"],
            weekly_digest_day=data["polling"]["weekly_digest_day"],
            readiness_token_threshold=data["polling"].get("readiness_token_threshold", 500),
            max_post_gap_hours=data["polling"].get("max_post_gap_hours", 12),
            max_daily_posts=data["polling"].get("max_daily_posts", 3),
        ),
        replies=replies_config,
        embeddings=embeddings_config,
        curated_sources=curated_sources_config,
        newsletter=newsletter_config,
        historical=historical_config,
    )
