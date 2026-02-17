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
    eval_threshold: float


@dataclass
class PollingConfig:
    interval_minutes: int
    daily_digest_hour: int
    weekly_digest_day: str


@dataclass
class Config:
    github: GitHubConfig
    x: XConfig
    anthropic: AnthropicConfig
    paths: PathsConfig
    synthesis: SynthesisConfig
    polling: PollingConfig


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
            eval_threshold=data["synthesis"]["eval_threshold"]
        ),
        polling=PollingConfig(
            interval_minutes=data["polling"]["interval_minutes"],
            daily_digest_hour=data["polling"]["daily_digest_hour"],
            weekly_digest_day=data["polling"]["weekly_digest_day"]
        )
    )
