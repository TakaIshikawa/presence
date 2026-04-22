"""Tests for configuration loading (src/config.py)."""

import yaml

import pytest

from config import (
    Config,
    GitHubConfig,
    XConfig,
    BlueskyConfig,
    AnthropicConfig,
    PathsConfig,
    SynthesisConfig,
    PollingConfig,
    RepliesConfig,
    EmbeddingsConfig,
    ImageGenConfig,
    ProactiveConfig,
    CuratedSource,
    CuratedSourcesConfig,
    TimeoutsConfig,
    _resolve_env_var,
    load_config,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _minimal_config_dict(**overrides) -> dict:
    """Return a minimal valid config dict. Override any top-level section."""
    base = {
        "github": {"username": "testuser", "token": "ghp_test"},
        "x": {
            "api_key": "xkey",
            "api_secret": "xsecret",
            "access_token": "xtoken",
            "access_token_secret": "xtokensecret",
        },
        "anthropic": {"api_key": "sk-ant-test"},
        "paths": {
            "claude_logs": "/tmp/logs",
            "static_site": "/tmp/site",
            "database": "/tmp/db.sqlite",
        },
        "synthesis": {
            "model": "claude-sonnet-4-5-20250514",
            "eval_threshold": 0.7,
        },
        "polling": {
            "interval_minutes": 30,
            "daily_digest_hour": 9,
            "weekly_digest_day": "monday",
        },
    }
    base.update(overrides)
    return base


def _write_yaml(path, data: dict) -> str:
    """Dump *data* as YAML to *path* and return the path as a string."""
    path.write_text(yaml.dump(data))
    return str(path)


# ---------------------------------------------------------------------------
# _resolve_env_var
# ---------------------------------------------------------------------------

class TestResolveEnvVar:
    def test_resolves_existing_env_var(self, monkeypatch):
        monkeypatch.setenv("MY_SECRET", "s3cret")
        assert _resolve_env_var("${MY_SECRET}") == "s3cret"

    def test_missing_env_var_returns_empty_string(self, monkeypatch):
        monkeypatch.delenv("NONEXISTENT_VAR", raising=False)
        assert _resolve_env_var("${NONEXISTENT_VAR}") == ""

    def test_plain_string_passes_through(self):
        assert _resolve_env_var("plain-value") == "plain-value"

    def test_partial_syntax_not_resolved(self):
        """Only exact ${...} wrapper triggers resolution."""
        assert _resolve_env_var("prefix${VAR}") == "prefix${VAR}"
        assert _resolve_env_var("${VAR}suffix") == "${VAR}suffix"

    def test_non_string_passes_through(self):
        assert _resolve_env_var(42) == 42
        assert _resolve_env_var(None) is None


# ---------------------------------------------------------------------------
# Dataclass parsing — full round-trip through load_config
# ---------------------------------------------------------------------------

class TestDataclassParsing:
    """Verify every config section materialises as the right dataclass."""

    def test_github_config(self, tmp_path):
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", _minimal_config_dict()))
        assert isinstance(cfg.github, GitHubConfig)
        assert cfg.github.username == "testuser"
        assert cfg.github.token == "ghp_test"

    def test_x_config(self, tmp_path):
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", _minimal_config_dict()))
        assert isinstance(cfg.x, XConfig)
        assert cfg.x.api_key == "xkey"
        assert cfg.x.api_secret == "xsecret"
        assert cfg.x.access_token == "xtoken"
        assert cfg.x.access_token_secret == "xtokensecret"

    def test_anthropic_config(self, tmp_path):
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", _minimal_config_dict()))
        assert isinstance(cfg.anthropic, AnthropicConfig)
        assert cfg.anthropic.api_key == "sk-ant-test"

    def test_paths_config(self, tmp_path):
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", _minimal_config_dict()))
        assert isinstance(cfg.paths, PathsConfig)
        assert cfg.paths.claude_logs == "/tmp/logs"
        assert cfg.paths.static_site == "/tmp/site"
        assert cfg.paths.database == "/tmp/db.sqlite"
        assert cfg.paths.allowed_projects is None

    def test_paths_allowed_projects_config(self, tmp_path):
        data = _minimal_config_dict()
        data["paths"]["allowed_projects"] = ["/work/project-a", "/work/project-b"]
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.paths.allowed_projects == ["/work/project-a", "/work/project-b"]

    def test_synthesis_config(self, tmp_path):
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", _minimal_config_dict()))
        assert isinstance(cfg.synthesis, SynthesisConfig)
        assert cfg.synthesis.model == "claude-sonnet-4-5-20250514"
        assert cfg.synthesis.eval_threshold == 0.7

    def test_polling_config(self, tmp_path):
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", _minimal_config_dict()))
        assert isinstance(cfg.polling, PollingConfig)
        assert cfg.polling.interval_minutes == 30
        assert cfg.polling.daily_digest_hour == 9
        assert cfg.polling.weekly_digest_day == "monday"

    def test_replies_config_when_present(self, tmp_path):
        data = _minimal_config_dict(replies={"enabled": False, "max_daily_replies": 5})
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert isinstance(cfg.replies, RepliesConfig)
        assert cfg.replies.enabled is False
        assert cfg.replies.max_daily_replies == 5

    def test_embeddings_config_when_present(self, tmp_path, monkeypatch):
        monkeypatch.setenv("VOYAGE_KEY", "vk-123")
        data = _minimal_config_dict(
            embeddings={
                "provider": "voyage",
                "model": "voyage-3",
                "api_key": "${VOYAGE_KEY}",
            }
        )
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert isinstance(cfg.embeddings, EmbeddingsConfig)
        assert cfg.embeddings.provider == "voyage"
        assert cfg.embeddings.model == "voyage-3"
        assert cfg.embeddings.api_key == "vk-123"

    def test_curated_sources_config(self, tmp_path):
        data = _minimal_config_dict(
            curated_sources={
                "rss_entries_per_source": 3,
                "x_accounts": [
                    {"username": "acct1", "name": "Account 1", "license": "open"}
                ],
                "blogs": [
                    {
                        "domain": "example.com",
                        "name": "Example Blog",
                        "feed_url": "https://example.com/feed.xml",
                    }
                ],
                "newsletters": [
                    {
                        "domain": "newsletter.example.com",
                        "name": "Example Newsletter",
                        "feed_url": "https://newsletter.example.com/rss",
                    }
                ],
            }
        )
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert isinstance(cfg.curated_sources, CuratedSourcesConfig)
        assert cfg.curated_sources.rss_entries_per_source == 3

        assert len(cfg.curated_sources.x_accounts) == 1
        src = cfg.curated_sources.x_accounts[0]
        assert isinstance(src, CuratedSource)
        assert src.identifier == "acct1"
        assert src.name == "Account 1"
        assert src.license == "open"

        assert len(cfg.curated_sources.blogs) == 1
        blog = cfg.curated_sources.blogs[0]
        assert blog.identifier == "example.com"
        assert blog.name == "Example Blog"
        assert blog.license == "attribution_required"  # default
        assert blog.feed_url == "https://example.com/feed.xml"

        assert len(cfg.curated_sources.newsletters) == 1
        newsletter = cfg.curated_sources.newsletters[0]
        assert newsletter.identifier == "newsletter.example.com"
        assert newsletter.name == "Example Newsletter"
        assert newsletter.feed_url == "https://newsletter.example.com/rss"

    def test_image_gen_config(self, tmp_path):
        data = _minimal_config_dict(
            image_gen={
                "provider": "pillow",
                "output_dir": "./generated_images",
            }
        )
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert isinstance(cfg.image_gen, ImageGenConfig)
        assert cfg.image_gen.provider == "pillow"
        assert cfg.image_gen.output_dir == "./generated_images"

    def test_proactive_config(self, tmp_path):
        data = _minimal_config_dict(
            proactive={
                "enabled": True,
                "max_daily_replies": 3,
                "account_cooldown_hours": 48,
            }
        )
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert isinstance(cfg.proactive, ProactiveConfig)
        assert cfg.proactive.enabled is True
        assert cfg.proactive.max_daily_replies == 3
        assert cfg.proactive.account_cooldown_hours == 48


# ---------------------------------------------------------------------------
# Environment variable resolution in load_config
# ---------------------------------------------------------------------------

class TestEnvVarResolution:
    def test_github_token_resolved(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GH_TOKEN", "resolved-token")
        data = _minimal_config_dict()
        data["github"]["token"] = "${GH_TOKEN}"
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.github.token == "resolved-token"

    def test_x_secrets_resolved(self, tmp_path, monkeypatch):
        monkeypatch.setenv("X_KEY", "k")
        monkeypatch.setenv("X_SECRET", "s")
        monkeypatch.setenv("X_TOKEN", "t")
        monkeypatch.setenv("X_TOKEN_SECRET", "ts")
        data = _minimal_config_dict(
            x={
                "api_key": "${X_KEY}",
                "api_secret": "${X_SECRET}",
                "access_token": "${X_TOKEN}",
                "access_token_secret": "${X_TOKEN_SECRET}",
            }
        )
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.x.api_key == "k"
        assert cfg.x.api_secret == "s"
        assert cfg.x.access_token == "t"
        assert cfg.x.access_token_secret == "ts"

    def test_anthropic_key_resolved(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ANT_KEY", "ant-resolved")
        data = _minimal_config_dict()
        data["anthropic"]["api_key"] = "${ANT_KEY}"
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.anthropic.api_key == "ant-resolved"

    def test_missing_env_var_resolves_to_empty(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DOES_NOT_EXIST", raising=False)
        data = _minimal_config_dict()
        data["github"]["token"] = "${DOES_NOT_EXIST}"
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.github.token == ""


# ---------------------------------------------------------------------------
# Default values
# ---------------------------------------------------------------------------

class TestDefaults:
    def test_synthesis_eval_model_defaults_to_model(self, tmp_path):
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", _minimal_config_dict()))
        assert cfg.synthesis.eval_model == cfg.synthesis.model

    def test_synthesis_eval_model_override(self, tmp_path):
        data = _minimal_config_dict()
        data["synthesis"]["eval_model"] = "claude-haiku-3"
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.synthesis.eval_model == "claude-haiku-3"

    def test_synthesis_num_candidates_default(self, tmp_path):
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", _minimal_config_dict()))
        assert cfg.synthesis.num_candidates == 3

    def test_synthesis_num_candidates_override(self, tmp_path):
        data = _minimal_config_dict()
        data["synthesis"]["num_candidates"] = 5
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.synthesis.num_candidates == 5

    def test_synthesis_claim_check_enabled_default(self, tmp_path):
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", _minimal_config_dict()))
        assert cfg.synthesis.claim_check_enabled is True

    def test_synthesis_claim_check_enabled_override(self, tmp_path):
        data = _minimal_config_dict()
        data["synthesis"]["claim_check_enabled"] = False
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.synthesis.claim_check_enabled is False

    def test_polling_defaults(self, tmp_path):
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", _minimal_config_dict()))
        assert cfg.polling.readiness_token_threshold == 500
        assert cfg.polling.max_post_gap_hours == 12
        assert cfg.polling.max_daily_posts == 3

    def test_polling_defaults_overridable(self, tmp_path):
        data = _minimal_config_dict()
        data["polling"]["readiness_token_threshold"] = 1000
        data["polling"]["max_post_gap_hours"] = 24
        data["polling"]["max_daily_posts"] = 10
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.polling.readiness_token_threshold == 1000
        assert cfg.polling.max_post_gap_hours == 24
        assert cfg.polling.max_daily_posts == 10

    def test_replies_defaults_when_section_missing(self, tmp_path):
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", _minimal_config_dict()))
        assert cfg.replies.enabled is True
        assert cfg.replies.max_daily_replies == 10

    def test_proactive_account_cooldown_default(self, tmp_path):
        data = _minimal_config_dict(proactive={"enabled": True})
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.proactive.account_cooldown_hours == 72

    def test_replies_partial_defaults(self, tmp_path):
        data = _minimal_config_dict(replies={"enabled": False})
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.replies.enabled is False
        assert cfg.replies.max_daily_replies == 10  # default preserved

    def test_embeddings_none_when_missing(self, tmp_path):
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", _minimal_config_dict()))
        assert cfg.embeddings is None

    def test_curated_sources_none_when_missing(self, tmp_path):
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", _minimal_config_dict()))
        assert cfg.curated_sources is None

    def test_bluesky_none_when_missing(self, tmp_path):
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", _minimal_config_dict()))
        assert cfg.bluesky is None

    def test_bluesky_config_loaded(self, tmp_path):
        data = _minimal_config_dict()
        data["bluesky"] = {
            "enabled": True,
            "handle": "test.bsky.social",
            "app_password": "test-password",
        }
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.bluesky is not None
        assert cfg.bluesky.enabled is True
        assert cfg.bluesky.handle == "test.bsky.social"
        assert cfg.bluesky.app_password == "test-password"

    def test_bluesky_config_disabled(self, tmp_path):
        data = _minimal_config_dict()
        data["bluesky"] = {
            "enabled": False,
            "handle": "test.bsky.social",
            "app_password": "test-password",
        }
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.bluesky is not None
        assert cfg.bluesky.enabled is False

    def test_bluesky_resolves_env_vars(self, tmp_path, monkeypatch):
        monkeypatch.setenv("BSKY_HANDLE", "user.bsky.social")
        monkeypatch.setenv("BSKY_PASSWORD", "secret123")
        data = _minimal_config_dict()
        data["bluesky"] = {
            "enabled": True,
            "handle": "${BSKY_HANDLE}",
            "app_password": "${BSKY_PASSWORD}",
        }
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.bluesky.handle == "user.bsky.social"
        assert cfg.bluesky.app_password == "secret123"


# ---------------------------------------------------------------------------
# Type coercion (YAML natively types numerics, verify they propagate)
# ---------------------------------------------------------------------------

class TestTypeCoercion:
    def test_eval_threshold_is_float(self, tmp_path):
        data = _minimal_config_dict()
        data["synthesis"]["eval_threshold"] = 0.85
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert isinstance(cfg.synthesis.eval_threshold, float)
        assert cfg.synthesis.eval_threshold == 0.85

    def test_integer_threshold_coerced_via_yaml(self, tmp_path):
        """YAML `1` is parsed as int; verify it lands in the float field."""
        data = _minimal_config_dict()
        data["synthesis"]["eval_threshold"] = 1
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.synthesis.eval_threshold == 1

    def test_num_candidates_is_int(self, tmp_path):
        data = _minimal_config_dict()
        data["synthesis"]["num_candidates"] = 7
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert isinstance(cfg.synthesis.num_candidates, int)
        assert cfg.synthesis.num_candidates == 7

    def test_interval_minutes_is_int(self, tmp_path):
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", _minimal_config_dict()))
        assert isinstance(cfg.polling.interval_minutes, int)

    def test_boolean_replies_enabled(self, tmp_path):
        for val in (True, False):
            data = _minimal_config_dict(replies={"enabled": val})
            cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
            assert cfg.replies.enabled is val


# ---------------------------------------------------------------------------
# Config file loading — path selection logic
# ---------------------------------------------------------------------------

class TestConfigFileLoading:
    def test_explicit_path(self, tmp_path):
        path = _write_yaml(tmp_path / "custom.yaml", _minimal_config_dict())
        cfg = load_config(path)
        assert isinstance(cfg, Config)

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_config(str(tmp_path / "nonexistent.yaml"))

    def test_local_overlay_preferred(self, tmp_path, monkeypatch):
        """When no explicit path is given, config.local.yaml wins over config.yaml."""
        base_data = _minimal_config_dict()
        base_data["github"]["username"] = "from-base"

        local_data = _minimal_config_dict()
        local_data["github"]["username"] = "from-local"

        project_root = tmp_path / "project"
        project_root.mkdir()
        src_dir = project_root / "src"
        src_dir.mkdir()

        _write_yaml(project_root / "config.yaml", base_data)
        _write_yaml(project_root / "config.local.yaml", local_data)

        # Patch __file__ inside the config module so base_path resolves to project_root
        import config as config_mod
        monkeypatch.setattr(config_mod, "__file__", str(src_dir / "config.py"))

        cfg = load_config()  # no explicit path
        assert cfg.github.username == "from-local"

    def test_falls_back_to_default_config(self, tmp_path, monkeypatch):
        """When config.local.yaml doesn't exist, falls back to config.yaml."""
        base_data = _minimal_config_dict()
        base_data["github"]["username"] = "from-base"

        project_root = tmp_path / "project"
        project_root.mkdir()
        src_dir = project_root / "src"
        src_dir.mkdir()

        _write_yaml(project_root / "config.yaml", base_data)
        # Deliberately no config.local.yaml

        import config as config_mod
        monkeypatch.setattr(config_mod, "__file__", str(src_dir / "config.py"))

        cfg = load_config()
        assert cfg.github.username == "from-base"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_curated_sources_empty_lists(self, tmp_path):
        data = _minimal_config_dict(
            curated_sources={"x_accounts": [], "blogs": []}
        )
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.curated_sources.x_accounts == []
        assert cfg.curated_sources.blogs == []
        assert cfg.curated_sources.newsletters == []

    def test_curated_sources_missing_sublists(self, tmp_path):
        """Section present but sub-keys omitted — defaults to empty lists."""
        data = _minimal_config_dict(curated_sources={})
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.curated_sources.x_accounts == []
        assert cfg.curated_sources.blogs == []
        assert cfg.curated_sources.newsletters == []

    def test_curated_source_default_license(self, tmp_path):
        data = _minimal_config_dict(
            curated_sources={
                "x_accounts": [{"username": "u", "name": "n"}],
                "blogs": [{"domain": "d.com", "name": "n"}],
            }
        )
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.curated_sources.x_accounts[0].license == "attribution_required"
        assert cfg.curated_sources.blogs[0].license == "attribution_required"

    def test_missing_required_section_raises(self, tmp_path):
        """Omitting a required top-level section raises ValueError."""
        data = _minimal_config_dict()
        del data["github"]
        with pytest.raises(ValueError, match="Missing required config field: github"):
            load_config(_write_yaml(tmp_path / "c.yaml", data))

    def test_missing_required_field_in_section(self, tmp_path):
        """Missing required field within a section raises descriptive ValueError."""
        data = _minimal_config_dict()
        del data["github"]["username"]
        with pytest.raises(ValueError, match="Missing required config field: github.username"):
            load_config(_write_yaml(tmp_path / "c.yaml", data))

        # Test another section
        data = _minimal_config_dict()
        del data["x"]["api_key"]
        with pytest.raises(ValueError, match="Missing required config field: x.api_key"):
            load_config(_write_yaml(tmp_path / "c.yaml", data))

    def test_missing_multiple_required_sections(self, tmp_path):
        """Missing entire required section raises descriptive ValueError."""
        data = _minimal_config_dict()
        del data["x"]
        with pytest.raises(ValueError, match="Missing required config field: x"):
            load_config(_write_yaml(tmp_path / "c.yaml", data))

        data = _minimal_config_dict()
        del data["anthropic"]
        with pytest.raises(ValueError, match="Missing required config field: anthropic"):
            load_config(_write_yaml(tmp_path / "c.yaml", data))

    def test_section_is_not_dict(self, tmp_path):
        """Section that is not a dict raises descriptive ValueError."""
        data = _minimal_config_dict()
        data["github"] = "invalid"
        with pytest.raises(ValueError, match="Invalid config section: 'github' must be a dictionary"):
            load_config(_write_yaml(tmp_path / "c.yaml", data))

        # Test nested case - section is dict but field within should be dict
        data = _minimal_config_dict()
        data["paths"] = ["not", "a", "dict"]
        with pytest.raises(ValueError, match="Invalid config section: 'paths' must be a dictionary"):
            load_config(_write_yaml(tmp_path / "c.yaml", data))


# ---------------------------------------------------------------------------
# TimeoutsConfig
# ---------------------------------------------------------------------------

class TestTimeoutsConfig:
    """Test timeout configuration parsing and defaults."""

    def test_timeouts_defaults_when_section_missing(self, tmp_path):
        """When timeouts section is omitted, defaults should be applied."""
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", _minimal_config_dict()))
        assert isinstance(cfg.timeouts, TimeoutsConfig)
        assert cfg.timeouts.anthropic_seconds == 300
        assert cfg.timeouts.github_seconds == 30
        assert cfg.timeouts.http_seconds == 30

    def test_timeouts_override(self, tmp_path):
        """When timeouts section is provided, values should override defaults."""
        data = _minimal_config_dict(
            timeouts={
                "anthropic_seconds": 600,
                "github_seconds": 60,
                "http_seconds": 45,
            }
        )
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.timeouts.anthropic_seconds == 600
        assert cfg.timeouts.github_seconds == 60
        assert cfg.timeouts.http_seconds == 45

    def test_timeouts_partial_override(self, tmp_path):
        """When only some timeout values are provided, others should use defaults."""
        data = _minimal_config_dict(
            timeouts={"anthropic_seconds": 500}
        )
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert cfg.timeouts.anthropic_seconds == 500
        assert cfg.timeouts.github_seconds == 30  # default
        assert cfg.timeouts.http_seconds == 30  # default

    def test_timeouts_types_are_int(self, tmp_path):
        """Timeout values should be integers."""
        data = _minimal_config_dict(
            timeouts={
                "anthropic_seconds": 300,
                "github_seconds": 30,
                "http_seconds": 30,
            }
        )
        cfg = load_config(_write_yaml(tmp_path / "c.yaml", data))
        assert isinstance(cfg.timeouts.anthropic_seconds, int)
        assert isinstance(cfg.timeouts.github_seconds, int)
        assert isinstance(cfg.timeouts.http_seconds, int)
