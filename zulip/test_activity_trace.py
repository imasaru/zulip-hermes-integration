"""Tests for Zulip adapter activity_trace module.

Covers:
1. TraceConfig.from_env() doesn't raise UnscopedSecretError during plugin load
   (before _profile_runtime_scope installs set_secret_scope)
2. TraceConfig.from_env() correctly resolves settings from env dict
3. TraceConfig defaults are correct
4. TraceConfig.allows_tool() matching logic
"""

import os
import pytest
from unittest.mock import patch
from zulip.activity_trace import TraceConfig


class TestTraceConfigFromEnv:
    """Tests for TraceConfig.from_env() resolution."""

    def test_from_env_with_explicit_env_dict(self):
        """from_env() should use the provided env dict directly."""
        env = {
            "ZULIP_ACTIVITY_TRACE": "1",
            "ZULIP_TRACE_COALESCE_MS": "500",
            "ZULIP_TRACE_MAX_RATE": "3.0",
            "ZULIP_TRACE_MAX_CONTENT": "4000",
            "ZULIP_TRACE_TOOL_MATCHER": "terminal,read",
        }
        cfg = TraceConfig.from_env(env)
        assert cfg.enabled is True
        assert cfg.coalesce_ms == 500
        assert cfg.max_rate == 3.0
        assert cfg.max_content == 4000
        assert cfg.tool_matcher == "terminal,read"

    def test_from_env_with_empty_env_dict(self):
        """from_env() with empty dict should use defaults."""
        cfg = TraceConfig.from_env({})
        assert cfg.enabled is False
        assert cfg.coalesce_ms == 400
        assert cfg.max_rate == 2.0
        assert cfg.max_content == 3500
        assert cfg.tool_matcher == ""

    def test_from_env_with_none_env_falls_back_to_os_environ(self):
        """from_env() with env=None should resolve from os.environ.

        This is the critical test: during plugin load, no profile scope is
        installed yet. The fix must fall back to os.environ instead of
        raising UnscopedSecretError.
        """
        os.environ["ZULIP_ACTIVITY_TRACE"] = "1"
        try:
            cfg = TraceConfig.from_env(None)
            assert cfg.enabled is True
        finally:
            del os.environ["ZULIP_ACTIVITY_TRACE"]

    def test_from_env_with_none_env_handles_missing_vars(self):
        """from_env() with env=None should handle missing env vars gracefully."""
        for k in ("ZULIP_ACTIVITY_TRACE", "ZULIP_TRACE_COALESCE_MS"):
            os.environ.pop(k, None)

        cfg = TraceConfig.from_env(None)
        assert cfg.enabled is False
        assert cfg.coalesce_ms == 400  # default

    def test_from_env_with_none_env_calls_runtime_scope_first(self):
        """from_env() with env=None should try runtime_scope.get_setting first."""
        with patch(
            "zulip.activity_trace.runtime_scope.get_setting",
            side_effect=Exception("No scope installed"),
        ):
            cfg = TraceConfig.from_env(None)
            # Should have fallen back to os.environ (which returns empty string)
            assert cfg.enabled is False

    def test_from_env_with_none_env_runtime_scope_returns_values(self):
        """from_env() with env=None should use runtime_scope values when available."""
        mock_get_setting = {
            "ZULIP_ACTIVITY_TRACE": "1",
            "ZULIP_TRACE_COALESCE_MS": "600",
            "ZULIP_TRACE_MAX_RATE": "5.0",
            "ZULIP_TRACE_MAX_CONTENT": "5000",
            "ZULIP_TRACE_TOOL_MATCHER": "!browser",
        }.get
        with patch(
            "zulip.activity_trace.runtime_scope.get_setting",
            side_effect=mock_get_setting,
        ):
            cfg = TraceConfig.from_env(None)
            assert cfg.enabled is True
            assert cfg.coalesce_ms == 600
            assert cfg.max_rate == 5.0
            assert cfg.max_content == 5000
            assert cfg.tool_matcher == "!browser"

    def test_from_env_defaults(self):
        """from_env() should use correct defaults when no env provided."""
        cfg = TraceConfig.from_env({})
        assert cfg.enabled is False
        assert cfg.coalesce_ms == 400
        assert cfg.max_rate == 2.0
        assert cfg.max_content == 3500
        assert cfg.tool_matcher == ""


class TestTraceConfigAllowsTool:
    """Tests for TraceConfig.allows_tool() matching logic."""

    def test_empty_matcher_allows_everything(self):
        """Empty tool_matcher allows all tools."""
        cfg = TraceConfig.from_env({})
        assert cfg.allows_tool("terminal") is True
        assert cfg.allows_tool("read") is True
        assert cfg.allows_tool("browser") is True

    def test_allowlist_only(self):
        """Plain names form an allowlist."""
        cfg = TraceConfig.from_env({
            "ZULIP_TRACE_TOOL_MATCHER": "terminal,read"
        })
        assert cfg.allows_tool("terminal") is True
        assert cfg.allows_tool("read") is True
        assert cfg.allows_tool("browser") is False
        assert cfg.allows_tool("unknown") is False

    def test_exclusion_prefix(self):
        """Names prefixed with ! are exclusions."""
        cfg = TraceConfig.from_env({
            "ZULIP_TRACE_TOOL_MATCHER": "!browser"
        })
        assert cfg.allows_tool("browser") is False
        assert cfg.allows_tool("terminal") is True
        assert cfg.allows_tool("read") is True

    def test_combined_allowlist_and_exclusion(self):
        """Both may be combined, and a denial always wins."""
        cfg = TraceConfig.from_env({
            "ZULIP_TRACE_TOOL_MATCHER": "terminal,!terminal"
        })
        # terminal is in allowlist but also in exclusion -> denied
        assert cfg.allows_tool("terminal") is False
        # browser is not in allowlist -> denied
        assert cfg.allows_tool("browser") is False

    def test_case_insensitive_matching(self):
        """Matching is exact and case-insensitive."""
        cfg = TraceConfig.from_env({
            "ZULIP_TRACE_TOOL_MATCHER": "TERMINAL"
        })
        assert cfg.allows_tool("terminal") is True
        assert cfg.allows_tool("Terminal") is True

    def test_unknown_tool_name_denied_with_allowlist(self):
        """With an allowlist, unknown tools are filtered out."""
        cfg = TraceConfig.from_env({
            "ZULIP_TRACE_TOOL_MATCHER": "terminal"
        })
        assert cfg.allows_tool("unknown_tool") is False
