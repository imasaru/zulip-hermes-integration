"""Tests for adapter.py stream trigger gating (onchar, oncall, mention)."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from zulip.adapter import _resolve_chatmode, _resolve_stream_overrides


class TestResolveChatmode:
    def test_default(self, monkeypatch):
        monkeypatch.delenv("ZULIP_CHATMODE", raising=False)
        monkeypatch.delenv("ZULIP_ONCHAR_PREFIXES", raising=False)
        monkeypatch.delenv("ZULIP_REQUIRE_MENTION", raising=False)
        mode, prefixes, require = _resolve_chatmode()
        assert mode == "onmessage"
        assert prefixes == [">", "!"]
        assert require is True

    def test_custom(self, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "onchar")
        monkeypatch.setenv("ZULIP_ONCHAR_PREFIXES", "?,@")
        monkeypatch.setenv("ZULIP_REQUIRE_MENTION", "false")
        mode, prefixes, require = _resolve_chatmode()
        assert mode == "onchar"
        assert prefixes == ["?", "@"]
        assert require is False

    def test_invalid_mode_fallback(self, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "invalid")
        mode, _, _ = _resolve_chatmode()
        assert mode == "onmessage"


class TestStreamGating:
    @pytest.fixture
    def adapter(self, mock_platform_config, monkeypatch):
        import zulip.adapter as adapter_module
        monkeypatch.setattr(adapter_module, "ZULIP_AVAILABLE", True)

        class MockZulipModule:
            class Client:
                def __init__(self, email=None, api_key=None, site=None):
                    pass

        monkeypatch.setattr(adapter_module, "zulip", MockZulipModule())
        from zulip.adapter import ZulipAdapter
        a = ZulipAdapter(mock_platform_config)
        a.email = "bot@zulip.com"  # for mention detection
        a.handle_message = AsyncMock()
        return a

    def _make_stream_msg(self, content: str) -> dict:
        return {
            "id": 1,
            "type": "stream",
            "stream_id": 1,
            "subject": "general",
            "display_recipient": "test",
            "content": content,
            "sender_email": "user@zulip.com",
            "sender_full_name": "User",
            "sender_id": 42,
        }

    @pytest.mark.asyncio
    async def test_onmessage_all_pass(self, adapter, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "onmessage")
        msg = self._make_stream_msg("hello world")
        await adapter._handle_message(msg)
        adapter.handle_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_oncall_mention_pass(self, adapter, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "oncall")
        msg = self._make_stream_msg("@bot hello")
        await adapter._handle_message(msg)
        adapter.handle_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_oncall_no_mention_drop(self, adapter, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "oncall")
        msg = self._make_stream_msg("hello world")
        await adapter._handle_message(msg)
        adapter.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_onchar_prefix_pass(self, adapter, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "onchar")
        msg = self._make_stream_msg("> help me")
        await adapter._handle_message(msg)
        adapter.handle_message.assert_called_once()
        # Verify prefix stripped
        call_args = adapter.handle_message.call_args[0][0]
        assert call_args.text == "help me"

    @pytest.mark.asyncio
    async def test_onchar_mention_pass(self, adapter, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "onchar")
        msg = self._make_stream_msg("@bot hello")
        await adapter._handle_message(msg)
        adapter.handle_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_onchar_no_trigger_drop(self, adapter, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "onchar")
        msg = self._make_stream_msg("hello world")
        await adapter._handle_message(msg)
        adapter.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_require_mention_true_blocks(self, adapter, monkeypatch):
        # onchar mode + require_mention: non-trigger, non-mention message blocked
        monkeypatch.setenv("ZULIP_CHATMODE", "onchar")
        monkeypatch.setenv("ZULIP_REQUIRE_MENTION", "true")
        msg = self._make_stream_msg("hello")
        await adapter._handle_message(msg)
        adapter.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_require_mention_false_passes(self, adapter, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "onmessage")
        monkeypatch.setenv("ZULIP_REQUIRE_MENTION", "false")
        msg = self._make_stream_msg("hello")
        await adapter._handle_message(msg)
        adapter.handle_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_dm_always_processed(self, adapter, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "oncall")  # strict mode
        msg = {
            "id": 1,
            "type": "private",
            "content": "hello",
            "sender_email": "user@zulip.com",
            "sender_full_name": "User",
            "sender_id": 42,
        }
        await adapter._handle_message(msg)
        adapter.handle_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_mention_stripped_from_content(self, adapter, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "onmessage")
        monkeypatch.setenv("ZULIP_REQUIRE_MENTION", "false")
        msg = self._make_stream_msg("@bot do this")
        await adapter._handle_message(msg)
        call_args = adapter.handle_message.call_args[0][0]
        assert "@bot" not in call_args.text
        assert call_args.text == "do this"


class TestResolveStreamOverrides:
    """ZULIP_STREAM_OVERRIDES parsing."""

    def test_empty_when_unset(self, monkeypatch):
        monkeypatch.delenv("ZULIP_STREAM_OVERRIDES", raising=False)
        assert _resolve_stream_overrides() == {}

    def test_parses_chatmode(self, monkeypatch):
        monkeypatch.setenv(
            "ZULIP_STREAM_OVERRIDES",
            '{"bot lab": {"chatmode": "onmessage"}, "alerts": {"chatmode": "oncall"}}',
        )
        assert _resolve_stream_overrides() == {
            "bot lab": {"chatmode": "onmessage"},
            "alerts": {"chatmode": "oncall"},
        }

    def test_keys_are_lowercased(self, monkeypatch):
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", '{"Bot Lab": {"chatmode": "oncall"}}')
        assert _resolve_stream_overrides() == {"bot lab": {"chatmode": "oncall"}}

    def test_stream_name_containing_colon(self, monkeypatch):
        monkeypatch.setenv(
            "ZULIP_STREAM_OVERRIDES", '{"team: general": {"chatmode": "onmessage"}}'
        )
        assert _resolve_stream_overrides() == {"team: general": {"chatmode": "onmessage"}}

    def test_require_mention_is_not_overridable(self, monkeypatch):
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", '{"a": {"requireMention": false}}')
        assert _resolve_stream_overrides() == {}

    def test_chatmode_kept_when_require_mention_also_present(self, monkeypatch):
        monkeypatch.setenv(
            "ZULIP_STREAM_OVERRIDES",
            '{"a": {"chatmode": "oncall", "requireMention": false}}',
        )
        assert _resolve_stream_overrides() == {"a": {"chatmode": "oncall"}}

    def test_setting_keys_are_case_insensitive(self, monkeypatch):
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", '{"a": {"chatMode": "oncall"}}')
        assert _resolve_stream_overrides() == {"a": {"chatmode": "oncall"}}

    def test_unrecognised_key_is_warned_and_ignored(self, monkeypatch, caplog):
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", '{"a": {"chatmodee": "oncall"}}')
        with caplog.at_level("WARNING"):
            assert _resolve_stream_overrides() == {}
        assert "unrecognised key" in caplog.text

    def test_invalid_json_ignored(self, monkeypatch):
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", "not json{")
        assert _resolve_stream_overrides() == {}

    def test_non_object_ignored(self, monkeypatch):
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", '["bot lab"]')
        assert _resolve_stream_overrides() == {}

    def test_non_object_entry_ignored(self, monkeypatch):
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", '{"a": "onmessage"}')
        assert _resolve_stream_overrides() == {}

    def test_unknown_chatmode_ignored(self, monkeypatch):
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", '{"a": {"chatmode": "nonsense"}}')
        assert _resolve_stream_overrides() == {}

    def test_cache_refreshes_when_env_changes(self, monkeypatch):
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", '{"a": {"chatmode": "oncall"}}')
        assert _resolve_stream_overrides() == {"a": {"chatmode": "oncall"}}
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", '{"a": {"chatmode": "onmessage"}}')
        assert _resolve_stream_overrides() == {"a": {"chatmode": "onmessage"}}


class TestChatmodeStreamOverride:
    """_resolve_chatmode() honours per-stream overrides."""

    def test_without_stream_name_uses_global(self, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "oncall")
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", '{"bot lab": {"chatmode": "onmessage"}}')
        mode, _, _ = _resolve_chatmode()
        assert mode == "oncall"

    def test_override_applies_to_named_stream(self, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "oncall")
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", '{"bot lab": {"chatmode": "onmessage"}}')
        mode, _, _ = _resolve_chatmode("bot lab")
        assert mode == "onmessage"

    def test_unlisted_stream_falls_back(self, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "oncall")
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", '{"bot lab": {"chatmode": "onmessage"}}')
        mode, _, _ = _resolve_chatmode("some other stream")
        assert mode == "oncall"

    def test_match_is_case_insensitive(self, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "oncall")
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", '{"bot lab": {"chatmode": "onmessage"}}')
        mode, _, _ = _resolve_chatmode("Bot Lab")
        assert mode == "onmessage"

    def test_global_require_mention_still_returned(self, monkeypatch):
        monkeypatch.setenv("ZULIP_REQUIRE_MENTION", "false")
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", '{"a": {"chatmode": "oncall"}}')
        _, _, require = _resolve_chatmode("a")
        assert require is False


class TestPerStreamGating:
    """End to end: the gate uses the override belonging to the message's stream."""

    @pytest.fixture
    def adapter(self, mock_platform_config, monkeypatch):
        import zulip.adapter as adapter_module
        monkeypatch.setattr(adapter_module, "ZULIP_AVAILABLE", True)

        class MockZulipModule:
            class Client:
                def __init__(self, email=None, api_key=None, site=None):
                    pass

        monkeypatch.setattr(adapter_module, "zulip", MockZulipModule())
        from zulip.adapter import ZulipAdapter
        a = ZulipAdapter(mock_platform_config)
        a.email = "bot@zulip.com"
        a.handle_message = AsyncMock()
        return a

    def _msg(self, content: str, stream: str) -> dict:
        return {
            "id": 1,
            "type": "stream",
            "stream_id": 1,
            "subject": "general",
            "display_recipient": stream,
            "content": content,
            "sender_email": "user@zulip.com",
            "sender_full_name": "User",
            "sender_id": 42,
        }

    @pytest.mark.asyncio
    async def test_overridden_stream_answers_without_mention(self, adapter, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "oncall")
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", '{"bot lab": {"chatmode": "onmessage"}}')
        await adapter._handle_message(self._msg("no mention here", "bot lab"))
        adapter.handle_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_other_stream_still_requires_mention(self, adapter, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "oncall")
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", '{"bot lab": {"chatmode": "onmessage"}}')
        await adapter._handle_message(self._msg("no mention here", "busy channel"))
        adapter.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_override_can_tighten_a_stream(self, adapter, monkeypatch):
        monkeypatch.setenv("ZULIP_CHATMODE", "onmessage")
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", '{"quiet room": {"chatmode": "oncall"}}')
        await adapter._handle_message(self._msg("no mention here", "quiet room"))
        adapter.handle_message.assert_not_called()

class TestGroupObservation:
    """Tests for ZULIP_OBSERVE_GROUP: stores non-addressed without triggering LLM."""

    @pytest.fixture
    def adapter(self, mock_platform_config, monkeypatch):
        import zulip.adapter as adapter_module
        monkeypatch.setattr(adapter_module, "ZULIP_AVAILABLE", True)

        class MockZulipModule:
            class Client:
                def __init__(self, email=None, api_key=None, site=None):
                    pass

        monkeypatch.setattr(adapter_module, "zulip", MockZulipModule())
        from zulip.adapter import ZulipAdapter
        a = ZulipAdapter(mock_platform_config)
        a.email = "bot@zulip.com"
        a.handle_message = AsyncMock()
        return a

    def _make_stream_msg(self, content: str, sender_name: str = "User") -> dict:
        return {
            "id": 42,
            "type": "stream",
            "stream_id": 7,
            "subject": "general",
            "display_recipient": "test",
            "content": content,
            "sender_email": "user@zulip.com",
            "sender_full_name": sender_name,
            "sender_id": 99,
            "timestamp": 1720000000,
        }

    @pytest.mark.asyncio
    async def test_observe_on_hard_gate_drop_no_llm(self, adapter, monkeypatch):
        # Enable observe, use hard gate oncall (no mention => drop)
        adapter._observe_group = True  # set post-init since env read at creation
        monkeypatch.setenv("ZULIP_CHATMODE", "oncall")

        mock_store = MagicMock()
        sess = MagicMock()
        sess.session_id = "test-sess-obs-1"
        mock_store.get_or_create_session.return_value = sess
        adapter._session_store = mock_store

        msg = self._make_stream_msg("just chatting here")
        await adapter._handle_message(msg)

        # Must NOT dispatch to LLM
        adapter.handle_message.assert_not_called()

        # Must have observed exactly once
        mock_store.get_or_create_session.assert_called_once()
        mock_store.append_to_transcript.assert_called_once()

        args = mock_store.append_to_transcript.call_args[0]
        assert args[0] == "test-sess-obs-1"
        entry = args[1]
        assert entry.get("observed") is True
        assert entry.get("role") == "user"
        assert "[User]" in entry.get("content", "")
        assert "just chatting here" in entry.get("content", "")
        assert "message_id" in entry

    @pytest.mark.asyncio
    async def test_observe_disabled_does_nothing(self, adapter, monkeypatch):
        adapter._observe_group = False
        monkeypatch.setenv("ZULIP_CHATMODE", "oncall")

        mock_store = MagicMock()
        adapter._session_store = mock_store

        msg = self._make_stream_msg("chatter ignored")
        await adapter._handle_message(msg)

        adapter.handle_message.assert_not_called()
        mock_store.append_to_transcript.assert_not_called()

    @pytest.mark.asyncio
    async def test_observe_soft_gate_never_observes(self, adapter, monkeypatch):
        # In soft gate, even non-mentioned are dispatched (addressed=False)
        adapter._observe_group = True
        adapter._soft_gate = True

        mock_store = MagicMock()
        adapter._session_store = mock_store

        msg = self._make_stream_msg("non mentioned in soft")
        await adapter._handle_message(msg)

        # Dispatched, no observe
        adapter.handle_message.assert_called_once()
        mock_store.append_to_transcript.assert_not_called()
