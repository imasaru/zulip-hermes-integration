"""Gateway-facing features ported from the feature-rich Zulip adapter work.

Covers:
- edit_message / supports_draft_streaming
- rename_topic / create_handoff_thread
- inbound stream topic routing (chat_type=thread + thread_id)
- slash command fallthrough for /stop vs local /help
- outbound topic resolution from metadata.thread_id
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def adapter(mock_platform_config, monkeypatch):
    import zulip.adapter as adapter_module
    from tests.conftest import MockZulipClient

    monkeypatch.setattr(adapter_module, "ZULIP_AVAILABLE", True)

    class MockZulipModule:
        class Client:
            def __init__(self, **kwargs):
                self._client = MockZulipClient(**kwargs)

            def __getattr__(self, name):
                return getattr(self._client, name)

    monkeypatch.setattr(adapter_module, "zulip", MockZulipModule())

    from zulip.adapter import ZulipAdapter

    ad = ZulipAdapter(mock_platform_config)
    ad.handle_message = AsyncMock()
    ad.build_source = MagicMock(
        side_effect=lambda **kw: SimpleNamespace(**kw, platform="zulip")
    )

    class FakeReactions:
        def __init__(self, *a, **k):
            pass

        async def start(self):
            pass

        async def success(self):
            pass

        async def error(self):
            pass

    monkeypatch.setattr(adapter_module, "ReactionLifecycle", FakeReactions)
    return ad


def _stream_msg(**overrides):
    base = {
        "id": 9,
        "type": "stream",
        "content": "hello",
        "sender_email": "user@example.com",
        "sender_id": 1,
        "sender_full_name": "User",
        "stream_id": 567,
        "display_recipient": "Hermes",
        "subject": "my-topic",
    }
    base.update(overrides)
    return base


class TestDraftStreaming:
    def test_supports_draft_streaming_default_true(self, adapter):
        assert adapter.supports_draft_streaming() is True

    def test_supports_draft_streaming_opt_out(self, adapter, monkeypatch):
        monkeypatch.setenv("ZULIP_EDIT_PLACEHOLDER", "false")
        assert adapter.supports_draft_streaming() is False

    @pytest.mark.asyncio
    async def test_edit_message_success(self, adapter):
        adapter.client.update_message = MagicMock(
            return_value={"result": "success"}
        )
        res = await adapter.edit_message("567", "100", "edited body")
        assert res.success is True
        assert res.message_id == "100"
        adapter.client.update_message.assert_called_once()


class TestTopicSync:
    @pytest.mark.asyncio
    async def test_rename_topic_success(self, adapter):
        adapter._stream_names["567"] = "Hermes"
        adapter.client.move_topic = MagicMock(
            return_value={"result": "success"}
        )
        ok, detail = await adapter.rename_topic("567", "old", "new-title")
        assert ok is True
        assert detail is None
        adapter.client.move_topic.assert_called_with(
            "Hermes", "Hermes", "old", "new-title"
        )

    @pytest.mark.asyncio
    async def test_rename_topic_dm_noop(self, adapter):
        ok, detail = await adapter.rename_topic("dm:123", "a", "b")
        assert ok is False
        assert "DM" in detail

    @pytest.mark.asyncio
    async def test_create_handoff_thread(self, adapter):
        adapter.client.send_message = MagicMock(
            return_value={"result": "success", "id": 1}
        )
        name = await adapter.create_handoff_thread("567", "Handoff A")
        assert name == "Handoff A"
        payload = adapter.client.send_message.call_args[0][0]
        assert payload["topic"] == "Handoff A"
        assert payload["type"] == "stream"


class TestInboundTopicRouting:
    @pytest.mark.asyncio
    async def test_stream_message_sets_thread_fields(self, adapter):
        await adapter._handle_message(_stream_msg())
        assert adapter.handle_message.await_count == 1
        event = adapter.handle_message.await_args.args[0]
        assert event.source.chat_type == "thread"
        assert event.source.thread_id == "my-topic"
        assert event.source.chat_topic == "my-topic"
        assert adapter._stream_names["567"] == "Hermes"


class TestSlashCommands:
    @pytest.mark.asyncio
    async def test_stop_falls_through_to_gateway(self, adapter):
        await adapter._handle_message(_stream_msg(content="/stop", id=10))
        assert adapter.handle_message.await_count == 1
        assert adapter.handle_message.await_args.args[0].text == "/stop"

    @pytest.mark.asyncio
    async def test_help_handled_locally(self, adapter):
        adapter.client.send_message = MagicMock(
            return_value={"result": "success", "id": 2}
        )
        await adapter._handle_message(_stream_msg(content="/help", id=11))
        assert adapter.handle_message.await_count == 0
        assert adapter.client.send_message.called


class TestOutboundTopic:
    @pytest.mark.asyncio
    async def test_send_uses_thread_id_metadata(self, adapter):
        adapter.client.send_message = MagicMock(
            return_value={"result": "success", "id": 77}
        )
        res = await adapter.send(
            "567", "hi", metadata={"thread_id": "topic-a"}
        )
        assert res.success is True
        payload = adapter.client.send_message.call_args[0][0]
        assert payload["topic"] == "topic-a"
