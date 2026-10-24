"""Stream slash commands must reach exactly one Zulip adapter.

Outcome tests drive ``inbound.handle_message`` the way the gateway does:
several profiles see one stream message. A command is processed only when
that adapter dispatches it to the gateway. A drop is no dispatch and no
reply.

DMs stay with the bot that was DM'd. In streams:

* an @-mention (or a mention flag) selects that bot, in either order;
* ``/approve`` and ``/deny`` follow the pending approval, not the topic owner;
* any other unnamed command follows the topic cron owner, or the fallback
  profile when the topic has none.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import zulip  # noqa: E402

from zulip import approval_routing, inbound  # noqa: E402
from zulip.adapter import ZulipAdapter  # noqa: E402
from zulip.approval_outcomes import ApprovalLedger  # noqa: E402
from zulip.auto_reply import (  # noqa: E402
    reset_claims_for_tests,
    reset_topic_owner_cache_for_tests,
)
from zulip.cron_ownership import (  # noqa: E402
    reset_bot_registry_for_tests,
    reset_indexes_for_tests,
)
from zulip.reactions import ReactionConfig  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _isolated_routing_state():
    reset_claims_for_tests()
    reset_topic_owner_cache_for_tests()
    reset_bot_registry_for_tests()
    reset_indexes_for_tests()
    yield
    reset_claims_for_tests()
    reset_topic_owner_cache_for_tests()
    reset_bot_registry_for_tests()
    reset_indexes_for_tests()


@pytest.fixture
def routing_env(monkeypatch):
    """Mention-gated streams, the posture issue #259 exists for."""
    monkeypatch.setenv("ZULIP_CHATMODE", "oncall")
    monkeypatch.setenv("ZULIP_REQUIRE_MENTION", "true")
    monkeypatch.setenv("ZULIP_AUTO_REPLY_ENABLED", "true")
    monkeypatch.setenv("ZULIP_AUTO_REPLY_FALLBACK_PROFILE", "ai-agent")
    monkeypatch.setenv("ZULIP_APPROVAL_AUTHORITY", "anyone")
    monkeypatch.setenv("ZULIP_SOFT_GATE", "false")
    monkeypatch.setenv("ZULIP_OBSERVE_GROUP", "false")
    monkeypatch.setenv("ZULIP_ENGAGEMENT_MODE", "off")


class _Client:
    def __init__(self, history):
        self.history = history
        self.sent = []

    def get_messages(self, _payload):
        return self.history

    def send_message(self, payload):
        self.sent.append(payload)
        return {"result": "success"}

    def add_reaction(self, _payload):
        return {"result": "success"}

    def update_message_flags(self, _payload):
        return {"result": "success"}


class _Audit:
    async def log_rate_limit_exceeded(self, **_kwargs):
        return None

    async def log_policy_block(self, **_kwargs):
        return None

    async def log_approval_rejected(self, **_kwargs):
        return None


class _Routing:
    def __init__(self):
        self._pending_conversations = {}
        self._conversations = None
        self._topic_cache = {}


class ProfileAdapter:
    """Enough of a ZulipAdapter for ``handle_message`` to run without I/O."""

    def __init__(self, profile, root, history, *, session_key):
        self.profile = profile
        self._owner_profile = profile
        self._data_dir = str(Path(root) / profile)
        self.email = f"{profile}-bot@example.com"
        self.bot_full_name = profile
        self._bot_user_id = {
            "homemanager": "101",
            "wellness": "202",
            "ai-agent": "303",
        }.get(profile, "9")
        self.session_key = session_key
        self.client = _Client(history)
        self.dispatched = []
        self._routing = _Routing()
        self._rate_limiter = SimpleNamespace(check=lambda _key: True, config={"max_per_minute": 60})
        self._audit_logger = _Audit()
        self._reaction_cfg = ReactionConfig(enabled=False)
        self._send_timeout = 5
        self._soft_gate = False
        self._engagement_cfg = SimpleNamespace(mode="off")
        self._streams_filter = None
        self._policy = SimpleNamespace(
            group_mode="open",
            mode="open",
            owner_email="",
            can_group_message=lambda _email: True,
            refresh_if_changed=lambda: None,
            check_dm=lambda _email: (True, None),
        )
        self._observe_group = False
        self._message_counts = {}
        self._last_message_time = {}
        self._last_topic_cache = {}
        self._dm_session_turn_limit = 0
        self._dm_base_message_counts = {}
        self._session_queue = None
        self._approval_ledger = ApprovalLedger()
        self._resolved_bot_owner_email = ""

    def _is_self_message(self, _message):
        return False

    async def _sdk_call(self, fn, payload=None, timeout=None):
        if payload is None:
            return fn()
        return fn(payload)

    async def _resolve_display_name(self, message):
        return message.get("sender_full_name") or "Evan"

    def build_source(self, **kwargs):
        return SimpleNamespace(**kwargs)

    async def _quoted_topic_history(self, *args, **kwargs):
        return ""

    async def _mark_read(self, _message_id):
        return None

    async def _stop_typing(self, _params):
        return None

    def _observe_stream_message(self, _message, _content):
        return None

    def _session_key_for_event(self, _event):
        return self.session_key

    def bot_owner_address(self):
        return self._resolved_bot_owner_email or self._policy.owner_email

    async def gate_approval_decision(self, **kwargs):
        return await ZulipAdapter.gate_approval_decision(self, **kwargs)

    def note_approval_decider(self, **kwargs):
        return ZulipAdapter.note_approval_decider(self, **kwargs)

    async def _dispatch_turn(self, event, reactions, typing_params, message_id):
        self.dispatched.append(event)


def _profiles(tmp_path):
    root = tmp_path / "profiles"
    for name in ("homemanager", "wellness", "ai-agent"):
        (root / name).mkdir(parents=True)
    return root


def _own_topic(root, job_id="job-hm"):
    path = root / "homemanager" / "cron" / "jobs.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"jobs": [{"id": job_id}]}), encoding="utf-8")
    return {
        "result": "success",
        "messages": [
            {
                "id": 1,
                "sender_id": 101,
                "sender_email": "homemanager-bot@example.com",
                "timestamp": time.time(),
                "content": f"briefing\n-# 🕒 cron:{job_id} run:run1\n",
            }
        ],
    }


def _unowned_history():
    return {"result": "success", "messages": []}


def _stream(content, mid, topic="morning-briefing"):
    return {
        "id": mid,
        "type": "stream",
        "content": content,
        "sender_email": "evan@example.com",
        "sender_id": 42,
        "sender_full_name": "Evan",
        "display_recipient": "Hermes",
        "stream_id": 614901,
        "subject": topic,
        "flags": [],
    }


def _adapters(root, history, names):
    return [
        ProfileAdapter(
            name,
            root,
            history,
            session_key=f"agent:{name}:zulip:thread:614901",
        )
        for name in names
    ]


async def _deliver(adapters, message):
    for adapter in adapters:
        await inbound.handle_message(adapter, dict(message))


def _texts(adapter):
    return [event.text for event in adapter.dispatched]


def _assert_only(adapters, profile, command):
    acted = [adapter.profile for adapter in adapters if adapter.dispatched]
    assert acted == [profile], (
        f"expected only {profile} to process {command!r}, got {acted} "
        f"with texts {[_texts(adapter) for adapter in adapters]}"
    )
    owner = next(adapter for adapter in adapters if adapter.profile == profile)
    assert len(owner.dispatched) == 1
    text = owner.dispatched[0].text.strip()
    assert text == command, f"{profile} dispatched {text!r}, expected {command!r}"
    assert "auto-reply" not in text.lower()
    for adapter in adapters:
        if adapter.profile == profile:
            continue
        assert adapter.dispatched == [], adapter.profile
        assert adapter.client.sent == [], (adapter.profile, adapter.client.sent)


def test_unnamed_stop_reaches_topic_owner_only(routing_env, tmp_path):
    root = _profiles(tmp_path)
    history = _own_topic(root)
    adapters = _adapters(root, history, ("homemanager", "wellness"))
    _run(_deliver(adapters, _stream("/stop", 1001)))
    _assert_only(adapters, "homemanager", "/stop")


def test_leading_mention_reaches_named_bot_not_topic_owner(routing_env, tmp_path):
    root = _profiles(tmp_path)
    history = _own_topic(root)
    adapters = _adapters(root, history, ("homemanager", "wellness"))
    _run(_deliver(adapters, _stream("@wellness /new", 1002)))
    _assert_only(adapters, "wellness", "/new")


def test_trailing_mention_reaches_named_bot(routing_env, tmp_path):
    root = _profiles(tmp_path)
    history = _own_topic(root)
    adapters = _adapters(root, history, ("homemanager", "wellness"))
    _run(_deliver(adapters, _stream("/new @**wellness**", 1003)))
    _assert_only(adapters, "wellness", "/new")


def test_command_mentioning_another_bot_is_dropped(routing_env, tmp_path):
    root = _profiles(tmp_path)
    history = _own_topic(root)
    adapters = _adapters(root, history, ("homemanager", "wellness"))
    _run(_deliver(adapters, _stream("/model @**wellness**", 1004)))
    homemanager = next(adapter for adapter in adapters if adapter.profile == "homemanager")
    assert homemanager.dispatched == []
    assert homemanager.client.sent == []
    _assert_only(adapters, "wellness", "/model")


def test_approve_reaches_only_profile_with_pending_approval(routing_env, tmp_path, monkeypatch):
    """Issue #259: /approve is not mention-gated away. Only the profile whose
    session holds the pending approval answers — even when another profile
    owns the topic.
    """
    root = _profiles(tmp_path)
    history = _own_topic(root)
    adapters = _adapters(root, history, ("homemanager", "wellness"))
    wellness = next(adapter for adapter in adapters if adapter.profile == "wellness")
    monkeypatch.setattr(
        approval_routing,
        "_host_has_blocking_approval",
        lambda key: key == wellness.session_key,
    )
    _run(_deliver(adapters, _stream("/approve", 1005)))
    _assert_only(adapters, "wellness", "/approve")


def test_unowned_topic_reaches_only_fallback_profile(routing_env, tmp_path):
    root = _profiles(tmp_path)
    history = _unowned_history()
    adapters = _adapters(root, history, ("homemanager", "wellness", "ai-agent"))
    _run(_deliver(adapters, _stream("/stop", 1006, topic="unowned-topic")))
    _assert_only(adapters, "ai-agent", "/stop")


def test_dm_reset_is_handled_by_the_dm_bot(routing_env, tmp_path):
    root = _profiles(tmp_path)
    history = _unowned_history()
    bot = _adapters(root, history, ("homemanager",))[0]
    message = {
        "id": 1007,
        "type": "private",
        "content": "/reset",
        "sender_email": "evan@example.com",
        "sender_id": 42,
        "sender_full_name": "Evan",
        "display_recipient": [
            {"id": 42, "email": "evan@example.com"},
            {"id": 101, "email": "homemanager-bot@example.com"},
        ],
    }
    _run(_deliver([bot], message))
    assert len(bot.dispatched) == 1
    assert bot.dispatched[0].text.strip() == "/reset"
    assert bot.client.sent == []
