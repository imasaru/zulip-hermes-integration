"""Named-command mention routing must reach exactly the named bot.

Bug: in stream #Hermes, topic inbox-manager (owned by manager),
``/stop @**Treasurer (hermes-ai-agent)**`` was handled by BOTH treasurer and
manager. ``classify_command_target`` only matched an exact lowercase name
against the bot registry; manager does not know the label is a bot (the
`` (hermes-ai-agent)`` suffix breaks the match), so it returned ``none`` and
the command fell through to the topic owner.

These outcome tests drive ``inbound.handle_message`` the way the gateway
does. A command is processed only when an adapter dispatches it to the
gateway; a drop is no dispatch and no reply. Reuses the fakes from
``test_slash_command_routing``.
"""

from __future__ import annotations

import asyncio
import json
import time
from types import SimpleNamespace

import pytest

import zulip  # noqa: E402

from zulip import cron_ownership, inbound  # noqa: E402
from tests.test_slash_command_routing import (  # noqa: E402
    ProfileAdapter,
    _deliver,
    _assert_only,
    _stream,
    _unowned_history,
)


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _isolated_routing_state():
    from zulip.auto_reply import (
        reset_claims_for_tests,
        reset_topic_owner_cache_for_tests,
    )

    reset_claims_for_tests()
    reset_topic_owner_cache_for_tests()
    cron_ownership.reset_bot_registry_for_tests()
    cron_ownership.reset_indexes_for_tests()
    yield
    reset_claims_for_tests()
    reset_topic_owner_cache_for_tests()
    cron_ownership.reset_bot_registry_for_tests()
    cron_ownership.reset_indexes_for_tests()


@pytest.fixture
def routing_env(monkeypatch):
    monkeypatch.setenv("ZULIP_CHATMODE", "oncall")
    monkeypatch.setenv("ZULIP_REQUIRE_MENTION", "true")
    monkeypatch.setenv("ZULIP_AUTO_REPLY_ENABLED", "true")
    monkeypatch.setenv("ZULIP_AUTO_REPLY_FALLBACK_PROFILE", "ai-agent")
    monkeypatch.setenv("ZULIP_APPROVAL_AUTHORITY", "anyone")
    monkeypatch.setenv("ZULIP_SOFT_GATE", "false")
    monkeypatch.setenv("ZULIP_OBSERVE_GROUP", "false")
    monkeypatch.setenv("ZULIP_ENGAGEMENT_MODE", "off")


def _profiles_for(root, names):
    """Create the profile dirs under <root>/profiles and return that root."""
    profiles = root / "profiles"
    for name in names:
        (profiles / name).mkdir(parents=True)
    return profiles


def _own_topic_for(root, profile="manager", job_id="job-mgr"):
    """Write ``profile``'s cron/jobs.json and return a history that names it."""
    path = root / profile / "cron" / "jobs.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"jobs": [{"id": job_id}]}), encoding="utf-8")
    email = f"{profile}-bot@example.com"
    return {
        "result": "success",
        "messages": [
            {
                "id": 1,
                "sender_id": 999,
                "sender_email": email,
                "timestamp": time.time(),
                "content": f"briefing\n-# 🕒 cron:{job_id} run:run1\n",
            }
        ],
    }


def _adapters_for(profiles_root, history, names):
    return [
        ProfileAdapter(
            name,
            profiles_root,
            history,
            session_key=f"agent:{name}:zulip:thread:614901",
        )
        for name in names
    ]


def test_named_command_reaches_named_bot_not_owner(routing_env, tmp_path):
    """Several bots exist; only the named one answers, owner stays silent."""
    profiles = _profiles_for(tmp_path, ("manager", "treasurer", "warden", "messenger"))
    history = _own_topic_for(profiles, "manager")
    adapters = _adapters_for(profiles, history, ("manager", "treasurer", "warden", "messenger"))
    _run(_deliver(adapters, _stream("/stop @**treasurer**", 2001)))
    _assert_only(adapters, "treasurer", "/stop")


def test_suffix_mention_regression_without_id(routing_env, tmp_path):
    """Exact regression: @**Treasurer (hermes-ai-agent)** -> treasurer only."""
    profiles = _profiles_for(tmp_path, ("manager", "treasurer"))
    history = _own_topic_for(profiles, "manager")
    adapters = _adapters_for(profiles, history, ("manager", "treasurer"))
    _run(_deliver(adapters, _stream("/stop @**Treasurer (hermes-ai-agent)**", 2002)))
    _assert_only(adapters, "treasurer", "/stop")


def test_suffix_mention_regression_with_id(routing_env, tmp_path):
    """With a user id the label is classified by id first -> treasurer only."""
    profiles = _profiles_for(tmp_path, ("manager", "treasurer"))
    history = _own_topic_for(profiles, "manager")
    adapters = _adapters_for(profiles, history, ("manager", "treasurer"))
    treasurer = next(a for a in adapters if a.profile == "treasurer")
    treasurer._bot_user_id = 500
    cron_ownership.register_bot_identity(treasurer)
    _run(_deliver(adapters, _stream("/stop @**Treasurer|500**", 2003)))
    _assert_only(adapters, "treasurer", "/stop")


def test_unknown_bot_mention_still_keeps_owner_silent(routing_env, tmp_path):
    """The named bot is not a profile dir and not registered, yet the owner
    stays silent: a bot-like mention (with suffix) is ``other``."""
    profiles = _profiles_for(tmp_path, ("manager", "warden", "messenger"))
    history = _own_topic_for(profiles, "manager")
    adapters = _adapters_for(
        profiles, history, ("manager", "treasurer", "warden", "messenger")
    )
    _run(_deliver(adapters, _stream("/stop @**Treasurer (hermes-ai-agent)**", 2004)))
    _assert_only(adapters, "treasurer", "/stop")


def test_plain_mention_reaches_only_named_bot(routing_env, tmp_path):
    """A plain non-command stream @-mention reaches only the named bot."""
    profiles = _profiles_for(tmp_path, ("manager", "treasurer"))
    history = _own_topic_for(profiles, "manager")
    adapters = _adapters_for(profiles, history, ("manager", "treasurer"))
    _run(_deliver(adapters, _stream("morning notes ready @**treasurer**", 2005)))
    # The plain message is not a slash command; assert only treasurer acted.
    acted = [a.profile for a in adapters if a.dispatched]
    assert acted == ["treasurer"], acted


def test_unformatted_plain_mention_command(routing_env, tmp_path):
    """The unformed plain form ``/stop @treasurer (hermes-ai-agent)`` routes."""
    profiles = _profiles_for(tmp_path, ("manager", "warden", "messenger"))
    history = _own_topic_for(profiles, "manager")
    adapters = _adapters_for(
        profiles, history, ("manager", "treasurer", "warden", "messenger")
    )
    _run(_deliver(adapters, _stream("/stop @treasurer (hermes-ai-agent)", 2006)))
    _assert_only(adapters, "treasurer", "/stop")
