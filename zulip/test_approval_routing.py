"""Tests for approval-decision routing (only the owning profile answers).

Covers:
1. profile_owns_approval with injected predicates (owner / non-owner /
   empty key / failing host read -> fail open).
2. The real host predicate (tools.approval.has_blocking_approval) resolves
   and answers False for a bogus session key — validates the import path
   the plugin will use in the live gateway process.
3. is_approval_decision matches every /approve and /deny variant the
   gateway accepts, and rejects lookalikes — the routing gate must see
   exactly the messages the gateway would act on.
"""

import sys

import pytest

sys.path.insert(0, "/usr/local/lib/hermes-agent")
sys.path.insert(0, "/root/.hermes/profiles/manager/plugins")

from zulip import approval_routing  # noqa: E402
from zulip.approval_routing import profile_owns_approval  # noqa: E402
from zulip.commands import is_approval_decision, is_yolo_command  # noqa: E402


# --- 1. routing predicate -------------------------------------------------


def test_owner_with_pending_approval_answers():
    assert profile_owns_approval("agent:main:zulip:1", has_pending=lambda k: True) is True


def test_non_owner_without_pending_stays_silent():
    assert profile_owns_approval("agent:main:zulip:2", has_pending=lambda k: False) is False


def test_empty_session_key_never_owns():
    called = []
    assert profile_owns_approval("", has_pending=lambda k: called.append(k) or True) is False
    assert profile_owns_approval(None, has_pending=lambda k: called.append(k) or True) is False
    assert called == []  # predicate must not even be consulted


def test_predicate_receives_the_session_key():
    seen = []
    profile_owns_approval("agent:main:zulip:9", has_pending=lambda k: seen.append(k) or False)
    assert seen == ["agent:main:zulip:9"]


def test_failing_host_read_fails_open():
    def boom(key):
        raise RuntimeError("core exploded")

    # Swallowing a real /approve is worse than a noise reply.
    assert profile_owns_approval("agent:main:zulip:3", has_pending=boom) is True


def test_falsy_predicate_result_is_not_owner():
    assert profile_owns_approval("k", has_pending=lambda k: 0) is False
    assert profile_owns_approval("k", has_pending=lambda k: None) is False


# --- 2. real host predicate ------------------------------------------------


def test_host_predicate_resolves_and_denies_bogus_key():
    # tools.approval.has_blocking_approval is the exact predicate the
    # gateway's /approve handler consults. A bogus key must answer False
    # without raising — this is the live code path the plugin will call.
    assert approval_routing._host_has_blocking_approval("zulip-routing-test:bogus") is False


def test_profile_owns_approval_default_path_uses_host():
    # Default (no injected predicate) goes through the real core.
    assert profile_owns_approval("zulip-routing-test:bogus") is False


# --- 3. command variants ----------------------------------------------------

APPROVAL_VARIANTS = [
    "/approve",
    "/deny",
    "/APPROVE",
    "/Deny",
    "/approve session",
    "/approve always",
    "/approve all",
    "/approve all session",
    "/deny all",
    "/deny all extra words here",
    "/deny because reasons",
    "  /approve  ",  # surrounding whitespace
    "/approve\tsession",  # tab separator
]

NOT_APPROVALS = [
    "",
    "   ",
    "approve",  # no slash
    "/approvals",  # the mode command — must NOT match
    "/approvals show",
    "/approved",
    "/approvex",
    "/denyall",
    "please /approve this",  # not at start
    "/help",
    "/status",
]


@pytest.mark.parametrize("text", APPROVAL_VARIANTS)
def test_all_decision_variants_match(text):
    assert is_approval_decision(text) is True, text


@pytest.mark.parametrize("text", NOT_APPROVALS)
def test_lookalikes_do_not_match(text):
    assert is_approval_decision(text) is False, text


def test_routing_predicate_is_command_variant_agnostic():
    # The predicate only cares about ownership, never the verb: every
    # variant above reaches the same gate, so owner/non-owner holds for all.
    for text in APPROVAL_VARIANTS:
        assert is_approval_decision(text)
    assert profile_owns_approval("k", has_pending=lambda k: True) is True
    assert profile_owns_approval("k", has_pending=lambda k: False) is False


# --- 4. /yolo routing --------------------------------------------------------

import asyncio  # noqa: E402
import types  # noqa: E402


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class _StubAdapter:
    def __init__(self, session_key="agent:manager:zulip:thread:1"):
        self._sk = session_key

    def _session_key_for_event(self, event):
        return self._sk


def _stub_topic_modules(monkeypatch, *, owner_profile=None, fallback="ai-agent", claim_won=True):
    """Stub zulip.auto_reply / zulip.cron_ownership for should_process_yolo."""
    ar = types.ModuleType("zulip.auto_reply")

    class _Owner:
        def __init__(self, owner_profile):
            self.owner_profile = owner_profile

    async def _resolve(adapter, stream_id, topic, settings=None, stream_name=""):
        return (_Owner(owner_profile) if owner_profile else None, True)

    class _Settings:
        fallback_profile = fallback

    ar.resolve_topic_cron_owner = _resolve
    ar.resolve_auto_reply_settings = lambda: _Settings()
    ar.claim_message = lambda message_id, profile, adapter=None: claim_won

    co = types.ModuleType("zulip.cron_ownership")
    co.register_bot_identity = lambda adapter: None
    co.own_profile_name = lambda adapter: "manager"

    monkeypatch.setitem(sys.modules, "zulip.auto_reply", ar)
    monkeypatch.setitem(sys.modules, "zulip.cron_ownership", co)
    # `from . import <mod>` also consults the package attribute, which the
    # real package __init__ chain may have populated — patch both.
    import zulip as _zp

    monkeypatch.setattr(_zp, "auto_reply", ar, raising=False)
    monkeypatch.setattr(_zp, "cron_ownership", co, raising=False)


def _yolo_msg():
    return {"stream_id": "614901", "subject": "morning-briefing", "sender_email": "evan@example.com"}


def test_yolo_pending_approval_processes(monkeypatch):
    monkeypatch.setattr(
        approval_routing, "profile_owns_approval", lambda sk: True
    )
    ok = _run(
        approval_routing.should_process_yolo(
            _StubAdapter(), _yolo_msg(), object(),
            msg_type="stream", stream_name="Hermes", message_id="111",
        )
    )
    assert ok is True


def test_yolo_dm_always_processes(monkeypatch):
    monkeypatch.setattr(
        approval_routing, "profile_owns_approval", lambda sk: False
    )
    ok = _run(
        approval_routing.should_process_yolo(
            _StubAdapter(), _yolo_msg(), object(),
            msg_type="private", stream_name="", message_id="112",
        )
    )
    assert ok is True


def test_yolo_stream_no_pending_topic_owner_processes(monkeypatch):
    monkeypatch.setattr(
        approval_routing, "profile_owns_approval", lambda sk: False
    )
    _stub_topic_modules(monkeypatch, owner_profile="manager", claim_won=True)
    ok = _run(
        approval_routing.should_process_yolo(
            _StubAdapter(), _yolo_msg(), object(),
            msg_type="stream", stream_name="Hermes", message_id="113",
        )
    )
    assert ok is True


def test_yolo_stream_no_pending_other_owner_drops(monkeypatch):
    monkeypatch.setattr(
        approval_routing, "profile_owns_approval", lambda sk: False
    )
    _stub_topic_modules(monkeypatch, owner_profile="treasurer")
    ok = _run(
        approval_routing.should_process_yolo(
            _StubAdapter(), _yolo_msg(), object(),
            msg_type="stream", stream_name="Hermes", message_id="114",
        )
    )
    assert ok is False


def test_yolo_stream_no_cron_owner_fallback_processes(monkeypatch):
    monkeypatch.setattr(
        approval_routing, "profile_owns_approval", lambda sk: False
    )
    # no cron owner; fallback is ai-agent, but "me" is manager -> drop
    _stub_topic_modules(monkeypatch, owner_profile=None, fallback="ai-agent")
    ok = _run(
        approval_routing.should_process_yolo(
            _StubAdapter(), _yolo_msg(), object(),
            msg_type="stream", stream_name="Hermes", message_id="115",
        )
    )
    assert ok is False


def test_yolo_claim_lost_drops(monkeypatch):
    monkeypatch.setattr(
        approval_routing, "profile_owns_approval", lambda sk: False
    )
    _stub_topic_modules(monkeypatch, owner_profile="manager", claim_won=False)
    ok = _run(
        approval_routing.should_process_yolo(
            _StubAdapter(), _yolo_msg(), object(),
            msg_type="stream", stream_name="Hermes", message_id="116",
        )
    )
    assert ok is False


def test_yolo_routing_failure_fails_open(monkeypatch):
    class _BrokenAdapter:
        def _session_key_for_event(self, event):
            raise RuntimeError("nope")

    ok = _run(
        approval_routing.should_process_yolo(
            _BrokenAdapter(), _yolo_msg(), object(),
            msg_type="stream", stream_name="Hermes", message_id="117",
        )
    )
    assert ok is True


YOLO_VARIANTS = ["/yolo", "/YOLO", "/yolo now", "  /yolo  "]
NOT_YOLO = ["", "/yolos", "/yolox", "/approve", "/yol", "yolo"]


@pytest.mark.parametrize("text", YOLO_VARIANTS)
def test_yolo_variants_match(text):
    assert is_yolo_command(text) is True, text


@pytest.mark.parametrize("text", NOT_YOLO)
def test_yolo_lookalikes_do_not_match(text):
    assert is_yolo_command(text) is False, text
