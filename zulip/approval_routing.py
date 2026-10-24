"""Approval-decision routing: only the owning profile answers.

When the decider sends ``/approve`` or ``/deny`` (and the session/always/all
variants) in a Zulip topic, every one of the six adapters in the gateway
receives the message. Without routing, each adapter starts a turn and the
gateway answers "No pending command to approve." on the five profiles that
own nothing — five noise replies per human decision (seen live 2026-10-19
in #Hermes > morning-briefing).

The fix lives in the plugin's inbound chain — the Hermes core is untouched.
After the approval-authority check passes, the adapter asks the host whether
THIS profile's session has a blocking approval waiting, using
``tools.approval.has_blocking_approval`` — the exact predicate the gateway's
own ``/approve``/``/deny`` handlers consult before resolving. Only the owner
proceeds to the gateway; every other profile marks the message read and
stays silent.

The owner path is byte-for-byte the old behaviour: when this profile owns
the pending approval, the message flows to the gateway exactly as before.
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)

#: Signature of the host's pending-approval predicate.
HasPendingFn = Callable[[str], bool]


def _host_has_blocking_approval(session_key: str) -> bool:
    """The host's own predicate, resolved lazily so a broken import cannot
    take down module import."""
    from tools.approval import has_blocking_approval

    return bool(has_blocking_approval(session_key))


def profile_owns_approval(
    session_key: Optional[str],
    has_pending: Optional[HasPendingFn] = None,
) -> bool:
    """Whether this profile's session owns a pending approval decision.

    True  -> the decision is for us; let it proceed to the gateway.
    False -> the gateway would answer "no pending" here; stay silent.

    ``has_pending`` defaults to the host predicate and is injectable for
    tests. An empty session key never owns anything. A failed host read
    fails OPEN (answers True) with a warning: swallowing a real ``/approve``
    is worse than a noise reply, and the gateway still resolves correctly.
    """
    if not session_key:
        return False
    check = has_pending if has_pending is not None else _host_has_blocking_approval
    try:
        return bool(check(session_key))
    except Exception:
        logger.warning(
            "zulip approval routing: pending-approval read failed; answering",
            exc_info=True,
        )
        return True


async def _topic_owner_processes(
    adapter,
    message: dict,
    *,
    stream_name: str,
    message_id,
    label: str,
) -> bool:
    """Topic cron owner, or the fallback profile, wins this stream message.

    The per-message claim settles two adapters that resolve the same owner.
    A lost claim drops. A claim that cannot be recorded processes: dropping
    the only owner is worse than a duplicate. Unexpected errors propagate
    so the caller can choose fail-open or fail-closed.
    """
    from . import auto_reply as _ar
    from . import cron_ownership as _co

    _co.register_bot_identity(adapter)
    me = _co.own_profile_name(adapter)
    cfg = _ar.resolve_auto_reply_settings()
    stream_id = message.get("stream_id")
    topic = (message.get("subject") or "").strip()
    found, _api_ok = await _ar.resolve_topic_cron_owner(
        adapter,
        stream_id,
        topic,
        settings=cfg,
        stream_name=stream_name,
    )
    owner = (
        found.owner_profile
        if (found is not None and found.owner_profile)
        else cfg.fallback_profile
    )
    if owner != me:
        logger.debug(
            "zulip %s dropped: topic owner is %s, not %s",
            label,
            owner,
            me,
        )
        return False
    try:
        won = _ar.claim_message(message_id, me, adapter=adapter)
    except Exception:
        logger.debug("zulip %s claim failed; processing", label, exc_info=True)
        return True
    if not won:
        logger.info(
            "zulip %s claim lost [msg=%s owner=%s]",
            label,
            message_id,
            owner,
        )
        return False
    return True


async def should_process_topic_command(
    adapter,
    message: dict,
    *,
    stream_name: str,
    message_id,
) -> bool:
    """Whether this adapter owns an unnamed stream slash command.

    Same owner and claim as ``/yolo``, but a routing failure does not fan
    out: only the fallback profile proceeds, and if that cannot be resolved
    nobody does.
    """
    try:
        return await _topic_owner_processes(
            adapter,
            message,
            stream_name=stream_name,
            message_id=message_id,
            label="slash command",
        )
    except Exception:
        logger.debug("zulip slash command routing failed", exc_info=True)
        try:
            from . import auto_reply as _ar
            from . import cron_ownership as _co

            me = _co.own_profile_name(adapter)
            cfg = _ar.resolve_auto_reply_settings()
            return me == cfg.fallback_profile
        except Exception:
            logger.debug(
                "zulip slash command fallback lookup failed; dropping",
                exc_info=True,
            )
            return False


async def should_process_yolo(
    adapter,
    message: dict,
    event,
    *,
    msg_type: str,
    stream_name: str,
    message_id,
) -> bool:
    """Whether this adapter should process a ``/yolo`` command.

    ``/yolo`` toggles the dangerous-command approval bypass for one session,
    so answering on all six adapters toggles six sessions at once. Exactly
    the relevant profile(s) act:

    1. a profile with a pending approval for this session processes it
       (the "stop asking me" case — usually exactly one profile);
    2. otherwise, in a stream, only the topic's cron owner — or the
       configured fallback profile when the topic has no cron owner —
       processes it, with the per-message claim settling races;
    3. DMs are per-bot (no fan-out possible): always process.

    Never raises: any failure means "process", i.e. today's behaviour.
    """
    try:
        session_key = adapter._session_key_for_event(event) or ""
        if profile_owns_approval(session_key):
            return True
        if msg_type != "stream":
            return True
        return await _topic_owner_processes(
            adapter,
            message,
            stream_name=stream_name,
            message_id=message_id,
            label="/yolo",
        )
    except Exception:
        logger.debug("zulip /yolo routing failed; processing", exc_info=True)
        return True
