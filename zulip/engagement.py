"""Sticky topic engagement for Zulip stream conversations.

After a user @mentions the bot (or triggers onchar) on a stream topic,
subsequent messages from that user on the same topic are accepted without
another mention until the idle TTL expires or they explicitly stop.

Mirrors Slack/Discord "thread engagement" behaviour, using Zulip's
(stream_id, topic) as the conversation unit.
"""

from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# Modes
MODE_OFF = "off"
MODE_STICKY_TOPIC = "sticky_topic"
_VALID_MODES = frozenset({MODE_OFF, MODE_STICKY_TOPIC})

# Scope: who can continue an engaged topic without @mention
SCOPE_USER = "user"  # only the user who engaged
SCOPE_TOPIC = "topic"  # anyone posting in the engaged topic
_VALID_SCOPES = frozenset({SCOPE_USER, SCOPE_TOPIC})

DEFAULT_TTL_MINUTES = 45
DEFAULT_EXPIRY_SCAN_SECONDS = 30

# Explicit stop phrases (whole-message, case-insensitive)
# Bare "/stop" is reserved for Hermes gateway interrupt — do NOT claim it here.
# Use explicit "stop listening" / "/unlisten" for engagement-only exit.
_STOP_COMMANDS = frozenset(
    {
        "stop listening",
        "stop listen",
        "/unlisten",
        "/stop-listening",
        "/stop_listening",
        "unsubscribe",
        "/unsubscribe",
    }
)

# /new is Hermes session reset — also clear engagement
_END_SESSION_COMMANDS = frozenset({"/new", "/reset", "new", "reset"})


def _truthy_list(raw: str) -> set[str]:
    if not raw or not raw.strip():
        return set()
    return {p.strip() for p in raw.split(",") if p.strip()}


def _env_truthy(name: str, default: str = "true") -> bool:
    raw = os.getenv(name, default).strip().lower()
    return raw not in ("false", "0", "", "no", "off")


@dataclass
class EngagementConfig:
    """Runtime engagement settings from env."""

    mode: str = MODE_STICKY_TOPIC
    scope: str = SCOPE_USER
    ttl_seconds: float = DEFAULT_TTL_MINUTES * 60
    free_response_streams: set[str] = field(default_factory=set)
    expiry_notice: bool = True
    expiry_scan_seconds: float = DEFAULT_EXPIRY_SCAN_SECONDS

    @classmethod
    def from_env(cls) -> "EngagementConfig":
        mode = os.getenv("ZULIP_ENGAGEMENT_MODE", MODE_STICKY_TOPIC).strip().lower()
        if mode not in _VALID_MODES:
            # Accept common aliases
            if mode in ("sticky", "on", "true", "1", "yes"):
                mode = MODE_STICKY_TOPIC
            elif mode in ("false", "0", "no", "disabled", "disable"):
                mode = MODE_OFF
            else:
                mode = MODE_STICKY_TOPIC

        scope = os.getenv("ZULIP_ENGAGEMENT_SCOPE", SCOPE_USER).strip().lower()
        if scope not in _VALID_SCOPES:
            scope = SCOPE_USER

        ttl_raw = os.getenv("ZULIP_ENGAGEMENT_TTL_MINUTES", str(DEFAULT_TTL_MINUTES)).strip()
        try:
            ttl_min = float(ttl_raw)
            if ttl_min <= 0:
                ttl_min = DEFAULT_TTL_MINUTES
        except ValueError:
            ttl_min = DEFAULT_TTL_MINUTES

        free = _truthy_list(os.getenv("ZULIP_FREE_RESPONSE_STREAMS", ""))
        # Also accept topic-qualified free response later if needed
        free |= _truthy_list(os.getenv("ZULIP_FREE_RESPONSE_CHANNELS", ""))

        scan_raw = os.getenv(
            "ZULIP_ENGAGEMENT_EXPIRY_SCAN_SECONDS",
            str(DEFAULT_EXPIRY_SCAN_SECONDS),
        ).strip()
        try:
            scan_s = float(scan_raw)
            if scan_s < 5:
                scan_s = DEFAULT_EXPIRY_SCAN_SECONDS
        except ValueError:
            scan_s = DEFAULT_EXPIRY_SCAN_SECONDS

        return cls(
            mode=mode,
            scope=scope,
            ttl_seconds=ttl_min * 60.0,
            free_response_streams=free,
            expiry_notice=_env_truthy("ZULIP_ENGAGEMENT_EXPIRY_NOTICE", "true"),
            expiry_scan_seconds=scan_s,
        )


@dataclass
class EngagementEntry:
    """One active engagement on a stream topic."""

    stream_id: str
    topic: str
    user_email: str  # lowercase; who opened the engagement
    last_active: float
    opened_at: float
    user_name: str = ""  # display name for optional @ in expiry notices


class TopicEngagementStore:
    """In-memory sticky engagement store (per gateway process)."""

    def __init__(self, config: Optional[EngagementConfig] = None):
        self.config = config or EngagementConfig.from_env()
        # key: "stream_id\\0topic\\0user_email" for user scope
        #      "stream_id\\0topic" for topic scope
        self._entries: dict[str, EngagementEntry] = {}

    def _topic_key(self, stream_id: str | int, topic: str) -> str:
        return f"{stream_id}\0{(topic or '').strip().lower()}"

    def _user_key(self, stream_id: str | int, topic: str, user_email: str) -> str:
        return f"{self._topic_key(stream_id, topic)}\0{(user_email or '').strip().lower()}"

    def _storage_key(self, stream_id: str | int, topic: str, user_email: str) -> str:
        if self.config.scope == SCOPE_TOPIC:
            return self._topic_key(stream_id, topic)
        return self._user_key(stream_id, topic, user_email)

    def _is_expired(self, entry: EngagementEntry, now: float) -> bool:
        return (now - entry.last_active) > self.config.ttl_seconds

    def is_free_response_stream(self, stream_id: str | int) -> bool:
        return str(stream_id) in self.config.free_response_streams

    def is_engaged(
        self,
        stream_id: str | int,
        topic: str,
        user_email: str,
        *,
        now: Optional[float] = None,
    ) -> bool:
        """Return True if engagement is active.

        Expired entries are left in place for the background expiry scanner
        so it can post a notice; they still count as not engaged here.
        """
        if self.config.mode == MODE_OFF:
            return False
        now = now if now is not None else time.time()
        key = self._storage_key(stream_id, topic, user_email)
        entry = self._entries.get(key)
        if entry is None:
            return False
        if self._is_expired(entry, now):
            return False
        if self.config.scope == SCOPE_USER:
            if entry.user_email != (user_email or "").strip().lower():
                return False
        return True

    def mark_engaged(
        self,
        stream_id: str | int,
        topic: str,
        user_email: str,
        *,
        user_name: str = "",
        now: Optional[float] = None,
    ) -> None:
        if self.config.mode == MODE_OFF:
            return
        now = now if now is not None else time.time()
        email = (user_email or "").strip().lower()
        name = (user_name or "").strip()
        key = self._storage_key(stream_id, topic, email)
        existing = self._entries.get(key)
        if existing:
            existing.last_active = now
            if self.config.scope == SCOPE_USER:
                existing.user_email = email
            if name:
                existing.user_name = name
        else:
            self._entries[key] = EngagementEntry(
                stream_id=str(stream_id),
                topic=(topic or "").strip(),
                user_email=email,
                last_active=now,
                opened_at=now,
                user_name=name,
            )
        logger.debug(
            "zulip engagement open/refresh [stream=%s topic=%s user=%s scope=%s]",
            stream_id,
            topic,
            email,
            self.config.scope,
        )

    def touch(
        self,
        stream_id: str | int,
        topic: str,
        user_email: str,
        *,
        now: Optional[float] = None,
    ) -> None:
        """Refresh TTL without changing opener."""
        if self.config.mode == MODE_OFF:
            return
        now = now if now is not None else time.time()
        key = self._storage_key(stream_id, topic, user_email)
        entry = self._entries.get(key)
        if entry and not self._is_expired(entry, now):
            entry.last_active = now

    def clear(
        self,
        stream_id: str | int,
        topic: str,
        user_email: str | None = None,
    ) -> bool:
        """Clear engagement for this topic (and user if user-scope).

        Silent — no expiry notice (used for /stop and /new).
        Returns True if something was removed.
        """
        removed = False
        if self.config.scope == SCOPE_TOPIC or user_email is None:
            key = self._topic_key(stream_id, topic)
            # topic scope single key
            if key in self._entries:
                del self._entries[key]
                removed = True
            # also clear any user-scoped keys for this topic
            prefix = self._topic_key(stream_id, topic) + "\0"
            for k in list(self._entries.keys()):
                if k == key or k.startswith(prefix):
                    del self._entries[k]
                    removed = True
        else:
            key = self._user_key(stream_id, topic, user_email)
            if key in self._entries:
                del self._entries[key]
                removed = True
        if removed:
            logger.info(
                "zulip engagement cleared [stream=%s topic=%s user=%s]",
                stream_id,
                topic,
                user_email,
            )
        return removed

    def pop_expired(self, *, now: Optional[float] = None) -> list[EngagementEntry]:
        """Remove and return all TTL-expired engagements (for expiry notices)."""
        now = now if now is not None else time.time()
        expired: list[EngagementEntry] = []
        for key, entry in list(self._entries.items()):
            if self._is_expired(entry, now):
                expired.append(entry)
                del self._entries[key]
        return expired

    def active_count(self) -> int:
        now = time.time()
        return sum(1 for e in self._entries.values() if not self._is_expired(e, now))


def format_expiry_notice_text(
    *,
    ttl_minutes: int,
    bot_display_name: str = "",
    user_names: list[str] | None = None,
) -> str:
    """Notice posted when sticky engagement expires on a topic."""
    ttl_min = max(1, int(ttl_minutes))
    who = ""
    names = [n for n in (user_names or []) if n]
    if names:
        uniq = list(dict.fromkeys(names))
        if len(uniq) == 1:
            who = f"@**{uniq[0]}** "
        elif len(uniq) <= 3:
            who = " ".join(f"@**{n}**" for n in uniq) + " "

    if bot_display_name:
        call_to_action = (
            f"@mention @**{bot_display_name}** if you want me to pick up the conversation."
        )
    else:
        call_to_action = (
            "@mention me again if you want me to pick up the conversation."
        )

    return (
        f"{who}I stopped auto-listening on this topic after "
        f"**{ttl_min}m** of quiet.\n\n"
        f"{call_to_action}"
    )


def is_stop_listening_message(content: str) -> bool:
    """True if the user is asking the bot to stop listening on this topic."""
    text = (content or "").strip().lower()
    if not text:
        return False
    # exact match
    if text in _STOP_COMMANDS:
        return True
    # engagement-specific slash prefixes
    if text.startswith("/unlisten") or text.startswith("/stop-listening") or text.startswith("/stop_listening"):
        return True
    # natural phrases — require "listening" so bare "stop" does not steal gateway /stop
    if re.fullmatch(r"(please\s+)?stop\s+listening[.!]*", text):
        return True
    return False


def is_end_session_message(content: str) -> bool:
    """True for /new or /reset style session enders that should clear engagement."""
    text = (content or "").strip().lower()
    if not text:
        return False
    first = text.split(maxsplit=1)[0]
    return first in _END_SESSION_COMMANDS or text in _END_SESSION_COMMANDS
