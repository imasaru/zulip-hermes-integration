"""Zulip platform adapter (Hermes plugin).

Bi-directional integration with Zulip: stream messages (with topics) and
private messages. Ships as a Hermes platform plugin under
``~/.hermes/plugins/zulip-hermes-integration/``.

Configuration (``config.yaml``)::

    gateway:
      platforms:
        zulip:
          enabled: true
          api_key: "..."
          extra:
            email: bot@example.zulipchat.com
            site: https://example.zulipchat.com
          home_channel:
            platform: zulip
            chat_id: "12345"
            name: general

Environment variables (env wins over YAML)::

    ZULIP_API_KEY, ZULIP_EMAIL, ZULIP_SITE
    ZULIP_ALLOWED_USERS, ZULIP_ALLOW_ALL_USERS
    ZULIP_HOME_CHANNEL, ZULIP_HOME_CHANNEL_NAME
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    import zulip

    ZULIP_AVAILABLE = True
except ImportError:
    ZULIP_AVAILABLE = False
    zulip = None  # type: ignore[assignment]

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

logger = logging.getLogger(__name__)

MAX_MESSAGE_LENGTH = 10000  # Zulip message body limit
RECONNECT_BACKOFF = [2, 5, 10, 30, 60]
_MENTION_RE = re.compile(r"\*\*([^*]+)\*\*")


# ---------------------------------------------------------------------------
# Requirements / config helpers
# ---------------------------------------------------------------------------


def check_requirements() -> bool:
    """Return True when the ``zulip`` package is importable."""
    return ZULIP_AVAILABLE


def _env(*names: str) -> str:
    """Return the first non-empty env var among *names*."""
    for name in names:
        val = os.getenv(name)
        if val is not None and str(val).strip():
            return str(val).strip()
    return ""


def _normalize_env_aliases() -> None:
    """Copy common alias env vars onto the canonical Hermes names.

    Safe to call multiple times; never overwrites a set canonical var.
    """
    if not os.getenv("ZULIP_EMAIL"):
        alias = _env("ZULIP_BOT_EMAIL")
        if alias:
            os.environ["ZULIP_EMAIL"] = alias
    if not os.getenv("ZULIP_SITE"):
        alias = _env("ZULIP_SITE_URL")
        if alias:
            os.environ["ZULIP_SITE"] = alias.rstrip("/")
    if not os.getenv("ZULIP_ALLOWED_USERS"):
        alias = _env("ZULIP_ALLOWED_EMAILS")
        if alias:
            os.environ["ZULIP_ALLOWED_USERS"] = alias


def validate_config(config) -> bool:
    """Validate that credentials are present (env or PlatformConfig)."""
    creds = _resolve_credentials(config)
    return bool(creds["api_key"] and creds["email"] and creds["site"])


def is_connected(config) -> bool:
    """Configured enough to consider Zulip "connected" for status UI."""
    return validate_config(config)


def _resolve_credentials(config: Optional[PlatformConfig] = None) -> Dict[str, str]:
    """Resolve api_key / email / site from env first, then config.

    Accepts both the canonical Hermes names and common aliases used by
    earlier Zulip plugin forks:

    * ``ZULIP_API_KEY``
    * ``ZULIP_EMAIL`` / ``ZULIP_BOT_EMAIL``
    * ``ZULIP_SITE`` / ``ZULIP_SITE_URL``
    """
    extra = (getattr(config, "extra", None) or {}) if config is not None else {}
    api_key = (
        _env("ZULIP_API_KEY")
        or (getattr(config, "api_key", None) if config is not None else None)
        or extra.get("api_key")
        or ""
    )
    email = (
        _env("ZULIP_EMAIL", "ZULIP_BOT_EMAIL")
        or extra.get("email")
        or extra.get("bot_email")
        or ""
    )
    site = (
        _env("ZULIP_SITE", "ZULIP_SITE_URL")
        or extra.get("site")
        or extra.get("site_url")
        or ""
    )
    return {
        "api_key": str(api_key).strip(),
        "email": str(email).strip(),
        "site": str(site).strip().rstrip("/"),
    }


def _make_client(creds: Dict[str, str]) -> "zulip.Client":
    if not ZULIP_AVAILABLE:
        raise ImportError("Zulip package not installed. Run: pip install zulip")
    missing = [k for k in ("api_key", "email", "site") if not creds.get(k)]
    if missing:
        raise ValueError(f"Zulip credentials incomplete. Missing: {', '.join(missing)}")
    return zulip.Client(
        email=creds["email"],
        api_key=creds["api_key"],
        site=creds["site"],
    )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class ZulipAdapter(BasePlatformAdapter):
    """Zulip platform adapter for Hermes Gateway."""

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH
    supports_code_blocks = True
    splits_long_messages = True

    def __init__(self, config: PlatformConfig):
        platform = Platform("zulip")
        super().__init__(config=config, platform=platform)

        if not ZULIP_AVAILABLE:
            raise ImportError("Zulip package not installed. Run: pip install zulip")

        creds = _resolve_credentials(config)
        self.api_key = creds["api_key"]
        self.email = creds["email"]
        self.site = creds["site"]

        if not (self.api_key and self.email and self.site):
            raise ValueError(
                "Zulip credentials incomplete. Set ZULIP_API_KEY, ZULIP_EMAIL, "
                "ZULIP_SITE (or platforms.zulip.api_key + extra.email/site in config.yaml)"
            )

        extra = config.extra or {}
        self.home_channel = (
            _env("ZULIP_HOME_CHANNEL")
            or (
                str(config.home_channel.chat_id)
                if getattr(config, "home_channel", None)
                else None
            )
            or extra.get("home_channel")
            or ""
        )
        self.home_channel_name = (
            _env("ZULIP_HOME_CHANNEL_NAME")
            or (
                config.home_channel.name
                if getattr(config, "home_channel", None)
                and getattr(config.home_channel, "name", None)
                else None
            )
            or extra.get("home_channel_name")
            or "general"
        )

        self.client = _make_client(creds)
        self._event_task: Optional[asyncio.Task] = None
        self._own_user_id: Optional[int] = None
        # stream_id (str) -> stream name, filled from inbound messages
        self._stream_names: Dict[str, str] = {}

    # -- Connection lifecycle ------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Test credentials and start the event listener."""
        logger.info(
            "[%s] Connecting to %s as %s (reconnect=%s)",
            self.name,
            self.site,
            self.email,
            is_reconnect,
        )
        try:
            result = await asyncio.to_thread(self.client.get_profile)
            if result.get("result") != "success":
                msg = result.get("msg") or str(result)
                logger.error("[%s] Connection failed: %s", self.name, msg)
                self._set_fatal_error("connect_failed", msg, retryable=True)
                return False
            self._own_user_id = result.get("user_id")
            logger.info(
                "[%s] Authenticated as %s (user_id=%s)",
                self.name,
                result.get("email") or self.email,
                self._own_user_id,
            )
        except Exception as e:
            logger.error("[%s] Connection error: %s", self.name, e)
            self._set_fatal_error("connect_failed", str(e), retryable=True)
            return False

        self._mark_connected()
        if self._event_task is None or self._event_task.done():
            self._event_task = asyncio.create_task(self._listen_for_events())
        return True

    async def disconnect(self) -> None:
        """Stop the event listener."""
        self._mark_disconnected()
        if self._event_task and not self._event_task.done():
            self._event_task.cancel()
            # Long-poll threads may not abort instantly — don't block shutdown.
            try:
                await asyncio.wait_for(self._event_task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass
        self._event_task = None
        logger.info("[%s] Disconnected", self.name)

    async def _listen_for_events(self) -> None:
        """Long-poll Zulip's event queue with automatic reconnection."""
        logger.info("[%s] Listening for messages...", self.name)
        queue_id: Optional[str] = None
        last_event_id = -1
        backoff_idx = 0

        while self._running:
            try:
                if queue_id is None:
                    register_result = await asyncio.to_thread(
                        self.client.register,
                        event_types=["message"],
                    )
                    if register_result.get("result") != "success":
                        raise RuntimeError(
                            f"register failed: {register_result.get('msg') or register_result}"
                        )
                    queue_id = register_result["queue_id"]
                    last_event_id = register_result.get("last_event_id", -1)
                    backoff_idx = 0
                    logger.debug(
                        "[%s] Event queue registered: %s (last_event_id=%s)",
                        self.name,
                        queue_id,
                        last_event_id,
                    )

                # Bound long-poll so disconnect/cancel can reclaim promptly.
                events = await asyncio.to_thread(
                    self.client.call_endpoint,
                    url="events",
                    method="GET",
                    longpolling=True,
                    request={
                        "queue_id": queue_id,
                        "last_event_id": last_event_id,
                    },
                    timeout=30.0,
                )
                if events.get("result") != "success":
                    # BAD_EVENT_QUEUE_ID etc. — re-register
                    logger.warning(
                        "[%s] get_events failed: %s — re-registering queue",
                        self.name,
                        events.get("msg") or events,
                    )
                    queue_id = None
                    continue

                for event in events.get("events", []):
                    last_event_id = event.get("id", last_event_id)
                    if event.get("type") == "message":
                        await self._handle_message(event["message"])

            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._running:
                    return
                logger.error("[%s] Event polling error: %s", self.name, e)
                queue_id = None
                delay = RECONNECT_BACKOFF[min(backoff_idx, len(RECONNECT_BACKOFF) - 1)]
                backoff_idx += 1
                await asyncio.sleep(delay)

    # -- Inbound -------------------------------------------------------------

    async def _handle_message(self, message: Dict[str, Any]) -> None:
        """Normalize an inbound Zulip message and dispatch to the gateway."""
        # Filter self-messages / echoes
        sender_email = (message.get("sender_email") or "").strip()
        sender_id = message.get("sender_id")
        if sender_email and sender_email == self.email:
            return
        if self._own_user_id is not None and sender_id == self._own_user_id:
            return

        content = (message.get("content") or "").strip()
        if not content:
            return

        # Strip Zulip bold-mention syntax (**name**) to plain text
        content = _MENTION_RE.sub(r"\1", content)

        message_type = message.get("type")  # "stream" or "private"
        message_id = str(message.get("id") or "")
        sender_name = message.get("sender_full_name") or sender_email or str(sender_id)

        if message_type == "stream":
            stream_id = message.get("stream_id")
            stream_name = message.get("display_recipient") or str(stream_id)
            if stream_id is not None and stream_name:
                self._stream_names[str(stream_id)] = str(stream_name)
            topic = message.get("subject") or message.get("topic") or ""
            source = self.build_source(
                chat_id=str(stream_id),
                chat_name=str(stream_name),
                chat_type="thread",
                user_id=sender_email or str(sender_id),
                user_name=sender_name,
                # Gateway reply metadata uses thread_id → we reuse it as Zulip topic
                thread_id=topic or None,
                chat_topic=topic or None,
                message_id=message_id or None,
            )
        else:
            # DMs: chat_id is the peer user id so replies route correctly
            source = self.build_source(
                chat_id=f"dm:{sender_id}",
                chat_name=sender_name,
                chat_type="dm",
                user_id=sender_email or str(sender_id),
                user_name=sender_name,
                message_id=message_id or None,
            )

        try:
            ts = message.get("timestamp")
            timestamp = (
                datetime.fromtimestamp(int(ts), tz=timezone.utc)
                if ts
                else datetime.now(tz=timezone.utc)
            )
        except (ValueError, OSError, TypeError):
            timestamp = datetime.now(tz=timezone.utc)

        event = MessageEvent(
            text=content,
            message_type=MessageType.TEXT,
            source=source,
            message_id=message_id or None,
            raw_message=message,
            timestamp=timestamp,
        )
        logger.debug(
            "[%s] Inbound from %s in %s: %s",
            self.name,
            sender_email or sender_id,
            source.chat_id,
            content[:80],
        )
        await self.handle_message(event)

    # -- Outbound ------------------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a message to a Zulip stream (with topic) or DM."""
        metadata = metadata or {}
        if not content:
            return SendResult(success=False, error="empty content")

        if len(content) > self.MAX_MESSAGE_LENGTH:
            logger.warning(
                "[%s] Truncating message from %d to %d chars",
                self.name,
                len(content),
                self.MAX_MESSAGE_LENGTH,
            )
            content = content[: self.MAX_MESSAGE_LENGTH]

        try:
            payload = self._build_send_payload(str(chat_id), content, metadata)
            result = await asyncio.to_thread(self.client.send_message, payload)
            if result.get("result") == "success":
                msg_id = result.get("id")
                return SendResult(
                    success=True,
                    message_id=str(msg_id) if msg_id is not None else None,
                    raw_response=result,
                )
            err = result.get("msg") or str(result)
            logger.error("[%s] Send failed: %s", self.name, err)
            return SendResult(success=False, error=err, raw_response=result)
        except Exception as e:
            logger.error("[%s] Send error: %s", self.name, e)
            return SendResult(success=False, error=str(e), retryable=True)

    def _build_send_payload(
        self,
        chat_id: str,
        content: str,
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Map Hermes chat_id/metadata to a Zulip send_message request."""
        # Explicit message type override from metadata (cron / tools)
        msg_type = (metadata.get("message_type") or metadata.get("type") or "").lower()
        topic = (
            metadata.get("topic")
            or metadata.get("thread_id")
            or metadata.get("subject")
            or self.home_channel_name
            or "general"
        )

        chat_id_s = str(chat_id)
        # Support dm:123, dm_user:123 (legacy home-channel form), or explicit type
        is_dm = (
            msg_type == "private"
            or chat_id_s.startswith("dm:")
            or chat_id_s.startswith("dm_user:")
        )
        if is_dm:
            if chat_id_s.startswith("dm:"):
                raw = chat_id_s[3:]
            elif chat_id_s.startswith("dm_user:"):
                raw = chat_id_s[len("dm_user:") :]
            else:
                raw = chat_id_s
            try:
                to: Any = [int(raw)]
            except ValueError:
                to = [raw]  # email address
            return {"type": "private", "to": to, "content": content}

        # Stream message — chat_id is stream id (preferred) or stream name
        try:
            to = int(chat_id_s)
        except (TypeError, ValueError):
            to = chat_id_s
        return {
            "type": "stream",
            "to": to,
            "topic": str(topic),
            "content": content,
        }

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Zulip has no public typing API for bots — no-op."""
        return None

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return basic chat metadata."""
        if str(chat_id).startswith("dm:"):
            return {"name": chat_id, "type": "dm", "chat_id": chat_id}
        return {
            "name": str(chat_id),
            "type": "channel",
            "chat_id": str(chat_id),
        }




    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """Edit a previously sent Zulip message (used for streaming drafts).

        ``finalize`` is unused on Zulip — an edit is an edit — but accepted
        for BasePlatformAdapter compatibility.
        """
        if not content:
            return SendResult(success=False, error="empty content")
        if not message_id:
            return SendResult(success=False, error="missing message_id")
        if len(content) > self.MAX_MESSAGE_LENGTH:
            content = content[: self.MAX_MESSAGE_LENGTH]
        try:
            msg_id = int(message_id)
        except (TypeError, ValueError):
            return SendResult(success=False, error=f"invalid message_id: {message_id!r}")
        try:
            payload = {"message_id": msg_id, "content": content}
            result = await asyncio.to_thread(self.client.update_message, payload)
            if result.get("result") == "success":
                # Debug aids gateway stream dogfood (grep: edit_message ok).
                logger.debug(
                    "[%s] edit_message ok id=%s len=%d finalize=%s",
                    self.name,
                    msg_id,
                    len(content),
                    finalize,
                )
                return SendResult(
                    success=True,
                    message_id=str(msg_id),
                    raw_response=result,
                )
            err = result.get("msg") or str(result)
            # Surface rate-limit language so stream_consumer flood backoff matches.
            logger.warning("[%s] edit_message failed: %s", self.name, err)
            err_l = str(err).lower()
            retryable = any(
                tok in err_l
                for tok in ("rate", "flood", "retry", "timeout", "temporarily")
            )
            return SendResult(
                success=False,
                error=err,
                raw_response=result,
                retryable=retryable,
            )
        except Exception as e:
            logger.error("[%s] edit_message error: %s", self.name, e)
            return SendResult(success=False, error=str(e), retryable=True)

    async def rename_topic(
        self,
        chat_id: str,
        old_topic: str,
        new_topic: str,
    ):
        """Rename a Zulip stream topic to match a Hermes session title.

        Uses Client.move_topic(stream, new_stream, topic, new_topic=...) with
        the same stream for both ends (rename in place).

        Returns ``(ok, detail)`` where *detail* is None on success, or a short
        human-readable reason on failure / no-op. Callers may still treat a bare
        bool as success/failure for backward compatibility.
        """
        if not old_topic or not new_topic:
            return False, "missing topic name"
        new_topic = str(new_topic).strip()[:60]
        old_topic = str(old_topic).strip()
        if not new_topic or new_topic == old_topic:
            return False, "topic unchanged"
        if str(chat_id).startswith("dm:"):
            return False, "DMs have no renameable topic"

        stream_key = str(chat_id)
        stream = self._stream_names.get(stream_key)
        if not stream:
            # Best-effort: known home stream id maps to "Hermes" on this install
            if stream_key == str(self.home_channel or ""):
                stream = "Hermes"
            else:
                stream = stream_key  # may already be a name
        try:
            result = await asyncio.to_thread(
                self.client.move_topic,
                stream,
                stream,
                old_topic,
                new_topic,
            )
            if isinstance(result, dict) and result.get("result") == "success":
                logger.info(
                    "[%s] Renamed topic %r -> %r on stream %s",
                    self.name, old_topic, new_topic, stream,
                )
                return True, None
            msg = ""
            if isinstance(result, dict):
                msg = str(result.get("msg") or result.get("code") or result)
            else:
                msg = str(result)
            logger.warning("[%s] rename_topic failed: %s", self.name, result)
            return False, (msg.strip() or "Zulip rename failed")
        except Exception as e:
            logger.warning("[%s] rename_topic error: %s", self.name, e)
            return False, str(e) or "Zulip rename error"

    async def create_handoff_thread(
        self,
        parent_chat_id: str,
        name: str,
    ) -> Optional[str]:
        """Create a dedicated Zulip topic for a CLI handoff.

        Returns the topic name on success (used as Hermes thread_id),
        or None on failure.
        """
        # No handoff threads for DMs
        if str(parent_chat_id).startswith("dm:"):
            return None

        # Sanitize name
        name = str(name).strip()
        if not name:
            name = "Hermes handoff"
        # Truncate to Zulip topic limit
        name = name[:60]

        seed_content = f"🔄 Hermes session handoff — {name}"

        try:
            payload = {
                "type": "stream",
                "to": int(parent_chat_id),
                "topic": name,
                "content": seed_content,
            }
            result = await asyncio.to_thread(self.client.send_message, payload)
            if result.get("result") == "success":
                logger.info(
                    "[%s] Handoff thread created: topic=%s in stream %s",
                    self.name, name, parent_chat_id,
                )
                return name
            else:
                err = result.get("msg") or str(result)
                logger.warning(
                    "[%s] Handoff thread creation failed: %s",
                    self.name, err,
                )
                return None
        except Exception as e:
            logger.warning(
                "[%s] Handoff thread creation error: %s",
                self.name, e,
            )
            return None

# ---------------------------------------------------------------------------
# Plugin hooks
# ---------------------------------------------------------------------------


def _env_enablement() -> Optional[dict]:
    """Seed PlatformConfig.extra (and home_channel) from env vars."""
    _normalize_env_aliases()
    creds = _resolve_credentials(None)
    if not (creds["api_key"] and creds["email"] and creds["site"]):
        return None

    seed: Dict[str, Any] = {
        "email": creds["email"],
        "site": creds["site"],
        "api_key": creds["api_key"],
    }
    home = _env("ZULIP_HOME_CHANNEL")
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": _env("ZULIP_HOME_CHANNEL_NAME") or "general",
        }
    return seed


def _apply_yaml_config(yaml_cfg: dict, platform_cfg: dict) -> Optional[dict]:
    """Bridge ``platforms.zulip`` / top-level ``zulip:`` YAML into env + extra.

    Env vars take precedence (only set when unset). Returns a dict merged into
    ``PlatformConfig.extra``.
    """
    seed: Dict[str, Any] = {}

    api_key = platform_cfg.get("api_key") or platform_cfg.get("token")
    if api_key and not os.getenv("ZULIP_API_KEY"):
        os.environ["ZULIP_API_KEY"] = str(api_key)
    if api_key:
        seed["api_key"] = str(api_key)

    extra = platform_cfg.get("extra") or {}
    if isinstance(extra, dict):
        email = extra.get("email")
        site = extra.get("site")
        if email and not os.getenv("ZULIP_EMAIL"):
            os.environ["ZULIP_EMAIL"] = str(email)
        if site and not os.getenv("ZULIP_SITE"):
            os.environ["ZULIP_SITE"] = str(site).rstrip("/")
        if email:
            seed["email"] = str(email)
        if site:
            seed["site"] = str(site).rstrip("/")

    # Top-level email/site (some users put them outside extra)
    for key, env_name in (("email", "ZULIP_EMAIL"), ("site", "ZULIP_SITE")):
        val = platform_cfg.get(key)
        if val:
            if not os.getenv(env_name):
                os.environ[env_name] = str(val).rstrip("/") if key == "site" else str(val)
            seed.setdefault(key, str(val).rstrip("/") if key == "site" else str(val))

    home = platform_cfg.get("home_channel")
    if isinstance(home, dict) and home.get("chat_id"):
        if not os.getenv("ZULIP_HOME_CHANNEL"):
            os.environ["ZULIP_HOME_CHANNEL"] = str(home["chat_id"])
        if home.get("name") and not os.getenv("ZULIP_HOME_CHANNEL_NAME"):
            os.environ["ZULIP_HOME_CHANNEL_NAME"] = str(home["name"])
        seed["home_channel"] = {
            "chat_id": str(home["chat_id"]),
            "name": str(home.get("name") or "general"),
        }

    allow_from = platform_cfg.get("allow_from") or extra.get("allowed_users")
    if allow_from is not None and not _env("ZULIP_ALLOWED_USERS", "ZULIP_ALLOWED_EMAILS"):
        if isinstance(allow_from, list):
            allow_from = ",".join(str(v) for v in allow_from)
        os.environ["ZULIP_ALLOWED_USERS"] = str(allow_from)

    _normalize_env_aliases()
    return seed or None


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Out-of-process send for cron / send_message_tool fallbacks."""
    if not ZULIP_AVAILABLE:
        return {"error": "zulip package not installed"}

    try:
        creds = _resolve_credentials(pconfig)
        client = _make_client(creds)
    except Exception as e:
        return {"error": f"zulip standalone send: {e}"}

    extra = getattr(pconfig, "extra", {}) or {}
    topic = (
        thread_id
        or os.getenv("ZULIP_HOME_CHANNEL_NAME")
        or extra.get("home_channel_name")
        or "general"
    )
    metadata = {"thread_id": topic, "topic": topic}

    if len(message) > MAX_MESSAGE_LENGTH:
        message = message[:MAX_MESSAGE_LENGTH]

    # Build payload using a throwaway adapter-less path
    # Reuse the same payload builder rules as the live adapter
    adapter_meta = {"thread_id": topic, "topic": topic}
    chat_id_s = str(chat_id)
    is_dm = chat_id_s.startswith("dm:") or chat_id_s.startswith("dm_user:")
    if is_dm:
        if chat_id_s.startswith("dm:"):
            raw = chat_id_s[3:]
        else:
            raw = chat_id_s[len("dm_user:") :]
        try:
            to: Any = [int(raw)]
        except ValueError:
            to = [raw]
        payload = {"type": "private", "to": to, "content": message}
    else:
        try:
            to = int(chat_id_s)
        except ValueError:
            to = chat_id_s
        payload = {
            "type": "stream",
            "to": to,
            "topic": str(adapter_meta["topic"]),
            "content": message,
        }

    try:
        result = await asyncio.to_thread(client.send_message, payload)
        if result.get("result") != "success":
            return {"error": result.get("msg") or str(result)}
        return {
            "success": True,
            "platform": "zulip",
            "chat_id": chat_id_s,
            "message_id": result.get("id"),
        }
    except Exception as e:
        return {"error": f"zulip standalone send failed: {e}"}


def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system at startup."""
    _normalize_env_aliases()
    ctx.register_platform(
        name="zulip",
        label="Zulip",
        adapter_factory=lambda cfg: ZulipAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["ZULIP_API_KEY", "ZULIP_EMAIL", "ZULIP_SITE"],
        install_hint="pip install zulip",
        env_enablement_fn=_env_enablement,
        apply_yaml_config_fn=_apply_yaml_config,
        cron_deliver_env_var="ZULIP_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="ZULIP_ALLOWED_USERS",
        allow_all_env="ZULIP_ALLOW_ALL_USERS",
        # Also honor ZULIP_ALLOWED_EMAILS via apply_yaml_config / env normalize.
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="💬",
        pii_safe=False,  # emails are PII
        allow_update_command=True,
        platform_hint=(
            "You are chatting via Zulip. Messages may be in streams (channels) "
            "with topics, or in private DMs. Prefer Zulip-flavored Markdown "
            "(bold, italics, code fences, spoilers, quotes). Keep replies "
            "focused; stream messages are organized by topic. Message limit "
            "is 10,000 characters."
        ),
    )
