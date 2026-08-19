"""
Zulip Platform Adapter for Hermes Gateway (Plugin)

Bi-directional integration with Zulip chat platform.
Supports stream messages (with topics) and private messages.
"""

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Optional, Any, overload

try:
    import zulip

    ZULIP_AVAILABLE = True
except ImportError:
    zulip = None  # type: ignore
    ZULIP_AVAILABLE = False

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.config import Platform, PlatformConfig

try:
    from zulip.logger import format_zulip_log, mask_pii
    from zulip.text_utils import (
        chunk_text,
        extract_topic_directive,
        strip_onchar_prefix,
        resolve_onchar_prefixes,
        create_mention_regex,
        normalize_mention,
        strip_html_to_text,
    )
    from zulip.media import upload_file_to_zulip
    from zulip.queue_manager import ZulipQueueManager
    from zulip.dedupe_store import ZulipDedupeStore
    from zulip.reactions import ReactionConfig, ReactionLifecycle
    from zulip.engagement import (
        TopicEngagementStore,
        EngagementConfig,
        is_stop_listening_message,
        is_end_session_message,
        format_expiry_notice_text,
    )
except ImportError:  # Hermes user-plugin layout (relative package)
    from .logger import format_zulip_log, mask_pii
    from .text_utils import (
        chunk_text,
        extract_topic_directive,
        strip_onchar_prefix,
        resolve_onchar_prefixes,
        create_mention_regex,
        normalize_mention,
        strip_html_to_text,
    )
    from .media import upload_file_to_zulip
    from .queue_manager import ZulipQueueManager
    from .dedupe_store import ZulipDedupeStore
    from .reactions import ReactionConfig, ReactionLifecycle
    from .engagement import (
        TopicEngagementStore,
        EngagementConfig,
        is_stop_listening_message,
        is_end_session_message,
        format_expiry_notice_text,
    )

logger = logging.getLogger(__name__)


# Chunking defaults (overridable via env)
DEFAULT_CHUNK_LIMIT = 10000  # Hermes registry max_message_length
DEFAULT_CHUNK_MODE = "length"

# Timeout defaults (seconds) — Issue #62
DEFAULT_CONNECT_TIMEOUT = 30.0
DEFAULT_READ_TIMEOUT = 60.0
DEFAULT_SEND_TIMEOUT = 90.0

# Typing indicator delay (seconds) — how long to keep typing visible after
# the API confirms the message was sent, so the response is visible in the UI
# before the typing indicator stops and the success reaction appears.
DEFAULT_TYPING_DELAY = 2.0


def _resolve_chunk_config() -> tuple[int, str]:
    """Read chunking config from environment."""
    limit_raw = os.getenv("ZULIP_TEXT_CHUNK_LIMIT", "").strip()
    limit = int(limit_raw) if limit_raw.isdigit() else DEFAULT_CHUNK_LIMIT
    mode = os.getenv("ZULIP_CHUNK_MODE", DEFAULT_CHUNK_MODE).strip()
    if mode not in ("length", "newline"):
        mode = DEFAULT_CHUNK_MODE
    return limit, mode


def _resolve_timeouts() -> tuple[float, float, float]:
    """Read timeout config from environment.

    Returns (connect_timeout, read_timeout, send_timeout) in seconds.
    """
    def _parse(val: str, default: float) -> float:
        try:
            return float(val.strip())
        except (ValueError, AttributeError):
            return default

    connect = _parse(os.getenv("ZULIP_CONNECT_TIMEOUT", ""), DEFAULT_CONNECT_TIMEOUT)
    read = _parse(os.getenv("ZULIP_READ_TIMEOUT", ""), DEFAULT_READ_TIMEOUT)
    send = _parse(os.getenv("ZULIP_SEND_TIMEOUT", ""), DEFAULT_SEND_TIMEOUT)
    return connect, read, send


def _resolve_typing_delay() -> float:
    """Read typing indicator delay from environment.

    After the message is accepted by the Zulip API, the typing indicator
    stays active for this many seconds so the response has time to propagate
    to all clients before the indicator stops and the success reaction fires.
    """
    try:
        val = float(os.getenv("ZULIP_TYPING_DELAY_SECONDS", "").strip())
        return max(0.0, val)
    except (ValueError, AttributeError):
        return DEFAULT_TYPING_DELAY


def _resolve_streams_filter() -> set[str] | None:
    """Read stream filtering config from environment.

    Returns None if all streams are allowed (default), or a set of
    lowercase stream names to monitor.
    """
    raw = os.getenv("ZULIP_STREAMS", "").strip()
    if not raw or raw == "*":
        return None
    return {s.strip().lower() for s in raw.split(",") if s.strip()}


def _resolve_response_prefix() -> str:
    """Read outbound response prefix from environment."""
    return os.getenv("ZULIP_RESPONSE_PREFIX", "")


def _resolve_stream_overrides() -> dict[str, dict[str, Any]]:
    """Read per-stream trigger overrides from the environment.

    ``ZULIP_STREAM_OVERRIDES`` is a JSON object mapping stream name to a
    settings object, overriding ``ZULIP_CHATMODE`` for that stream::

        ZULIP_STREAM_OVERRIDES='{
          "bot lab":       {"chatmode": "onmessage"},
          "team: general": {"chatmode": "oncall"}
        }'

    Only ``chatmode`` is supported. ``requireMention`` is deliberately not
    overridable: in the current gate it is inert in every mode.

    JSON is used rather than delimited pairs because Zulip stream names may
    legitimately contain both colons and commas.

    Stream names and setting keys are both matched case-insensitively.
    Unrecognised setting keys are warned about. Malformed configuration is
    logged and ignored rather than raised.
    """
    raw = os.getenv("ZULIP_STREAM_OVERRIDES", "").strip()
    if len(raw.encode("utf-8")) > _MAX_JSON_OVERRIDES_BYTES:
        logger.warning(
            "ZULIP_STREAM_OVERRIDES exceeds max size (%d > %d bytes); ignoring overrides",
            len(raw.encode("utf-8")),
            _MAX_JSON_OVERRIDES_BYTES,
        )
        return _remember({})
    cached_raw, cached = _stream_overrides_cache
    if raw == cached_raw:
        return cached

    def _remember(value: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        global _stream_overrides_cache
        _stream_overrides_cache = (raw, value)
        return value

    if not raw:
        return _remember({})

    try:
        parsed = json.loads(raw)
    except ValueError:
        logger.warning("ZULIP_STREAM_OVERRIDES is not valid JSON; ignoring overrides")
        return _remember({})

    if not isinstance(parsed, dict):
        logger.warning(
            "ZULIP_STREAM_OVERRIDES must be a JSON object mapping stream name "
            "to a settings object; ignoring overrides"
        )
        return _remember({})

    overrides: dict[str, dict[str, Any]] = {}
    for name, settings in parsed.items():
        if not isinstance(settings, dict):
            logger.warning(
                "ZULIP_STREAM_OVERRIDES[%r] must be an object, e.g. "
                '{"chatmode": "onmessage"}; ignoring entry',
                name,
            )
            continue

        entry: dict[str, Any] = {}

        # Setting keys are matched case-insensitively
        normalised = {str(k).strip().lower(): v for k, v in settings.items()}

        # Warn about unrecognised keys
        unknown = sorted(
            k for k in normalised
            if k not in ("chatmode", "requiremention", "require_mention")
        )
        if unknown:
            logger.warning(
                "ZULIP_STREAM_OVERRIDES[%r]: ignoring unrecognised key(s) %s; "
                "the only supported key is 'chatmode'",
                name, ", ".join(unknown),
            )

        mode = normalised.get("chatmode")
        if mode is not None:
            mode = str(mode).strip().lower()
            if mode in ("onmessage", "oncall", "onchar"):
                entry["chatmode"] = mode
            else:
                logger.warning(
                    "ZULIP_STREAM_OVERRIDES[%r].chatmode=%r is not one of "
                    "onmessage/oncall/onchar; ignoring it",
                    name, mode,
                )

        if entry:
            overrides[str(name).strip().lower()] = entry

    return _remember(overrides)


@overload
def _resolve_chatmode() -> tuple[str, list[str], bool]:
    ...


@overload
def _resolve_chatmode(stream_name: str) -> tuple[str, list[str], bool]:
    ...


def _resolve_chatmode(stream_name: Optional[str] = None) -> tuple[str, list[str], bool]:
    """Read stream trigger mode config from environment.

    When ``stream_name`` is supplied, a matching entry in
    ``ZULIP_STREAM_OVERRIDES`` takes precedence over the global
    ``ZULIP_CHATMODE`` for that stream only.
    """
    mode = os.getenv("ZULIP_CHATMODE", "onmessage").strip().lower()
    if mode not in ("onmessage", "oncall", "onchar"):
        mode = "onmessage"
    prefixes = resolve_onchar_prefixes(os.getenv("ZULIP_ONCHAR_PREFIXES", ""))
    require_mention = os.getenv("ZULIP_REQUIRE_MENTION", "true").strip().lower() not in ("false", "0", "no", "off")

    if stream_name:
        override = _resolve_stream_overrides().get(stream_name.strip().lower())
        if override:
            mode = override.get("chatmode", mode)

    return mode, prefixes, require_mention


class ZulipAdapter(BasePlatformAdapter):
    """Zulip platform adapter for Hermes Gateway."""

    # Zulip message body soft limit used by send/edit paths.
    MAX_MESSAGE_LENGTH = DEFAULT_CHUNK_LIMIT

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("zulip"))
        extra = config.extra or {}

        self.api_key = os.getenv("ZULIP_API_KEY") or extra.get("api_key", "")
        self.email = os.getenv("ZULIP_EMAIL") or extra.get("email", "")
        self.site = os.getenv("ZULIP_SITE") or extra.get("site", "")
        self.home_topic = (
            os.getenv("ZULIP_HOME_CHANNEL_NAME")
            or extra.get("home_topic")
            or extra.get("home_channel_name")
            or "general"
        )
        # Numeric home stream id (cron / handoff default destination).
        home_channel = (
            os.getenv("ZULIP_HOME_CHANNEL")
            or extra.get("home_channel")
            or ""
        )
        if isinstance(home_channel, dict):
            self.home_channel = str(
                home_channel.get("chat_id") or home_channel.get("id") or ""
            ).strip() or None
            if home_channel.get("name"):
                self.home_topic = str(home_channel.get("name"))
        else:
            self.home_channel = str(home_channel).strip() or None

        if not ZULIP_AVAILABLE:
            logger.error(
                "zulip package not installed. Run: pip install zulip"
            )
            raise ImportError(
                "zulip package not installed. Run: pip install zulip"
            )

        self.client = zulip.Client(
            email=self.email,
            api_key=self.api_key,
            site=self.site,
        )

        # Bot identity for mention matching (email local-part is NOT the display name)
        self._bot_username = self.email.split("@")[0] if self.email else ""
        self._bot_full_name = ""
        try:
            profile = self.client.get_profile()
            if isinstance(profile, dict) and profile.get("result") == "success":
                self._bot_full_name = (profile.get("full_name") or "").strip()
            elif isinstance(profile, dict) and profile.get("full_name"):
                self._bot_full_name = (profile.get("full_name") or "").strip()
        except Exception:
            pass

        # Track latest topic per stream so replies stay threaded
        self._topic_cache: dict[str, str] = {}
        # stream_id -> stream name (required by Client.move_topic)
        self._stream_names: dict[str, str] = {}
        # Zulip message id (int) -> Hermes session_id (str) for reply banners
        self._zulip_to_session: dict[int, str] = {}
        # chat_id -> last numbered list of recent messages shown in a /reply banner
        # Used so user can do "/reply 3" to pick the 3rd item instead of typing a long id.
        self._last_reply_list: dict[str, list[dict]] = {}
        # Per-topic (or per-chat for DMs) sticky/pending context session overrides.
        # key = f"{chat_id}:{topic}" for streams, just chat_id for DMs.
        # _sticky_context: permanent until changed.
        # _pending_context: consumed on the next inbound message only (non-permanent).
        self._sticky_context: dict[str, str] = {}
        self._pending_context: dict[str, str] = {}

        # Per-topic anchor (zulip message id) for the most recent message the user
        # explicitly targeted with /reply or emoji reaction (including cron outputs
        # and messages without a prior cached Hermes session).
        # Keyed the same as ckey. Lets us thread the banner and future responses to
        # that specific Zulip message so the user can "start from" a cron digest, report, etc.
        self._topic_anchors: dict[str, int] = {}

        # Sticky topic engagement (mention-to-start, free follow-ups)
        self._engagement = TopicEngagementStore(EngagementConfig.from_env())
        logger.info(
            "zulip engagement [mode=%s scope=%s ttl_min=%.0f expiry_notice=%s free_streams=%s]",
            self._engagement.config.mode,
            self._engagement.config.scope,
            self._engagement.config.ttl_seconds / 60.0,
            self._engagement.config.expiry_notice,
            sorted(self._engagement.config.free_response_streams) or "none",
        )

        self._data_dir = os.environ.get("HERMES_DATA_DIR", os.path.expanduser("~/.hermes"))

        # Timeout configuration (Issue #62)
        self._connect_timeout, self._read_timeout, self._send_timeout = _resolve_timeouts()

        # Typing indicator delay (Issue #96)
        self._typing_delay = _resolve_typing_delay()

        # Stream filtering (Issue #65) — None means all streams
        self._streams_filter = _resolve_streams_filter()

        # Response prefix (Issue #65) — prepended to every outbound message
        self._response_prefix = _resolve_response_prefix()

        # Rate limiter (per-sender, sliding window)
        self._rate_limiter = RateLimiter(
            max_per_minute=int(
                os.getenv("ZULIP_MAX_MESSAGES_PER_MINUTE", "60").strip()
            ),
        )

        # Audit logger for security events
        self._audit_logger = AuditLogger(
            data_dir=self._data_dir,
            account_id=self.email or "default",
        )

        # Persistent queue and dedupe
        self._queue_mgr = ZulipQueueManager(
            account_id=self.email or "default",
            data_dir=self._data_dir,
            register_fn=lambda: self.client.register(
                event_types=["message", "reaction"], fetch_event_id=0
            ),
        )
        self._dedupe = ZulipDedupeStore(
            account_id=self.email or "default",
            data_dir=self._data_dir,
            ttl_ms=300_000,
            max_size=2000,
        )
        self._dedupe.load()

        # Reaction config
        self._reaction_cfg = ReactionConfig.from_env()

        self._listening = False
        self._event_task: Optional[asyncio.Task] = None
        self._engagement_task: Optional[asyncio.Task] = None

    async def _sdk_call(self, fn, *args, timeout: float, **kwargs):
        """Wrap a synchronous SDK call in asyncio.to_thread + asyncio.wait_for.

        Provides outer-timeout protection so the gateway event loop never
        blocks indefinitely on a hung Zulip API request.
        """
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(fn, *args, **kwargs),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "zulip SDK call timed out after %.1fs [fn=%s]",
                timeout,
                getattr(fn, "__name__", repr(fn)),
            )
            raise

    @staticmethod
    def _validate_message_id(message_id: Any) -> int:
        """Validate and convert a message ID to int.

        Raises ValueError if the message ID is not a valid positive integer
        or exceeds the maximum safe value.
        Prevents path traversal, injection, and overflow via malformed IDs.
        """
        if message_id is None:
            raise ValueError("message_id is required")
        try:
            mid = int(str(message_id).strip())
        except (ValueError, TypeError):
            raise ValueError(f"Invalid message_id: {message_id}")
        if mid <= 0:
            raise ValueError(f"message_id must be positive: {message_id}")
        if mid > 2**63 - 1:
            raise ValueError(f"message_id exceeds maximum safe value: {message_id}")
        return mid

    async def _stop_typing(self, typing_params: Optional[dict]) -> None:
        """Stop typing indicator if it was started. Safe to call multiple times."""
        if typing_params is None:
            return
        params = dict(typing_params)
        params["op"] = "stop"
        try:
            await self._sdk_call(
                self.client.set_typing_status,
                params,
                timeout=self._send_timeout,
            )
        except Exception:
            pass

    async def _mark_read(self, message_id: Any) -> None:
        """Mark a message as read. Best-effort."""
        try:
            await self._sdk_call(
                self.client.update_message_flags,
                {"messages": [message_id], "op": "add", "flag": "read"},
                timeout=self._send_timeout,
            )
        except Exception:
            pass

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Initialize connection and start listening."""
        logger.info("Zulip adapter connecting...")
        try:
            result = await asyncio.to_thread(self.client.get_members)
            if result.get("result") != "success":
                raise ConnectionError(f"Zulip connection failed: {result}")
        except Exception as e:
            logger.error(f"Zulip connection error: {e}")
            raise

        logger.info(
            format_zulip_log(
                "zulip connection established",
                site=mask_pii(self.site),
            )
        )

        # Ensure queue is registered before starting listener
        await self._queue_mgr.ensure_queue()

        # Recover interrupted messages from previous gateway instance
        bot_user_id = str(probe_result.get("bot", {}).get("id", ""))
        asyncio.create_task(
            recover_interrupted_messages(
                client=self.client,
                bot_email=self.email,
                bot_user_id=bot_user_id,
                reaction_start=self._reaction_cfg.on_start,
                reaction_success=self._reaction_cfg.on_success,
                reaction_error=self._reaction_cfg.on_error,
                handle_message=self._handle_message,
                sdk_call=self._sdk_call,
                send_timeout=self._send_timeout,
            )
        )

        self._listening = True
        self._event_task = asyncio.create_task(self._listen_for_events())
        if (
            self._engagement.config.mode != "off"
            and self._engagement.config.expiry_notice
        ):
            self._engagement_task = asyncio.create_task(
                self._engagement_expiry_loop()
            )
        self._mark_connected()
        return True

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        """Get information about a chat/channel."""
        if chat_id.startswith("dm:"):
            return {"name": chat_id, "type": "dm"}
        return {"name": chat_id, "type": "stream"}

    async def disconnect(self) -> None:
        """Stop listening and close connection."""
        self._listening = False
        for task_attr in ("_event_task", "_engagement_task"):
            task = getattr(self, task_attr, None)
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                setattr(self, task_attr, None)
        self._mark_disconnected()
        logger.info("Zulip adapter disconnected")

    async def _engagement_expiry_loop(self) -> None:
        """Periodically expire sticky engagements and post topic notices.

        Only natural idle TTL expiry notifies. /unlisten and /new clear silently.
        Gateway shutdown does not spam leftover topics.
        """
        interval = self._engagement.config.expiry_scan_seconds
        ttl_min = max(1, int(round(self._engagement.config.ttl_seconds / 60.0)))
        bot_name = self._bot_full_name or ""
        logger.info(
            "zulip engagement expiry scanner started [interval=%.0fs ttl=%sm]",
            interval,
            ttl_min,
        )
        while self._listening:
            try:
                await asyncio.sleep(interval)
                if not self._listening:
                    break
                expired = self._engagement.pop_expired()
                if not expired:
                    continue

                # Coalesce: one notice per (stream_id, topic)
                by_topic: dict[tuple[str, str], list] = {}
                for entry in expired:
                    key = (str(entry.stream_id), entry.topic or self.home_topic)
                    by_topic.setdefault(key, []).append(entry)

                for (stream_id, topic), entries in by_topic.items():
                    text_out = format_expiry_notice_text(
                        ttl_minutes=ttl_min,
                        bot_display_name=bot_name,
                        user_names=[e.user_name for e in entries if e.user_name],
                    )
                    try:
                        result = await asyncio.to_thread(
                            self.client.send_message,
                            {
                                "type": "stream",
                                "to": int(stream_id),
                                "topic": topic,
                                "content": text_out,
                            },
                        )
                        if result.get("result") == "success":
                            logger.info(
                                "zulip engagement expired notice [stream=%s topic=%s users=%d]",
                                stream_id,
                                topic,
                                len(entries),
                            )
                        else:
                            logger.warning(
                                "zulip engagement expired notice failed [stream=%s topic=%s result=%s]",
                                stream_id,
                                topic,
                                result,
                            )
                    except Exception as e:
                        logger.warning(
                            "zulip engagement expired notice error [stream=%s topic=%s]: %s",
                            stream_id,
                            topic,
                            e,
                        )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("zulip engagement expiry loop error: %s", e)

    async def _listen_for_events(self):
        """Listen for incoming Zulip messages via persistent event queue."""
        logger.info("zulip adapter listening [account=%s]", mask_pii(self.email))

        while self._listening:
            try:
                queue = await self._queue_mgr.ensure_queue()

                events = await self._sdk_call(
                    self.client.get_events,
                    queue_id=queue.queue_id,
                    last_event_id=queue.last_event_id,
                    timeout=self._read_timeout,
                )

                if events.get("result") == "error":
                    msg = events.get("msg", "")
                    is_bad_queue = (
                        events.get("code") == "BAD_EVENT_QUEUE_ID"
                        or "bad event queue" in msg.lower()
                    )
                    if is_bad_queue:
                        logger.warning("zulip queue expired, re-registering")
                        self._queue_mgr.mark_queue_expired()
                        continue
                    logger.warning(
                        format_zulip_log(
                            "zulip event queue error",
                            error=mask_pii(msg),
                        )
                    )
                    await asyncio.sleep(1)
                    continue

                batch_max_event_id = queue.last_event_id
                processing_tasks = []
                for event in events.get("events", []):
                    event_id = event["id"]
                    if event_id > batch_max_event_id:
                        batch_max_event_id = event_id
                    if event.get("type") == "message":
                        msg = event["message"]
                        # Zulip puts mention flags on the EVENT, not the message body
                        if "flags" in event and "flags" not in msg:
                            msg = {**msg, "flags": event.get("flags") or []}
                        elif "flags" in event:
                            merged = list(dict.fromkeys(
                                list(msg.get("flags") or []) + list(event.get("flags") or [])
                            ))
                            msg = {**msg, "flags": merged}
                        msg_id = str(msg.get("id", ""))
                        # Dedupe check
                        if self._dedupe.check(msg_id):
                            logger.debug("zulip dedupe hit [msg=%s]", mask_pii(msg_id))
                            continue
                        # Process messages concurrently so a slow model call
                        # does not block the poll loop for unrelated messages.
                        # Per-session serialization is handled by the gateway.
                        task = asyncio.create_task(self._handle_message(msg))
                        processing_tasks.append(task)

                # Fire-and-forget: don't await processing tasks here so the
                # poll loop keeps fetching events. Errors are logged inside
                # _handle_message.

                    elif event.get("type") == "reaction":
                        await self._handle_reaction(event)

                # Batch update event ID
                if batch_max_event_id > queue.last_event_id:
                    self._queue_mgr.update_last_event_id(batch_max_event_id)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(
                    format_zulip_log(
                        "zulip event polling error",
                        error=mask_pii(str(e)),
                    )
                )
                await asyncio.sleep(5)

    def _message_mentions_bot(self, message: dict, plain_content: str) -> tuple[bool, Optional[re.Pattern]]:
        """Detect @mentions of this bot.

        Prefer Zulip's own ``flags`` (authoritative). Fall back to matching
        the bot display name and email local-part in plain text — the display
        name (e.g. ``Ai-agent (hermes)``) is what users actually @-mention,
        not the email local-part.
        """
        flags = message.get("flags") or []
        if "mentioned" in flags or "wildcard_mentioned" in flags:
            return True, self._mention_strip_regex()

        candidates: list[str] = []
        if self._bot_full_name:
            candidates.append(self._bot_full_name)
        if self._bot_username:
            candidates.append(self._bot_username)

        for name in candidates:
            # Avoid \b after ')' (display names like "Ai-agent (hermes)")
            pattern = re.compile(
                rf"@{re.escape(name)}(?=\s|[.,!?;:]|$)",
                re.IGNORECASE,
            )
            if pattern.search(plain_content):
                return True, pattern

        if self._bot_username:
            rx = create_mention_regex(self._bot_username)
            if rx.search(plain_content):
                return True, rx
        return False, None

    def _mention_strip_regex(self) -> Optional[re.Pattern]:
        """Regex used to strip the bot mention from user text after a hit."""
        names = [n for n in (self._bot_full_name, self._bot_username) if n]
        if not names:
            return None
        alts = "|".join(re.escape(n) for n in names)
        return re.compile(rf"@(?:{alts})(?=\s|[.,!?;:]|$)", re.IGNORECASE)

    async def _handle_message(self, message: dict):
        """Process incoming Zulip message."""
        # Filter self-messages to prevent loops
        if message.get("sender_email") == self.email:
            return

        msg_type = message.get("type")  # "stream" or "private"
        content = message.get("content", "")
        message_id = message.get("id")
        sender_email = message.get("sender_email", "")
        sender_full_name = message.get("sender_full_name", "Unknown")

        # --- Rate limiting (per-sender) ---
        sender_key = sender_email or str(message.get("sender_id", ""))
        if not self._rate_limiter.check(sender_key):
            logger.warning(
                "zulip rate limit hit [sender=%s msg=%s]",
                mask_pii(sender_key),
                mask_pii(str(message_id)),
            )
            await self._audit_logger.log_rate_limit_exceeded(
                sender_id=sender_key,
                limit=self._rate_limiter.config["max_per_minute"],
            )
            return

        # Strip Zulip @-mention syntax and HTML
        content = strip_html_to_text(content)

        # --- /reply command must be handled BEFORE chatmode gating ---
        # so it works even when bot is in oncall mode and message has no @mention.
        if content.strip().lower().startswith("/reply"):
            reply_to_id = None
            reply_args = content.strip()[6:].strip().lower()
            permanent = False
            # Detect permanent/sticky mode in one command
            for flag in ("sticky", "permanent", "set", "persist"):
                if flag in reply_args:
                    permanent = True
                    reply_args = reply_args.replace(flag, "").strip()
            if reply_args.endswith("!"):
                permanent = True
                reply_args = reply_args.rstrip("!").strip()

            # Re-parse numeric part after stripping flags
            num_str = reply_args.strip()
            chosen = None
            if num_str:
                if num_str.isdigit():
                    n = int(num_str)
                    if 1 <= n <= 9:
                        # Compute topic-scoped key (same logic as in _build_reply_banner)
                        if msg_type == "stream":
                            t = topic_for_gate or (extra_meta.get("topic") or "")
                            list_key = f"{chat_id}:{t}" if chat_id else chat_id
                        else:
                            list_key = chat_id
                        last = self._last_reply_list.get(list_key) or []
                        if 1 <= n <= len(last):
                            chosen = last[n-1]
                            reply_to_id = chosen.get("zulip_id") or n
                        else:
                            reply_to_id = n
                    else:
                        reply_to_id = n
                else:
                    reply_to_id = message_id
            else:
                reply_to_id = message_id

            # Build source/metadata early for banner
            if msg_type == "stream":
                stream_id_for_gate = message.get("stream_id")
                topic_for_gate = message.get("subject") or message.get("topic") or ""
                stream_name = message.get("display_recipient") or str(stream_id_for_gate)
                chat_id = str(stream_id_for_gate)
                extra_meta = {
                    "topic": topic_for_gate,
                    "thread_id": topic_for_gate,
                    "stream_id": stream_id_for_gate,
                    "stream_name": str(stream_name),
                }
            else:
                sender_id = message.get("sender_id")
                chat_id = f"dm:{sender_id}"
                extra_meta = {"user_id": sender_id, "user_email": sender_email}

            if reply_to_id:
                # Show reactions for /reply
                reactions = ReactionLifecycle(
                    self.client, str(message_id), self._reaction_cfg
                )
                await reactions.start()

                # Determine the target Zulip id and (if known) Hermes session id
                target_zid = int(reply_to_id) if isinstance(reply_to_id, (int, str)) and str(reply_to_id).isdigit() else None
                target_sid = None
                if chosen and chosen.get("session_id"):
                    target_sid = chosen.get("session_id")
                elif target_zid is not None:
                    target_sid = self._zulip_to_session.get(target_zid)
                if not target_sid:
                    # Resolve just enough to learn the current message's session if bare /reply
                    ev = MessageEvent(
                        text=content,
                        message_type=MessageType.TEXT,
                        source=None,
                        message_id=str(message_id) if message_id is not None else None,
                        metadata=extra_meta,
                        resolve_only=True,
                    )
                    try:
                        await self.handle_message(ev)
                        if message_id is not None and getattr(ev, "session_id", None):
                            self._zulip_to_session[int(message_id)] = ev.session_id
                            if target_zid is None or target_zid == message_id:
                                target_sid = ev.session_id
                    except Exception:
                        pass

                # Compute per-topic key and apply permanent vs non-permanent context
                ckey = None
                t = topic_for_gate if "topic_for_gate" in locals() else (extra_meta.get("topic") or extra_meta.get("thread_id") or "")
                if msg_type == "stream" and chat_id:
                    ckey = f"{chat_id}:{t or ''}"
                else:
                    ckey = chat_id

                mode_label = "permanent" if permanent else "temporary (one-off)"
                if ckey and target_sid:
                    if permanent:
                        self._sticky_context[ckey] = target_sid
                    else:
                        self._pending_context[ckey] = target_sid

                # Always record the zulip message anchor so we can thread to cron outputs
                # and other messages that don't yet have a Hermes session.
                if ckey and target_zid:
                    self._topic_anchors[ckey] = int(target_zid)

                # Visual marker: add a context emoji reaction to the target message (if we have its Zulip id)
                # This is an alternative / addition to the text banner. Try different emojis to see what feels best.
                if target_zid:
                    try:
                        ctx_emoji = "pushpin" if permanent else "bookmark"   # 📌 vs 🔖
                        await asyncio.to_thread(
                            self.client.add_reaction,
                            {"message_id": int(target_zid), "emoji_name": ctx_emoji},
                        )
                    except Exception:
                        pass  # best effort

                # Build informative output (banner + selection note)
                banner = await self._build_reply_banner(chat_id, extra_meta, reply_to_id)
                note = ""
                if target_sid:
                    if permanent:
                        note = "\n\nContext set to `" + str(target_sid) + "` (permanent). Future messages in this topic will use this session until you change it."
                    else:
                        note = "\n\nContext set to `" + str(target_sid) + "` (temporary/one-off for your next message)."
                elif target_zid:
                    note = "\n\nTargeting zulip message #" + str(target_zid) + " (no cached session yet). Banner is threaded under it; your next message here starts context from this point."
                content_to_send = (banner or "Selected.") + note

                await self.send(
                    chat_id,
                    content_to_send,
                    reply_to=target_zid if target_zid else None,
                    metadata=extra_meta,
                )
                await reactions.success()
                return

        # --- Stream trigger gating (BEFORE reactions/typing) ---
        mention_regex: Optional[re.Pattern] = None
        stream_id_for_gate: Optional[Any] = None
        topic_for_gate: str = ""
        engaged_followup = False
        free_response_hit = False

        if msg_type == "stream":
            stream_id_for_gate = message.get("stream_id")
            topic_for_gate = message.get("subject") or message.get("topic") or ""
            chatmode, onchar_prefixes, require_mention = _resolve_chatmode()

            onchar_triggered, stripped = strip_onchar_prefix(content, onchar_prefixes)
            if onchar_triggered:
                content = stripped

            was_mentioned, mention_regex = self._message_mentions_bot(message, content)

            if stream_id_for_gate is not None and self._engagement.is_free_response_stream(
                stream_id_for_gate
            ):
                free_response_hit = True

            if (
                not free_response_hit
                and stream_id_for_gate is not None
                and self._engagement.is_engaged(
                    stream_id_for_gate, topic_for_gate, sender_email
                )
            ):
                engaged_followup = True

            should_process = False
            if free_response_hit:
                should_process = True
            elif chatmode == "onmessage":
                should_process = True
            elif chatmode == "oncall":
                should_process = was_mentioned or engaged_followup
            elif chatmode == "onchar":
                should_process = onchar_triggered or was_mentioned or engaged_followup

            if (
                chatmode != "onmessage"
                and not free_response_hit
                and not engaged_followup
                and require_mention
                and not was_mentioned
                and not onchar_triggered
            ):
                should_process = False

            if not should_process:
                logger.info(
                    "zulip drop [mode=%s mentioned=%s onchar=%s engaged=%s free=%s] msg=%s",
                    chatmode,
                    was_mentioned,
                    onchar_triggered,
                    engaged_followup,
                    free_response_hit,
                    message_id,
                )
                return

            if was_mentioned:
                content = normalize_mention(content, mention_regex)

            # Explicit engagement stop ("stop listening" / /unlisten) — not bare /stop
            if is_stop_listening_message(content):
                cleared = self._engagement.clear(
                    stream_id_for_gate, topic_for_gate, sender_email
                )
                await self._ack_engagement_stop(
                    message, cleared=cleared, stream_id=stream_id_for_gate, topic=topic_for_gate
                )
                return

            if was_mentioned or onchar_triggered or engaged_followup or free_response_hit:
                self._engagement.mark_engaged(
                    stream_id_for_gate,
                    topic_for_gate,
                    sender_email,
                    user_name=sender_full_name,
                )

            # Bare /stop and /new clear engagement but still fall through to gateway
            first_token = content.strip().split(maxsplit=1)[0].lower() if content.strip() else ""
            if first_token in ("/stop", "/new", "/reset") or is_end_session_message(content):
                self._engagement.clear(
                    stream_id_for_gate, topic_for_gate, sender_email
                )

        # --- Reactions (only once we know we'll process) ---
        reactions = ReactionLifecycle(
            self.client, str(message_id), self._reaction_cfg,
            timeout=self._send_timeout,
        )

        if msg_type == "stream":
            stream_id = message.get("stream_id")
            topic = message.get("subject") or message.get("topic") or ""
            stream_name = message.get("display_recipient") or str(stream_id)

            # Cache topic + stream name for reply threading and rename_topic.
            chat_id = str(stream_id)
            self._topic_cache[chat_id] = topic
            if stream_id is not None and stream_name:
                self._stream_names[chat_id] = str(stream_name)

            # chat_type="thread" + thread_id=topic is what gateway expects for
            # Zulip topic lanes (session keys, /title rename, handoffs).
            source = self.build_source(
                chat_id=chat_id,
                chat_name=str(stream_name),
                chat_type="thread",
                user_id=sender_email,
                user_name=sender_full_name,
                thread_id=topic or None,
                chat_topic=topic or None,
                message_id=str(message_id) if message_id is not None else None,
            )
            extra_meta = {
                "topic": topic,
                "thread_id": topic,
                "stream_id": stream_id,
                "stream_name": str(stream_name),
                "engaged_followup": engaged_followup,
            }
        else:
            sender_id = message.get("sender_id")
            chat_id = f"dm:{sender_id}"

            source = self.build_source(
                chat_id=chat_id,
                chat_name=sender_full_name,
                chat_type="dm",
                user_id=sender_email,
                user_name=sender_full_name,
                message_id=str(message_id) if message_id is not None else None,
            )
            extra_meta = {"user_id": sender_id, "user_email": sender_email}

        # Local admin commands (/help, /status, …) stay in-plugin.
        # Unknown slash commands — including /stop — fall through so the
        # gateway slash/interrupt layer can handle them.
        try:
            from .commands import handle_command

            cmd_result = handle_command(
                content,
                chat_id=chat_id,
                sender_email=sender_email,
                sender_name=sender_full_name,
            )
            if cmd_result.handled:
                await self.send(
                    chat_id,
                    cmd_result.reply or "",
                    metadata=extra_meta,
                )
                await reactions.success()
                if typing_params:
                    typing_params["op"] = "stop"
                    try:
                        await asyncio.to_thread(
                            self.client.set_typing_status, typing_params
                        )
                    except Exception:
                        pass
                return
        except Exception as exc:
            logger.debug("zulip local command handling failed: %s", exc)

        # --- Apply sticky or pending session context override ---
        # This lets /reply (or future /context) permanently or temporarily switch
        # which Hermes session the AI uses for this topic/chat.
        try:
            ckey = None
            if msg_type == "stream":
                t = topic_for_gate or (message.get("subject") or message.get("topic") or "")
                ckey = f"{chat_id}:{t}" if chat_id else None
            else:
                ckey = chat_id
            if ckey:
                sid = None
                if ckey in self._pending_context:
                    sid = self._pending_context.pop(ckey, None)
                    if sid:
                        extra_meta = dict(extra_meta)
                        extra_meta["gateway_session_id"] = sid
                        extra_meta["_context_mode"] = "pending"
                elif ckey in self._sticky_context:
                    sid = self._sticky_context.get(ckey)
                    if sid:
                        extra_meta = dict(extra_meta)
                        extra_meta["gateway_session_id"] = sid
                        extra_meta["_context_mode"] = "sticky"
        except Exception as _ctx_exc:
            logger.debug("context override failed: %s", _ctx_exc)

        event = MessageEvent(
            text=content,
            message_type=MessageType.TEXT,
            source=source,
            message_id=str(message_id) if message_id is not None else None,
            metadata=extra_meta,
        )

        try:
            await self.handle_message(event)
            # Record mapping so banners can show session ids instead of opaque Zulip ids
            if message_id is not None and getattr(event, "session_id", None):
                try:
                    self._zulip_to_session[int(message_id)] = event.session_id
                except Exception:
                    pass
            # Successful turn keeps sticky engagement warm
            if msg_type == "stream" and stream_id_for_gate is not None:
                self._engagement.touch(
                    stream_id_for_gate, topic_for_gate, sender_email
                )
            await reactions.success()
        except Exception:
            await reactions.error()
            raise
        finally:
            if typing_params:
                typing_params["op"] = "stop"
                try:
                    await asyncio.to_thread(
                        self.client.set_typing_status, typing_params
                    )
                except Exception:
                    pass


    async def _handle_reaction(self, event: dict):
        """Handle emoji reactions as an alternative way to select context.

        Users can react directly to a message with:
          📌 (pushpin)  → permanent/sticky context for the topic
          🔖 (bookmark) → temporary/one-off context for the next message

        This complements /reply N and /reply N sticky.

        Key behaviors:
        - Always sets _topic_anchors[ckey] = zid so future replies thread to
          the reacted message (even when no session is cached yet).
        - When a cached session exists, sets _sticky_context or _pending_context.
        - When no cached session exists (e.g. cron output), still anchors the
          topic so the user's next message will thread to the right message and
          create a new session seeded from that message's content.
        - Also tries to warm the session cache by fetching the reply banner,
          which populates _zulip_to_session for messages visible in the topic.
        """
        try:
            if event.get("op") != "add":
                return

            user = event.get("user") or {}
            sender_email = user.get("email") or ""
            if sender_email == self.email:
                return

            message_id = event.get("message_id")
            emoji = (event.get("emoji_name") or "").lower().strip()

            permanent = False
            if emoji in ("pushpin", "pin"):
                permanent = True
            elif emoji in ("bookmark", "book"):
                permanent = False
            else:
                return

            if not message_id:
                return
            zid = int(message_id)

            sid = self._zulip_to_session.get(zid)

            # Fetch the reacted message to get stream/topic for ckey
            topic = ""
            stream_id = None
            is_private = False
            sender_id = None
            try:
                res = await asyncio.to_thread(
                    self.client.get_messages,
                    {
                        "anchor": zid,
                        "num_before": 0,
                        "num_after": 0,
                        "include_anchor": True,
                    },
                )
                if res.get("result") == "success" and res.get("messages"):
                    m = res["messages"][0]
                    if m.get("type") == "private":
                        is_private = True
                        sender_id = m.get("sender_id")
                    else:
                        stream_id = m.get("stream_id")
                        topic = m.get("subject") or m.get("topic") or ""
            except Exception:
                pass

            # Build context key
            if is_private:
                ckey = f"dm:{sender_id}" if sender_id else f"dm:msg:{zid}"
            elif stream_id is not None:
                ckey = f"{stream_id}:{topic}"
            else:
                ckey = f"unknown:{zid}"

            # ----------------------------------------------------------------
            # Always set the topic anchor so future replies thread to this
            # message, regardless of whether we have a cached session.
            # This is the primary fix for the "stale anchor" bug.
            # ----------------------------------------------------------------
            if ckey and not is_private:
                self._topic_anchors[ckey] = zid

            # ----------------------------------------------------------------
            # Try to warm the session cache by fetching the reply banner.
            # This populates _zulip_to_session for messages visible in the
            # topic, including cron outputs that were not cached on delivery.
            # ----------------------------------------------------------------
            if not sid and stream_id and topic:
                try:
                    banner = await self._build_reply_banner(
                        str(stream_id),
                        {"topic": topic, "stream_id": stream_id},
                        zid,
                    )
                    # _build_reply_banner populates _zulip_to_session for
                    # all messages in the banner. Re-check after the call.
                    sid = self._zulip_to_session.get(zid)
                except Exception:
                    pass  # best effort — proceed without banner

            if sid:
                if permanent:
                    self._sticky_context[ckey] = sid
                else:
                    self._pending_context[ckey] = sid

                # Ensure visual marker
                try:
                    marker = "pushpin" if permanent else "bookmark"
                    await asyncio.to_thread(
                        self.client.add_reaction,
                        {"message_id": zid, "emoji_name": marker},
                    )
                except Exception:
                    pass

                # Short confirmation in the topic (best effort)
                ack = (
                    f"📌 Context set to `{sid}` (permanent) via reaction."
                    if permanent
                    else f"🔖 Context set to `{sid}` (temporary/one-off) via reaction."
                )
                try:
                    if not is_private and stream_id is not None:
                        await asyncio.to_thread(
                            self.client.send_message,
                            {
                                "type": "stream",
                                "to": int(stream_id),
                                "topic": topic or self.home_topic,
                                "content": ack,
                            },
                        )
                except Exception:
                    pass
            else:
                # No cached session id — set anchor and guide the user.
                # The anchor is already set above; this just notifies the user
                # that their next message will create a new session from this
                # message's content.
                try:
                    hint = (
                        f"Got 📌/🔖 on message #{zid}. No session cached for it yet — your next message in this topic will start a new session from this message. "
                        f"Use `/reply` to see numbered sessions with cached context, then react or `/reply N`."
                    )
                    if not is_private and stream_id is not None:
                        await asyncio.to_thread(
                            self.client.send_message,
                            {
                                "type": "stream",
                                "to": int(stream_id),
                                "topic": topic or self.home_topic,
                                "content": hint,
                            },
                        )
                except Exception:
                    pass

        except Exception as e:
            logger.debug("zulip reaction context handler error: %s", e)


    async def _ack_engagement_stop(
        self,
        message: dict,
        *,
        cleared: bool,
        stream_id: Any,
        topic: str,
    ) -> None:
        """Send a short confirmation that sticky listening ended."""
        msg_type = message.get("type")
        if cleared:
            text_out = (
                "Okay — I'll stop listening on this topic. "
                "@mention me again when you want to continue."
            )
        else:
            text_out = (
                "I wasn't actively listening on this topic. "
                "@mention me to start a conversation."
            )
        try:
            if msg_type == "private":
                await asyncio.to_thread(
                    self.client.send_message,
                    {
                        "type": "private",
                        "to": [message.get("sender_id")],
                        "content": text_out,
                    },
                )
            else:
                await asyncio.to_thread(
                    self.client.send_message,
                    {
                        "type": "stream",
                        "to": int(stream_id),
                        "topic": topic or self.home_topic,
                        "content": text_out,
                    },
                )
        except Exception as e:
            logger.warning("zulip engagement stop ack failed: %s", e)

    async def _build_reply_banner(self, chat_id, metadata, reply_to):
        """Build a reply context banner using Hermes *session* ids when known.

        Shows the target (if we have its session) and recent non-noisy messages
        in the topic with their session ids (falling back to Zulip id).
        Filters out gateway restart / status spam.
        """
        lines = []

        topic = metadata.get("topic") or metadata.get("thread_id") or metadata.get("subject")
        stream_id = metadata.get("stream_id")

        # Only filter truly spammy gateway status messages, not cron outputs
        # or other bot messages that the user may want to target.
        _NOISY_BOT_RE = re.compile(
            r"gateway (online|restarting|restart|reboot)|hermes is back",
            re.I,
        )

        target_sid = self._zulip_to_session.get(int(reply_to)) if reply_to else None
        if target_sid:
            lines.append(f"📎 Replying in session `{target_sid}` (zulip #{reply_to})")
        else:
            lines.append(f"📎 Replying to zulip message #{reply_to} (session unknown)")

        if not (stream_id and topic):
            return "\n".join(lines)

        try:
            # Fetch a few more than we need so we can filter noise
            # Use explicit narrow for reliable topic scoping (stream + topic)
            narrow = [
                {"operator": "stream", "operand": str(stream_id)},
                {"operator": "topic", "operand": topic},
            ]
            recent = await asyncio.to_thread(
                self.client.get_messages,
                {
                    "anchor": "newest",
                    "num_before": 12,
                    "num_after": 0,
                    "narrow": narrow,
                },
            )
            if recent.get("result") == "success" and recent.get("messages"):
                candidates = []
                for msg in recent["messages"]:
                    zid = msg.get("id")
                    sender = (msg.get("sender_full_name") or "?")[:28]
                    preview = msg.get("content", "") or ""
                    preview = re.sub(r"<[^>]+>", "", preview)
                    preview = re.sub(r"\s+", " ", preview).strip()[:55]

                    # Skip our own noisy status messages
                    if msg.get("sender_email") == self.email:
                        if _NOISY_BOT_RE.search(preview or "") or _NOISY_BOT_RE.search(msg.get("content", "")):
                            continue

                    sid = self._zulip_to_session.get(int(zid)) if zid else None
                    idx = len(candidates) + 1
                    candidates.append({
                        "index": idx,
                        "zulip_id": int(zid) if zid else None,
                        "session_id": sid,
                        "sender": sender,
                        "preview": preview,
                    })

                # Build a mixed list so the banner is always useful:
                # - Prefer messages that already have a cached Hermes session.
                # - Always include 1-2 recent "zulip#..." fallbacks (cron outputs, bot summaries,
                #   fresh messages, etc.) so the user has selectable targets and can start a
                #   new session/context from them.
                with_session = [c for c in candidates if c.get("session_id")]
                without = [c for c in candidates if not c.get("session_id")]

                display = list(with_session)
                if len(display) < 5:
                    display += without[: (5 - len(display)) ]

                for i, c in enumerate(display, 1):
                    c["index"] = i

                # Topic-scoped key (different topics in the same stream stay independent)
                if stream_id and topic:
                    list_key = f"{stream_id}:{topic}"
                else:
                    list_key = chat_id

                if display:
                    self._last_reply_list[list_key] = display[:5]
                    # Keep sid cache warm (useful for direct 📌/🔖 emoji reactions too)
                    for c in display:
                        if c.get("zulip_id") and c.get("session_id"):
                            self._zulip_to_session[c["zulip_id"]] = c["session_id"]

                    lines.append("Recent messages in this topic (use /reply N or react 📌/🔖):")
                    for c in display[:5]:
                        if c.get("session_id"):
                            label = f"`{c['session_id']}`"
                        else:
                            label = f"zulip#{c.get('zulip_id')} (no session yet — start from here)"
                        lines.append(f"  {c['index']}. {label} — {c['sender']}: {c['preview']}...")
                else:
                    lines.append("No recent messages to target in this topic yet.")
        except Exception as e:
            logger.debug("zulip failed to fetch recent messages for banner: %s", e)
            lines.append("⚠️ Could not fetch recent messages")

        return "\n".join(lines)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to=None,
        metadata=None,
        media_files=None,
    ) -> SendResult:
        """Send message to a Zulip stream or DM, with chunking, topic directives, and files."""
        metadata = metadata or {}
        media_files = media_files or []

        # If the caller didn't specify reply_to, but the user previously used /reply
        # or emoji to target a message in this topic (e.g. a cron output), thread to it.
        if reply_to is None and chat_id:
            try:
                t = (metadata or {}).get("topic") or (metadata or {}).get("thread_id") or ""
                ckey = f"{chat_id}:{t or ''}" if t else str(chat_id)
                if ckey in self._topic_anchors:
                    reply_to = self._topic_anchors.pop(ckey, None)
            except Exception:
                pass

        # Upload files first
        uploaded_urls = []
        if media_files:
            data_dir = os.environ.get("HERMES_DATA_DIR", os.path.expanduser("~/.hermes"))
            for file_path in media_files:
                # Security: reject URL-like values in media_files (must be local paths)
                if isinstance(file_path, str) and (file_path.startswith("http://") or file_path.startswith("https://")):
                    logger.warning(
                        "zulip send rejected URL in media_files [url=%s]",
                        mask_pii(file_path),
                    )
                    continue
                try:
                    url = await upload_file_to_zulip(
                        self.client, file_path, data_dir
                    )
                    uploaded_urls.append(url)
                except Exception as e:
                    logger.error("zulip upload failed [file=%s]: %s", mask_pii(file_path), e)

        # Append uploaded file links to content
        if uploaded_urls:
            file_links = "\n".join(f"[{Path(u).name}]({u})" for u in uploaded_urls)
            if content:
                content = f"{content}\n\n{file_links}"
            else:
                content = file_links

        # Add reply context banner with recent message IDs if replying to a message
        if reply_to is not None:
            banner = await self._build_reply_banner(chat_id, metadata, reply_to)
            if banner:
                content = f"{content}\n\n{banner}"

        # Extract inline topic directive if present
        content, topic_override = extract_topic_directive(content)

        limit, mode = _resolve_chunk_config()
        chunks = chunk_text(content, limit=limit, mode=mode)

        if not chunks:
            chunks = [""]

        last_result: Optional[SendResult] = None

        for idx, chunk in enumerate(chunks):
            result = await self._send_single(chat_id, chunk, metadata, topic_override, reply_to)
            last_result = result
            if not result.success:
                logger.error(
                    "zulip send failed on chunk %d/%d [chat=%s]",
                    idx + 1,
                    len(chunks),
                    mask_pii(chat_id),
                )

        return last_result or SendResult(success=False, message_id="")

    async def _send_single(
        self,
        chat_id: str,
        content: str,
        metadata: dict,
        topic_override: Optional[str],
        reply_to: Optional[int] = None,
    ) -> SendResult:
        """Send a single (unchunked) message."""
        try:
            chat_id_s = str(chat_id)
            if chat_id_s.startswith("dm:") or chat_id_s.startswith("dm_user:"):
                raw = (
                    chat_id_s[3:]
                    if chat_id_s.startswith("dm:")
                    else chat_id_s[len("dm_user:") :]
                )
                try:
                    to = [int(raw)]
                except ValueError:
                    to = [raw]
                payload: dict[str, Any] = {
                    "type": "private",
                    "to": to,
                    "content": content,
                }
                if reply_to is not None:
                    payload["reply_to"] = reply_to
                result = await asyncio.to_thread(self.client.send_message, payload)
            else:
                # Support "stream_id:topic" (cron deliver / explicit routing).
                stream_key = chat_id_s
                embedded_topic = None
                if ":" in chat_id_s and not chat_id_s.startswith("http"):
                    left, right = chat_id_s.rsplit(":", 1)
                    if left.isdigit() or left:
                        stream_key = left
                        embedded_topic = right or None

                try:
                    stream_to: Any = int(stream_key)
                except (TypeError, ValueError):
                    stream_to = stream_key

                topic = (
                    topic_override
                    or metadata.get("topic")
                    or metadata.get("thread_id")
                    or metadata.get("subject")
                    or embedded_topic
                    or self._topic_cache.get(str(stream_key))
                    or self.home_topic
                    or "general"
                )

                payload = {
                    "type": "stream",
                    "to": stream_to,
                    "topic": str(topic),
                    "content": content,
                }
                if reply_to is not None:
                    payload["reply_to"] = reply_to
                result = await asyncio.to_thread(self.client.send_message, payload)

                # Keep caches warm for later reply / rename.
                self._topic_cache[str(stream_key)] = str(topic)

            if result.get("result") == "success":
                logger.debug("zulip message sent to %s", chat_id)
                return SendResult(
                    success=True, message_id=str(result.get("id", ""))
                )
            else:
                logger.error(
                    format_zulip_log(
                        "zulip send failed",
                        chat_id=mask_pii(chat_id),
                        error=mask_pii(str(result)),
                    )
                )
                return SendResult(
                    success=False,
                    message_id="",
                    error=str(result.get("msg") or result),
                    raw_response=result,
                )

        except Exception as e:
            logger.error(
                format_zulip_log(
                    "zulip send error",
                    chat_id=mask_pii(chat_id),
                    error=mask_pii(str(e)),
                )
            )
            return SendResult(success=False, message_id="", error=str(e), retryable=True)

    # ------------------------------------------------------------------
    # Gateway integration methods (ported from feature-rich adapter work)
    # ------------------------------------------------------------------

    def supports_draft_streaming(
        self,
        chat_type: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> bool:
        """Zulip can stream via edit_message (send + progressive updates).

        Core still defaults ``display.platforms.zulip.streaming`` to False
        because progressive edits show EDITED tags and re-render Markdown.
        Returning True here only enables the path when the user opts in.
        """
        # Explicit opt-out via the plugin's historical env flag.
        if os.getenv("ZULIP_EDIT_PLACEHOLDER", "true").strip().lower() in (
            "false",
            "0",
            "no",
            "off",
        ):
            return False
        return True

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """Edit a previously sent Zulip message (streaming drafts / finalize).

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
            return SendResult(
                success=False, error=f"invalid message_id: {message_id!r}"
            )
        try:
            payload = {"message_id": msg_id, "content": content}
            result = await asyncio.to_thread(self.client.update_message, payload)
            if result.get("result") == "success":
                logger.debug(
                    "zulip edit_message ok id=%s len=%d finalize=%s",
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
            logger.warning("zulip edit_message failed: %s", err)
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
            logger.error("zulip edit_message error: %s", e)
            return SendResult(success=False, error=str(e), retryable=True)

    async def rename_topic(
        self,
        chat_id: str,
        old_topic: str,
        new_topic: str,
    ):
        """Rename a Zulip stream topic to match a Hermes session title.

        Uses Client.move_topic(stream, new_stream, topic, new_topic=...) with
        the same stream on both ends (in-place rename).

        Returns ``(ok, detail)`` where *detail* is None on success, or a short
        human-readable reason on failure / no-op.
        """
        if not old_topic or not new_topic:
            return False, "missing topic name"
        new_topic = str(new_topic).strip()[:60]
        old_topic = str(old_topic).strip()
        if not new_topic or new_topic == old_topic:
            return False, "topic unchanged"
        if str(chat_id).startswith("dm:") or str(chat_id).startswith("dm_user:"):
            return False, "DMs have no renameable topic"

        stream_key = str(chat_id)
        if ":" in stream_key:
            stream_key = stream_key.rsplit(":", 1)[0]

        stream = self._stream_names.get(stream_key)
        if not stream:
            # Best-effort fallbacks: home stream name, or the raw id/name.
            if stream_key == str(self.home_channel or ""):
                stream = self.home_topic or stream_key
            else:
                stream = stream_key
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
                    "zulip renamed topic %r -> %r on stream %s",
                    old_topic,
                    new_topic,
                    stream,
                )
                # Keep topic cache coherent for the stream.
                self._topic_cache[stream_key] = new_topic
                return True, None
            msg = ""
            if isinstance(result, dict):
                msg = str(result.get("msg") or result.get("code") or result)
            else:
                msg = str(result)
            logger.warning("zulip rename_topic failed: %s", result)
            return False, (msg.strip() or "Zulip rename failed")
        except Exception as e:
            logger.warning("zulip rename_topic error: %s", e)
            return False, str(e) or "Zulip rename error"

    async def create_handoff_thread(
        self,
        parent_chat_id: str,
        name: str,
    ) -> Optional[str]:
        """Create a dedicated Zulip topic for a CLI/session handoff.

        Returns the topic name on success (used as Hermes thread_id),
        or None on failure.
        """
        if str(parent_chat_id).startswith("dm:") or str(parent_chat_id).startswith(
            "dm_user:"
        ):
            return None

        name = str(name).strip() or "Hermes handoff"
        name = name[:60]
        seed_content = f"🔄 Hermes session handoff — {name}"

        stream_key = str(parent_chat_id)
        if ":" in stream_key:
            stream_key = stream_key.rsplit(":", 1)[0]
        try:
            stream_to: Any = int(stream_key)
        except (TypeError, ValueError):
            stream_to = stream_key

        try:
            payload = {
                "type": "stream",
                "to": stream_to,
                "topic": name,
                "content": seed_content,
            }
            result = await asyncio.to_thread(self.client.send_message, payload)
            if result.get("result") == "success":
                logger.info(
                    "zulip handoff thread created: topic=%s in stream %s",
                    name,
                    parent_chat_id,
                )
                self._topic_cache[str(stream_key)] = name
                return name
            err = result.get("msg") or str(result)
            logger.warning("zulip handoff thread creation failed: %s", err)
            return None
        except Exception as e:
            logger.warning("zulip handoff thread creation error: %s", e)
            return None


def check_requirements() -> bool:
    """Return True if the zulip SDK is installed."""
    return ZULIP_AVAILABLE


def validate_config(config) -> bool:
    """Validate that required credentials are present."""
    extra = getattr(config, "extra", {}) or {}
    return bool(
        (os.getenv("ZULIP_API_KEY") or extra.get("api_key"))
        and (os.getenv("ZULIP_EMAIL") or extra.get("email"))
        and (os.getenv("ZULIP_SITE") or extra.get("site"))
    )


def _env_enablement() -> dict | None:
    """Seed PlatformConfig.extra from environment variables."""
    key = os.getenv("ZULIP_API_KEY", "").strip()
    email = os.getenv("ZULIP_EMAIL", "").strip()
    site = os.getenv("ZULIP_SITE", "").strip()
    if not (key and email and site):
        return None

    seed = {"api_key": key, "email": email, "site": site}
    home = os.getenv("ZULIP_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": os.getenv("ZULIP_HOME_CHANNEL_NAME", "general"),
        }
    return seed


async def _standalone_send(
    pconfig,
    chat_id,
    message,
    *,
    thread_id=None,
    media_files=None,
    force_document=False,
):
    """Send from cron without a live gateway adapter."""
    if not ZULIP_AVAILABLE:
        return {"error": "zulip package not installed"}

    extra = getattr(pconfig, "extra", {}) or {}
    email = extra.get("email")
    api_key = extra.get("api_key")
    site = extra.get("site")
    home_topic = extra.get("home_topic", "general")

    if not (email and api_key and site):
        return {"error": "Zulip credentials missing in platform config"}

    # Resolve chunking config (same as live adapter)
    limit_raw = os.getenv("ZULIP_TEXT_CHUNK_LIMIT", "").strip()
    limit = int(limit_raw) if limit_raw.isdigit() else 10000
    mode = os.getenv("ZULIP_CHUNK_MODE", "length").strip()

    # Split message into chunks if needed
    chunks = chunk_text(message, limit=limit, mode=mode)
    if not chunks:
        chunks = [""]

    try:
        client = zulip.Client(email=email, api_key=api_key, site=site)

        if chat_id.startswith("dm:"):
            user_id = int(chat_id[3:])
            last_result = None
            for chunk in chunks:
                result = await asyncio.to_thread(
                    client.send_message,
                    {
                        "type": "private",
                        "to": [user_id],
                        "content": chunk,
                    },
                )
                last_result = result
        else:
            topic = thread_id or home_topic
            # Parse chat_id as "stream_id:topic" if it contains a colon
            if ":" in chat_id:
                parts = chat_id.rsplit(":", 1)
                stream_id = int(parts[0])
                if not topic:
                    topic = parts[1]
            else:
                stream_id = int(chat_id)
            last_result = None
            for chunk in chunks:
                result = await asyncio.to_thread(
                    client.send_message,
                    {
                        "type": "stream",
                        "to": stream_id,
                        "topic": topic,
                        "content": chunk,
                    },
                )
                last_result = result

        if last_result.get("result") == "success":
            return {"success": True, "message_id": str(last_result.get("id", ""))}
        else:
            return {"error": f"Zulip send failed: {last_result}"}

    except Exception as e:
        return {"error": f"Zulip standalone send error: {e}"}


def interactive_setup() -> None:
    """Interactive `hermes gateway setup` flow for the Zulip platform.

    Lazy-imports ``hermes_cli.setup`` helpers so the plugin stays importable
    in non-CLI contexts (gateway runtime, tests).
    """
    from hermes_cli.setup import (
        prompt,
        prompt_yes_no,
        save_env_value,
        get_env_value,
        print_header,
        print_info,
        print_warning,
        print_success,
    )

    print_header("Zulip")
    existing_email = get_env_value("ZULIP_EMAIL")
    if existing_email:
        print_info(f"Zulip: already configured ({existing_email})")
        if not prompt_yes_no("Reconfigure Zulip?", False):
            return

    print_info("Connect Hermes to Zulip via a bot account.")
    print_info("   Create a bot at: Settings → Bots → Add a new bot (Generic bot)")
    print()

    site = prompt(
        "Zulip site URL (e.g. https://your-org.zulipchat.com)",
        default=get_env_value("ZULIP_SITE") or "",
    )
    if not site:
        print_warning("Site URL is required — skipping Zulip setup")
        return
    save_env_value("ZULIP_SITE", site.rstrip("/").strip())

    email = prompt(
        "Bot email address (e.g. hermes-bot@your-org.zulipchat.com)",
        default=get_env_value("ZULIP_EMAIL") or "",
    )
    if not email:
        print_warning("Bot email is required — skipping Zulip setup")
        return
    save_env_value("ZULIP_EMAIL", email.strip())

    api_key = prompt(
        "Bot API key",
        default=get_env_value("ZULIP_API_KEY") or "",
        password=True,
    )
    if not api_key:
        print_warning("API key is required — skipping Zulip setup")
        return
    save_env_value("ZULIP_API_KEY", api_key.strip())

    # Authorization (optional but recommended)
    allowed = prompt(
        "Allowed user emails (comma-separated, or empty for none yet)",
        default=get_env_value("ZULIP_ALLOWED_USERS") or "",
    )
    if allowed:
        save_env_value("ZULIP_ALLOWED_USERS", allowed.strip())

    # Home channel for cron deliveries (optional)
    home = prompt(
        "Home stream ID for cron deliveries (numeric, or empty to set later)",
        default=get_env_value("ZULIP_HOME_CHANNEL") or "",
    )
    if home:
        try:
            int(home)
            save_env_value("ZULIP_HOME_CHANNEL", home.strip())
        except ValueError:
            print_warning(f"Invalid stream ID '{home}' — must be numeric")

    home_topic = prompt(
        "Default topic for cron deliveries (default: general)",
        default=get_env_value("ZULIP_HOME_CHANNEL_NAME") or "general",
    )
    if home_topic:
        save_env_value("ZULIP_HOME_CHANNEL_NAME", home_topic.strip())

    print_success("Zulip configured.")
    print_info("Tip: Subscribe your bot to streams via Stream settings → Subscribers")


def register(ctx):
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="zulip",
        label="Zulip",
        adapter_factory=lambda cfg: ZulipAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=["ZULIP_API_KEY", "ZULIP_EMAIL", "ZULIP_SITE"],
        install_hint="pip install zulip",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="ZULIP_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="ZULIP_ALLOWED_USERS",
        allow_all_env="ZULIP_ALLOW_ALL_USERS",
        max_message_length=10000,
        platform_hint=(
            "You are chatting via Zulip. Messages are organized into streams and topics. "
            "When replying to a stream message, preserve the original topic unless asked to change it."
        ),
        emoji="📬",
        setup_fn=interactive_setup,
    )
