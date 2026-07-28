"""
Zulip Platform Adapter for Hermes Gateway (Plugin)

Bi-directional integration with Zulip chat platform.
Supports stream messages (with topics) and private messages.
"""

import asyncio
import logging
import os
import re
from pathlib import Path
from typing import Optional, Any

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

logger = logging.getLogger(__name__)


# Chunking defaults (overridable via env)
DEFAULT_CHUNK_LIMIT = 10000  # Hermes registry max_message_length
DEFAULT_CHUNK_MODE = "length"


def _resolve_chunk_config() -> tuple[int, str]:
    """Read chunking config from environment."""
    limit_raw = os.getenv("ZULIP_TEXT_CHUNK_LIMIT", "").strip()
    limit = int(limit_raw) if limit_raw.isdigit() else DEFAULT_CHUNK_LIMIT
    mode = os.getenv("ZULIP_CHUNK_MODE", DEFAULT_CHUNK_MODE).strip()
    if mode not in ("length", "newline"):
        mode = DEFAULT_CHUNK_MODE
    return limit, mode


def _resolve_chatmode() -> tuple[str, list[str], bool]:
    """Read stream trigger mode config from environment."""
    mode = os.getenv("ZULIP_CHATMODE", "onmessage").strip().lower()
    if mode not in ("onmessage", "oncall", "onchar"):
        mode = "onmessage"
    prefixes = resolve_onchar_prefixes(os.getenv("ZULIP_ONCHAR_PREFIXES", ""))
    require_mention = os.getenv("ZULIP_REQUIRE_MENTION", "true").strip().lower() not in ("false", "0", "no", "off")
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

        # Track latest topic per stream so replies stay threaded
        self._topic_cache: dict[str, str] = {}
        # stream_id -> stream name (required by Client.move_topic)
        self._stream_names: dict[str, str] = {}

        self._data_dir = os.environ.get("HERMES_DATA_DIR", os.path.expanduser("~/.hermes"))

        # Persistent queue and dedupe
        self._queue_mgr = ZulipQueueManager(
            account_id=self.email or "default",
            data_dir=self._data_dir,
            register_fn=lambda: self.client.register(
                event_types=["message"], fetch_event_id=0
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

        self._listening = True
        self._event_task = asyncio.create_task(self._listen_for_events())
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
        if self._event_task:
            self._event_task.cancel()
            try:
                await self._event_task
            except asyncio.CancelledError:
                pass
        self._mark_disconnected()
        logger.info("Zulip adapter disconnected")

    async def _listen_for_events(self):
        """Listen for incoming Zulip messages via persistent event queue."""
        logger.info("zulip adapter listening [account=%s]", mask_pii(self.email))

        while self._listening:
            try:
                queue = await self._queue_mgr.ensure_queue()

                events = await asyncio.to_thread(
                    self.client.get_events,
                    queue_id=queue.queue_id,
                    last_event_id=queue.last_event_id,
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
                for event in events.get("events", []):
                    event_id = event["id"]
                    if event_id > batch_max_event_id:
                        batch_max_event_id = event_id
                    if event.get("type") == "message":
                        msg = event["message"]
                        msg_id = str(msg.get("id", ""))
                        # Dedupe check
                        if self._dedupe.check(msg_id):
                            logger.debug("zulip dedupe hit [msg=%s]", msg_id)
                            continue
                        await self._handle_message(msg)

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

        # Strip Zulip @-mention syntax and HTML
        content = strip_html_to_text(content)

        # --- Reactions ---
        reactions = ReactionLifecycle(
            self.client, str(message_id), self._reaction_cfg
        )
        await reactions.start()

        # --- Typing indicator ---
        typing_params = None
        if msg_type == "private":
            typing_params = {
                "op": "start",
                "type": "direct",
                "to": [message.get("sender_id")],
            }
        elif msg_type == "stream":
            stream_id = message.get("stream_id")
            topic = message.get("subject", "")
            if stream_id:
                typing_params = {
                    "op": "start",
                    "type": "stream",
                    "stream_id": stream_id,
                    "topic": topic,
                }

        if typing_params:
            try:
                await asyncio.to_thread(
                    self.client.set_typing_status, typing_params
                )
            except Exception:
                pass  # typing is best-effort

        # --- Stream trigger gating ---
        if msg_type == "stream":
            chatmode, onchar_prefixes, require_mention = _resolve_chatmode()

            # Check onchar trigger
            onchar_triggered, stripped = strip_onchar_prefix(content, onchar_prefixes)
            if onchar_triggered:
                content = stripped

            # Check mention (simple substring; bot username from email prefix)
            bot_username = self.email.split("@")[0] if self.email else ""
            mention_regex = create_mention_regex(bot_username) if bot_username else None
            was_mentioned = bool(mention_regex and mention_regex.search(content))

            # Apply gating
            should_process = False
            if chatmode == "onmessage":
                should_process = True
            elif chatmode == "oncall":
                should_process = was_mentioned
            elif chatmode == "onchar":
                should_process = onchar_triggered or was_mentioned

            # requireMention acts as additional gate (ignored in onmessage mode)
            if chatmode != "onmessage" and require_mention and not was_mentioned and not onchar_triggered:
                should_process = False

            if not should_process:
                logger.debug("zulip drop [mode=%s, no trigger] msg=%s", chatmode, message_id)
                return

            # Normalize mention from content
            if was_mentioned and mention_regex:
                content = normalize_mention(content, mention_regex)

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

        event = MessageEvent(
            text=content,
            message_type=MessageType.TEXT,
            source=source,
            message_id=str(message_id) if message_id is not None else None,
            metadata=extra_meta,
        )

        try:
            await self.handle_message(event)
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

        # Upload files first
        uploaded_urls = []
        if media_files:
            data_dir = os.environ.get("HERMES_DATA_DIR", os.path.expanduser("~/.hermes"))
            for file_path in media_files:
                try:
                    url = await upload_file_to_zulip(
                        self.client, file_path, data_dir
                    )
                    uploaded_urls.append(url)
                except Exception as e:
                    logger.error("zulip upload failed [file=%s]: %s", file_path, e)

        # Append uploaded file links to content
        if uploaded_urls:
            file_links = "\n".join(f"[{Path(u).name}]({u})" for u in uploaded_urls)
            if content:
                content = f"{content}\n\n{file_links}"
            else:
                content = file_links

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
                    chat_id,
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

    try:
        client = zulip.Client(email=email, api_key=api_key, site=site)

        if chat_id.startswith("dm:"):
            user_id = int(chat_id[3:])
            result = await asyncio.to_thread(
                client.send_message,
                {
                    "type": "private",
                    "to": [user_id],
                    "content": message,
                },
            )
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
            result = await asyncio.to_thread(
                client.send_message,
                {
                    "type": "stream",
                    "to": stream_id,
                    "topic": topic,
                    "content": message,
                },
            )

        if result.get("result") == "success":
            return {"success": True, "message_id": str(result.get("id", ""))}
        else:
            return {"error": f"Zulip send failed: {result}"}

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
