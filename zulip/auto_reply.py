"""Automatic reply routing for unmentioned Zulip stream messages.

When a human posts in a stream topic without @-mentioning any bot, exactly
one bot owns the reply:

1. the profile that posted the topic's most recent cron-marked post (inside
   the configured window), via :mod:`zulip.cron_ownership`; otherwise
2. the configured fallback profile (default ``ai-agent``).

An explicit @-mention always wins and keeps today's behaviour. A
per-message-id claim (:class:`MessageClaimRegistry`) guarantees the six
adapters in one gateway never double-reply: normally only the owner even
tries, and the claim settles the race when two adapters disagree.

The owning bot runs a normal turn with a gating instruction prepended
(LINE group-observation style): the model judges whether the message needs
a reply and outputs exactly ``[SILENT]`` when it does not. The event carries
``reply_expected=False`` so the gateway lets the silence marker stand, and
``metadata["zulip_auto_reply"]`` so the adapter's run-end audit can tell
silence-by-design apart from a failed delivery.
"""

from __future__ import annotations

import logging
import os
import re
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from . import cron_ownership
from . import runtime_scope
from .engagement import is_bot_sender
from .topic_ingest import parse_cron_marker, plain_content

logger = logging.getLogger(__name__)

SILENT_MARKER = "[SILENT]"

#: A successful claim stays valid this long; stale claim files are reaped.
CLAIM_TTL_SECONDS = 600.0

#: How many of the topic's newest messages to scan for a cron marker.
_CRON_SCAN_DEPTH = 100

#: How long a successful topic owner lookup is cached (shared by all
#: adapters). A new cron post can take up to this long to change the owner;
#: the per-message claim still guarantees a single replier.
_TOPIC_OWNER_CACHE_TTL = 60.0

#: Timeout for the single get_messages round-trip behind the topic-owner
#: lookup. Zulip API calls from the homelab routinely take several seconds,
#: so this is generous on purpose; the lookup runs in a worker thread and
#: its result is cached, so a slow call delays one message, not the loop.
_TOPIC_OWNER_API_TIMEOUT_S = 15.0

_topic_owner_lock = threading.RLock()
# (stream_id, topic.lower()) -> (expires_at, TopicCronOwner | None)
_topic_owner_cache: dict[tuple[str, str], tuple[float, Optional["TopicCronOwner"]]] = {}


def _topic_owner_cache_get(stream_id: Any, topic: str) -> Optional[tuple[bool, Optional["TopicCronOwner"]]]:
    """(hit, owner) — hit False means "no usable cache entry"."""
    key = (str(stream_id), (topic or "").strip().lower())
    now = time.time()
    with _topic_owner_lock:
        entry = _topic_owner_cache.get(key)
        if entry is None or entry[0] <= now:
            return None
        return True, entry[1]


def _topic_owner_cache_put(stream_id: Any, topic: str, owner: Optional["TopicCronOwner"]) -> None:
    key = (str(stream_id), (topic or "").strip().lower())
    with _topic_owner_lock:
        if len(_topic_owner_cache) > 4096:
            now = time.time()
            for k in [k for k, v in _topic_owner_cache.items() if v[0] <= now]:
                _topic_owner_cache.pop(k, None)
        _topic_owner_cache[key] = (time.time() + _TOPIC_OWNER_CACHE_TTL, owner)


def reset_topic_owner_cache_for_tests() -> None:
    with _topic_owner_lock:
        _topic_owner_cache.clear()


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AutoReplySettings:
    enabled: bool = True
    window_hours: float = 48.0
    fallback_profile: str = "ai-agent"
    streams: frozenset = frozenset()  # empty = every stream


def _truthy(raw: Optional[str], default: bool) -> bool:
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() not in ("false", "0", "no", "off")


def resolve_auto_reply_settings() -> AutoReplySettings:
    """Read ``ZULIP_AUTO_REPLY_*`` through the profile-scoped resolver."""
    enabled = _truthy(runtime_scope.get_setting("ZULIP_AUTO_REPLY_ENABLED", "true"), True)
    window_raw = (runtime_scope.get_setting("ZULIP_AUTO_REPLY_WINDOW_HOURS", "48") or "").strip()
    try:
        window_hours = float(window_raw)
    except ValueError:
        logger.warning("ZULIP_AUTO_REPLY_WINDOW_HOURS=%r invalid; using 48", window_raw)
        window_hours = 48.0
    fallback = (runtime_scope.get_setting("ZULIP_AUTO_REPLY_FALLBACK_PROFILE", "ai-agent") or "").strip()
    streams_raw = (runtime_scope.get_setting("ZULIP_AUTO_REPLY_STREAMS", "") or "").strip()
    streams = frozenset(s.strip().lower() for s in streams_raw.split(",") if s.strip())
    return AutoReplySettings(
        enabled=enabled,
        window_hours=window_hours,
        fallback_profile=fallback or "ai-agent",
        streams=streams,
    )


# ---------------------------------------------------------------------------
# Gating instruction (LINE group-observation style)
# ---------------------------------------------------------------------------

GATE_HEADER = "[Zulip auto-reply \u2014 you were not @-mentioned]"


def build_gating_prefix(*, profile: str, reason: str) -> str:
    """Instruction prepended to an auto-reply turn's prompt."""
    return (
        f"{GATE_HEADER}\n"
        f"A person posted in this topic without mentioning any bot. "
        f"You ({profile}) own replies here: {reason}.\n"
        "First decide whether this message needs a reply from you \u2014 is it "
        "addressed to you, asking you something, or calling for action?\n"
        "- If yes: reply normally and helpfully.\n"
        f"- If no: output exactly {SILENT_MARKER} and nothing else. "
        "No explanation, no punctuation, no surrounding text.\n"
        "\n"
    )


def is_silent_reply(text: Any) -> bool:
    """Exact ``[SILENT]`` match after stripping whitespace."""
    return isinstance(text, str) and text.strip() == SILENT_MARKER


# ---------------------------------------------------------------------------
# Mention detection — an explicit mention of *anyone* defers to today's path
# ---------------------------------------------------------------------------

_MENTION_RE = re.compile(r"@(?:\*\*|\*|all\b|everyone\b|channel\b|topic\b)", re.IGNORECASE)


def message_mentions_anyone(raw_content: Any) -> bool:
    """True when the raw Zulip content mentions a user, group, or wildcard."""
    if not isinstance(raw_content, str) or not raw_content:
        return False
    return bool(_MENTION_RE.search(raw_content))


# ---------------------------------------------------------------------------
# Per-message-id claim: exactly one adapter handles a message
# ---------------------------------------------------------------------------


def _claims_dir_for(adapter: Any) -> Path:
    root = cron_ownership.profiles_root_for(adapter)
    base = root if root is not None else Path(tempfile.gettempdir())
    directory = base / ".zulip-auto-reply-claims"
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        logger.debug("auto_reply: claims dir not writable: %s", exc)
    return directory


class MessageClaimRegistry:
    """Single-winner claim per Zulip message id.

    In-process winners are recorded in a shared dict (all six adapters run
    in one gateway process); an atomic claim file (O_CREAT|O_EXCL) makes the
    verdict hold across processes too. Claims expire after
    :data:`CLAIM_TTL_SECONDS`.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._claims: dict[str, tuple[str, float]] = {}  # message_id -> (profile, expires_at)

    def _file_claim(self, message_id: str, profile: str, claims_dir: Path) -> bool:
        path = claims_dir / f"{message_id}.claim"
        now = time.time()
        payload = f"{profile}\n{now + CLAIM_TTL_SECONDS:.0f}".encode("utf-8")
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            try:
                raw = path.read_text(encoding="utf-8").splitlines()
                winner = raw[0].strip() if raw else ""
                expires = float(raw[1]) if len(raw) > 1 else 0.0
            except Exception:
                winner, expires = "", 0.0
            if expires > now:
                # Someone already won this message. A re-claim by the
                # recorded winner (e.g. after a gateway restart cleared the
                # in-process dict) still counts as a win.
                return winner == profile
            # Stale claim: reap and try once more.
            try:
                path.unlink()
            except OSError:
                return False
            try:
                fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                return False
        except OSError:
            # Claims dir unusable: fall back to the in-process verdict only.
            return True
        try:
            os.write(fd, payload)
        finally:
            os.close(fd)
        return True

    def claim(self, message_id: Any, profile: str, *, claims_dir: Optional[Path] = None) -> bool:
        """Return True when ``profile`` wins the claim for ``message_id``."""
        mid = str(message_id or "").strip()
        if not mid:
            return False
        now = time.time()
        with self._lock:
            winner = self._claims.get(mid)
            if winner is not None and winner[1] > now:
                return winner[0] == profile
            # Opportunistic reap of expired entries.
            if len(self._claims) > 4096:
                self._claims = {k: v for k, v in self._claims.items() if v[1] > now}
        directory = claims_dir if claims_dir is not None else Path(tempfile.gettempdir())
        won_file = self._file_claim(mid, profile, directory)
        with self._lock:
            current = self._claims.get(mid)
            if current is not None and current[1] > now:
                return current[0] == profile
            if won_file:
                self._claims[mid] = (profile, now + CLAIM_TTL_SECONDS)
                return True
            return False

    def reset_for_tests(self) -> None:
        with self._lock:
            self._claims.clear()


_CLAIMS = MessageClaimRegistry()


def claim_message(message_id: Any, profile: str, *, adapter: Any = None) -> bool:
    """Module-level shared claim used by every adapter in the gateway."""
    claims_dir = _claims_dir_for(adapter) if adapter is not None else None
    return _CLAIMS.claim(message_id, profile, claims_dir=claims_dir)


def reset_claims_for_tests() -> None:
    _CLAIMS.reset_for_tests()


# ---------------------------------------------------------------------------
# Topic cron-owner lookup
# ---------------------------------------------------------------------------


@dataclass
class TopicCronOwner:
    job_id: str
    run_id: str
    sender_id: Any
    sender_email: str
    posted_at: float
    owner_profile: Optional[str]


async def resolve_topic_cron_owner(
    adapter: Any,
    stream_id: Any,
    topic: str,
    *,
    settings: AutoReplySettings,
    stream_name: str = "",
) -> tuple[Optional[TopicCronOwner], bool]:
    """Most recent in-window cron post in the topic, with its owning profile.

    Returns ``(owner_or_None, api_ok)``. One bounded Zulip API round-trip
    (newest-first narrow). ``api_ok=False`` means the fetch failed and the
    caller must not treat the miss as "no cron here".
    """
    client = getattr(adapter, "client", None)
    sdk_call = getattr(adapter, "_sdk_call", None)
    if client is None or sdk_call is None:
        return None, False
    narrow = [{"operator": "stream", "operand": stream_name or stream_id}]
    if topic:
        narrow.append({"operator": "topic", "operand": topic})
    try:
        result = await sdk_call(
            client.get_messages,
            {
                "anchor": "newest",
                "num_before": _CRON_SCAN_DEPTH,
                "num_after": 0,
                "narrow": narrow,
            },
            timeout=_TOPIC_OWNER_API_TIMEOUT_S,
        )
    except Exception as exc:
        logger.debug("auto_reply: topic history fetch failed: %s", exc)
        return None, False
    if not isinstance(result, dict) or result.get("result") != "success":
        return None, False
    messages = result.get("messages")
    if not isinstance(messages, list):
        return None, False
    now = time.time()
    window_s = max(0.0, settings.window_hours) * 3600.0
    for msg in sorted(messages, key=lambda m: m.get("id", 0) or 0, reverse=True):
        marker = parse_cron_marker(plain_content(str(msg.get("content") or "")))
        if marker is None:
            continue
        posted_at = msg.get("timestamp")
        try:
            posted_at = float(posted_at)
        except (TypeError, ValueError):
            continue
        if now - posted_at > window_s:
            # Newest marker is already out of the window; older ones are too.
            return None, True
        owner = cron_ownership.resolve_cron_owner(
            adapter=adapter,
            job_id=marker.job_id,
            sender_id=msg.get("sender_id"),
            sender_email=str(msg.get("sender_email") or ""),
        )
        return TopicCronOwner(
            job_id=marker.job_id,
            run_id=marker.run_id,
            sender_id=msg.get("sender_id"),
            sender_email=str(msg.get("sender_email") or ""),
            posted_at=posted_at,
            owner_profile=owner,
        ), True
    return None, True


# ---------------------------------------------------------------------------
# Routing decision
# ---------------------------------------------------------------------------


@dataclass
class AutoReplyRoute:
    owner_profile: str
    reason: str
    is_owner: bool


async def evaluate_auto_reply(
    adapter: Any,
    message: Any,
    *,
    was_mentioned: bool,
    addressed: bool,
    msg_type: str,
    stream_name: str,
    settings: Optional[AutoReplySettings] = None,
) -> Optional[AutoReplyRoute]:
    """Decide whether this adapter owns the reply to an unmentioned message.

    Returns None when auto-reply does not apply (today's drop path stays);
    otherwise an :class:`AutoReplyRoute` naming the owning profile.
    """
    cfg = settings or resolve_auto_reply_settings()
    if not cfg.enabled:
        return None
    if msg_type != "stream":
        return None
    if cfg.streams and stream_name.strip().lower() not in cfg.streams:
        return None
    # Loop prevention: never auto-reply to bot traffic.
    if is_bot_sender(
        str(message.get("sender_email") or ""),
        sender_id=message.get("sender_id"),
        bot_email=getattr(adapter, "email", ""),
        bot_user_id=getattr(adapter, "_bot_user_id", ""),
    ):
        return None
    # An explicit @-mention always wins and keeps today's behaviour.
    if was_mentioned or addressed:
        return None
    # Someone was mentioned by name (another bot or a person): defer to the
    # mention path rather than risk a second, unaddressed reply.
    if message_mentions_anyone(message.get("content") if isinstance(message, dict) else ""):
        return None
    cron_ownership.register_bot_identity(adapter)
    me = cron_ownership.own_profile_name(adapter)
    stream_id = message.get("stream_id")
    topic = (message.get("subject") or "").strip()
    cached = _topic_owner_cache_get(stream_id, topic)
    if cached is not None:
        _, found = cached
    else:
        found, api_ok = await resolve_topic_cron_owner(
            adapter,
            stream_id,
            topic,
            settings=cfg,
            stream_name=stream_name,
        )
        # Cache only successful lookups; an API failure must not suppress
        # the retry on the next message.
        if api_ok:
            _topic_owner_cache_put(stream_id, topic, found)
    if found is not None and found.owner_profile:
        reason = (
            f"the most recent cron post here (job {found.job_id}) "
            f"was posted by your profile"
        )
        return AutoReplyRoute(owner_profile=found.owner_profile, reason=reason,
                              is_owner=found.owner_profile == me)
    reason = f"you are the configured fallback bot ({cfg.fallback_profile})"
    return AutoReplyRoute(owner_profile=cfg.fallback_profile, reason=reason,
                          is_owner=cfg.fallback_profile == me)
