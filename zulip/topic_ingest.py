"""Record Zulip topic traffic into the live Hermes topic session.

The session is the adapter's existing one. ``TopicConversationRegistry``
maps ``(channel_id, topic_name)`` to a ``conversation_id``. The transcript
key is the Hermes thread key::

    agent:<ns>:zulip:thread:<stream_id>:<conversation_id>

``<ns>`` follows ``gateway.session._session_key_namespace`` (default profile
→ ``main``, a profile literally named ``main`` → ``main~``). There is no
user-email suffix. Renames and moves are not aliased here: the poll loop
calls ``adapter._handle_topic_update``, which is
``RoutingState.handle_topic_update`` (R2/R3/R5/R8 in ``zulip.routing``).

Observation matches LINE: a ``role=user`` row with ``observed=True``, and
no reply. Dispatched (addressed) human messages are not written here. The
gateway already appends that user turn in ``gateway/run_turn.py`` (the
``_user_row`` / ``append_to_transcript`` block). Ingesting them again would
double-record.

``group_sessions_per_user``
---------------------------
``gateway.session.build_session_key`` appends the participant id when
``isolate_user`` is true. For a non-DM:

    isolate_user = group_sessions_per_user and not (
        thread_id and not thread_sessions_per_user
    )

``group_sessions_per_user`` defaults true (``gateway/config.py``). Threads
stay shared unless ``thread_sessions_per_user`` is also true (default
false). LINE forces both flags off in the adapter and, independently,
``build_session_key`` hardcodes ``isolate_user = False`` for platform
``line`` so one chat is one session.

Zulip topic sessions set ``chat_type="thread"`` and ``thread_id`` to the
conversation id (``zulip.inbound``). Under the defaults that key is already
shared. If ``thread_sessions_per_user`` is turned on, a message from user A
lands in ``...:<conversation_id>:a@...`` and user B's turn reads a different
key. A cron delivery has no human sender; keying it on the bot email (or on
no suffix) misses both human sessions.

This module always writes the shared topic key (``user_id`` left empty on
the source passed to the store, so a Hermes ``build_session_key`` omits the
participant even when thread isolation is on). ``install_shared_topic_sessions``
also sets the two extra flags the way LINE does, which
``BasePlatformAdapter._source_session_key`` reads. It does not change
``SessionStore._generate_session_key``, which reads ``GatewayConfig``
directly (``gateway/session_recovery.py``). See REPORT.
"""

from __future__ import annotations

import logging
import os
import re
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional

from . import cron_ownership
from . import history
from .text_utils import strip_html_to_text

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cron marker (single implementation; cron.zulip_marker re-exports these)
# ---------------------------------------------------------------------------

CRON_MARKER_PREFIX = "-# 🕒"
_MARKER_LINE_RE = re.compile(
    r"^[ \t]*-#[ \t]*(?:🕒[ \t]*)?cron:(?P<job_id>[A-Za-z0-9._-]+)[ \t]+"
    r"run:(?P<run_id>[A-Za-z0-9._-]+)[ \t]*$",
    re.MULTILINE,
)
_MARKER_INLINE_RE = re.compile(
    r"-#[ \t]*(?:🕒[ \t]*)?cron:(?P<job_id>[A-Za-z0-9._-]+)[ \t]+"
    r"run:(?P<run_id>[A-Za-z0-9._-]+)"
)

BOT_EMAIL_SUFFIX = "-bot"
_SEEN_MAX = 4096


@dataclass(frozen=True)
class CronMarker:
    job_id: str
    run_id: str

    def line(self) -> str:
        return f"{CRON_MARKER_PREFIX} cron:{self.job_id} run:{self.run_id}"


def format_cron_marker(job_id: str, run_id: str) -> str:
    """A single footer line (no surrounding newlines)."""
    job = _sanitize_marker_id(job_id)
    run = _sanitize_marker_id(run_id)
    if not job or not run:
        raise ValueError("job_id and run_id must be non-empty [A-Za-z0-9._-]+")
    return CronMarker(job, run).line()


def _sanitize_marker_id(value: str) -> str:
    text = (value or "").strip()
    return re.sub(r"[^A-Za-z0-9._-]+", "-", text).strip("-._")


def parse_cron_marker(content: str) -> Optional[CronMarker]:
    """Return the last cron marker found in ``content``, or None."""
    if not content:
        return None
    matches = list(_MARKER_LINE_RE.finditer(content))
    if not matches:
        matches = list(_MARKER_INLINE_RE.finditer(content))
    if not matches:
        return None
    match = matches[-1]
    return CronMarker(job_id=match.group("job_id"), run_id=match.group("run_id"))


def append_cron_marker(content: str, job_id: str, run_id: str) -> str:
    """Append the footer unless this job/run marker is already present."""
    marker = format_cron_marker(job_id, run_id)
    body = content if content is not None else ""
    existing = parse_cron_marker(body)
    if (
        existing
        and existing.job_id == _sanitize_marker_id(job_id)
        and existing.run_id == _sanitize_marker_id(run_id)
    ):
        return body
    if body and not body.endswith("\n"):
        body += "\n"
    return body + "\n" + marker + "\n"


def strip_cron_marker(content: str) -> str:
    """Remove marker lines so the stored body is the delivery text."""
    if not content:
        return content or ""
    lines = [ln for ln in content.splitlines() if not _MARKER_LINE_RE.match(ln)]
    return "\n".join(lines).rstrip() + ("\n" if content.endswith("\n") else "")


def plain_content(content: str) -> str:
    """Marker-bearing text after the adapter's HTML strip.

    ``strip_html_to_text`` drops tags (including HTML comments). A footer
    wrapped in ``<p>`` still parses; a comment does not.
    """
    return strip_html_to_text(content or "")


# ---------------------------------------------------------------------------
# Session key (shared per topic — no participant suffix)
# ---------------------------------------------------------------------------


def session_namespace(profile: Optional[str]) -> str:
    """Match ``gateway.session._session_key_namespace``."""
    if not profile or profile == "default":
        return "agent:main"
    if profile == "main":
        return "agent:main~"
    return f"agent:{profile}"


def topic_session_key(
    profile: Optional[str], stream_id: int | str, conversation_id: str
) -> str:
    """Shared topic key. No sender email.

    Equals ``build_session_key`` for a Zulip thread when
    ``thread_sessions_per_user`` is false (the Hermes default), and also
    when the source's ``user_id`` is empty (what ingest passes the store).
    """
    try:
        sid = int(stream_id)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"stream_id must be an int, got {stream_id!r}") from exc
    conv = str(conversation_id or "").strip()
    if not conv:
        raise ValueError("conversation_id is required")
    return f"{session_namespace(profile)}:zulip:thread:{sid}:{conv}"


def hermes_session_key(
    *,
    profile: Optional[str],
    platform: str = "zulip",
    chat_type: str = "thread",
    chat_id: str = "",
    thread_id: Optional[str] = None,
    user_id: Optional[str] = None,
    group_sessions_per_user: bool = True,
    thread_sessions_per_user: bool = False,
) -> str:
    """Mirror of the Zulip/LINE branch of ``gateway.session.build_session_key``.

    Tests use this to show the default Hermes key and the per-user key
    without importing the gateway. ``thread_id`` here is ``source.thread_id``
    (Zulip sets ``chat_type`` to ``thread`` itself). The prospective-thread
    slot rewrite is not modeled.
    """
    is_dm = chat_type == "dm"
    if is_dm:
        isolate = not bool(chat_id)
    else:
        isolate = bool(group_sessions_per_user) and not (
            bool(thread_id) and not thread_sessions_per_user
        )
    if chat_id and str(platform).lower() == "line":
        isolate = False
    parts = [session_namespace(profile), str(platform), chat_type]
    if chat_id:
        parts.append(str(chat_id))
    user_part = [str(user_id)] if isolate and user_id else []
    thread_part = [str(thread_id)] if thread_id else []
    parts += user_part + thread_part if is_dm else thread_part + user_part
    return ":".join(parts)


def adapter_profile(adapter: Any) -> str:
    for obj in (adapter, getattr(adapter, "config", None)):
        if obj is None:
            continue
        for attr in ("profile", "profile_name", "_profile"):
            value = getattr(obj, attr, None)
            if isinstance(value, str) and value.strip():
                return value.strip()
    env = (os.environ.get("HERMES_PROFILE") or "").strip()
    return env or "default"


def install_shared_topic_sessions(adapter: Any) -> None:
    """Force one session per topic on the adapter extra dict (LINE's flags).

    ``BasePlatformAdapter._source_session_key`` reads
    ``config.extra['group_sessions_per_user']`` and
    ``thread_sessions_per_user``. The session store does not: it reads
    ``GatewayConfig``. Ingest still passes an empty ``user_id`` so the
    stored key stays shared either way.
    """
    cfg = getattr(adapter, "config", None)
    extra = getattr(cfg, "extra", None)
    if isinstance(extra, dict):
        extra["group_sessions_per_user"] = False
        extra["thread_sessions_per_user"] = False


# ---------------------------------------------------------------------------
# Sender policy
# ---------------------------------------------------------------------------


@dataclass
class UserRecord:
    user_id: int | str
    email: str = ""
    is_bot: Optional[bool] = None
    bot_type: Optional[int] = None
    full_name: str = ""


class UserCache:
    """Bot-ness from ``/register`` ``realm_users`` (messages have no ``is_bot``)."""

    def __init__(self, users: Optional[Iterable[Mapping[str, Any]]] = None):
        self._by_id: dict[str, UserRecord] = {}
        self._by_email: dict[str, UserRecord] = {}
        if users:
            for row in users:
                self.upsert(row)

    def upsert(self, row: Mapping[str, Any]) -> UserRecord:
        uid = row.get("user_id", row.get("id"))
        rec = UserRecord(
            user_id=uid if uid is not None else "",
            email=str(row.get("email") or "").strip(),
            is_bot=_optional_bool(row.get("is_bot")),
            bot_type=row.get("bot_type"),
            full_name=str(row.get("full_name") or "").strip(),
        )
        if rec.user_id != "" and rec.user_id is not None:
            self._by_id[str(rec.user_id)] = rec
        if rec.email:
            self._by_email[rec.email.lower()] = rec
        return rec

    def lookup(
        self, *, sender_id: Any = None, sender_email: str = ""
    ) -> Optional[UserRecord]:
        if sender_id is not None and str(sender_id) != "":
            found = self._by_id.get(str(sender_id))
            if found is not None:
                return found
        email = (sender_email or "").strip().lower()
        if email:
            return self._by_email.get(email)
        return None


def _optional_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("true", "1", "yes"):
            return True
        if text in ("false", "0", "no"):
            return False
    return None


def is_bot_email(email: str) -> bool:
    text = (email or "").strip().lower()
    local, sep, _domain = text.partition("@")
    return bool(sep) and local.endswith(BOT_EMAIL_SUFFIX)


def sender_is_bot(
    *,
    sender_email: str = "",
    sender_id: Any = None,
    own_bot_email: str = "",
    own_bot_user_id: Any = None,
    user_cache: Optional[UserCache] = None,
    is_bot_hint: Optional[bool] = None,
) -> bool:
    """Cache ``is_bot``, else own identity, else the ``-bot@`` local-part."""
    if is_bot_hint is True:
        return True
    cached_human = False
    if user_cache is not None:
        rec = user_cache.lookup(sender_id=sender_id, sender_email=sender_email)
        if rec is not None and rec.is_bot is True:
            return True
        if rec is not None and rec.is_bot is False:
            cached_human = True
    own_email = (own_bot_email or "").strip().lower()
    email = (sender_email or "").strip().lower()
    if email and own_email and email == own_email:
        return True
    if (
        sender_id is not None
        and str(sender_id) != ""
        and own_bot_user_id is not None
        and str(own_bot_user_id) != ""
        and str(sender_id) == str(own_bot_user_id)
    ):
        return True
    if is_bot_hint is False or cached_human:
        return False
    return is_bot_email(email)


def _parse_ids(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, (list, tuple, set)):
        parts: Iterable[Any] = value
    else:
        parts = re.split(r"[\s,]+", str(value).strip())
    return {str(part).strip() for part in parts if str(part).strip()}


def _config_extra(adapter: Any) -> Optional[dict]:
    cfg = getattr(adapter, "config", None)
    extra = getattr(cfg, "extra", None)
    if isinstance(extra, dict):
        return extra
    if isinstance(cfg, dict):
        return cfg
    return None


def _config_cron_bot_ids(adapter: Any) -> Any:
    """The configured value, or a private missing sentinel's stand-in (None).

    None means the key is absent. An empty list is an explicit empty allowlist.
    """
    extra = _config_extra(adapter)
    if not extra:
        return None
    dotted = "zulip.topic_ingest.cron_bot_ids"
    if dotted in extra:
        return extra[dotted]
    zulip_cfg = extra.get("zulip")
    if isinstance(zulip_cfg, dict):
        topic_cfg = zulip_cfg.get("topic_ingest")
        if isinstance(topic_cfg, dict) and "cron_bot_ids" in topic_cfg:
            return topic_cfg["cron_bot_ids"]
    return None


def _env_cron_bot_ids() -> Optional[str]:
    """``ZULIP_CRON_BOT_IDS`` from the active profile, then process env.

    Under ``gateway.multiplex_profiles``, ``ZULIP_*`` lives in the profile
    secret scope and is not copied into ``os.environ``. ``runtime_scope.get_setting``
    reads that scope; ``os.environ`` is only the fallback when the helper is
    unavailable (standalone tests / ImportError).
    """
    raw: Any = None
    try:
        from .runtime_scope import get_setting

        raw = get_setting("ZULIP_CRON_BOT_IDS")
    except Exception:
        raw = None
    if raw is None or not str(raw).strip():
        raw = os.environ.get("ZULIP_CRON_BOT_IDS")
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def cron_bot_allowlist(adapter: Any) -> set[str]:
    """Cron-delivery bot user ids allowed to be ingested.

    Precedence: env ``ZULIP_CRON_BOT_IDS`` (profile secret scope, then
    ``os.environ``), then config ``zulip.topic_ingest.cron_bot_ids``, else
    the adapter's own ``_bot_user_id``. An explicit empty list allows no bots.

    ``*`` (the string, or a list containing ``*``) means any bot sender
    with a valid cron marker is accepted. It does not classify humans as
    bots.
    """
    raw_env = _env_cron_bot_ids()
    if raw_env is not None and raw_env.strip():
        return _parse_ids(raw_env)
    configured = _config_cron_bot_ids(adapter)
    if configured is not None:
        return _parse_ids(configured)
    own = getattr(adapter, "_bot_user_id", None)
    if own is None or str(own).strip() == "":
        return set()
    return {str(own)}


@dataclass(frozen=True)
class IngestDecision:
    ingest: bool
    reason: str
    cron_marker: Optional[CronMarker] = None

    def __bool__(self) -> bool:
        return self.ingest


def should_ingest(
    *,
    content: str = "",
    sender_email: str = "",
    sender_id: Any = None,
    own_bot_email: str = "",
    own_bot_user_id: Any = None,
    user_cache: Optional[UserCache] = None,
    is_bot_hint: Optional[bool] = None,
    event_type: str = "message",
    rendering_only: bool = False,
    cron_bot_ids: Optional[Iterable[Any]] = None,
) -> IngestDecision:
    """Whether this event is recorded.

    Humans are recorded. A bot is recorded only when the body has the cron
    marker and ``sender_id`` is in ``cron_bot_ids`` (or the allowlist is
    ``*``). Ids in that set are treated as bots even if the email does not
    look like one. ``rendering_only`` is never recorded. Content should
    already be plain text (see :func:`plain_content`); this function also
    strips HTML so a ``<p>`` footer still matches.
    """
    if rendering_only:
        return IngestDecision(False, "rendering_only")

    plain = plain_content(content)
    marker = parse_cron_marker(plain)
    allow = {str(item) for item in (cron_bot_ids or []) if str(item).strip()}
    wildcard = "*" in allow
    in_allowlist = (
        sender_id is not None and str(sender_id).strip() != "" and str(sender_id) in allow
    )
    is_bot = in_allowlist or sender_is_bot(
        sender_email=sender_email,
        sender_id=sender_id,
        own_bot_email=own_bot_email,
        own_bot_user_id=own_bot_user_id,
        user_cache=user_cache,
        is_bot_hint=is_bot_hint,
    )
    kind = (event_type or "message").strip().lower()
    if not is_bot:
        return IngestDecision(True, "human", marker)
    if marker is None:
        if (
            own_bot_user_id is not None
            and str(sender_id) == str(own_bot_user_id)
        ) or (
            own_bot_email
            and sender_email
            and sender_email.strip().lower() == own_bot_email.strip().lower()
        ):
            return IngestDecision(False, "own_bot_non_cron")
        return IngestDecision(False, "bot_without_cron_marker")
    if not in_allowlist and not wildcard:
        return IngestDecision(False, "bot_not_allowlisted", marker)
    if kind == "update_message":
        return IngestDecision(True, "bot_cron_marker_on_edit", marker)
    return IngestDecision(True, "bot_cron_marker", marker)


# ---------------------------------------------------------------------------
# Dedupe — one transcript row per Zulip message id
# ---------------------------------------------------------------------------


class IngestSeen:
    """Bounded set of message ids already written.

    The poll loop's ``ZulipDedupeStore.check`` inserts the id *before* the
    self-message branch, and a hit means "do not dispatch". Reusing that
    check for ingest would skip the first (and only) chance to record a
    cron echo. This set means "already written into the topic transcript".
    """

    def __init__(self, max_size: int = _SEEN_MAX):
        self.max_size = max(1, int(max_size))
        self._ids: OrderedDict[str, None] = OrderedDict()
        self._lock = threading.Lock()

    def __contains__(self, message_id: object) -> bool:
        with self._lock:
            return str(message_id) in self._ids

    def claim(self, message_id: str) -> bool:
        """True if this id was not yet ingested and is now reserved."""
        with self._lock:
            if message_id in self._ids:
                self._ids.move_to_end(message_id)
                return False
            self._ids[message_id] = None
            while len(self._ids) > self.max_size:
                self._ids.popitem(last=False)
            return True

    def __len__(self) -> int:
        with self._lock:
            return len(self._ids)


def seen_for(adapter: Any) -> IngestSeen:
    seen = getattr(adapter, "_topic_ingest_seen", None)
    if isinstance(seen, IngestSeen):
        return seen
    seen = IngestSeen()
    try:
        adapter._topic_ingest_seen = seen
    except Exception:
        logger.debug("zulip topic-ingest seen-set attach failed", exc_info=True)
    return seen


# ---------------------------------------------------------------------------
# Registry resolve + transcript write
# ---------------------------------------------------------------------------


@dataclass
class SharedTopicSource:
    """Store source with no participant, so the key cannot grow a user suffix."""

    chat_id: str
    thread_id: str
    chat_name: str = ""
    user_name: str = ""
    chat_type: str = "thread"
    user_id: Optional[str] = None
    platform: str = "zulip"


def _registry(adapter: Any) -> Any:
    routing = getattr(adapter, "_routing", None)
    if routing is None:
        return None
    return getattr(routing, "_conversations", None)


def resolve_conversation_id(
    adapter: Any,
    *,
    stream_id: Any,
    topic: str,
    message_id: Any = None,
    pre_resolved: Optional[str] = None,
) -> Optional[str]:
    """Conversation id from the stash or ``TopicConversationRegistry.resolve``.

    ``pre_resolved`` is the id the poll loop stashed (F1) and ``handle_message``
    popped. Re-resolving after a later rename would mint a new id under a
    freed name. When the stash is empty (self echoes are dropped before
    pre-resolve), ``resolve`` is the registry API and is idempotent for a
    live name.
    """
    if pre_resolved:
        return str(pre_resolved)
    registry = _registry(adapter)
    if registry is None:
        return None
    try:
        sid = int(stream_id)
    except (TypeError, ValueError):
        return None
    name = (topic or "").strip()
    if not name:
        return None
    anchor = None
    if message_id is not None and str(message_id).strip():
        try:
            anchor = int(message_id)
        except (TypeError, ValueError):
            anchor = None
    try:
        conversation_id = registry.resolve(sid, name, anchor_message_id=anchor)
    except Exception:
        logger.debug("zulip topic-ingest resolve failed", exc_info=True)
        return None
    return str(conversation_id) if conversation_id else None


def _user_cache(adapter: Any) -> Optional[UserCache]:
    cache = getattr(adapter, "_topic_user_cache", None)
    return cache if isinstance(cache, UserCache) else None


def _stream_name(message: Mapping[str, Any]) -> str:
    raw = message.get("display_recipient") or ""
    return raw if isinstance(raw, str) else ""


def _message_id(message: Mapping[str, Any]) -> Optional[str]:
    mid = message.get("id", message.get("message_id"))
    if mid is None or str(mid).strip() == "":
        return None
    return str(mid)


def build_observed_row(
    *,
    session_key: str,
    content: str,
    sender_name: str = "",
    sender_email: str = "",
    message_id: Any = None,
    cron_marker: Optional[CronMarker] = None,
    timestamp: Optional[str] = None,
) -> dict[str, Any]:
    label = (sender_name or "").strip() or (sender_email or "").strip() or "Unknown"
    body = strip_cron_marker(plain_content(content)).strip()
    if cron_marker is not None:
        text = f"[{label} | cron:{cron_marker.job_id} run:{cron_marker.run_id}]\n{body}"
    else:
        text = f"[{label}]\n{body}"
    row: dict[str, Any] = {
        "role": "user",
        "content": text,
        "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        "observed": True,
        "session_key": session_key,
    }
    if message_id is not None and str(message_id).strip() != "":
        row["message_id"] = str(message_id)
    if cron_marker is not None:
        row["cron_job_id"] = cron_marker.job_id
        row["cron_run_id"] = cron_marker.run_id
    return row


def _make_source(
    adapter: Any,
    *,
    stream_id: int,
    stream_name: str,
    conversation_id: str,
    sender_name: str,
) -> Any:
    kwargs = dict(
        chat_id=str(stream_id),
        chat_name=stream_name or str(stream_id),
        chat_type="thread",
        thread_id=str(conversation_id),
        # Empty participant: build_session_key adds no user suffix.
        user_id=None,
        user_name=sender_name or None,
    )
    build = getattr(adapter, "build_source", None)
    if callable(build):
        return build(**kwargs)
    return SharedTopicSource(
        chat_id=kwargs["chat_id"],
        thread_id=kwargs["thread_id"],
        chat_name=kwargs["chat_name"] or "",
        user_name=sender_name or "",
    )


def _write_row(adapter: Any, source: Any, row: Mapping[str, Any]) -> None:
    store = getattr(adapter, "_session_store", None)
    if store is None:
        logger.debug("zulip topic-ingest skipped write; no session store")
        return
    session_entry = store.get_or_create_session(source)
    store.append_to_transcript(session_entry.session_id, dict(row))


def _record(
    adapter: Any,
    *,
    stream_id: int,
    topic: str,
    content: str,
    sender_email: str,
    sender_name: str,
    message_id: Optional[str],
    decision: IngestDecision,
    pre_resolved: Optional[str],
    stream_name: str,
) -> None:
    conversation_id = resolve_conversation_id(
        adapter,
        stream_id=stream_id,
        topic=topic,
        message_id=message_id,
        pre_resolved=pre_resolved,
    )
    if not conversation_id:
        return
    # Claim only once a row will be written. A rejected or unresolvable id
    # must stay free so a later cron-marked edit of the same message can
    # still be recorded. Two callers that both pass the filter: one claim.
    if message_id and not seen_for(adapter).claim(message_id):
        return
    session_key = topic_session_key(adapter_profile(adapter), stream_id, conversation_id)
    # A human-pasted marker is not a cron delivery. The label stays the person.
    row = build_observed_row(
        session_key=session_key,
        content=content,
        sender_name=sender_name,
        sender_email=sender_email,
        message_id=message_id,
        cron_marker=None if decision.reason == "human" else decision.cron_marker,
    )
    source = _make_source(
        adapter,
        stream_id=stream_id,
        stream_name=stream_name,
        conversation_id=conversation_id,
        sender_name=sender_name,
    )
    _write_row(adapter, source, row)


def ingest_message(
    adapter: Any,
    message: Mapping[str, Any],
    *,
    pre_resolved: Optional[str] = None,
    event_type: str = "message",
) -> None:
    """The only ingest entry point for a ``message`` event.

    Called from the poll-loop self drop and from ``handle_message`` (self
    drop and the no-dispatch return). A message id is written at most once.
    Never raises into the caller.
    """
    try:
        install_shared_topic_sessions(adapter)
        msg_type = str(message.get("type") or "")
        if msg_type not in ("stream", "channel"):
            return
        stream_id = message.get("stream_id")
        topic = (message.get("subject") or message.get("topic") or "").strip()
        try:
            sid = int(stream_id)
        except (TypeError, ValueError):
            return
        if not topic:
            return
        content = str(message.get("content") or "")
        sender_email = str(message.get("sender_email") or "")
        sender_id = message.get("sender_id")
        sender_name = str(message.get("sender_full_name") or "").strip()
        decision = should_ingest(
            content=content,
            sender_email=sender_email,
            sender_id=sender_id,
            own_bot_email=str(getattr(adapter, "email", "") or ""),
            own_bot_user_id=getattr(adapter, "_bot_user_id", None),
            user_cache=_user_cache(adapter),
            is_bot_hint=_optional_bool(message.get("is_bot")),
            event_type=event_type,
            rendering_only=bool(message.get("rendering_only")),
            cron_bot_ids=cron_bot_allowlist(adapter),
        )
        if not decision.ingest:
            return
        # Dedupe: a cron-marked bot post is recorded ONLY by its owning
        # profile (the profile whose cron/jobs.json holds the job, else the
        # sender bot's profile). Every other adapter skips it entirely and
        # creates no session. A human-pasted marker (reason "human") is not a
        # cron delivery and keeps today's behaviour; an unknowable owner also
        # fails open rather than dropping a post nobody owns.
        if (
            decision.cron_marker is not None
            and decision.reason in ("bot_cron_marker", "bot_cron_marker_on_edit")
        ):
            owner = cron_ownership.resolve_cron_owner(
                adapter=adapter,
                job_id=decision.cron_marker.job_id,
                sender_id=sender_id,
                sender_email=sender_email,
            )
            me = cron_ownership.own_profile_name(adapter)
            if owner is not None and owner != me:
                logger.info(
                    "zulip ingest dedupe: skipping cron %s owned by %s",
                    decision.cron_marker.job_id,
                    owner,
                )
                return
        _record(
            adapter,
            stream_id=sid,
            topic=topic,
            content=content,
            sender_email=sender_email,
            sender_name=sender_name,
            message_id=_message_id(message),
            decision=decision,
            pre_resolved=pre_resolved,
            stream_name=_stream_name(message),
        )
    except Exception:
        logger.debug("zulip topic-ingest failed", exc_info=True)


def ingest_update_message(adapter: Any, event: Mapping[str, Any]) -> None:
    """Ingest a content edit that carries a cron marker.

    Topic renames are not handled here. The caller runs
    ``adapter._handle_topic_update`` first, so ``resolve`` on the new name
    returns the same conversation id (R2) or a new one (R3/R8).
    ``rendering_only`` edits are ignored. Never raises.
    """
    try:
        if event.get("rendering_only"):
            return
        content = event.get("content")
        if not isinstance(content, str) or not content:
            return
        stream_id = event.get("new_stream_id", event.get("stream_id"))
        topic = event.get("subject") or event.get("orig_subject") or ""
        sender_id = event.get("sender_id", event.get("user_id"))
        synthetic = {
            "type": "stream",
            "stream_id": stream_id,
            "subject": topic,
            "content": content,
            "sender_email": event.get("sender_email") or "",
            "sender_id": sender_id,
            "sender_full_name": event.get("sender_full_name") or "",
            "id": event.get("message_id"),
            "display_recipient": event.get("display_recipient") or "",
        }
        ingest_message(
            adapter,
            synthetic,
            event_type="update_message",
        )
    except Exception:
        logger.debug("zulip topic-ingest update failed", exc_info=True)


# ---------------------------------------------------------------------------
# Mentioned-topic backfill (dedupe companion)
# ---------------------------------------------------------------------------

#: Default number of recent topic messages fetched for a mentioned turn.
DEDUPE_BACKFILL_DEFAULT_MESSAGES = 30


def resolve_dedupe_backfill_messages() -> int:
    """``ZULIP_DEDUPE_BACKFILL_MESSAGES`` (default 30, 0 disables)."""
    raw = (cron_ownership.runtime_scope.get_setting("ZULIP_DEDUPE_BACKFILL_MESSAGES", "30") or "").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        logger.warning("ZULIP_DEDUPE_BACKFILL_MESSAGES=%r invalid; using 30", raw)
        return DEDUPE_BACKFILL_DEFAULT_MESSAGES


async def _fetch_topic_messages(
    adapter: Any, stream_name: str, topic: str, num_before: int
) -> list:
    """Newest ``num_before`` topic messages, best-effort (never raises)."""
    sdk_call = getattr(adapter, "_sdk_call", None)
    client = getattr(adapter, "client", None)
    if sdk_call is None or client is None or num_before <= 0:
        return []
    narrow = [{"operator": "stream", "operand": stream_name}]
    if topic:
        narrow.append({"operator": "topic", "operand": topic})
    try:
        result = await sdk_call(
            client.get_messages,
            {
                "anchor": "newest",
                "num_before": num_before,
                "num_after": 0,
                "narrow": narrow,
            },
            timeout=history.HISTORY_FETCH_TIMEOUT,
        )
    except Exception as exc:
        logger.debug("zulip dedupe backfill fetch failed: %s", exc)
        return []
    if not isinstance(result, dict) or result.get("result") != "success":
        return []
    messages = result.get("messages")
    return messages if isinstance(messages, list) else []


async def mentioned_topic_backfill(
    *,
    adapter: Any,
    stream_id: Any,
    topic: str,
    conversation_id: Any,
    stream_name: str,
) -> str:
    """Recent topic history for a mentioned turn that holds no local session.

    Ingest dedupe means a non-owner profile never recorded this topic's cron
    posts, so an @-mention here would otherwise meet an empty session. Fetch
    the last N messages from the Zulip API (N =
    ``ZULIP_DEDUPE_BACKFILL_MESSAGES``, default 30) and render them the same
    way harvested history is rendered. Returns "" when this adapter already
    holds the topic's session, when there is nothing to fetch, or when the
    fetch fails — never raises.
    """
    try:
        num = resolve_dedupe_backfill_messages()
        if num <= 0 or not conversation_id:
            return ""
        # We already hold this topic: the session is the context.
        store = getattr(adapter, "_session_store", None)
        if store is not None:
            try:
                key = topic_session_key(
                    cron_ownership.own_profile_name(adapter),
                    int(stream_id),
                    str(conversation_id),
                )
                lookup = getattr(store, "lookup_by_session_key", None)
                if callable(lookup) and lookup(key) is not None:
                    return ""
            except Exception:
                logger.debug(
                    "zulip dedupe backfill session check failed (fail open)",
                    exc_info=True,
                )
        messages = await _fetch_topic_messages(adapter, stream_name, topic, num)
        if not messages:
            return ""
        block = history.render_history_block(
            messages,
            max_messages=num,
            window_hours=24 * 30,
            max_chars=6000,
        )
        if not block.strip():
            return ""
        return f"{history.FETCHED_HISTORY_LABEL}\n{block}\n\n"
    except Exception:
        logger.debug("zulip dedupe backfill failed", exc_info=True)
        return ""
