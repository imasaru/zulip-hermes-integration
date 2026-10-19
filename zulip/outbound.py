"""Outbound content egress: render, guard, chunk, transmit.

Everything the plugin puts *into* Zulip lives here, so the four invariants a
reply must satisfy have one home:

* :func:`detect_secret_leak` / :func:`refuse_secret_leak` — a known host
  credential is refused before anything leaves the process (issue #136), and
  the refusal happens *before* media upload so a refused message cannot leave a
  stray upload behind.
* :func:`render_refs` — validated GitHub refs are rewritten before truncation
  and chunking, so a ``[[zulip_ref: …]]`` marker can never be split across two
  messages (issue #150). Best-effort: a rendering failure leaves the reply
  exactly as written.
* :func:`send` — the full reply pipeline in its one correct order: strip the
  model's inline reasoning block, run the secret guard, upload media, append the
  uploaded-file links, extract the topic directive, render refs, apply the hard
  length cap, then chunk. :func:`send_single` prepends the configured response
  prefix to each chunk as it is transmitted.
* :func:`typing_params_for_chat` / :func:`send_typing` / :func:`stop_typing` —
  the gateway's typing hooks, translated to ``set_typing_status`` params.

Layering: L3 — depends on L0–L2 (``settings``, ``text_utils``, ``secret_guard``,
``media``, ``refs``, ``zulip_client``, ``runtime_scope``, ``logger``) and the
host's ``gateway.platforms.base``.

The adapter is passed in as an opaque collaborator and its state (``client``,
``_sdk_call``, ``_send_timeout``, ``_audit_logger``, ``_routed_topic``,
``_topic_cache``, ``_response_prefix``, ...) is read at call time, so an
instance patch of any of those still takes effect. ``adapter`` is deliberately
never imported here — that would be a cycle, and the adapter's own methods are
thin delegates into this module.

``media.upload_file_to_zulip`` and ``refs.render_refs`` are reached through
their owning modules rather than imported as names, so patching
``zulip.media`` / ``zulip.refs`` keeps working.
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Optional, Sequence

from . import media
from . import refs
from . import runtime_scope
from . import settings
from .logger import format_zulip_log, mask_pii
from .secret_guard import (
    KnownSecret,
    block_secret_leaks_enabled,
    collect_known_secrets,
    describe_leaked_secrets,
    find_leaked_secrets,
)
from .text_utils import (
    chunk_text,
    extract_topic_directive,
    strip_think_blocks,
    truncate_text,
)
from .topic_ingest import append_cron_marker, parse_cron_marker
from .zulip_client import parse_target

logger = logging.getLogger(__name__)


try:  # Prefer the gateway's SendResult so adapters share one result type.
    from gateway.platforms.base import SendResult  # type: ignore
except Exception:  # pragma: no cover - offline / test import fallback
    from dataclasses import dataclass

    @dataclass
    class SendResult:  # type: ignore
        """Minimal SendResult mirroring gateway.platforms.base.SendResult.

        The plugin keeps this fallback so it can be imported and unit-tested
        without the full gateway on sys.path; at runtime the gateway's real
        class is used.
        """

        success: bool
        message_id: Optional[str] = None
        error: Optional[str] = None
        raw_response: Any = None
        retryable: bool = False
        retry_after: Optional[float] = None
        continuation_message_ids: tuple = ()
        error_kind: Optional[str] = None


# Margin below the realm edit limit: an edit made within this window of the
# deadline is allowed, past it refused — avoids racing the server clock.
EDIT_MARGIN_SECONDS = 10


def _now_epoch() -> float:
    """Current epoch seconds (imported lazily so tests can monkeypatch)."""
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).timestamp()


def metadata_topic(metadata: Any) -> str | None:
    """Outbound routing topic from gateway send metadata.

    The gateway core routes replies by ``thread_id`` — the session origin's
    topic (``gateway.platforms.base._thread_metadata_for_source``) — so a reply
    generated for one topic lands there even if other topics have seen traffic
    since. ``topic`` is read too for parity with the adapter's own event
    metadata. First non-empty value wins; None when neither is set.
    """
    if not isinstance(metadata, dict):
        return None
    for key in ("thread_id", "topic"):
        value = metadata.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def safe_delete_temp_file(file_path: str) -> None:
    """Delete a local file only if it resides under /tmp or a bot workspace.

    Prevents accidental deletion of user-owned files outside temp dirs.
    Uses stat with follow_symlinks=False to prevent TOCTOU symlink swaps.
    Errors are logged, not raised.
    """
    try:
        p = Path(file_path).resolve()
        tmp = Path(tempfile.gettempdir()).resolve()
        ws = tmp / "hermes_bot_workspace"
        if not (str(p).startswith(str(tmp)) or str(p).startswith(str(ws))):
            return
        # Atomic stat + unlink to prevent TOCTOU symlink race
        st = p.stat(follow_symlinks=False)
        if st.st_ino != p.resolve().stat().st_ino:
            logger.warning(
                "temp file cleanup skipped: symlink detected [path=%s]",
                mask_pii(file_path),
            )
            return
        p.unlink()
        logger.debug("cleaned up temp file [path=%s]", mask_pii(file_path))
    except OSError as e:
        logger.warning("temp file cleanup failed [path=%s]: %s", mask_pii(file_path), e)


# ── Outbound secret guard (issue #136) ──────────────────────────────────────


def known_secrets(adapter: Any) -> list[KnownSecret]:
    """Credential values that must never be transmitted (Issue #136).

    Memoised on the adapter: the credential set does not change at runtime, and
    re-walking the config and environment on every send would be wasted work.
    """
    if adapter._known_secrets_cache is None:
        adapter._known_secrets_cache = collect_known_secrets(
            adapter._platform_extra,
            extra=[("zulip.api_key", adapter.api_key)],
            # Read at call time, not import time: the environment is the
            # live mapping, so a test that sets a credential sees it.
            env=os.environ,
        )
    return adapter._known_secrets_cache


def detect_secret_leak(adapter: Any, content: str) -> list[KnownSecret]:
    """Credentials ``content`` would transmit, if the guard is enabled."""
    if not block_secret_leaks_enabled():
        return []
    return find_leaked_secrets(content, known_secrets(adapter))


async def refuse_secret_leak(
    adapter: Any, hits: list[KnownSecret], chat_id: str
) -> str:
    """Audit a refused send, naming only *where* the credential came from.

    The value itself is never logged or audited: a message describing a
    leak must not become one. (Issue #136)
    """
    summary = describe_leaked_secrets(hits)
    logger.error(
        format_zulip_log(
            "zulip outbound blocked: message contained host credentials",
            chat_id=mask_pii(str(chat_id)),
            leaked=summary,
        )
    )
    await adapter._audit_logger.log_event(
        "secret_leak_blocked",
        {
            "chat_id": mask_pii(str(chat_id)),
            "direction": "outbound",
            "sources": [hit.name for hit in hits],
            "count": len(hits),
        },
    )
    await adapter._audit_logger.log_deliver_skipped(
        "secret_leak_blocked", chat_id=chat_id
    )
    return summary


async def render_refs(text: str) -> str:
    """Rewrite validated GitHub refs before chunking (#150).

    Handles both ``[[zulip_ref: …]]`` markers and bare ``github.com``
    pull/issue/commit/run URLs written in prose. Best-effort: rendering
    must never fail a send, and must never make a reply worse -- an
    unconfirmed bare URL is left exactly as written.
    """
    try:
        return await refs.render_refs(text)
    except Exception as e:
        logger.warning(
            "zulip ref rendering failed, sending as-is: %s", mask_pii(str(e))
        )
        return text


# ── Typing indicators ───────────────────────────────────────────────────────


def typing_params_for_chat(
    adapter: Any, chat_id: str, op: str, topic: str | None = None
) -> dict | None:
    """Map a gateway chat_id to Zulip set_typing_status params.

    DM session rotation can suffix chat ids ("dm:<id>:session:<n>"), and a
    group DM carries every recipient ("dm:<id>,<id>"); both are stripped to
    the recipient list. Streams are numeric stream ids;
    ``topic`` (the routing metadata's thread_id) wins over the per-stream
    reply cache, so typing shows in the session's own topic.
    """
    if not chat_id:
        return None
    parts = chat_id.split(":")
    if parts[0] == "dm":
        if len(parts) < 2:
            return None
        try:
            user_ids = [
                int(part) for part in parts[1].split(",") if part.strip()
            ]
        except ValueError:
            return None
        if not user_ids:
            return None
        return {"op": op, "type": "direct", "to": user_ids}
    if chat_id.isdigit():
        resolved = (topic or "").strip() or adapter._routing._topic_cache.get(chat_id, "")
        return {
            "op": op,
            "type": "stream",
            "stream_id": int(chat_id),
            "topic": resolved,
        }
    return None


async def send_typing(adapter: Any, chat_id: str, metadata: Any = None) -> None:
    """Core typing hook: the gateway calls this every ~2s while the agent
    runs (platform typing state expires after ~5s). Best-effort."""
    try:
        params = typing_params_for_chat(
            adapter,
            str(chat_id),
            "start",
            topic=adapter._routed_topic_for_chat(str(chat_id), metadata),
        )
        if params:
            await adapter._sdk_call(
                adapter.client.set_typing_status,
                params,
                timeout=adapter._send_timeout,
            )
    except Exception:
        pass  # typing is best-effort


async def stop_typing(adapter: Any, chat_id: str, metadata: Any = None) -> None:
    """Core typing hook: called when the agent run finishes. Best-effort.

    Accepts ``metadata`` so the gateway base's introspecting
    ``_stop_typing_with_metadata`` forwards the run's routing metadata
    (a thread-scoped clear must not depend on the last-seen topic).
    """
    try:
        params = typing_params_for_chat(
            adapter,
            str(chat_id),
            "stop",
            topic=adapter._routed_topic_for_chat(str(chat_id), metadata),
        )
        if params:
            await adapter._sdk_call(
                adapter.client.set_typing_status,
                params,
                timeout=adapter._send_timeout,
            )
    except Exception:
        pass  # typing is best-effort


async def stop_typing_with_params(adapter: Any, typing_params: dict | None) -> None:
    """Stop typing indicator if it was started. Safe to call multiple times."""
    if typing_params is None:
        return
    params = dict(typing_params)
    params["op"] = "stop"
    try:
        await adapter._sdk_call(
            adapter.client.set_typing_status,
            params,
            timeout=adapter._send_timeout,
        )
    except Exception:
        pass


# ── Media egress ────────────────────────────────────────────────────────────


async def _send_uploaded_media(
    adapter: Any,
    *,
    chat_id: str,
    file_path: str,
    caption: str | None,
    reply_to: Any,
    metadata: Any,
    as_image: bool,
) -> SendResult:
    data_dir = runtime_scope.get_profile_data_dir()
    try:
        url = await media.upload_file_to_zulip(
            adapter.client, file_path, data_dir, account_id=adapter.email
        )
    except Exception as e:
        logger.error(
            "[%s] native media upload failed [file=%s]: %s",
            adapter.name, mask_pii(file_path), e,
        )
        text = "⚠️ Couldn't deliver the image attachment." if as_image else "⚠️ Couldn't deliver the file attachment."
        if caption:
            text = f"{caption}\n{text}"
        return await send(adapter, chat_id=chat_id, content=text, reply_to=reply_to, metadata=metadata)

    safe_delete_temp_file(file_path)

    name = Path(file_path).name
    # `![alt](url)` renders inline in Zulip; plain `[name](url)` is a
    # downloadable link — same distinction the platform's own compose
    # box makes for drag-and-drop uploads.
    link = f"![{name}]({url})" if as_image else f"[{name}]({url})"
    content = f"{caption}\n{link}" if caption else link
    return await send(adapter, chat_id=chat_id, content=content, reply_to=reply_to, metadata=metadata)


async def send_image_file(
    adapter: Any,
    chat_id: str,
    image_path: str,
    caption: str | None = None,
    reply_to: str | None = None,
    metadata: dict | None = None,
    **kwargs: Any,
) -> SendResult:
    """Send a local image as a native Zulip attachment (overrides the
    base class's "unavailable" stub — MEDIA:<path> screenshots were
    silently failing to deliver on Zulip; see #123).

    Zulip has no separate "photo" primitive: an uploaded file becomes an
    inline image automatically when its URL is embedded in message
    markdown (``![name](url)``), so this just uploads via
    ``media.upload_file_to_zulip()`` and sends that markdown as the body.
    """
    return await _send_uploaded_media(
        adapter,
        chat_id=chat_id,
        file_path=image_path,
        caption=caption,
        reply_to=reply_to,
        metadata=metadata,
        as_image=True,
    )


async def send_document(
    adapter: Any,
    chat_id: str,
    file_path: str,
    caption: str | None = None,
    file_name: str | None = None,
    reply_to: str | None = None,
    metadata: dict | None = None,
    **kwargs: Any,
) -> SendResult:
    """Send a local file as a native Zulip attachment (overrides the
    base class's "unavailable" stub)."""
    return await _send_uploaded_media(
        adapter,
        chat_id=chat_id,
        file_path=file_path,
        caption=caption,
        reply_to=reply_to,
        metadata=metadata,
        as_image=False,
    )


# ── Send path ───────────────────────────────────────────────────────────────


async def send(
    adapter: Any,
    chat_id: str,
    content: str,
    reply_to: Any = None,
    metadata: Any = None,
    media_files: Sequence[str] | None = None,
    counts_as_reply: bool = True,
) -> SendResult:
    """Send message to a Zulip stream or DM, with chunking, topic directives, and files.

    ``counts_as_reply=False`` is for a message posted on a run's behalf that is
    not that run's answer — the silent-run notice (#219). It routes, chunks and
    secret-checks like any other send, but must not record the work item as
    having replied, or the silent run it is reporting on would stop looking
    silent (the trace would say "replied", and ``deliver_empty`` would be
    contradicted by a ``deliver_payload``).
    """
    metadata = metadata or {}
    media_files = media_files or []

    # Reasoning hygiene (Issue #152): a model that emits its scratchpad
    # inline must never ship that block to Zulip. Applied before the secret
    # guard so a credential inside a block is dropped, not refused.
    content = strip_think_blocks(content)

    # Outbound secret guard (Issue #136). Checked before media upload, so a
    # message we are about to refuse cannot leave a stray upload behind.
    leaked = detect_secret_leak(adapter, content)
    if leaked:
        adapter._note_delivery_attempt(chat_id, metadata)
        summary = await refuse_secret_leak(adapter, leaked, chat_id)
        logger.error(
            "zulip send refused: %s. Remove it and rotate the credential.",
            summary,
        )
        return SendResult(success=False, message_id="")

    # Upload files first
    uploaded_urls = []
    uploaded_local_paths = []
    if media_files:
        data_dir = runtime_scope.get_profile_data_dir()
        for file_path in media_files:
            # Security: reject URL-like values in media_files (must be local paths)
            if isinstance(file_path, str) and (file_path.startswith("http://") or file_path.startswith("https://")):
                logger.warning(
                    "zulip send rejected URL in media_files [url=%s]",
                    mask_pii(file_path),
                )
                continue
            try:
                url = await media.upload_file_to_zulip(
                    adapter.client, file_path, data_dir, account_id=adapter.email
                )
                uploaded_urls.append(url)
                uploaded_local_paths.append(file_path)
            except Exception as e:
                logger.error("zulip upload failed [file=%s]: %s", mask_pii(file_path), e)

    # Clean up local temp files after upload (best-effort)
    for local_path in uploaded_local_paths:
        safe_delete_temp_file(local_path)

    # Append uploaded file links to content
    if uploaded_urls:
        file_links = "\n".join(f"[{Path(u).name}]({u})" for u in uploaded_urls)
        if content:
            content = f"{content}\n\n{file_links}"
        else:
            content = file_links

    # Extract inline topic directive if present
    content, topic_override = extract_topic_directive(content)

    # Rewrite actionable refs before truncation and chunking, so a marker
    # can never be split across two messages (Issue #150).
    content = await render_refs(content)

    # Hard cap before chunking (mirrors sibling plugin's maxMessageLength).
    max_length = settings.resolve_max_message_length()
    if max_length > 0:
        content = truncate_text(content, max_length)

    limit, mode = settings.resolve_chunk_config()
    chunks = chunk_text(content, limit=limit, mode=mode)

    if not chunks:
        chunks = [""]

    last_result: SendResult | None = None

    # When block streaming is enabled, send each chunk as a separate message
    # immediately. This requires gateway-level support (not yet implemented).
    for idx, chunk in enumerate(chunks):
        result = await send_single(
            adapter, chat_id, chunk, metadata, topic_override,
            counts_as_reply=counts_as_reply,
        )
        last_result = result
        if not result.success:
            logger.error(
                "zulip send failed on chunk %d/%d [chat=%s]",
                idx + 1,
                len(chunks),
                mask_pii(chat_id),
            )

    return last_result or SendResult(success=False, message_id="")


async def send_single(
    adapter: Any,
    chat_id: str,
    content: str,
    metadata: dict,
    topic_override: str | None,
    counts_as_reply: bool = True,
) -> SendResult:
    """Send a single (unchunked) message."""
    # Prepend response prefix if configured (Issue #65)
    if adapter._response_prefix and content:
        content = adapter._response_prefix + content

    # This work item produced something to deliver, so a run that does not
    # send here must not also be reported as "produced nothing" (#145). A notice
    # sent on behalf of a run that produced *nothing* is not a reply, so it does
    # not mark one (#219).
    if counts_as_reply:
        adapter._note_delivery_attempt(chat_id, metadata)
    audit_topic = metadata_topic(metadata)

    try:
        target = parse_target(chat_id)
        if target["type"] == "dm":
            audit_topic = None
            result = await adapter._sdk_call(
                adapter.client.send_message,
                {
                    "type": "private",
                    "to": target["user_ids"],
                    "content": content,
                },
                timeout=adapter._send_timeout,
            )
        else:
            stream_id = target["stream_id"]
            # Fork: explicit override > topic in the "<stream>:<topic>" address
            # > routed metadata > cache.
            topic = (
                topic_override
                or target.get("topic")
                or adapter._routed_topic(stream_id, metadata)
            )
            if not topic:
                topic = adapter._routing._topic_cache.get(chat_id, "general")
            audit_topic = topic
            # Gateway send metadata may lack the topic name that dispatch keyed
            # the audit turn on; mark that key too or the silent-run notice false-fires.
            if counts_as_reply and audit_topic:
                adapter._note_delivery_attempt(chat_id, {"topic": audit_topic})


            result = await adapter._sdk_call(
                adapter.client.send_message,
                {
                    "type": "stream",
                    "to": stream_id,
                    "topic": topic,
                    "content": content,
                },
                timeout=adapter._send_timeout,
            )

        if result.get("result") == "success":
            logger.debug("zulip message sent to %s", chat_id)
            message_id = str(result.get("id", ""))
            # Remember when this message was sent, keyed by its Zulip id, so a
            # later edit_message can recover the original send time when the
            # gateway does not pass metadata["original_sent_ts"]. Bounded to the
            # most recent 2000 ids (oldest dropped first).
            if message_id:
                try:
                    sent = adapter._sent_ts
                    sent[int(message_id)] = _now_epoch()
                    if len(sent) > 2000:
                        # Drop the oldest entries (smallest timestamp first).
                        for _ in range(len(sent) - 2000):
                            oldest = min(sent, key=sent.get)
                            del sent[oldest]
                except Exception:
                    pass
            # This work item produced a reply, so its trace can say so
            # instead of reporting a silent run (epic #139 / #158).
            if counts_as_reply:
                adapter._note_trace_reply(chat_id, metadata)
                await adapter._audit_logger.log_deliver_payload(
                    chat_id=chat_id, topic=audit_topic, message_id=message_id
                )
            return SendResult(success=True, message_id=message_id)
        else:
            logger.error(
                format_zulip_log(
                    "zulip send failed",
                    chat_id=mask_pii(chat_id),
                    error=mask_pii(str(result)),
                )
            )
            await adapter._audit_logger.log_deliver_failed(
                "api_error", chat_id=chat_id, topic=audit_topic
            )
            return SendResult(success=False, message_id="")

    except Exception as e:
        logger.error(
            format_zulip_log(
                "zulip send error",
                chat_id=mask_pii(chat_id),
                error=mask_pii(str(e)),
            )
        )
        await adapter._audit_logger.log_deliver_failed(
            "send_exception", chat_id=chat_id, topic=audit_topic
        )
        return SendResult(success=False, message_id="")


# ── Edit path ───────────────────────────────────────────────────────────────


def _edit_audit_topic(chat_id: str, metadata: Any) -> str | None:
    """Audit topic for an edit — mirrors :func:`send_single`'s stream topic.

    DMs audit on no topic; streams audit on the routed topic (metadata wins,
    then the parsed chat address). Best-effort: an unparseable chat_id just
    audits on whatever metadata carries.
    """
    topic = metadata_topic(metadata)
    if topic:
        return topic
    try:
        target = parse_target(str(chat_id))
        if target["type"] == "dm":
            return None
    except Exception:
        return None
    return topic


def _edit_content_limit_seconds(adapter: Any) -> Optional[int]:
    """Realm ``realm_message_content_edit_limit_seconds`` for the bot.

    Cached on the adapter after the first call; ``None`` means "unknown"
    (treated as unlimited — the edit is allowed and the API rejects an
    over-limit edit, which the caller turns into a failed result).
    """
    cached = getattr(adapter, "_edit_limit_seconds_cache", None)
    if cached is not None:
        return cached
    limit = None
    register = getattr(adapter, "_register_response", None)
    if isinstance(register, dict):
        raw = register.get("realm_message_content_edit_limit_seconds")
        if raw is not None:
            try:
                limit = int(raw)
            except (TypeError, ValueError):
                limit = None
    if limit is None:
        try:
            settings_result = getattr(adapter.client, "get_server_settings", None)
            if callable(settings_result):
                raw = adapter._sdk_call(
                    settings_result, {}, timeout=adapter._send_timeout
                )
                if isinstance(raw, dict):
                    val = raw.get("realm_message_content_edit_limit_seconds")
                    if val is not None:
                        limit = int(val)
        except Exception:
            # Best-effort: a failed fetch leaves ``limit`` None (unlimited).
            pass
    adapter._edit_limit_seconds_cache = limit
    return limit


async def edit_message(
    adapter: Any,
    chat_id: str,
    message_id: str,
    content: str,
    metadata: Any = None,
    *,
    finalize: bool = False,
) -> SendResult:
    """Edit a previously sent Zulip message in place.

    Reuses :func:`send`'s content pipeline — reasoning strip, secret guard,
    ref render, hard cap, chunk — so the edit looks like a normal reply.

    Returns a *failed* result (``success=False``) so the gateway falls back to
    its normal non-edit path whenever editing is not possible:

    * the content exceeds the chunk limit — editing only the first chunk is
      not a faithful edit, so the whole edit is refused;
    * the original message is older than the realm edit-time limit minus a
      10s margin (the gateway does not pass the original's timestamp, so an
      unknown limit lets the API decide);
    * the Zulip API rejects the edit (edit window closed, permission denied,
      rate limit, …) — logged as a warning, never raised.

    A cron marker carried by the original message or the new content is kept
    on the edited body, and the edit is counted as a delivery attempt.
    """
    metadata = metadata or {}

    # ── Content pipeline (same order as send) ──────────────────────────
    content = strip_think_blocks(content)

    leaked = detect_secret_leak(adapter, content)
    if leaked:
        adapter._note_delivery_attempt(chat_id, metadata)
        summary = await refuse_secret_leak(adapter, leaked, chat_id)
        logger.error(
            "zulip edit refused: %s. Remove it and rotate the credential.",
            summary,
        )
        return SendResult(success=False, message_id=message_id)

    content = await render_refs(content)

    max_length = settings.resolve_max_message_length()
    if max_length > 0:
        content = truncate_text(content, max_length)

    limit, mode = settings.resolve_chunk_config()
    chunks = chunk_text(content, limit=limit, mode=mode)
    if not chunks:
        chunks = [""]

    # Editing must reproduce the original message, not just its first chunk.
    # A partial edit is worse than no edit, so refuse rather than send a
    # truncated body.
    if len(chunks) > 1:
        logger.warning(
            "zulip edit skipped: content %d chunks exceeds limit %d [msg=%s]",
            len(chunks), limit, mask_pii(str(message_id)),
        )
        adapter._note_delivery_attempt(chat_id, metadata)
        return SendResult(
            success=False,
            message_id=message_id,
            error="edit_content_too_long",
        )

    edited = chunks[0] if chunks else ""

    # ── Cron marker preservation ──────────────────────────────────────
    # A cron-marked final edit must remain marked so the poll loop ingests
    # it (at most once — the message id is deduped by IngestSeen).
    job_id = metadata.get("cron_job_id") if metadata else None
    run_id = metadata.get("cron_run_id") if metadata else None
    if not isinstance(job_id, str) or not isinstance(run_id, str):
        job_id = run_id = None
    marker = parse_cron_marker(edited)
    if marker and (job_id is None or run_id is None):
        job_id = job_id or marker.job_id
        run_id = run_id or marker.run_id
    if job_id and run_id:
        edited = append_cron_marker(edited, job_id, run_id)

    # ── Edit-time limit ───────────────────────────────────────────────
    # The gateway does not hand us the original message's timestamp, so we
    # cannot know its age here. Respect the limit only when we can recover
    # the send time: the caller may pass ``metadata["original_sent_ts"]``
    # (epoch seconds), or we fall back to the time we recorded when the
    # message was last sent (``adapter._sent_ts``). An unknown limit defers
    # to the API.
    original_ts = None
    if metadata:
        raw_ts = metadata.get("original_sent_ts")
        if raw_ts is not None:
            try:
                original_ts = float(raw_ts)
            except (TypeError, ValueError):
                original_ts = None
    if original_ts is None:
        try:
            original_ts = float(getattr(adapter, "_sent_ts", {}).get(int(message_id)))
        except (TypeError, ValueError, KeyError):
            original_ts = None
    if original_ts is not None:
        limit_secs = _edit_content_limit_seconds(adapter)
        if limit_secs is not None:
            age = _now_epoch() - original_ts
            if age > limit_secs - EDIT_MARGIN_SECONDS:
                logger.warning(
                    "zulip edit skipped: message older than edit limit "
                    "(age=%.0fs limit=%ds) [msg=%s]",
                    age, limit_secs, mask_pii(str(message_id)),
                )
                adapter._note_delivery_attempt(chat_id, metadata)
                return SendResult(
                    success=False,
                    message_id=message_id,
                    error="edit_time_limit_exceeded",
                )

    # ── Transmit ──────────────────────────────────────────────────────
    adapter._note_delivery_attempt(chat_id, metadata)
    audit_topic = _edit_audit_topic(chat_id, metadata)
    try:
        result = await adapter._sdk_call(
            adapter.client.update_message,
            {"message_id": int(message_id), "content": edited},
            timeout=adapter._send_timeout,
        )
    except Exception as e:
        logger.warning(
            format_zulip_log(
                "zulip edit failed",
                message_id=mask_pii(str(message_id)),
                error=mask_pii(str(e)),
            )
        )
        await adapter._audit_logger.log_deliver_failed(
            "edit_exception", chat_id=chat_id, topic=audit_topic
        )
        return SendResult(success=False, message_id=message_id, error=str(e))

    if isinstance(result, dict) and result.get("result") == "success":
        logger.debug("zulip message edited to %s", chat_id)
        return SendResult(success=True, message_id=message_id)

    # "The time limit for editing this message has passed", permission
    # denied, rate limit, … — log and let the gateway fall back.
    logger.warning(
        format_zulip_log(
            "zulip edit failed",
            message_id=mask_pii(str(message_id)),
            error=mask_pii(str(result)),
        )
    )
    await adapter._audit_logger.log_deliver_failed(
        "edit_error", chat_id=chat_id, topic=audit_topic
    )
    return SendResult(success=False, message_id=message_id, error=str(result))
