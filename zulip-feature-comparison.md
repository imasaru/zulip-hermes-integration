# LINE Gateway vs Zulip Plugin — Feature Comparison

**Date:** 2026-08-28
**Sources:**
- LINE: `/usr/local/lib/hermes-agent/plugins/platforms/line/adapter.py` (2,549 lines) + `plugin.yaml`
- Zulip: `/mnt/projects/zulip-hermes-integration/zulip/` (14 modules)

---

## Feature Matrix

| # | Feature | LINE | Zulip | Gap Severity | Porting Notes |
|---|---------|------|-------|-------------|---------------|
| 1 | **Typing indicators** | ✅ DM-only loading animation (`client.loading()`) | ❌ Missing | **High** | Zulip has no native typing indicator API. Would need to simulate via ephemeral status or a "thinking…" reaction. LINE uses `client.loading(chat_id)` — Zulip would need a different mechanism since Zulip doesn't support real-time typing. |
| 2 | **Mark as read** | ✅ `client.mark_read(mark_as_read_token)` per message | ❌ Missing | **Medium** | Zulip has no read-receipt API. No porting path — not applicable. |
| 3 | **Postback buttons (slow-LLM UX)** | ✅ Slow-response postback button with cached payload; user taps to fetch result | ❌ Missing | **High** | Zulip has no inline button API. Would need a workaround: post a "click here for result" link or use a Zulip message flag/emoji reaction as a trigger. The entire postback cache (`_cache`, `_pending_buttons`, `State` enum) would need a Zulip-equivalent. |
| 4 | **Media download (inbound)** | ✅ image, audio, video, file via `client.fetch_content()` | ✅ Partial — `media.py` handles uploads only | **High** | LINE downloads inbound media to local cache for vision processing. Zulip has no inbound media download — only outbound upload via `media.py`. Zulip needs a `download_media(message_id)` equivalent using Zulip's `/api/v1/messages/{id}/attachments` endpoint. |
| 5 | **Media serving (outbound)** | ✅ Built-in HTTPS endpoint (`/media/{token}/{filename}`) for serving local files to LINE | ❌ Missing | **High** | LINE serves cached files via its own aiohttp server with token-based auth and TTL expiry. Zulip uses `/api/v1/user_uploads` for uploads instead — different model. Would need a similar tokenized serving endpoint or switch to Zulip's upload API fully. |
| 6 | **Display name resolution** | ✅ Group member profile API + personal profile API, cached in-memory and persisted to disk | ❌ Missing | **Medium** | Zulip has `/api/v1/users` for user lookup. Would need a similar cache layer (like LINE's `_display_names` + `_save_line_contacts()`) using Zulip's user API. Zulip's `text_utils.py` handles mention normalization but not display name resolution. |
| 7 | **Bridged display names** | ✅ First-time: `"Name|Uxxxx"`, subsequent: `"Name"` — for clean attribution in transcripts | ❌ Missing | **Medium** | LINE embeds bridged names directly in message content (`[Name|Uxxxx]\n<message>`). Zulip would need to inject similar attribution into session transcripts. |
| 8 | **Group message observation** | ✅ Non-addressed group messages stored in session transcript for context without triggering LLM | ❌ Missing | **High** | Zulip has no equivalent to `_observe_group_message()`. Would need to detect non-bot messages in streams and append them to session transcripts without dispatching. |
| 9 | **Soft gate / hard gate** | ✅ `soft_gate` mode: always dispatch to LLM but mark `addressed=False`; hard gate: only dispatch on mention/keyword | ❌ Missing | **High** | Zulip's `policy.py` handles access control but not dispatch gating. Would need a gate mode in the adapter's message handler. |
| 10 | **Continuation window** | ✅ `_is_in_continuation_window()` — allows responses without explicit mention within a time window | ❌ Missing | **Medium** | Zulip would need a per-stream conversation window tracker (chat_id → last_addressed_time). |
| 11 | **Session title seeding** | ✅ Seeds group/room session titles from chat names; migrates old raw-ID titles | ❌ Missing | **Medium** | Zulip adapter would need to call `store.set_auto_title_if_empty()` with stream names. |
| 12 | **Webhook signature verification** | ✅ `verify_line_signature()` — HMAC verification of X-Line-Signature header | ❌ Partial — Zulip has its own webhook secret | **Low** | Zulip adapter should verify webhook secrets from Zulip server. LINE's implementation is a good reference for the pattern. |
| 13 | **Body size limits** | ✅ `WEBHOOK_BODY_MAX_BYTES` cap on webhook payload | ❌ Partial | **Low** | Zulip adapter should add similar body size limits on incoming webhook requests. |
| 14 | **Self-message filtering** | ✅ Filters events where `source.userId == bot_user_id` | ❌ Partial | **Medium** | Zulip adapter should verify sender is not the bot itself. |
| 15 | **Connection lifecycle management** | ✅ `connect()`/`disconnect()` with scoped lock, health probe, media cleanup | ❌ Partial | **Medium** | Zulip adapter has basic connect logic but lacks the structured lifecycle with token locks and cleanup. |
| 16 | **Persistent contact/name caching** | ✅ Disk-persisted `line_contacts.json` with atomic writes, merge-on-load | ❌ Missing | **Medium** | Zulip would need a similar contacts cache for user display names. |
| 17 | **Timezone-aware timestamps** | ✅ Configurable timezone (default JST) for formatted timestamps in messages | ❌ Missing | **Low** | Zulip adapter would need to apply timezone formatting to message timestamps. |
| 18 | **Sticker message support** | ✅ Parses sticker keywords into `[sticker: ...]` text | ❌ Missing | **Low** | Zulip has no sticker concept. Could map to emoji reactions. |
| 19 | **Location message support** | ✅ Parses location with title + address | ❌ Missing | **Low** | Zulip has no native location messages. |
| 20 | **Postback event handling** | ✅ Handles slow-LLM postback taps with state machine (READY/ERROR/PENDING/DELIVERED) | ❌ Missing | **High** | See #3 — Zulip has no postback mechanism. Would need an alternative trigger. |
| 21 | **Interrupt session activity** | ✅ Cancels pending postbacks on session interrupt | ❌ Missing | **Medium** | Zulip adapter needs to clean up any pending state when sessions are interrupted. |
| 22 | **System busy-ack bypass** | ✅ System messages (interrupting/queued/steered) bypass postback cache | ❌ Missing | **Medium** | Zulip would need to route system messages directly to the platform send method. |
| 23 | **Reply token management** | ✅ Stashes reply tokens with TTL, consumes on send, falls back to push | ❌ Missing | **High** | Zulip uses message threading (`thread` field) instead of reply tokens. Would need a Zulip equivalent. |
| 24 | **Message chunking (outbound)** | ✅ `split_for_line()` — splits long responses for LINE's per-message limit | ✅ `text_utils.chunk_text()` — similar functionality | ✅ Present | Zulip already has this in `text_utils.py` with length, newline, and markdown modes. |
| 25 | **Markdown formatting** | ✅ `strip_markdown_preserving_urls()` — LINE-specific rendering | ✅ `text_utils.py` — HTML-to-text, mention normalization, table conversion | ✅ Present | Zulip has more comprehensive text utilities. |
| 26 | **Deduplication** | ✅ Webhook event ID dedup (`_dedup.is_duplicate()`) | ✅ `dedupe_store.py` — message ID dedup | ✅ Present | Both have dedup; different strategies (event ID vs message ID). |
| 27 | **Rate limiting** | ✅ Built into base adapter | ✅ `rate_limiter.py` — dedicated module | ✅ Present | |
| 28 | **Policy engine** | ✅ Allowlist (user IDs, group IDs, room IDs) | ✅ `policy.py` — DM policy (open/allowlist/pairing/disabled) + group policy | ✅ Present | Zulip's policy engine is more sophisticated (pairing codes, disk persistence). |
| 29 | **Command handling** | ❌ Minimal inline handling | ✅ `commands.py` — dedicated command router | ✅ Present | |
| 30 | **Reactions for status** | ❌ Not used | ✅ `reactions.py` — 👀 start / ✅ success / ⚠️ error reactions | ✅ Present | |
| 31 | **Recovery of interrupted messages** | ❌ Not needed (webhook-based, stateless) | ✅ `recovery.py` — scans for stale reactions and re-dispatches | ✅ Present | |
| 32 | **Health probe** | ✅ `/health` endpoint on webhook server | ✅ `probe.py` — connectivity check | ✅ Present | |
| 33 | **Audit logging** | ❌ Not present | ✅ `audit_logger.py` — JSON-line rotating log | ✅ Present | |
| 34 | **Queue manager** | ❌ Not present | ✅ `queue_manager.py` — message batching | ✅ Present | |
| 35 | **Self-updater** | ❌ Not present | ✅ `updater.py` — GitHub-based plugin updates with checksum verification | ✅ Present | |
| 36 | **Bot workspace** | ❌ Not present | ✅ `workspace.py` — sandboxed file generation with TTL cleanup | ✅ Present | |
| 37 | **Logger utilities** | ❌ Not present | ✅ `logger.py` — PII masking, structured logging | ✅ Present | |

---

## Summary: Critical Gaps (High Severity)

| Gap | Description | Effort |
|-----|-------------|--------|
| **Typing indicators** | No way to show "bot is thinking" to users | Medium — requires Zulip workaround (reaction-based or status) |
| **Postback buttons** | No slow-LLM UX — users get no feedback during long waits | High — requires alternative trigger mechanism |
| **Inbound media download** | Can't process images/audio/video sent to bot | High — needs Zulip attachment download API |
| **Outbound media serving** | Can't serve cached files directly to users | High — needs tokenized serving endpoint or full upload API integration |
| **Group message observation** | Non-addressed stream messages not visible to bot context | High — needs stream message listener |
| **Soft/hard gate** | No dispatch gating for group streams | High — needs gate mode in message handler |

## Summary: Medium Gaps

| Gap | Description | Effort |
|-----|-------------|--------|
| **Display name resolution** | User names not resolved for attribution | Medium — needs Zulip user API + cache |
| **Bridged display names** | No first-time/subsequent name formatting | Medium — formatting layer |
| **Continuation window** | No context-aware follow-up without mention | Medium — needs time-window tracker |
| **Session title seeding** | Stream sessions not named after streams | Medium — needs session store integration |
| **Connection lifecycle** | Basic connect/disconnect, no structured lifecycle | Medium — needs lock/cleanup pattern |
| **Contact caching** | No persistent name cache | Medium — needs disk-persisted cache |
| **Interrupt session cleanup** | No cleanup of pending state on interrupt | Medium — needs interrupt handler |
| **System message bypass** | System messages may not route correctly | Medium — needs routing override |
| **Reply token equivalent** | No Zulip-thread equivalent for reply management | High — needs thread-aware send logic |

## Summary: Low Gaps

| Gap | Description | Effort |
|-----|-------------|--------|
| **Webhook signature verification** | Should verify Zulip webhook secrets | Low — pattern from LINE |
| **Body size limits** | Should cap incoming webhook payloads | Low — simple guard |
| **Self-message filtering** | Should skip bot's own messages | Medium — simple check |
| **Timezone timestamps** | Messages lack formatted timestamps | Low — formatting layer |
| **Sticker/Location** | Not applicable to Zulip | N/A |

---

## Porting Priority Recommendation

1. **Priority 1 (Core UX):** Typing indicators, inbound media download, group message observation
2. **Priority 2 (Reliability):** Self-message filtering, soft/hard gate, session title seeding, display name resolution
3. **Priority 3 (Nice-to-have):** Postback button equivalent, continuation window, contact caching, timezone formatting
