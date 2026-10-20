"""Behavior-contract tests for four Zulip modules previously reported with no dedicated
test coverage: topic_ingest, approval_outcomes, conversations, tracing.

Outcome-based: drive the public functions with real-shaped inputs and assert on the
observable result (a decision, a stored row, a posted payload, a trace key). The Zulip
HTTP boundary is mocked via tests.conftest.MockZulipClient; no live network.
"""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
from types import SimpleNamespace

import pytest

import zulip.approval_outcomes as approval_outcomes
import zulip.conversations as conversations
import zulip.topic_ingest as topic_ingest
import zulip.tracing as tracing
import zulip.activity_trace as activity_trace
from tests.conftest import MockZulipClient


# ---------------------------------------------------------------------------
# topic_ingest
# ---------------------------------------------------------------------------

class TestTopicIngest:

    def test_human_message_is_ingested(self):
        """A human sender is recorded regardless of markers."""
        decision = topic_ingest.should_ingest(
            content="hello everyone", sender_email="alice@example.com",
            own_bot_email="bot@zulip.com")
        assert bool(decision) is True
        assert decision.reason == "human"

    def test_bot_message_without_cron_marker_is_not_ingested(self):
        """The bot's own non-cron echo (sender_id == own_bot_user_id) is dropped."""
        decision = topic_ingest.should_ingest(
            content="just running", sender_id=42, own_bot_user_id=42,
            own_bot_email="bot@zulip.com")
        assert bool(decision) is False
        assert decision.reason == "own_bot_non_cron"

    def test_own_bot_non_cron_message_is_not_ingested(self):
        """The bot's own non-cron echo is not recorded."""
        decision = topic_ingest.should_ingest(
            content="done", sender_id=99, own_bot_user_id=99)
        assert bool(decision) is False
        assert decision.reason == "own_bot_non_cron"

    def test_bot_message_with_cron_marker_is_ingested(self):
        """A cron bot echo carrying the marker and an allowlisted id is recorded."""
        marker = topic_ingest.format_cron_marker("job-1", "run-7")
        decision = topic_ingest.should_ingest(
            content=f"deployed {marker}", sender_id=42,
            own_bot_user_id=99, own_bot_email="bot@zulip.com",
            cron_bot_ids=[42])
        assert bool(decision) is True
        assert decision.reason == "bot_cron_marker"
        assert decision.cron_marker is not None
        assert decision.cron_marker.job_id == "job-1"

    def test_rendering_only_is_not_ingested(self):
        decision = topic_ingest.should_ingest(
            content="hi", sender_email="alice@example.com", rendering_only=True)
        assert bool(decision) is False
        assert decision.reason == "rendering_only"

    def test_ingest_seen_claim_is_idempotent(self):
        """The second claim of a message id is a duplicate; the set is bounded."""
        seen = topic_ingest.IngestSeen(max_size=2)
        assert seen.claim("m1") is True
        assert seen.claim("m1") is False
        assert len(seen) == 1

    def test_cron_marker_format_and_parse_roundtrip(self):
        marker = topic_ingest.format_cron_marker("job 1.2", "run-3")
        assert marker == "-# 🕒 cron:job-1.2 run:run-3"
        parsed = topic_ingest.parse_cron_marker(marker)
        assert parsed is not None
        assert (parsed.job_id, parsed.run_id) == ("job-1.2", "run-3")

    def test_append_cron_marker_is_idempotent(self):
        body = "answer\n"
        with_marker = topic_ingest.append_cron_marker(body, "job-1", "run-7")
        again = topic_ingest.append_cron_marker(with_marker, "job-1", "run-7")
        assert again == with_marker  # no duplicate marker

    def test_plain_content_strips_html(self):
        """A <p>-wrapped footer still parses as a marker after HTML strip."""
        content = "<p>answer</p>\n-# 🕒 cron:job-1 run:run-7"
        decision = topic_ingest.should_ingest(
            content=content, sender_id=42, own_bot_user_id=99, cron_bot_ids=[42])
        assert bool(decision) is True


# ---------------------------------------------------------------------------
# approval_outcomes
# ---------------------------------------------------------------------------

class TestApprovalOutcomes:

    def test_notice_for_timeout_deny_no_host_posts_refusal(self):
        outcome = approval_outcomes.ApprovalOutcome(
            choice=approval_outcomes.TIMEOUT_CHOICE, decider="timeout",
            request_id="r1", session_key="s1")
        text = approval_outcomes.notice_for(
            outcome, policy=approval_outcomes.APPROVAL_ON_TIMEOUT_DENY,
            host_notices=False)
        assert text == approval_outcomes.TIMEOUT_REFUSAL_TEXT

    def test_notice_for_timeout_allow_is_quiet(self):
        outcome = approval_outcomes.ApprovalOutcome(
            choice=approval_outcomes.TIMEOUT_CHOICE, decider="timeout",
            request_id="r1", session_key="s1")
        text = approval_outcomes.notice_for(
            outcome, policy=approval_outcomes.APPROVAL_ON_TIMEOUT_ALLOW,
            host_notices=False)
        assert text is None

    def test_notice_for_timeout_with_host_notices_is_quiet(self):
        outcome = approval_outcomes.ApprovalOutcome(
            choice=approval_outcomes.TIMEOUT_CHOICE, decider="timeout",
            request_id="r1", session_key="s1")
        text = approval_outcomes.notice_for(
            outcome, policy=approval_outcomes.APPROVAL_ON_TIMEOUT_DENY,
            host_notices=True)
        assert text is None

    def test_notice_for_non_timeout_is_quiet(self):
        outcome = approval_outcomes.ApprovalOutcome(
            choice="approve", decider="alice@example.com",
            request_id="r1", session_key="s1")
        assert approval_outcomes.notice_for(
            outcome, policy=approval_outcomes.APPROVAL_ON_TIMEOUT_DENY,
            host_notices=False) is None

    def test_audit_decider_masks_human_address_but_keeps_token(self):
        masked = approval_outcomes.audit_decider("alice@example.com")
        assert "alice" not in masked
        assert masked.endswith("@example.com")
        assert approval_outcomes.audit_decider("timeout") == "timeout"
        assert approval_outcomes.audit_decider("") == "unknown"

    def test_is_owner_decision_is_case_insensitive(self):
        assert approval_outcomes.is_owner_decision("Alice@Zulip.com", "alice@zulip.com")
        assert not approval_outcomes.is_owner_decision("bob@example.com", "alice@zulip.com")

    def test_rejection_reason_owner_not_owner(self):
        approval_outcomes.APPROVAL_ON_TIMEOUT_DENY = approval_outcomes.APPROVAL_ON_TIMEOUT_DENY
        # authority is read at call time via current_authority(); pin it.
        orig = approval_outcomes.resolve_approval_authority
        approval_outcomes.resolve_approval_authority = lambda: approval_outcomes.APPROVAL_AUTHORITY_OWNER
        try:
            reason = approval_outcomes.rejection_reason("bob@example.com", "alice@zulip.com")
            assert reason == approval_outcomes.REJECT_NOT_OWNER
        finally:
            approval_outcomes.resolve_approval_authority = orig

    def test_ledger_resolve_pops_oldest_request(self):
        ledger = approval_outcomes.ApprovalLedger()
        first = ledger.note_request(session_key="s1", surface="gateway")
        ledger.note_request(session_key="s1", surface="gateway")
        outcome = ledger.resolve(session_key="s1", choice="approve", decided_by="alice@example.com")
        assert outcome is not None
        assert outcome.request_id == first.request_id
        assert outcome.choice == "approve"

    def test_ledger_resolve_unknown_session_returns_none(self):
        ledger = approval_outcomes.ApprovalLedger()
        assert ledger.resolve(session_key="nope", choice="approve") is None

    def test_ledger_note_request_rejects_non_gateway_surface(self):
        ledger = approval_outcomes.ApprovalLedger()
        assert ledger.note_request(session_key="s1", surface="smart") is None

    def test_unresolved_authority_warning_names_the_fix(self):
        warning = approval_outcomes.unresolved_authority_warning()
        assert "ZULIP_OWNER_EMAIL" in warning


# ---------------------------------------------------------------------------
# conversations
# ---------------------------------------------------------------------------

class TestConversations:

    @pytest.fixture
    def registry(self, tmp_path):
        return conversations.TopicConversationRegistry(account_id="acc1", data_dir=str(tmp_path))

    def test_resolve_mints_new_conversation(self, registry):
        conv = registry.resolve(channel_id=1, topic_name="dinner")
        assert isinstance(conv, str) and conv  # a non-empty conversation id

    def test_resolve_is_stable_for_same_topic(self, registry):
        first = registry.resolve(channel_id=1, topic_name="dinner")
        second = registry.resolve(channel_id=1, topic_name="dinner")
        assert first == second

    def test_repoint_moves_conversation(self, registry):
        conv = registry.resolve(channel_id=1, topic_name="old")
        moved = registry.repoint(channel_id=1, old_name="old", new_name="new")
        assert moved == conv
        # old name no longer resolves to the conversation
        assert registry.current_name(1, conv) is None or registry.current_name(1, conv) != "old"

    def test_repoint_unmapped_returns_none(self, registry):
        assert registry.repoint(channel_id=1, old_name="ghost", new_name="new") is None

    def test_lookup_maps_topic_to_conversation(self, registry):
        conv = registry.resolve(channel_id=1, topic_name="dinner")
        assert registry.lookup(channel_id=1, topic_name="dinner") == conv

    def test_record_and_derive_session_start(self, registry):
        conv = registry.resolve(channel_id=1, topic_name="dinner")
        registry.record_session_start(
            channel_id=1, conversation_id=conv, session_id="sess-1", started_in="dinner")
        derived = registry.derive_session_start(channel_id=1, conversation_id=conv, session_id="sess-1")
        assert derived == "dinner"


# ---------------------------------------------------------------------------
# tracing
# ---------------------------------------------------------------------------

class TestTracing:

    def test_trace_key_combines_chat_and_topic(self):
        key = tracing.trace_key("chat-1", {"topic": "dinner"})
        assert key == "chat-1\x00dinner"

    def test_trace_key_uses_metadata_topic(self):
        # metadata_topic reads the "topic" key of the metadata mapping.
        key = tracing.trace_key("chat-1", {"topic": "standup"})
        assert key == "chat-1\x00standup"

    def test_trace_records_path_uses_email(self, tmp_path):
        adapter = SimpleNamespace(email="bot@zulip.com", _data_dir=str(tmp_path))
        path = tracing.trace_records_path(adapter)
        # The email is sanitized: '@' and '.' become underscores, letters/digits kept.
        assert path.name.startswith("zulip_traces_bot_zulip.com.json")
        assert path.parent == tmp_path

    def test_save_and_load_trace_records_roundtrip(self, tmp_path):
        adapter = SimpleNamespace(email="bot@zulip.com", _data_dir=str(tmp_path))
        key = "chat-1\x00dinner"
        tracing.persist_trace_record(adapter, key, "chat-1", {"topic": "dinner"}, 555)
        records = tracing.load_trace_records(adapter)
        assert key in records
        assert records[key]["message_id"] == 555
        assert records[key]["chat_id"] == "chat-1"

    def test_load_trace_records_missing_file_returns_empty(self, tmp_path):
        adapter = SimpleNamespace(email="bot@zulip.com", _data_dir=str(tmp_path))
        assert tracing.load_trace_records(adapter) == {}

    def test_drop_trace_record_removes_key(self, tmp_path):
        adapter = SimpleNamespace(email="bot@zulip.com", _data_dir=str(tmp_path))
        key = "chat-1\x00dinner"
        tracing.persist_trace_record(adapter, key, "chat-1", {"topic": "dinner"}, 555)
        tracing.drop_trace_record(adapter, key)
        assert key not in tracing.load_trace_records(adapter)

    def test_note_trace_reply_marks_replied(self):
        adapter = SimpleNamespace(_traces={"k1": True}, _trace_replied=set())
        tracing.note_trace_reply(adapter, "chat-1", {"topic": "dinner"})
        assert tracing.trace_key("chat-1", {"topic": "dinner"}) in adapter._trace_replied

    def test_record_progress_step_returns_false_without_trace(self):
        adapter = SimpleNamespace(_trace_cfg=SimpleNamespace(enabled=True), _trace_sessions={})
        assert tracing.record_progress_step(adapter, "thinking") is False

    def test_start_trace_and_persist_roundtrip(self, tmp_path):
        async def run():
            async def post(content):
                return 0  # simulate a successful send returning message_id 0
            async def edit(message_id, content):
                return True
            adapter = SimpleNamespace(
                _trace_cfg=activity_trace.TraceConfig(enabled=True),
                _trace_sessions={}, _trace_started={}, _trace_records={},
                _traces={}, _data_dir=str(tmp_path), email="bot@zulip.com",
                _send_timeout=30, client=MockZulipClient())
            await tracing.start_trace_and_persist(
                adapter, activity_trace.ActivityTrace(
                    post, edit, adapter._trace_cfg, title="Working"),
                tracing.trace_key("chat-1", {"topic": "dinner"}), "chat-1", {"topic": "dinner"})
            return tracing.load_trace_records(adapter)
        records = asyncio.run(run())
        # A trace record is persisted for the session.
        assert tracing.trace_key("chat-1", {"topic": "dinner"}) in records
        assert records[tracing.trace_key("chat-1", {"topic": "dinner"})]["message_id"] == 0
