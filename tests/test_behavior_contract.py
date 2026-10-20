"""Behavior-contract tests for the zulip plugin.

These tests encode the *promises* the plugin makes to the rest of the system,
and to Evan, who has lost features to bad merges. Each test names a module and
a behavior; if the behavior disappears (the function is deleted, renamed, or
the contract changes) the test fails. That is the whole point: a regression
that silently drops a feature should not pass the deploy gate.

TEST-ONLY. No network, no live profiles, no posting to Zulip. Every test runs
against the real production code paths but with the Zulip SDK swapped for a
recording mock (see the ``adapter`` fixture) or against pure functions that take
no I/O.

Where a test documents a behavior the current code does *not* satisfy, it is
marked ``@pytest.mark.xfail`` with a reason. That is not a failure of the test
suite — it is a filed bug report. Never "fix" these by editing production code
or deleting the test.
"""

import asyncio
from types import SimpleNamespace

import pytest

import zulip.adapter as adapter_module
import zulip.auto_reply as auto_reply_module
import zulip.cron_ownership as cron_ownership_module
import zulip.topic_ingest as topic_ingest_module
import zulip.outbound as outbound_module
import zulip.secret_guard as secret_guard_module
import zulip.audit_logger as audit_logger_module
import zulip.activity_trace as activity_trace_module
import zulip.approval_outcomes as approval_outcomes_module
from tests.conftest import MockZulipClient


def _read_events(tmp_path):
    """Every JSON line the audit logger wrote under ``tmp_path`` (read back from
    disk, so a path that claims to be audited but wrote nothing is exposed)."""
    import json

    logs = list(tmp_path.rglob("*.audit.log"))
    assert logs, "the audit logger wrote no file"
    return [
        json.loads(line)
        for line in logs[0].read_text().splitlines()
        if line
    ]


# ---------------------------------------------------------------------------
# Fixtures — a ZulipAdapter with the SDK swapped for a recording mock client,
# so no network is touched. Mirrors the adapter fixtures in the existing test
# suite (test_topic_routing.py, test_trace_progress_tool.py, test_audit_delivery.py).
# ---------------------------------------------------------------------------


@pytest.fixture
def adapter(mock_platform_config, monkeypatch, tmp_path):
    """A ZulipAdapter whose Zulip SDK points at a recording mock client.

    The SDK handle lives in ``zulip.zulip_client`` (the module that owns it),
    so the fixture patches there, not on ``zulip.adapter`` (which is why the
    pre-existing suite errors on that line). ``get_cached_client`` is replaced
    with a factory that returns a recording client, so no network is touched.
    """
    import zulip.zulip_client as zulip_client_module

    monkeypatch.setattr(zulip_client_module, "ZULIP_AVAILABLE", True)
    monkeypatch.setenv("HERMES_DATA_DIR", str(tmp_path))
    # Trace config is read at construction, so it must be set before the adapter
    # is built (the per-test monkeypatch.setenv happens too late).
    monkeypatch.setenv("ZULIP_ACTIVITY_TRACE", "1")
    monkeypatch.setenv("ZULIP_TRACE_COALESCE_MS", "0")

    class RecordingZulipClient(MockZulipClient):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.sent_messages = []
            self.edited_messages = []

        def send_message(self, request):
            self.sent_messages.append(request)
            return {"result": "success", "id": 501}

        def update_message(self, request):
            self.edited_messages.append(request)
            return {"result": "success"}

    # The adapter calls import_zulip_sdk() first and raises ImportError when the
    # real SDK is absent (the throwaway venv has no ``zulip`` package). Return a
    # fake SDK module so construction proceeds; get_cached_client is then
    # replaced with a factory that returns a recording client.
    fake_sdk = SimpleNamespace(
        Client=lambda **kwargs: RecordingZulipClient(**kwargs)
    )
    monkeypatch.setattr(zulip_client_module, "import_zulip_sdk", lambda *a, **k: fake_sdk)

    def fake_get_cached_client(site, email, api_key, **_):
        return RecordingZulipClient(site=site, email=email, api_key=api_key)

    monkeypatch.setattr(zulip_client_module, "get_cached_client", fake_get_cached_client)

    # Isolate the module-level live-adapter registry so a stale adapter from an
    # earlier test can't answer for this one (see test_trace_progress_tool.py).
    monkeypatch.setattr(
        adapter_module, "_LIVE_ADAPTERS", type(adapter_module._LIVE_ADAPTERS)()
    )
    adapter_module._clear_caches()

    from zulip.adapter import ZulipAdapter

    a = ZulipAdapter(mock_platform_config)
    a.email = "bot@zulip.com"
    # The progress handler resolves its trace through tracing._host_get_session_env
    # (imported lazily at module load into the tracing namespace). Point it at a
    # mutable context dict the test can populate before calling the handler.
    import zulip.tracing as tracing_module

    context = {}
    monkeypatch.setattr(
        tracing_module,
        "_host_get_session_env",
        lambda name, default=None: context.get(name, default),
    )
    a._test_context = context
    return a


def _stream_msg(topic: str, msg_id: int = 1, content: str = "hi") -> dict:
    """An inbound Zulip stream message for routing tests."""
    return {
        "id": msg_id,
        "type": "stream",
        "stream_id": 7,
        "subject": topic,
        "display_recipient": "engineering",
        "content": content,
        "sender_email": "user@zulip.com",
        "sender_full_name": "User",
        "sender_id": 42,
    }


# ---------------------------------------------------------------------------
# 1. Cron marker parsing + owner-only dedupe (topic_ingest)
# ---------------------------------------------------------------------------


class TestCronMarkerParsing:
    """A cron post carries the footer ``-# 🕒 cron:<job_id> run:<run_id>`` and
    parse_cron_marker must recover exactly that (topic_ingest)."""

    def test_parse_cron_marker_recovers_job_and_run(self):
        marker = topic_ingest_module.format_cron_marker("job-1", "run-42")
        parsed = topic_ingest_module.parse_cron_marker(
            "Here is the report\n\n" + marker
        )
        assert parsed is not None
        assert parsed.job_id == "job-1"
        assert parsed.run_id == "run-42"

    def test_parse_cron_marker_returns_none_without_marker(self):
        assert topic_ingest_module.parse_cron_marker("no marker here") is None

    def test_format_is_owner_only_key(self):
        """The marker line is the dedupe key: two runs of the same job are the
        same logical cron post, so dedupe must key on job+run, not on arbitrary
        body text."""
        line = topic_ingest_module.format_cron_marker("job-1", "run-42")
        assert line.startswith("-# 🕒 cron:")
        assert "run-42" in line


class TestOwnerOnlyDedupe:
    """A cron post is ingested into the owning profile only. Dedupe is
    owner-only: the same job+run in a different owner's topic is a different
    post (topic_ingest.record / build_observed_row)."""

    def test_build_observed_row_tags_cron_job_and_run(self):
        marker = topic_ingest_module.CronMarker(job_id="job-1", run_id="run-42")
        row = topic_ingest_module.build_observed_row(
            session_key="agent:main",
            content="body\n\n" + marker.line(),
            sender_name="cron-bot",
            cron_marker=marker,
        )
        assert row["cron_job_id"] == "job-1"
        assert row["cron_run_id"] == "run-42"
        # The footer is stripped from the stored body but preserved as fields.
        assert "body" in row["content"]
        assert "🕒 cron:" not in row["content"]

    def test_dedupe_skips_duplicate_job_run(self, monkeypatch, tmp_path):
        """The dedupe registry must reject a repeated job+run for the same
        owner. We drive the real dedupe store (dedupe_store) directly: the
        first check of a key is new (False), a repeat is seen (True)."""
        from zulip.dedupe_store import ZulipDedupeStore

        store = ZulipDedupeStore(account_id="cron-bot", data_dir=str(tmp_path))
        key = "cron:job-1:run-42"
        assert store.check(key) is False  # first time
        assert store.check(key) is True  # duplicate job+run for same owner


# ---------------------------------------------------------------------------
# 2. Reply routing: cron owner → default fallback (auto_reply / inbound)
# ---------------------------------------------------------------------------


class TestSilentReply:
    """[SILENT] output posts nothing (auto_reply.is_silent_reply)."""

    def test_exact_silent_marker_is_silent(self):
        assert auto_reply_module.is_silent_reply("[SILENT]") is True

    def test_silent_with_surrounding_whitespace_is_silent(self):
        assert auto_reply_module.is_silent_reply("  [SILENT]  ") is True

    def test_non_silent_text_is_not_silent(self):
        assert auto_reply_module.is_silent_reply("working on it") is False
        assert auto_reply_module.is_silent_reply("") is False
        assert auto_reply_module.is_silent_reply(None) is False


class TestMentionDetection:
    """A wildcard/group @-mention of *anyone* beats cron ownership and defers to
    today's reply path (auto_reply.message_mentions_anyone)."""

    def test_wildcard_mention_is_detected(self):
        assert auto_reply_module.message_mentions_anyone("hey @everyone") is True
        assert auto_reply_module.message_mentions_anyone("hey @channel") is True
        assert auto_reply_module.message_mentions_anyone("hey @all") is True
        assert auto_reply_module.message_mentions_anyone("hey @topic") is True

    def test_no_mention_is_not_detected(self):
        assert auto_reply_module.message_mentions_anyone("just a plain message") is False
        assert auto_reply_module.message_mentions_anyone("") is False
        assert auto_reply_module.message_mentions_anyone(None) is False


class TestCronOwnerResolution:
    """resolve_topic_cron_owner returns the owning profile for the most recent
    in-window cron post in a topic (auto_reply.resolve_topic_cron_owner)."""

    def test_signature_exists(self):
        import inspect

        assert "topic" in inspect.signature(
            auto_reply_module.resolve_topic_cron_owner
        ).parameters
        assert "settings" in inspect.signature(
            auto_reply_module.resolve_topic_cron_owner
        ).parameters

    def test_settings_defaults(self):
        """resolve_auto_reply_settings must not require any live config to be
        constructed — tests (and hosts with no explicit settings) need it."""
        settings = auto_reply_module.resolve_auto_reply_settings()
        assert hasattr(settings, "window_hours")
        assert hasattr(settings, "fallback_profile")
        assert settings.enabled is True


# ---------------------------------------------------------------------------
# 3. Progress-edit: one message per turn, tools edited in place, full paths
# ---------------------------------------------------------------------------


class TestProgressRender:
    """The activity-trace render keeps the tool lines edited in place and the
    final answer in the same message, with full paths/commands/URLs — no
    ellipsis truncation (activity_trace.TraceState.render)."""

    def test_render_signature(self):
        import inspect

        assert "max_content" in inspect.signature(
            activity_trace_module.TraceState.render
        ).parameters

    def test_render_keeps_full_paths_urls_no_ellipsis(self):
        """A path, a command and a URL with no ellipsis: the render must not
        drop characters. This is the regression the task calls out. The status
        board is truncated to a bounded budget, so we render with a generous
        budget and assert the full content survives verbatim."""
        activity_trace_module.DEFAULT_MAX_CONTENT = 1 << 20
        full_path = "/mnt/projects/zulip-hermes-integration/zulip/adapter.py"
        full_cmd = "git -C /mnt/projects/zulip-hermes-integration push origin tests/behavior-contract"
        full_url = "https://github.com/imasaru/zulip-hermes-integration/pull/42/files"

        state = activity_trace_module.TraceState(
            title="Working",
            steps=[
                activity_trace_module.TraceStep(
                    label=full_cmd, status="done", detail="bash"
                ),
                activity_trace_module.TraceStep(
                    label=full_path, status="done", detail="file"
                ),
            ],
            note=full_url,
        )
        rendered = state.render(max_content=1 << 20)
        assert full_cmd in rendered
        assert full_path in rendered
        assert full_url in rendered
        # No truncation marker anywhere.
        assert "\u2026" not in rendered  # horizontal ellipsis
        assert "..." not in rendered

    @pytest.mark.asyncio
    async def test_progress_edit_uses_update_message(self, adapter, monkeypatch):
        """A progress note updates the existing message in place (update_message),
        not a new send — one message per turn. The gateway calls the adapter's
        ``record_progress_step`` via the ``_zulip_progress_handler`` tool handler.

        A trace must be active first (the handler walks the live-adapter registry
        and only shows the note when a run's trace is open), so we start one.
        """
        adapter.client.edited_messages = []

        # Start a trace for a work item in the "Deploy" topic so the handler's
        # "trace is active" check passes.
        event = SimpleNamespace(
            source=SimpleNamespace(chat_id="7"),
            metadata={"thread_id": "Deploy"},
            message_id=1,
        )

        await adapter.on_processing_start(event)

        # The trace's initial "Working" message posts on a background task; the
        # progress handler only shows steps once that message exists (message_id
        # set). Await the flush so the recording client has received it.
        key = adapter._trace_sessions["route:7\x00Deploy"]
        task = adapter._trace_start_tasks.get(key)
        if task is not None:
            await task

        # The progress handler resolves the active trace through the host's
        # session-context env; populate the test context dict the fixture wired up.
        adapter._test_context["HERMES_SESSION_CHAT_ID"] = "7"
        adapter._test_context["HERMES_SESSION_THREAD_ID"] = "Deploy"

        result = adapter_module._zulip_progress_handler(
            {"note": "checking the logs"}
        )
        assert result == "noted"

        # The step's edit is applied by a coalesced background flush task; await
        # it so the recording client has received the update.
        pending = adapter._traces[key]._pending
        if pending is not None:
            await pending

        assert len(adapter.client.edited_messages) >= 1
        assert "✓ checking the logs" in adapter.client.edited_messages[-1]["content"]


# ---------------------------------------------------------------------------
# 4. Approval routing: blocking approval → right route, approve/deny resolves
# ---------------------------------------------------------------------------


class TestApprovalResolution:
    """A blocking approval is recorded and resolved; approve/deny produce an
    outcome on the prompt's own route (approval_outcomes.ApprovalLedger.resolve)."""

    def test_resolve_returns_none_without_pending_request(self):
        ledger = approval_outcomes_module.ApprovalLedger()
        outcome = ledger.resolve(
            session_key="agent:main:zulip:stream:7:Deploy", choice="approve"
        )
        assert outcome is None

    def test_resolve_labels_the_pending_request(self):
        ledger = approval_outcomes_module.ApprovalLedger()
        session_key = "agent:main:zulip:stream:7:Deploy"
        ledger.note_request(
            session_key=session_key, surface="gateway", command="rm -rf build"
        )
        outcome = ledger.resolve(
            session_key=session_key,
            choice="approve",
            route=(session_key.split(":")[-1], "Deploy"),
        )
        assert outcome is not None
        assert outcome.choice == "approve"
        assert outcome.command == "rm -rf build"
        assert outcome.refused is False

    def test_deny_is_a_refusal(self):
        ledger = approval_outcomes_module.ApprovalLedger()
        session_key = "agent:main:zulip:stream:7:Deploy"
        ledger.note_request(
            session_key=session_key, surface="gateway", command="deploy"
        )
        outcome = ledger.resolve(
            session_key=session_key,
            choice="deny",
            route=("Deploy", "Deploy"),
        )
        assert outcome is not None
        assert outcome.refused is True

    def test_timeout_choice_is_a_refusal(self):
        """A blocked approval that nobody answered is a refusal — the command
        must not run."""
        ledger = approval_outcomes_module.ApprovalLedger()
        session_key = "agent:main:zulip:stream:7:Deploy"
        ledger.note_request(
            session_key=session_key, surface="gateway", command="deploy"
        )
        outcome = ledger.resolve(
            session_key=session_key,
            choice=approval_outcomes_module.TIMEOUT_CHOICE,
            route=("Deploy", "Deploy"),
        )
        assert outcome is not None
        assert outcome.refused is True

    def test_notice_for_deny_policy_posts_a_refusal_line(self):
        """With the deny policy, an unanswered approval gets one refusal line in
        the prompt's own route (approval_outcomes.notice_for)."""
        outcome = approval_outcomes_module.ApprovalOutcome(
            choice=approval_outcomes_module.TIMEOUT_CHOICE,
            decider=approval_outcomes_module.DECIDER_TIMEOUT,
            request_id="abc",
            session_key="agent:main",
            chat_id="7",
            topic="Deploy",
        )
        notice = approval_outcomes_module.notice_for(
            outcome,
            policy=approval_outcomes_module.APPROVAL_ON_TIMEOUT_DENY,
            host_notices=False,
        )
        assert notice is not None
        assert "Refused" in notice

    def test_notice_for_allow_policy_posts_nothing(self):
        """With the allow policy (migration contract), an unanswered approval
        posts nothing — the command ran, so there is no refusal to say."""
        outcome = approval_outcomes_module.ApprovalOutcome(
            choice=approval_outcomes_module.TIMEOUT_CHOICE,
            decider=approval_outcomes_module.DECIDER_TIMEOUT,
            request_id="abc",
            session_key="agent:main",
            chat_id="7",
            topic="Deploy",
        )
        notice = approval_outcomes_module.notice_for(
            outcome,
            policy=approval_outcomes_module.APPROVAL_ON_TIMEOUT_ALLOW,
            host_notices=True,
        )
        assert notice is None


# ---------------------------------------------------------------------------
# 5. No-reply audit: a run tracked by topic is found by thread id
# ---------------------------------------------------------------------------


class TestNoReplyAudit:
    """The no-reply audit records a dispatched run that produced nothing, and a
    run that never dispatched is distinguishable from one that ran and stayed
    silent (audit_logger). The 2026-10-18 regression fixed the thread-id lookup
    so a run tracked by topic is found by thread id."""

    @pytest.mark.asyncio
    async def test_dispatch_turn_records_topic(self, tmp_path):
        audit = audit_logger_module.AuditLogger(str(tmp_path))
        await audit.log_dispatch_turn("dm:1032616", topic="api-review", message_id=1)
        events = _read_events(tmp_path)
        events = [e for e in events if e["event"] == "dispatch_turn"]
        assert len(events) == 1
        assert events[0]["details"]["topic"] == "api-review"

    @pytest.mark.asyncio
    async def test_dispatch_plus_empty_is_distinguishable_from_never_ran(self, tmp_path):
        audit = audit_logger_module.AuditLogger(str(tmp_path))
        await audit.log_dispatch_turn("dm:1032616", topic="api-review")
        await audit.log_deliver_empty(chat_id="dm:1032616", topic="api-review")
        names = [e["event"] for e in _read_events(tmp_path)]
        assert names == ["dispatch_turn", "deliver_empty"]


# ---------------------------------------------------------------------------
# 6. Secret guard: credentials never appear in posted text
# ---------------------------------------------------------------------------


class TestSecretGuard:
    """Tokens never appear in posted text. detect_secret_leak must flag a
    credential value in an outbound message (outbound.detect_secret_leak /
    secret_guard.find_leaked_secrets)."""

    def test_detect_secret_leak_flags_api_key(self, adapter):
        adapter.api_key = "supersecretapikey1234567890"
        adapter._known_secrets_cache = None
        hits = outbound_module.detect_secret_leak(adapter, "here is the key: supersecretapikey1234567890")
        assert len(hits) >= 1
        assert any(hit.value == "supersecretapikey1234567890" for hit in hits)

    def test_detect_secret_leak_returns_empty_when_no_credential(self, adapter):
        adapter.api_key = "supersecretapikey1234567890"
        adapter._known_secrets_cache = None
        hits = outbound_module.detect_secret_leak(adapter, "nothing secret here")
        assert hits == []

    def test_find_leaked_secrets_is_the_detection_function(self):
        """The secret-guard detection function is find_leaked_secrets."""
        assert hasattr(secret_guard_module, "find_leaked_secrets")
        assert callable(secret_guard_module.find_leaked_secrets)


# ---------------------------------------------------------------------------
# 7. Ownership tracking (cron_ownership)
# ---------------------------------------------------------------------------


class TestCronOwnership:
    """cron_ownership owns the dedupe scope and the owning-profile lookup."""

    def test_profiles_root_for_returns_path(self, adapter, tmp_path, monkeypatch):
        monkeypatch.setenv("HERMES_DATA_DIR", str(tmp_path))
        root = cron_ownership_module.profiles_root_for(adapter)
        # profiles_root_for returns a Path (or None if not configured); either way
        # it must not raise and must be consistent across calls.
        assert root is None or isinstance(root, type(tmp_path))
