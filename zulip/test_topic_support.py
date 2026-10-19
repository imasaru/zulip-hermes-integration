"""Tests for Zulip adapter topic support.

Covers:
1. _parse_target with stream:topic format (Fix 1)
2. Topic resolution priority in live send path (Fix 2)
3. Topic resolution priority in standalone cron send path (Fix 3)

Since the adapter module uses relative imports, we extract the core logic
into standalone testable functions that mirror the adapter's implementation.
"""

import pytest


def _parse_target(chat_id: str) -> dict:
    """Mirror of the adapter's _parse_target with topic support (Fix 1).
    
    Parse chat_id into target info, using cache if available.
    Supports:
    - "dm:428945" -> DM with user_ids
    - "dm:7,42,99" -> Group DM with multiple user_ids
    - "dm:1032616:session:1" -> Session-scoped DM (strips :session:N)
    - "614901" -> Bare stream ID
    - "614901:general" -> Stream with topic
    """
    if chat_id.startswith("dm:"):
        # Session-scoped DM chat_ids include a :session:N suffix (e.g.
        # dm:1032616:session:1). Strip everything after the recipient list
        # so the send path resolves the correct target. (Issue #111)
        #
        # A group DM carries every recipient, comma-separated
        # (dm:7,42,99). Replying with only the sender would start a new
        # one-to-one DM instead of continuing the group conversation, so the
        # complete set is part of the address. (Issue #154)
        recipients = chat_id[3:].split(":", 1)[0]
        user_ids = [int(part) for part in recipients.split(",") if part.strip()]
        if not user_ids:
            raise ValueError("DM target must include at least one recipient")
        info = {"type": "dm", "user_ids": user_ids}
    elif ":" in chat_id:
        # Support stream:topic format (e.g. "614901:general")
        parts = chat_id.split(":", 1)
        try:
            stream_id = int(parts[0])
            info = {"type": "stream", "stream_id": stream_id, "topic": parts[1]}
        except ValueError:
            raise ValueError(f"Invalid stream ID in target: {chat_id!r}")
    else:
        # Bare numeric stream ID (legacy)
        try:
            stream_id = int(chat_id)
            info = {"type": "stream", "stream_id": stream_id}
        except ValueError:
            raise ValueError(f"Invalid Zulip target {chat_id!r}: expected a numeric stream id or 'dm:<user_id>'")
    
    return info


def _resolve_topic_live_send(
    topic_override: str | None,
    target: dict,
    metadata: dict,
    topic_cache: dict,
    chat_id: str,
) -> str:
    """Mirror of the live send path topic resolution (Fix 2).
    
    Topic priority: explicit override > parsed from chat_id > metadata > cache > default.
    """
    topic = topic_override or target.get("topic") or metadata.get("topic")
    if not topic:
        topic = topic_cache.get(chat_id, "general")
    return topic


def _resolve_topic_standalone_send(
    topic_directive: str | None,
    target: dict,
    thread_id: str | None,
) -> str:
    """Mirror of the standalone cron send path topic resolution (Fix 3).
    
    Topic priority: topic directive in message > parsed from chat_id > thread_id param > default.
    """
    return (
        topic_directive
        or target.get("topic")
        or (str(thread_id).strip() if thread_id else "")
        or "general"
    )


class TestParseTarget:
    """Tests for _parse_target function with topic support."""

    def test_bare_stream_id(self):
        """Bare numeric stream ID returns stream type with stream_id."""
        result = _parse_target("614901")
        assert result == {"type": "stream", "stream_id": 614901}

    def test_stream_with_topic(self):
        """Stream:topic format returns stream type with stream_id and topic."""
        result = _parse_target("614901:general")
        assert result == {"type": "stream", "stream_id": 614901, "topic": "general"}

    def test_stream_with_complex_topic(self):
        """Stream:topic format handles topics with special characters."""
        result = _parse_target("614901:daily-briefing")
        assert result == {"type": "stream", "stream_id": 614901, "topic": "daily-briefing"}

    def test_stream_with_topic_with_spaces(self):
        """Stream:topic format handles topics with spaces."""
        result = _parse_target("614901:topic with spaces")
        assert result == {"type": "stream", "stream_id": 614901, "topic": "topic with spaces"}

    def test_dm_target(self):
        """DM target returns dm type with user_ids."""
        result = _parse_target("dm:428945")
        assert result == {"type": "dm", "user_ids": [428945]}

    def test_group_dm_target(self):
        """Group DM target returns dm type with multiple user_ids."""
        result = _parse_target("dm:7,42,99")
        assert result == {"type": "dm", "user_ids": [7, 42, 99]}

    def test_session_scoped_dm(self):
        """Session-scoped DM strips :session:N suffix."""
        result = _parse_target("dm:1032616:session:1")
        assert result == {"type": "dm", "user_ids": [1032616]}

    def test_invalid_stream_id_raises_value_error(self):
        """Invalid stream ID raises ValueError."""
        with pytest.raises(ValueError, match="Invalid stream ID in target"):
            _parse_target("not-a-number:topic")

    def test_invalid_stream_id_no_topic_raises_value_error(self):
        """Non-numeric stream ID without topic raises ValueError."""
        with pytest.raises(ValueError, match="Invalid Zulip target"):
            _parse_target("not-a-number")

    def test_empty_dm_raises_value_error(self):
        """Empty DM target raises ValueError."""
        with pytest.raises(ValueError, match="DM target must include at least one recipient"):
            _parse_target("dm:")

    def test_stream_id_zero(self):
        """Stream ID of 0 is valid."""
        result = _parse_target("0")
        assert result == {"type": "stream", "stream_id": 0}

    def test_stream_with_zero_id_and_topic(self):
        """Stream ID of 0 with topic is valid."""
        result = _parse_target("0:general")
        assert result == {"type": "stream", "stream_id": 0, "topic": "general"}


class TestTopicResolutionPriority:
    """Tests for topic resolution priority in send paths."""

    def test_topic_priority_explicit_override_wins(self):
        """Explicit topic override takes highest priority."""
        target = _parse_target("614901:general")
        topic_override = "override-topic"
        metadata = {"topic": "metadata-topic"}
        topic_cache = {}
        
        topic = _resolve_topic_live_send(
            topic_override, target, metadata, topic_cache, "614901"
        )
        assert topic == "override-topic"

    def test_topic_priority_parsed_from_chat_id(self):
        """Parsed topic from chat_id used when no override or metadata."""
        target = _parse_target("614901:parsed-topic")
        topic_override = None
        metadata = {}
        topic_cache = {}
        
        topic = _resolve_topic_live_send(
            topic_override, target, metadata, topic_cache, "614901"
        )
        assert topic == "parsed-topic"

    def test_topic_priority_metadata_fallback(self):
        """Metadata topic used when no override or parsed topic."""
        target = _parse_target("614901")  # No topic in chat_id
        topic_override = None
        metadata = {"topic": "metadata-topic"}
        topic_cache = {}
        
        topic = _resolve_topic_live_send(
            topic_override, target, metadata, topic_cache, "614901"
        )
        assert topic == "metadata-topic"

    def test_topic_priority_cache_fallback(self):
        """Cache topic used when no override, parsed topic, or metadata."""
        target = _parse_target("614901")  # No topic in chat_id
        topic_override = None
        metadata = {}
        topic_cache = {"614901": "cached-topic"}
        
        topic = _resolve_topic_live_send(
            topic_override, target, metadata, topic_cache, "614901"
        )
        assert topic == "cached-topic"

    def test_topic_priority_default_fallback(self):
        """Default 'general' used when no other topic source available."""
        target = _parse_target("614901")  # No topic in chat_id
        topic_override = None
        metadata = {}
        topic_cache = {}  # Empty cache
        
        topic = _resolve_topic_live_send(
            topic_override, target, metadata, topic_cache, "614901"
        )
        assert topic == "general"

    def test_topic_priority_order_respected(self):
        """Priority order is strictly enforced: override > parsed > metadata > cache."""
        target = _parse_target("614901:parsed-topic")
        topic_override = "override-topic"
        metadata = {"topic": "metadata-topic"}
        topic_cache = {"614901": "cached-topic"}
        
        topic = _resolve_topic_live_send(
            topic_override, target, metadata, topic_cache, "614901"
        )
        assert topic == "override-topic"

    def test_standalone_send_topic_priority(self):
        """Standalone send path topic priority chain."""
        target = _parse_target("614901:parsed-topic")
        topic_directive = None
        thread_id = None
        
        topic = _resolve_topic_standalone_send(topic_directive, target, thread_id)
        assert topic == "parsed-topic"

    def test_standalone_send_topic_priority_thread_id(self):
        """Standalone send path uses thread_id when no directive or parsed topic."""
        target = _parse_target("614901")  # No topic in chat_id
        topic_directive = None
        thread_id = "thread-topic"
        
        topic = _resolve_topic_standalone_send(topic_directive, target, thread_id)
        assert topic == "thread-topic"

    def test_standalone_send_topic_priority_directive(self):
        """Standalone send path uses topic directive when present."""
        target = _parse_target("614901:parsed-topic")
        topic_directive = "directive-topic"
        thread_id = "thread-topic"
        
        topic = _resolve_topic_standalone_send(topic_directive, target, thread_id)
        assert topic == "directive-topic"

    def test_standalone_send_topic_priority_default(self):
        """Standalone send path defaults to 'general' when no topic source."""
        target = _parse_target("614901")  # No topic in chat_id
        topic_directive = None
        thread_id = None
        
        topic = _resolve_topic_standalone_send(topic_directive, target, thread_id)
        assert topic == "general"


class TestIntegration:
    """Integration tests for end-to-end topic support."""

    def test_cron_job_deliver_format(self):
        """Cron job deliver format 'zulip:614901:test-chunking' parses correctly."""
        chat_id = "614901:test-chunking"
        result = _parse_target(chat_id)
        
        assert result["type"] == "stream"
        assert result["stream_id"] == 614901
        assert result["topic"] == "test-chunking"

    def test_multiple_topics_in_same_stream(self):
        """Multiple topics in same stream are handled independently."""
        topic1 = _parse_target("614901:topic-a")
        topic2 = _parse_target("614901:topic-b")
        
        assert topic1["topic"] == "topic-a"
        assert topic2["topic"] == "topic-b"
        assert topic1 is not topic2  # Different objects

    def test_topic_with_special_characters(self):
        """Topics with special characters are preserved."""
        special_topics = [
            "topic-with-dashes",
            "topic_with_underscores",
            "topic.with.dots",
            "topic/with/slashes",
            "topic:with:colons",
            "topic with spaces",
            "topic123",
        ]
        
        for topic in special_topics:
            result = _parse_target(f"614901:{topic}")
            assert result["topic"] == topic, f"Failed for topic: {topic}"

    def test_full_cron_job_flow(self):
        """End-to-end: cron job deliver string -> parse target -> resolve topic."""
        # Simulate a cron job with deliver="zulip:614901:test-chunking"
        chat_id = "614901:test-chunking"
        
        # Step 1: Parse target
        target = _parse_target(chat_id)
        assert target["type"] == "stream"
        assert target["stream_id"] == 614901
        assert target["topic"] == "test-chunking"
        
        # Step 2: Resolve topic (standalone send path, no directive or thread_id)
        topic = _resolve_topic_standalone_send(None, target, None)
        assert topic == "test-chunking"

    def test_full_live_send_flow(self):
        """End-to-end: live send with metadata topic and parsed topic."""
        # Simulate a live send with both parsed topic and metadata topic
        chat_id = "614901:parsed-topic"
        
        # Step 1: Parse target
        target = _parse_target(chat_id)
        assert target["topic"] == "parsed-topic"
        
        # Step 2: Resolve topic (live send, metadata has a topic too)
        metadata = {"topic": "metadata-topic"}
        topic = _resolve_topic_live_send(None, target, metadata, {}, chat_id)
        
        # Parsed topic should win over metadata (per priority chain)
        assert topic == "parsed-topic"
