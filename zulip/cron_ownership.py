"""Shared cron-ownership resolution for the Zulip plugin.

One cron-marked Zulip post must be recorded (ingest dedupe) and answered
(auto-reply routing) by exactly one profile. The owner is the profile whose
``cron/jobs.json`` contains the job_id; when no jobs.json names it, the
profile whose bot user id sent the message.

The index is module-level and therefore shared by every adapter in the
gateway process, so all six profiles resolve the same owner. It refreshes
when any jobs.json mtime changes (throttled), so a job added or moved to
another profile is picked up without a restart.

Bot identities (user id -> profile) are registered lazily by each adapter —
reading another profile's secrets to learn its bot id is not an option, so
each adapter registers itself and the map converges as adapters connect.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Optional

from . import runtime_scope

logger = logging.getLogger(__name__)

#: How often (seconds) the jobs.json sweep may re-stat the profiles root.
_INDEX_RESCAN_SECONDS = 30.0


# ---------------------------------------------------------------------------
# This adapter's own profile
# ---------------------------------------------------------------------------


def own_profile_name(adapter: Any) -> str:
    """Best-effort name of the Hermes profile that owns ``adapter``.

    Order: the gateway-stamped ``_owner_profile`` (authoritative under
    ``gateway.multiplex_profiles``), the profile data dir's basename
    (``.../profiles/<name>``), then the ``HERMES_PROFILE`` env fallback.
    """
    owner = getattr(adapter, "_owner_profile", None)
    if isinstance(owner, str) and owner.strip():
        return owner.strip()
    data_dir = getattr(adapter, "_data_dir", None)
    if data_dir:
        name = Path(str(data_dir)).name.strip()
        if name and name != runtime_scope.LEGACY_HERMES_HOME:
            return name
    env = (os.environ.get("HERMES_PROFILE") or "").strip()
    return env or "default"


def profiles_root_for(adapter: Any) -> Optional[Path]:
    """The ``.../profiles`` directory holding every profile, if detectable."""
    data_dir = getattr(adapter, "_data_dir", None)
    if not data_dir:
        try:
            data_dir = runtime_scope.get_profile_data_dir()
        except Exception:
            return None
    path = Path(str(data_dir)).expanduser()
    if path.name and path.parent.name == "profiles":
        return path.parent
    return None


# ---------------------------------------------------------------------------
# Bot identity registry (user id / email -> profile)
# ---------------------------------------------------------------------------

_bot_lock = threading.RLock()
_bot_id_to_profile: dict[str, str] = {}
_bot_email_to_profile: dict[str, str] = {}
# Mention label (profile name, full name, email local-part) -> profile.
_bot_alias_to_profile: dict[str, str] = {}


def _remember_alias(alias: str, profile: str) -> None:
    key = (alias or "").strip().lower()
    if key and profile:
        _bot_alias_to_profile[key] = profile


def register_bot_identity(adapter: Any) -> None:
    """Record this adapter's bot user id, email, and mention names."""
    try:
        profile = own_profile_name(adapter)
        bot_id = getattr(adapter, "_bot_user_id", None)
        email = getattr(adapter, "email", None)
        full_name = getattr(adapter, "bot_full_name", None)
        with _bot_lock:
            if bot_id is not None and str(bot_id).strip():
                _bot_id_to_profile[str(bot_id).strip()] = profile
            if isinstance(email, str) and email.strip():
                _bot_email_to_profile[email.strip().lower()] = profile
            _remember_alias(profile, profile)
            if isinstance(email, str) and email.strip():
                local = email.split("@")[0].strip().lower()
                _remember_alias(local, profile)
                if local.endswith("-bot") and local[: -len("-bot")]:
                    _remember_alias(local[: -len("-bot")], profile)
            if isinstance(full_name, str):
                _remember_alias(full_name, profile)
    except Exception:
        logger.debug("cron_ownership: bot identity registration failed", exc_info=True)


def profile_for_bot(sender_id: Any = None, sender_email: str = "") -> Optional[str]:
    """Profile whose bot sent a message, or None when unknown."""
    with _bot_lock:
        if sender_id is not None and str(sender_id).strip():
            hit = _bot_id_to_profile.get(str(sender_id).strip())
            if hit:
                return hit
        email = (sender_email or "").strip().lower()
        if email:
            return _bot_email_to_profile.get(email)
        return None


def profile_for_alias(name: str) -> Optional[str]:
    """Profile whose mention label is ``name``, or None when unregistered."""
    key = (name or "").strip().lower()
    if not key:
        return None
    with _bot_lock:
        return _bot_alias_to_profile.get(key)


def self_aliases(adapter: Any) -> set[str]:
    """Mention labels that mean this adapter, whether or not it has registered."""
    names: set[str] = set()
    profile = own_profile_name(adapter)
    if profile and profile.strip():
        names.add(profile.strip().lower())
    full_name = getattr(adapter, "bot_full_name", None)
    if isinstance(full_name, str) and full_name.strip():
        names.add(full_name.strip().lower())
    email = getattr(adapter, "email", None)
    if isinstance(email, str) and email.strip():
        local = email.split("@")[0].strip().lower()
        if local:
            names.add(local)
            if local.endswith("-bot") and local[: -len("-bot")]:
                names.add(local[: -len("-bot")])
    return names


def profile_dir_names(adapter: Any) -> set[str]:
    """Profile directory names next to this adapter, lowercased.

    Each Hermes profile is one bot. A stream mention of that name is a bot
    mention even before that bot has connected and registered its full name.
    """
    root = profiles_root_for(adapter)
    if root is None:
        return set()
    try:
        entries = root.iterdir()
    except OSError:
        return set()
    names: set[str] = set()
    for entry in entries:
        try:
            if entry.is_dir() and not entry.name.startswith("."):
                names.add(entry.name.strip().lower())
        except OSError:
            continue
    return names


# A trailing " (...)" suffix is what Zulip appends to a bot mention, e.g.
# ``@**Treasurer (hermes-ai-agent)**``. Strip it before name matching and use
# its presence as the signal that the label names a bot-like identity.
_SUFFIX_RE = re.compile(r"\s+\([^)]*\)\s*$")


def _strip_mention_suffix(label: str) -> str:
    """Remove a trailing " (...)" suffix from a mention label."""
    return _SUFFIX_RE.sub("", label).strip()


def classify_command_target(adapter: Any, target: str | None) -> str:
    """``self``, ``other``, or ``none`` for a mention label on a slash command.

    A label names a bot-like identity (and so must not fall through to the
    topic owner) when it is self, matches a registered bot or profile dir, or
    carries the `` (suffix)`` that Zulip adds to bot mentions. A plain name
    with no suffix that is not self or a known profile is treated as a human
    mention and left for the topic owner.
    """
    if not target or not target.strip():
        return "none"

    me = own_profile_name(adapter).strip().lower()
    label = target.strip()

    # (a) Classify by the mentioning user's id first. A command may arrive as
    #     ``Name|<id>``; keep the id so this works even when the name would
    #     otherwise look like the owner.
    name_part, _, id_part = label.partition("|")
    id_part = id_part.strip()
    if id_part.isdigit():
        bot_profile = profile_for_bot(sender_id=id_part)
        if bot_profile is not None:
            return "self" if bot_profile.strip().lower() == me else "other"

    # (b) Compare names case-insensitively after stripping a trailing
    #     " (...)" suffix, and also against profile names and email
    #     local-parts.
    key = _strip_mention_suffix(name_part).lower()
    if key:
        if key in self_aliases(adapter):
            return "self"
        alias_profile = profile_for_alias(key)
        if alias_profile:
            return "self" if alias_profile.strip().lower() == me else "other"
        if key in profile_dir_names(adapter):
            return "self" if key == me else "other"

    # (d) The label names a bot-like identity (Zulip suffix) that is not self
    #     and not otherwise known: it must not fall through to the topic owner.
    if _SUFFIX_RE.search(label):
        return "other"

    return "none"


def reset_bot_registry_for_tests() -> None:
    """Clear the bot registry (tests only)."""
    with _bot_lock:
        _bot_id_to_profile.clear()
        _bot_email_to_profile.clear()
        _bot_alias_to_profile.clear()


# ---------------------------------------------------------------------------
# job_id -> profile index over cron/jobs.json
# ---------------------------------------------------------------------------


def _read_jobs_file(path: Path) -> dict[str, str]:
    """Map job_id -> profile-name-hint from one jobs.json. Never raises."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.debug("cron_ownership: unreadable %s: %s", path, exc)
        return {}
    jobs = data.get("jobs") if isinstance(data, dict) else None
    if not isinstance(jobs, list):
        return {}
    profile = path.parent.parent.name  # .../profiles/<name>/cron/jobs.json
    out: dict[str, str] = {}
    for job in jobs:
        if not isinstance(job, dict):
            continue
        job_id = job.get("id")
        if isinstance(job_id, str) and job_id.strip():
            out[job_id.strip()] = profile
    return out


class CronOwnershipIndex:
    """Cached job_id -> profile map, refreshed on jobs.json mtime changes."""

    def __init__(self, profiles_root: Path) -> None:
        self._root = Path(profiles_root)
        self._lock = threading.RLock()
        self._job_to_profile: dict[str, str] = {}
        self._mtimes: dict[str, float] = {}
        self._last_sweep = 0.0

    def _jobs_files(self) -> list[Path]:
        try:
            entries = sorted(self._root.iterdir())
        except OSError:
            return []
        return [
            entry / "cron" / "jobs.json"
            for entry in entries
            if entry.is_dir() and (entry / "cron" / "jobs.json").is_file()
        ]

    def refresh(self, *, force: bool = False) -> None:
        """Re-read every jobs.json whose mtime changed (throttled)."""
        now = time.monotonic()
        with self._lock:
            if not force and now - self._last_sweep < _INDEX_RESCAN_SECONDS:
                return
            self._last_sweep = now
            seen: set[str] = set()
            for path in self._jobs_files():
                key = str(path)
                seen.add(key)
                try:
                    mtime = path.stat().st_mtime
                except OSError:
                    continue
                if not force and self._mtimes.get(key) == mtime:
                    continue
                self._mtimes[key] = mtime
                # Drop this file's old rows before re-adding (a job may
                # have moved to another profile's jobs.json).
                stale = [
                    job_id
                    for job_id, prof in self._job_to_profile.items()
                    if prof == path.parent.parent.name
                ]
                for job_id in stale:
                    self._job_to_profile.pop(job_id, None)
                self._job_to_profile.update(_read_jobs_file(path))
            for key in [k for k in self._mtimes if k not in seen]:
                self._mtimes.pop(key, None)

    def profile_for_job(self, job_id: str) -> Optional[str]:
        self.refresh()
        with self._lock:
            return self._job_to_profile.get((job_id or "").strip())


_index_lock = threading.RLock()
_indexes: dict[str, CronOwnershipIndex] = {}


def get_index(profiles_root: Path) -> CronOwnershipIndex:
    """Module-level shared index for one profiles root."""
    key = str(profiles_root)
    with _index_lock:
        index = _indexes.get(key)
        if index is None:
            index = CronOwnershipIndex(profiles_root)
            _indexes[key] = index
        return index


def reset_indexes_for_tests() -> None:
    """Drop cached indexes (tests only)."""
    with _index_lock:
        _indexes.clear()


# ---------------------------------------------------------------------------
# Owner resolution
# ---------------------------------------------------------------------------


def resolve_cron_owner(
    *,
    adapter: Any,
    job_id: str,
    sender_id: Any = None,
    sender_email: str = "",
) -> Optional[str]:
    """Owning profile for a cron-marked post, or None when unknowable.

    jobs.json wins; otherwise the sender bot's profile. ``None`` means
    "no determinable owner" — callers fail open (keep today's behaviour)
    rather than dropping a post nobody owns.
    """
    register_bot_identity(adapter)
    root = profiles_root_for(adapter)
    if root is not None:
        try:
            owner = get_index(root).profile_for_job(job_id)
        except Exception:
            logger.debug("cron_ownership: index lookup failed", exc_info=True)
            owner = None
        if owner:
            return owner
    return profile_for_bot(sender_id, sender_email)
