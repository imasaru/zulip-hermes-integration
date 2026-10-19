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


def register_bot_identity(adapter: Any) -> None:
    """Record this adapter's bot user id and email against its profile."""
    try:
        profile = own_profile_name(adapter)
        bot_id = getattr(adapter, "_bot_user_id", None)
        email = getattr(adapter, "email", None)
        with _bot_lock:
            if bot_id is not None and str(bot_id).strip():
                _bot_id_to_profile[str(bot_id).strip()] = profile
            if isinstance(email, str) and email.strip():
                _bot_email_to_profile[email.strip().lower()] = profile
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


def reset_bot_registry_for_tests() -> None:
    """Clear the bot registry (tests only)."""
    with _bot_lock:
        _bot_id_to_profile.clear()
        _bot_email_to_profile.clear()


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
