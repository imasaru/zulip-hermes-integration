"""Regression tests for the Zulip failures seen live on 2026-10-10.

Every test below reproduces a failure Evan actually hit (or the condition that
caused it) and asserts what a user should observe when things work. Each test
docstring states: SCENARIO (the real situation) and EXPECTED (correct behavior).

Background: on the ai-agent Hermes gateway (multiplexed profiles) the bot
added a 👀 reaction to Evan's Zulip message and then went silent; cron job
a724ee2dd303 (daily-morning-report) failed delivery with
"zulip package not installed" although the SDK was installed; replies that
the core sends after a turn failed with "Hermes could not read this
profile's ZULIP_BLOCK_SECRET_LEAKS"; and deliveries to legacy
``dm_user:<id>`` targets were rejected as "Invalid stream ID".
"""

from __future__ import annotations

import contextvars
import json
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock

import pytest

import zulip.adapter as adapter_module

REPO_ROOT = Path(__file__).resolve().parent.parent
PLUGIN_DIR = REPO_ROOT / "zulip"
STUBS_DIR = Path(__file__).resolve().parent / "stubs"


# ---------------------------------------------------------------------------
# Fresh-process harness (scenarios a and b)
# ---------------------------------------------------------------------------
# The child loads the plugin exactly the way Hermes does: as the package
# ``hermes_plugins.zulip`` while the plugins directory itself is on sys.path,
# so a bare ``import zulip`` would resolve to the plugin, not the SDK.
_CHILD = textwrap.dedent(
    r'''
    import asyncio, importlib.util, json, os, sys, types
    repo, stubs, action = sys.argv[1], sys.argv[2], sys.argv[3]
    plugin_dir = os.path.join(repo, "zulip")
    sys.path.insert(0, stubs)
    sys.path.insert(0, repo)  # plugins dir on sys.path, like Hermes
    pkg = types.ModuleType("hermes_plugins"); pkg.__path__ = []
    sys.modules["hermes_plugins"] = pkg
    spec = importlib.util.spec_from_file_location(
        "hermes_plugins.zulip", os.path.join(plugin_dir, "__init__.py"),
        submodule_search_locations=[plugin_dir])
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    A = sys.modules["hermes_plugins.zulip.adapter"]
    out = {}
    # Stub the network: the SDK's Client() fetches server settings and
    # send_message POSTs; both go through requests.Session.request.
    sent = []
    try:
        import requests
        _PAYLOAD = {"result": "success", "msg": "", "id": 4242,
                    "zulip_version": "8.0", "zulip_feature_level": 200}
        class _Resp:
            status_code = 200
            headers = {}
            text = json.dumps(_PAYLOAD)
            def json(self):
                return dict(_PAYLOAD)
        def _fake_request(self, method, url, *a, **k):
            sent.append({"method": method, "url": url, "data": k.get("data")})
            return _Resp()
        requests.Session.request = _fake_request
    except ImportError:
        pass  # no requests -> no SDK either; nothing can reach the network
    if action == "import":
        before = list(sys.path)
        try:
            sdk = A._import_zulip_sdk()
            out["returned_none"] = sdk is None
            out["has_client"] = bool(sdk is not None and hasattr(sdk, "Client"))
            out["sdk_file"] = getattr(sdk, "__file__", None)
        except BaseException as e:
            out["exc_type"] = type(e).__name__; out["exc"] = str(e)
        out["sys_path_unchanged"] = sys.path == before
        out["plugin_modules_intact"] = "hermes_plugins.zulip.adapter" in sys.modules
        # What a caller (adapter / cron delivery) reports:
        try:
            A._clear_caches()
            A._get_cached_client("https://zulip.example.test", "bot@example.test", "k" * 32)
            out["client_error"] = None
        except BaseException as e:
            out["client_error_type"] = type(e).__name__; out["client_error"] = str(e)
    elif action == "standalone_send":
        from types import SimpleNamespace
        try:
            res = asyncio.run(A._standalone_send(
                SimpleNamespace(extra={}), "614901", "Morning report body",
                thread_id="daily-morning-report"))
        except BaseException as e:
            res = {"raised": type(e).__name__ + ": " + str(e)}
        out["result"] = res
        out["requests"] = [s for s in sent if "messages" in s["url"]]
    print("RESULT=" + json.dumps(out, default=str))
    '''
)


def _run_child(python: str, action: str, tmp_path: Path, extra_env=None) -> dict:
    script = tmp_path / "child.py"
    script.write_text(_CHILD)
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(tmp_path),
        "HERMES_DATA_DIR": str(tmp_path / "data"),
        "ZULIP_SITE": "https://zulip.example.test",
        "ZULIP_EMAIL": "bot@example.test",
        "ZULIP_API_KEY": "k" * 32,
    }
    env.update(extra_env or {})
    proc = subprocess.run(
        [python, str(script), str(REPO_ROOT), str(STUBS_DIR), action],
        capture_output=True, text=True, env=env, timeout=120, cwd=str(tmp_path),
    )
    for line in proc.stdout.splitlines():
        if line.startswith("RESULT="):
            return json.loads(line[len("RESULT="):])
    raise AssertionError(f"child produced no result\nstdout={proc.stdout}\nstderr={proc.stderr}")


def _site_packages_with_sdk() -> Path:
    import importlib.util as iu
    for entry in sys.path:
        cand = Path(entry) / "zulip" / "__init__.py"
        if cand.is_file() and cand.parent != PLUGIN_DIR and "class Client" in cand.read_text(errors="ignore"):
            return Path(entry)
    pytest.skip("python-zulip-api SDK is not installed in the test interpreter")


def _hermes_runtime_python(tmp_path: Path, site_packages: Path) -> str:
    """Build an interpreter laid out like Hermes' bundled runtime.

    ``<tmp>/.hermes/tools/python-test/bin/python3`` with its own
    ``lib/pythonX.Y/site-packages`` (a venv over the base interpreter). The
    gateway and cron run from exactly this layout, which is the path the
    original SDK-import code special-cased.
    """
    root = tmp_path / ".hermes" / "tools" / "python-test"
    (root / "bin").mkdir(parents=True)
    ver = f"python{sys.version_info.major}.{sys.version_info.minor}"
    (root / "lib" / ver).mkdir(parents=True)
    os.symlink(site_packages, root / "lib" / ver / "site-packages")
    base_exe = Path(sys._base_executable if hasattr(sys, "_base_executable") else sys.executable)
    (root / "pyvenv.cfg").write_text(
        f"home = {base_exe.parent}\ninclude-system-site-packages = false\n"
        f"version = {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}\n"
    )
    exe = root / "bin" / "python3"
    os.symlink(base_exe, exe)
    return str(exe)


class TestFreshProcessSdkImport:
    def test_hermes_runtime_first_import_returns_the_sdk(self, tmp_path):
        """SCENARIO: a brand-new gateway/cron process on Hermes' bundled runtime
        imports the Zulip SDK for the first time (the live failure: the first
        call raised NameError 'original_path' from a finally block).
        EXPECTED: the real python-zulip-api module (with Client) is returned,
        no exception, sys.path is left as it was, the plugin's own modules stay
        loaded, and building a client works (no "not installed" error)."""
        py = _hermes_runtime_python(tmp_path, _site_packages_with_sdk())
        out = _run_child(py, "import", tmp_path)
        assert "exc_type" not in out, out
        assert out["has_client"] is True, out
        assert not str(out["sdk_file"]).startswith(str(PLUGIN_DIR)), out
        assert out["sys_path_unchanged"] is True
        assert out["plugin_modules_intact"] is True
        assert out["client_error"] is None, out

    def test_non_hermes_interpreter_first_import_returns_the_sdk(self, tmp_path):
        """SCENARIO: the plugin runs under an interpreter that is not Hermes'
        bundled runtime (e.g. a venv python running `hermes cron run`), with
        the SDK installed. The old code only looked under '.../hermes/tools/
        python' and reported the SDK missing.
        EXPECTED: the SDK is found and returned; no "not installed" error."""
        out = _run_child(sys.executable, "import", tmp_path)
        assert "exc_type" not in out, out
        assert out["has_client"] is True, out
        assert out["client_error"] is None, out

    def test_sdk_truly_absent_reports_not_installed(self, tmp_path):
        """SCENARIO: the zulip SDK is genuinely not installed in the runtime.
        EXPECTED: the import helper returns None (never raises) and the caller
        raises ImportError whose message says 'zulip package not installed' --
        a clear, actionable error, not NameError/UnboundLocalError."""
        empty = tmp_path / "empty-site"
        empty.mkdir()
        py = _hermes_runtime_python(tmp_path, empty)
        out = _run_child(py, "import", tmp_path)
        assert "exc_type" not in out, out
        assert out["returned_none"] is True
        assert out["client_error_type"] == "ImportError", out
        assert "zulip package not installed" in out["client_error"]

    def test_unimportable_sdk_reports_not_installed_not_nameerror(self, tmp_path):
        """SCENARIO: a zulip/__init__.py is present but cannot be imported
        (broken/partial install). In the old code the import failure path hit
        the same finally block and surfaced as NameError.
        EXPECTED: the helper returns None without raising, and the caller's
        error is the clear ImportError 'zulip package not installed'."""
        broken = tmp_path / "broken-site"
        (broken / "zulip").mkdir(parents=True)
        (broken / "zulip" / "__init__.py").write_text(
            "import a_dependency_that_does_not_exist_xyz\nclass Client: pass\n"
        )
        py = _hermes_runtime_python(tmp_path, broken)
        out = _run_child(py, "import", tmp_path)
        assert "exc_type" not in out, out
        assert out["returned_none"] is True
        assert out["client_error_type"] == "ImportError", out
        assert "zulip package not installed" in out["client_error"]


class TestCronStandaloneDeliveryFreshProcess:
    @pytest.mark.parametrize("layout", ["hermes_runtime", "plain_venv"])
    def test_cron_delivery_succeeds_with_sdk_installed(self, tmp_path, layout):
        """SCENARIO: cron job a724ee2dd303 (daily-morning-report) delivers to
        zulip:614901:daily-morning-report from a fresh process with no live
        adapter, so Hermes calls the plugin's standalone sender. Live it failed
        with 'delivery error: zulip package not installed' although the SDK
        was installed. (The network send is stubbed; nothing leaves the box.)
        EXPECTED: delivery reports success with the Zulip message id, exactly
        one message is posted to stream 614901, topic 'daily-morning-report',
        carrying the report body; no 'not installed' error."""
        sp = _site_packages_with_sdk()
        py = _hermes_runtime_python(tmp_path, sp) if layout == "hermes_runtime" else sys.executable
        out = _run_child(py, "standalone_send", tmp_path)
        res = out["result"]
        assert "not installed" not in json.dumps(res), res
        assert res.get("success") is True, res
        assert str(res.get("message_id")) == "4242"
        assert len(out["requests"]) == 1, out
        body = str(out["requests"][0]["data"])
        assert "614901" in body and "daily-morning-report" in body
        assert "Morning report body" in body


# ---------------------------------------------------------------------------
# In-process adapter harness (scenarios c, d, e)
# ---------------------------------------------------------------------------
class _FakeSecretScope:
    """Behavioral stand-in for Hermes' agent.secret_scope (multiplex mode).

    Like the real module: a context-local scope, set/reset tokens, and a
    fail-closed get_secret() that raises when multiplexing is on and no
    profile scope is bound."""

    class UnscopedSecretError(RuntimeError):
        pass

    def __init__(self):
        self._var = contextvars.ContextVar("scope", default=None)
        self.multiplex = True

    def is_multiplex_active(self):
        return self.multiplex

    def set_secret_scope(self, secrets, *, profile_home=None):
        return self._var.set(None if secrets is None else (dict(secrets), profile_home))

    def reset_secret_scope(self, token):
        self._var.reset(token)

    def current_secret_scope(self):
        b = self._var.get()
        return b[0] if b else None

    def current_secret_scope_home(self):
        b = self._var.get()
        return b[1] if b else None

    def get_secret(self, name, default=None):
        b = self._var.get()
        if b is None:
            if self.multiplex:
                raise self.UnscopedSecretError(
                    f"Hermes could not read this profile's {name} (unscoped)")
            return os.environ.get(name, default)
        return b[0].get(name, default)


@pytest.fixture
def hermes_multiplex(monkeypatch, tmp_path):
    fake = _FakeSecretScope()
    agent = ModuleType("agent")
    ss = ModuleType("agent.secret_scope")
    for name in ("is_multiplex_active", "set_secret_scope", "reset_secret_scope",
                 "current_secret_scope", "current_secret_scope_home", "get_secret"):
        setattr(ss, name, getattr(fake, name))
    ss.UnscopedSecretError = fake.UnscopedSecretError
    agent.secret_scope = ss
    constants = ModuleType("hermes_constants")
    constants.get_hermes_home = lambda: tmp_path / "profile-home"
    (tmp_path / "profile-home").mkdir()
    monkeypatch.setitem(sys.modules, "agent", agent)
    monkeypatch.setitem(sys.modules, "agent.secret_scope", ss)
    monkeypatch.setitem(sys.modules, "hermes_constants", constants)
    return fake


PROFILE_ENV = {
    "ZULIP_SITE": "https://smov.example.test",
    "ZULIP_EMAIL": "hermes-ai-agent-bot@example.test",
    "ZULIP_API_KEY": "a" * 32,
    "ZULIP_HOME_CHANNEL": "614901",
}


class _SdkClient:
    sent: list = []

    session = None

    def __init__(self, email=None, api_key=None, site=None, **kw):
        self.email, self.site = email, site

    def ensure_session(self):
        return None

    def send_message(self, request):
        _SdkClient.sent.append(dict(request))
        return {"result": "success", "id": 5151}

    def __getattr__(self, name):
        return lambda *a, **k: {"result": "success"}


@pytest.fixture
def fake_sdk(monkeypatch):
    sdk = ModuleType("fake_zulip_sdk")
    sdk.Client = _SdkClient
    _SdkClient.sent = []
    monkeypatch.setattr(adapter_module, "zulip", sdk)
    monkeypatch.setattr(adapter_module, "ZULIP_AVAILABLE", True)
    return _SdkClient


def _build_adapter_in_profile_scope(fake, mock_platform_config):
    """Construct the adapter the way the multiplexed gateway does: inside the
    ai-agent profile's secret scope."""
    token = fake.set_secret_scope(PROFILE_ENV, profile_home="/profiles/ai-agent")
    try:
        return adapter_module.ZulipAdapter(mock_platform_config)
    finally:
        fake.reset_secret_scope(token)


class TestInboundAfterReaction:
    @pytest.mark.asyncio
    async def test_mentioned_stream_message_reaches_agent(self, fake_sdk, mock_platform_config):
        """SCENARIO: Evan @-mentions the bot in stream 614901 / topic
        token-refresh. Live, the bot added 👀 and then the handler died with
        AttributeError: 'dict' object has no attribute 'put' (the display-name
        cache was overwritten by a legacy dict), so no reply ever came.
        EXPECTED: the message is handed to the agent exactly once, attributed
        to the sender's display name, in the right stream; the sender's name
        is remembered in the display-name cache."""
        a = adapter_module.ZulipAdapter(mock_platform_config)
        a.handle_message = AsyncMock()
        a._sdk_call = AsyncMock(return_value={"result": "success", "id": 1})
        msg = {
            "id": 630632040, "type": "stream", "stream_id": 614901,
            "subject": "token-refresh", "display_recipient": "general",
            "content": "Can we do this @**AI Agent**", "flags": ["mentioned"],
            "sender_email": "evan@example.test", "sender_full_name": "Evan Muir",
            "sender_id": 428945,
        }
        await a._handle_message(msg)
        a.handle_message.assert_awaited_once()
        event = a.handle_message.await_args.args[0]
        assert event.source.chat_id == "614901"
        assert event.source.user_name == "Evan Muir"
        assert "Can we do this" in event.text
        assert a._display_names.get(428945) == "Evan Muir"


class TestUnscopedOutboundUnderMultiplex:
    @pytest.mark.asyncio
    async def test_final_reply_sent_outside_turn_scope_is_delivered(
        self, hermes_multiplex, fake_sdk, mock_platform_config
    ):
        """SCENARIO: on the multiplexed gateway the core sends the final reply
        after the turn's profile scope has closed. Live this failed with
        "Hermes could not read this profile's ZULIP_BLOCK_SECRET_LEAKS" and the
        user never saw the answer.
        EXPECTED: the reply is delivered (success, Zulip message id), it is
        sent while the adapter's own profile scope is active (so settings
        resolve to the ai-agent profile), and afterwards the caller is left
        with no scope bound (nothing leaks into the caller's context)."""
        fake = hermes_multiplex
        a = _build_adapter_in_profile_scope(fake, mock_platform_config)
        seen_scope = []
        orig = _SdkClient.send_message

        def _record(self, request):
            seen_scope.append(fake.current_secret_scope())
            return orig(self, request)

        _SdkClient.send_message = _record
        try:
            assert fake.current_secret_scope() is None  # unscoped caller
            result = await a.send("614901:token-refresh", "Here is the answer.")
        finally:
            _SdkClient.send_message = orig
        assert result.success is True, result
        assert str(result.message_id) == "5151"
        assert len(fake_sdk.sent) == 1
        assert "Here is the answer." in fake_sdk.sent[0]["content"]
        assert fake_sdk.sent[0]["to"] == 614901
        assert seen_scope and seen_scope[0] is not None
        assert seen_scope[0]["ZULIP_EMAIL"] == PROFILE_ENV["ZULIP_EMAIL"]
        assert fake.current_secret_scope() is None

    def test_secret_leak_guard_fails_safe_when_unscoped(self, hermes_multiplex):
        """SCENARIO: the secret-leak guard's on/off flag is read where no
        profile scope is bound under multiplexing (the startup home-channel
        notice and post-turn sends hit this).
        EXPECTED: the guard reports enabled (True, its documented default,
        so credentials stay blocked) instead of raising and killing the send."""
        from zulip.secret_guard import block_secret_leaks_enabled
        assert hermes_multiplex.current_secret_scope() is None
        assert block_secret_leaks_enabled() is True

    def test_home_channel_comes_from_this_profile_not_process_env(
        self, hermes_multiplex, monkeypatch
    ):
        """SCENARIO: the gateway process environment belongs to the default
        profile (live: ZULIP_HOME_CHANNEL=dm_user:428945) while the ai-agent
        profile's own .env says ZULIP_HOME_CHANNEL=614901. Live, ai-agent's
        startup notice went to the default profile's DM target.
        EXPECTED: the ai-agent profile's home channel (614901) is used."""
        monkeypatch.setenv("ZULIP_HOME_CHANNEL", "dm_user:428945")
        token = hermes_multiplex.set_secret_scope(PROFILE_ENV, profile_home="/p/ai-agent")
        try:
            extra = adapter_module._env_enablement()
        finally:
            hermes_multiplex.reset_secret_scope(token)
        assert extra["home_channel"]["chat_id"] == "614901"


class TestLegacyDmUserTarget:
    def test_dm_user_target_parses_as_direct_message(self):
        """SCENARIO: home channels and cron delivery targets saved by earlier
        plugin versions use 'dm_user:<id>' (live: zulip:dm_user:428945 for the
        startup notice and paused cron job 7b72da9c670c). It was rejected with
        "Invalid stream ID in target: 'dm_user:428945'".
        EXPECTED: it means a direct message to user 428945, same as 'dm:428945'."""
        assert adapter_module._parse_target("dm_user:428945") == {
            "type": "dm", "user_ids": [428945]}
        assert adapter_module._parse_target("dm_user:428945") == adapter_module._parse_target("dm:428945")

    @pytest.mark.asyncio
    async def test_send_to_dm_user_target_delivers_private_message(
        self, fake_sdk, mock_platform_config
    ):
        """SCENARIO: the gateway sends its startup notice to 'dm_user:428945'.
        EXPECTED: one private message to user 428945 is sent and the send
        reports success."""
        a = adapter_module.ZulipAdapter(mock_platform_config)
        result = await a.send("dm_user:428945", "Gateway restarted")
        assert result.success is True, result
        assert len(fake_sdk.sent) == 1
        assert fake_sdk.sent[0]["type"] == "private"
        assert fake_sdk.sent[0]["to"] == [428945]


class TestStreamOverridesOversized:
    def test_oversized_overrides_are_ignored_not_crashing(self, monkeypatch):
        """SCENARIO: ZULIP_STREAM_OVERRIDES is larger than the allowed size.
        The size guard called a helper before it was defined
        (UnboundLocalError), which would crash stream gating for every
        inbound stream message.
        EXPECTED: overrides are ignored (empty mapping) with a warning; no
        exception."""
        big = json.dumps({f"stream-{i}": {"chatmode": "onmessage"} for i in range(20000)})
        monkeypatch.setenv("ZULIP_STREAM_OVERRIDES", big)
        assert adapter_module._resolve_stream_overrides() == {}
