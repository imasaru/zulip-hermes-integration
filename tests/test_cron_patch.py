"""Tests for zulip.cron_patch — auto-patching _KNOWN_DELIVERY_PLATFORMS.

Verifies the patch script correctly adds 'zulip' to the frozenset in
Hermes scheduler_delivery.py across workspaces.
"""

import os
import tempfile
import shutil

from zulip.cron_patch import patch_scheduler_delivery, find_workspaces


class TestPatchSchedulerDelivery:
    """Test the patching function in isolation."""

    def setup_method(self):
        """Create a temp directory for each test."""
        self.tmpdir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up temp directory."""
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_file(self, content):
        """Write content to a temp scheduler_delivery.py."""
        filepath = os.path.join(self.tmpdir, 'scheduler_delivery.py')
        with open(filepath, 'w') as f:
            f.write(content)
        return filepath

    def test_adds_zulip_when_missing_brackets(self):
        """Script must add 'zulip' to _KNOWN_DELIVERY_PLATFORMS with []."""
        content = '''
_KNOWN_DELIVERY_PLATFORMS = frozenset([
    "telegram", "discord", "slack",
])
'''
        filepath = self._write_file(content)
        result = patch_scheduler_delivery(filepath)

        assert result is True, "Must return True when patching"
        with open(filepath) as f:
            patched = f.read()

        assert "'zulip'" in patched or '"zulip"' in patched, (
            "Must contain 'zulip' in the patched file"
        )

    def test_adds_zulip_when_missing_braces(self):
        """Script must add 'zulip' to _KNOWN_DELIVERY_PLATFORMS with {}."""
        content = '''
_KNOWN_DELIVERY_PLATFORMS = frozenset({
    "telegram", "discord",
})
'''
        filepath = self._write_file(content)
        result = patch_scheduler_delivery(filepath)

        assert result is True, "Must return True when patching"
        with open(filepath) as f:
            patched = f.read()

        assert "'zulip'" in patched or '"zulip"' in patched, (
            "Must contain 'zulip' in the patched file"
        )

    def test_skips_when_zulip_already_present(self):
        """Script must not re-patch if zulip is already there."""
        content = '''
_KNOWN_DELIVERY_PLATFORMS = frozenset({
    "telegram", "zulip",
})
'''
        filepath = self._write_file(content)
        result = patch_scheduler_delivery(filepath)

        assert result is False, "Must return False when already patched"

    def test_preserves_existing_platforms(self):
        """Patching must not remove existing platforms."""
        content = '''
_KNOWN_DELIVERY_PLATFORMS = frozenset({
    "telegram", "discord", "slack", "signal",
})
'''
        filepath = self._write_file(content)
        patch_scheduler_delivery(filepath)

        with open(filepath) as f:
            patched = f.read()

        for platform in ['telegram', 'discord', 'slack', 'signal']:
            assert platform in patched, f"Must preserve {platform}"

    def test_preserves_file_structure(self):
        """Patching must not corrupt the file."""
        content = '''
_KNOWN_DELIVERY_PLATFORMS = frozenset({
    "telegram", "discord",
})

def _is_known_delivery_platform(platform):
    return platform.lower() in _KNOWN_DELIVERY_PLATFORMS
'''
        filepath = self._write_file(content)
        result = patch_scheduler_delivery(filepath)

        assert result is True
        with open(filepath) as f:
            patched = f.read()

        # Must still be valid Python
        try:
            compile(patched, filepath, 'exec')
        except SyntaxError as e:
            assert False, f"Patched file has syntax error: {e}"

        # Must preserve the function
        assert '_is_known_delivery_platform' in patched

    def test_handles_trailing_comma(self):
        """Patching must handle files with trailing commas."""
        content = '''
_KNOWN_DELIVERY_PLATFORMS = frozenset({
    "telegram", "discord",
})
'''
        filepath = self._write_file(content)
        result = patch_scheduler_delivery(filepath)

        assert result is True
        with open(filepath) as f:
            patched = f.read()

        # Must be valid Python (no double commas)
        try:
            compile(patched, filepath, 'exec')
        except SyntaxError as e:
            assert False, f"Patched file has syntax error: {e}"

    def test_returns_false_on_missing_file(self):
        """Script must return False for non-existent files."""
        result = patch_scheduler_delivery('/nonexistent/path.py')
        assert result is False

    def test_returns_false_on_missing_frozenset(self):
        """Script must return False if frozenset is not found."""
        content = '''
# No _KNOWN_DELIVERY_PLATFORMS defined
some_other_variable = 42
'''
        filepath = self._write_file(content)
        result = patch_scheduler_delivery(filepath)
        assert result is False


class TestFindWorkspaces:
    """Test workspace discovery."""

    def test_returns_list_of_paths(self):
        """Script must return a list of file paths."""
        workspaces = find_workspaces()
        assert isinstance(workspaces, list)

    def test_paths_end_with_scheduler_delivery_py(self):
        """All paths must end with scheduler_delivery.py."""
        workspaces = find_workspaces()
        for ws in workspaces:
            assert ws.endswith('scheduler_delivery.py'), (
                f"Workspace path must end with scheduler_delivery.py: {ws}"
            )

    def test_workspaces_are_real_files(self):
        """All found workspaces must be real files."""
        workspaces = find_workspaces()
        for ws in workspaces:
            assert os.path.isfile(ws), f"Workspace must be a file: {ws}"
