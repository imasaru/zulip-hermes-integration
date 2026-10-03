"""Auto-patch Hermes workspaces to add 'zulip' to _KNOWN_DELIVERY_PLATFORMS.

This module is called by the plugin's ``register()`` function to ensure cron
delivery to Zulip survives Hermes upgrades that reset workspace copies.

Usage as a standalone CLI:
    python -m zulip.cron_patch

Works by finding all workspace copies and patching scheduler_delivery.py
if not already patched. Idempotent — safe to run multiple times.
"""

import glob
import re
import sys


def patch_scheduler_delivery(filepath: str) -> bool:
    """Add 'zulip' to _KNOWN_DELIVERY_PLATFORMS if not already present.

    Returns True if the file was patched, False if already patched or
    if the file could not be found/modified.
    """
    try:
        with open(filepath, 'r') as f:
            content = f.read()
    except OSError:
        print(f"ERROR: Could not read {filepath}")
        return False

    # Check if already patched
    if '"zulip"' in content or "'zulip'" in content:
        return False  # Already patched

    # Find the _KNOWN_DELIVERY_PLATFORMS definition
    # Handles both frozenset([...]) and frozenset({...})
    pattern = r"(_KNOWN_DELIVERY_PLATFORMS\s*=\s*frozenset\(\[)"
    match = re.search(pattern, content, re.DOTALL)

    if not match:
        # Try with curly braces
        pattern = r"(_KNOWN_DELIVERY_PLATFORMS\s*=\s*frozenset\(\{)"
        match = re.search(pattern, content, re.DOTALL)

    if not match:
        print(f"ERROR: Could not find _KNOWN_DELIVERY_PLATFORMS in {filepath}")
        return False

    # Find the closing bracket/brace
    opener = match.group(1)
    start_idx = match.end()

    if '[' in opener:
        closer = ']'
    else:
        closer = '}'

    # Find the matching closer
    depth = 1
    idx = start_idx
    while idx < len(content) and depth > 0:
        if content[idx] == opener[1]:  # opener[1] is '[' or '{'
            depth += 1
        elif content[idx] == closer:
            depth -= 1
        idx += 1

    if depth != 0:
        print(f"ERROR: Could not find closing {closer} in {filepath}")
        return False

    # idx now points past the closing bracket
    before = content[:match.start()]
    middle = content[start_idx:idx - 1]  # content between opener and closer
    after = content[idx:]

    # Strip trailing comma and whitespace from the last element
    middle = middle.rstrip()
    if middle.endswith(','):
        middle = middle[:-1].rstrip()

    # Add 'zulip' to the list
    new_middle = middle + ",\n        'zulip',\n    "
    new_content = before + opener + new_middle + closer + after

    try:
        with open(filepath, 'w') as f:
            f.write(new_content)
    except OSError:
        print(f"ERROR: Could not write {filepath}")
        return False

    return True


def find_workspaces() -> list[str]:
    """Find all workspace copies of scheduler_delivery.py."""
    workspace_pattern = (
        "/root/.hermes/installs/*/environments/*/workspace/cron/"
        "scheduler_delivery.py"
    )
    return glob.glob(workspace_pattern)


def main() -> None:
    """CLI entry point — finds and patches all workspace copies."""
    files = find_workspaces()

    if not files:
        print("WARNING: No workspace copies found. Is Hermes installed?")
        sys.exit(1)

    patched = 0
    skipped = 0

    for filepath in sorted(files):
        if patch_scheduler_delivery(filepath):
            print(f"Patched: {filepath}")
            patched += 1
        else:
            print(f"Already patched: {filepath}")
            skipped += 1

    print(f"\nSummary: {patched} patched, {skipped} already up-to-date")

    if patched > 0:
        print("\nNOTE: Restart the Hermes gateway for changes to take effect:")
        print("  hermes -p ai-agent gateway restart")


if __name__ == "__main__":
    main()
