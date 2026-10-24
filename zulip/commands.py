"""Admin command framework for Zulip Hermes integration.

Commands are parsed BEFORE messages reach the AI agent.
Unknown commands fall through to the AI.

Gateway-native slash commands (``/help``, ``/status``, ``/model``, ``/stop``,
``/new``, ...) are deliberately **not** registered here: the Hermes gateway
implements them natively, so registering them in the plugin would shadow the
gateway and swallow the command (issue #190). Only plugin-specific commands
are registered below; everything else falls through to the gateway.
"""

from __future__ import annotations

import re
from typing import Callable
from dataclasses import dataclass

from .logger import mask_pii

logger = __import__("logging").getLogger(__name__)


# The host's own exec-approval decision commands. Recognised (not consumed) so
# the plugin can record *who* decided an approval (issue #222): the gateway
# resolves the approval from this message and its ``post_approval_response``
# hook carries no human identity on the gateway surface. ``/approvals`` — the
# command that shows or sets the approval *mode* — must not match, so the
# pattern requires a word boundary after the verb.
_APPROVAL_DECISION_RE = re.compile(r"^/(?:approve|deny)\b", re.IGNORECASE)


_YOLO_RE = re.compile(r"^/yolo\b", re.IGNORECASE)

# A bot mention may lead the command (``@**Bot** /cmd args``) or trail it
# (``/cmd args @**Bot**``). ``strip_html_to_text`` reduces ``@**Bot**`` to
# ``@Bot`` before this runs, so both the markup and the bare form are accepted.
# The label may contain spaces (``@Home Manager``) and a Zulip ``|id`` suffix.
_MARKUP_MENTION = r"_?\*\*[^*|]+?(?:\|\d+)?\*\*"
_PLAIN_MENTION = r"[\w][\w.-]*(?:\s+[\w][\w.-]*)*(?:\|\d+)?"
_MENTION_NAME = rf"(?:{_MARKUP_MENTION}|{_PLAIN_MENTION})"
_LEADING_COMMAND_RE = re.compile(
    rf"^@(?P<name>{_MENTION_NAME})\s+(?P<body>/[^\s].*)$",
    re.DOTALL,
)
_TRAILING_COMMAND_RE = re.compile(
    rf"^(?P<body>/[^\s]+(?:\s+.*?)?)\s+@(?P<name>{_MENTION_NAME})\s*$",
    re.DOTALL,
)


def _mention_label(name: str) -> str:
    """Display name inside one mention token, without markup or ``|id``."""
    text = (name or "").strip()
    if text.startswith("_**") and text.endswith("**"):
        text = text[3:-2]
    elif text.startswith("**") and text.endswith("**"):
        text = text[2:-2]
    if "|" in text:
        text = text.split("|", 1)[0]
    return text.strip()


def _command_body(content: str) -> str:
    """Command text with a leading ``@bot`` removed so the verb is first.

    A trailing mention is left in place: ``/cmd args`` already parses, and
    the argument may be a person rather than a target bot. Stream routing
    strips a trailing bot mention itself once it knows the name is a bot.
    """
    text = (content or "").strip()
    if not text:
        return ""
    lead = _LEADING_COMMAND_RE.match(text)
    if lead:
        return lead.group("body").strip()
    return text


def split_command_target(content: str) -> tuple[str, str | None] | None:
    """Split a slash command into ``(bare_command, mention_label_or_None)``.

    ``None`` when ``content`` is not a command. The bare command has a leading
    or trailing mention removed so ``@**Bot** /cmd args`` and
    ``/cmd args @**Bot**`` both yield ``/cmd args``. Callers that do not know
    whether the label is a bot should keep the original text for a trailing
    mention — see :func:`_command_body`.
    """
    text = (content or "").strip()
    if not text:
        return None
    lead = _LEADING_COMMAND_RE.match(text)
    if lead:
        return lead.group("body").strip(), _mention_label(lead.group("name"))
    if not text.startswith("/"):
        return None
    trail = _TRAILING_COMMAND_RE.match(text)
    if trail:
        return trail.group("body").strip(), _mention_label(trail.group("name"))
    return text, None


def is_yolo_command(content: str) -> bool:
    """Whether ``content`` is the host's ``/yolo`` toggle.

    The gateway owns ``/yolo`` (it toggles the dangerous-command approval
    bypass for one session). It is recognised here — never consumed — only
    so the plugin can route it to the relevant profile instead of all six
    adapters answering at once.
    """
    text = _command_body(content)
    if not text:
        return False
    return bool(_YOLO_RE.match(text))


def is_approval_decision(content: str) -> bool:
    """Whether ``content`` is the host's ``/approve`` / ``/deny`` command.

    Covers the modifiers the host accepts (``/approve session``, ``/approve
    always``, ``/approve all``, ``/deny all``). The message is never consumed or
    answered here: it still reaches the gateway, which owns slash authorization
    and the decision itself.
    """
    text = _command_body(content)
    if not text:
        return False
    return bool(_APPROVAL_DECISION_RE.match(text))

CommandHandler = Callable[[str, str, str, str], str]

_COMMANDS: dict[str, CommandHandler] = {}


@dataclass
class CommandResult:
    """Result of command parsing."""

    handled: bool
    reply: str = ""


def register_command(name: str, handler: CommandHandler | None = None):
    """Register a command handler.

    Can be used as a decorator:
        @register_command("ping")
        def _cmd_ping(args, chat_id, sender_email, sender_name) -> str:
            return "🏓 Pong!"

    Do not register a name the gateway owns (``/help``, ``/status``, ``/model``,
    ...); that shadows the gateway command (issue #190).
    """

    def decorator(func: CommandHandler) -> CommandHandler:
        _COMMANDS[name] = func
        return func

    if handler is not None:
        _COMMANDS[name] = handler
        return handler
    return decorator


def _extract_command(content: str) -> tuple[str, str] | None:
    """Parse \"/command arg1 arg2\" from message text.

    A leading ``@bot`` is not part of the verb: ``@**Bot** /command args``
    parses as ``command``. Returns (cmd, args) or None if not a command.
    """
    stripped = _command_body(content)
    if stripped.startswith("/"):
        parts = stripped[1:].split(maxsplit=1)
        if not parts or not parts[0]:
            return None
        cmd = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ""
        return cmd, args
    return None


def handle_command(
    content: str,
    chat_id: str,
    sender_email: str,
    sender_name: str,
    version: str = "unknown",
) -> CommandResult:
    """Try to parse and execute a command.

    Returns CommandResult(handled=True, reply=...) if a known command matched.
    Returns CommandResult(handled=False) to fall through to AI agent.
    """
    parsed = _extract_command(content)
    if not parsed:
        return CommandResult(handled=False)

    cmd, args = parsed
    handler = _COMMANDS.get(cmd)
    if handler is None:
        return CommandResult(handled=False)

    try:
        reply = handler(args, chat_id, sender_email, sender_name)
        return CommandResult(handled=True, reply=reply)
    except Exception as e:
        logger.warning("command error [cmd=%s sender=%s]: %s", cmd, mask_pii(sender_email), e)
        return CommandResult(handled=True, reply=f"❌ Error processing /{cmd}. Please try again later.")


def is_command(content: str) -> bool:
    """Check if content looks like a command (starts with /, or ``@bot /cmd``)."""
    return _command_body(content).startswith("/")


# ------------------------------------------------------------------
# Plugin-specific commands
#
# Nothing here may share a name with a gateway-native command. The gateway
# owns /help, /status, /model, /stop, /new, /reset, /version, ...; those must
# reach it, not be absorbed by this router (issue #190).
# ------------------------------------------------------------------

@register_command("streams")
def _cmd_streams(
    args: str, chat_id: str, sender_email: str, sender_name: str
) -> str:
    """List streams the bot can see.

    Usage: /streams [--all]
    """
    return (
        "Stream management is available through the AI agent.\n"
        "Ask the bot to list, create, or manage streams "
        "using natural language."
    )


@register_command("user")
def _cmd_user(
    args: str, chat_id: str, sender_email: str, sender_name: str
) -> str:
    """Get user information.

    Usage: /user <email_or_id>
    """
    if not args:
        return "Usage: `/user <email_or_id>`"
    return (
        "User information is available through the AI agent.\n"
        f"Ask the bot about user `{args.strip()}`."
    )


@register_command("pin")
def _cmd_pin(
    args: str, chat_id: str, sender_email: str, sender_name: str
) -> str:
    """Star/pin a message.

    Usage: /pin <message_id>
    """
    if not args:
        return "Usage: `/pin <message_id>`"
    return (
        "Message pinning is available through the AI agent.\n"
        f"Ask the bot to pin message `{args.strip()}`."
    )


@register_command("unpin")
def _cmd_unpin(
    args: str, chat_id: str, sender_email: str, sender_name: str
) -> str:
    """Unstar/unpin a message.

    Usage: /unpin <message_id>
    """
    if not args:
        return "Usage: `/unpin <message_id>`"
    return (
        "Message unpinning is available through the AI agent.\n"
        f"Ask the bot to unpin message `{args.strip()}`."
    )
