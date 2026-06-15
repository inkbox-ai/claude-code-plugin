"""Command-line entry points: setup, run, start/stop/status/restart, doctor, whoami."""

from __future__ import annotations

import argparse
import sys

try:
    from . import daemon
    from .config import read_config
    from .doctor import print_doctor
    from .setup_wizard import interactive_setup
except ImportError:  # pragma: no cover - direct local import/test fallback
    import daemon
    from config import read_config
    from doctor import print_doctor
    from setup_wizard import interactive_setup


def _cmd_whoami() -> int:
    cfg = read_config()
    if not cfg.api_key or not cfg.identity:
        print("INKBOX_API_KEY / INKBOX_IDENTITY not set — run doctor first.")
        return 1
    from inkbox import Inkbox

    identity = Inkbox(api_key=cfg.api_key, base_url=cfg.base_url).get_identity(cfg.identity)
    mailbox = getattr(identity, "mailbox", None)
    phone = getattr(identity, "phone_number", None)
    print(f"handle:   {identity.agent_handle}")
    print(f"email:    {getattr(mailbox, 'email_address', None) or '-'}")
    print(f"phone:    {getattr(phone, 'number', None) or '-'}")
    print(f"imessage: {'enabled' if getattr(identity, 'imessage_enabled', False) else 'disabled'}")
    print(f"project:  {cfg.project_dir}")
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI dispatcher.

    Args:
        argv (list[str] | None): Argument vector; defaults to sys.argv[1:].

    Returns:
        int: Process exit code.
    """
    parser = argparse.ArgumentParser(
        prog="inkbox-claude",
        description="Talk to Claude Code over email, SMS, iMessage, and voice via Inkbox.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("setup", help="run the interactive setup wizard")
    sub.add_parser("run", help="run the bridge gateway in the foreground")
    sub.add_parser("start", help="start the bridge gateway in the background")
    sub.add_parser("stop", help="stop the background bridge gateway")
    sub.add_parser("restart", help="restart the background bridge gateway")
    sub.add_parser("status", help="show whether the background gateway is running")
    uninstall_parser = sub.add_parser("uninstall", help="remove the background service and launcher")
    uninstall_parser.add_argument(
        "--purge", action="store_true",
        help="also delete config, logs, and sessions in ~/.inkbox-claude",
    )
    sub.add_parser("doctor", help="check configuration and dependencies")
    sub.add_parser("whoami", help="show the bridged Inkbox identity")

    args = parser.parse_args(argv)
    if args.command == "setup":
        interactive_setup()
        return 0
    if args.command == "run":
        return daemon.run_foreground()
    if args.command == "start":
        return daemon.start()
    if args.command == "stop":
        return daemon.stop()
    if args.command == "restart":
        return daemon.restart()
    if args.command == "status":
        return daemon.status()
    if args.command == "uninstall":
        return daemon.uninstall(purge=getattr(args, "purge", False))
    if args.command == "doctor":
        return print_doctor()
    if args.command == "whoami":
        return _cmd_whoami()
    return 2


if __name__ == "__main__":
    sys.exit(main())
