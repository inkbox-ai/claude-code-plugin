"""Command-line entry points: setup, run, start/stop/status/restart, doctor, whoami."""

from __future__ import annotations

import argparse
import json
import os
import sys

try:
    from . import daemon
    from .bootstrap import bootstrap
    from .config import inkbox_client_kwargs, read_config
    from .doctor import print_doctor
    from .setup_wizard import interactive_setup
except ImportError:  # pragma: no cover - direct local import/test fallback
    import daemon
    from bootstrap import bootstrap
    from config import inkbox_client_kwargs, read_config
    from doctor import print_doctor
    from setup_wizard import interactive_setup


def _cmd_whoami() -> int:
    cfg = read_config()
    if not cfg.api_key or not cfg.identity:
        print("INKBOX_API_KEY / INKBOX_IDENTITY not set — run doctor first.")
        return 1
    from inkbox import Inkbox

    identity = Inkbox(**inkbox_client_kwargs(cfg.api_key, cfg.base_url)).get_identity(cfg.identity)
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
    bootstrap_parser = sub.add_parser("bootstrap", help="configure an existing identity without prompts")
    bootstrap_parser.add_argument("--identity", required=True)
    bootstrap_parser.add_argument("--api-key-stdin", action="store_true")
    bootstrap_parser.add_argument("--base-url", default="")
    bootstrap_parser.add_argument("--project-dir", default="")
    bootstrap_parser.add_argument("--voice-ai", action="store_true")
    bootstrap_parser.add_argument("--voice-ai-instructions-file")
    bootstrap_parser.add_argument("--rotate-signing-key", action="store_true")
    bootstrap_parser.add_argument("--start-gateway", action="store_true")
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
    if args.command == "bootstrap":
        api_key = sys.stdin.read().strip() if args.api_key_stdin else os.getenv("INKBOX_API_KEY", "").strip()
        instructions = None
        if args.voice_ai_instructions_file:
            instructions = open(args.voice_ai_instructions_file, encoding="utf-8").read()
        result = bootstrap(
            identity_handle=args.identity,
            api_key=api_key,
            base_url=args.base_url,
            project_dir=args.project_dir,
            voice_ai=args.voice_ai,
            voice_ai_instructions=instructions,
            rotate_signing_key=args.rotate_signing_key,
            start_gateway=args.start_gateway,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result.get("status") == "configured" else 2
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
