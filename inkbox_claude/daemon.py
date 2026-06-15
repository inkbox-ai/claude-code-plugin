"""Run the bridge gateway in the foreground or as a background daemon.

`inkbox-claude run` stays in the foreground (what systemd/Docker/debugging
want). `start`/`stop`/`status`/`restart` manage a detached background
process with a PID file and a log file under ``~/.inkbox-claude/`` — the
same shape as `hermes gateway start`/`stop`.
"""

from __future__ import annotations

import asyncio
import logging
import os
import platform
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

try:
    from .config import read_config
    from .gateway import InkboxGateway
except ImportError:  # pragma: no cover - direct local import/test fallback
    from config import read_config
    from gateway import InkboxGateway


def _state_dir() -> Path:
    root = Path(os.getenv("INKBOX_CLAUDE_HOME") or Path.home() / ".inkbox-claude")
    root.mkdir(parents=True, exist_ok=True)
    return root


def _pid_file() -> Path:
    return _state_dir() / "gateway.pid"


def _log_file() -> Path:
    return _state_dir() / "gateway.log"


def _read_pid() -> int | None:
    """Return the PID of a live daemon, or None (clearing a stale PID file).

    Returns:
        int | None: The running gateway's PID, or None if not running.
    """
    try:
        pid = int(_pid_file().read_text().strip())
    except (FileNotFoundError, ValueError):
        return None
    try:
        os.kill(pid, 0)  # signal 0 just probes liveness
    except OSError:
        _pid_file().unlink(missing_ok=True)  # stale — process is gone
        return None
    return pid


def _maybe_load_env_file() -> None:
    """Fill missing config from a ``.env`` file so the daemon just works.

    Loads the first that exists — ``$INKBOX_CLAUDE_ENV_FILE``, then ``./.env``,
    then ``~/.inkbox-claude/.env`` (where the installer writes it for a global
    install) — and sets any vars not already in the environment (real env wins).

    Returns:
        None
    """
    candidates = []
    explicit = os.getenv("INKBOX_CLAUDE_ENV_FILE")
    if explicit:
        candidates.append(Path(explicit))
    candidates.append(Path.cwd() / ".env")
    candidates.append(_state_dir() / ".env")

    path = next((p for p in candidates if p.exists()), None)
    if path is None:
        return
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        if stripped.startswith("export "):
            stripped = stripped[len("export "):]
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def run_foreground() -> int:
    """Run the gateway in the foreground until interrupted.

    Returns:
        int: Process exit code.
    """
    _maybe_load_env_file()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Make SIGTERM (how `stop` and service managers ask us to quit) unwind the
    # same graceful path as Ctrl+C, so the tunnel and webhook server close.
    if os.name == "posix":
        signal.signal(signal.SIGTERM, signal.default_int_handler)

    gateway = InkboxGateway(read_config())
    try:
        asyncio.run(gateway.run())
    except KeyboardInterrupt:
        print("bye")
    return 0


def start() -> int:
    """Start the gateway as a detached background process.

    Returns:
        int: 0 on success, 1 on failure.
    """
    if os.name != "posix":
        print("Background mode needs a POSIX system. Use `inkbox-claude run` (or a service manager).")
        return 1

    existing = _read_pid()
    if existing:
        print(f"Already running (pid {existing}). Logs: {_log_file()}")
        return 0

    # Validate config in the foreground so misconfig fails loudly here rather
    # than silently in the detached child.
    _maybe_load_env_file()
    cfg = read_config()
    if not cfg.api_key or not cfg.identity:
        print("INKBOX_API_KEY and INKBOX_IDENTITY are not set — run `inkbox-claude setup` first.")
        return 1

    log_path = _log_file()
    pid = os.fork()
    if pid > 0:
        # Parent: record the daemon's PID, then give it a moment to fail fast
        # (bad tunnel, 401, ...) so we can surface that instead of "started".
        _pid_file().write_text(f"{pid}\n")
        time.sleep(1.5)
        if _read_pid() != pid:
            print("Gateway exited right after starting — check the log:")
            print(f"  {log_path}")
            return 1
        print(f"inkbox-claude gateway started in the background (pid {pid}).")
        print(f"  logs:  {log_path}")
        print(f"  tail:  tail -f {log_path}")
        print("  stop:  inkbox-claude stop")
        return 0

    # Child: detach from the terminal and run the gateway.
    os.setsid()
    _redirect_stdio(log_path)
    run_foreground()
    os._exit(0)


def _redirect_stdio(log_path: Path) -> None:
    sys.stdout.flush()
    sys.stderr.flush()
    with open(os.devnull, "rb") as devnull:
        os.dup2(devnull.fileno(), sys.stdin.fileno())
    logf = open(log_path, "a", buffering=1)  # line-buffered so `tail -f` is live
    os.dup2(logf.fileno(), sys.stdout.fileno())
    os.dup2(logf.fileno(), sys.stderr.fileno())


def stop() -> int:
    """Stop the background gateway, escalating to SIGKILL if it lingers.

    Returns:
        int: 0 on success (or already stopped), 1 if the signal failed.
    """
    pid = _read_pid()
    if not pid:
        print("Not running.")
        return 0
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError as exc:
        print(f"Could not signal pid {pid}: {exc}")
        return 1

    # Wait up to ~5s for a graceful exit before forcing it.
    for _ in range(50):
        time.sleep(0.1)
        try:
            os.kill(pid, 0)
        except OSError:
            break
    else:
        print(f"Did not stop after 5s — sending SIGKILL to {pid}.")
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass

    _pid_file().unlink(missing_ok=True)
    print("Stopped.")
    return 0


def status() -> int:
    """Report whether the background gateway is running.

    Returns:
        int: 0 if running, 1 if not.
    """
    pid = _read_pid()
    if pid:
        print(f"running (pid {pid})")
        print(f"  logs: {_log_file()}")
        return 0
    print("not running")
    return 1


def restart() -> int:
    """Stop the background gateway if running, then start a fresh one.

    Returns:
        int: Exit code from :func:`start`.
    """
    stop()
    return start()


# ----------------------------------------------------------------------
# Boot / login autostart (systemd user service or launchd agent)
# ----------------------------------------------------------------------

SERVICE_NAME = "inkbox-claude"
LAUNCHD_LABEL = "ai.inkbox.claude"


def _launcher_path() -> str:
    """Absolute path to the `inkbox-claude` console script.

    Returns:
        str: Path to the launcher (or the bare name as a last resort).
    """
    sibling = Path(sys.executable).with_name("inkbox-claude")
    if sibling.exists():
        return str(sibling)
    return shutil.which("inkbox-claude") or "inkbox-claude"


def install_autostart(env_file: str) -> bool:
    """Install and enable a service that runs the gateway on boot/login.

    Args:
        env_file (str): Absolute path to the .env the service should load.

    Returns:
        bool: True if the service was installed and enabled, else False.
    """
    exe = _launcher_path()
    system = platform.system()
    if system == "Linux":
        return _install_systemd_user(exe, env_file)
    if system == "Darwin":
        return _install_launchd(exe, env_file)
    print(f"  Boot autostart isn't supported on {system}. Use `inkbox-claude start` instead.")
    return False


def _install_systemd_user(exe: str, env_file: str) -> bool:
    unit_dir = Path.home() / ".config" / "systemd" / "user"
    unit_dir.mkdir(parents=True, exist_ok=True)
    unit = unit_dir / f"{SERVICE_NAME}.service"
    unit.write_text(
        "[Unit]\n"
        "Description=Inkbox bridge for Claude Code\n"
        "After=network-online.target\n"
        "Wants=network-online.target\n\n"
        "[Service]\n"
        "Type=simple\n"
        f"Environment=INKBOX_CLAUDE_ENV_FILE={env_file}\n"
        f"ExecStart={exe} run\n"
        "Restart=on-failure\n"
        "RestartSec=5\n\n"
        "[Install]\n"
        "WantedBy=default.target\n"
    )
    print(f"  Wrote {unit}")

    # systemd will own the gateway now — stop any fork-based one first.
    if _read_pid():
        stop()

    user = os.environ.get("USER") or os.environ.get("LOGNAME") or ""
    subprocess.run(["systemctl", "--user", "daemon-reload"], capture_output=True, text=True)
    enabled = subprocess.run(
        ["systemctl", "--user", "enable", "--now", f"{SERVICE_NAME}.service"],
        capture_output=True, text=True,
    )
    if enabled.returncode == 0:
        # enable-linger keeps user services alive across logout / on boot.
        linger = subprocess.run(["loginctl", "enable-linger", user], capture_output=True, text=True)
        print("  Enabled — the bridge is running now and will start on boot.")
        if linger.returncode != 0:
            print(f"  To keep it running while logged out: sudo loginctl enable-linger {user or '$USER'}")
        print(f"  Manage it: systemctl --user status|restart|stop {SERVICE_NAME}")
        return True

    detail = (enabled.stderr or "").strip().splitlines()
    print("  Could not enable the systemd user service automatically.")
    if detail:
        print(f"    {detail[-1]}")
    print("  The unit is written — enable it once a user session exists:")
    print(f"    loginctl enable-linger {user or '$USER'}")
    print("    systemctl --user daemon-reload")
    print(f"    systemctl --user enable --now {SERVICE_NAME}.service")
    return False


def _install_launchd(exe: str, env_file: str) -> bool:
    plist_dir = Path.home() / "Library" / "LaunchAgents"
    plist_dir.mkdir(parents=True, exist_ok=True)
    plist = plist_dir / f"{LAUNCHD_LABEL}.plist"
    log = _log_file()
    plist.write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" '
        '"http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n'
        '<plist version="1.0">\n<dict>\n'
        f"  <key>Label</key><string>{LAUNCHD_LABEL}</string>\n"
        "  <key>ProgramArguments</key>\n"
        f"  <array><string>{exe}</string><string>run</string></array>\n"
        "  <key>EnvironmentVariables</key>\n"
        f"  <dict><key>INKBOX_CLAUDE_ENV_FILE</key><string>{env_file}</string></dict>\n"
        "  <key>RunAtLoad</key><true/>\n  <key>KeepAlive</key><true/>\n"
        f"  <key>StandardOutPath</key><string>{log}</string>\n"
        f"  <key>StandardErrorPath</key><string>{log}</string>\n"
        "</dict>\n</plist>\n"
    )
    print(f"  Wrote {plist}")

    if _read_pid():
        stop()
    subprocess.run(["launchctl", "unload", str(plist)], capture_output=True, text=True)
    loaded = subprocess.run(["launchctl", "load", "-w", str(plist)], capture_output=True, text=True)
    if loaded.returncode == 0:
        print("  Loaded — the bridge is running now and will start at login.")
        print(f"  Manage it: launchctl unload/load {plist}")
        return True
    print("  Could not load the launchd agent automatically.")
    detail = (loaded.stderr or "").strip()
    if detail:
        print(f"    {detail}")
    print(f"  Load it yourself: launchctl load -w {plist}")
    return False
