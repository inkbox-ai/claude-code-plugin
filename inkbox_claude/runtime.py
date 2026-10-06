"""Native host ownership and positive saved-answer recovery evidence."""

from __future__ import annotations

import json
from pathlib import Path

import psutil


def process_identity(client):
    process = getattr(getattr(client, "_transport", None), "_process", None)
    pid = getattr(process, "pid", None)
    if type(pid) is not int:
        return None
    try:
        owner = psutil.Process(pid)
        return {"pid": pid, "created": owner.create_time(), "children": [
            {"pid": child.pid, "created": child.create_time()}
            for child in owner.children(recursive=True)
        ]}
    except psutil.Error:
        return None


def fence_process(identity):
    """Fence a still-observable owner; an old orphan snapshot is not proof."""
    if not identity:
        return False
    processes = {}
    try:
        parent = psutil.Process(identity["pid"])
        if parent.create_time() != identity["created"]:
            return False
        # Freeze the parent and descendants while discovering them, preventing
        # a normal tool child from forking between discovery and termination.
        parent.suspend()
        processes[parent.pid] = parent
        for entry in identity.get("children", []):
            try:
                child = psutil.Process(entry["pid"])
                if child.create_time() == entry["created"]:
                    child.suspend()
                    processes[child.pid] = child
            except psutil.NoSuchProcess:
                continue
        for _ in range(16):
            added = False
            for owner in list(processes.values()):
                try:
                    children = owner.children(recursive=True)
                except psutil.NoSuchProcess:
                    continue
                for child in children:
                    if child.pid not in processes:
                        try:
                            child.suspend()
                            processes[child.pid] = child
                            added = True
                        except psutil.NoSuchProcess:
                            continue
            if not added:
                break
        else:
            return False
        for process in processes.values():
            try:
                process.kill()
            except psutil.NoSuchProcess:
                pass
        _, alive = psutil.wait_procs(list(processes.values()), timeout=3)
        return not any(process.is_running() and process.status() not in (psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD)
                       for process in alive)
    except (psutil.Error, KeyError, TypeError):
        # Missing/reused parent means it could have spawned unrecorded children
        # before disappearing. Keep the scope quarantined, even with no children
        # in its old snapshot. Never kill an unrelated reused PID.
        return False
    finally:
        for process in processes.values():
            try:
                process.resume()
            except psutil.Error:
                pass


def saved_answer(directory: Path | None, session_id: str, submission_id: str):
    """Require a matching input and a terminal text answer in that exact session."""
    if directory is None or not session_id or Path(session_id).name != session_id:
        return None
    path = directory / f"{session_id}.jsonl"
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return None
    matched = False
    answer = None
    for line in lines:
        try:
            row = json.loads(line)
        except ValueError:
            return None
        if row.get("sessionId") not in (None, session_id):
            continue
        message = row.get("message") or {}
        content = message.get("content")
        blocks = [{"type": "text", "text": content}] if isinstance(content, str) else content or []
        text = "\n".join(block.get("text", "") for block in blocks if isinstance(block, dict) and block.get("type") == "text")
        if row.get("type") == "user" and not any(isinstance(block, dict) and block.get("type") == "tool_result" for block in blocks):
            if matched:
                return answer
            matched = f"Logical input: {submission_id}\n" in text
        elif matched and row.get("type") == "assistant":
            answer = text if message.get("stop_reason") == "end_turn" and text else None
    return answer


def local_readiness():
    """Check native CLI authentication without a model turn or secret output."""
    import shutil
    import subprocess
    cli = shutil.which("claude")
    if not cli:
        return False, "Claude CLI is missing"
    try:
        status = subprocess.run([cli, "auth", "status", "--json"], capture_output=True,
                                text=True, timeout=8)
        data = json.loads(status.stdout)
        authenticated = data.get("loggedIn") is True
        if status.returncode != 0 or not authenticated:
            return False, "Claude login is unavailable; run claude /login locally"
        return True, "Native CLI and authentication are available; model execution is not yet verified"
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return False, "Native CLI authentication check could not complete"
