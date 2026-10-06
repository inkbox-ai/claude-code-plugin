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
    """Fence recorded process identities, including children orphaned by exit."""
    if not identity:
        return False
    processes = {}
    try:
        for entry in [identity, *identity.get("children", [])]:
            try:
                process = psutil.Process(entry["pid"])
                if process.create_time() != entry["created"]:
                    continue
                processes[process.pid] = process
                for child in process.children(recursive=True):
                    processes[child.pid] = child
            except psutil.NoSuchProcess:
                continue
        for process in processes.values():
            try:
                process.terminate()
            except psutil.NoSuchProcess:
                pass
        _, alive = psutil.wait_procs(list(processes.values()), timeout=3)
        for process in alive:
            try:
                process.kill()
            except psutil.NoSuchProcess:
                pass
        _, alive = psutil.wait_procs(alive, timeout=3)
        return not alive
    except (psutil.AccessDenied, KeyError, TypeError):
        return False


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
