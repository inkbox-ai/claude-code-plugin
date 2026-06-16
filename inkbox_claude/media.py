"""Media helpers for the bridge.

Inbound: download attachments that arrive on webhooks (MMS/iMessage media,
email attachments) to local files so Claude can open them with the Read tool.
Outbound: turn local files into the shapes each channel's send API wants
(base64 for email, an uploaded URL for SMS/iMessage).
"""

from __future__ import annotations

import base64
import logging
import mimetypes
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import aiohttp
except ImportError:  # pragma: no cover - aiohttp is a runtime dep
    aiohttp = None  # type: ignore

logger = logging.getLogger(__name__)

_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


def media_dir() -> Path:
    """Directory inbound attachments are saved to (created if missing)."""
    base = os.getenv("INKBOX_CLAUDE_MEDIA_DIR") or str(Path.home() / ".inkbox-claude" / "media")
    path = Path(base)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _extension_for(content_type: Optional[str], url: str) -> str:
    """Pick a file extension from the URL, falling back to the MIME type."""
    name = url.split("?", 1)[0]
    _, dot, ext = name.rpartition(".")
    if dot and 1 <= len(ext) <= 5 and ext.isalnum():
        return f".{ext.lower()}"
    if content_type:
        guessed = mimetypes.guess_extension(content_type.split(";", 1)[0].strip())
        if guessed:
            return guessed
    return ".bin"


def _item_field(item: Any, name: str) -> Any:
    # Webhook payloads are dicts; SDK objects expose the same attrs.
    if isinstance(item, dict):
        return item.get(name)
    return getattr(item, name, None)


async def download_media(items: List[Any], *, prefix: str) -> List[Dict[str, Any]]:
    """Download inbound media items to local files, best-effort.

    Args:
        items (list): Media items (each carrying a ``url`` + ``content_type``),
            from a webhook's ``media`` list or fetched email attachments.
        prefix (str): Filename prefix (e.g. ``sms-<msgid>``) for the saved files.

    Returns:
        list[dict]: The files that downloaded, as
        ``{"path", "content_type", "size"}``. Items that fail are skipped.
    """
    if aiohttp is None or not items:
        return []
    dest = media_dir()
    safe_prefix = _UNSAFE.sub("-", prefix) or "media"
    saved: List[Dict[str, Any]] = []
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for idx, item in enumerate(items):
            url = str(_item_field(item, "url") or "")
            if not url:
                continue
            content_type = _item_field(item, "content_type")
            target = dest / f"{safe_prefix}-{idx}{_extension_for(content_type, url)}"
            try:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        logger.warning("[media] download %s… -> HTTP %s", url[:60], resp.status)
                        continue
                    target.write_bytes(await resp.read())
            except Exception as exc:
                logger.warning("[media] download failed: %s", exc)
                continue
            saved.append({
                "path": str(target),
                "content_type": content_type or "application/octet-stream",
                "size": _item_field(item, "size"),
            })
    return saved


def inbound_media_note(saved: List[Dict[str, Any]]) -> str:
    """Render a note about saved attachments to append to an inbound message."""
    if not saved:
        return ""
    lines = ["", "[Attachments received — saved locally. Use the Read tool to view images/files:]"]
    for item in saved:
        lines.append(f"- {item['path']} ({item['content_type']})")
    return "\n".join(lines)


def file_to_email_attachment(path: str) -> Dict[str, str]:
    """Read a local file into the email attachment shape (base64 inline).

    Args:
        path (str): Local file path to attach.

    Returns:
        dict: ``{"filename", "content_type", "content_base64"}`` for send_email.
    """
    resolved = Path(path).expanduser()
    data = resolved.read_bytes()
    content_type = mimetypes.guess_type(resolved.name)[0] or "application/octet-stream"
    return {
        "filename": resolved.name,
        "content_type": content_type,
        "content_base64": base64.b64encode(data).decode("ascii"),
    }
