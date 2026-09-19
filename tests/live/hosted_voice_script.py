"""Build the short caller turns used by the hosted SMS scenario."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


def driver_diagnostic_lines(log: str) -> list[str]:
    """Return bounded stage metadata, never caller text or identity data."""
    pattern = re.compile(
        r"driver (spoke stage=(?:None|[0-2]) chars=\d+"
        r"|heard final chars=\d+ active_stage=(?:True|False)"
        r"|reask stage=[0-2] total_reasks=[0-2]"
        r"|peer interrupted tts=(?:True|False)"
        r"|peer did not pause before the greeting deadline"
        r"|sent stop \(hangup\))$"
    )
    return [
        match.group(1)
        for line in log.splitlines()
        if (match := pattern.search(line))
    ][-60:]


def hosted_sms_stages(marker: str) -> list[dict[str, str]]:
    marker = " ".join(marker.split())
    if len(marker.split()) != 3:
        raise ValueError("hosted SMS scenario requires a three-word marker")
    spoken_marker = ", ".join(marker.split())
    return [
        {"text": "I'd like you to text me something after our call."},
        {
            "text": (
                f"After we hang up send me exactly {spoken_marker} by SMS. "
                "Repeat the body."
            ),
            "expected_reply": marker,
        },
    ]


if __name__ == "__main__":
    marker_path, output_path = map(Path, sys.argv[1:])
    output_path.write_text(
        json.dumps(hosted_sms_stages(marker_path.read_text()), indent=2) + "\n"
    )
