"""Shared SMS failure classification for replies and hosted-call tools."""

from __future__ import annotations

from typing import Any

_TERMINAL_CODES = frozenset({
    "recipient_not_opted_in",
    "recipient_opted_out",
    "recipient_blocked",
    "invalid_phone_number",
    "carrier_rejected",
    "sender_sms_pending",
    "sender_sms_assignment_failed",
    "sender_not_registered",
    "sender_registration_required",
    "messaging_profile_disabled",
    "toll_free_sms_unsupported",
})
_TERMINAL_MARKERS = (
    "opted out",
    "opt-out",
    "not opted in",
    "invalid number",
    "invalid phone",
    "unreachable",
    "unknown subscriber",
    "cannot receive",
    "unsafe",
    "harmful",
    "abusive",
    "harassment",
    "threatening",
    "illegal content",
)
_RETRY_MARKERS = (
    "40002",
    "spam",
    "content",
    "too_long",
    "too long",
    "markdown",
    "emoji",
    "profanity",
    "temporar",
    "carrier_unavailable",
)
_HOSTED_TERMINAL_CODES = frozenset({
    *_TERMINAL_CODES,
    "unauthorized",
    "forbidden",
    "insufficient_authority",
    "carrier_rate_limit",
    "carrier_unavailable",
    "carrier_temporarily_unavailable",
    "inkbox_duplicate_body",
    "inkbox_carrier_backoff",
})
_HOSTED_RECOVERABLE_CODES = frozenset({
    "sms_too_long",
    "message_too_long",
    "message_blocked_spam_filter",
    "content_flagged_as_spam",
    "content_rejected_by_carrier",
    "content_blocked_by_policy",
})
_HOSTED_TERMINAL_MARKERS = (
    *_TERMINAL_MARKERS,
    "duplicate_body",
    "carrier_backoff",
    "carrier_unavailable",
    "carrier temporarily unavailable",
    "rate limit",
    "timed out",
    "timeout",
)
_HOSTED_RECOVERABLE_MARKERS = (
    "invalid argument",
    "invalid params",
    "required property",
    "missing required",
    "schema",
    "format",
    "e.164",
    "maximum is",
    "too long",
    "must be",
    "specify exactly one",
    "message_blocked_spam_filter",
    "content_flagged_as_spam",
    "content_rejected_by_carrier",
    "content_blocked_by_policy",
)
_COMMIT_AMBIGUOUS_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


def sms_delivery_failure_policy(
    error_code: str | None,
    error_detail: str | None,
) -> str:
    """Classify an SMS failure as retry, stop, or conditional."""
    code = str(error_code or "").strip().lower()
    detail = str(error_detail or "").strip().lower()
    combined = f"{code} {detail}"
    if code in _TERMINAL_CODES or any(
        marker in combined for marker in _TERMINAL_MARKERS
    ):
        return "stop"
    if any(marker in combined for marker in _RETRY_MARKERS):
        return "retry"
    return "conditional"


def sms_tool_failure_kind(error: Any) -> str:
    """Classify one SDK/local SMS failure without returning sensitive detail."""
    status = getattr(error, "status_code", None)
    try:
        status = int(status) if status is not None else None
    except (TypeError, ValueError):
        status = None

    detail = getattr(error, "detail", None)
    code = str(getattr(error, "error_code", "") or "").strip()
    rule = ""
    message = ""
    if isinstance(detail, dict):
        code = str(
            detail.get("error")
            or detail.get("error_code")
            or detail.get("code")
            or code
        ).strip()
        rule = str(detail.get("rule") or "").strip()
        message = str(detail.get("message") or detail.get("reason") or "").strip()
    elif detail is not None:
        message = str(detail)

    fallback = str(error or "")
    normalized_code = code.lower()
    combined = " ".join(
        value.lower() for value in (code, rule, message, fallback) if value
    )
    if (
        normalized_code in _HOSTED_TERMINAL_CODES
        or status in _COMMIT_AMBIGUOUS_STATUSES
        or any(marker in combined for marker in _HOSTED_TERMINAL_MARKERS)
    ):
        return "terminal"
    if (
        normalized_code in _HOSTED_RECOVERABLE_CODES
        or any(marker in combined for marker in _HOSTED_RECOVERABLE_MARKERS)
    ):
        return "recoverable"
    if status in {401, 403}:
        return "terminal"
    return "unknown"
