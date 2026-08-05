"""Exact-leg correlation and transcript proof for live voice tests."""

from __future__ import annotations

import re
import time
from datetime import UTC, datetime, timedelta


def _digits(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def _created_at(record) -> datetime | None:
    value = getattr(record, "created_at", None)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return None


def _exception_status(exc: Exception) -> int | None:
    status = getattr(exc, "status_code", None)
    if isinstance(status, int):
        return status
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    return status if isinstance(status, int) else None


def _list_by_pair(client, pair_id: str):
    try:
        return client.calls.list(limit=2, paired_call_id=pair_id)
    except TypeError:
        return None
    except Exception as exc:
        if _exception_status(exc) == 422:
            return None
        raise


def _matching_agent_calls(
    client,
    *,
    direction: str,
    driver_number: str,
    before_ids: set,
    started_at: datetime,
):
    tail = _digits(driver_number)[-10:]
    lower_bound = started_at - timedelta(seconds=10)
    return [
        call
        for call in client.calls.list(limit=200)
        if call.id not in before_ids
        and (getattr(call, "direction", "") or "").lower() == direction
        and _digits(getattr(call, "remote_phone_number", "") or "")[-10:] == tail
        and (created_at := _created_at(call)) is not None
        and created_at >= lower_bound
    ]


def wait_for_agent_leg(
    driver_client,
    agent_client,
    driver_call_id,
    *,
    direction: str,
    driver_number: str,
    before_agent_ids: set,
    started_at: datetime,
    deadline: float,
    poll_every: float = 6.0,
):
    """Return the exact related leg as seen through the agent identity."""
    last = "agent leg not visible"
    while time.monotonic() < deadline:
        driver_call = driver_client.calls.get(driver_call_id)
        pair_id = getattr(driver_call, "paired_call_id", None)
        if pair_id:
            paired = _list_by_pair(agent_client, str(pair_id))
            if paired is not None:
                assert len(paired) <= 1, (
                    f"pair_id={pair_id} returned duplicate agent legs "
                    f"ids={[str(call.id) for call in paired]}"
                )
                if paired:
                    call = paired[0]
                    assert (getattr(call, "direction", "") or "").lower() == direction
                    return call
                last = f"pair_id={pair_id} has no agent-visible leg yet"
                time.sleep(poll_every)
                continue

        candidates = _matching_agent_calls(
            agent_client,
            direction=direction,
            driver_number=driver_number,
            before_ids=before_agent_ids,
            started_at=started_at,
        )
        assert len(candidates) <= 1, (
            "fallback correlation found ambiguous agent legs "
            f"ids={[str(call.id) for call in candidates]}"
        )
        if candidates:
            call = candidates[0]
            candidate_pair = getattr(call, "paired_call_id", None)
            if pair_id and candidate_pair:
                assert str(candidate_pair) == str(pair_id)
            driver_created = _created_at(driver_call)
            agent_created = _created_at(call)
            assert driver_created is not None and agent_created is not None
            assert abs((driver_created - agent_created).total_seconds()) <= 60
            return call
        last = f"pair_id={pair_id!s} fallback_candidates=0"
        time.sleep(poll_every)
    raise AssertionError(f"agent leg was not correlated before the deadline ({last})")


def _transcript_parties(client, call_id):
    segments = client.calls.transcripts(call_id)
    remote = [
        segment
        for segment in segments
        if (getattr(segment, "party", "") or "").lower() == "remote"
        and (getattr(segment, "text", "") or "").strip()
    ]
    local = [
        segment
        for segment in segments
        if (getattr(segment, "party", "") or "").lower() == "local"
        and (getattr(segment, "text", "") or "").strip()
    ]
    return remote, local


def _wait_for_speech(
    client,
    call_id,
    *,
    require_remote: bool,
    deadline: float,
    poll_every: float,
    ended_grace: float,
    label: str,
):
    ended_at = None
    last = "transcript not ready"
    while time.monotonic() < deadline:
        try:
            remote, local = _transcript_parties(client, call_id)
        except Exception as exc:
            remote, local = [], []
            last = f"transcripts not ready: {type(exc).__name__}"
        if local and (remote or not require_remote):
            return " | ".join(segment.text.strip() for segment in local)
        call = client.calls.get(call_id)
        status = (getattr(call, "status", "") or "").lower()
        last = f"remote={len(remote)} local={len(local)} status={status!r}"
        if status in {"canceled", "failed"}:
            raise AssertionError(f"{label} ended before transcript proof ({last})")
        if status == "completed":
            ended_at = ended_at or time.monotonic()
            if time.monotonic() - ended_at > ended_grace:
                raise AssertionError(f"{label} ended without transcript proof ({last})")
        time.sleep(poll_every)
    raise AssertionError(f"{label} lacked transcript proof before the deadline ({last})")


def wait_for_driver_local_speech(
    client,
    call_id,
    *,
    deadline: float,
    poll_every: float = 6.0,
    ended_grace: float = 15.0,
):
    """Require only the scripted driver's own speech on the driver leg."""
    return _wait_for_speech(
        client,
        call_id,
        require_remote=False,
        deadline=deadline,
        poll_every=poll_every,
        ended_grace=ended_grace,
        label="driver leg",
    )


def wait_for_agent_two_way_conversation(
    client,
    call_id,
    *,
    deadline: float,
    poll_every: float = 6.0,
    ended_grace: float = 15.0,
):
    """Require both parties on the leg fetched with the agent identity."""
    return _wait_for_speech(
        client,
        call_id,
        require_remote=True,
        deadline=deadline,
        poll_every=poll_every,
        ended_grace=ended_grace,
        label="agent leg",
    )
