import asyncio

from tests.live.voice_followup import listen_with_followups


def run_peer(*, nudge="verify the existing action", heard_at=()):
    now = [0.0]
    spoken = []
    last_heard = [0.0]

    async def sleep(seconds):
        now[0] += seconds
        if now[0] in heard_at:
            last_heard[0] = now[0]

    async def say(text):
        spoken.append((now[0], text))

    asyncio.run(listen_with_followups(
        say, seconds=180, nudge=nudge, last_heard=lambda: last_heard[0],
        clock=lambda: now[0], sleep=sleep,
    ))
    return spoken, now[0]


def test_quiet_peer_clarifies_twice_without_extending_call_deadline():
    spoken, elapsed = run_peer()
    assert spoken == [(30.0, "verify the existing action"), (60.0, "verify the existing action")]
    assert elapsed == 180


def test_agent_speech_defers_clarification_until_next_quiet_period():
    spoken, elapsed = run_peer(heard_at=(20.0, 40.0))
    assert [when for when, text in spoken] == [70.0, 100.0]
    assert elapsed == 180


def test_default_driver_does_not_add_unsolicited_turns():
    spoken, elapsed = run_peer(nudge="")
    assert spoken == []
    assert elapsed == 180
