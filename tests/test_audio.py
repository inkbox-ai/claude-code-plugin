import asyncio
import base64
import json
import math
import struct
import types

import pytest

from inkbox_claude.audio import AudioConverter, CallAudio
from inkbox_claude import realtime


def encoded(raw):
    return base64.b64encode(raw).decode("ascii")


def converted(converter, raw):
    return base64.b64decode(converter.convert(encoded(raw)))


@pytest.mark.parametrize("rates", [(16000, 24000), (24000, 16000)])
def test_streamed_resampling_matches_whole_buffer_even_across_split_samples(rates):
    source, target = rates
    raw = struct.pack("<" + "h" * source, *[
        round(18000 * math.sin(2 * math.pi * 1000 * n / source))
        for n in range(source)
    ])
    expected = converted(AudioConverter(source, target), raw)
    converter = AudioConverter(source, target)
    streamed = b"".join(converted(converter, raw[n:n + 137]) for n in range(0, len(raw), 137))
    assert streamed == expected
    assert abs(len(streamed) // 2 - target) <= 1
    samples = struct.unpack("<" + "h" * (len(streamed) // 2), streamed)
    # Preserve a real tone's sign, gain and duration, not merely its byte count.
    assert max(samples) > 16000
    assert min(samples) < -16000
    crossings = sum(a <= 0 < b for a, b in zip(samples, samples[1:]))
    assert abs(crossings - 1000) <= 1


def test_pcm_little_endian_and_silence():
    raw = struct.pack("<4h", -32768, -1, 0, 32767)
    assert converted(AudioConverter(16000, 16000), raw) == raw
    silence = converted(AudioConverter(16000, 24000), bytes(320))
    assert silence and not any(silence)


def test_output_reset_discards_partial_sample_and_prior_audio():
    converter = AudioConverter(24000, 16000)
    converted(converter, b"\xff\x7f\xff")
    converter.reset()
    silence = bytes(480)
    assert converted(converter, silence) == converted(AudioConverter(24000, 16000), silence)


def test_legacy_audio_is_decoded_and_encoded_not_relabelled():
    audio = CallAudio()
    decoded = converted(audio.inbound, b"\xff" * 160)
    assert 950 <= len(decoded) <= 960
    assert not any(decoded)
    output = converted(audio.outbound, bytes(960))
    assert len(output) == 160
    assert output == b"\xff" * 160
    audio.configure({"encoding": "PCMU", "sample_rate": 8000, "channels": 1})
    assert converted(audio.inbound, b"\xff" * 160) == decoded


@pytest.mark.parametrize("descriptor", [
    {"encoding": "L16", "sample_rate": 8000, "channels": 1},
    {"encoding": "L16", "sample_rate": 16000, "channels": 2},
    {}, "pcm", {"encoding": "unknown", "sample_rate": 16000, "channels": 1},
])
def test_unknown_formats_fail_instead_of_sending_mislabeled_audio(descriptor):
    with pytest.raises(ValueError, match="Unsupported call audio format"):
        CallAudio().configure(descriptor)


class Socket:
    def __init__(self, frames=()):
        self.frames = frames
        self.sent = []

    async def send_str(self, value):
        self.sent.append(json.loads(value))

    async def __aiter__(self):
        for frame in self.frames:
            yield types.SimpleNamespace(type=realtime.aiohttp.WSMsgType.TEXT, data=json.dumps(frame))


def test_start_negotiates_hd_and_pump_converts_inbound_bytes():
    raw = struct.pack("<3h", 0, 12000, 0)
    state = realtime._BridgeState()
    caller = Socket([
        {"event": "start", "stream_id": "s1", "start": {"media_format": {"encoding": "L16", "sample_rate": 16000, "channels": 1}}},
        {"event": "media", "media": {"payload": encoded(raw[:1])}},
        {"event": "media", "media": {"payload": encoded(raw[1:])}},
    ])
    model = Socket()
    meta = realtime.RealtimeCallMeta(call_id="c1", remote_phone_number=None)
    asyncio.run(realtime._inkbox_to_openai_pump(caller, model, state, meta))
    audio_frames = [frame for frame in model.sent if frame["type"] == "input_audio_buffer.append"]
    assert len(audio_frames) == 1
    assert base64.b64decode(audio_frames[0]["audio"]) == converted(AudioConverter(16000, 24000), raw)
    assert state.stream_id == "s1"


@pytest.mark.parametrize("boundary", ["response.output_audio.done", "input_audio_buffer.speech_started"])
def test_outbound_pump_converts_pcm_and_resets_at_boundary(boundary):
    state = realtime._BridgeState()
    state.stream_id = "s1"
    state.audio.configure({"encoding": "L16", "sample_rate": 16000, "channels": 1})
    first = struct.pack("<3h", 32000, 30000, 25000) + b"\xff"
    second = bytes(12)
    model = Socket([
        {"type": "response.output_audio.delta", "delta": encoded(first)},
        {"type": boundary},
        {"type": "response.output_audio.delta", "delta": encoded(second)},
    ])
    caller = Socket()
    asyncio.run(realtime._openai_to_inkbox_pump(
        openai_ws=model, inkbox_ws=caller, state=state,
        config=realtime.RealtimeConfig(),
        meta=realtime.RealtimeCallMeta(call_id="c1", remote_phone_number=None),
        on_agent_consult=None,
    ))
    frames = [frame for frame in caller.sent if frame["event"] == "media"]
    assert len(frames) == 2
    assert base64.b64decode(frames[0]["media"]["payload"]) == converted(AudioConverter(24000, 16000), first)
    assert base64.b64decode(frames[1]["media"]["payload"]) == converted(AudioConverter(24000, 16000), second)
    assert all(frame["stream_id"] == "s1" for frame in frames)
