"""
Feature: handle_twilio_call - Twilio Media Streams event loop

    handle_twilio_call is the WebSocket handler Twilio talks to for the
    lifetime of one phone call. It:

      - Receives JSON events over the socket ("connected", "start",
        "media", "stop").
      - Creates a CallSession on "start" (which spins up the
        Speechmatics RT connection).
      - Base64-decodes each "media" payload and routes it to the
        session by track label.
      - Closes the session cleanly when the call ends, whether that's
        a graceful "stop" event or an abrupt connection drop.

    The tests below feed canned event streams through a fake WebSocket
    (see conftest.fake_ws) and assert on the interaction with
    CallSession. CallSession itself is patched to an AsyncMock so the
    real Speechmatics client is never contacted - these are pure logic
    tests for the event router.
"""

import base64
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import websockets

import proxy


@pytest.fixture
def mock_session():
    """
    Replace proxy.CallSession with a MagicMock for the duration of a test.

    Yields the class mock and its return-value instance so callers can
    both:
      - assert that the constructor was invoked (or not), via `Session`
      - assert on method calls on the instance (start, enqueue, close)

    close() is explicitly wired as an AsyncMock because handle_twilio_call
    awaits it - a plain MagicMock would raise "coroutine was never awaited"
    warnings.
    """
    with patch("proxy.CallSession") as MockSession:
        instance = MagicMock()
        instance.close = AsyncMock()
        MockSession.return_value = instance
        yield MockSession, instance


async def test_start_event_creates_and_starts_session(fake_ws, start_event, mock_session):
    """
    Scenario: A well-formed Twilio start event
        Given a fake WebSocket queued with ["connected", <start>, "stop"]
         When handle_twilio_call consumes the stream
         Then CallSession() was constructed exactly once
          And session.start() was invoked exactly once

    Purpose:
        Guards the "call kicks off a Speechmatics session" step. If
        this regresses, no audio ever reaches Speechmatics for the
        rest of the call.
    """
    Session, instance = mock_session

    # Minimal happy path: handshake, start, immediate hang-up.
    ws = fake_ws([{"event": "connected"}, start_event, {"event": "stop"}])

    await proxy.handle_twilio_call(ws)

    # Session must be created exactly once per call.
    Session.assert_called_once()

    # And it must be told to start (which is what launches the RT task).
    instance.start.assert_called_once()


async def test_ignores_events_before_start(fake_ws, media_event, mock_session):
    """
    Scenario: A "media" event arrives before "start"
        Given a fake WebSocket queued with [<media>, "stop"]
          And no "start" event has yet been seen
         When handle_twilio_call consumes the stream
         Then session.enqueue was never called

    Purpose:
        Defensive: Twilio's protocol always sends start before media,
        but a broken client, a network reorder, or a future protocol
        change could break that. Silently ignoring pre-start media
        keeps us from crashing on a None session.
    """
    _, instance = mock_session

    # Deliberately out-of-order stream - media before start.
    ws = fake_ws([media_event("inbound"), {"event": "stop"}])

    await proxy.handle_twilio_call(ws)

    # No session was created, so enqueue shouldn't have been called.
    instance.enqueue.assert_not_called()


async def test_media_event_base64_decoded_and_forwarded(
    fake_ws, start_event, media_event, mock_session
):
    """
    Scenario: A media event with a base64-encoded payload
        Given a WebSocket stream containing start -> media -> stop
          And the media event's payload is base64(bytes(range(32)))
         When handle_twilio_call consumes the stream
         Then session.enqueue is called once with ("inbound", bytes(range(32)))

    Purpose:
        Verifies both requirements at once:
          - "Decodes incoming u-law 8kHz base64 encoded audio frames"
          - "Forwards audio frames to Speechmatics"
        The exact bytes must be recovered from base64 and land on the
        right track, or the transcript will be garbled.
    """
    _, instance = mock_session

    # Predictable non-trivial bytes so we spot any encoding drift.
    raw_audio = bytes(range(32))

    ws = fake_ws(
        [
            start_event,
            media_event("inbound", raw_audio),
            {"event": "stop"},
        ]
    )

    await proxy.handle_twilio_call(ws)

    # Only one media event was fed; enqueue must have been called
    # exactly once, with the *decoded* bytes (not the base64 string).
    instance.enqueue.assert_called_once_with("inbound", raw_audio)


async def test_both_tracks_forwarded_to_session_by_label(
    fake_ws, start_event, media_event, mock_session
):
    """
    Scenario: Interleaved inbound + outbound media events
        Given a stream containing start -> media(inbound) -> media(outbound) -> stop
         When handle_twilio_call consumes the stream
         Then enqueue was called with ("inbound", <inbound bytes>)
          And enqueue was called with ("outbound", <outbound bytes>)

    Purpose:
        This is the multi-channel diarization contract: each track
        must be routed with its own label so Speechmatics can produce
        per-speaker transcripts. Crossed wires here would smoosh both
        speakers onto one channel.
    """
    _, instance = mock_session

    # Distinct filler bytes so a mis-routed frame would be obvious.
    in_bytes = b"\xaa" * 160
    out_bytes = b"\xbb" * 160

    ws = fake_ws(
        [
            start_event,
            media_event("inbound", in_bytes),
            media_event("outbound", out_bytes),
            {"event": "stop"},
        ]
    )

    await proxy.handle_twilio_call(ws)

    # Extract the positional args of each enqueue call so we can
    # assert both tuples were passed regardless of ordering.
    call_args = [c.args for c in instance.enqueue.call_args_list]
    assert ("inbound", in_bytes) in call_args
    assert ("outbound", out_bytes) in call_args


async def test_media_without_payload_is_skipped(
    fake_ws, start_event, mock_session
):
    """
    Scenario: A media event that is missing the "payload" key
        Given a stream containing start -> {media without payload} -> stop
         When handle_twilio_call consumes the stream
         Then session.enqueue was never called

    Purpose:
        Defensive: base64.b64decode(None) would raise TypeError and
        kill the whole handler. This test proves we skip such events
        instead of taking the whole call down.
    """
    _, instance = mock_session

    ws = fake_ws(
        [
            start_event,
            # Malformed - no "payload" field.
            {"event": "media", "media": {"track": "inbound"}},
            {"event": "stop"},
        ]
    )

    await proxy.handle_twilio_call(ws)

    # Malformed event was silently skipped - enqueue never fired.
    instance.enqueue.assert_not_called()


async def test_stop_event_closes_session(fake_ws, start_event, mock_session):
    """
    Scenario: The call ends via Twilio's "stop" event
        Given an active call (start event received)
         When a "stop" event is delivered
         Then session.close() is awaited exactly once

    Purpose:
        The primary graceful-shutdown path. Without this, every call
        leaks a CallSession task that hangs on read() forever - the
        classic "memory grows one session per call" bug.
    """
    _, instance = mock_session

    ws = fake_ws([start_event, {"event": "stop"}])

    await proxy.handle_twilio_call(ws)

    # session.close() must be awaited to drain sources and let the
    # Speechmatics session unwind cleanly.
    instance.close.assert_awaited_once()


async def test_connection_closed_still_closes_session(start_event, mock_session):
    """
    Scenario: The WebSocket drops abruptly mid-call
        Given an active call (start event received)
         When the WebSocket raises ConnectionClosed on the next iteration
         Then session.close() is still awaited exactly once

    Purpose:
        Networks aren't reliable and Twilio's WebSocket may drop
        without a "stop" event. The `finally` block in
        handle_twilio_call is what makes shutdown safe here - this
        test would fail loudly if someone accidentally moved the
        cleanup into the try block.
    """
    _, instance = mock_session

    # Custom async-iterable that yields the start event, then blows
    # up with ConnectionClosed the way the real websockets library
    # would on an unexpected disconnect.
    class DropAfterStart:
        async def __aiter__(self):
            import json as _json
            yield _json.dumps(start_event)
            raise websockets.exceptions.ConnectionClosed(None, None)

    await proxy.handle_twilio_call(DropAfterStart())

    # Even though we exited via exception, cleanup still ran.
    instance.close.assert_awaited_once()


async def test_no_session_ever_started_is_a_noop(fake_ws, mock_session):
    """
    Scenario: The socket opens and closes with no "start" event
        Given a WebSocket that only ever sends "connected"
         When handle_twilio_call consumes the stream and returns
         Then no exception was raised
          And session.close() was never awaited (because session is None)

    Purpose:
        Guards the `if session:` check inside the finally block. If
        that check went missing, this scenario would blow up trying
        to await None.close(). Twilio can (and does) open sockets
        for health checks that never progress to a real call.
    """
    _, instance = mock_session

    # Only "connected" arrives - handshake with no follow-up call.
    ws = fake_ws([{"event": "connected"}])

    # Must not raise.
    await proxy.handle_twilio_call(ws)

    # No session was ever built, so close cannot have been called.
    instance.close.assert_not_awaited()


async def test_undecodable_payload_drops_the_frame_not_the_call(
    fake_ws, start_event, media_event, mock_session, capsys
):
    """
    Scenario: One media frame has a malformed base64 payload
        Given a call with a good frame, a bad frame, then another good frame
         When handle_twilio_call consumes the stream
         Then it does not raise
          And both good frames still reach the session
          And the bad frame is reported on stderr
          And the session is still closed cleanly

    Purpose:
        base64.b64decode raised binascii.Error on a single bad payload,
        which ended that call. A bad frame should cost one frame of audio.
    """
    _Session, instance = mock_session

    bad = {"event": "media", "media": {"track": "inbound", "payload": "abcde"}}
    ws = fake_ws(
        [
            start_event,
            media_event("inbound", b"\x01\x02"),
            bad,
            media_event("inbound", b"\x03\x04"),
            {"event": "stop"},
        ]
    )

    await proxy.handle_twilio_call(ws)

    # Both good frames arrived, in order, and the bad one did not.
    assert [c.args[1] for c in instance.enqueue.call_args_list] == [b"\x01\x02", b"\x03\x04"]
    instance.close.assert_awaited_once()

    err = capsys.readouterr().err
    assert "dropping undecodable audio frame" in err
    assert "dropped 1 undecodable" in err


async def test_many_bad_frames_log_once_and_are_tallied(
    fake_ws, start_event, mock_session, capsys
):
    """
    Scenario: A stream of malformed frames
        Given five consecutive bad payloads
         When handle_twilio_call consumes them
         Then the per-frame warning is printed once
          And the end-of-call tally says five were dropped

    Purpose:
        At 50 frames/s a per-frame log would bury every other line.
    """
    bad = {"event": "media", "media": {"track": "inbound", "payload": "abcde"}}
    ws = fake_ws([start_event, *([bad] * 5), {"event": "stop"}])

    await proxy.handle_twilio_call(ws)

    err = capsys.readouterr().err
    assert err.count("dropping undecodable audio frame") == 1
    assert "dropped 5 undecodable" in err
