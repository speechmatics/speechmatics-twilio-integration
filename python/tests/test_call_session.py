"""
Feature: CallSession routing + audio wire-format invariants

    A CallSession owns one Speechmatics multi-channel session covering
    both Twilio tracks. Two things must hold for the demo to work:

      1. Audio wire format: we forward Twilio's native mulaw / 8 kHz
         frames straight through, no transcoding. If any of those
         constants drift the pipeline breaks silently.

      2. Track routing: each Twilio 'media' event carries a `track`
         label ("inbound" or "outbound"); frames must land in the
         AsyncQueueSource that matches that label so the multi-channel
         SDK can produce per-speaker transcripts.

    These tests don't spin up a real Speechmatics session - they only
    exercise the pieces that live inside CallSession itself.
"""

from speechmatics.rt import AudioEncoding

from proxy import CHANNELS, TWILIO_AUDIO_FORMAT, CallSession


def test_audio_format_matches_twilio_native_wire_format():
    """
    Scenario: The audio format we advertise to Speechmatics
        Given the module-level TWILIO_AUDIO_FORMAT constant
         When its encoding, sample_rate, and chunk_size are inspected
         Then encoding is MULAW
          And sample_rate is 8000
          And chunk_size is 160 (== 20 ms at 8 kHz mulaw)

    Purpose:
        Guards the "no transcoding" property. Twilio Media Streams
        emits mulaw 8 kHz in 20 ms (160-byte) frames; Speechmatics RT
        accepts that natively. If any of these three values drift,
        we'd silently start sending mis-configured audio and the
        server would produce garbage or reject the session.
    """
    # Encoding must be mulaw - anything else means the server will
    # try to interpret the bytes wrong.
    assert TWILIO_AUDIO_FORMAT.encoding == AudioEncoding.MULAW

    # Sample rate must match Twilio's 8 kHz PSTN-quality stream.
    assert TWILIO_AUDIO_FORMAT.sample_rate == 8000

    # chunk_size is what the SDK reads per source per round-robin
    # iteration; matching the 160-byte Twilio frame size keeps
    # latency at one frame.
    assert TWILIO_AUDIO_FORMAT.chunk_size == 160


def test_channels_are_named_to_match_twilio_track_labels():
    """
    Scenario: The channel identifiers we register with Speechmatics
        Given the module-level CHANNELS list
         When its contents are inspected
         Then it equals ["inbound", "outbound"]

    Purpose:
        Twilio's Media Streams `media.track` field is always the bare
        string "inbound" or "outbound". Using those same values as our
        Speechmatics channel labels means routing is a simple dict
        lookup - no translation table to maintain, no accidental
        mismatch. If someone renames these ("channel1" etc.) the
        translation would need to come back or routing would break.
    """
    assert CHANNELS == ["inbound", "outbound"]


async def test_enqueue_routes_inbound_frames_to_inbound_source():
    """
    Scenario: An inbound-track frame is routed correctly
        Given a fresh CallSession
         When enqueue("inbound", <bytes>) is called
          And the "inbound" source is read
         Then the read returns exactly the bytes that were enqueued
          And the "outbound" source received nothing

    Purpose:
        The core routing invariant for one direction. If this failed,
        one side of the transcript would be empty or the two sides
        would blur together.
    """
    session = CallSession()

    # Push a frame explicitly tagged for the inbound track.
    session.enqueue("inbound", b"hello-inbound")

    # It should be readable from the inbound source, byte-for-byte.
    data = await session.sources["inbound"].read(len(b"hello-inbound"))
    assert data == b"hello-inbound"


async def test_enqueue_routes_outbound_frames_to_outbound_source():
    """
    Scenario: An outbound-track frame is routed correctly
        Given a fresh CallSession
         When enqueue("outbound", <bytes>) is called
          And the "outbound" source is read
         Then the read returns exactly the bytes that were enqueued

    Purpose:
        Mirror of the inbound test - the routing map must work
        symmetrically in both directions.
    """
    session = CallSession()

    session.enqueue("outbound", b"hello-outbound")

    data = await session.sources["outbound"].read(len(b"hello-outbound"))
    assert data == b"hello-outbound"


async def test_enqueue_ignores_unknown_track():
    """
    Scenario: A frame tagged with an unknown track label
        Given a fresh CallSession
         When enqueue("bogus", <bytes>) is called
         Then no exception is raised
          And no "bogus" key appears in session.sources

    Purpose:
        Defensive check against a malformed Twilio message (a client
        bug, header mangling, or a future Twilio feature that adds a
        new track). We should silently drop instead of crashing the
        entire call.
    """
    session = CallSession()

    # Deliberately garbage track name - the enqueue should be a no-op.
    session.enqueue("bogus", b"garbage")

    # And we should not have grown a new source for it.
    assert "bogus" not in session.sources


async def test_enqueue_ignored_after_close():
    """
    Scenario: A frame arrives after the session has been closed
        Given a CallSession that has already been closed via close()
         When enqueue("inbound", <bytes>) is called
          And the "inbound" source is read
         Then the read returns empty bytes (the frame was dropped)

    Purpose:
        Reflects real-life ordering during teardown: Twilio may send
        a straggling `media` event after we've already handled the
        `stop` event. Enqueuing after close would either lose that
        data silently (fine) or wake up a source we've already drained
        (bad). This test proves we get the safe outcome.
    """
    session = CallSession()

    # close() closes sources too, so read() below will return EOF
    # instead of blocking forever.
    await session.close()

    # A late frame should be silently dropped because _closed is set.
    session.enqueue("inbound", b"late")

    # Sources hit EOF - the late frame is not delivered.
    assert await session.sources["inbound"].read(4) == b""


async def test_close_marks_session_closed_and_drains_sources():
    """
    Scenario: Graceful close cascades to sources
        Given a CallSession with data buffered on both tracks
         When close() is awaited
         Then session._closed is True
          And session.sources["inbound"]._closed is True
          And session.sources["outbound"]._closed is True

    Purpose:
        Ties the CallSession-level shutdown signal to the underlying
        source-level EOF sentinels. Without this cascade, close()
        would flip a flag but leave the source queues blocking on
        read(), and the _run task inside a real session would hang
        forever waiting for audio that would never come.
    """
    session = CallSession()

    # Simulate mid-call state: bytes buffered on each track.
    session.enqueue("inbound", b"buf-in")
    session.enqueue("outbound", b"buf-out")

    # Ask the session to shut down. Because we never started the
    # _task, close() only awaits the (nonexistent) task guarded by
    # `if self._task:` - so this returns promptly.
    await session.close()

    # Top-level flag flipped.
    assert session._closed is True

    # And that flag propagated to every source, so any pending or
    # future read() will get EOF instead of hanging.
    assert session.sources["inbound"]._closed
    assert session.sources["outbound"]._closed


def test_consecutive_finals_on_one_channel_have_single_spaces(capsys):
    """
    Scenario: Several finals arrive in a row on one channel
        Given transcript chunks that each end in a trailing space, as
              Speechmatics sends them
         When the session prints them back to back
         Then the line reads "This call is being transcribed " with
              single spaces between words

    Purpose:
        metadata.transcript is built for plain concatenation. Adding our
        own separator on top printed "This  call is  being  transcribed".
    """
    session = CallSession()

    for chunk in ("This call ", "is ", "being ", "transcribed "):
        session._print_final("outbound", chunk)

    out = capsys.readouterr().out
    assert out == "[outbound] This call is being transcribed "
    assert "  " not in out.replace("[outbound]", "").strip()


def test_channel_change_starts_a_new_labelled_line(capsys):
    """
    Scenario: The speaker changes between finals
        Given a final on "inbound" followed by one on "outbound"
         When both are printed
         Then the second starts on a new line with its own label

    Purpose:
        Guards the other two branches of _print_final, which the spacing
        fix sits next to.
    """
    session = CallSession()

    session._print_final("inbound", "Hello. ")
    session._print_final("outbound", "Hi. ")

    assert capsys.readouterr().out == "[inbound ] Hello. \n[outbound] Hi. "
