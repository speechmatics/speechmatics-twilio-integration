"""
Feature: TwiML builder (_build_twiml)

    When a call comes in, Twilio fetches HTTP GET on our /twiml
    endpoint and executes whatever TwiML we return. The response has
    to contain exactly the right elements in the right shape:

      <Response>
        <Start>
          <Stream track="both_tracks" url="wss://.../twilio" />
        </Start>
        <Say>...</Say>
        <Dial>+E164</Dial>
      </Response>

    A single wrong attribute (e.g. `track="inbound"` instead of
    `"both_tracks"`) and Twilio silently plays "an application error
    has occurred" without any obvious clue why. These tests pin the
    shape so refactors can't sneak a regression in.
"""

from xml.etree import ElementTree as ET

from proxy import _build_twiml


def _parse(body: bytes) -> ET.Element:
    """Parse the TwiML bytes into an ElementTree root for assertions."""
    return ET.fromstring(body)


def test_twiml_returns_bytes():
    """
    Scenario: Return type is bytes, not str
        Given any valid stream URL and forward number
         When _build_twiml is invoked
         Then the returned object is a `bytes` instance

    Purpose:
        The /twiml handler stores this value verbatim as the HTTP
        response body and sets Content-Length from `len(...)`. It
        must be bytes-typed or the websockets library will reject the
        response.
    """
    body = _build_twiml("wss://x/twilio", "+441234567890")
    assert isinstance(body, bytes)


def test_twiml_root_is_response():
    """
    Scenario: Document root element
        Given the TwiML output for any inputs
         When it is parsed as XML
         Then the root element's tag is "Response"

    Purpose:
        Twilio expects every TwiML document to be wrapped in a
        top-level <Response>. If we ever accidentally returned a
        naked <Stream> or <Dial>, Twilio would reject the whole
        document.
    """
    root = _parse(_build_twiml("wss://x/twilio", "+441234567890"))
    assert root.tag == "Response"


def test_twiml_start_stream_has_correct_url_and_track():
    """
    Scenario: The <Start><Stream> that forks the call audio
        Given the TwiML output for a specific ngrok stream URL
         When the document is parsed
         Then a <Stream> element exists inside <Start>
          And its `url` attribute is that stream URL verbatim
          And its `track` attribute is "both_tracks"

    Purpose:
        The <Stream> URL is the WebSocket the multi-channel client
        connects to. `track="both_tracks"` is what forks caller and
        callee audio into two separate channels; anything else (e.g.
        "inbound_track") would silently give us a single mixed
        channel and the diarization split would collapse.
    """
    root = _parse(_build_twiml("wss://tunnel.example.dev/twilio", "+441234567890"))

    # <Start> should contain a single <Stream> child.
    stream = root.find("./Start/Stream")
    assert stream is not None, "TwiML must contain <Start><Stream>"

    # URL must be exact - a substituted value or trailing slash would
    # cause a WebSocket 404 at call time.
    assert stream.attrib["url"] == "wss://tunnel.example.dev/twilio"

    # both_tracks == forked stereo (Twilio sends interleaved
    # `media.track` = "inbound" and "outbound" frames).
    assert stream.attrib["track"] == "both_tracks"


def test_twiml_dial_contains_forward_number():
    """
    Scenario: The <Dial> that bridges to the second human
        Given the TwiML output for a specific forwarding number
         When the document is parsed
         Then a <Dial> element exists at the top level
          And its inner text is that forwarding number verbatim

    Purpose:
        Without a correct <Dial>, the caller hears the greeting and
        the call hangs up - no second party, no two-way audio, and
        therefore no interesting multi-channel transcript. This
        catches an accidental swap between the forwarding number and
        the Twilio number.
    """
    # +44 7700 900xxx is Ofcom's reserved drama range - never a real subscriber.
    root = _parse(_build_twiml("wss://x/twilio", "+447700900123"))

    dial = root.find("./Dial")
    assert dial is not None

    # Number must be present verbatim, in E.164 form.
    assert dial.text == "+447700900123"


def test_twiml_say_present():
    """
    Scenario: The <Say> greeting spoken before the bridge
        Given the TwiML output for any inputs
         When the document is parsed
         Then a <Say> element exists at the top level
          And its text mentions "transcribed" (case-insensitive)

    Purpose:
        Two reasons to keep the greeting present:
          - Legal / consent: in many jurisdictions callers must be
            told a call is being transcribed or recorded.
          - Ops signal: hearing the greeting is a fast sanity check
            that Twilio successfully reached /twiml and executed the
            TwiML. Silent connect => you know something's wrong.
    """
    root = _parse(_build_twiml("wss://x/twilio", "+441234567890"))

    say = root.find("./Say")
    assert say is not None
    assert say.text and "transcribed" in say.text.lower()
