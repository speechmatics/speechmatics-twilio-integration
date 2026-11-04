"""
Twilio Voice <-> Speechmatics Realtime proxy server (multi-channel).

Bridges Twilio Media Streams to Speechmatics' Realtime API using channel
diarization. This demo uses `track="both_tracks"`, so each call produces
two per-speaker transcript streams (Inbound / Outbound), transcribed
via a single Speechmatics RT multi-channel session.

Run:
    python proxy.py

The server listens on localhost:<PORT>/twilio and, on startup, opens
an ngrok tunnel and points the configured Twilio number's Voice URL at
the tunnel's /twiml endpoint. Dial the Twilio number to trigger a call.
"""

import asyncio
import base64
import binascii
import json
import os
import sys
from http import HTTPStatus
from typing import Optional

import ngrok
import websockets
from dotenv import load_dotenv
from speechmatics.rt import (
    AsyncMultiChannelClient,
    AudioEncoding,
    AudioFormat,
    AuthenticationError,
    JWTAuth,
    Model,
    ServerMessageType,
    TranscriptionConfig,
    TranscriptResult,
)
from twilio.rest import Client as TwilioClient
from twilio.twiml.voice_response import VoiceResponse

load_dotenv()

SM_API_KEY = os.getenv("SPEECHMATICS_API_KEY")
NGROK_AUTHTOKEN = os.getenv("NGROK_AUTHTOKEN")
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER")
FORWARD_TO_NUMBER = os.getenv("FORWARD_TO_NUMBER")
PORT = int(os.getenv("SM_TWILIO_PROXY_PORT", "5000"))
CHANNELS = ["inbound", "outbound"]
LANGUAGE = "en"

ENV_VARS = (
    "SPEECHMATICS_API_KEY",
    "NGROK_AUTHTOKEN",
    "TWILIO_ACCOUNT_SID",
    "TWILIO_AUTH_TOKEN",
    "TWILIO_PHONE_NUMBER",
    "FORWARD_TO_NUMBER",
)

# Twilio Media Streams sends mu-law 8 kHz mono in 20ms frames (160 bytes each),
# base64-encoded inside JSON. Speechmatics Realtime accepts this natively.
TWILIO_AUDIO_FORMAT = AudioFormat(
    encoding=AudioEncoding.MULAW,
    sample_rate=8000,
    chunk_size=160,
)


class AsyncQueueSource:
    """File-like async source backed by an asyncio.Queue of audio frames.

    read() blocks until data is available, returning empty bytes only on
    explicit close() - otherwise the multi-channel SDK would treat a
    stall as EOF and tear down the whole session.
    """

    def __init__(self) -> None:
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._buffer = b""
        self._closed = False

    def enqueue(self, data: bytes) -> None:
        if data and not self._closed:
            self._queue.put_nowait(data)

    def close(self) -> None:
        # Signal EOF; the sentinel unblocks any pending read().
        self._closed = True
        self._queue.put_nowait(b"")

    async def read(self, n: int) -> bytes:
        while len(self._buffer) < n:
            chunk = await self._queue.get()
            if not chunk:
                # EOF sentinel. Put it back so subsequent reads also see EOF,
                # then return whatever we have (may be a short read or empty).
                self._queue.put_nowait(b"")
                break
            self._buffer += chunk

        out, self._buffer = self._buffer[:n], self._buffer[n:]
        return out


class CallSession:
    """One Speechmatics multi-channel session covering both Twilio tracks."""

    def __init__(self) -> None:
        self.sources: dict[str, AsyncQueueSource] = {
            channel: AsyncQueueSource() for channel in CHANNELS
        }
        self._closed = False
        self._task: Optional[asyncio.Task] = None
        # Track the last speaker so consecutive finals from the same channel
        # continue on one line and speaker-changes start a new line.
        self._last_final_channel: Optional[str] = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    def enqueue(self, track: str, frame: bytes) -> None:
        if self._closed or track not in self.sources:
            return
        self.sources[track].enqueue(frame)

    async def close(self) -> None:
        self._closed = True
        for source in self.sources.values():
            source.close()
        if self._task:
            await self._task

    def _print_final(self, channel: str, text: str) -> None:
        """Print one final transcript chunk under its channel label."""
        if self._last_final_channel is None:
            sys.stdout.write(f"[{channel:<8}] {text}")
        elif channel != self._last_final_channel:
            sys.stdout.write(f"\n[{channel:<8}] {text}")
        else:
            sys.stdout.write(text)
        self._last_final_channel = channel
        sys.stdout.flush()

    async def _run(self) -> None:
        transcription_config = TranscriptionConfig(
            language=LANGUAGE,
            max_delay=0.7,
            enable_partials=True,
            model=Model.ENHANCED,
            diarization="channel",
            channel_diarization_labels=CHANNELS,
        )

        try:
            async with AsyncMultiChannelClient(api_key=SM_API_KEY) as client:
                # transcribe() returns as soon as the sources hit EOF
                # Server still needs a moment to flush its final transcripts.
                eot = asyncio.Event()

                @client.on(ServerMessageType.END_OF_TRANSCRIPT)
                def _on_eot(_msg):
                    eot.set()

                @client.on(ServerMessageType.ADD_PARTIAL_TRANSCRIPT)
                def _on_partial(_msg):
                    # Stub. Wire partials here if wanted.
                    pass

                @client.on(ServerMessageType.ADD_TRANSCRIPT)
                def _on_final(msg):
                    text = TranscriptResult.from_message(msg).metadata.transcript
                    channel = msg.get("channel", "?")
                    if not text:
                        return
                    self._print_final(channel, text)

                await client.transcribe(
                    sources=self.sources,
                    transcription_config=transcription_config,
                    audio_format=TWILIO_AUDIO_FORMAT,
                )
                try:
                    await asyncio.wait_for(eot.wait(), timeout=10.0)
                except asyncio.TimeoutError:
                    print("Timed out waiting for END_OF_TRANSCRIPT", file=sys.stderr)
                sys.stdout.write("\n")
                sys.stdout.flush()
        except AuthenticationError as e:
            print(f"Speechmatics auth error: {e}", file=sys.stderr)
        except Exception as e:  # noqa: BLE001
            if not self._closed:
                print(
                    f"Speechmatics session error: {type(e).__name__}: {e}",
                    file=sys.stderr,
                )
        finally:
            # Stop accepting further audio from Twilio; the RT connection
            # already closed on the way out of the `async with` above.
            self._closed = True
            for source in self.sources.values():
                source.close()


async def handle_twilio_call(ws) -> None:
    """Consume a Twilio Media Streams WebSocket and pump audio into a session."""
    session: Optional[CallSession] = None
    stream_sid: Optional[str] = None
    bad_frames = 0

    try:
        async for raw in ws:
            message = json.loads(raw)
            event = message.get("event")

            if event == "connected":
                print("[twilio] protocol handshake ok")

            elif event == "start":
                start = message["start"]
                stream_sid = start.get("streamSid")
                print(
                    f"[twilio] call started - stream={stream_sid} "
                    f"call={start.get('callSid')} tracks={start.get('tracks')}"
                )
                session = CallSession()
                session.start()

            elif event == "media" and session is not None:
                media = message["media"]
                # Twilio's runtime `media.track` is always bare "inbound"/"outbound"
                payload = media.get("payload")
                if payload:
                    try:
                        frame = base64.b64decode(payload)
                    except (binascii.Error, ValueError, TypeError) as e:
                        bad_frames += 1
                        if bad_frames == 1:
                            print(
                                f"[twilio] dropping undecodable audio frame ({e}); "
                                "further bad frames are counted, not logged",
                                file=sys.stderr,
                            )
                        continue
                    session.enqueue(media.get("track", "inbound"), frame)

            elif event == "stop":
                print(f"[twilio] call stopped (stream={stream_sid})")
                break

    except websockets.exceptions.ConnectionClosed:
        print(f"[twilio] connection closed (stream={stream_sid})")
    finally:
        if bad_frames:
            print(
                f"[twilio] call ended having dropped {bad_frames} undecodable "
                f"audio frame(s) (stream={stream_sid})",
                file=sys.stderr,
            )
        if session:
            await session.close()


async def ws_handler(websocket, _path):
    print("[twilio] incoming Media Streams connection")
    await handle_twilio_call(websocket)


def _check_env_vars() -> None:
    missing = [name for name in ENV_VARS if not os.getenv(name)]
    if missing:
        print(f"Missing required env vars in .env: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)


async def _check_speechmatics_key() -> None:
    """
    Prove the Speechmatics key works before anything else is set up.

    Without this, a bad key only surfaces on the first call - after ngrok is
    up and the Twilio number has been repointed - so everything looks healthy
    except the one thing that matters. Minting a short-lived token is the
    cheapest request that rejects an invalid key. The token is discarded.
    """
    try:
        await JWTAuth(SM_API_KEY, ttl=60).get_auth_headers()
    except AuthenticationError as e:
        print(
            f"Could not authenticate with Speechmatics: {e}\n"
            "Check SPEECHMATICS_API_KEY in .env (and that mp.speechmatics.com is "
            "reachable). Nothing has been started and your Twilio number has not "
            "been changed.",
            file=sys.stderr,
        )
        sys.exit(1)


async def _start_ngrok_tunnel() -> str:
    """Returns the tunnel base URL (https://...)."""
    listener = await ngrok.forward(addr=PORT, authtoken=NGROK_AUTHTOKEN, proto="http")
    return listener.url()


def _build_twiml(stream_url: str, forward_to: str) -> bytes:
    """Build the TwiML Twilio will fetch when a call comes in."""
    response = VoiceResponse()
    start = response.start()
    start.stream(url=stream_url, track="both_tracks")
    response.say("This call is being transcribed.", voice="woman", language="en")
    response.dial(forward_to)
    return str(response).encode()


async def _configure_twilio_number(twiml_url: str) -> None:
    """Point the Twilio number's voice URL at our /twiml endpoint."""
    client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    numbers = await asyncio.to_thread(
        client.incoming_phone_numbers.list,
        phone_number=TWILIO_PHONE_NUMBER,
    )
    number = next((n for n in numbers if n.phone_number == TWILIO_PHONE_NUMBER), None)
    if number is None:
        print(f"Twilio number {TWILIO_PHONE_NUMBER} not found on this account.", file=sys.stderr)
        sys.exit(1)
    await asyncio.to_thread(
        client.incoming_phone_numbers(number.sid).update,
        voice_url=twiml_url,
        voice_method="GET",
    )


async def main() -> None:
    _check_env_vars()
    await _check_speechmatics_key()

    base_url = await _start_ngrok_tunnel()
    wss_url = base_url.replace("https://", "wss://") + "/twilio"
    twiml_body = _build_twiml(wss_url, FORWARD_TO_NUMBER)
    await _configure_twilio_number(base_url + "/twiml")

    async def _process_request(path, _headers):
        if path.split("?", 1)[0] == "/twiml":
            return (
                HTTPStatus.OK,
                [
                    ("Content-Type", "application/xml"),
                    ("Content-Length", str(len(twiml_body))),
                ],
                twiml_body,
            )
        return None

    async with websockets.serve(
        ws_handler, "localhost", PORT, process_request=_process_request
    ):
        print(f"\nListening on ws://localhost:{PORT}/twilio")
        print(f"Twilio number {TWILIO_PHONE_NUMBER} pointed at {wss_url}. Call it now.\n")
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down.")
