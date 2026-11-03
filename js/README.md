# Speechmatics ↔ Twilio Voice — Node.js / TypeScript Reference Integration

Bridges a Twilio Voice phone call to Speechmatics Realtime and prints
per-speaker transcripts to your local console.

> For the Python sibling, see [`../python/README.md`](../python/README.md).

This is a **reference implementation** for engineers and developers looking
to add Speechmatics transcription to a Twilio-powered voice product using
the [`@speechmatics/real-time-client`](https://www.npmjs.com/package/@speechmatics/real-time-client)
JS SDK. Both sides of the call are transcribed via Speechmatics' multi-channel
diarization, so you get a clean `Inbound` vs `Outbound` split without any
client-side audio mixing.

See the [root README](../README.md#architecture) for the end-to-end
diagram of Caller ↔ Twilio ↔ proxy ↔ Speechmatics.

## Prerequisites

- Node.js 20 or newer
- A [Speechmatics account](https://portal.speechmatics.com/) and API key
- A [Twilio account](https://www.twilio.com/) with a voice-capable phone number
- An [ngrok](https://ngrok.com/) account and auth token (used to expose your
  local server to Twilio)

## Setup

```bash
# 1. Clone and enter the js/ folder
git clone <this-repo>
cd speechmatics-twilio-integration/js

# 2. Install dependencies
npm install

# 3. Copy the env template at the repo root and fill in your keys.
#    (Both the Python and Node implementations read from ../.env.)
cp ../.env.example ../.env
$EDITOR ../.env
```

## Run

```bash
npm start
```

On startup `proxy.ts` will:

1. Open an ngrok tunnel.
2. Serve a `/twiml` endpoint on that tunnel that returns the required
   `<Start><Stream>` + short greeting + `<Dial>` to `FORWARD_TO_NUMBER`.
3. Update your Twilio number's Voice URL to point at that `/twiml`.

Dial the Twilio number from any phone; `FORWARD_TO_NUMBER` rings; answer
it, and the two-party conversation is transcribed live to the console.

The Voice URL stays pointed at the last ngrok tunnel after Ctrl-C — the
next `npm start` overwrites it with the fresh URL. If you need Twilio to
route calls elsewhere while the proxy isn't running, set that in the Twilio
Console.

## What you should see

```
[twilio] incoming Media Streams connection
[twilio] protocol handshake ok
[twilio] call started - stream=MZxxx... call=CAxxx... tracks=["inbound","outbound"]
[outbound] This call is being transcribed.
[inbound ] Hello. Can you hear me?
[outbound] Hey, hey. Yes, loud and clear.
[inbound ] Awesome.
[twilio] call stopped (stream=MZxxx...)
```

- **inbound** = the person who called the Twilio number.
- **outbound** = audio Twilio sends to the caller.
- **Transcripts from the two sides interleave** as they arrive, so a
  sentence can be split across lines. Closing punctuation may land on a later line.

## Key design choices

- **Multi-channel diarization**, not two separate sessions. One
  `MultiChannelRealtimeClient` handles both tracks over a single WebSocket
  and tags each transcript with its channel label.
- **`MultiChannelRealtimeClient`** is a thin subclass of the SDK's
  `RealtimeClient` that exposes `sendChannelAudio(channel, base64)` and
  `endChannel(...)` — the underlying `AddChannelAudio` / `EndOfChannel`
  wire messages are supported by the SDK types but not yet by a public
  method. The subclass is <10 lines.
- **Base64 pass-through, zero decoding.** Twilio's `media.payload` is
  already base64; that's exactly the shape `AddChannelAudio.data` wants,
  so frames go straight through with no CPU work.
- **μ-law forwarded natively**, no transcoding. Twilio ships μ-law 8 kHz;
  Speechmatics accepts μ-law 8 kHz. One less pipeline stage.

## Tests

See [`tests/README.md`](tests/README.md) for the Vitest suite that
covers the integration requirements.

## Troubleshooting

- **`Missing required env vars in .env: ...`** — copy `../.env.example`
  to `../.env` at the repo root and fill in the listed values.
- **`Twilio number ... not found on this account`** — check that
  `TWILIO_PHONE_NUMBER` is in E.164 form and belongs to the account whose
  SID/token you provided.
