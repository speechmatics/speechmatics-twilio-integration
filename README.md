# Speechmatics ↔ Twilio Voice — Reference Integration

A local proxy that bridges a Twilio Voice phone call to Speechmatics Realtime
and prints per-speaker transcripts to your console. Both sides of a bridged
two-party call are transcribed via Speechmatics' multi-channel diarization —
you get a clean `Inbound` vs `Outbound` split with no client-side audio mixing.

This repo ships two identical-behaviour implementations. Pick the one that
matches your stack and follow the README in that folder:

| Language | Folder | Start here |
|---|---|---|
| Python | [`python/`](python/) | [`python/README.md`](python/README.md) |
| Node.js / TypeScript | [`js/`](js/) | [`js/README.md`](js/README.md) |

## Architecture

```
  ┌──────────┐      ┌────────┐      ┌──────────┐
  │ Caller A │◀──══▶│ Twilio │◀──══▶│ Caller B │      ═══  bridged voice
  └──────────┘      └────┬───┘      └──────────┘           (PSTN)
                         │
                         │  audio fork  (both_tracks, μ-law 8 kHz)
                         ▼
                 ┌───────────────┐
                 │ proxy.py /    │
                 │ proxy.ts      │
                 └───────┬───────┘
                         │
                         │  Realtime WebSocket
                         ▼
             ┌───────────────────────┐
             │ Speechmatics Realtime │
             │ (multi-channel)       │
             └───────────┬───────────┘
                         │
                         │  per-channel transcripts
                         ▼
                    ┌─────────┐
                    │ console │
                    └─────────┘
```

- **Caller A** dials the Twilio number; Twilio bridges the call to
  **Caller B** via TwiML `<Dial>` — two humans talk normally.
- Twilio simultaneously **forks** the live call audio (both tracks) to
  the proxy over WebSocket in Twilio's native μ-law 8 kHz format.
- The proxy relays those frames to Speechmatics Realtime — **no
  transcoding**, no re-encoding.
- Speechmatics returns per-channel transcripts tagged with the track
  they came from; the proxy prints the finals to the console. Consecutive
  finals on one channel continue on the same line, and a change of channel starts a new line. Partial transcripts are requested but not printed - the handlers are stubs for you to wire in.

The language-specific READMEs cover prerequisites, setup, `.env` variables,
what to expect at runtime, tests, and troubleshooting.
