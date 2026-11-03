/**
 * Twilio Voice <-> Speechmatics Realtime proxy server (multi-channel).
 *
 * Node.js/TypeScript sibling of ../proxy.py. Bridges Twilio Media Streams to
 * Speechmatics' Realtime API using channel diarization: each call produces
 * two per-speaker transcript streams (inbound / outbound), transcribed via
 * a single Speechmatics RT multi-channel session.
 *
 * Run:
 *     pnpm start          (or `npm start`)
 *
 * The server listens on localhost:<PORT>/twilio and, on startup, opens an
 * ngrok tunnel and points the configured Twilio number's Voice URL at the
 * tunnel's /twiml endpoint. Dial the Twilio number to trigger a call.
 *
 * Environment variables (loaded from ../.env):
 *   SPEECHMATICS_API_KEY   Speechmatics Realtime API key.
 *   NGROK_AUTHTOKEN        ngrok auth token (for the public tunnel).
 *   TWILIO_ACCOUNT_SID     Twilio Account SID.
 *   TWILIO_AUTH_TOKEN      Twilio Auth Token.
 *   TWILIO_PHONE_NUMBER    E.164 number to auto-configure.
 *   FORWARD_TO_NUMBER      E.164 number the <Dial> bridges to.
 *   SM_TWILIO_PROXY_PORT   Optional. Local server port. Default: 5000.
 */

import http from "node:http";
import { fileURLToPath } from "node:url";
import path from "node:path";

import dotenv from "dotenv";
import ngrok from "@ngrok/ngrok";
import { WebSocketServer, type WebSocket } from "ws";
import { createSpeechmaticsJWT } from "@speechmatics/auth";
import {
  RealtimeClient,
  type AddChannelAudio,
  type EndOfChannel,
  type RealtimeClientMessage,
  type RealtimeTranscriptionConfig,
  type StartRecognition,
} from "@speechmatics/real-time-client";
import twilio from "twilio";

// Load ../.env so JS and Python demos can share the same secrets file.
const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, "..", ".env") });

// ─────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────

const SM_API_KEY = process.env.SPEECHMATICS_API_KEY;
const NGROK_AUTHTOKEN = process.env.NGROK_AUTHTOKEN;
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN;
const TWILIO_PHONE_NUMBER = process.env.TWILIO_PHONE_NUMBER;
const FORWARD_TO_NUMBER = process.env.FORWARD_TO_NUMBER;
const PORT = Number.parseInt(process.env.SM_TWILIO_PROXY_PORT ?? "5000", 10);

const REQUIRED_ENV_VARS = [
  "SPEECHMATICS_API_KEY",
  "NGROK_AUTHTOKEN",
  "TWILIO_ACCOUNT_SID",
  "TWILIO_AUTH_TOKEN",
  "TWILIO_PHONE_NUMBER",
  "FORWARD_TO_NUMBER",
] as const;

// Twilio Media Streams sends mulaw 8 kHz mono in 20 ms frames (160 bytes
// each), base64-encoded inside JSON. Speechmatics Realtime accepts this
// natively - no transcoding.
const AUDIO_FORMAT = {
  type: "raw",
  encoding: "mulaw",
  sample_rate: 8000,
} as const;

// The channel IDs we register with Speechmatics match Twilio's own track
// labels so routing is a direct lookup - no translation table needed.
export const CHANNELS = ["inbound", "outbound"] as const;
export type Channel = (typeof CHANNELS)[number];

const LANGUAGE = "en";

// Twilio starts shipping media ~20 ms after the "start" event, but fetching
// a JWT and completing the Speechmatics WebSocket handshake takes hundreds
// of milliseconds. Frames that arrive in that gap are buffered per channel
// (see CallSession.enqueue) and flushed once the session is ready.
//
// Cap: 8 kHz mulaw is 8 KB/s/channel = 50 x 160-byte frames/s, so 750
// frames is 15 s of audio - comfortably longer than any start-up race the
// SDK can still win (it abandons StartRecognition after a 10 s timeout,
// RealtimeClientOptions.connectionTimeout) while bounding a session that
// never connects to ~165 KB of base64 per channel instead of unbounded
// growth for the life of the socket.
const MAX_BUFFERED_FRAMES_PER_CHANNEL = 750;

// ─────────────────────────────────────────────────────────────────────
// Env validation
// ─────────────────────────────────────────────────────────────────────

function checkEnvVars(): void {
  const missing = REQUIRED_ENV_VARS.filter((name) => !process.env[name]);
  if (missing.length > 0) {
    console.error(`Missing required env vars in .env: ${missing.join(", ")}`);
    process.exit(1);
  }
}

// ─────────────────────────────────────────────────────────────────────
// MultiChannelRealtimeClient
//
// The JS SDK's RealtimeClient exposes sendAudio(binary) for single-channel
// raw audio, but no public sendChannelAudio(channel, base64). The
// multi-channel protocol *is* fully supported by the server - we just have
// to construct AddChannelAudio JSON messages ourselves. The SDK's own
// sendMessage() is private, so this subclass provides thin, typed helpers.
// ─────────────────────────────────────────────────────────────────────

interface PrivateSend {
  sendMessage(msg: RealtimeClientMessage): void;
}

class MultiChannelRealtimeClient extends RealtimeClient {
  sendChannelAudio(channel: string, base64Data: string): void {
    const msg: AddChannelAudio = {
      message: "AddChannelAudio",
      channel,
      data: base64Data,
    };
    (this as unknown as PrivateSend).sendMessage(msg);
  }

  endChannel(channel: string, lastSeqNo: number): void {
    const msg: EndOfChannel = {
      message: "EndOfChannel",
      channel,
      last_seq_no: lastSeqNo,
    };
    (this as unknown as PrivateSend).sendMessage(msg);
  }
}

// ─────────────────────────────────────────────────────────────────────
// CallSession - one Speechmatics session covering both Twilio tracks
// ─────────────────────────────────────────────────────────────────────

export class CallSession {
  private client: MultiChannelRealtimeClient | null = null;
  private closed = false;
  private lastFinalChannel: string | null = null;
  private readonly seqNo: Record<Channel, number> = { inbound: 0, outbound: 0 };
  private startPromise: Promise<void> | null = null;
  private eotResolve: (() => void) | null = null;
  private readonly eotPromise: Promise<void>;

  // Per-channel FIFO of base64 frames that arrived before the RT socket was
  // usable. These queues exist from construction - the same property that
  // makes ../python/proxy.py immune, where AsyncQueueSource is built in
  // CallSession.__init__ and buffers until the SDK's reader drains it.
  private readonly pending: Record<Channel, string[]> = { inbound: [], outbound: [] };

  // True only between RecognitionStarted and the socket closing: the one
  // window in which sendChannelAudio() is guaranteed not to throw.
  private ready = false;

  // Latched when the session can never become usable (connect/start failed,
  // socket closed, or a send threw). Further frames are dropped, loudly,
  // rather than buffered forever.
  private failed = false;

  private readonly dropped: Record<Channel, number> = { inbound: 0, outbound: 0 };

  constructor() {
    // Track EndOfTranscript arrival so close() can wait for the server
    // to flush any final results before disconnecting.
    this.eotPromise = new Promise<void>((resolve) => {
      this.eotResolve = resolve;
    });
  }

  /** Kick off the Speechmatics RT connection. Non-blocking. */
  start(): void {
    this.startPromise = this.run();
  }

  /**
   * Route a base64-encoded audio frame from Twilio to Speechmatics.
   *
   * Never throws, and never drops a frame merely for being early. Twilio's
   * first media frame lands ~20 ms after "start", long before the JWT fetch
   * and WebSocket handshake finish, so anything arriving before the session
   * is ready is queued and flushed in order by drainPending(). Sending
   * eagerly instead would throw "WebSocket is not open: readyState 0
   * (CONNECTING)" from inside a ws listener and take the process down.
   */
  enqueue(track: string, base64Payload: string): void {
    if (this.closed || !this.isKnownChannel(track)) {
      return;
    }
    if (this.ready) {
      // A frame the socket refuses is still a frame lost - count it, so
      // close()'s tally reflects every gap in the transcript.
      if (!this.sendFrame(track, base64Payload)) {
        this.dropped[track] += 1;
      }
      return;
    }
    if (this.failed || this.pending[track].length >= MAX_BUFFERED_FRAMES_PER_CHANNEL) {
      this.noteDrop(track);
      return;
    }
    this.pending[track].push(base64Payload);
  }

  /**
   * Cleanly shut down the RT session: EndOfChannel per track, wait for
   * EndOfTranscript so any last finals print, then disconnect.
   */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;

    // Let the start sequence settle before tearing down. On a very short
    // call the socket may still be coming up with frames queued behind it;
    // EndOfChannel has to follow that audio, not precede it. This mirrors
    // python's close(), which signals EOF on the queues and then awaits the
    // reader task. Bounded by the SDK's 10 s StartRecognition timeout.
    if (this.startPromise) {
      await this.startPromise.catch(() => undefined);
    }

    if (this.client) {
      // Anything still queued goes out now, before the seq_no below is read.
      this.drainPending();

      for (const channel of CHANNELS) {
        try {
          this.client.endChannel(channel, this.seqNo[channel]);
        } catch {
          // Socket may already be closed - non-fatal.
        }
      }

      // Wait up to 10s for the server to flush the last transcripts
      // (EndOfTranscript arrives after all EndOfChannel are ack'd).
      await Promise.race([
        this.eotPromise,
        new Promise<void>((resolve) => setTimeout(resolve, 10_000)),
      ]);

      try {
        await this.client.stopRecognition({ noTimeout: true });
      } catch {
        // Already-closed sockets throw; safe to swallow on teardown.
      }

      process.stdout.write("\n");
    }

    const lost = this.dropped.inbound + this.dropped.outbound;
    if (lost > 0) {
      console.error(
        `[speechmatics] call ended having dropped ${lost} audio frame(s) ` +
          `(inbound=${this.dropped.inbound}, outbound=${this.dropped.outbound}).`,
      );
    }
  }

  private isKnownChannel(track: string): track is Channel {
    return (CHANNELS as readonly string[]).includes(track);
  }

  /**
   * Send one frame and advance that channel's sequence counter.
   *
   * seqNo counts frames the server has actually been *sent*, not frames
   * received from Twilio, because close() passes it as
   * EndOfChannel.last_seq_no - counting buffered-but-unsent or dropped
   * frames would leave the server waiting for audio that never arrives.
   *
   * Returns false (rather than throwing) if the socket rejected the frame:
   * callers run inside a ws "message" listener, where a throw escapes the
   * promise chain and becomes a fatal uncaught exception.
   */
  private sendFrame(channel: Channel, base64Payload: string): boolean {
    try {
      this.client?.sendChannelAudio(channel, base64Payload);
      this.seqNo[channel] += 1;
      return true;
    } catch (err) {
      this.ready = false;
      this.failed = true;
      console.error(
        `[speechmatics] ${channel}: send failed, abandoning audio for this call - ${
          err instanceof Error ? err.message : String(err)
        }`,
      );
      return false;
    }
  }

  /**
   * Deliver every buffered frame in arrival order, or account for whatever
   * cannot be delivered. Deliberately synchronous: no await between two
   * frames means a concurrent enqueue() can never interleave itself into
   * the middle of a flush, so per-channel ordering holds exactly.
   */
  private drainPending(): void {
    for (const channel of CHANNELS) {
      const queue = this.pending[channel];
      this.pending[channel] = [];

      let sent = 0;
      while (sent < queue.length && this.ready && this.sendFrame(channel, queue[sent])) {
        sent += 1;
      }
      if (sent > 0) {
        console.log(`[speechmatics] ${channel}: flushed ${sent} buffered frame(s)`);
      }
      for (let i = sent; i < queue.length; i += 1) {
        this.noteDrop(channel);
      }
    }
  }

  /**
   * Count a dropped frame, shouting once per channel. At 50 frames/s a
   * per-frame log would bury every other line in the console, but silence
   * would hide real audio loss - so the first drop is loud and the total
   * is reported again by close().
   */
  private noteDrop(channel: Channel): void {
    this.dropped[channel] += 1;
    if (this.dropped[channel] === 1) {
      console.error(
        `[speechmatics] ${channel}: dropping audio - session unusable or buffer full ` +
          `(${MAX_BUFFERED_FRAMES_PER_CHANNEL} frames / ` +
          `${(MAX_BUFFERED_FRAMES_PER_CHANNEL * 20) / 1000}s). ` +
          `Transcripts for this call will have gaps.`,
      );
    }
  }

  private async run(): Promise<void> {
    if (!SM_API_KEY) throw new Error("SPEECHMATICS_API_KEY missing at runtime");

    const jwt = await createSpeechmaticsJWT({
      type: "rt",
      apiKey: SM_API_KEY,
      ttl: 60,
    });

    this.client = new MultiChannelRealtimeClient();

    // The SDK dispatches socketStateChange when the socket is created, on
    // error and on close - but *not* on open (see the RealtimeClient
    // constructor in the installed SDK). It is therefore only useful as a
    // "stop sending" signal; readiness comes from start() resolving below.
    this.client.addEventListener("socketStateChange", ({ socketState }) => {
      if (socketState === "closing" || socketState === "closed") {
        this.ready = false;
        this.failed = true;
      }
    });

    this.client.addEventListener("receiveMessage", ({ data }) => {
      if (data.message === "EndOfTranscript") {
        this.eotResolve?.();
        return;
      }
      if (data.message === "AddPartialTranscript") {
        // Stub - wire partials here if you want to render them (live UI,
        // LLM prompt, agent-assist, etc.). Example:
        //   const text = data.metadata.transcript;
        //   const channel = data.channel ?? "?";
        return;
      }
      if (data.message === "AddTranscript") {
        this.handleFinal(data);
        return;
      }
      if (data.message === "Error") {
        console.error(`Speechmatics error: ${data.type} - ${data.reason ?? ""}`);
      }
    });

    const startMsg: RealtimeTranscriptionConfig = {
      transcription_config: {
        language: LANGUAGE,
        max_delay: 0.7,
        enable_partials: true,
        model: "enhanced",
        diarization: "channel",
        channel_diarization_labels: [...CHANNELS],
      },
      audio_format: AUDIO_FORMAT,
    };

    try {
      // start() resolves only once the server answers StartRecognition with
      // RecognitionStarted, which means the socket is OPEN and the session
      // is configured. That is the SDK's only trustworthy readiness signal,
      // so it is the moment we unlock direct sends and flush the backlog.
      await this.client.start(jwt, startMsg);
      this.ready = true;
      this.drainPending();
    } catch (err) {
      this.ready = false;
      this.failed = true;
      if (!this.closed) {
        console.error(
          `Speechmatics session error: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
      // Account for the backlog we will never be able to send.
      this.drainPending();
    }
  }

  private handleFinal(msg: { metadata: { transcript: string }; channel?: string }): void {
    // metadata.transcript is the SDK's pre-formatted text. Joining results[]
    // by hand would put a space before every punctuation result ("today ?").
    // It is built for concatenation: each chunk already carries its own
    // trailing space, so consecutive chunks are appended with no separator.
    const text = msg.metadata.transcript;
    if (!text) return;

    const channel = msg.channel ?? "?";
    const label = `[${channel.padEnd(8)}]`;

    if (this.lastFinalChannel === null) {
      process.stdout.write(`${label} ${text}`);
    } else if (channel !== this.lastFinalChannel) {
      process.stdout.write(`\n${label} ${text}`);
    } else {
      process.stdout.write(text);
    }
    this.lastFinalChannel = channel;
  }
}

// ─────────────────────────────────────────────────────────────────────
// Twilio Media Streams WebSocket handler
// ─────────────────────────────────────────────────────────────────────

interface TwilioMediaEvent {
  event: string;
  start?: {
    streamSid?: string;
    callSid?: string;
    tracks?: string[];
  };
  media?: {
    track?: string;
    payload?: string;
  };
}

export async function handleTwilioCall(ws: WebSocket): Promise<void> {
  // Wrap the session in an object so TypeScript's control-flow analysis
  // doesn't wrongly infer "never assigned" from the outer scope - reads
  // via `.session` correctly narrow after the callback mutates it.
  const state: { session: CallSession | null; streamSid?: string } = {
    session: null,
  };

  const closed = new Promise<void>((resolve) => {
    ws.on("close", () => resolve());
    ws.on("error", () => resolve());
  });

  ws.on("message", (raw: Buffer) => {
    let message: TwilioMediaEvent;
    try {
      message = JSON.parse(raw.toString("utf8"));
    } catch {
      return;
    }

    // This listener is invoked synchronously by ws's event emitter, so a
    // throw in here does NOT reach the .catch() on handleTwilioCall's
    // promise - it becomes an uncaught exception and kills the process,
    // dropping every other call in flight. One bad event must cost us that
    // event only, so the whole body is guarded and the error is logged
    // rather than swallowed.
    try {
      switch (message.event) {
        case "connected":
          console.log("[twilio] protocol handshake ok");
          break;
        case "start": {
          state.streamSid = message.start?.streamSid;
          console.log(
            `[twilio] call started - stream=${state.streamSid} call=${message.start?.callSid} tracks=${JSON.stringify(message.start?.tracks)}`,
          );
          state.session = new CallSession();
          state.session.start();
          break;
        }
        case "media": {
          if (!state.session || !message.media?.payload) return;
          // Twilio's `media.track` is always bare "inbound"/"outbound" -
          // the "_track" suffix only appears in the TwiML attribute.
          const track = message.media.track ?? "inbound";
          // Base64 pass-through: Twilio's payload feeds AddChannelAudio.data
          // as-is, no decode/re-encode needed.
          state.session.enqueue(track, message.media.payload);
          break;
        }
        case "stop":
          console.log(`[twilio] call stopped (stream=${state.streamSid})`);
          ws.close();
          break;
      }
    } catch (err) {
      console.error(
        `[twilio] error handling "${message.event}" event: ${
          err instanceof Error ? err.message : String(err)
        }`,
      );
    }
  });

  await closed;
  if (state.session) {
    await state.session.close();
  }
}

// ─────────────────────────────────────────────────────────────────────
// ngrok tunnel + TwiML + Twilio number configuration
// ─────────────────────────────────────────────────────────────────────

async function startNgrokTunnel(): Promise<string> {
  const listener = await ngrok.forward({
    addr: PORT,
    authtoken: NGROK_AUTHTOKEN,
    proto: "http",
  });
  const url = listener.url();
  if (!url) throw new Error("ngrok tunnel returned no URL");
  return url;
}

export function buildTwiml(streamUrl: string, forwardTo: string): string {
  const response = new twilio.twiml.VoiceResponse();
  const start = response.start();
  start.stream({ url: streamUrl, track: "both_tracks" });
  response.say(
    { voice: "woman", language: "en-GB" },
    "This call is being transcribed.",
  );
  response.dial(forwardTo);
  return response.toString();
}

async function configureTwilioNumber(twimlUrl: string): Promise<void> {
  const client = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
  // The `phoneNumber` filter is a substring/wildcard match; verify locally
  // that we pick the exact E.164 the user configured.
  const numbers = await client.incomingPhoneNumbers.list({
    phoneNumber: TWILIO_PHONE_NUMBER,
  });
  const match = numbers.find((n) => n.phoneNumber === TWILIO_PHONE_NUMBER);
  if (!match) {
    console.error(`Twilio number ${TWILIO_PHONE_NUMBER} not found on this account.`);
    process.exit(1);
  }
  await client.incomingPhoneNumbers(match.sid).update({
    voiceUrl: twimlUrl,
    voiceMethod: "GET",
  });
}

// ─────────────────────────────────────────────────────────────────────
// HTTP + WS server
// ─────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  checkEnvVars();

  const baseUrl = await startNgrokTunnel();
  const wssUrl = baseUrl.replace(/^https:/, "wss:") + "/twilio";
  const twimlBody = buildTwiml(wssUrl, FORWARD_TO_NUMBER as string);
  await configureTwilioNumber(baseUrl + "/twiml");

  const httpServer = http.createServer((req, res) => {
    const url = req.url ?? "/";
    if (url.split("?", 1)[0] === "/twiml") {
      res.writeHead(200, {
        "Content-Type": "application/xml",
        "Content-Length": Buffer.byteLength(twimlBody).toString(),
      });
      res.end(twimlBody);
      return;
    }
    res.writeHead(404);
    res.end();
  });

  const wss = new WebSocketServer({ noServer: true });
  httpServer.on("upgrade", (request, socket, head) => {
    const url = request.url ?? "";
    if (url.split("?", 1)[0] !== "/twilio") {
      socket.destroy();
      return;
    }
    wss.handleUpgrade(request, socket, head, (ws) => {
      console.log("[twilio] incoming Media Streams connection");
      handleTwilioCall(ws).catch((err) => {
        console.error(`Handler error: ${err instanceof Error ? err.message : String(err)}`);
      });
    });
  });

  await new Promise<void>((resolve) => {
    httpServer.listen(PORT, "localhost", () => resolve());
  });

  console.log(`\nListening on ws://localhost:${PORT}/twilio`);
  console.log(
    `Twilio number ${TWILIO_PHONE_NUMBER} pointed at ${wssUrl}. Call it now.\n`,
  );

  // Block forever until SIGINT.
  await new Promise<void>((resolve) => {
    process.on("SIGINT", () => {
      console.log("\nShutting down.");
      resolve();
    });
  });

  await new Promise<void>((resolve) => httpServer.close(() => resolve()));
}

// Only run main() when this file is executed directly (not when imported
// by tests).
const invokedDirectly =
  process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);

if (invokedDirectly) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
