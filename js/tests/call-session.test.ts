/**
 * Feature: CallSession routing + audio wire-format invariants
 *
 *   1. Audio wire format: we forward Twilio's native mulaw / 8 kHz frames
 *      straight through, no transcoding.
 *   2. Track routing: each Twilio 'media' event carries a `track` label
 *      ("inbound" or "outbound"); frames must land in the Speechmatics
 *      channel that matches that label.
 *
 * These tests don't spin up a real Speechmatics session - we stub
 * MultiChannelRealtimeClient onto the session and assert on the calls it
 * receives.
 */

import { describe, expect, it, vi } from "vitest";

import { CallSession, CHANNELS } from "../proxy.js";

describe("CHANNELS constant", () => {
  it("matches Twilio's own track labels", () => {
    /**
     * Scenario: Channel identifiers registered with Speechmatics
     *   Given the module-level CHANNELS constant
     *    When it is inspected
     *    Then it equals ["inbound", "outbound"]
     *
     * Purpose: Twilio's `media.track` field is always the bare string
     * "inbound" or "outbound". Using the same values as Speechmatics
     * channel labels means routing is a direct lookup.
     */
    expect(CHANNELS).toEqual(["inbound", "outbound"]);
  });
});

describe("CallSession.enqueue", () => {
  function withStubbedClient() {
    /**
     * Build a CallSession with its internal RT client swapped for a
     * plain spy. We poke the private fields directly - this is a test
     * harness so escaping visibility is fine.
     *
     * `ready` is the flag the session sets once StartRecognition has been
     * acknowledged; a client alone is not enough to make sends safe, so
     * these tests set it to model an already-established session.
     */
    const session = new CallSession();
    const spies = {
      sendChannelAudio: vi.fn(),
      endChannel: vi.fn(),
      stopRecognition: vi.fn().mockResolvedValue(undefined),
    };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (session as any).client = spies;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (session as any).ready = true;
    return { session, spies };
  }

  it("routes inbound frames to the inbound channel", () => {
    /**
     * Scenario: An inbound-track frame is routed correctly
     *   Given a CallSession with a stubbed RT client
     *    When enqueue("inbound", base64Payload) is called
     *    Then sendChannelAudio was called once with ("inbound", base64Payload)
     *
     * Purpose: routing invariant for one direction.
     */
    const { session, spies } = withStubbedClient();
    session.enqueue("inbound", "AAA=");
    expect(spies.sendChannelAudio).toHaveBeenCalledWith("inbound", "AAA=");
  });

  it("routes outbound frames to the outbound channel", () => {
    /**
     * Scenario: An outbound-track frame is routed correctly
     *   Given a CallSession with a stubbed RT client
     *    When enqueue("outbound", base64Payload) is called
     *    Then sendChannelAudio was called once with ("outbound", base64Payload)
     *
     * Purpose: mirror of the inbound test - routing must be symmetric.
     */
    const { session, spies } = withStubbedClient();
    session.enqueue("outbound", "BBB=");
    expect(spies.sendChannelAudio).toHaveBeenCalledWith("outbound", "BBB=");
  });

  it("silently drops frames with an unknown track", () => {
    /**
     * Scenario: A frame tagged with an unknown track label
     *   Given a CallSession with a stubbed RT client
     *    When enqueue("bogus", base64Payload) is called
     *    Then no exception is raised
     *     And sendChannelAudio is never called
     *
     * Purpose: defensive against malformed Twilio messages or future
     * protocol changes that add a new track name.
     */
    const { session, spies } = withStubbedClient();
    expect(() => session.enqueue("bogus", "XXX=")).not.toThrow();
    expect(spies.sendChannelAudio).not.toHaveBeenCalled();
  });
});

describe("CallSession.enqueue - start-of-call race", () => {
  /**
   * Build the spy client without attaching it, so the session is in the
   * state it really occupies for the first few hundred milliseconds of a
   * call: constructed and started, but with no usable socket yet.
   */
  function stubClient() {
    return {
      sendChannelAudio: vi.fn(),
      endChannel: vi.fn(),
      stopRecognition: vi.fn().mockResolvedValue(undefined),
    };
  }

  /**
   * Promote a session to "ready" exactly the way run() does once
   * StartRecognition has been acknowledged: attach the client, flip the
   * flag, then flush the backlog.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  function becomeReady(session: CallSession, spies: any) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (session as any).client = spies;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (session as any).ready = true;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (session as any).drainPending();
  }

  it("buffers frames that arrive before the session is ready, losing none", () => {
    /**
     * Scenario: Twilio media arrives during the JWT fetch / RT handshake
     *   Given a CallSession whose RT client does not exist yet
     *    When five frames are enqueued on each track
     *    Then no frame reaches the client while it is unready
     *     And once the session becomes ready all ten are delivered
     *     And each channel receives its frames in arrival order
     *
     * Purpose: Twilio sends its first media frame ~20 ms after "start",
     * but createSpeechmaticsJWT() plus a TLS WebSocket handshake takes
     * hundreds of ms. Before this was buffered, enqueue() returned early
     * on a null client and silently binned the opening of every single
     * call. This is the ../python/proxy.py AsyncQueueSource guarantee -
     * the queues exist from construction, so early audio waits rather
     * than evaporating.
     */
    const session = new CallSession();
    const spies = stubClient();

    for (let i = 0; i < 5; i += 1) {
      session.enqueue("inbound", `in${i}`);
      session.enqueue("outbound", `out${i}`);
    }

    // Nothing can have been sent - there is no socket to send on.
    expect(spies.sendChannelAudio).not.toHaveBeenCalled();

    becomeReady(session, spies);

    expect(spies.sendChannelAudio).toHaveBeenCalledTimes(10);
    const byChannel = (channel: string) =>
      spies.sendChannelAudio.mock.calls
        .filter((call: unknown[]) => call[0] === channel)
        .map((call: unknown[]) => call[1]);
    expect(byChannel("inbound")).toEqual(["in0", "in1", "in2", "in3", "in4"]);
    expect(byChannel("outbound")).toEqual(["out0", "out1", "out2", "out3", "out4"]);
  });

  it("keeps seq_no aligned with what was actually sent", async () => {
    /**
     * Scenario: EndOfChannel after a buffered start
     *   Given three inbound and one outbound frame buffered pre-readiness
     *    When the session becomes ready and is then closed
     *    Then endChannel reports last_seq_no 3 for inbound and 1 for outbound
     *
     * Purpose: close() passes seqNo as EndOfChannel.last_seq_no, which the
     * server uses to know when a channel is complete. The counter must
     * therefore track frames the server was *sent*, not frames Twilio
     * handed us - a buffered frame counted before delivery would leave the
     * server waiting forever for audio that never went out.
     */
    const session = new CallSession();
    const spies = stubClient();

    session.enqueue("inbound", "a");
    session.enqueue("inbound", "b");
    session.enqueue("inbound", "c");
    session.enqueue("outbound", "d");

    becomeReady(session, spies);

    // Resolve EndOfTranscript so close() skips its 10-second fallback.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (session as any).eotResolve?.();
    await session.close();

    expect(spies.endChannel).toHaveBeenCalledWith("inbound", 3);
    expect(spies.endChannel).toHaveBeenCalledWith("outbound", 1);
  });

  it("never throws when the socket rejects a frame", () => {
    /**
     * Scenario: The RT socket is still CONNECTING when a frame is sent
     *   Given a ready session whose client throws "WebSocket is not open"
     *    When a frame is enqueued
     *    Then enqueue() does not throw
     *     And the failure is reported on the console
     *
     * Purpose: enqueue() is called from inside ws's synchronous
     * "message" listener, so anything it throws bypasses the promise
     * chain around handleTwilioCall and becomes an uncaught exception
     * that takes the whole process - and every concurrent call - down.
     * Failures must be logged, never raised, and never swallowed.
     */
    const session = new CallSession();
    const spies = stubClient();
    spies.sendChannelAudio.mockImplementation(() => {
      throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
    });
    const errorLog = vi.spyOn(console, "error").mockImplementation(() => undefined);

    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (session as any).client = spies;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (session as any).ready = true;

      expect(() => session.enqueue("inbound", "boom")).not.toThrow();
      expect(errorLog).toHaveBeenCalled();
    } finally {
      errorLog.mockRestore();
    }
  });

  it("bounds the buffer and complains when it overflows", () => {
    /**
     * Scenario: A session that never connects keeps receiving audio
     *   Given a CallSession that never becomes ready
     *    When far more frames are enqueued than the per-channel cap
     *    Then the buffer stops growing at the cap
     *     And the overflow is reported on the console, not hidden
     *
     * Purpose: a hung RT connection must not turn into an unbounded
     * memory leak - 8 kHz mulaw is 8 KB/s/channel and a Twilio socket can
     * stay open for hours. Dropping audio is bad, so it has to be loud.
     */
    const session = new CallSession();
    const errorLog = vi.spyOn(console, "error").mockImplementation(() => undefined);

    try {
      for (let i = 0; i < 1_000; i += 1) {
        session.enqueue("inbound", `f${i}`);
      }

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const buffered: string[] = (session as any).pending.inbound;
      expect(buffered.length).toBeLessThan(1_000);
      expect(buffered[0]).toBe("f0");
      expect(errorLog).toHaveBeenCalled();
    } finally {
      errorLog.mockRestore();
    }
  });
});

describe("CallSession.handleFinal", () => {
  it("prints the SDK's formatted transcript, not a space-joined results[]", () => {
    /**
     * Scenario: A final transcript containing punctuation results
     *   Given an AddTranscript whose results include separate punctuation
     *         entries and whose metadata.transcript is already formatted
     *    When the session handles it
     *    Then the console shows "Hello there. How are you?"
     *     And there is no space before the full stop or question mark
     *
     * Purpose: punctuation arrives as its own result, so joining results[]
     * on " " prints "Hello there . How are you ?". metadata.transcript is
     * the SDK's own concatenation and is the source of truth.
     */
    const session = new CallSession();
    const write = vi.spyOn(process.stdout, "write").mockImplementation(() => true);

    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (session as any).handleFinal({
        channel: "inbound",
        metadata: { transcript: "Hello there. How are you?" },
        results: [
          { type: "word", alternatives: [{ content: "Hello" }] },
          { type: "word", alternatives: [{ content: "there" }] },
          { type: "punctuation", alternatives: [{ content: "." }] },
          { type: "word", alternatives: [{ content: "How" }] },
          { type: "word", alternatives: [{ content: "are" }] },
          { type: "word", alternatives: [{ content: "you" }] },
          { type: "punctuation", alternatives: [{ content: "?" }] },
        ],
      });

      const printed = write.mock.calls.map((call) => String(call[0])).join("");
      expect(printed).toContain("Hello there. How are you?");
      expect(printed).not.toContain(" .");
      expect(printed).not.toContain(" ?");
    } finally {
      write.mockRestore();
    }
  });
});

describe("CallSession.handleFinal - consecutive finals", () => {
  it("does not add a second space between finals on the same channel", () => {
    /**
     * Scenario: Several finals arrive in a row on one channel
     *   Given AddTranscript chunks whose metadata.transcript each end in
     *         a trailing space, as the server sends them
     *    When the session handles them back to back
     *    Then the line reads "This call is being transcribed " with
     *         single spaces between words
     *
     * Purpose: the chunks are built for plain concatenation. Adding our
     * own separator on top printed "This call  is  being  transcribed".
     */
    const session = new CallSession();
    const write = vi.spyOn(process.stdout, "write").mockImplementation(() => true);

    try {
      for (const transcript of ["This call ", "is ", "being ", "transcribed "]) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (session as any).handleFinal({ channel: "outbound", metadata: { transcript } });
      }

      const printed = write.mock.calls.map((call) => String(call[0])).join("");
      expect(printed).toBe("[outbound] This call is being transcribed ");
      expect(printed).not.toMatch(/\S {2,}\S/);
    } finally {
      write.mockRestore();
    }
  });
});

describe("CallSession.close", () => {
  it("sends EndOfChannel for each channel then stops recognition", async () => {
    /**
     * Scenario: Graceful close cascades to the RT client
     *   Given a CallSession with a stubbed RT client and some enqueued frames
     *    When close() is awaited
     *    Then endChannel is called once for "inbound"
     *     And endChannel is called once for "outbound"
     *     And stopRecognition is called
     *
     * Purpose: teardown must let the RT server know each channel is done
     * (via EndOfChannel with the final seq_no) so it can flush the last
     * finals before END_OF_TRANSCRIPT.
     */
    const session = new CallSession();
    const spies = {
      sendChannelAudio: vi.fn(),
      endChannel: vi.fn(),
      stopRecognition: vi.fn().mockResolvedValue(undefined),
    };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (session as any).client = spies;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (session as any).ready = true;

    // Simulate a couple of frames on each channel.
    session.enqueue("inbound", "A");
    session.enqueue("inbound", "B");
    session.enqueue("outbound", "C");

    // Manually resolve the EndOfTranscript promise so close() doesn't
    // wait the full 10-second fallback timeout.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (session as any).eotResolve?.();

    await session.close();

    // Both channels are told about their last seq_no.
    expect(spies.endChannel).toHaveBeenCalledWith("inbound", 2);
    expect(spies.endChannel).toHaveBeenCalledWith("outbound", 1);
    expect(spies.stopRecognition).toHaveBeenCalled();
  });

  it("is idempotent on repeated calls", async () => {
    /**
     * Scenario: close() called twice
     *   Given a CallSession that has already been closed
     *    When close() is awaited a second time
     *    Then endChannel/stopRecognition are not called again
     *
     * Purpose: teardown paths overlap (Twilio "stop" event + WebSocket
     * "close" event) - it must be safe to double-close without spurious
     * network activity or errors.
     */
    const session = new CallSession();
    const spies = {
      sendChannelAudio: vi.fn(),
      endChannel: vi.fn(),
      stopRecognition: vi.fn().mockResolvedValue(undefined),
    };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (session as any).client = spies;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (session as any).ready = true;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (session as any).eotResolve?.();

    await session.close();
    spies.endChannel.mockClear();
    spies.stopRecognition.mockClear();
    await session.close();

    expect(spies.endChannel).not.toHaveBeenCalled();
    expect(spies.stopRecognition).not.toHaveBeenCalled();
  });
});
