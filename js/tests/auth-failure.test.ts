/**
 * Feature: a failed token fetch costs one call, not the process
 *
 *   CallSession.start() is fire-and-forget, so anything run() throws becomes
 *   an unhandled rejection and kills the whole proxy - every other call in
 *   flight included. A bad SPEECHMATICS_API_KEY makes createSpeechmaticsJWT()
 *   reject, so that path must be handled inside run().
 *
 * @speechmatics/auth is mocked, so no network is involved. This lives in its
 * own file because vi.mock applies to the whole module graph of a test file.
 */

import { afterEach, describe, expect, it, vi } from "vitest";

// SM_API_KEY is read once when proxy.ts loads, so it must be set before the
// import below is evaluated. vi.hoisted runs ahead of the (hoisted) imports.
vi.hoisted(() => {
  process.env.SPEECHMATICS_API_KEY = "test-key-not-real";
});

vi.mock("@speechmatics/auth", () => ({
  createSpeechmaticsJWT: vi.fn().mockRejectedValue(new Error("Failed to parse JSON response")),
}));

import { createSpeechmaticsJWT } from "@speechmatics/auth";

import { CallSession, checkSpeechmaticsKey } from "../proxy.js";

describe("checkSpeechmaticsKey (startup validation)", () => {
  it("exits with a clear message when the key is rejected", async () => {
    /**
     * Scenario: Operator starts the proxy with a bad key
     *   Given the token request rejects
     *    When the startup check runs
     *    Then the process exits with code 1
     *     And the message names SPEECHMATICS_API_KEY
     *     And it says the Twilio number has not been changed
     *
     * Purpose: fail before ngrok and the Twilio Voice URL rewrite, not on
     * the first call when everything else already looks healthy.
     */
    const exit = vi.spyOn(process, "exit").mockImplementation((() => undefined) as never);
    const error = vi.spyOn(console, "error").mockImplementation(() => undefined);

    try {
      await checkSpeechmaticsKey();

      expect(exit).toHaveBeenCalledWith(1);
      const logged = error.mock.calls.map((call) => String(call[0])).join("\n");
      expect(logged).toContain("SPEECHMATICS_API_KEY");
      expect(logged).toContain("Twilio number has not been changed");
    } finally {
      exit.mockRestore();
      error.mockRestore();
    }
  });

  it("returns quietly when the key is accepted", async () => {
    /**
     * Scenario: Operator starts the proxy with a valid key
     *   Given the token request succeeds
     *    When the startup check runs
     *    Then it resolves and the process is not exited
     */
    vi.mocked(createSpeechmaticsJWT).mockResolvedValueOnce("jwt");
    const exit = vi.spyOn(process, "exit").mockImplementation((() => undefined) as never);

    try {
      await expect(checkSpeechmaticsKey()).resolves.toBeUndefined();
      expect(exit).not.toHaveBeenCalled();
    } finally {
      exit.mockRestore();
    }
  });
});

describe("CallSession with a bad API key", () => {
  const errorLog = vi.spyOn(console, "error").mockImplementation(() => undefined);

  afterEach(() => {
    errorLog.mockClear();
  });

  it("settles start() instead of leaving an unhandled rejection", async () => {
    /**
     * Scenario: The token fetch fails
     *   Given createSpeechmaticsJWT rejects (as it does for an invalid key)
     *    When a call starts and run() fails
     *    Then the start promise resolves rather than rejects
     *     And no unhandledRejection is raised
     *     And the console names SPEECHMATICS_API_KEY as the thing to check
     *
     * Purpose: Node exits on an unhandled rejection. Before this was
     * guarded, one bad key ended the process.
     */
    const unhandled = vi.fn();
    process.on("unhandledRejection", unhandled);

    try {
      const session = new CallSession();
      session.start();

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      await expect((session as any).startPromise).resolves.toBeUndefined();
      // Give a rejection, if there were one, a tick to surface.
      await new Promise((resolve) => setImmediate(resolve));

      expect(unhandled).not.toHaveBeenCalled();
      const logged = errorLog.mock.calls.map((call) => String(call[0])).join("\n");
      expect(logged).toContain("SPEECHMATICS_API_KEY");
    } finally {
      process.off("unhandledRejection", unhandled);
    }
  });

  it("drops later audio loudly and still closes cleanly", async () => {
    /**
     * Scenario: Twilio keeps sending audio after the session failed
     *   Given a session whose token fetch failed
     *    When more frames are enqueued and the call then ends
     *    Then enqueue() never throws
     *     And the dropped frames are reported
     *     And close() resolves
     *
     * Purpose: the call is still up on Twilio's side and keeps streaming.
     * Silence would look like a working call that produced no text, so the
     * loss has to be visible.
     */
    const session = new CallSession();
    session.start();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await (session as any).startPromise;

    expect(() => {
      session.enqueue("inbound", "a");
      session.enqueue("outbound", "b");
    }).not.toThrow();

    await expect(session.close()).resolves.toBeUndefined();

    const logged = errorLog.mock.calls.map((call) => String(call[0])).join("\n");
    expect(logged).toContain("dropping audio");
    expect(logged).toContain("dropped 2 audio frame(s)");
  });
});
