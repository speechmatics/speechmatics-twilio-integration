/**
 * Feature: TwiML builder (buildTwiml)
 *
 * When a call comes in, Twilio fetches /twiml and executes whatever we
 * return. A single wrong attribute (e.g. track="inbound" instead of
 * "both_tracks") makes Twilio silently play "an application error has
 * occurred" without any obvious clue why. These tests pin the shape so
 * refactors can't sneak a regression in.
 */

import { describe, expect, it } from "vitest";
import { XMLParser } from "fast-xml-parser";

import { buildTwiml } from "../proxy.js";

function parse(body: string) {
  // parseTagValue: false keeps the leading "+" on phone numbers - the
  // default would coerce "+447..." to a number and lose it.
  return new XMLParser({
    ignoreAttributes: false,
    parseTagValue: false,
  }).parse(body);
}

describe("buildTwiml", () => {
  it("wraps output in a <Response> root", () => {
    /**
     * Scenario: Document root element
     *   Given the TwiML output for any inputs
     *    When it is parsed as XML
     *    Then a top-level "Response" node exists
     *
     * Purpose: Twilio expects every TwiML document to be wrapped in a
     * top-level <Response>. A naked <Stream> or <Dial> would be rejected.
     */
    const doc = parse(buildTwiml("wss://x/twilio", "+441234567890"));
    expect(doc.Response).toBeDefined();
  });

  it("puts a <Stream> with the right url + track inside <Start>", () => {
    /**
     * Scenario: The <Start><Stream> that forks call audio
     *   Given the TwiML output for a specific ngrok stream URL
     *    When the document is parsed
     *    Then <Response><Start><Stream> exists
     *     And its `url` attribute matches verbatim
     *     And its `track` attribute is "both_tracks"
     *
     * Purpose: `both_tracks` is what forks caller and callee audio into
     * two channels. Anything else silently collapses diarization to a
     * single mixed channel.
     */
    const doc = parse(
      buildTwiml("wss://tunnel.example.dev/twilio", "+441234567890"),
    );
    const stream = doc.Response.Start.Stream;
    expect(stream).toBeDefined();
    expect(stream["@_url"]).toBe("wss://tunnel.example.dev/twilio");
    expect(stream["@_track"]).toBe("both_tracks");
  });

  it("bridges to the forwarding number via <Dial>", () => {
    /**
     * Scenario: The <Dial> that bridges to the second human
     *   Given the TwiML output for a specific forwarding number
     *    When the document is parsed
     *    Then <Response><Dial> exists
     *     And its inner text is the forwarding number verbatim
     *
     * Purpose: Without <Dial>, the caller hears the greeting and hangs
     * up - no second party, no two-way audio, no multi-channel transcript.
     */
    // +44 7700 900xxx is Ofcom's reserved drama range - never a real subscriber.
    const doc = parse(buildTwiml("wss://x/twilio", "+447700900123"));
    expect(String(doc.Response.Dial)).toBe("+447700900123");
  });

  it("includes a <Say> whose text mentions transcription", () => {
    /**
     * Scenario: The <Say> greeting spoken before the bridge
     *   Given the TwiML output for any inputs
     *    When the document is parsed
     *    Then a <Say> element exists at the top level
     *     And its text contains "transcribed" (case-insensitive)
     *
     * Purpose: legal / consent notice, plus a useful ops signal - if the
     * caller doesn't hear the greeting, /twiml is broken.
     */
    const doc = parse(buildTwiml("wss://x/twilio", "+441234567890"));
    const sayText: string =
      typeof doc.Response.Say === "string"
        ? doc.Response.Say
        : String(doc.Response.Say["#text"] ?? doc.Response.Say);
    expect(sayText.toLowerCase()).toContain("transcribed");
  });
});
