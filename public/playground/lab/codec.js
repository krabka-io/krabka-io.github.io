// Compact text encodings for things that travel in a URL: a shared scenario
// (`#s=<code>`) and a WebRTC invite or answer (`?join=<code>`).
//
// The code is one tag character followed by URL-safe base64. Tag `d` means the
// JSON was compressed with `deflate-raw` through `CompressionStream`; tag `p`
// means plain UTF-8, for browsers without the streams API. A `d` code opens
// everywhere: without `DecompressionStream` the bytes go through the
// JavaScript decoder in `inflate.js`.

import { inflateRaw } from "./inflate.js";

function toBase64Url(bytes) {
  let binary = "";
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function fromBase64Url(text) {
  const padded = text.replace(/-/g, "+").replace(/_/g, "/");
  const pad = padded.length % 4 === 0 ? "" : "=".repeat(4 - (padded.length % 4));
  const binary = atob(padded + pad);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

async function pipe(bytes, stream) {
  const blob = new Blob([bytes]);
  const buffer = await new Response(blob.stream().pipeThrough(stream)).arrayBuffer();
  return new Uint8Array(buffer);
}

export async function encodeShare(value) {
  const bytes = new TextEncoder().encode(JSON.stringify(value));
  if (typeof CompressionStream === "function") {
    try {
      const packed = await pipe(bytes, new CompressionStream("deflate-raw"));
      return `d${toBase64Url(packed)}`;
    } catch {
      // The plain encoding below always works.
    }
  }
  return `p${toBase64Url(bytes)}`;
}

// What a person sees for a code that does not decode: a truncated or edited
// link, not the browser's own error.
const BAD_CODE = "that code is damaged or cut short; copy it again in full";

export async function decodeShare(code) {
  const text = String(code || "").trim();
  if (!text) throw new Error("empty code");
  const tag = text[0];
  if (tag !== "d" && tag !== "p") throw new Error(BAD_CODE);
  try {
    const bytes = fromBase64Url(text.slice(1));
    const json = tag === "d" ? (typeof DecompressionStream === "function" ? await pipe(bytes, new DecompressionStream("deflate-raw")) : inflateRaw(bytes)) : bytes;
    return JSON.parse(new TextDecoder().decode(json));
  } catch {
    throw new Error(BAD_CODE);
  }
}
