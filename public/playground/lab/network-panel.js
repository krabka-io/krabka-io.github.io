// Recent bytes on a selected link. The world keeps 40 frames per node pair;
// each frame keeps its length and the first 16 KiB of payload bytes.
import { el, button, fmtBytes, fmtMs } from "./dom.js";
import { hexDump } from "./storage-panel.js";

const PAGE_BYTES = 256;

function field(label, value, title) {
  const item = el("span", "lab-wire-field");
  item.title = title;
  item.tabIndex = 0;
  item.append(el("strong", null, `${label} `), el("span", null, String(value)));
  return item;
}

export function decodeWire(bytes, request) {
  if (bytes.length < 4) return [];
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const declared = view.getInt32(0);
  if (declared < 0) return [];
  const fields = [["length", declared, "Bytes 0–3: signed, big-endian size of the Kafka message after this prefix."]];
  if (request && bytes.length >= 12) {
    fields.push(["API key", view.getInt16(4), "Bytes 4–5: Kafka API key identifying the request operation."]);
    fields.push(["version", view.getInt16(6), "Bytes 6–7: version of that Kafka API request."]);
    fields.push(["correlation", view.getInt32(8), "Bytes 8–11: request ID matched to the response on this connection."]);
  } else if (!request && bytes.length >= 8) {
    fields.push(["correlation", view.getInt32(4), "Bytes 4–7: request ID echoed by the broker in its response."]);
  }
  return fields;
}

export class NetworkPanel {
  constructor(container, hooks) {
    this.hooks = hooks; // frames(a, b), nodeName(id)
    this.root = el("details", "lab-wire lab-side-section");
    this.root.hidden = true;
    this.root.appendChild(el("summary", "lab-panel-title", "Network bytes"));
    this.body = el("div", "lab-wire-body");
    this.root.appendChild(this.body);
    this.root.addEventListener("toggle", () => { if (this.root.open) this.refresh(); });
    container.appendChild(this.root);
  }

  update(selection) {
    const [a, b] = selection || [];
    const key = a != null && b != null ? [Math.min(a, b), Math.max(a, b)].join("-") : "";
    this.root.hidden = !key;
    if (key !== this.key) {
      this.key = key;
      this.pair = key ? [a, b] : null;
      // The dock shows this panel as a tab, so it stays open while a link is
      // selected; the frames load when the tab does.
      this.root.open = Boolean(key);
      this.body.replaceChildren();
    }
  }

  open() {
    if (!this.pair) return;
    this.root.open = true;
    this.refresh();
    this.root.scrollIntoView({ block: "nearest" });
  }

  refresh() {
    if (!this.pair || !this.root.open) return;
    const [a, b] = this.pair;
    const frames = this.hooks.frames(a, b);
    this.body.replaceChildren();
    const head = el("div", "lab-wire-head");
    head.append(el("strong", null, `${this.hooks.nodeName(a)} ↔ ${this.hooks.nodeName(b)}`));
    head.appendChild(button("Refresh", "lab-btn-sm", () => this.refresh()));
    this.body.appendChild(head);
    this.body.appendChild(el("p", "lab-muted lab-small", `${frames.length} recent frames · up to 40 per link · first 16 KiB per frame. Hover or focus a decoded field for its byte key.`));
    if (!frames.length) {
      this.body.appendChild(el("p", "lab-muted lab-small", "No frames on this link yet. Run the scenario, then refresh."));
      return;
    }
    const list = el("div", "lab-wire-list");
    for (const frame of frames.slice().reverse()) {
      const item = el("details", "lab-wire-frame");
      const src = `${this.hooks.nodeName(frame.src.node)}:${frame.src.port}`;
      const dst = `${this.hooks.nodeName(frame.dst.node)}:${frame.dst.port}`;
      // A non-Kafka frame (KRaft on 9093) can open with raw bytes: name it plainly.
      const label = /[\u0000-\u001f�]/.test(frame.label) ? "binary" : String(frame.label).slice(0, 32);
      // The label already says open or close; only a data frame has a size to add.
      const tail = frame.kind === "data" ? ` · ${fmtBytes(frame.size)}` : frame.kind === frame.label ? "" : ` · ${frame.kind}`;
      item.appendChild(el("summary", null, `${fmtMs(frame.at)} · ${src} → ${dst} · ${label}${tail}`));
      const content = el("div", "lab-wire-content");
      content.appendChild(el("p", "lab-muted lab-small", `Connection ${frame.conn} · ${frame.kind === "data" ? `${frame.size} payload bytes` : `TCP ${frame.kind}; no payload bytes`}`));
      if (frame.kind === "data") {
        const bytes = Uint8Array.from(atob(frame.bytes), (ch) => ch.charCodeAt(0));
        const kafka = frame.dst.port === 9092 || frame.src.port === 9092;
        const decoded = kafka ? decodeWire(bytes, frame.dst.port === 9092) : [];
        if (decoded.length) {
          const key = el("div", "lab-wire-key");
          for (const [label, value, title] of decoded) key.appendChild(field(label, value, title));
          content.appendChild(key);
        }
        const nav = el("div", "lab-file-nav");
        const pre = el("pre", "lab-hex-view");
        const info = el("span", "lab-muted lab-small");
        let offset = 0;
        const previous = button("Previous", "lab-btn-sm", () => page(offset - PAGE_BYTES));
        const next = button("Next", "lab-btn-sm", () => page(offset + PAGE_BYTES));
        const page = (at) => {
          offset = Math.max(0, Math.min(at, Math.max(0, bytes.length - 1)));
          const end = Math.min(bytes.length, offset + PAGE_BYTES);
          pre.textContent = hexDump(bytes.subarray(offset, end), offset);
          info.textContent = `${offset}–${end} of ${frame.size} bytes`;
          previous.disabled = offset === 0;
          next.disabled = end >= bytes.length;
        };
        nav.append(previous, next, info);
        content.append(nav, pre);
        page(0);
        if (frame.size > bytes.length) content.appendChild(el("p", "lab-muted lab-small", `${frame.size - bytes.length} more bytes beyond the 16 KiB capture limit.`));
      }
      item.appendChild(content);
      list.appendChild(item);
    }
    this.body.appendChild(list);
  }
}
