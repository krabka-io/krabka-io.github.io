// Small DOM helpers shared by every Cluster Lab module: element construction,
// buttons with keyboard-friendly defaults, HTML escaping, formatting, and the
// toast area that surfaces errors instead of a broken page.

export const SVG_NS = "http://www.w3.org/2000/svg";

// Create an HTML element with an optional class and text.
export function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
}

// Create an SVG element with attributes.
export function svg(tag, attrs) {
  const e = document.createElementNS(SVG_NS, tag);
  if (attrs) setAttrs(e, attrs);
  return e;
}

// Set (or with `null`, remove) several attributes at once.
export function setAttrs(e, attrs) {
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null || v === false) e.removeAttribute(k);
    else e.setAttribute(k, String(v));
  }
  return e;
}

// A `<button type="button">`: never submits a form, always reachable by Tab.
export function button(text, cls, onClick, opts = {}) {
  const b = el("button", `lab-btn ${cls || ""}`.trim());
  b.type = "button";
  b.textContent = text;
  if (opts.title) b.title = opts.title;
  if (opts.ariaLabel) b.setAttribute("aria-label", opts.ariaLabel);
  if (opts.data) for (const [k, v] of Object.entries(opts.data)) b.dataset[k] = v;
  if (opts.disabled) b.disabled = true;
  if (onClick) b.addEventListener("click", onClick);
  return b;
}

// A `<select>` from `[{ value, label }]` (or plain strings).
export function select(options, value, onChange, cls) {
  const s = el("select", `lab-select ${cls || ""}`.trim());
  for (const opt of options) {
    const o = document.createElement("option");
    const v = typeof opt === "object" ? opt.value : opt;
    o.value = String(v);
    o.textContent = typeof opt === "object" ? opt.label : String(opt);
    s.appendChild(o);
  }
  if (value != null) s.value = String(value);
  if (onChange) s.addEventListener("change", () => onChange(s.value));
  return s;
}

// A labelled form row. The control is named by the label text alone; the help
// line sits inside the label for layout but is a description, not part of the name.
let fieldCount = 0;
export function labelled(text, control, help) {
  const wrap = el("label", "lab-field");
  const span = el("span", "lab-field-label", text);
  span.id = `lab-field-${++fieldCount}`;
  control.setAttribute("aria-labelledby", span.id);
  wrap.append(span, control);
  if (help) {
    const small = el("small", "lab-field-help", help);
    small.id = `${span.id}-help`;
    control.setAttribute("aria-describedby", small.id);
    wrap.appendChild(small);
  }
  return wrap;
}

export function escapeHtml(s) {
  return String(s).replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
}

// Simulated time for humans: "12 ms", "1.234 s", "2m 03.4s".
export function fmtMs(ms) {
  const n = Number(ms) || 0;
  if (n < 1000) return `${Math.round(n)} ms`;
  if (n < 60_000) return `${(n / 1000).toFixed(n < 10_000 ? 3 : 1)} s`;
  const m = Math.floor(n / 60_000);
  const s = (n - m * 60_000) / 1000;
  return `${m}m ${s.toFixed(1).padStart(4, "0")}s`;
}

export function fmtNum(n) {
  if (n == null || Number.isNaN(Number(n))) return "–";
  return Number(n).toLocaleString("en-US");
}

// "1 partition", "3 partitions".
export function plural(n, word, many = `${word}s`) {
  return `${fmtNum(n)} ${Number(n) === 1 ? word : many}`;
}

export function fmtBytes(n) {
  const v = Number(n) || 0;
  if (v < 1024) return `${v} B`;
  if (v < 1024 * 1024) return `${(v / 1024).toFixed(1)} KiB`;
  return `${(v / (1024 * 1024)).toFixed(2)} MiB`;
}

export function clamp(v, lo, hi) {
  return Math.min(hi, Math.max(lo, v));
}

export function debounce(fn, ms) {
  let timer = null;
  const wrapped = (...args) => {
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => {
      timer = null;
      fn(...args);
    }, ms);
  };
  wrapped.cancel = () => {
    if (timer) clearTimeout(timer);
    timer = null;
  };
  return wrapped;
}

// One line of JSON for a timeline row or a label. Objects become `k=v` pairs,
// long values are cut, and the result never exceeds `max` characters.
export function shortJson(value, max = 90) {
  let text;
  if (value == null) text = "";
  else if (typeof value === "object" && !Array.isArray(value)) {
    text = Object.entries(value)
      .filter(([k]) => k !== "level")
      .map(([k, v]) => `${k}=${typeof v === "object" ? JSON.stringify(v) : String(v)}`)
      .join(" ");
  } else if (typeof value === "object") text = JSON.stringify(value);
  else text = String(value);
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

// A file download from text, for the scenario export.
export function download(filename, text, type = "application/json") {
  const blob = new Blob([text], { type });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export function readFileText(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result));
    reader.onerror = () => reject(reader.error || new Error("file read failed"));
    reader.readAsText(file);
  });
}

// Copy text to the clipboard; resolves false when the browser refuses.
export async function copyToClipboard(text) {
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {
    // fall through to the legacy path
  }
  try {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand("copy");
    ta.remove();
    return ok;
  } catch {
    return false;
  }
}

// The toast area: short-lived messages in a corner of the panel. Errors stay
// longer and are also logged, so a broken call never becomes a broken page.
export class Toasts {
  constructor(container) {
    this.root = el("div", "lab-toasts");
    this.root.setAttribute("role", "status");
    this.root.setAttribute("aria-live", "polite");
    container.appendChild(this.root);
  }

  // `action`: { label, run }, a button that does something (Undo) and dismisses
  // the toast. A toast with an action stays longer, and every toast waits
  // while the pointer is over it or focus is in it, so there is time to reach it.
  show(message, { level = "info", action = null, ttl = action ? 15_000 : 5000 } = {}) {
    const t = el("div", `lab-toast lab-toast-${level}`);
    const text = el("span", "lab-toast-text", message);
    const close = button("×", "lab-toast-close", () => t.remove(), {
      ariaLabel: "Dismiss",
    });
    t.append(text);
    if (action) {
      t.appendChild(
        button(action.label, "lab-btn-sm lab-toast-action", () => {
          t.remove();
          action.run();
        }),
      );
    }
    t.append(close);
    this.root.appendChild(t);
    while (this.root.children.length > 5) this.root.firstChild.remove();
    if (ttl > 0) {
      let timer = 0;
      const arm = () => {
        clearTimeout(timer);
        if (!t.matches(":hover, :focus-within")) timer = setTimeout(() => t.remove(), ttl);
      };
      for (const ev of ["pointerenter", "focusin"]) t.addEventListener(ev, () => clearTimeout(timer));
      for (const ev of ["pointerleave", "focusout"]) t.addEventListener(ev, arm);
      arm();
    }
    t.addEventListener("keydown", (e) => {
      if (e.key !== "Escape") return;
      e.stopPropagation();
      t.remove();
    });
    return t;
  }

  // An Undo that belongs to a scenario that is gone must not be offered.
  dropActions() {
    for (const a of this.root.querySelectorAll(".lab-toast-action")) a.closest(".lab-toast").remove();
  }

  info(message) {
    return this.show(message, { level: "info" });
  }

  warn(message) {
    return this.show(message, { level: "warn", ttl: 8000 });
  }

  error(err, context) {
    const message = err instanceof Error ? err.message : String(err);
    // eslint-disable-next-line no-console
    console.warn("krabka lab:", context || "", err);
    return this.show(context ? `${context}: ${message}` : message, {
      level: "error",
      ttl: 10_000,
    });
  }
}
