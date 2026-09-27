// The generic form builder and the modal dialog.
//
// `buildForm` turns the field specs of `kinds.js` into controls and reads a
// `config` object back out of them, with validation: required fields,
// numbers, JSON documents, node references picked from the existing nodes,
// nested groups (optional groups fold to a checkbox), `advanced` disclosures
// whose fields sit in the same config object as the rest, a record value
// (format and template), and the streams topology ops editor. The same form
// adds a node and edits an existing one.
//
// A field's `default` is what the node does when the key is missing. With
// `emitDefault: false` a field that still holds its default is left out of
// the config, so a spec carries only what differs from the node's defaults.
//
// `openDialog` shows a `<dialog>` with a title, a body and Apply/Cancel
// buttons. The dialog traps focus and closes on Escape, so everything in it
// works from the keyboard.

import { el, button, select, labelled } from "./dom.js";

// The placeholders a record template takes (`apps::templates`).
export const TEMPLATE_HELP = "Placeholders: {seq}, {seq % n}, {now}, {rand a b}, {pick a|b|c}, {uuid}; {{ and }} are literal braces.";

// The parameters of each streams topology op (`apps::topology`). `filter`
// stores its comparison as `{ op: "filter", field, <cmp>: value }`, matching
// the design document's `{ "op": "filter", "field": "total", "gt": 100 }`.
const STORE_FIELD = { key: "store", label: "Store", type: "text", placeholder: "count-by-key-1", help: "The state store's name; its changelog is <application id>-<store>-changelog. Empty: <op>-<index>." };
const OP_SPECS = {
  filter: {
    label: "filter — keep records whose field compares",
    fields: [
      { key: "field", label: "Field", type: "text", required: true, placeholder: "total" },
      { key: "cmp", label: "Comparison", type: "select", default: "gt", options: ["gt", "gte", "lt", "lte", "eq", "ne", "contains"] },
      { key: "value", label: "Value", type: "scalar", default: 100, help: "A number, or text in quotes." },
    ],
  },
  map: {
    label: "map — reshape the value",
    fields: [
      { key: "select", label: "Select", type: "list", placeholder: "id, total", help: "Keep only these fields." },
      { key: "rename", label: "Rename", type: "json", placeholder: '{"total": "amount"}', validate: stringMap, help: "Field → new name." },
      { key: "set", label: "Set", type: "json", placeholder: '{"source": "lab-{seq}"}', validate: stringMap, help: `Field → template; {seq} is the source offset, {now} its timestamp. ${TEMPLATE_HELP}` },
      { key: "upper", label: "Upper", type: "list", placeholder: "customer", help: "Upper-case these text fields. Applied in this order: select, rename, set, upper." },
    ],
  },
  select_key: {
    label: "select_key — re-key by a field",
    fields: [{ key: "field", label: "Field", type: "text", required: true, placeholder: "customer", help: "An aggregation after it reads through a repartition topic." }],
  },
  count_by_key: { label: "count_by_key — running count per key", fields: [STORE_FIELD] },
  sum_by_key: {
    label: "sum_by_key — running sum of a field per key",
    fields: [{ key: "field", label: "Field", type: "text", required: true, placeholder: "total" }, STORE_FIELD],
  },
  window_count: {
    label: "window_count — count per key per window",
    fields: [
      { key: "size_ms", label: "Window size (ms)", type: "number", required: true, default: 1000, min: 1, step: 1 },
      { key: "advance_ms", label: "Advance (ms)", type: "number", min: 1, step: 1, help: "Hopping windows; empty is a tumbling window (advance = size)." },
      { key: "grace_ms", label: "Grace (ms)", type: "number", min: 0, step: 1, help: "How late a record may be before its window closes; empty is 0." },
      STORE_FIELD,
    ],
  },
};
const CMP_KEYS = ["gt", "gte", "lt", "lte", "eq", "ne", "contains"];

// A JSON object of strings, as `map` takes for `rename` and `set`.
function stringMap(v) {
  if (!v || typeof v !== "object" || Array.isArray(v)) return "an object";
  const bad = Object.entries(v).find(([, s]) => typeof s !== "string");
  return bad ? `${bad[0]} is not a string` : null;
}

// Whether two JSON values are the same document.
export function sameJson(a, b) {
  return JSON.stringify(a) === JSON.stringify(b);
}

// Build a form. `ctx.nodes` lists `{ id, kind, name }` for node pickers;
// `ctx.self` is the id of the node being edited (excluded from pickers).
export function buildForm(fields, values = {}, ctx = {}) {
  const root = el("div", "lab-form");
  const source = values && typeof values === "object" ? values : {};
  // An `advanced` disclosure reads its fields from the same object.
  const controls = fields.map((spec) => makeControl(spec, spec.type === "advanced" ? source : source[spec.key], ctx));
  for (const c of controls) root.appendChild(c.root);
  return {
    root,
    // Returns `{ value, errors }`; `errors` is a list of `{ key, message }`.
    read() {
      const value = {};
      const errors = [];
      for (const c of controls) {
        const r = c.read();
        c.setError(r.error || "");
        if (r.error) errors.push({ key: c.spec.key ?? c.spec.label, message: r.error });
        else if (r.flat) Object.assign(value, r.flat);
        else if (r.value !== undefined) value[c.spec.key] = r.value;
      }
      return { value, errors };
    },
    focus() {
      const first = root.querySelector("input, select, textarea, button");
      if (first) first.focus();
    },
  };
}

function makeControl(spec, value, ctx) {
  const wrap = el("div", `lab-control lab-control-${spec.type}`);
  const errorEl = el("div", "lab-field-error");
  errorEl.hidden = true;
  const setError = (msg) => {
    errorEl.textContent = msg;
    errorEl.hidden = !msg;
    wrap.classList.toggle("lab-invalid", Boolean(msg));
  };
  let read;
  const initial = value !== undefined ? value : spec.default;
  const label = spec.required ? `${spec.label} *` : spec.label;
  const help = spec.help;

  switch (spec.type) {
    case "text": {
      const input = el("input", "lab-input");
      input.type = "text";
      if (spec.placeholder) input.placeholder = spec.placeholder;
      input.value = initial == null ? "" : String(initial);
      wrap.appendChild(labelled(label, input, help));
      read = () => {
        const v = input.value.trim();
        if (!v) return spec.required ? { error: "required" } : { value: undefined };
        const invalid = spec.validate ? spec.validate(v) : null;
        if (invalid) return { error: invalid };
        if (spec.emitDefault === false && v === spec.default) return { value: undefined };
        return { value: v };
      };
      break;
    }
    case "number": {
      const input = el("input", "lab-input");
      input.type = "number";
      if (spec.min != null) input.min = String(spec.min);
      if (spec.max != null) input.max = String(spec.max);
      if (spec.step != null) input.step = String(spec.step);
      input.value = initial == null ? "" : String(initial);
      wrap.appendChild(labelled(label, input, help));
      read = () => {
        const raw = input.value.trim();
        if (!raw) return spec.required ? { error: "required" } : { value: undefined };
        const n = Number(raw);
        if (!Number.isFinite(n)) return { error: "not a number" };
        if (spec.step === 1 && !Number.isInteger(n)) return { error: "a whole number" };
        if (spec.min != null && n < spec.min) return { error: `at least ${spec.min}` };
        if (spec.max != null && n > spec.max) return { error: `at most ${spec.max}` };
        const invalid = spec.validate ? spec.validate(n) : null;
        if (invalid) return { error: invalid };
        if (spec.emitDefault === false && n === spec.default) return { value: undefined };
        return { value: n };
      };
      break;
    }
    case "boolean": {
      const input = el("input", "lab-checkbox");
      input.type = "checkbox";
      input.checked = Boolean(initial);
      const row = el("label", "lab-field lab-field-inline");
      row.append(input, el("span", "lab-field-label", spec.label));
      wrap.appendChild(row);
      if (help) wrap.appendChild(el("small", "lab-field-help", help));
      read = () => {
        const v = input.checked;
        if (spec.emitDefault === false && v === Boolean(spec.default)) return { value: undefined };
        return { value: v };
      };
      break;
    }
    case "select": {
      const options = spec.options.map((o) => (typeof o === "object" ? o : { value: o, label: String(o) }));
      const sel = select(options, initial, null);
      wrap.appendChild(labelled(label, sel, help));
      read = () => {
        const opt = options.find((o) => String(o.value) === sel.value);
        const v = opt ? opt.value : sel.value;
        if (spec.emitDefault === false && v === spec.default) return { value: undefined };
        return { value: v };
      };
      break;
    }
    case "scalar": {
      // A JSON scalar typed as text: numbers stay numbers, quoted text is text.
      const input = el("input", "lab-input");
      input.type = "text";
      input.value = initial == null ? "" : typeof initial === "string" ? JSON.stringify(initial) : String(initial);
      wrap.appendChild(labelled(label, input, help));
      read = () => {
        const raw = input.value.trim();
        if (!raw) return spec.required ? { error: "required" } : { value: undefined };
        try {
          return { value: JSON.parse(raw) };
        } catch {
          return { value: raw };
        }
      };
      break;
    }
    case "list": {
      const input = el("input", "lab-input");
      input.type = "text";
      if (spec.placeholder) input.placeholder = spec.placeholder;
      input.value = Array.isArray(initial) ? initial.join(", ") : initial == null ? "" : String(initial);
      wrap.appendChild(labelled(label, input, help));
      read = () => {
        const items = input.value
          .split(/[,\n]/)
          .map((s) => s.trim())
          .filter(Boolean);
        if (!items.length) return spec.required ? { error: "at least one entry" } : { value: undefined };
        return { value: items };
      };
      break;
    }
    case "noderef": {
      const candidates = nodesOf(ctx, spec.of);
      const options = candidates.map((n) => ({ value: n.id, label: `${n.name} (#${n.id}, ${n.kind})` }));
      if (!spec.required) options.unshift({ value: "", label: "(none)" });
      const sel = select(options.length ? options : [{ value: "", label: "(no matching node yet)" }], initial ?? "", null);
      wrap.appendChild(labelled(label, sel, help));
      read = () => {
        if (sel.value === "") return spec.required ? { error: `pick a ${(spec.of || ["node"]).join(" or ")} node` } : { value: undefined };
        return { value: Number(sel.value) };
      };
      break;
    }
    case "noderefs": {
      const candidates = nodesOf(ctx, spec.of);
      const list = el("div", "lab-checklist");
      const boxes = [];
      const chosen = new Set((Array.isArray(initial) ? initial : []).map(Number));
      for (const n of candidates) {
        const row = el("label", "lab-check-row");
        const box = el("input");
        box.type = "checkbox";
        box.value = String(n.id);
        box.checked = chosen.has(n.id) || (!Array.isArray(initial) && spec.required);
        boxes.push(box);
        row.append(box, el("span", null, `${n.name} (#${n.id})`));
        list.appendChild(row);
      }
      if (!candidates.length) list.appendChild(el("p", "lab-muted", `Add a ${(spec.of || ["node"]).join(" or ")} first.`));
      const field = el("div", "lab-field");
      field.append(el("span", "lab-field-label", label), list);
      if (help) field.appendChild(el("small", "lab-field-help", help));
      wrap.appendChild(field);
      read = () => {
        const ids = boxes.filter((b) => b.checked).map((b) => Number(b.value));
        if (!ids.length) return spec.required ? { error: "pick at least one" } : { value: undefined };
        return { value: ids };
      };
      break;
    }
    case "json": {
      const ta = el("textarea", "lab-textarea");
      ta.rows = 4;
      ta.spellcheck = false;
      if (spec.placeholder) ta.placeholder = spec.placeholder;
      let text = "";
      if (typeof initial === "string") {
        try {
          text = JSON.stringify(JSON.parse(initial), null, 2);
        } catch {
          text = initial;
        }
      } else if (initial !== undefined) text = JSON.stringify(initial, null, 2);
      ta.value = text;
      wrap.appendChild(labelled(label, ta, help));
      read = () => {
        const raw = ta.value.trim();
        if (!raw) return spec.required ? { error: "required" } : { value: undefined };
        let parsed;
        try {
          parsed = JSON.parse(raw);
        } catch (err) {
          return { error: `invalid JSON: ${err.message}` };
        }
        const invalid = spec.validate ? spec.validate(parsed) : null;
        if (invalid) return { error: invalid };
        return { value: spec.stringify ? JSON.stringify(parsed) : parsed };
      };
      break;
    }
    case "record-value": {
      // A producer's `value`: `{ format: "json", template: <document> }` or
      // `{ format: "text", template: <text> }`.
      const start = initial && typeof initial === "object" ? initial : spec.default || { format: "json", template: {} };
      const formats = spec.formats || ["json", "text"];
      const fmt = select(formats, start.format || "json", null);
      const ta = el("textarea", "lab-textarea");
      ta.rows = 4;
      ta.spellcheck = false;
      ta.value = typeof start.template === "string" ? start.template : JSON.stringify(start.template ?? {}, null, 2);
      const fieldset = el("fieldset", "lab-group");
      fieldset.appendChild(el("legend", null, spec.label));
      fieldset.append(labelled("Format", fmt, "json: a JSON document whose string values are templates; text: one text template."), labelled("Template", ta, help));
      wrap.appendChild(fieldset);
      read = () => {
        const format = fmt.value;
        const raw = ta.value.trim();
        if (!raw) return { error: "the template is required" };
        let template = raw;
        if (format === "json") {
          try {
            template = JSON.parse(raw);
          } catch (err) {
            return { error: `invalid JSON: ${err.message}` };
          }
        }
        const value = { format, template };
        if (spec.emitDefault === false && sameJson(value, spec.default)) return { value: undefined };
        return { value };
      };
      break;
    }
    case "advanced": {
      // Fields a reader seldom touches, folded away; their keys sit in the
      // same config object as the rest. Opens by itself when one of them
      // holds something other than the node's default.
      const details = el("details", "lab-advanced");
      details.appendChild(el("summary", "lab-advanced-title", spec.label || "Advanced"));
      const inner = buildForm(spec.fields, initial && typeof initial === "object" ? initial : {}, ctx);
      inner.root.classList.add("lab-advanced-body");
      details.appendChild(inner.root);
      if (help) details.appendChild(el("small", "lab-field-help", help));
      details.open = spec.fields.some((f) => initial && initial[f.key] !== undefined && !sameJson(initial[f.key], f.default));
      wrap.appendChild(details);
      read = () => {
        const r = inner.read();
        if (r.errors.length) {
          details.open = true;
          return { error: r.errors.map((e) => `${e.key}: ${e.message}`).join("; ") };
        }
        return { flat: r.value };
      };
      break;
    }
    case "group": {
      const fieldset = el("fieldset", "lab-group");
      const legend = el("legend", null, spec.label);
      fieldset.appendChild(legend);
      let enabled = null;
      const present = initial !== undefined && initial !== null;
      if (spec.optional) {
        enabled = el("input");
        enabled.type = "checkbox";
        enabled.checked = present;
        const row = el("label", "lab-field-inline lab-group-toggle");
        row.append(enabled, el("span", null, "enabled"));
        legend.textContent = "";
        legend.append(el("span", null, `${spec.label} `), row);
      }
      const inner = buildForm(spec.fields, present && typeof initial === "object" ? initial : {}, ctx);
      inner.root.classList.add("lab-group-body");
      fieldset.appendChild(inner.root);
      if (help) fieldset.appendChild(el("small", "lab-field-help", help));
      const sync = () => {
        inner.root.hidden = enabled ? !enabled.checked : false;
      };
      if (enabled) enabled.addEventListener("change", sync);
      sync();
      wrap.appendChild(fieldset);
      read = () => {
        if (enabled && !enabled.checked) return { value: undefined };
        const r = inner.read();
        if (r.errors.length) return { error: r.errors.map((e) => `${e.key}: ${e.message}`).join("; ") };
        return { value: r.value };
      };
      break;
    }
    case "ops": {
      const editor = opsEditor(Array.isArray(initial) ? initial : [], ctx);
      const field = el("div", "lab-field");
      field.append(el("span", "lab-field-label", label), editor.root);
      if (help) field.appendChild(el("small", "lab-field-help", help));
      wrap.appendChild(field);
      read = editor.read;
      break;
    }
    default: {
      wrap.appendChild(el("p", "lab-muted", `unknown field type ${spec.type}`));
      read = () => ({ value: undefined });
    }
  }
  wrap.appendChild(errorEl);
  return { spec, root: wrap, read, setError };
}

function nodesOf(ctx, kinds) {
  const list = (ctx.nodes || []).filter((n) => n.id !== ctx.self);
  if (!kinds || !kinds.length) return list;
  return list.filter((n) => kinds.includes(n.kind));
}

// ---- the topology ops editor -------------------------------------------------------------

function opsEditor(initialOps, ctx) {
  const root = el("div", "lab-ops");
  const list = el("ol", "lab-ops-list");
  root.appendChild(list);
  const rows = [];

  const render = () => {
    list.innerHTML = "";
    rows.forEach((row, i) => {
      row.index.textContent = `${i + 1}.`;
      row.up.disabled = i === 0;
      row.down.disabled = i === rows.length - 1;
      list.appendChild(row.root);
    });
    empty.hidden = rows.length > 0;
  };

  const addRow = (op) => {
    const row = makeOpRow(op, ctx, {
      remove: () => {
        rows.splice(rows.indexOf(row), 1);
        render();
      },
      up: () => {
        const i = rows.indexOf(row);
        if (i > 0) {
          rows.splice(i, 1);
          rows.splice(i - 1, 0, row);
          render();
        }
      },
      down: () => {
        const i = rows.indexOf(row);
        if (i < rows.length - 1) {
          rows.splice(i, 1);
          rows.splice(i + 1, 0, row);
          render();
        }
      },
    });
    rows.push(row);
    render();
    return row;
  };

  const empty = el("p", "lab-muted", "No operations: records pass from the source to the sink unchanged.");
  root.appendChild(empty);
  const adder = el("div", "lab-ops-add");
  const pick = select(
    Object.entries(OP_SPECS).map(([value, s]) => ({ value, label: s.label })),
    "filter",
    null,
  );
  adder.append(pick, button("+ Add op", "", () => addRow({ op: pick.value })));
  root.appendChild(adder);
  for (const op of initialOps) addRow(op);

  return {
    root,
    read() {
      const ops = [];
      for (const row of rows) {
        const r = row.read();
        if (r.error) return { error: r.error };
        ops.push(r.value);
      }
      return { value: ops };
    },
  };
}

function makeOpRow(op, ctx, actions) {
  const root = el("li", "lab-op");
  const head = el("div", "lab-op-head");
  const index = el("span", "lab-op-index", "1.");
  const type = String(op.op || "filter");
  const spec = OP_SPECS[type] || { label: type, fields: [] };
  const title = el("span", "lab-op-title", spec.label);
  const up = button("↑", "lab-btn-sm", actions.up, { ariaLabel: "Move up" });
  const down = button("↓", "lab-btn-sm", actions.down, { ariaLabel: "Move down" });
  const remove = button("×", "lab-btn-sm", actions.remove, { ariaLabel: "Remove op" });
  head.append(index, title, up, down, remove);
  root.appendChild(head);

  // Turn the stored op back into field values.
  const values = {};
  if (type === "filter") {
    values.field = op.field;
    const cmp = CMP_KEYS.find((k) => k in op);
    if (cmp) {
      values.cmp = cmp;
      values.value = op[cmp];
    }
  } else {
    for (const f of spec.fields) if (f.key in op) values[f.key] = typeof op[f.key] === "string" && f.type === "list" ? [op[f.key]] : op[f.key];
  }
  const form = buildForm(spec.fields, values, ctx);
  root.appendChild(form.root);

  return {
    root,
    index,
    up,
    down,
    read() {
      const r = form.read();
      if (r.errors.length) return { error: `${type}: ${r.errors.map((e) => `${e.key} ${e.message}`).join(", ")}` };
      const v = r.value;
      if (type === "filter") {
        const out = { op: "filter", field: v.field };
        out[v.cmp || "gt"] = v.value !== undefined ? v.value : 0;
        return { value: out };
      }
      if (type === "map" && !["select", "rename", "set", "upper"].some((k) => v[k] !== undefined)) {
        return { error: "map: needs at least one of select, rename, set or upper" };
      }
      return { value: { op: type, ...v } };
    },
  };
}

// ---- dialogs ----------------------------------------------------------------------------------

// Show a modal dialog. `onSubmit` returns true (or a promise of true) to
// close it; a falsy result keeps it open, for validation errors. Resolves to
// true when submitted, false when cancelled.
export function openDialog(container, { title, body, submitLabel = "Apply", cancelLabel = "Cancel", onSubmit, wide = false, focus }) {
  return new Promise((resolve) => {
    const dialog = el("dialog", wide ? "lab-dialog lab-dialog-wide" : "lab-dialog");
    dialog.setAttribute("aria-label", title);
    const form = el("form", "lab-dialog-form");
    form.method = "dialog";
    form.appendChild(el("h3", "lab-dialog-title", title));
    const bodyWrap = el("div", "lab-dialog-body");
    bodyWrap.appendChild(body);
    form.appendChild(bodyWrap);
    const actions = el("div", "lab-dialog-actions");
    const cancel = button(cancelLabel, "", () => close(false));
    const submit = el("button", "lab-btn lab-primary");
    submit.type = "submit";
    submit.textContent = submitLabel;
    actions.append(cancel, submit);
    form.appendChild(actions);
    dialog.appendChild(form);
    container.appendChild(dialog);

    let done = false;
    const close = (result) => {
      if (done) return;
      done = true;
      try {
        if (dialog.open) dialog.close();
      } catch {
        // Not a modal in this browser; removal below is enough.
      }
      dialog.remove();
      resolve(result);
    };
    form.addEventListener("submit", async (e) => {
      e.preventDefault();
      let ok = true;
      if (onSubmit) {
        submit.disabled = true;
        try {
          ok = await onSubmit();
        } finally {
          submit.disabled = false;
        }
      }
      if (ok) close(true);
    });
    dialog.addEventListener("cancel", (e) => {
      e.preventDefault();
      close(false);
    });
    dialog.addEventListener("click", (e) => {
      if (e.target === dialog) close(false);
    });
    if (typeof dialog.showModal === "function") dialog.showModal();
    else dialog.setAttribute("open", "");
    if (focus) focus();
    else {
      const first = bodyWrap.querySelector("input, select, textarea, button");
      (first || submit).focus();
    }
  });
}
