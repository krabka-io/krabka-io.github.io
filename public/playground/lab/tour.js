// A short guided tour of the lab.
//
// A card floats over the canvas and walks through five things: what the
// canvas shows, the clock, the inspector, the fault bar, and the rail. While
// a step is showing, the part of the page it talks about is outlined. Some
// steps carry a button that does the thing for the reader (select a broker,
// kill it, restart it), so the tour ends with a cluster that was broken and
// healed. It opens by itself once per browser and any time from the toolbar.

import { el, button } from "./dom.js";

const STORAGE_KEY = "krabka-lab.tour";

function seen() {
  try {
    return localStorage.getItem(STORAGE_KEY) === "done";
  } catch {
    return false;
  }
}

function remember() {
  try {
    localStorage.setItem(STORAGE_KEY, "done");
  } catch {
    // Without storage the tour simply opens again next time.
  }
}

export class Tour {
  // hooks: steps() → [{ title, text, target (selector), action?: { label, run } }],
  // onChange()
  constructor(container, hooks) {
    this.hooks = hooks;
    this.index = -1;
    this.highlighted = null;
    this.root = el("aside", "lab-tour");
    this.root.setAttribute("aria-label", "Cluster Lab tour");
    this.root.hidden = true;
    container.appendChild(this.root);
    this.lab = container.closest("#krabka-lab");
  }

  get active() {
    return this.index >= 0;
  }

  // Opens the tour the first time this browser sees the lab.
  maybeStart() {
    if (!seen()) this.start();
  }

  start() {
    this.index = 0;
    this.render();
  }

  close() {
    this.index = -1;
    this.root.hidden = true;
    this.clearHighlight();
    remember();
    this.hooks.onChange?.();
  }

  go(delta) {
    const steps = this.hooks.steps();
    const next = this.index + delta;
    if (next < 0) return;
    if (next >= steps.length) {
      this.close();
      return;
    }
    this.index = next;
    this.render();
  }

  clearHighlight() {
    if (this.highlighted) this.highlighted.classList.remove("lab-tour-target");
    this.highlighted = null;
  }

  render() {
    const steps = this.hooks.steps();
    const step = steps[this.index];
    if (!step) {
      this.close();
      return;
    }
    this.clearHighlight();
    const target = step.target && this.lab ? this.lab.querySelector(step.target) : null;
    if (target) {
      target.classList.add("lab-tour-target");
      this.highlighted = target;
    }
    this.root.hidden = false;
    this.root.replaceChildren();
    const last = this.index === steps.length - 1;
    const head = el("div", "lab-tour-head");
    head.append(el("span", "lab-tour-step", `Step ${this.index + 1} of ${steps.length}`), button("Skip tour", "lab-btn-sm lab-tour-skip", () => this.close()));
    this.root.append(head, el("h3", "lab-tour-title", step.title), el("p", "lab-tour-text", step.text));
    if (step.action) {
      this.root.appendChild(
        button(step.action.label, "lab-btn-sm lab-tour-action", () => {
          step.action.run();
        }),
      );
    }
    const nav = el("div", "lab-tour-nav");
    const back = button("Back", "lab-btn-sm", () => this.go(-1));
    back.disabled = this.index === 0;
    nav.append(back, button(last ? "Done" : "Next", "lab-btn-sm lab-primary", () => this.go(1)));
    this.root.appendChild(nav);
    this.hooks.onChange?.();
  }
}
