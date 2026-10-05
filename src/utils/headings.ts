import { slugify } from './slugify.mjs';

// Every h2 to h4 in the page's main content gets a "#" link to itself, so any
// section can be linked to. The build gives each one an id (heading-ids.mjs);
// a heading a script adds later gets one here. Safe to call twice.

export function enhanceHeadings(): HTMLElement[] {
  const main = document.getElementById('main');
  if (!main) return [];
  const headings = [...main.querySelectorAll<HTMLElement>('h2, h3, h4')].filter(
    (h) => !h.closest('nav, aside, [data-no-anchor], details > summary'),
  );
  const used = new Set([...document.querySelectorAll('[id]')].map((el) => el.id));
  for (const h of headings) {
    if (h.dataset.anchored !== undefined) continue;
    h.dataset.anchored = '';
    if (!h.id) {
      const base = slugify(h.textContent ?? '') || 'section';
      let id = base;
      for (let i = 2; used.has(id); i++) id = `${base}-${i}`;
      used.add(id);
      h.id = id;
    }
    // Card headings keep their section ids, but their enclosing link already
    // supplies the action. A nested permalink would make invalid links.
    if (h.closest('a')) continue;
    const link = document.createElement('a');
    link.className = 'heading-anchor';
    link.href = `#${h.id}`;
    link.setAttribute('aria-label', `Link to section: ${h.textContent?.trim()}`);
    link.textContent = '#';
    h.append(link);
  }
  return headings;
}
