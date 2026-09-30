/**
 * Makes a tablist behave as a tab widget. The markup carries the roles (`role=tab` with
 * `aria-controls`, `role=tabpanel` with `aria-labelledby`, panels beyond the first `hidden`);
 * this adds selection, a roving tabindex, and the Arrow/Home/End keys. Style the selected
 * tab with the `aria-selected:` variant so no class juggling is needed here.
 */
export function initTabs(list: HTMLElement, onSelect?: (tab: HTMLElement, panel: HTMLElement) => void) {
  const tabs = [...list.querySelectorAll<HTMLElement>('[role="tab"]')];

  function select(tab: HTMLElement, focus = false) {
    tabs.forEach((t) => {
      const on = t === tab;
      t.setAttribute('aria-selected', String(on));
      t.tabIndex = on ? 0 : -1;
      const panel = document.getElementById(t.getAttribute('aria-controls')!);
      if (panel) panel.hidden = !on;
      if (on && panel) onSelect?.(t, panel);
    });
    if (focus) tab.focus();
  }

  tabs.forEach((tab, i) => {
    tab.addEventListener('click', () => select(tab));
    tab.addEventListener('keydown', (e) => {
      const to =
        e.key === 'ArrowRight' ? (i + 1) % tabs.length :
        e.key === 'ArrowLeft' ? (i - 1 + tabs.length) % tabs.length :
        e.key === 'Home' ? 0 :
        e.key === 'End' ? tabs.length - 1 : -1;
      if (to < 0) return;
      e.preventDefault();
      select(tabs[to], true);
    });
  });
}
