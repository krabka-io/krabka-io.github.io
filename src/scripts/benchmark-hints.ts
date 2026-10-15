const tooltip = document.createElement('div');
tooltip.id = 'benchmark-workload-tooltip';
tooltip.setAttribute('role', 'tooltip');
tooltip.hidden = true;
document.body.append(tooltip);
let active: HTMLElement | null = null;
let pinned = false;
let closeTimer: ReturnType<typeof setTimeout>;
const trigger = (target: EventTarget | null) => target instanceof Element ? target.closest<HTMLElement>('[data-benchmark-hint]') : null;
function hide() { clearTimeout(closeTimer); tooltip.hidden = true; active = null; pinned = false; }
function show(button: HTMLElement) {
  clearTimeout(closeTimer);
  if (active !== button) pinned = false;
  active = button;
  tooltip.textContent = button.dataset.hintText ?? '';
  tooltip.hidden = false;
  const rect = button.getBoundingClientRect();
  const left = Math.max(12, Math.min(rect.left, innerWidth - tooltip.offsetWidth - 12));
  const top = rect.bottom + 8 + tooltip.offsetHeight <= innerHeight - 12 ? rect.bottom + 8 : Math.max(12, rect.top - tooltip.offsetHeight - 8);
  tooltip.style.left = `${left}px`;
  tooltip.style.top = `${top}px`;
}
document.addEventListener('pointerover', event => { const button = trigger(event.target); if (button) show(button); });
document.addEventListener('pointerout', event => {
  const button = trigger(event.target);
  if (button && !button.contains(event.relatedTarget as Node) && !pinned) closeTimer = setTimeout(hide, 150);
});
tooltip.addEventListener('pointerenter', () => clearTimeout(closeTimer));
tooltip.addEventListener('pointerleave', () => { if (!pinned) hide(); });
document.addEventListener('focusin', event => { const button = trigger(event.target); if (button) show(button); });
document.addEventListener('focusout', event => { if (trigger(event.target)) hide(); });
document.addEventListener('click', event => {
  const button = trigger(event.target);
  if (button) { if (active === button && pinned) hide(); else { show(button); pinned = true; } }
  else if (!tooltip.contains(event.target as Node)) hide();
});
document.addEventListener('keydown', event => { if (event.key === 'Escape') hide(); });
window.addEventListener('resize', hide);
window.addEventListener('scroll', () => {
  if (!active) return;
  const rect = active.getBoundingClientRect();
  if (rect.bottom < 0 || rect.top > innerHeight) hide(); else show(active);
});
