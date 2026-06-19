import { normalizeSpaces, paneTabBreakClass, panesForSpace, selectedSpaceId } from './space-panes.js';

export function statusTone(status) {
  const normalized = String(status || '').toLowerCase();
  if (['blocked', 'error', 'failed', 'paused'].some((term) => normalized.includes(term))) return 'red';
  if (['working', 'running', 'busy', 'pending'].some((term) => normalized.includes(term))) return 'yellow';
  if (['idle', 'done', 'complete', 'ready'].some((term) => normalized.includes(term))) return 'green';
  return 'muted';
}

function renderEmpty(el, label) {
  el.innerHTML = '';
  const empty = document.createElement('span');
  empty.className = 'drove__empty';
  empty.textContent = label;
  el.appendChild(empty);
}

function renderHead({ item, focused, className = '', onClick }) {
  const el = document.createElement('button');
  const tone = statusTone(item.status);
  el.className = ['head', `head--${tone}`, focused ? 'head--focused' : '', className].filter(Boolean).join(' ');
  el.innerHTML = '<span class="head__status" aria-hidden="true"></span><span class="head__name"></span>';
  el.setAttribute('aria-label', `${item.label}: ${item.status || 'unknown'}`);
  el.querySelector('.head__name').textContent = item.label;
  el.addEventListener('click', onClick);
  return el;
}

export function renderSpacePaneNavigation({ spacesEl, panesEl, status, selected, onSpace, onPane }) {
  const panes = status.panes || [];
  const spaces = normalizeSpaces({ spaces: status.spaces || [], panes });
  const current = selectedSpaceId({ spaces, panes, previous: selected });

  spacesEl.innerHTML = '';
  panesEl.innerHTML = '';
  if (!spaces.length) {
    renderEmpty(spacesEl, 'no spaces yet');
    renderEmpty(panesEl, 'no panes yet');
    return current;
  }

  for (const space of spaces) {
    spacesEl.appendChild(renderHead({
      item: space,
      focused: space.id === current,
      onClick: () => onSpace(space.id),
    }));
  }

  const visiblePanes = panesForSpace(panes, current);
  if (!visiblePanes.length) {
    renderEmpty(panesEl, 'no panes in space');
    return current;
  }
  for (let i = 0; i < visiblePanes.length; i += 1) {
    const pane = visiblePanes[i];
    panesEl.appendChild(renderHead({
      item: pane,
      focused: !!pane.focused,
      className: paneTabBreakClass(pane, visiblePanes[i - 1]),
      onClick: () => onPane(pane.id),
    }));
  }
  return current;
}
