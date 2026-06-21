import { normalizeSpaces, paneTabBreakClass, panesForSpace, selectedSpaceId } from './space-panes.js';

export function statusTone(status) {
  const normalized = String(status || '').toLowerCase();
  if (['blocked', 'error', 'failed', 'paused'].some((term) => normalized.includes(term))) return 'red';
  if (['working', 'running', 'busy', 'pending'].some((term) => normalized.includes(term))) return 'yellow';
  if (['idle', 'done', 'complete', 'ready'].some((term) => normalized.includes(term))) return 'green';
  return 'muted';
}

const AGENT_BADGES = {
  codex: { initial: 'C', cls: 'agent--codex' },
  claude: { initial: 'Cl', cls: 'agent--claude' },
  devin: { initial: 'D', cls: 'agent--devin' },
  kimi: { initial: 'K', cls: 'agent--kimi' },
  omp: { initial: 'O', cls: 'agent--omp' },
};

function agentBadge(agent) {
  const key = String(agent || '').toLowerCase().trim();
  return AGENT_BADGES[key] || null;
}

function headClassName(tone, focused, extra = '') {
  return ['head', `head--${tone}`, focused ? 'head--focused' : '', extra].filter(Boolean).join(' ');
}

function bindHead(el, item, onClick) {
  el.dataset.id = item.id || '';
  el.onclick = onClick || null;
}

function createSpaceHead({ item, paneCount, focused, onClick }) {
  const el = document.createElement('button');
  const tone = statusTone(item.status);
  el.className = headClassName(tone, focused);
  el.innerHTML = '<span class="head__status" aria-hidden="true"></span><span class="head__name"></span>' +
    (paneCount > 0 ? '<span class="head__count"></span>' : '');
  el.querySelector('.head__name').textContent = item.label;
  if (paneCount > 0) el.querySelector('.head__count').textContent = String(paneCount);
  bindHead(el, item, onClick);
  return el;
}

function updateSpaceHead(el, { item, paneCount, focused, onClick }) {
  const tone = statusTone(item.status);
  const cls = headClassName(tone, focused);
  if (el.className !== cls) el.className = cls;
  bindHead(el, item, onClick);
  const nameEl = el.querySelector('.head__name');
  if (nameEl && nameEl.textContent !== item.label) nameEl.textContent = item.label;
  const countEl = el.querySelector('.head__count');
  const wantCount = paneCount > 0;
  if (wantCount && !countEl) {
    const c = document.createElement('span');
    c.className = 'head__count';
    c.textContent = String(paneCount);
    el.appendChild(c);
  } else if (!wantCount && countEl) {
    countEl.remove();
  } else if (countEl && countEl.textContent !== String(paneCount)) {
    countEl.textContent = String(paneCount);
  }
}

function createPaneHead({ item, focused, className = '', onClick }) {
  const el = document.createElement('button');
  const tone = statusTone(item.status);
  const badge = agentBadge(item.agent);
  el.className = headClassName(tone, focused, className);
  el.innerHTML =
    '<span class="head__status" aria-hidden="true"></span>' +
    (badge ? `<span class="head__agent ${badge.cls}" aria-hidden="true">${badge.initial}</span>` : '') +
    '<span class="head__name"></span>';
  el.querySelector('.head__name').textContent = item.label;
  bindHead(el, item, onClick);
  return el;
}

function updatePaneHead(el, { item, focused, className = '', onClick }) {
  const tone = statusTone(item.status);
  const badge = agentBadge(item.agent);
  const cls = headClassName(tone, focused, className);
  if (el.className !== cls) el.className = cls;
  bindHead(el, item, onClick);
  const nameEl = el.querySelector('.head__name');
  if (nameEl && nameEl.textContent !== item.label) nameEl.textContent = item.label;
  const existingBadge = el.querySelector('.head__agent');
  const wantBadge = !!badge;
  if (wantBadge && !existingBadge) {
    const b = document.createElement('span');
    b.className = `head__agent ${badge.cls}`;
    b.setAttribute('aria-hidden', 'true');
    b.textContent = badge.initial;
    el.insertBefore(b, nameEl);
  } else if (!wantBadge && existingBadge) {
    existingBadge.remove();
  } else if (wantBadge && existingBadge) {
    const wantCls = `head__agent ${badge.cls}`;
    if (existingBadge.className !== wantCls) existingBadge.className = wantCls;
    if (existingBadge.textContent !== badge.initial) existingBadge.textContent = badge.initial;
  }
}

function syncList(el, items, createFn, updateFn) {
  const existing = el.querySelectorAll(':scope > .head');
  if (!items.length && !existing.length) return;
  if (!items.length) {
    el.innerHTML = '';
    return;
  }
  for (let i = 0; i < items.length; i++) {
    const data = items[i];
    let node = existing[i];
    if (!node) {
      node = createFn(data);
      el.appendChild(node);
    } else {
      updateFn(node, data);
    }
  }
  for (let i = items.length; i < existing.length; i++) {
    existing[i].remove();
  }
}

function renderEmpty(el, label) {
  const existing = el.querySelector('.drove__empty');
  if (existing) {
    if (existing.textContent !== label) existing.textContent = label;
    return;
  }
  el.innerHTML = '';
  const empty = document.createElement('span');
  empty.className = 'drove__empty';
  empty.textContent = label;
  el.appendChild(empty);
}

export function renderSpacePaneNavigation({ spacesEl, panesEl, status, selected, onSpace, onPane }) {
  const panes = status.panes || [];
  const spaces = normalizeSpaces({ spaces: status.spaces || [], panes });
  const current = selectedSpaceId({ spaces, panes, previous: selected });

  if (!spaces.length) {
    syncList(spacesEl, [], null, null);
    renderEmpty(spacesEl, 'no spaces yet');
    renderEmpty(panesEl, 'no panes yet');
    return current;
  }

  const spaceItems = spaces.map((space) => ({
    item: space,
    paneCount: panesForSpace(panes, space.id).length,
    focused: space.id === current,
    onClick: () => onSpace(space.id),
  }));
  syncList(spacesEl, spaceItems, createSpaceHead, updateSpaceHead);
  const spaceEmpty = spacesEl.querySelector('.drove__empty');
  if (spaceEmpty) spaceEmpty.remove();

  const visiblePanes = panesForSpace(panes, current);
  if (!visiblePanes.length) {
    syncList(panesEl, [], null, null);
    renderEmpty(panesEl, 'no panes in space');
    return current;
  }
  const paneItems = visiblePanes.map((pane, i) => ({
    item: pane,
    focused: !!pane.focused,
    className: paneTabBreakClass(pane, visiblePanes[i - 1]),
    onClick: () => onPane(pane.id),
  }));
  syncList(panesEl, paneItems, createPaneHead, updatePaneHead);
  const paneEmpty = panesEl.querySelector('.drove__empty');
  if (paneEmpty) paneEmpty.remove();

  return current;
}
