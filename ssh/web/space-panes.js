export function paneSpaceId(pane) {
  return pane.spaceId || pane.workspaceId || String(pane.id || '').split(':')[0] || '';
}

export function normalizeSpaces({ spaces = [], panes = [] }) {
  const seen = new Set();
  const out = [];
  for (const space of spaces) {
    if (!space.id || seen.has(space.id)) continue;
    seen.add(space.id);
    out.push(space);
  }
  for (const pane of panes) {
    const id = paneSpaceId(pane);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    out.push({ id, label: id, status: pane.status, focused: pane.focused, number: null });
  }
  return out;
}

export function selectedSpaceId({ spaces, panes, previous }) {
  const normalized = normalizeSpaces({ spaces, panes });
  if (previous && normalized.some((space) => space.id === previous)) return previous;
  const focusedSpace = normalized.find((space) => space.focused);
  if (focusedSpace) return focusedSpace.id;
  const focusedPane = panes.find((pane) => pane.focused);
  if (focusedPane) return paneSpaceId(focusedPane);
  return normalized[0]?.id || '';
}

export function panesForSpace(panes, spaceId) {
  return panes.filter((pane) => paneSpaceId(pane) === spaceId);
}

export function paneTabBreakClass(pane, previousPane) {
  if (!previousPane) return '';
  const tabId = pane.tabId || '';
  const previousTabId = previousPane.tabId || '';
  if (!tabId || !previousTabId || tabId === previousTabId) return '';
  return 'head--tab-break';
}

export function focusPaneInStatus(status, paneId) {
  const panes = status.panes || [];
  let targetSpace = '';
  const nextPanes = panes.map((pane) => {
    const focused = pane.id === paneId;
    if (focused) targetSpace = paneSpaceId(pane);
    return pane.focused === focused ? pane : { ...pane, focused };
  });
  if (!targetSpace) return status;
  const nextSpaces = (status.spaces || []).map((space) => {
    const focused = space.id === targetSpace;
    return space.focused === focused ? space : { ...space, focused };
  });
  return { ...status, spaces: nextSpaces, panes: nextPanes };
}
