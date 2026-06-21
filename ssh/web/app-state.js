import { paneSpaceId, panesForSpace } from './space-panes.js';

export function selectedPaneForSend(status, selectedSpace) {
  const panes = status?.panes || [];
  const visible = selectedSpace ? panesForSpace(panes, selectedSpace) : [];
  if (visible.length) return visible.find((pane) => pane.focused) || (visible.length === 1 ? visible[0] : null);
  return panes.find((pane) => pane.focused) || (panes.length === 1 ? panes[0] : null);
}

export function focusPayloadForPane(pane) {
  if (!pane?.id) return null;
  return { t: 'focus', id: pane.id, spaceId: paneSpaceId(pane) };
}

export function textSubmitPayload({ status, selectedSpace, text }) {
  const body = String(text || '');
  if (!body) return null;
  const pane = selectedPaneForSend(status, selectedSpace);
  return pane?.id ? { t: 'send_text', id: pane.id, text: body } : null;
}
