export function focusPaneCommands({ paneId, spaceId }) {
  if (!paneId) return [];
  const commands = [];
  if (spaceId) commands.push(['workspace', 'focus', String(spaceId)]);
  commands.push(['agent', 'focus', String(paneId)]);
  return commands;
}

export function sendTextArgs({ paneId, text }) {
  const body = String(text || '');
  if (!paneId || !body) return null;
  return ['pane', 'run', String(paneId), body];
}
