import { spawn as spawnProc } from 'node:child_process';

export function runHerdrJson(herdrBin, args) {
  return new Promise((resolve, reject) => {
    const p = spawnProc(herdrBin, args, { timeout: 6000 });
    let out = '';
    let err = '';
    p.stdout.on('data', (b) => (out += b));
    p.stderr.on('data', (b) => (err += b));
    p.on('error', reject);
    p.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(err.trim() || `${herdrBin} ${args.join(' ')} exited ${code}`));
        return;
      }
      try {
        resolve(JSON.parse(out));
      } catch (e) {
        reject(e);
      }
    });
  });
}

export function buildStatusPayload({ paneData, workspaceData }) {
  const rawPanes = paneData.result?.panes || paneData.panes || [];
  const rawSpaces = workspaceData.result?.workspaces || workspaceData.workspaces || [];
  const panes = rawPanes.map((x) => ({
    id: x.pane_id,
    label: x.label || x.title || x.agent || x.pane_id,
    agent: x.agent || '',
    status: x.agent_status || 'unknown',
    focused: !!x.focused,
    spaceId: x.workspace_id || String(x.pane_id || '').split(':')[0],
    tabId: x.tab_id || '',
  }));
  const seen = new Set();
  const spaces = rawSpaces.map((x) => {
    const id = x.workspace_id;
    seen.add(id);
    return {
      id,
      label: x.label || id,
      status: x.agent_status || 'unknown',
      focused: !!x.focused,
      number: Number.isFinite(x.number) ? x.number : null,
    };
  });
  for (const pane of panes) {
    if (!pane.spaceId || seen.has(pane.spaceId)) continue;
    seen.add(pane.spaceId);
    spaces.push({
      id: pane.spaceId,
      label: pane.spaceId,
      status: pane.status,
      focused: pane.focused,
      number: null,
    });
  }
  return { spaces, panes };
}

export async function collectStatus(herdrBin) {
  const [paneData, workspaceData] = await Promise.all([
    runHerdrJson(herdrBin, ['pane', 'list']),
    runHerdrJson(herdrBin, ['workspace', 'list']),
  ]);
  return buildStatusPayload({ paneData, workspaceData });
}
