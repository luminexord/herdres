import assert from 'node:assert/strict';
import test from 'node:test';

import { buildStatusPayload } from './status.js';

test('buildStatusPayload keeps workspace labels for the space row and pane membership for the pane row', () => {
  const payload = buildStatusPayload({
    workspaceData: {
      result: {
        workspaces: [
          { workspace_id: 'w1', label: 'Rust', agent_status: 'working', focused: false, number: 1 },
          { workspace_id: 'w2', label: 'herdres', agent_status: 'idle', focused: true, number: 2 },
        ],
      },
    },
    paneData: {
      result: {
        panes: [
          { pane_id: 'w1:p1', workspace_id: 'w1', tab_id: 'w1:t1', agent: 'claude', agent_status: 'working' },
          { pane_id: 'w2:p1', workspace_id: 'w2', tab_id: 'w2:t1', agent: 'codex', agent_status: 'idle', focused: true },
        ],
      },
    },
  });

  assert.deepEqual(payload.spaces.map((space) => space.label), ['Rust', 'herdres']);
  assert.equal(payload.panes[0].spaceId, 'w1');
  assert.equal(payload.panes[1].spaceId, 'w2');
  assert.equal(payload.panes[1].label, 'codex');
});
