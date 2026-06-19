import assert from 'node:assert/strict';
import test from 'node:test';

import { createScrollWriter } from './scroll-control.js';

test('createScrollWriter writes wheel events inside the focused pane layout', async () => {
  const writes = [];
  let loads = 0;
  const writer = createScrollWriter({
    getSize: () => ({ cols: 178, rows: 57 }),
    getTerm: () => ({ write: (input) => writes.push(input) }),
    loadLayout: () => {
      loads += 1;
      return {
        result: {
          layout: {
            focused_pane_id: 'w2:p4',
            panes: [
              { pane_id: 'w2:p1', rect: { height: 56, width: 88, x: 26, y: 1 } },
              { focused: true, pane_id: 'w2:p4', rect: { height: 56, width: 64, x: 114, y: 1 } },
            ],
          },
        },
      };
    },
  });

  await writer.write({ direction: 'down', units: 2 });
  await writer.write({ direction: 'up', units: 1 });

  assert.deepEqual(writes, ['\x1b[<65;146;29M\x1b[<65;146;29M', '\x1b[<64;146;29M']);
  assert.equal(loads, 1);
});
