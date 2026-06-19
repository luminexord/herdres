import assert from 'node:assert/strict';
import test from 'node:test';

import { focusedPaneWheelTarget, terminalMouseWheelInput } from './terminal-wheel.js';

test('terminalMouseWheelInput emits SGR mouse wheel events for the terminal app', () => {
  assert.equal(terminalMouseWheelInput('down', 2, 80, 24, 7, 9), '\x1b[<65;7;9M\x1b[<65;7;9M');
  assert.equal(terminalMouseWheelInput('up', 1, 81, 25), '\x1b[<64;40;12M');
  assert.equal(terminalMouseWheelInput('down', 1, 10, 10, 99, -3), '\x1b[<65;10;1M');
  assert.equal(terminalMouseWheelInput('sideways', 2, 80, 24), '');
});

test('focusedPaneWheelTarget picks the focused pane rectangle in a split tab', () => {
  const layout = {
    result: {
      layout: {
        focused_pane_id: 'w2:p4',
        panes: [
          { focused: false, pane_id: 'w2:p1', rect: { height: 56, width: 88, x: 26, y: 1 } },
          { focused: true, pane_id: 'w2:p4', rect: { height: 56, width: 64, x: 114, y: 1 } },
        ],
      },
    },
  };

  assert.deepEqual(focusedPaneWheelTarget(layout, 178, 57), { x: 146, y: 29 });
  assert.equal(focusedPaneWheelTarget({ result: { layout: { panes: [] } } }), null);
});
