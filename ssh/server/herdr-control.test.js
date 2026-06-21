import assert from 'node:assert/strict';
import test from 'node:test';

import { focusPaneCommands, sendTextArgs } from './herdr-control.js';

test('focusPaneCommands focuses the workspace before the exact pane target', () => {
  assert.deepEqual(focusPaneCommands({ paneId: 'w5:pA', spaceId: 'w5' }), [
    ['workspace', 'focus', 'w5'],
    ['agent', 'focus', 'w5:pA'],
  ]);
});

test('sendTextArgs submits text through the exact pane id', () => {
  assert.deepEqual(sendTextArgs({ paneId: 'w2:p1', text: 'how are you' }), [
    'pane',
    'run',
    'w2:p1',
    'how are you',
  ]);
  assert.equal(sendTextArgs({ paneId: '', text: 'hello' }), null);
  assert.equal(sendTextArgs({ paneId: 'w2:p1', text: '' }), null);
});
