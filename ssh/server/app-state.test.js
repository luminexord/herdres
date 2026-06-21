import assert from 'node:assert/strict';
import test from 'node:test';

import { focusPayloadForPane, selectedPaneForSend, textSubmitPayload } from '../web/app-state.js';

test('textSubmitPayload targets the selected space pane instead of a focused pane in another space', () => {
  const status = {
    panes: [
      { id: 'w1:p1', spaceId: 'w1', agent: 'claude', focused: false },
      { id: 'w2:p1', spaceId: 'w2', agent: 'codex', focused: true },
    ],
  };

  assert.equal(selectedPaneForSend(status, 'w1').id, 'w1:p1');
  assert.deepEqual(textSubmitPayload({ status, selectedSpace: 'w1', text: 'hello' }), {
    t: 'send_text',
    id: 'w1:p1',
    text: 'hello',
  });
});

test('textSubmitPayload refuses ambiguous selected spaces with no focused pane', () => {
  const status = {
    panes: [
      { id: 'w1:p1', spaceId: 'w1', focused: false },
      { id: 'w1:p2', spaceId: 'w1', focused: false },
      { id: 'w2:p1', spaceId: 'w2', focused: true },
    ],
  };

  assert.equal(selectedPaneForSend(status, 'w1'), null);
  assert.equal(textSubmitPayload({ status, selectedSpace: 'w1', text: 'hello' }), null);
});

test('focusPayloadForPane includes the pane workspace', () => {
  assert.deepEqual(focusPayloadForPane({ id: 'w4:p7', spaceId: 'w4' }), {
    t: 'focus',
    id: 'w4:p7',
    spaceId: 'w4',
  });
});
