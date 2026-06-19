import assert from 'node:assert/strict';
import test from 'node:test';

import { statusTone } from '../web/navigation.js';
import { focusPaneInStatus, normalizeSpaces, paneTabBreakClass, panesForSpace, selectedSpaceId } from '../web/space-panes.js';

test('selectedSpaceId prefers the focused Herdr space and panesForSpace filters panes to it', () => {
  const spaces = [
    { id: 'w1', label: 'Rust', focused: false },
    { id: 'w2', label: 'herdres', focused: true },
  ];
  const panes = [
    { id: 'w1:p1', label: 'claude', spaceId: 'w1' },
    { id: 'w2:p1', label: 'codex', spaceId: 'w2' },
    { id: 'w2:p4', label: 'claude', spaceId: 'w2' },
  ];

  const current = selectedSpaceId({ spaces, panes, previous: '' });

  assert.equal(current, 'w2');
  assert.deepEqual(panesForSpace(panes, current).map((pane) => pane.label), ['codex', 'claude']);
});

test('normalizeSpaces adds a fallback space when panes arrive before workspace labels', () => {
  const spaces = normalizeSpaces({
    spaces: [],
    panes: [{ id: 'w9:p1', label: 'kimi', status: 'idle', focused: false }],
  });

  assert.deepEqual(spaces, [{ id: 'w9', label: 'w9', status: 'idle', focused: false, number: null }]);
});

test('focusPaneInStatus optimistically focuses one pane and its space', () => {
  const status = {
    spaces: [
      { id: 'w1', label: 'Rust', focused: true },
      { id: 'w2', label: 'herdres', focused: false },
    ],
    panes: [
      { id: 'w1:p1', label: 'claude', spaceId: 'w1', focused: true },
      { id: 'w2:p1', label: 'codex', spaceId: 'w2', focused: false },
      { id: 'w2:p4', label: 'claude', spaceId: 'w2', focused: false },
    ],
  };

  const next = focusPaneInStatus(status, 'w2:p4');

  assert.deepEqual(next.spaces.map((space) => space.focused), [false, true]);
  assert.deepEqual(next.panes.map((pane) => pane.focused), [false, false, true]);
});

test('paneTabBreakClass only separates panes across tab boundaries', () => {
  const first = { id: 'w2:p1', tabId: 'w2:t1' };
  const secondSameTab = { id: 'w2:p2', tabId: 'w2:t1' };
  const thirdNewTab = { id: 'w2:p3', tabId: 'w2:t2' };

  assert.equal(paneTabBreakClass(first, null), '');
  assert.equal(paneTabBreakClass(secondSameTab, first), '');
  assert.equal(paneTabBreakClass(thirdNewTab, secondSameTab), 'head--tab-break');
});

test('statusTone maps pane statuses to mobile navigation dot colors', () => {
  assert.equal(statusTone('blocked'), 'red');
  assert.equal(statusTone('paused'), 'red');
  assert.equal(statusTone('working'), 'yellow');
  assert.equal(statusTone('idle'), 'green');
  assert.equal(statusTone('unknown'), 'muted');
});
