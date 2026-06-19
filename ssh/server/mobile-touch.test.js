import assert from 'node:assert/strict';
import test from 'node:test';

import { terminalSwipeAction, terminalTapAction } from '../web/touch-scroll.js';

test('terminalSwipeAction suppresses real terminal swipes without scrolling the pane', () => {
  assert.equal(terminalSwipeAction({ startY: 520, currentY: 516 }), 'ignore');
  assert.equal(terminalSwipeAction({ startY: 520, currentY: 360 }), 'suppress');
});

test('terminalTapAction suppresses tap focus when mobile keyboard focus is disabled', () => {
  assert.equal(terminalTapAction({ moved: false, tapped: true, focusOnTap: false }), 'suppress');
  assert.equal(terminalTapAction({ moved: false, tapped: true, focusOnTap: true }), 'focus');
  assert.equal(terminalTapAction({ moved: true, tapped: true, focusOnTap: false }), 'none');
});
