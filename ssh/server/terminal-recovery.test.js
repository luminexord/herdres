import assert from 'node:assert/strict';
import test from 'node:test';

import {
  recoverTerminalAfterFit,
  recoverTerminalViewport,
  shouldRecoverTerminalViewport,
} from '../web/terminal-recovery.js';

test('shouldRecoverTerminalViewport only recovers mobile terminal after a real size change', () => {
  assert.equal(shouldRecoverTerminalViewport({
    coarsePointer: true,
    previousCols: 80,
    previousRows: 24,
    nextCols: 51,
    nextRows: 19,
  }), true);
  assert.equal(shouldRecoverTerminalViewport({
    coarsePointer: false,
    previousCols: 80,
    previousRows: 24,
    nextCols: 51,
    nextRows: 19,
  }), false);
  assert.equal(shouldRecoverTerminalViewport({
    coarsePointer: true,
    previousCols: 51,
    previousRows: 19,
    nextCols: 51,
    nextRows: 19,
  }), false);
  assert.equal(shouldRecoverTerminalViewport({
    coarsePointer: true,
    previousCols: 0,
    previousRows: 0,
    nextCols: 51,
    nextRows: 19,
  }), false);
});

test('recoverTerminalViewport clears stale scrollback and returns to the live screen', () => {
  const calls = [];
  const term = {
    clear() {
      calls.push('clear');
    },
    scrollToBottom() {
      calls.push('scrollToBottom');
    },
  };

  recoverTerminalViewport(term);

  assert.deepEqual(calls, ['clear', 'scrollToBottom']);
});

test('recoverTerminalAfterFit returns the next terminal size and recovers stale mobile buffer', () => {
  const calls = [];
  const term = {
    cols: 51,
    rows: 19,
    clear() {
      calls.push('clear');
    },
    scrollToBottom() {
      calls.push('scrollToBottom');
    },
  };

  const size = recoverTerminalAfterFit({
    coarsePointer: true,
    previousSize: { cols: 47, rows: 45 },
    term,
  });

  assert.deepEqual(size, { cols: 51, rows: 19 });
  assert.deepEqual(calls, ['clear', 'scrollToBottom']);
});
