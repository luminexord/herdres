import assert from 'node:assert/strict';
import test from 'node:test';

import { shouldAutoFocusTerminal } from '../web/focus-policy.js';

test('shouldAutoFocusTerminal disables automatic terminal focus for coarse pointer devices', () => {
  assert.equal(shouldAutoFocusTerminal({ coarsePointer: true }), false);
  assert.equal(shouldAutoFocusTerminal({ coarsePointer: false }), true);
});
